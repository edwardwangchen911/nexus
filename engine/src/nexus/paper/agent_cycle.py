"""Resumable, append-only coordination for one internal Agent paper cycle."""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import ActionableResearchError
from nexus.agents.codex_candidate import (
    require_candidate_consumer_profile,
    require_candidate_model_profile,
)
from nexus.agents.paper_context import (
    paper_contexts_match_projection,
    prepare_agent_paper_research_snapshot,
    read_agent_paper_research_context,
)
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_research import (
    AGENT_VERSION_CHALLENGER_ACTOR,
    AGENT_VERSION_INCUMBENT_ACTOR,
    admit_agent_paper_research,
)
from nexus.jobs.models import JobType, job_input_hash
from nexus.paper.agent_cycle_authority import (
    CycleAuthority,
    load_cycle_authority,
    load_recorded_schwab_cycle_authority,
    prepare_candidates,
    validate_candidate_manifest,
)
from nexus.paper.agent_cycle_models import (
    AgentPaperCycleResult,
    AgentPaperTriggerReservation,
    CandidateManifestEntry,
    CycleStatus,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    floor_decision_slot,
    parse_candidate_manifest,
)
from nexus.paper.agent_decision import (
    AGENT_PAPER_SIZING_POLICY_HASH,
    AGENT_PAPER_SIZING_V1,
    AgentPaperDecisionError,
    build_agent_paper_decision,
)
from nexus.paper.agent_decision_store import (
    persist_agent_paper_decision,
    persist_portfolio_proposal,
)
from nexus.paper.candidate_batch import (
    has_active_portfolio_synthesis_prompt,
    load_agent_paper_candidate_batch,
    validate_agent_paper_candidate_batch,
)
from nexus.paper.errors import PaperBatchError
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH, PORTFOLIO_ALLOCATION_V1
from nexus.paper.portfolio_proposal import KNOWN_PORTFOLIO_PROPOSAL_REJECTIONS
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.agent_versions import AgentVersionEvaluationRecord
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.risk.authority import (
    RiskAuthorityInput,
    prepare_risk_market_snapshot,
    prepare_schwab_realtime_risk_snapshot,
)
from nexus.risk.models import PaperPortfolioProjection

__all__ = [
    "AgentPaperCycleResult",
    "AgentPaperTriggerReservation",
    "CandidateManifestEntry",
    "CycleStatus",
    "AgentPaperCycleError",
    "advance_agent_paper_cycle",
    "finish_schwab_agent_paper_trigger",
    "reserve_schwab_agent_paper_trigger",
    "resume_schwab_agent_paper_cycle",
    "start_schwab_agent_paper_cycle",
    "candidate_manifest_hash",
    "canonical_cycle_event_hash",
]


class AgentPaperCycleError(ValueError):
    """Stable fail-closed error for the internal-only cycle boundary."""


_TRIGGER_FAILURE_CODES = frozenset(
    {
        "AGENT_PAPER_ACCOUNT_INVALID",
        "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
        "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        "AGENT_PAPER_MARKET_WINDOW_CLOSED",
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
        "CANDIDATE_BATCH_REQUIRED",
        "CANDIDATE_BATCH_INVALID",
    }
)
_MANUAL_RECOVERY_FAILURE_CODES = frozenset({"SCHWAB_L1_FRESHNESS_UNPROVEN", "SCHWAB_L1_INCOMPLETE"})


async def _require_synthesis_recovery_predecessor(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_session: date,
    candidate_batch_id: UUID,
    now: datetime,
) -> None:
    """Permit one new cycle only after a fully known pre-synthesis terminal failure."""
    code = "AGENT_PAPER_SYNTHESIS_RECOVERY_UNAVAILABLE"
    prior_claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}",
            IdempotencyRecord.route == "agent-paper-manual-trigger-v1",
            IdempotencyRecord.idempotency_key == f"manual-recovery:{decision_session.isoformat()}",
            IdempotencyRecord.schema_version == "agent_paper_manual_trigger_v1",
        )
    )
    payload = None if prior_claim is None else prior_claim.response_payload
    raw_result = payload.get("result") if isinstance(payload, dict) else None
    if payload is None or payload.get("outcome") != "completed" or not isinstance(raw_result, dict):
        raise ValueError(code)
    try:
        prior_result = AgentPaperCycleResult.model_validate(raw_result)
    except ValueError:
        raise ValueError(code) from None
    cycles = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord)
            .where(
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_session == decision_session,
            )
            .limit(2)
        )
    )
    if len(cycles) != 1:
        raise ValueError(code)
    cycle = cycles[0]
    if (
        cycle.id != prior_result.cycle_id
        or cycle.candidate_batch_id != candidate_batch_id
        or not cycle.execution_locked
    ):
        raise ValueError(code)
    events = await _events(session, cycle.id)
    if (
        len(events) != 3
        or [event.to_status for event in events] != ["prepared", "researching", "unavailable"]
        or events[1].result_code != "RESEARCH_JOBS_PREPARED"
        or events[2].from_status != "researching"
        or events[2].result_code != "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
    ):
        raise ValueError(code)
    prompt = await session.scalar(
        select(AgentPromptVersionRecord).where(
            AgentPromptVersionRecord.role == "portfolio_decision",
            AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
            AgentPromptVersionRecord.release_channel == "active",
        )
    )
    if prompt is None or not events[2].created_at < prompt.created_at <= now:
        raise ValueError(code)
    raw_run_ids = events[1].fact_ids.get("research_run_ids")
    if not isinstance(raw_run_ids, list) or not 1 <= len(raw_run_ids) <= 5:
        raise ValueError(code)
    try:
        run_ids = tuple(UUID(str(value)) for value in raw_run_ids)
    except ValueError:
        raise ValueError(code) from None
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type.in_(
                    (JobType.AGENT_RESEARCH.value, JobType.AGENT_PAPER_RESEARCH.value)
                ),
                Job.payload["run_id"].as_string().in_(tuple(str(value) for value in run_ids)),
            )
        )
    )
    if (
        len(run_ids) != len(set(run_ids))
        or len(jobs) != len(run_ids)
        or any(job.status != "succeeded" or job.finished_at is None for job in jobs)
    ):
        raise ValueError(code)
    try:
        for run_id in run_ids:
            await load_actionable_research_output(
                session, run_id=run_id, decision_time=now, allow_safe_no_trade=True
            )
    except ActionableResearchError:
        raise ValueError(code) from None
    if await session.scalar(
        select(AgentPortfolioSynthesisRunRecord.id).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
        )
    ) or await session.scalar(
        select(StrategyDecisionRecord.id).where(
            StrategyDecisionRecord.agent_paper_cycle_id == cycle.id
        )
    ):
        raise ValueError(code)


async def reserve_schwab_agent_paper_trigger(
    session: AsyncSession,
    *,
    account_id: UUID,
    client_idempotency_key: str,
    now: datetime,
    candidate_batch_id: UUID | None = None,
    trigger_mode: Literal["scheduled", "manual_recovery", "synthesis_recovery"] = "scheduled",
    require_synthesis_prompt: bool = False,
) -> AgentPaperTriggerReservation:
    """Reserve one database-derived decision slot before provider work begins."""
    if (
        now.tzinfo is None
        or now.utcoffset() is None
        or not client_idempotency_key
        or len(client_idempotency_key) > 255
    ):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    local_time = now.astimezone(ZoneInfo("America/New_York"))
    scheduled_local = datetime.combine(
        local_time.date(),
        PORTFOLIO_ALLOCATION_V1.decision_local_time,
        tzinfo=ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
    )
    scheduled_slot = scheduled_local.astimezone(UTC)
    if trigger_mode == "scheduled":
        if floor_decision_slot(now) != scheduled_slot:
            raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
        decision_slot_at = scheduled_slot
        route = "agent-paper-schwab-trigger-v2"
        schema = "agent_paper_schwab_trigger_v2"
        claim_key = decision_slot_at.isoformat()
    elif trigger_mode == "manual_recovery":
        # One distinct same-day opportunity only after the scheduled trigger
        # has failed before creating a cycle. The fixed v1 schedule is unchanged.
        if not time(10, 5) <= local_time.time().replace(tzinfo=None) < time(15, 0):
            raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
        decision_slot_at = floor_decision_slot(now)
        route = "agent-paper-manual-trigger-v1"
        schema = "agent_paper_manual_trigger_v1"
        claim_key = f"manual-recovery:{local_time.date().isoformat()}"
        scheduled_claim = await session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}",
                IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                IdempotencyRecord.idempotency_key == scheduled_slot.isoformat(),
                IdempotencyRecord.schema_version == "agent_paper_schwab_trigger_v2",
            )
        )
        payload = None if scheduled_claim is None else scheduled_claim.response_payload
        if (
            not isinstance(payload, dict)
            or payload.get("outcome") != "failed"
            or payload.get("error_code") not in _MANUAL_RECOVERY_FAILURE_CODES
        ):
            raise ValueError("AGENT_PAPER_MANUAL_RECOVERY_UNAVAILABLE")
    elif trigger_mode == "synthesis_recovery":
        if not time(10, 5) <= local_time.time().replace(tzinfo=None) < time(15, 0):
            raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
        decision_slot_at = floor_decision_slot(now)
        route = "agent-paper-synthesis-recovery-v1"
        schema = "agent_paper_synth_recovery_v1"
        claim_key = f"synthesis-recovery:{local_time.date().isoformat()}"
    else:
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise AgentPaperCycleError("CANDIDATE_BATCH_INVALID")
    if candidate_batch_id is None:
        raise ValueError("CANDIDATE_BATCH_REQUIRED")
    batch = await load_agent_paper_candidate_batch(session, batch_id=candidate_batch_id)
    if batch is None or batch.account_id != account_id:
        raise ValueError("CANDIDATE_BATCH_INVALID")
    request_payload = {
        "schema": route,
        "account_id": str(account_id),
        "decision_session": local_time.date().isoformat(),
        "decision_slot_at": decision_slot_at.isoformat(),
        "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
        "client_idempotency_hash": sha256(client_idempotency_key.encode()).hexdigest(),
        "candidate_batch_id": str(batch.batch_id),
        "candidate_batch_hash": batch.batch_hash,
    }
    if trigger_mode != "scheduled":
        request_payload["trigger_mode"] = trigger_mode
    request_hash = job_input_hash(request_payload)
    claim, inserted = await claim_or_lock(
        session,
        f"local-agent-paper-schwab:{account_id}",
        route,
        claim_key,
        request_hash,
        schema,
    )
    if claim.request_hash != request_hash:
        raise ValueError("AGENT_PAPER_IDEMPOTENCY_CONFLICT")
    reservation = AgentPaperTriggerReservation(
        claim_id=claim.id,
        trigger_mode=trigger_mode,
        decision_session=local_time.date(),
        decision_slot_at=decision_slot_at,
        candidate_batch_id=batch.batch_id,
        candidate_batch_hash=batch.batch_hash,
    )
    if inserted:
        if require_synthesis_prompt:
            if not await has_active_portfolio_synthesis_prompt(session, as_of=now):
                raise ValueError("PORTFOLIO_SYNTHESIS_PROMPT_INVALID")
        if trigger_mode == "manual_recovery":
            existing_cycle = await session.scalar(
                select(AgentPaperCycleRecord.id)
                .where(
                    AgentPaperCycleRecord.account_id == account_id,
                    AgentPaperCycleRecord.decision_session == local_time.date(),
                )
                .limit(1)
            )
            if existing_cycle is not None:
                raise ValueError("AGENT_PAPER_MANUAL_RECOVERY_UNAVAILABLE")
        elif trigger_mode == "synthesis_recovery":
            await _require_synthesis_recovery_predecessor(
                session,
                account_id=account_id,
                decision_session=local_time.date(),
                candidate_batch_id=batch.batch_id,
                now=now,
            )
        await validate_agent_paper_candidate_batch(
            session,
            batch_id=batch.batch_id,
            account_id=account_id,
            as_of=now,
        )
        return reservation
    if not claim.response_payload:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    outcome = claim.response_payload.get("outcome")
    if outcome == "failed":
        error_code = claim.response_payload.get("error_code")
        if isinstance(error_code, str) and error_code in _TRIGGER_FAILURE_CODES:
            raise ValueError(error_code)
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    if outcome != "completed" or not isinstance(claim.response_payload.get("result"), dict):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    try:
        replay = AgentPaperCycleResult.model_validate(claim.response_payload["result"])
    except ValueError:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN") from None
    return reservation.model_copy(update={"replay": replay.model_copy(update={"replayed": True})})


async def finish_schwab_agent_paper_trigger(
    session: AsyncSession,
    *,
    reservation: AgentPaperTriggerReservation,
    result: AgentPaperCycleResult | None = None,
    error_code: str | None = None,
) -> None:
    """Seal a committed trigger reservation with one fixed safe outcome."""
    if (result is None) == (error_code is None):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    if error_code is not None and error_code not in _TRIGGER_FAILURE_CODES:
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    claim = await session.scalar(
        select(IdempotencyRecord)
        .where(IdempotencyRecord.id == reservation.claim_id)
        .with_for_update()
    )
    if reservation.trigger_mode == "scheduled":
        route = "agent-paper-schwab-trigger-v2"
        schema = "agent_paper_schwab_trigger_v2"
        claim_key = reservation.decision_slot_at.isoformat()
    elif reservation.trigger_mode == "manual_recovery":
        route = "agent-paper-manual-trigger-v1"
        schema = "agent_paper_manual_trigger_v1"
        claim_key = f"manual-recovery:{reservation.decision_session.isoformat()}"
    else:
        route = "agent-paper-synthesis-recovery-v1"
        schema = "agent_paper_synth_recovery_v1"
        claim_key = f"synthesis-recovery:{reservation.decision_session.isoformat()}"
    if (
        claim is None
        or claim.route != route
        or claim.schema_version != schema
        or claim.idempotency_key != claim_key
    ):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    payload: dict[str, object]
    if result is not None:
        payload = {"outcome": "completed", "result": result.model_dump(mode="json")}
    else:
        payload = {"outcome": "failed", "error_code": cast(str, error_code)}
    if claim.response_payload and claim.response_payload != payload:
        raise ValueError("AGENT_PAPER_IDEMPOTENCY_CONFLICT")
    claim.response_payload = payload
    await session.flush()


async def _require_cycle_candidate_profile(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    candidate_stage_capacity: bool,
    now: datetime | None = None,
    expected_model_id: UUID | None = None,
) -> UUID | None:
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    events = await _events(session, cycle.id)
    facts = {} if not events else events[0].fact_ids
    if not candidate_stage_capacity:
        if "candidate_stage_capacity" in facts or expected_model_id is not None:
            raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
        return None
    if (
        not events
        or events[0].result_code != "CYCLE_PREPARED"
        or facts.get("candidate_stage_capacity") is not True
        or cycle.candidate_batch_id is None
        or cycle.execution_locked is not True
    ):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    try:
        model_id = UUID(str(facts["candidate_model_config_id"]))
    except (KeyError, TypeError, ValueError):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID") from None
    if expected_model_id is not None and model_id != expected_model_id:
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    at = now if now is not None else await session.scalar(select(func.now()))
    if not isinstance(at, datetime):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    model = await require_candidate_model_profile(
        session,
        candidate_stage_capacity=True,
        model_config_id=model_id,
        now=at,
    )
    if model is None or facts.get("candidate_model_hash") != model.config_hash:
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    return model_id


async def start_schwab_agent_paper_cycle(
    session: AsyncSession,
    *,
    authority: CycleAuthority,
    decision_slot_at: datetime,
    idempotency_key: str,
    requested_instrument_ids: tuple[UUID, ...],
    candidate_batch_id: UUID | None = None,
    daily_limit_usd: Decimal = Decimal("3.00"),
    version_evaluation_id: UUID | None = None,
    version_lane: str | None = None,
    version_source_run_id: UUID | None = None,
    version_prompt_id: UUID | None = None,
    version_model_id: UUID | None = None,
    execution_scope_max_symbols: int | None = None,
    candidate_stage_capacity: bool = False,
    candidate_model_config_id: UUID | None = None,
) -> AgentPaperCycleResult:
    """Start one same-session cycle after its external facts are frozen."""
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    await require_candidate_model_profile(
        session,
        candidate_stage_capacity=candidate_stage_capacity,
        model_config_id=candidate_model_config_id,
        now=authority.decision_time,
    )
    if candidate_stage_capacity and (
        candidate_batch_id is None or version_evaluation_id is not None
    ):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    if (
        authority.execution_session != authority.decision_session
        or not idempotency_key
        or len(idempotency_key) > 255
    ):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    explicit_version = version_prompt_id is not None or version_model_id is not None
    if version_evaluation_id is None:
        if version_lane is not None or version_source_run_id is not None or explicit_version:
            raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
        if candidate_batch_id is None:
            raise ValueError("CANDIDATE_BATCH_REQUIRED")
        frozen_batch = await load_agent_paper_candidate_batch(session, batch_id=candidate_batch_id)
        if (
            frozen_batch is None
            or frozen_batch.account_id != authority.account_id
            or frozen_batch.universe_snapshot_id != authority.universe_snapshot_id
            or frozen_batch.instrument_ids != requested_instrument_ids
        ):
            raise ValueError("CANDIDATE_BATCH_INVALID")
    elif (
        version_lane not in {"incumbent", "challenger"}
        or (version_prompt_id is None) != (version_model_id is None)
        or (version_source_run_id is None) == (not explicit_version)
    ):
        raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
    account = await session.get(PaperAccountRecord, authority.account_id)
    if account is None or account.portfolio_id != authority.portfolio_id:
        raise AgentPaperCycleError("AGENT_PAPER_ACCOUNT_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-paper-cycle:{authority.account_id}"},
    )
    cycle = await session.scalar(
        select(AgentPaperCycleRecord).where(
            AgentPaperCycleRecord.account_id == authority.account_id,
            AgentPaperCycleRecord.decision_slot_at == decision_slot_at,
        )
    )
    if cycle is not None:
        await _require_cycle_candidate_profile(
            session,
            cycle=cycle,
            candidate_stage_capacity=candidate_stage_capacity,
            now=authority.decision_time,
            expected_model_id=candidate_model_config_id,
        )
        if (
            cycle.decision_price_policy_id != PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            or cycle.execution_price_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
            or cycle.candidate_batch_id != candidate_batch_id
            or (
                cycle.candidate_manifest
                and tuple(UUID(str(row["instrument_id"])) for row in cycle.candidate_manifest)
                != requested_instrument_ids
            )
        ):
            raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
        events = await _events(session, cycle.id)
        if not events:
            raise ValueError("AGENT_PAPER_EVENT_CHAIN_INVALID")
        return _result(cycle.id, events, replayed=True)
    return await _start_cycle(
        session,
        authority=authority,
        decision_slot_at=decision_slot_at,
        daily_limit_usd=daily_limit_usd,
        requested_instrument_ids=requested_instrument_ids,
        candidate_batch_id=candidate_batch_id,
        version_evaluation_id=version_evaluation_id,
        version_lane=version_lane,
        version_source_run_id=version_source_run_id,
        version_prompt_id=version_prompt_id,
        version_model_id=version_model_id,
        execution_scope_max_symbols=execution_scope_max_symbols,
        candidate_stage_capacity=candidate_stage_capacity,
        candidate_model_config_id=candidate_model_config_id,
    )


async def resume_schwab_agent_paper_cycle(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    idempotency_key: str,
    now: datetime | None = None,
    daily_limit_usd: Decimal | None = None,
    candidate_stage_capacity: bool = False,
) -> AgentPaperCycleResult:
    """Resume only from the persisted Job/Run/Output state of a Schwab cycle."""
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    if not idempotency_key or len(idempotency_key) > 255:
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if (
        cycle is None
        or cycle.decision_price_policy_id != PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        or cycle.execution_price_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        or cycle.execution_session != cycle.decision_session
    ):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    await _require_cycle_candidate_profile(
        session, cycle=cycle, candidate_stage_capacity=candidate_stage_capacity, now=now
    )
    request_hash = job_input_hash(
        {"schema": "agent-paper-schwab-resume-v1", "cycle_id": str(cycle_id)}
    )
    claim, _ = await claim_or_lock(
        session,
        f"local-agent-paper-schwab-resume:{cycle_id}",
        "agent-paper-schwab-resume-v1",
        idempotency_key,
        request_hash,
        "agent_paper_schwab_resume_v1",
    )
    if claim.request_hash != request_hash:
        raise ValueError("AGENT_PAPER_IDEMPOTENCY_CONFLICT")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-paper-cycle:{cycle.account_id}"},
    )
    if cycle.candidate_batch_id is not None and now is not None:
        from nexus.agents.portfolio_synthesis_worker import (
            read_synthesis_job,
            reconcile_synthesis_orphans,
        )
        from nexus.jobs.models import QueueName
        from nexus.jobs.repository import reclaim_expired_jobs

        synthesis_job = await read_synthesis_job(session, cycle_id=cycle.id)
        if synthesis_job is not None:
            exact_job_ids = (synthesis_job.id,)
            await reclaim_expired_jobs(
                session, queue=QueueName.GENERAL, now=now, job_ids=exact_job_ids
            )
            await reconcile_synthesis_orphans(session, now=now, job_ids=exact_job_ids)
    events = await _events(session, cycle.id)
    if not events:
        raise ValueError("AGENT_PAPER_EVENT_CHAIN_INVALID")
    if events[-1].to_status in {"completed", "unavailable", "failed", "awaiting_market"}:
        return _result(cycle.id, events, replayed=True)
    authority = await load_recorded_schwab_cycle_authority(session, cycle=cycle)
    manifest = parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=False)
    valid_manifest = await validate_candidate_manifest(
        session, authority=authority, manifest=manifest
    )
    if candidate_manifest_hash(manifest) != cycle.candidate_hash or not valid_manifest:
        raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
    if events[-1].to_status == "researching":
        return await _resume_research(
            session,
            cycle=cycle,
            authority=authority,
            events=events,
            now=now,
            daily_limit_usd=daily_limit_usd,
            candidate_stage_capacity=candidate_stage_capacity,
        )
    return await _resume_batch(
        session,
        cycle=cycle,
        events=events,
        now=now,
        candidate_stage_capacity=candidate_stage_capacity,
    )


async def advance_agent_paper_cycle(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_time: datetime,
    idempotency_key: str,
    daily_limit_usd: Decimal | None = None,
    candidate_batch_id: UUID | None = None,
    now: datetime | None = None,
    version_evaluation_id: UUID | None = None,
    version_lane: str | None = None,
    version_source_run_id: UUID | None = None,
    version_prompt_id: UUID | None = None,
    version_model_id: UUID | None = None,
    candidate_stage_capacity: bool = False,
    candidate_model_config_id: UUID | None = None,
) -> AgentPaperCycleResult:
    """Advance one persisted cycle without polling or executing model work."""
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    candidate_model = await require_candidate_model_profile(
        session,
        candidate_stage_capacity=candidate_stage_capacity,
        model_config_id=candidate_model_config_id,
        now=now if now is not None else decision_time,
    )
    if candidate_stage_capacity and (
        candidate_batch_id is None or version_evaluation_id is not None
    ):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    if (
        decision_time.tzinfo is None
        or decision_time.utcoffset() != UTC.utcoffset(decision_time)
        or not idempotency_key
        or len(idempotency_key) > 255
    ):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise AgentPaperCycleError("AGENT_PAPER_ACCOUNT_INVALID")
    explicit_version = version_prompt_id is not None or version_model_id is not None
    if version_evaluation_id is None:
        if version_lane is not None or version_source_run_id is not None or explicit_version:
            raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
    elif (
        version_lane not in {"incumbent", "challenger"}
        or (version_prompt_id is None) != (version_model_id is None)
        or (version_source_run_id is None) == (not explicit_version)
    ):
        raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
    request_payload: dict[str, object] = {
        "schema": "agent-paper-cycle-v1",
        "account_id": str(account_id),
        "decision_time": decision_time.isoformat(),
    }
    if candidate_batch_id is not None:
        batch = await load_agent_paper_candidate_batch(session, batch_id=candidate_batch_id)
        if batch is None or batch.account_id != account_id:
            raise ValueError("CANDIDATE_BATCH_INVALID")
        request_payload.update(
            candidate_batch_id=str(batch.batch_id), candidate_batch_hash=batch.batch_hash
        )
    if version_evaluation_id is not None:
        request_payload.update(
            {
                "version_evaluation_id": str(version_evaluation_id),
                "version_lane": version_lane,
                "version_source_run_id": (
                    str(version_source_run_id) if version_source_run_id is not None else None
                ),
                "version_prompt_id": (
                    str(version_prompt_id) if version_prompt_id is not None else None
                ),
                "version_model_id": (
                    str(version_model_id) if version_model_id is not None else None
                ),
            }
        )
    if candidate_stage_capacity:
        assert candidate_model is not None
        request_payload.update(
            candidate_stage_capacity=True,
            candidate_model_config_id=str(candidate_model_config_id),
            candidate_model_hash=candidate_model.config_hash,
        )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-paper-cycle:{account_id}"},
    )
    prior_cycle = await session.scalar(
        select(AgentPaperCycleRecord).where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_slot_at == floor_decision_slot(decision_time),
        )
    )
    if prior_cycle is not None:
        await _require_cycle_candidate_profile(
            session,
            cycle=prior_cycle,
            candidate_stage_capacity=candidate_stage_capacity,
            now=now,
            expected_model_id=candidate_model_config_id,
        )
    request_hash = job_input_hash(request_payload)
    claim, _ = await claim_or_lock(
        session,
        f"local-agent-paper-cycle:{account_id}",
        "agent-paper-cycle-v1",
        idempotency_key,
        request_hash,
        "agent_paper_cycle_v1",
    )
    if claim.request_hash != request_hash:
        raise ValueError("AGENT_PAPER_IDEMPOTENCY_CONFLICT")
    authority = await load_cycle_authority(
        session, account_id=account_id, decision_time=decision_time
    )
    decision_slot_at = floor_decision_slot(decision_time)
    cycle = await session.scalar(
        select(AgentPaperCycleRecord).where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_slot_at == decision_slot_at,
        )
    )
    if cycle is None:
        if version_evaluation_id is not None:
            evaluation = await session.get(AgentVersionEvaluationRecord, version_evaluation_id)
            if evaluation is None:
                raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
            requested_ids: tuple[UUID, ...] = (evaluation.instrument_id,)
        else:
            batch, _ = await validate_agent_paper_candidate_batch(
                session,
                batch_id=candidate_batch_id,
                account_id=account_id,
                as_of=decision_time,
                universe_snapshot_id=authority.universe_snapshot_id,
            )
            requested_ids = batch.instrument_ids
        return await _start_cycle(
            session,
            authority=authority,
            decision_slot_at=decision_slot_at,
            daily_limit_usd=Decimal("3.00") if daily_limit_usd is None else daily_limit_usd,
            requested_instrument_ids=requested_ids,
            candidate_batch_id=candidate_batch_id,
            version_evaluation_id=version_evaluation_id,
            version_lane=version_lane,
            version_source_run_id=version_source_run_id,
            version_prompt_id=version_prompt_id,
            version_model_id=version_model_id,
            candidate_stage_capacity=candidate_stage_capacity,
            candidate_model_config_id=candidate_model_config_id,
        )
    if cycle.decision_time != decision_time or (
        candidate_batch_id is not None and cycle.candidate_batch_id != candidate_batch_id
    ):
        raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
    allow_legacy = cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
    manifest = parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=allow_legacy)
    current_manifest = tuple(
        candidate for candidate in manifest if isinstance(candidate, CandidateManifestEntry)
    )
    valid_manifest = len(current_manifest) == len(manifest) and await validate_candidate_manifest(
        session, authority=authority, manifest=current_manifest
    )
    if allow_legacy and not current_manifest:
        valid_manifest = True
    if candidate_manifest_hash(manifest) != cycle.candidate_hash or not valid_manifest:
        raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
    events = await _events(session, cycle.id)
    if not events:
        raise ValueError("AGENT_PAPER_EVENT_CHAIN_INVALID")
    if events[-1].to_status in {"completed", "unavailable", "failed", "awaiting_market"}:
        return _result(cycle.id, events, replayed=True)
    if events[-1].to_status == "prepared":
        return await _admit_cycle_research(
            session,
            cycle=cycle,
            authority=authority,
            projection=await load_paper_projection(
                session,
                account_id=cycle.account_id,
                as_of=cycle.decision_time,
            ),
            daily_limit_usd=Decimal("3.00") if daily_limit_usd is None else daily_limit_usd,
            version_evaluation_id=version_evaluation_id,
            version_lane=version_lane,
            version_source_run_id=version_source_run_id,
            version_prompt_id=version_prompt_id,
            version_model_id=version_model_id,
            candidate_stage_capacity=candidate_stage_capacity,
        )
    if events[-1].to_status == "researching":
        return await _resume_research(
            session,
            cycle=cycle,
            authority=authority,
            events=events,
            now=now,
            daily_limit_usd=daily_limit_usd,
            candidate_stage_capacity=candidate_stage_capacity,
        )
    return await _resume_batch(
        session,
        cycle=cycle,
        events=events,
        now=now,
        candidate_stage_capacity=candidate_stage_capacity,
    )


async def _start_cycle(
    session: AsyncSession,
    *,
    authority: CycleAuthority,
    decision_slot_at: datetime,
    daily_limit_usd: Decimal,
    requested_instrument_ids: tuple[UUID, ...],
    candidate_batch_id: UUID | None = None,
    version_evaluation_id: UUID | None = None,
    version_lane: str | None = None,
    version_source_run_id: UUID | None = None,
    version_prompt_id: UUID | None = None,
    version_model_id: UUID | None = None,
    execution_scope_max_symbols: int | None = None,
    candidate_stage_capacity: bool = False,
    candidate_model_config_id: UUID | None = None,
) -> AgentPaperCycleResult:
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    candidate_model = await require_candidate_model_profile(
        session,
        candidate_stage_capacity=candidate_stage_capacity,
        model_config_id=candidate_model_config_id,
        now=authority.decision_time,
    )
    if candidate_stage_capacity and (
        candidate_batch_id is None or version_evaluation_id is not None
    ):
        raise ValueError("CODEX_CANDIDATE_CYCLE_PROFILE_INVALID")
    if version_evaluation_id is None:
        batch, _ = await validate_agent_paper_candidate_batch(
            session,
            batch_id=candidate_batch_id,
            account_id=authority.account_id,
            as_of=authority.decision_time,
            universe_snapshot_id=authority.universe_snapshot_id,
        )
        if batch.instrument_ids != requested_instrument_ids:
            raise ValueError("CANDIDATE_BATCH_INVALID")
    else:
        evaluation = await session.get(AgentVersionEvaluationRecord, version_evaluation_id)
        if (
            candidate_batch_id is not None
            or evaluation is None
            or requested_instrument_ids != (evaluation.instrument_id,)
            or authority.account_id
            != (
                evaluation.incumbent_account_id
                if version_lane == "incumbent"
                else evaluation.challenger_account_id
            )
        ):
            raise ValueError("AGENT_PAPER_VERSION_AUTHORITY_INVALID")
    projection = await load_paper_projection(
        session, account_id=authority.account_id, as_of=authority.decision_time
    )
    has_open_order = bool(projection.open_orders) and version_evaluation_id is not None
    manifest = (
        ()
        if has_open_order
        else await prepare_candidates(
            session,
            authority=authority,
            requested_instrument_ids=requested_instrument_ids,
        )
    )
    same_session = authority.execution_session == authority.decision_session
    cycle = AgentPaperCycleRecord(
        id=new_uuid7(),
        account_id=authority.account_id,
        portfolio_id=authority.portfolio_id,
        decision_session=authority.decision_session,
        decision_time=authority.decision_time,
        decision_slot_at=decision_slot_at,
        execution_session=authority.execution_session,
        universe_snapshot_id=authority.universe_snapshot_id,
        quality_report_id=authority.quality_report_id,
        decision_price_policy_id=(
            PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            if same_session
            else PAPER_DECISION_SOURCE_POLICY_ID
        ),
        execution_price_policy_id=(
            PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
            if same_session
            else PAPER_EXECUTION_SOURCE_POLICY_ID
        ),
        candidate_manifest=[row.model_dump(mode="json") for row in manifest],
        candidate_hash=candidate_manifest_hash(manifest),
        candidate_batch_id=candidate_batch_id,
        sizing_policy_version=(
            PORTFOLIO_ALLOCATION_V1.version
            if candidate_batch_id is not None
            else AGENT_PAPER_SIZING_V1.version
        ),
        sizing_policy_hash=(
            PORTFOLIO_ALLOCATION_POLICY_HASH
            if candidate_batch_id is not None
            else AGENT_PAPER_SIZING_POLICY_HASH
        ),
        execution_locked=True,
    )
    session.add(cycle)
    await session.flush()
    profile_facts = (
        {}
        if candidate_model is None
        else {
            "candidate_stage_capacity": True,
            "candidate_model_config_id": str(candidate_model.id),
            "candidate_model_hash": candidate_model.config_hash,
        }
    )
    await _append_event(session, cycle, None, "prepared", "CYCLE_PREPARED", profile_facts)
    if has_open_order or not manifest:
        await _append_event(
            session,
            cycle,
            "prepared",
            "unavailable",
            (
                "OPEN_ORDER_EXISTS"
                if has_open_order
                else "RISK_INPUT_REQUIRED"
                if same_session
                else "NO_ELIGIBLE_CANDIDATE"
            ),
            {},
        )
        return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    if same_session:
        try:
            execution_required = {
                *(candidate.instrument_id for candidate in manifest),
                *(position.instrument_id for position in projection.positions),
                *(order.instrument_id for order in projection.open_orders),
            }
            if candidate_batch_id is not None:
                from nexus.paper.agent_decision_store import load_portfolio_proposal

                current_target = await session.scalar(
                    select(PaperTargetVersionRecord)
                    .where(
                        PaperTargetVersionRecord.account_id == cycle.account_id,
                        PaperTargetVersionRecord.activated_at <= cycle.decision_time,
                        PaperTargetVersionRecord.created_at <= cycle.decision_time,
                    )
                    .order_by(PaperTargetVersionRecord.version_seq.desc())
                    .limit(1)
                )
                if current_target is not None:
                    current_proposal = await load_portfolio_proposal(
                        session,
                        account_id=cycle.account_id,
                        decision_id=current_target.strategy_decision_id,
                        candidate_stage_capacity=candidate_stage_capacity,
                    )
                    if current_proposal is None:
                        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
                    execution_required.update(
                        instrument_id
                        for instrument_id, weight in current_proposal.proposal.weights.items()
                        if weight > 0
                    )
            await prepare_schwab_realtime_risk_snapshot(
                session,
                cycle=cycle,
                required_instrument_ids=tuple(
                    sorted(execution_required, key=lambda value: value.int)
                ),
                max_instruments=(
                    len(execution_required)
                    if execution_scope_max_symbols is None
                    else execution_scope_max_symbols
                ),
            )
        except ValueError as error:
            code = str(error)
            if code == "AGENT_PAPER_PROVIDER_CAPACITY_INVALID":
                raise
            await _append_event(
                session,
                cycle,
                "prepared",
                "unavailable",
                (
                    code
                    if code == "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED"
                    else "RISK_INPUT_REQUIRED"
                ),
                {},
            )
            return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    return await _admit_cycle_research(
        session,
        cycle=cycle,
        authority=authority,
        projection=projection,
        daily_limit_usd=daily_limit_usd,
        version_evaluation_id=version_evaluation_id,
        version_lane=version_lane,
        version_source_run_id=version_source_run_id,
        version_prompt_id=version_prompt_id,
        version_model_id=version_model_id,
        candidate_stage_capacity=candidate_stage_capacity,
    )


async def _admit_cycle_research(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    authority: CycleAuthority,
    projection: PaperPortfolioProjection,
    daily_limit_usd: Decimal,
    version_evaluation_id: UUID | None = None,
    version_lane: str | None = None,
    version_source_run_id: UUID | None = None,
    version_prompt_id: UUID | None = None,
    version_model_id: UUID | None = None,
    candidate_stage_capacity: bool = False,
) -> AgentPaperCycleResult:
    candidate_model_id = await _require_cycle_candidate_profile(
        session,
        cycle=cycle,
        candidate_stage_capacity=candidate_stage_capacity,
        now=authority.decision_time,
    )
    manifest = parse_candidate_manifest(
        cycle.candidate_manifest,
        allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
    )
    run_ids: list[UUID] = []
    try:
        async with session.begin_nested():
            for candidate in manifest:
                research_snapshot_id = candidate.research_snapshot_id
                if authority.execution_session == authority.decision_session:
                    if not isinstance(candidate, CandidateManifestEntry):
                        raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
                    research_snapshot_id = (
                        await prepare_agent_paper_research_snapshot(
                            session,
                            cycle_id=cycle.id,
                            decision_time=authority.decision_time,
                            candidate=candidate,
                            projection=projection,
                        )
                    ).id
                if version_evaluation_id is None:
                    admission = await admit_agent_paper_research(
                        session,
                        cycle_id=cycle.id,
                        instrument_id=candidate.instrument_id,
                        research_snapshot_id=research_snapshot_id,
                        research_as_of=authority.decision_time,
                        daily_limit_usd=daily_limit_usd,
                        now=authority.decision_time,
                        model_config_id=candidate_model_id,
                        candidate_stage_capacity=candidate_stage_capacity,
                    )
                else:
                    admission = await admit_agent_paper_research(
                        session,
                        cycle_id=cycle.id,
                        instrument_id=candidate.instrument_id,
                        research_snapshot_id=research_snapshot_id,
                        research_as_of=authority.decision_time,
                        daily_limit_usd=daily_limit_usd,
                        now=authority.decision_time,
                        actor_id=(
                            AGENT_VERSION_INCUMBENT_ACTOR
                            if version_lane == "incumbent"
                            else AGENT_VERSION_CHALLENGER_ACTOR
                        ),
                        admission_idempotency_key=(
                            f"agent-version:{version_evaluation_id}:{version_lane}:"
                            f"{cycle.decision_slot_at.isoformat()}:"
                            f"{candidate.instrument_id}"
                        ),
                        version_source_run_id=version_source_run_id,
                        prompt_version_id=version_prompt_id,
                        model_config_id=version_model_id,
                    )
                run_ids.append(admission.run_id)
    except ValueError:
        await _append_event(
            session,
            cycle,
            "prepared",
            "unavailable",
            "RESEARCH_ADMISSION_UNAVAILABLE",
            {},
        )
        return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    await _append_event(
        session,
        cycle,
        "prepared",
        "researching",
        "RESEARCH_JOBS_PREPARED",
        {"research_run_ids": [str(value) for value in run_ids]},
    )
    return _result(cycle.id, await _events(session, cycle.id), replayed=False)


async def _resume_research(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    authority: CycleAuthority,
    events: tuple[AgentPaperCycleEventRecord, ...],
    now: datetime | None = None,
    daily_limit_usd: Decimal | None = None,
    candidate_stage_capacity: bool = False,
) -> AgentPaperCycleResult:
    await _require_cycle_candidate_profile(
        session, cycle=cycle, candidate_stage_capacity=candidate_stage_capacity, now=now
    )
    run_ids = _fact_uuids(events, "research_run_ids")
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type.in_(
                    (JobType.AGENT_RESEARCH.value, JobType.AGENT_PAPER_RESEARCH.value)
                ),
                Job.payload["run_id"].as_string().in_(tuple(str(value) for value in run_ids)),
            )
        )
    )
    if len(jobs) != len(run_ids) or any(
        job.status not in {"queued", "running", "succeeded"} for job in jobs
    ):
        return await _stop_unavailable(session, cycle, events)
    if any(job.status in {"queued", "running"} for job in jobs):
        return _result(cycle.id, events, replayed=True)
    if any(job.finished_at is None for job in jobs):
        return await _stop_unavailable(session, cycle, events)
    if cycle.candidate_batch_id is not None:
        return await _resume_portfolio_synthesis(
            session,
            cycle=cycle,
            events=events,
            run_ids=run_ids,
            now=now,
            daily_limit_usd=daily_limit_usd,
            candidate_stage_capacity=candidate_stage_capacity,
        )
    output_available_at = max(
        cycle.decision_time,
        *(job.finished_at for job in jobs if job.finished_at is not None),
    )
    try:
        actions = tuple(
            [
                await load_actionable_research_output(
                    session,
                    run_id=run_id,
                    decision_time=output_available_at,
                    allow_safe_no_trade=True,
                    candidate_stage_capacity=candidate_stage_capacity,
                )
                for run_id in run_ids
            ]
        )
    except ActionableResearchError:
        return await _stop_unavailable(session, cycle, events)
    try:
        projection = await load_paper_projection(
            session, account_id=cycle.account_id, as_of=cycle.decision_time
        )
        if cycle.decision_price_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID:
            contexts = []
            for run_id in run_ids:
                run = await session.get(AgentResearchRunRecord, run_id)
                context = (
                    None
                    if run is None
                    else await read_agent_paper_research_context(
                        session,
                        snapshot_id=run.data_snapshot_id,
                        instrument_id=run.instrument_id,
                        as_of=run.decision_time,
                    )
                )
                if context is None:
                    raise ValueError("AGENT_PAPER_RESEARCH_CONTEXT_INVALID")
                contexts.append(context[0])
            if not paper_contexts_match_projection(tuple(contexts), projection):
                raise ValueError("AGENT_PAPER_PROJECTION_CONFLICT")
        required = tuple(
            sorted(
                {
                    *(action.instrument_id for action in actions),
                    *(position.instrument_id for position in projection.positions),
                },
                key=lambda value: value.int,
            )
        )
        if cycle.decision_price_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID:
            market = await prepare_schwab_realtime_risk_snapshot(
                session,
                cycle=cycle,
                required_instrument_ids=required,
                max_instruments=len(required),
            )
        else:
            market = await prepare_risk_market_snapshot(
                session,
                authority=RiskAuthorityInput(
                    decision_session=cycle.decision_session,
                    execution_session=cycle.execution_session,
                    decision_time=cycle.decision_time,
                    source_policy_id=cycle.decision_price_policy_id,
                    calendar_authority_id=authority.calendar_authority_id,
                    universe_snapshot_id=cycle.universe_snapshot_id,
                    quality_report_id=cycle.quality_report_id,
                ),
                required_instrument_ids=required,
            )
        decision = build_agent_paper_decision(
            cycle=cycle, actions=actions, projection=projection, market=market
        )
    except (ValueError, AgentPaperDecisionError):
        return await _stop_unavailable(session, cycle, events)
    try:
        persisted = await persist_agent_paper_decision(session, decision)
    except ValueError:
        return await _stop_failed(session, cycle, events, "AGENT_DECISION_CONFLICT")
    await _append_event(
        session,
        cycle,
        "researching",
        "decision_recorded",
        "AGENT_DECISION_RECORDED",
        {
            "strategy_decision_id": str(persisted.decision_id),
            "research_output_ids": [str(action.output_id) for action in actions],
        },
    )
    return await _resume_batch(
        session,
        cycle=cycle,
        events=await _events(session, cycle.id),
        now=now,
        candidate_stage_capacity=candidate_stage_capacity,
    )


async def _resume_batch(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    now: datetime | None = None,
    candidate_stage_capacity: bool = False,
) -> AgentPaperCycleResult:
    await _require_cycle_candidate_profile(
        session, cycle=cycle, candidate_stage_capacity=candidate_stage_capacity, now=now
    )
    if events[-1].to_status in {"completed", "unavailable", "failed", "awaiting_market"}:
        return _result(cycle.id, events, replayed=True)
    decision_id = _fact_uuid(events, "strategy_decision_id") or await session.scalar(
        select(StrategyDecisionRecord.id).where(
            StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
            StrategyDecisionRecord.decision_origin == "agent_research",
        )
    )
    if decision_id is None:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
    if cycle.candidate_batch_id is not None:
        if events[-1].to_status != "decision_recorded" or now is None:
            return _result(cycle.id, events, replayed=True).model_copy(
                update={"result_code": "TARGET_ACTIVATION_PENDING"}
            )
        from nexus.paper.target_service import (
            TargetActivationBlockedError,
            activate_portfolio_target,
        )

        try:
            activation = await activate_portfolio_target(
                session,
                account_id=cycle.account_id,
                strategy_decision_id=decision_id,
                request_time=now,
                idempotency_key=f"portfolio-target:{cycle.id}",
                candidate_stage_capacity=candidate_stage_capacity,
            )
        except TargetActivationBlockedError as error:
            facts: dict[str, object] = {
                "strategy_decision_id": str(decision_id),
                "blocking_plan_id": str(error.blocking_plan_id),
            }
            await _append_event(
                session,
                cycle,
                "decision_recorded",
                "completed",
                "TARGET_ACTIVATION_BLOCKED",
                facts,
            )
            return _result(cycle.id, await _events(session, cycle.id), replayed=False)
        except ValueError as error:
            if str(error) == "PORTFOLIO_PROPOSAL_BASELINE_CONFLICT":
                raise ValueError("AGENT_PAPER_RESULT_UNKNOWN") from error
            raise
        if activation.batch_id is None:
            await _append_event(
                session,
                cycle,
                "decision_recorded",
                "completed",
                "NO_ORDER_REQUIRED",
                {
                    "strategy_decision_id": str(decision_id),
                    "target_version_id": str(activation.target_version_id),
                },
            )
            return _result(cycle.id, await _events(session, cycle.id), replayed=False)
        stored_batch = await session.get(PaperOrderBatchRecord, activation.batch_id)
        if stored_batch is None:
            raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
        order_ids = tuple(
            await session.scalars(
                select(PaperOrderRecord.id)
                .where(PaperOrderRecord.batch_id == stored_batch.id)
                .order_by(PaperOrderRecord.id)
            )
        )
        await _append_event(
            session,
            cycle,
            "decision_recorded",
            "risk_evaluated",
            "PAPER_BATCH_EVALUATED",
            {
                "strategy_decision_id": str(decision_id),
                "target_version_id": str(activation.target_version_id),
                "batch_id": str(stored_batch.id),
                "order_ids": [str(value) for value in order_ids],
            },
        )
        events = await _events(session, cycle.id)
    if events[-1].to_status == "decision_recorded":
        try:
            batch = await create_paper_order_batch(
                session,
                account_id=cycle.account_id,
                strategy_decision_id=decision_id,
                idempotency_key=f"agent-paper-batch:{cycle.id}",
                request_time=cycle.decision_time,
            )
        except (PaperBatchError, ValueError):
            return await _stop_failed(session, cycle, events, "PAPER_BATCH_CONFLICT")
        await _append_event(
            session,
            cycle,
            "decision_recorded",
            "risk_evaluated",
            "PAPER_BATCH_EVALUATED",
            {
                "strategy_decision_id": str(decision_id),
                "batch_id": str(batch.batch_id),
                "order_ids": [str(value) for value in batch.order_ids],
            },
        )
        events = await _events(session, cycle.id)
    if events[-1].to_status == "risk_evaluated":
        order_ids = _fact_uuids(events, "order_ids")
        status: CycleStatus = "awaiting_market" if order_ids else "completed"
        code = "PAPER_ORDERS_AWAITING_MARKET" if order_ids else "NO_ORDER_REQUIRED"
        await _append_event(
            session,
            cycle,
            "risk_evaluated",
            status,
            code,
            {
                "strategy_decision_id": str(decision_id),
                "order_ids": [str(value) for value in order_ids],
            },
        )
        events = await _events(session, cycle.id)
    return _result(cycle.id, events, replayed=False)


async def _resume_portfolio_synthesis(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    run_ids: tuple[UUID, ...],
    now: datetime | None,
    daily_limit_usd: Decimal | None,
    candidate_stage_capacity: bool = False,
) -> AgentPaperCycleResult:
    await _require_cycle_candidate_profile(
        session, cycle=cycle, candidate_stage_capacity=candidate_stage_capacity, now=now
    )
    if events[-1].to_status != "researching":
        return _result(cycle.id, events, replayed=True)
    from nexus.agents.portfolio_synthesis_contracts import (
        PortfolioSynthesisIntentV1,
        canonical_portfolio_synthesis_hash,
        validate_portfolio_synthesis_intent,
    )
    from nexus.agents.portfolio_synthesis_persistence import _frozen_run, admit_portfolio_synthesis
    from nexus.agents.portfolio_synthesis_worker import read_synthesis_job
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisOutputRecord,
        AgentPortfolioSynthesisRunRecord,
    )
    from nexus.persistence.models.paper import PaperTargetVersionRecord

    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
        )
    )
    if run is None and (now is None or daily_limit_usd is None):
        raise ValueError("PORTFOLIO_SYNTHESIS_ADMISSION_CONTEXT_REQUIRED")
    try:
        if run is None:
            assert now is not None and daily_limit_usd is not None
            actions = tuple(
                [
                    await load_actionable_research_output(
                        session,
                        run_id=run_id,
                        decision_time=now,
                        allow_safe_no_trade=True,
                        candidate_stage_capacity=candidate_stage_capacity,
                    )
                    for run_id in run_ids
                ]
            )
            projection = await load_paper_projection(
                session, account_id=cycle.account_id, as_of=cycle.decision_time
            )
            target_id = await session.scalar(
                select(PaperTargetVersionRecord.id)
                .where(PaperTargetVersionRecord.account_id == cycle.account_id)
                .order_by(PaperTargetVersionRecord.version_seq.desc())
                .limit(1)
            )
            run = (
                await admit_portfolio_synthesis(
                    session,
                    cycle_id=cycle.id,
                    research_output_ids=tuple(action.output_id for action in actions),
                    active_target_version_id=target_id,
                    projection_hash=projection.projection_hash,
                    daily_limit_usd=daily_limit_usd,
                    now=now,
                    candidate_stage_capacity=candidate_stage_capacity,
                )
            ).run
        frozen = _frozen_run(run)
        job = await read_synthesis_job(session, cycle_id=cycle.id)
        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
        if job is None:
            raise ValueError("PORTFOLIO_SYNTHESIS_JOB_INVALID")
        if (
            output is None
            and job.status in {"queued", "running"}
            and run.status in {"prepared", "running"}
        ):
            return _result(cycle.id, events, replayed=True).model_copy(
                update={"result_code": "PORTFOLIO_SYNTHESIS_PENDING"}
            )
        if (
            output is None
            or output.status != "completed"
            or run.status != "completed"
            or job.status != "succeeded"
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_UNAVAILABLE")
        intent = PortfolioSynthesisIntentV1.model_validate(output.intent)
        validate_portfolio_synthesis_intent(intent, frozen)
        content_hash = canonical_portfolio_synthesis_hash(
            {
                "schema_version": "portfolio_synthesis_terminal_v1",
                "status": "completed",
                "intent": intent.model_dump(mode="json"),
            }
        )
        if (
            output.content_hash != content_hash
            or output.binding_hash
            != canonical_portfolio_synthesis_hash(
                {
                    "schema_version": "portfolio_synthesis_binding_v1",
                    "run_id": run.id,
                    "input_hash": run.input_hash,
                    "content_hash": content_hash,
                }
            )
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_OUTPUT_INVALID")
    except ValueError:
        await _append_event(
            session, cycle, "researching", "unavailable", "PORTFOLIO_SYNTHESIS_UNAVAILABLE", {}
        )
        return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    try:
        if now is None:
            raise ValueError("PORTFOLIO_PROPOSAL_TIME_REQUIRED")
        persisted = await persist_portfolio_proposal(
            session,
            cycle_id=cycle.id,
            synthesis_output_id=output.id,
            now=now,
            candidate_stage_capacity=candidate_stage_capacity,
        )
    except ValueError as error:
        if str(error) == "POLICY_UNCONFIRMED":
            # Missing initial authority is a durable-input wait, not a terminal
            # research failure. Preserve the completed synthesis for exact resume.
            return _result(cycle.id, events, replayed=True).model_copy(
                update={
                    "result_code": "POLICY_UNCONFIRMED",
                    "portfolio_synthesis_output_id": output.id,
                }
            )
        code = "PORTFOLIO_PROPOSAL_UNAVAILABLE"
        facts: dict[str, object] = {"portfolio_synthesis_output_id": str(output.id)}
        if str(error) in KNOWN_PORTFOLIO_PROPOSAL_REJECTIONS:
            facts["proposal_rejection_code"] = str(error)
        await _append_event(
            session,
            cycle,
            "researching",
            "unavailable",
            code,
            facts,
        )
        return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    await _append_event(
        session,
        cycle,
        "researching",
        "decision_recorded",
        "TARGET_ACTIVATION_PENDING",
        {
            "strategy_decision_id": str(persisted.decision_id),
            "portfolio_synthesis_output_id": str(output.id),
        },
    )
    return await _resume_batch(
        session,
        cycle=cycle,
        events=await _events(session, cycle.id),
        now=now,
        candidate_stage_capacity=candidate_stage_capacity,
    )


async def _stop_unavailable(
    session: AsyncSession,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
) -> AgentPaperCycleResult:
    await _append_event(
        session,
        cycle,
        cast(CycleStatus, events[-1].to_status),
        "unavailable",
        "RESEARCH_OUTPUT_NOT_ACTIONABLE",
        {},
    )
    return _result(cycle.id, await _events(session, cycle.id), replayed=False)


async def _stop_failed(
    session: AsyncSession,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    code: str,
) -> AgentPaperCycleResult:
    await _append_event(
        session,
        cycle,
        cast(CycleStatus, events[-1].to_status),
        "failed",
        code,
        {},
    )
    return _result(cycle.id, await _events(session, cycle.id), replayed=False)


async def _append_event(
    session: AsyncSession,
    cycle: AgentPaperCycleRecord,
    from_status: CycleStatus | None,
    to_status: CycleStatus,
    result_code: str,
    fact_ids: dict[str, object],
) -> None:
    sequence = (
        int(
            await session.scalar(
                select(func.coalesce(func.max(AgentPaperCycleEventRecord.sequence), 0)).where(
                    AgentPaperCycleEventRecord.cycle_id == cycle.id
                )
            )
            or 0
        )
        + 1
    )
    event = AgentPaperCycleEventRecord(
        cycle_id=cycle.id,
        sequence=sequence,
        from_status=from_status,
        to_status=to_status,
        result_code=result_code,
        fact_ids=fact_ids,
        event_hash=canonical_cycle_event_hash(
            cycle_id=cycle.id,
            sequence=sequence,
            from_status=from_status,
            to_status=to_status,
            result_code=result_code,
            fact_ids=fact_ids,
            occurred_at=cycle.decision_time,
        ),
        occurred_at=cycle.decision_time,
    )
    session.add(event)
    await session.flush()


async def _events(session: AsyncSession, cycle_id: UUID) -> tuple[AgentPaperCycleEventRecord, ...]:
    return tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )


def _fact_uuids(events: tuple[AgentPaperCycleEventRecord, ...], key: str) -> tuple[UUID, ...]:
    for event in reversed(events):
        raw = event.fact_ids.get(key)
        if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
            return tuple(UUID(value) for value in raw)
    return ()


def _fact_uuid(events: tuple[AgentPaperCycleEventRecord, ...], key: str) -> UUID | None:
    for event in reversed(events):
        raw = event.fact_ids.get(key)
        if isinstance(raw, str):
            return UUID(raw)
    return None


def _result(
    cycle_id: UUID,
    events: tuple[AgentPaperCycleEventRecord, ...],
    *,
    replayed: bool,
) -> AgentPaperCycleResult:
    latest = events[-1]
    return AgentPaperCycleResult(
        cycle_id=cycle_id,
        status=cast(CycleStatus, latest.to_status),
        result_code=latest.result_code,
        research_run_ids=_fact_uuids(events, "research_run_ids"),
        strategy_decision_id=_fact_uuid(events, "strategy_decision_id"),
        portfolio_synthesis_output_id=_fact_uuid(events, "portfolio_synthesis_output_id"),
        batch_id=_fact_uuid(events, "batch_id"),
        order_ids=_fact_uuids(events, "order_ids"),
        replayed=replayed,
    )
