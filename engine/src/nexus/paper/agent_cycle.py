"""Resumable, append-only coordination for one internal Agent paper cycle."""

from datetime import UTC, datetime, time
from decimal import Decimal
from hashlib import sha256
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import ActionableResearchError
from nexus.agents.persistence import load_actionable_research_output
from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_research import admit_agent_paper_research
from nexus.jobs.models import job_input_hash
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
)
from nexus.paper.agent_decision import (
    AGENT_PAPER_SIZING_POLICY_HASH,
    AGENT_PAPER_SIZING_V1,
    AgentPaperDecisionError,
    build_agent_paper_decision,
)
from nexus.paper.agent_decision_store import persist_agent_paper_decision
from nexus.paper.errors import PaperBatchError
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.risk.authority import (
    RiskAuthorityInput,
    prepare_risk_market_snapshot,
    prepare_schwab_realtime_risk_snapshot,
)

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
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
    }
)


async def reserve_schwab_agent_paper_trigger(
    session: AsyncSession,
    *,
    account_id: UUID,
    client_idempotency_key: str,
    now: datetime,
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
    if not time(7) <= local_time.time().replace(tzinfo=None) < time(16):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise AgentPaperCycleError("AGENT_PAPER_ACCOUNT_INVALID")
    decision_slot_at = floor_decision_slot(now)
    request_hash = job_input_hash(
        {
            "schema": "agent-paper-schwab-trigger-v2",
            "account_id": str(account_id),
            "decision_session": local_time.date().isoformat(),
            "decision_slot_at": decision_slot_at.isoformat(),
            "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
            "client_idempotency_hash": sha256(client_idempotency_key.encode()).hexdigest(),
        }
    )
    claim, inserted = await claim_or_lock(
        session,
        f"local-agent-paper-schwab:{account_id}",
        "agent-paper-schwab-trigger-v2",
        decision_slot_at.isoformat(),
        request_hash,
        "agent_paper_schwab_trigger_v2",
    )
    if claim.request_hash != request_hash:
        raise ValueError("AGENT_PAPER_IDEMPOTENCY_CONFLICT")
    reservation = AgentPaperTriggerReservation(
        claim_id=claim.id,
        decision_session=local_time.date(),
        decision_slot_at=decision_slot_at,
    )
    if inserted:
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
    if (
        claim is None
        or claim.route != "agent-paper-schwab-trigger-v2"
        or claim.schema_version != "agent_paper_schwab_trigger_v2"
        or claim.idempotency_key != reservation.decision_slot_at.isoformat()
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


async def start_schwab_agent_paper_cycle(
    session: AsyncSession,
    *,
    authority: CycleAuthority,
    decision_slot_at: datetime,
    idempotency_key: str,
    daily_limit_usd: Decimal = Decimal("3.00"),
) -> AgentPaperCycleResult:
    """Start one same-session cycle after its external facts are frozen."""
    if (
        authority.execution_session != authority.decision_session
        or not idempotency_key
        or len(idempotency_key) > 255
    ):
        raise ValueError("AGENT_PAPER_REQUEST_INVALID")
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
        if (
            cycle.decision_price_policy_id != PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            or cycle.execution_price_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
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
    )


async def resume_schwab_agent_paper_cycle(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    idempotency_key: str,
) -> AgentPaperCycleResult:
    """Resume only from the persisted Job/Run/Output state of a Schwab cycle."""
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
    events = await _events(session, cycle.id)
    if not events:
        raise ValueError("AGENT_PAPER_EVENT_CHAIN_INVALID")
    if events[-1].to_status in {"completed", "unavailable", "failed", "awaiting_market"}:
        return _result(cycle.id, events, replayed=True)
    authority = await load_recorded_schwab_cycle_authority(session, cycle=cycle)
    manifest = tuple(CandidateManifestEntry.model_validate(row) for row in cycle.candidate_manifest)
    valid_manifest = await validate_candidate_manifest(
        session, authority=authority, manifest=manifest
    )
    if candidate_manifest_hash(manifest) != cycle.candidate_hash or not valid_manifest:
        raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
    if events[-1].to_status == "researching":
        return await _resume_research(session, cycle=cycle, authority=authority, events=events)
    return await _resume_batch(session, cycle=cycle, events=events)


async def advance_agent_paper_cycle(
    session: AsyncSession,
    *,
    account_id: UUID,
    decision_time: datetime,
    idempotency_key: str,
    daily_limit_usd: Decimal = Decimal("3.00"),
) -> AgentPaperCycleResult:
    """Advance one persisted cycle without polling or executing model work."""
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
    request_hash = job_input_hash(
        {
            "schema": "agent-paper-cycle-v1",
            "account_id": str(account_id),
            "decision_time": decision_time.isoformat(),
        }
    )
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
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-paper-cycle:{account_id}"},
    )
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
        return await _start_cycle(
            session,
            authority=authority,
            decision_slot_at=decision_slot_at,
            daily_limit_usd=daily_limit_usd,
        )
    if cycle.decision_time != decision_time:
        raise ValueError("AGENT_PAPER_CANDIDATE_CONFLICT")
    manifest = tuple(CandidateManifestEntry.model_validate(row) for row in cycle.candidate_manifest)
    valid_manifest = await validate_candidate_manifest(
        session, authority=authority, manifest=manifest
    )
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
            daily_limit_usd=daily_limit_usd,
        )
    if events[-1].to_status == "researching":
        return await _resume_research(session, cycle=cycle, authority=authority, events=events)
    return await _resume_batch(session, cycle=cycle, events=events)


async def _start_cycle(
    session: AsyncSession,
    *,
    authority: CycleAuthority,
    decision_slot_at: datetime,
    daily_limit_usd: Decimal,
) -> AgentPaperCycleResult:
    projection = await load_paper_projection(
        session, account_id=authority.account_id, as_of=authority.decision_time
    )
    has_open_order = bool(projection.open_orders)
    manifest = () if has_open_order else await prepare_candidates(session, authority=authority)
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
        sizing_policy_version=AGENT_PAPER_SIZING_V1.version,
        sizing_policy_hash=AGENT_PAPER_SIZING_POLICY_HASH,
        execution_locked=True,
    )
    session.add(cycle)
    await session.flush()
    await _append_event(session, cycle, None, "prepared", "CYCLE_PREPARED", {})
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
            await prepare_schwab_realtime_risk_snapshot(session, cycle=cycle)
        except ValueError:
            await _append_event(
                session,
                cycle,
                "prepared",
                "unavailable",
                "RISK_INPUT_REQUIRED",
                {},
            )
            return _result(cycle.id, await _events(session, cycle.id), replayed=False)
    return await _admit_cycle_research(
        session,
        cycle=cycle,
        authority=authority,
        daily_limit_usd=daily_limit_usd,
    )


async def _admit_cycle_research(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    authority: CycleAuthority,
    daily_limit_usd: Decimal,
) -> AgentPaperCycleResult:
    manifest = tuple(CandidateManifestEntry.model_validate(row) for row in cycle.candidate_manifest)
    run_ids: list[UUID] = []
    try:
        async with session.begin_nested():
            for candidate in manifest:
                admission = await admit_agent_paper_research(
                    session,
                    cycle_id=cycle.id,
                    instrument_id=candidate.instrument_id,
                    research_snapshot_id=candidate.research_snapshot_id,
                    research_as_of=authority.decision_time,
                    daily_limit_usd=daily_limit_usd,
                    now=authority.decision_time,
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
) -> AgentPaperCycleResult:
    run_ids = _fact_uuids(events, "research_run_ids")
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type == "agent.research",
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
    output_available_at = max(
        cycle.decision_time,
        *(job.finished_at for job in jobs if job.finished_at is not None),
    )
    try:
        actions = tuple(
            [
                await load_actionable_research_output(
                    session, run_id=run_id, decision_time=output_available_at
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
            market = await prepare_schwab_realtime_risk_snapshot(session, cycle=cycle)
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
    return await _resume_batch(session, cycle=cycle, events=await _events(session, cycle.id))


async def _resume_batch(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
) -> AgentPaperCycleResult:
    decision_id = _fact_uuid(events, "strategy_decision_id") or await session.scalar(
        select(StrategyDecisionRecord.id).where(
            StrategyDecisionRecord.account_id == cycle.account_id,
            StrategyDecisionRecord.decision_session == cycle.decision_session,
            StrategyDecisionRecord.decision_origin == "agent_research",
        )
    )
    if decision_id is None:
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")
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
            {"order_ids": [str(value) for value in order_ids]},
        )
        events = await _events(session, cycle.id)
    return _result(cycle.id, events, replayed=False)


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
        batch_id=_fact_uuid(events, "batch_id"),
        order_ids=_fact_uuids(events, "order_ids"),
        replayed=replayed,
    )
