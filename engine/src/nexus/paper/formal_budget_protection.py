"""Budget-day protection. The normal admission paths must use database authority."""

import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import TYPE_CHECKING, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.audit.service import append_audit_event
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.formal_budget_inventory import Reservation, reservation_usage
from nexus.persistence.models.agent_versions import (
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
    FormalBudgetBindingRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord

SCHEMA = "formal_budget_binding_v1"
DOMAIN = "agent_global_v1"

if TYPE_CHECKING:
    from nexus.paper.economic_budget_hold import AllocationRequest


def decision_within_slot(slot: datetime, decision_time: datetime) -> bool:
    """Collection freezes its real cutoff inside the original legal 10 ET slot."""
    return (
        slot.utcoffset() == timedelta(0)
        and decision_time.utcoffset() == timedelta(0)
        and slot <= decision_time < slot + timedelta(minutes=5)
    )


class FrozenTask(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    key: str
    kind: str
    instrument_id: UUID | None
    maximum_usd: Decimal


class ProtectedDemand(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    binding_id: UUID
    daily_limit_usd: Decimal
    native_usd: Decimal
    hold_usd: Decimal
    future_usd: Decimal
    future_tasks: tuple[FrozenTask, ...]


def _hash(payload: dict[str, object]) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


async def _native_usage(
    session: AsyncSession,
    row: AgentDailyBudgetReservationRecord
    | AgentPortfolioSynthesisBudgetReservationRecord
    | PortfolioDecisionBudgetReservationRecord,
) -> Decimal:
    usage, known = reservation_usage(row)
    run_type = (
        AgentResearchRunRecord
        if isinstance(row, AgentDailyBudgetReservationRecord)
        else AgentPortfolioSynthesisRunRecord
        if isinstance(row, AgentPortfolioSynthesisBudgetReservationRecord)
        else PortfolioDecisionRunRecord
    )
    run = cast(
        AgentResearchRunRecord
        | AgentPortfolioSynthesisRunRecord
        | PortfolioDecisionRunRecord
        | None,
        await session.get(run_type, row.run_id),
    )
    if (
        not known
        or run is None
        or row.pricing_version != run.pricing_version
        or (row.state != "reserved" and run.status not in {"completed", "failed", "abstained"})
    ):
        raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
    if isinstance(row, AgentDailyBudgetReservationRecord) and row.state == "reserved":
        from nexus.agents.research_usage import research_reserved_occupancy

        return await research_reserved_occupancy(session, row)
    return usage


async def latest_binding(
    session: AsyncSession, *, business_date: date
) -> FormalBudgetBindingRecord | None:
    return cast(
        FormalBudgetBindingRecord | None,
        await session.scalar(
            select(FormalBudgetBindingRecord)
            .where(
                FormalBudgetBindingRecord.budget_domain == DOMAIN,
                FormalBudgetBindingRecord.business_date == business_date,
            )
            .order_by(FormalBudgetBindingRecord.sequence.desc())
            .limit(1)
        ),
    )


async def _validate_binding(session: AsyncSession, binding: FormalBudgetBindingRecord) -> None:
    audit = await session.get(AuditEvent, binding.audit_event_id)
    if (
        binding.schema_version != SCHEMA
        or binding.content_hash != _hash(binding.payload)
        or audit is None
        or audit.action != "formal_budget_binding_apply"
        or audit.payload
        != {"binding_hash": binding.content_hash, "binding_payload": binding.payload}
        or audit.occurred_at != binding.applied_at
    ):
        raise ValueError("FORMAL_BUDGET_BINDING_INVALID")


async def apply_protection(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    enabled: bool,
    daily_limit_usd: Decimal,
    configuration_hash: str,
    idempotency_key: str,
    now: datetime | None = None,
    formal_model_config_id: UUID | None = None,
) -> FormalBudgetBindingRecord:
    """Internal authority builder; the product CLI owns protected config and DB clock."""
    from nexus.agents.persistence import _lock_budget_day, load_complete_prompt_authority

    if now is None:
        now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
        raise ValueError("FORMAL_BUDGET_CLOCK_INVALID")
    if not daily_limit_usd.is_finite() or daily_limit_usd < 0 or not idempotency_key:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    await _lock_budget_day(session, now.date().isoformat())
    prior = await latest_binding(session, business_date=now.date())
    existing = await session.scalar(
        select(FormalBudgetBindingRecord).where(
            FormalBudgetBindingRecord.idempotency_key == idempotency_key,
        )
    )
    if existing is not None:
        await _validate_binding(session, existing)
        p = existing.payload
        if (
            existing.business_date != now.date()
            or p["account_id"] != str(account_id)
            or p["batch_id"] != str(batch_id)
            or p["enabled"] != enabled
            or p["configuration_hash"] != configuration_hash
            or (formal_model_config_id is not None and p["model_id"] != str(formal_model_config_id))
            or existing.daily_limit_usd != daily_limit_usd
        ):
            raise ValueError("FORMAL_BUDGET_APPLY_CONFLICT")
        return existing
    if prior is not None:
        await _validate_binding(session, prior)
        previous_claim = await session.scalar(
            select(IdempotencyRecord.id)
            .where(
                IdempotencyRecord.formal_budget_binding_id == prior.id,
            )
            .limit(1)
        )
        if previous_claim is not None:
            raise ValueError("FORMAL_BUDGET_SLOT_ALREADY_CLAIMED")
    if (
        await session.scalar(
            select(AgentVersionEvaluationRecord.id)
            .outerjoin(
                AgentVersionEvaluationDecisionRecord,
                AgentVersionEvaluationDecisionRecord.evaluation_id
                == AgentVersionEvaluationRecord.id,
            )
            .where(AgentVersionEvaluationDecisionRecord.id.is_(None))
            .limit(1)
        )
        is not None
    ):
        raise ValueError("FORMAL_REGISTERED_EVALUATION_UNSUPPORTED")
    batch, _ = await validate_agent_paper_candidate_batch(
        session,
        account_id=account_id,
        batch_id=batch_id,
        as_of=now,
    )
    # New York 10:00 always falls on the same UTC date. At UTC midnight,
    # New York is still on yesterday; that session cannot fund today's tasks.
    decision_session = now.date()
    slot = datetime.combine(decision_session, time(10), ZoneInfo("America/New_York")).astimezone(
        UTC
    )
    budget_day = now.date()

    async def require_apply_clock() -> datetime:
        fresh = await session.scalar(select(func.clock_timestamp()))
        if (
            not isinstance(fresh, datetime)
            or fresh.utcoffset() != timedelta(0)
            or fresh.date() != budget_day
            or fresh >= slot
        ):
            raise ValueError("FORMAL_BUDGET_APPLY_WINDOW_PASSED")
        return fresh

    if slot.date() != now.date() or now >= slot:
        raise ValueError("FORMAL_BUDGET_APPLY_WINDOW_PASSED")
    now = await require_apply_clock()
    if (
        await session.scalar(
            select(IdempotencyRecord.id)
            .where(
                IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}",
                IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                IdempotencyRecord.idempotency_key == slot.isoformat(),
            )
            .limit(1)
        )
        is not None
        or await session.scalar(
            select(AgentPaperCycleRecord.id)
            .where(
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_slot_at == slot,
            )
            .limit(1)
        )
        is not None
    ):
        raise ValueError("FORMAL_BUDGET_SLOT_ALREADY_CLAIMED")
    calendar = (
        await session.execute(
            select(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarSessionRecord,
            )
            .join(
                ExchangeCalendarSessionRecord,
                ExchangeCalendarSessionRecord.calendar_authority_id
                == ExchangeCalendarAuthorityRecord.id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= now,
                ExchangeCalendarAuthorityRecord.created_at <= now,
                ExchangeCalendarSessionRecord.created_at <= now,
                ExchangeCalendarSessionRecord.session == decision_session,
                ExchangeCalendarSessionRecord.market_open_at <= slot,
                ExchangeCalendarSessionRecord.market_close_at > slot,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    prompts = await load_complete_prompt_authority(session)
    model: AgentModelConfigRecord | None
    if formal_model_config_id is not None:
        from nexus.agents.codex_formal_stage import require_formal_stage_model_profile

        model = await require_formal_stage_model_profile(
            session, model_config_id=formal_model_config_id, now=now
        )
        if model.pricing_expires_at <= slot:
            raise ValueError("CODEX_FORMAL_AUTHORITY_INVALID")
    else:
        model = await session.scalar(
            select(AgentModelConfigRecord)
            .where(
                AgentModelConfigRecord.version.in_(ACTIONABLE_CONFIG_VERSIONS),
                AgentModelConfigRecord.created_at <= now,
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > slot,
            )
            .order_by(AgentModelConfigRecord.created_at.desc(), AgentModelConfigRecord.id.desc())
            .limit(1)
        )
    synthesis = await session.scalar(
        select(AgentPromptVersionRecord).where(
            AgentPromptVersionRecord.role == "portfolio_decision",
            AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
            AgentPromptVersionRecord.release_channel == "active",
            AgentPromptVersionRecord.created_at <= now,
        )
    )
    from nexus.agents.execution_authority import load_frozen_prompts
    from nexus.paper.agent_version_evaluation import load_selected_internal_agent_version

    selected_version = await load_selected_internal_agent_version(session, now=now)
    if selected_version is not None:
        source = await session.get(AgentResearchRunRecord, selected_version.source_run_id)
        if source is None:
            raise ValueError("FORMAL_BUDGET_SOURCE_UNAVAILABLE")
        frozen_prompts = await load_frozen_prompts(session, source)
        prompts = None if frozen_prompts is None else dict(frozen_prompts)
        model = await session.get(AgentModelConfigRecord, source.model_config_id)
        if (
            model is None
            or (formal_model_config_id is not None and model.id != formal_model_config_id)
            or model.config_hash != source.model_hash
            or model.version != source.pricing_version
            or model.created_at > now
            or model.pricing_available_at > now
            or model.pricing_expires_at <= slot
        ):
            raise ValueError("FORMAL_BUDGET_SOURCE_UNAVAILABLE")
    if (
        calendar is None
        or prompts is None
        or model is None
        or synthesis is None
        or any(prompt.created_at > now for prompt in prompts.values())
        or sha256(synthesis.content.encode()).hexdigest() != synthesis.content_hash
    ):
        raise ValueError("FORMAL_BUDGET_SOURCE_UNAVAILABLE")

    def prompt_identity(row: AgentPromptVersionRecord) -> dict[str, str]:
        return {"id": str(row.id), "version": row.version, "content_hash": row.content_hash}

    tasks = tuple(
        FrozenTask(
            key=_hash(
                {
                    "account": str(account_id),
                    "slot": slot.isoformat(),
                    "kind": kind,
                    "instrument": None if instrument is None else str(instrument),
                }
            ),
            kind=kind,
            instrument_id=instrument,
            maximum_usd=model.worst_case_cost_usd,
        )
        for kind, instrument in (
            *(("research", instrument) for instrument in batch.instrument_ids),
            ("synthesis", None),
        )
    )
    runtime_spec = {
        "research_job_timeout_seconds": 120,
        "research_handler_timeout_seconds": 135,
        "research_max_provider_calls": 3,
        "synthesis_max_provider_calls": 1,
    }
    if formal_model_config_id is not None:
        from nexus.agents.codex_formal_stage import formal_stage_config

        stage = json.loads(str(formal_stage_config()["stage_time_budget"]))
        runtime_spec["research_job_timeout_seconds"] = stage["job_absolute_seconds"]
        runtime_spec["research_handler_timeout_seconds"] = stage["handler_absolute_seconds"]
    payload: dict[str, object] = {
        "schema": SCHEMA,
        "account_id": str(account_id),
        "batch_id": str(batch_id),
        "batch_hash": batch.batch_hash,
        "instrument_ids": [str(i) for i in batch.instrument_ids],
        "enabled": enabled,
        "configuration_hash": configuration_hash,
        "slot": slot.isoformat(),
        "calendar_id": str(calendar[0].id),
        "calendar_hash": calendar[0].content_hash,
        "model_id": str(model.id),
        "model_hash": model.config_hash,
        "pricing_version": model.version,
        "research_prompts": {role: prompt_identity(prompt) for role, prompt in prompts.items()},
        "synthesis_prompt": prompt_identity(synthesis),
        "model_spec": {
            "max_input_tokens": model.max_input_tokens,
            "max_output_tokens": model.max_output_tokens,
            "max_tool_calls": model.max_tool_calls,
            "input_price": str(model.input_usd_per_million),
            "output_price": str(model.output_usd_per_million),
            "pricing_available_at": model.pricing_available_at.isoformat(),
            "pricing_expires_at": model.pricing_expires_at.isoformat(),
        },
        "runtime_spec": runtime_spec,
        "tasks": [task.model_dump(mode="json") for task in tasks],
    }
    # Recompute N/H under the same lock. Never trust an observed inventory amount.
    native = Decimal(0)
    for table in (
        AgentDailyBudgetReservationRecord,
        AgentPortfolioSynthesisBudgetReservationRecord,
        PortfolioDecisionBudgetReservationRecord,
    ):
        for row in await session.scalars(select(table).where(table.business_date == now.date())):
            native += await _native_usage(
                session,
                cast(
                    AgentDailyBudgetReservationRecord
                    | AgentPortfolioSynthesisBudgetReservationRecord
                    | PortfolioDecisionBudgetReservationRecord,
                    row,
                ),
            )
    from nexus.paper.economic_budget_hold import held_pair_budget

    hold = await held_pair_budget(session, business_date=now.date())
    # Disabling producers never frees existing frozen obligations.
    future = sum((task.maximum_usd for task in tasks), Decimal(0))
    if not enabled and prior is None:
        future = Decimal(0)
    if native + hold + future > daily_limit_usd:
        raise BudgetExhausted("FORMAL_BUDGET_PROTECTED")
    content_hash = _hash(payload)
    written_at = await require_apply_clock()
    async with session.begin_nested():
        audit = await append_audit_event(
            session,
            actor_id="formal-budget-apply",
            action="formal_budget_binding_apply",
            schema_version=SCHEMA,
            payload={"binding_hash": content_hash, "binding_payload": payload},
            occurred_at=written_at,
        )
        binding = FormalBudgetBindingRecord(
            budget_domain=DOMAIN,
            business_date=now.date(),
            sequence=1 if prior is None else prior.sequence + 1,
            previous_id=None if prior is None else prior.id,
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            schema_version=SCHEMA,
            daily_limit_usd=daily_limit_usd,
            payload=payload,
            applied_at=written_at,
            audit_event_id=audit.id,
        )
        session.add(binding)
        await session.flush()
        await require_apply_clock()
    return binding


async def protected_demand(
    session: AsyncSession,
    *,
    business_date: date,
    now: datetime,
) -> ProtectedDemand | None:
    binding = await latest_binding(session, business_date=business_date)
    if binding is None:
        return None
    await _validate_binding(session, binding)
    p = binding.payload
    tasks = tuple(FrozenTask.model_validate(t) for t in cast(list[object], p["tasks"]))
    native = Decimal(0)
    reservations: dict[tuple[str, UUID], object] = {}
    for kind, table in (
        ("research", AgentDailyBudgetReservationRecord),
        ("synthesis", AgentPortfolioSynthesisBudgetReservationRecord),
        ("portfolio_decision", PortfolioDecisionBudgetReservationRecord),
    ):
        for row in await session.scalars(select(table).where(table.business_date == business_date)):
            native += await _native_usage(
                session,
                cast(
                    AgentDailyBudgetReservationRecord
                    | AgentPortfolioSynthesisBudgetReservationRecord
                    | PortfolioDecisionBudgetReservationRecord,
                    row,
                ),
            )
            reservations[kind, cast(AgentDailyBudgetReservationRecord, row).run_id] = row
    cycles = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.account_id == UUID(str(p["account_id"])),
                AgentPaperCycleRecord.decision_slot_at == datetime.fromisoformat(str(p["slot"])),
            )
        )
    )
    future: list[FrozenTask] = []
    if not p["enabled"] and binding.previous_id is None:
        tasks = ()
    for task in tasks:
        run = None
        if cycles:
            if (
                len(cycles) != 1
                or str(cycles[0].candidate_batch_id) != p["batch_id"]
                or cycles[0].formal_budget_binding_id != binding.id
            ):
                raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
            cycle = cycles[0]
            if task.kind == "research":
                run = await session.scalar(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.actor_id == "local-agent-paper-cycle",
                        AgentResearchRunRecord.idempotency_key
                        == f"agent-paper:{cycle.id}:{task.instrument_id}",
                    )
                )
            else:
                run = await session.scalar(
                    select(AgentPortfolioSynthesisRunRecord).where(
                        AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id,
                    )
                )
        if run is not None and (task.kind, run.id) in reservations:
            reservation = cast(Reservation, reservations[task.kind, run.id])
            if reservation.state != "reserved" and run.status not in {"completed", "failed"}:
                raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
            await _require_exact_run(session, binding=binding, cycle=cycle, run=run, task=task)
        else:
            future.append(task)
    from nexus.paper.economic_budget_hold import held_pair_budget

    return ProtectedDemand(
        binding_id=binding.id,
        daily_limit_usd=binding.daily_limit_usd,
        native_usd=native,
        hold_usd=await held_pair_budget(session, business_date=business_date),
        future_usd=sum((task.maximum_usd for task in future), Decimal(0)),
        future_tasks=tuple(future),
    )


async def require_protected_admission(
    session: AsyncSession,
    *,
    kind: str,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    amount: Decimal,
    allocation: "AllocationRequest | None" = None,
) -> bool:
    """Shared native gate, called after the existing UTC-day lock.

    Return False for unprotected days so the legacy gate retains its behavior.
    No caller may declare a formal credit: only an exactly pinned stored run
    can consume its own future obligation.
    """
    from nexus.agents.research_usage import require_research_accounting_clear

    await require_research_accounting_clear(session)
    demand = await protected_demand(session, business_date=now.date(), now=now)
    if demand is None:
        return False
    if demand.daily_limit_usd != daily_limit_usd:
        raise ValueError("FORMAL_BUDGET_LIMIT_PENDING_APPLY")
    credit = Decimal(0)
    consume_hold = Decimal(0)
    binding = await session.get(FormalBudgetBindingRecord, demand.binding_id)
    assert binding is not None
    if allocation is not None:
        from nexus.paper.economic_budget_hold import (
            TRANSFER_ACTION,
            _require_new_transfer_authority,
            decode_hold,
        )

        event = await session.get(AuditEvent, allocation.hold_event_id)
        if event is None or event.occurred_at.date() != now.date():
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
        hold = decode_hold(event)
        receiver_type = (
            AgentPortfolioSynthesisRunRecord
            if allocation.action == "synthesis"
            else AgentResearchRunRecord
        )
        receiver = await session.get(receiver_type, run_id)
        expected = (
            hold.request.synthesis_cost_usd
            if allocation.action == "synthesis"
            else hold.request.research_cost_usd
        )
        spent = await session.scalar(
            select(AuditEvent.id)
            .where(
                AuditEvent.action == TRANSFER_ACTION,
                AuditEvent.payload["allocation"]["hold_event_id"].as_string()
                == str(allocation.hold_event_id),
                AuditEvent.payload["allocation"]["lane"].as_string() == allocation.lane,
                AuditEvent.payload["allocation"]["action"].as_string() == allocation.action,
            )
            .limit(1)
        )
        if (
            receiver is None
            or amount != expected
            or spent is not None
            or hold.request.global_limit_usd != daily_limit_usd
            or kind != ("synthesis" if allocation.action == "synthesis" else "research")
        ):
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
        await _require_new_transfer_authority(
            session, hold=hold, allocation=allocation, run=receiver, now=now
        )
        consume_hold = expected
    if kind in {"research", "synthesis"}:
        model = AgentResearchRunRecord if kind == "research" else AgentPortfolioSynthesisRunRecord
        run = cast(
            AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord | None,
            await session.get(model, run_id),
        )
        cycle = None
        if isinstance(run, AgentResearchRunRecord) and run.actor_id == "local-agent-paper-cycle":
            pieces = run.idempotency_key.split(":")
            if len(pieces) == 3 and pieces[0] == "agent-paper":
                try:
                    cycle = await session.get(AgentPaperCycleRecord, UUID(pieces[1]))
                except ValueError:
                    raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN") from None
        elif isinstance(run, AgentPortfolioSynthesisRunRecord):
            cycle = await session.get(AgentPaperCycleRecord, run.cycle_id)
        if cycle is not None and cycle.formal_budget_binding_id is not None:
            task = next(
                (
                    t
                    for t in demand.future_tasks
                    if t.kind == kind
                    and (
                        not isinstance(run, AgentResearchRunRecord)
                        or t.instrument_id == run.instrument_id
                    )
                ),
                None,
            )
            if (
                task is None
                or run is None
                or run.status != "prepared"
                or amount != task.maximum_usd
            ):
                raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
            await _require_exact_run(session, binding=binding, cycle=cycle, run=run, task=task)
            credit = task.maximum_usd
    proposed = proposed_budget_total(
        native=demand.native_usd,
        hold=demand.hold_usd,
        future=demand.future_usd,
        amount=amount,
        consume_formal=credit,
        consume_hold=consume_hold,
    )
    if proposed > daily_limit_usd:
        raise BudgetExhausted("FORMAL_BUDGET_PROTECTED")
    return True


async def _require_exact_run(
    session: AsyncSession,
    *,
    binding: FormalBudgetBindingRecord,
    cycle: AgentPaperCycleRecord,
    run: AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord,
    task: FrozenTask,
) -> None:
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
    from nexus.paper.agent_cycle_models import candidate_manifest_hash, parse_candidate_manifest

    p = binding.payload
    claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{cycle.account_id}",
            IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
            IdempotencyRecord.idempotency_key == cycle.decision_slot_at.isoformat(),
        )
    )
    manifest = parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=False)
    if (
        cycle.formal_budget_binding_id != binding.id
        or claim is None
        or claim.formal_budget_binding_id != binding.id
        or str(cycle.account_id) != p["account_id"]
        or str(cycle.candidate_batch_id) != p["batch_id"]
        or cycle.decision_slot_at.isoformat() != p["slot"]
        or not cycle.execution_locked
        or not decision_within_slot(cycle.decision_slot_at, cycle.decision_time)
        or sorted(str(t.instrument_id) for t in manifest)
        != sorted(cast(list[str], p["instrument_ids"]))
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
        or str(run.model_config_id) != p["model_id"]
        or run.model_hash != p["model_hash"]
        or run.pricing_version != p["pricing_version"]
    ):
        raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
    if isinstance(run, AgentResearchRunRecord):
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        if (
            run.instrument_id != task.instrument_id
            or run.prompt_manifest != p["research_prompts"]
            or run.idempotency_key != f"agent-paper:{cycle.id}:{task.instrument_id}"
            or context is None
            or context[0].cycle_id != cycle.id
            or run.decision_time != cycle.decision_time
        ):
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
        evidence = context[1]
        frozen_evidence = {
            "id": str(evidence.id),
            "source": evidence.source,
            "provider_object_id": evidence.provider_object_id,
            "revision": evidence.revision,
            "available_at": evidence.available_at.isoformat().replace("+00:00", "Z"),
            "location": evidence.location,
            "content_hash": evidence.content_hash,
        }
        if frozen_evidence not in run.evidence_manifest:
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
    else:
        frozen = await load_frozen_synthesis_authority(session, run)
        identity = cast(dict[str, str], p["synthesis_prompt"])
        if (
            run.idempotency_key != f"portfolio-synthesis:{cycle.id}"
            or str(frozen.prompt_version_id) != identity["id"]
            or frozen.prompt_hash != identity["content_hash"]
            or frozen.prompt_version != identity["version"]
            or str(frozen.calendar_authority_id) != p["calendar_id"]
            or frozen.cycle_id != cycle.id
            or frozen.account_id != cycle.account_id
            or sorted(str(i) for i in frozen.candidate_instrument_ids)
            != sorted(cast(list[str], p["instrument_ids"]))
        ):
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")


async def binding_for_cycle(
    session: AsyncSession, cycle_id: UUID
) -> FormalBudgetBindingRecord | None:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.formal_budget_binding_id is None:
        return None
    binding = await session.get(FormalBudgetBindingRecord, cycle.formal_budget_binding_id)
    if binding is None:
        raise ValueError("FORMAL_BUDGET_BINDING_INVALID")
    await _validate_binding(session, binding)
    return binding


async def pinned_research_authorities(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
) -> tuple[dict[str, AgentPromptVersionRecord], AgentModelConfigRecord] | None:
    if actor_id != "local-agent-paper-cycle":
        return None
    pieces = idempotency_key.split(":")
    if len(pieces) != 3 or pieces[0] != "agent-paper":
        return None
    binding = await binding_for_cycle(session, UUID(pieces[1]))
    if binding is None:
        return None
    p = binding.payload
    prompts: dict[str, AgentPromptVersionRecord] = {}
    for role, identity in cast(dict[str, dict[str, str]], p["research_prompts"]).items():
        prompt = await session.get(AgentPromptVersionRecord, UUID(identity["id"]))
        if (
            prompt is None
            or prompt.role != role
            or prompt.version != identity["version"]
            or prompt.content_hash != identity["content_hash"]
            or sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
        ):
            raise ValueError("FORMAL_BUDGET_BINDING_INVALID")
        prompts[role] = prompt
    model = await session.get(AgentModelConfigRecord, UUID(str(p["model_id"])))
    if (
        model is None
        or model.config_hash != p["model_hash"]
        or model.version != p["pricing_version"]
    ):
        raise ValueError("FORMAL_BUDGET_BINDING_INVALID")
    return prompts, model


def proposed_budget_total(
    *,
    native: Decimal,
    hold: Decimal,
    future: Decimal | None,
    amount: Decimal,
    consume_formal: Decimal = Decimal(0),
    consume_hold: Decimal = Decimal(0),
) -> Decimal:
    values = (native, hold, future, amount, consume_formal, consume_hold)
    if any(value is None or not value.is_finite() or value < 0 for value in values):
        raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
    assert future is not None
    if (
        consume_formal > future
        or consume_hold > hold
        or consume_formal not in (Decimal(0), amount)
        or consume_hold not in (Decimal(0), amount)
        or (consume_formal != 0 and consume_hold != 0)
    ):
        raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
    return native + hold + future + amount - consume_formal - consume_hold
