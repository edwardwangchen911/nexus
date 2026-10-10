"""Whole-pair diagnostic holds counted by the native UTC shared budget."""

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.paper.portfolio_economic_preflight import EconomicPlan, PlannedSlot
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
)
from nexus.persistence.models.runtime import AuditEvent

ACTION = "portfolio.economic.budget_held"
SCHEMA = "economic_pair_budget_hold_v1"
ACTOR = "local-economic-budget-hold-cli"
TRANSFER_ACTION = "portfolio.economic.budget_transferred"
TRANSFER_SCHEMA = "economic_budget_transfer_v1"


async def _require_budget_day(session: AsyncSession, *, expected: date, code: str) -> datetime:
    """Recheck the DB clock after waits; never move a new write into another day."""
    observed = await session.scalar(select(func.clock_timestamp()))
    if (
        not isinstance(observed, datetime)
        or observed.tzinfo is None
        or observed.utcoffset() is None
        or observed.astimezone(UTC).date() != expected
    ):
        raise ValueError(code)
    return observed.astimezone(UTC)


class StrictModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class HoldRequest(StrictModel):
    evaluation_key: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    protocol_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    slot_ordinal: int = Field(ge=1, le=24)
    research_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    synthesis_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    global_limit_usd: Decimal = Field(gt=0, allow_inf_nan=False)
    uncovered_formal_usd: Decimal = Field(ge=0, allow_inf_nan=False)

    @property
    def pair_ceiling(self) -> Decimal:
        return 4 * self.research_cost_usd + 2 * self.synthesis_cost_usd


def require_protocol_budget_day(
    protocol: EconomicPlan, request: HoldRequest, *, observed_at: datetime
) -> PlannedSlot:
    """Bind a budget transfer to its persisted slot; this does not authorize a trigger."""
    if (
        observed_at.tzinfo is None
        or observed_at.utcoffset() is None
        or protocol.content_hash != request.protocol_hash
        or protocol.request.evaluation_key != request.evaluation_key
        or not 1 <= request.slot_ordinal <= len(protocol.slots)
    ):
        raise ValueError("ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID")
    slot = protocol.slots[request.slot_ordinal - 1]
    if slot.ordinal != request.slot_ordinal or slot.decision_at.utcoffset() is None:
        raise ValueError("ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID")
    if slot.decision_at.astimezone(UTC).date() != observed_at.astimezone(UTC).date():
        raise ValueError("ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH")
    return slot


class PairHold(StrictModel):
    schema_version: Literal["economic_pair_budget_hold_v1"] = "economic_pair_budget_hold_v1"
    status: Literal["diagnostic_pair_held"] = "diagnostic_pair_held"
    request: HoldRequest
    allowed_model_jobs: Literal[0] = 0
    activation_permitted: Literal[False] = False


class AllocationRequest(StrictModel):
    hold_event_id: UUID
    lane: Literal["incumbent", "challenger"]
    action: Literal["research_1", "research_2", "synthesis"]


class TransferReceipt(StrictModel):
    allocation: AllocationRequest
    run_id: UUID
    reservation_id: UUID
    pricing_version: str = Field(min_length=1, max_length=100)
    amount_usd: Decimal = Field(ge=0, allow_inf_nan=False)


def decode_transfer(event: AuditEvent) -> TransferReceipt:
    receipt = TransferReceipt.model_validate(event.payload)
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
        )
    )
    if (
        event.action != TRANSFER_ACTION
        or event.schema_version != TRANSFER_SCHEMA
        or event.actor_id != ACTOR
        or canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
    ):
        raise ValueError("ECONOMIC_TRANSFER_INVALID")
    return receipt


def decode_hold(event: AuditEvent) -> PairHold:
    hold = PairHold.model_validate(event.payload)
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
        )
    )
    if (
        event.action != ACTION
        or event.schema_version != SCHEMA
        or event.actor_id != ACTOR
        or canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
    ):
        raise ValueError("ECONOMIC_HOLD_INVALID")
    return hold


async def held_pair_budget(session: AsyncSession, *, business_date: date) -> Decimal:
    start = datetime.combine(business_date, time.min, tzinfo=UTC)
    events = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.occurred_at >= start,
                AuditEvent.occurred_at < start + timedelta(days=1),
            )
        )
    )
    keys = set()
    total = Decimal(0)
    for event in events:
        hold = decode_hold(event)
        hold_key = (hold.request.evaluation_key, hold.request.slot_ordinal)
        if hold_key in keys:
            raise ValueError("ECONOMIC_HOLD_INVALID")
        keys.add(hold_key)
        total += hold.request.pair_ceiling
    holds = {event.id: decode_hold(event) for event in events}
    transfers = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == TRANSFER_ACTION,
                AuditEvent.occurred_at >= start,
                AuditEvent.occurred_at < start + timedelta(days=1),
            )
        )
    )
    allocated = set()
    children = set()
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentPortfolioSynthesisBudgetReservationRecord,
    )

    for event in transfers:
        receipt = decode_transfer(event)
        source_hold = holds.get(receipt.allocation.hold_event_id)
        key = (receipt.allocation.hold_event_id, receipt.allocation.lane, receipt.allocation.action)
        if source_hold is None or key in allocated or receipt.reservation_id in children:
            raise ValueError("ECONOMIC_TRANSFER_INVALID")
        allocated.add(key)
        children.add(receipt.reservation_id)
        synthesis = receipt.allocation.action == "synthesis"
        expected = (
            source_hold.request.synthesis_cost_usd
            if synthesis
            else source_hold.request.research_cost_usd
        )
        model = (
            AgentPortfolioSynthesisBudgetReservationRecord
            if synthesis
            else AgentDailyBudgetReservationRecord
        )
        child = cast(
            AgentDailyBudgetReservationRecord
            | AgentPortfolioSynthesisBudgetReservationRecord
            | None,
            await session.get(model, receipt.reservation_id),
        )
        if (
            child is None
            or child.run_id != receipt.run_id
            or child.business_date != business_date
            or child.reserved_usd != expected
            or receipt.amount_usd != expected
            or child.pricing_version != receipt.pricing_version
        ):
            raise ValueError("ECONOMIC_TRANSFER_INVALID")
        total -= expected
    return total


async def _require_new_transfer_authority(
    session: AsyncSession,
    *,
    hold: PairHold,
    allocation: AllocationRequest,
    run: object,
    now: datetime,
) -> None:
    from nexus.paper.economic_receiver import require_economic_receiver
    from nexus.paper.economic_version_binding import read_binding
    from nexus.paper.portfolio_economic_protocol import read_protocol
    from nexus.persistence.models.agents import (
        AgentModelConfigRecord,
        AgentPortfolioSynthesisRunRecord,
        AgentResearchRunRecord,
    )

    _, _, protocol = await read_protocol(session, as_of=now)
    if protocol is None:
        raise ValueError("ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID")
    require_protocol_budget_day(protocol, hold.request, observed_at=now)
    _, binding = await read_binding(session, as_of=now)
    if binding is None or not isinstance(
        run, AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord
    ):
        raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.paper.economic_cycle import read_economic_cycle

    if isinstance(run, AgentResearchRunRecord):
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        economic = (
            None
            if context is None
            else await read_economic_cycle(session, cycle_id=context[0].cycle_id)
        )
    else:
        from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority

        try:
            await load_frozen_synthesis_authority(session, run)
        except ValueError as error:
            raise ValueError("ECONOMIC_TRANSFER_RECEIVER_INVALID") from error
        economic = await read_economic_cycle(session, cycle_id=run.cycle_id)
    if economic is not None:
        ref, authority = economic
        if (
            allocation.lane != ref.lane
            or hold.request.protocol_hash != authority.identity.protocol_hash
            or hold.request.slot_ordinal != authority.slot.ordinal
            or binding.content_hash != authority.identity.binding_hash
        ):
            raise ValueError("ECONOMIC_TRANSFER_LANE_AUTHORITY_INVALID")
    require_economic_receiver(protocol, binding, allocation, run)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    expected = (
        hold.request.synthesis_cost_usd
        if allocation.action == "synthesis"
        else hold.request.research_cost_usd
    )
    if (
        model is None
        or model.config_hash != binding.shared_model_hash
        or model.version != binding.shared_model_version
        or model.created_at > now
        or model.worst_case_cost_usd != expected
        or binding.configured_cost_usd != expected
        or not model.pricing_available_at <= now < model.pricing_expires_at
        or not binding.pricing_available_at <= now < binding.pricing_expires_at
    ):
        raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")


async def reserve_from_hold(
    session: AsyncSession,
    *,
    allocation: AllocationRequest,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    worst_case_usd: Decimal,
    pricing_version: str,
) -> AgentDailyBudgetReservationRecord | AgentPortfolioSynthesisBudgetReservationRecord:
    from sqlalchemy import func, text

    from nexus.agents.persistence import _lock_budget_day, _shared_daily_budget_used
    from nexus.domain.ids import new_uuid7
    from nexus.paper.portfolio_economic_protocol import _ISOLATED_DATABASE
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentPortfolioSynthesisBudgetReservationRecord,
        AgentPortfolioSynthesisRunRecord,
        AgentResearchRunRecord,
    )

    actual = await session.scalar(text("SELECT current_database()"))
    if not isinstance(actual, str) or not _ISOLATED_DATABASE.fullmatch(actual):
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    observed = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(observed, datetime) or observed.date() != now.date():
        raise ValueError("ECONOMIC_TRANSFER_DAY_MISMATCH")
    await _lock_budget_day(session, now.date().isoformat())
    event = await session.get(AuditEvent, allocation.hold_event_id)
    if event is None or event.occurred_at.date() != now.date():
        raise ValueError("ECONOMIC_TRANSFER_DAY_MISMATCH")
    hold = decode_hold(event)
    synthesis = allocation.action == "synthesis"
    expected = hold.request.synthesis_cost_usd if synthesis else hold.request.research_cost_usd
    if worst_case_usd != expected or daily_limit_usd != hold.request.global_limit_usd:
        raise ValueError("ECONOMIC_TRANSFER_AMOUNT_MISMATCH")
    existing = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == TRANSFER_ACTION,
                AuditEvent.payload["allocation"]["hold_event_id"].as_string()
                == str(allocation.hold_event_id),
                AuditEvent.payload["allocation"]["lane"].as_string() == allocation.lane,
                AuditEvent.payload["allocation"]["action"].as_string() == allocation.action,
            )
            .limit(2)
        )
    )
    model = (
        AgentPortfolioSynthesisBudgetReservationRecord
        if synthesis
        else AgentDailyBudgetReservationRecord
    )
    if existing:
        if len(existing) != 1:
            raise ValueError("ECONOMIC_TRANSFER_INVALID")
        frozen = decode_transfer(existing[0])
        if (
            frozen.run_id != run_id
            or frozen.pricing_version != pricing_version
            or frozen.amount_usd != expected
        ):
            raise ValueError("ECONOMIC_TRANSFER_CONFLICT")
        child = cast(
            AgentDailyBudgetReservationRecord
            | AgentPortfolioSynthesisBudgetReservationRecord
            | None,
            await session.get(model, frozen.reservation_id),
        )
        if child is None:
            raise ValueError("ECONOMIC_TRANSFER_INVALID")
        await _shared_daily_budget_used(session, business_date=now.date())
        return child
    observed = await _require_budget_day(
        session, expected=now.date(), code="ECONOMIC_TRANSFER_DAY_MISMATCH"
    )
    run_model = AgentPortfolioSynthesisRunRecord if synthesis else AgentResearchRunRecord
    run = cast(
        AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord | None,
        await session.get(run_model, run_id),
    )
    if (
        run is None
        or run.status != "prepared"
        or run.pricing_version != pricing_version
        or (isinstance(run, AgentResearchRunRecord) and run.reserved_cost_usd != expected)
    ):
        raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")
    if await session.scalar(select(model.id).where(model.run_id == run_id)) is not None:
        raise ValueError("ECONOMIC_TRANSFER_CONFLICT")
    await _require_new_transfer_authority(
        session, hold=hold, allocation=allocation, run=run, now=observed
    )
    from nexus.paper.formal_budget_protection import require_protected_admission

    await require_protected_admission(
        session,
        kind="synthesis" if synthesis else "research",
        run_id=run_id,
        now=observed,
        daily_limit_usd=daily_limit_usd,
        amount=expected,
        allocation=allocation,
    )
    # Native reservation and immutable credit receipt commit or roll back together.
    async with session.begin_nested():
        written_at = await _require_budget_day(
            session, expected=now.date(), code="ECONOMIC_TRANSFER_DAY_MISMATCH"
        )
        child = cast(
            AgentDailyBudgetReservationRecord | AgentPortfolioSynthesisBudgetReservationRecord,
            model(
                id=new_uuid7(),
                business_date=now.date(),
                run_id=run_id,
                pricing_version=pricing_version,
                reserved_usd=expected,
                state="reserved",
            ),
        )
        session.add(child)
        await session.flush()
        receipt = TransferReceipt(
            allocation=allocation,
            run_id=run_id,
            reservation_id=child.id,
            pricing_version=pricing_version,
            amount_usd=expected,
        )
        await append_audit_event(
            session,
            actor_id=ACTOR,
            action=TRANSFER_ACTION,
            schema_version=TRANSFER_SCHEMA,
            payload=receipt.model_dump(mode="json"),
            occurred_at=written_at,
        )
        await _require_budget_day(
            session, expected=now.date(), code="ECONOMIC_TRANSFER_DAY_MISMATCH"
        )
        used = await _shared_daily_budget_used(session, business_date=now.date())
        if used + hold.request.uncovered_formal_usd > daily_limit_usd:
            raise BudgetExhausted("ECONOMIC_PAIR_BUDGET_INSUFFICIENT")
        await _require_budget_day(
            session, expected=now.date(), code="ECONOMIC_TRANSFER_DAY_MISMATCH"
        )
    return child


async def hold_pair_budget(
    session: AsyncSession,
    request: HoldRequest,
    *,
    expected_database: str,
) -> AuditEvent:
    from sqlalchemy import func, text

    from nexus.agents.persistence import _lock_budget_day, _shared_daily_budget_used
    from nexus.paper.economic_isolation import require_hold_isolation
    from nexus.paper.portfolio_economic_protocol import _ISOLATED_DATABASE

    actual = await session.scalar(text("SELECT current_database()"))
    if actual != expected_database or not _ISOLATED_DATABASE.fullmatch(expected_database):
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    await _lock_budget_day(session, now.date().isoformat())
    events = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["request"]["evaluation_key"].as_string()
                == request.evaluation_key,
                AuditEvent.payload["request"]["slot_ordinal"].as_integer() == request.slot_ordinal,
            )
            .limit(2)
        )
    )
    if events:
        if len(events) != 1:
            raise ValueError("ECONOMIC_HOLD_INVALID")
        if decode_hold(events[0]).request != request:
            raise ValueError("ECONOMIC_HOLD_CONFLICT")
        return events[0]
    now = await _require_budget_day(session, expected=now.date(), code="ECONOMIC_HOLD_DAY_MISMATCH")
    await require_hold_isolation(
        session, request=request, expected_database=expected_database, now=now
    )
    used = await _shared_daily_budget_used(session, business_date=now.date())
    from nexus.paper.formal_budget_protection import protected_demand

    demand = await protected_demand(session, business_date=now.date(), now=now)
    uncovered = request.uncovered_formal_usd
    if demand is not None:
        if demand.daily_limit_usd != request.global_limit_usd:
            raise ValueError("FORMAL_BUDGET_LIMIT_PENDING_APPLY")
        used = demand.native_usd + demand.hold_usd
        uncovered = demand.future_usd
    if used + uncovered + request.pair_ceiling > request.global_limit_usd:
        raise BudgetExhausted("ECONOMIC_PAIR_BUDGET_INSUFFICIENT")
    hold = PairHold(request=request)
    async with session.begin_nested():
        written_at = await _require_budget_day(
            session, expected=now.date(), code="ECONOMIC_HOLD_DAY_MISMATCH"
        )
        event = await append_audit_event(
            session,
            actor_id=ACTOR,
            action=ACTION,
            schema_version=SCHEMA,
            payload=hold.model_dump(mode="json"),
            occurred_at=written_at,
        )
        await _require_budget_day(session, expected=now.date(), code="ECONOMIC_HOLD_DAY_MISMATCH")
    return event
