"""Start one registered pair through the native cycle and research producers."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.agent_cycle_authority import CycleAuthority
from nexus.paper.economic_budget_hold import (
    ACTION,
    AllocationRequest,
    HoldRequest,
    decode_hold,
    hold_pair_budget,
)
from nexus.paper.economic_version_binding import VersionBinding, decode_binding
from nexus.paper.portfolio_economic_preflight import EconomicPlan
from nexus.paper.portfolio_economic_protocol import (
    decode_protocol,
    require_economic_database_target,
)
from nexus.persistence.models.economic import (
    EconomicEvaluationRecord,
    EconomicLaneCycleRecord,
    EconomicPairSlotRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent


@dataclass(frozen=True)
class EconomicLaneRef:
    pair_slot_id: UUID
    lane: Literal["incumbent", "challenger"]


@dataclass(frozen=True)
class EconomicLaneAuthority:
    identity: EconomicEvaluationRecord
    slot: EconomicPairSlotRecord
    protocol: EconomicPlan
    binding: VersionBinding


@dataclass(frozen=True)
class EconomicPairResult:
    pair_slot_id: UUID
    cycle_ids: tuple[UUID, ...]
    status: str
    replayed: bool
    lane_states: tuple[tuple[str, str, str], ...] = ()


async def require_economic_lane(
    session: AsyncSession, *, ref: EconomicLaneRef
) -> EconomicLaneAuthority:
    if ref.lane not in {"incumbent", "challenger"}:
        raise ValueError("ECONOMIC_PAIR_LANE_INVALID")
    slot = await session.get(EconomicPairSlotRecord, ref.pair_slot_id)
    identity = (
        None if slot is None else await session.get(EconomicEvaluationRecord, slot.evaluation_id)
    )
    if slot is None or identity is None:
        raise ValueError("ECONOMIC_PAIR_LANE_INVALID")
    protocol_event = await session.get(AuditEvent, identity.protocol_event_id)
    binding_event = await session.get(AuditEvent, identity.binding_event_id)
    if protocol_event is None or binding_event is None:
        raise ValueError("ECONOMIC_PAIR_AUTHORITY_UNAVAILABLE")
    protocol, binding = decode_protocol(protocol_event), decode_binding(binding_event)
    frozen = protocol.slots[slot.ordinal - 1]
    if (
        protocol.content_hash != identity.protocol_hash
        or binding.content_hash != identity.binding_hash
        or binding.protocol_hash != identity.protocol_hash
        or (
            slot.ordinal,
            slot.session,
            slot.decision_at,
            slot.cutoff_at,
            slot.market_close_at,
            slot.execution_locked,
        )
        != (
            frozen.ordinal,
            frozen.session,
            frozen.decision_at,
            frozen.decision_at,
            frozen.market_close_at,
            True,
        )
    ):
        raise ValueError("ECONOMIC_PAIR_AUTHORITY_UNAVAILABLE")
    return EconomicLaneAuthority(identity, slot, protocol, binding)


def lane_account(authority: EconomicLaneAuthority, lane: str) -> tuple[UUID, UUID]:
    if lane == "incumbent":
        return authority.identity.incumbent_account_id, authority.identity.incumbent_portfolio_id
    if lane == "challenger":
        return authority.identity.challenger_account_id, authority.identity.challenger_portfolio_id
    raise ValueError("ECONOMIC_PAIR_LANE_INVALID")


async def require_economic_creation(
    session: AsyncSession,
    *,
    ref: EconomicLaneRef | None,
    authority: CycleAuthority,
    decision_slot_at: datetime,
) -> EconomicLaneAuthority | None:
    registered = await session.scalar(
        select(EconomicEvaluationRecord.id).where(
            (EconomicEvaluationRecord.incumbent_account_id == authority.account_id)
            | (EconomicEvaluationRecord.challenger_account_id == authority.account_id)
        )
    )
    if ref is None:
        if registered is not None:
            raise ValueError("ECONOMIC_PAIR_LANE_REQUIRED")
        return None
    economic = await require_economic_lane(session, ref=ref)
    if (
        registered != economic.identity.id
        or lane_account(economic, ref.lane) != (authority.account_id, authority.portfolio_id)
        or authority.decision_time != economic.slot.decision_at
        or decision_slot_at != economic.slot.decision_at
        or authority.decision_session != economic.slot.session
        or authority.execution_session != economic.slot.session
    ):
        raise ValueError("ECONOMIC_PAIR_LANE_INVALID")
    actual = await session.scalar(text("SELECT current_database()"))
    await require_economic_database_target(session, expected=actual)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now < economic.slot.decision_at:
        raise ValueError("ECONOMIC_PAIR_SLOT_NOT_DUE")
    if now >= min(economic.slot.decision_at + timedelta(minutes=5), economic.slot.market_close_at):
        raise ValueError("ECONOMIC_PAIR_SLOT_MISSED")
    return economic


async def read_economic_cycle(
    session: AsyncSession, *, cycle_id: UUID
) -> tuple[EconomicLaneRef, EconomicLaneAuthority] | None:
    link = await session.scalar(
        select(EconomicLaneCycleRecord).where(EconomicLaneCycleRecord.cycle_id == cycle_id)
    )
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if link is None:
        if cycle is not None and await session.scalar(
            select(EconomicEvaluationRecord.id).where(
                (EconomicEvaluationRecord.incumbent_account_id == cycle.account_id)
                | (EconomicEvaluationRecord.challenger_account_id == cycle.account_id)
            )
        ):
            raise ValueError("ECONOMIC_PAIR_CYCLE_NOT_BOUND")
        return None
    ref = EconomicLaneRef(link.pair_slot_id, link.lane)  # type: ignore[arg-type]
    economic = await require_economic_lane(session, ref=ref)
    if (
        cycle is None
        or (cycle.account_id, cycle.portfolio_id) != lane_account(economic, ref.lane)
        or cycle.decision_time != economic.slot.decision_at
        or cycle.decision_slot_at != economic.slot.decision_at
        or not cycle.execution_locked
    ):
        raise ValueError("ECONOMIC_PAIR_CYCLE_INVALID")
    return ref, economic


async def research_allocation(
    session: AsyncSession,
    *,
    ref: EconomicLaneRef,
    authority: EconomicLaneAuthority,
    instrument_id: UUID,
) -> AllocationRequest:
    instruments = tuple(item.instrument_id for item in authority.protocol.request.instruments)
    if instrument_id not in instruments:
        raise ValueError("ECONOMIC_PAIR_CANDIDATE_INVALID")
    holds = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["request"]["protocol_hash"].as_string()
                == authority.identity.protocol_hash,
                AuditEvent.payload["request"]["slot_ordinal"].as_integer()
                == authority.slot.ordinal,
            )
            .limit(2)
        )
    )
    if len(holds) != 1:
        raise ValueError("ECONOMIC_PAIR_HOLD_UNAVAILABLE")
    hold = decode_hold(holds[0])
    if hold.request.evaluation_key != authority.protocol.request.evaluation_key:
        raise ValueError("ECONOMIC_PAIR_HOLD_UNAVAILABLE")
    return AllocationRequest(
        hold_event_id=holds[0].id,
        lane=ref.lane,
        action="research_1" if instruments.index(instrument_id) == 0 else "research_2",
    )


async def synthesis_allocation(
    session: AsyncSession, *, ref: EconomicLaneRef, authority: EconomicLaneAuthority
) -> AllocationRequest:
    allocation = await research_allocation(
        session,
        ref=ref,
        authority=authority,
        instrument_id=authority.protocol.request.instruments[0].instrument_id,
    )
    return allocation.model_copy(update={"action": "synthesis"})


async def start_economic_pair(
    session: AsyncSession, *, pair_slot_id: UUID, expected_database: str
) -> EconomicPairResult:
    # A caller that catches rejection must not commit half a pair or its hold.
    async with session.begin_nested():
        return await _start_economic_pair(
            session, pair_slot_id=pair_slot_id, expected_database=expected_database
        )


async def _start_economic_pair(
    session: AsyncSession, *, pair_slot_id: UUID, expected_database: str
) -> EconomicPairResult:
    from nexus.paper.agent_cycle import _events, _result, _start_cycle
    from nexus.paper.agent_cycle_authority import (
        load_quasi_realtime_cycle_authority,
        select_agent_paper_service_calendar_authority,
    )
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch

    await require_economic_database_target(session, expected=expected_database)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,2026))"),
        {"scope": f"economic-pair:{pair_slot_id}"},
    )
    economic = await require_economic_lane(session, ref=EconomicLaneRef(pair_slot_id, "incumbent"))
    links = tuple(
        await session.scalars(
            select(EconomicLaneCycleRecord)
            .where(EconomicLaneCycleRecord.pair_slot_id == pair_slot_id)
            .order_by(EconomicLaneCycleRecord.lane)
        )
    )
    if links:
        if len(links) != 2 or {link.lane for link in links} != {"incumbent", "challenger"}:
            raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
        states = []
        for link in links:
            await read_economic_cycle(session, cycle_id=link.cycle_id)
            events = await _events(session, link.cycle_id)
            if not events:
                raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
            states.append(_result(link.cycle_id, events, replayed=True).status)
        return EconomicPairResult(
            pair_slot_id,
            tuple(link.cycle_id for link in links),
            "researching" if all(state == "researching" for state in states) else "existing",
            True,
        )
    from nexus.paper.economic_isolation import require_running_isolation

    # An old hold is a receipt, not permission to admit new native facts.
    await require_running_isolation(
        session, authority=economic, expected_database=expected_database
    )
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    if now < economic.slot.decision_at:
        raise ValueError("ECONOMIC_PAIR_SLOT_NOT_DUE")
    if now >= min(economic.slot.decision_at + timedelta(minutes=5), economic.slot.market_close_at):
        raise ValueError("ECONOMIC_PAIR_SLOT_MISSED")
    hold = await hold_pair_budget(
        session,
        HoldRequest(
            evaluation_key=economic.protocol.request.evaluation_key,
            protocol_hash=economic.identity.protocol_hash,
            slot_ordinal=economic.slot.ordinal,
            research_cost_usd=economic.protocol.request.research_cost_usd,
            synthesis_cost_usd=economic.protocol.request.synthesis_cost_usd,
            global_limit_usd=economic.protocol.request.global_daily_limit_usd,
            uncovered_formal_usd=Decimal(0),
        ),
        expected_database=expected_database,
    )
    assert hold is not None
    calendar = await select_agent_paper_service_calendar_authority(
        session, now=economic.slot.decision_at
    )
    if calendar.market_close_at != economic.slot.market_close_at:
        raise ValueError("ECONOMIC_PAIR_CALENDAR_INVALID")
    ids = tuple(
        sorted(
            (item.instrument_id for item in economic.protocol.request.instruments),
            key=lambda item: item.int,
        )
    )
    results = []
    for lane in ("challenger", "incumbent"):
        ref = EconomicLaneRef(pair_slot_id, lane)  # type: ignore[arg-type]
        account_id, _ = lane_account(economic, lane)
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=account_id,
            now=economic.slot.decision_at,
            expected_calendar_authority_id=calendar.calendar_authority_id,
        )
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=account_id,
            universe_snapshot_id=authority.universe_snapshot_id,
            instrument_ids=ids,
            idempotency_key=f"economic-batch:{pair_slot_id}:{lane}",
            created_at=economic.slot.decision_at,
        )
        result = await _start_cycle(
            session,
            authority=authority,
            decision_slot_at=economic.slot.decision_at,
            daily_limit_usd=economic.protocol.request.global_daily_limit_usd,
            requested_instrument_ids=ids,
            candidate_batch_id=batch.batch_id,
            economic_lane_ref=ref,
        )
        if result.status != "researching":
            raise ValueError("ECONOMIC_PAIR_RESEARCH_UNAVAILABLE")
        results.append(result)
    return EconomicPairResult(
        pair_slot_id,
        tuple(result.cycle_id for result in results),
        "researching"
        if all(result.status == "researching" for result in results)
        else "unavailable",
        False,
    )


async def resume_economic_pair(
    session: AsyncSession, *, pair_slot_id: UUID, expected_database: str
) -> EconomicPairResult:
    """Advance registered lanes from native persisted facts, never execute a Worker."""
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord

    async with session.begin_nested():
        await require_economic_database_target(session, expected=expected_database)
        authority = await require_economic_lane(
            session, ref=EconomicLaneRef(pair_slot_id, "incumbent")
        )
        # Existing deterministic paper producers take the account lock. Acquire
        # both in stable order before the pair, never acquire a peer from a lane.
        account_ids = sorted(
            (lane_account(authority, lane)[0] for lane in ("incumbent", "challenger")),
            key=lambda item: item.int,
        )
        for account_id in account_ids:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,1818))"),
                {"scope": f"internal-paper:{account_id}"},
            )
        for account_id in account_ids:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))"),
                {"scope": f"nexus:agent-paper-cycle:{account_id}"},
            )
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope,2026))"),
            {"scope": f"economic-pair:{pair_slot_id}"},
        )
        links = tuple(
            await session.scalars(
                select(EconomicLaneCycleRecord)
                .where(EconomicLaneCycleRecord.pair_slot_id == pair_slot_id)
                .order_by(EconomicLaneCycleRecord.lane)
            )
        )
        if len(links) != 2 or {link.lane for link in links} != {"incumbent", "challenger"}:
            raise ValueError("ECONOMIC_PAIR_RESULT_UNKNOWN")
        for link in links:
            owner = await read_economic_cycle(session, cycle_id=link.cycle_id)
            if owner is None or owner[0].pair_slot_id != pair_slot_id or owner[0].lane != link.lane:
                raise ValueError("ECONOMIC_PAIR_LANE_INVALID")
        synthesis_ids_query = (
            select(AgentPortfolioSynthesisRunRecord.id)
            .where(
                AgentPortfolioSynthesisRunRecord.cycle_id.in_(
                    tuple(link.cycle_id for link in links)
                )
            )
            .order_by(AgentPortfolioSynthesisRunRecord.id)
        )
        before_synthesis_ids = tuple(await session.scalars(synthesis_ids_query))
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        results = []
        for link in links:
            results.append(
                await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=link.cycle_id,
                    idempotency_key=f"economic-pair-resume:{pair_slot_id}:{link.lane}",
                    now=now,
                    daily_limit_usd=authority.protocol.request.global_daily_limit_usd,
                )
            )
        states = {result.status for result in results}
        status = next(iter(states)) if len(states) == 1 else "partial"
        return EconomicPairResult(
            pair_slot_id,
            tuple(link.cycle_id for link in links),
            status,
            all(result.replayed for result in results)
            and tuple(await session.scalars(synthesis_ids_query)) == before_synthesis_ids,
            tuple(
                (link.lane, result.status, result.result_code)
                for link, result in zip(links, results, strict=True)
            ),
        )
