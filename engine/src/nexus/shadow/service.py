"""Atomic application service for static Proposal Shadow sessions."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.optimizer.models import ProposalGroupStatus
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.optimization import (
    CostPolicyRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.shadow import (
    ShadowAdvanceAttemptRecord,
    ShadowCheckpointMetricRecord,
    ShadowCheckpointRecord,
    ShadowGroupRecord,
    ShadowInstrumentStateRecord,
    ShadowPortfolioRecord,
    ShadowPortfolioStateRecord,
    ShadowReceivableStateRecord,
    ShadowSealRecord,
    ShadowSessionAuthorityRecord,
    ShadowSessionEventInputRecord,
    ShadowSessionPriceInputRecord,
    ShadowSimulatedFillRecord,
    ShadowSimulatedOutcomeRecord,
)
from nexus.shadow.authority import (
    LockedShadowRoot,
    ShadowSessionInput,
    load_shadow_session_input,
)
from nexus.shadow.canonical import (
    checkpoint_metrics_content_hash,
    final_authority_content_hash,
    simulation_state_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.cost_policy import cost_model_from_policy
from nexus.shadow.metrics import calculate_checkpoint_metrics
from nexus.shadow.models import (
    CHECKPOINT_ORDINALS,
    INITIAL_NAV,
    SHADOW_VARIANTS,
    ShadowNavPoint,
    ShadowVariant,
    VariantMetrics,
)
from nexus.shadow.repository import (
    load_proposals_and_positions,
    lock_proposal_group,
    target_weight_for,
)
from nexus.simulation.canonical import canonical_decimal
from nexus.simulation.costs import CostModel
from nexus.simulation.engine import (
    apply_session_events,
    execute_target_weights,
    mark_to_market,
)
from nexus.simulation.models import (
    DividendReceivable,
    SessionEvents,
    SimulationFill,
    SimulationInstrumentState,
    SimulationMark,
    SimulationOutcome,
    SimulationState,
    TargetWeight,
    TargetWeightCommand,
)


@dataclass(frozen=True)
class ShadowAdvanceResult:
    shadow_group_id: UUID
    session: date
    ordinal: int | None
    result: Literal[
        "applied",
        "replayed",
        "data_partial",
        "data_stale",
        "source_conflict",
        "session_gap",
        "sealed",
    ]
    reason: str | None
    proposal_status: ProposalGroupStatus
    checkpoint_ordinal: int | None


@dataclass(frozen=True)
class ShadowPortfolioSessionResult:
    """One complete in-memory ledger transition before atomic persistence."""

    authority_hash: str
    state: SimulationState
    nav: Decimal
    outcomes: tuple[SimulationOutcome, ...]
    fills: tuple[SimulationFill, ...]
    turnover_notional: Decimal
    fees: Decimal


@dataclass(frozen=True)
class _PortfolioLedger:
    state: SimulationState
    cumulative_turnover: Decimal
    cumulative_fees: Decimal


async def advance_shadow_session(
    session: AsyncSession,
    *,
    group_id: UUID,
    session_date: date,
    observed_at: datetime,
) -> ShadowAdvanceResult:
    """Persist one exact next session or one sanitized failure attempt."""
    if observed_at.tzinfo is None or observed_at.utcoffset() != UTC.utcoffset(observed_at):
        raise ValueError("OBSERVED_AT_MUST_BE_UTC")
    observed_at = observed_at.astimezone(UTC)
    group = await lock_proposal_group(session, group_id)
    if group is None:
        raise ValueError("SHADOW_NOT_STARTED")
    shadow = cast(
        ShadowGroupRecord | None,
        await session.scalar(
            select(ShadowGroupRecord)
            .where(ShadowGroupRecord.proposal_group_id == group.id)
            .with_for_update()
        ),
    )
    if shadow is None:
        raise ValueError("SHADOW_NOT_STARTED")

    existing = cast(
        ShadowSessionAuthorityRecord | None,
        await session.scalar(
            select(ShadowSessionAuthorityRecord).where(
                ShadowSessionAuthorityRecord.shadow_group_id == shadow.id,
                ShadowSessionAuthorityRecord.session == session_date,
            )
        ),
    )
    if existing is not None:
        return _advance_result(
            group=group,
            shadow=shadow,
            session_date=session_date,
            ordinal=existing.ordinal,
            result="replayed",
        )
    seal = cast(
        ShadowSealRecord | None,
        await session.scalar(
            select(ShadowSealRecord).where(ShadowSealRecord.shadow_group_id == shadow.id)
        ),
    )
    if seal is not None:
        return _advance_result(
            group=group,
            shadow=shadow,
            session_date=session_date,
            ordinal=None,
            result="sealed",
            reason=seal.reason_code,
        )
    if group.status != "shadowing":
        raise ValueError("SHADOW_LIFECYCLE_MISMATCH")

    authorities = tuple(
        await session.scalars(
            select(ShadowSessionAuthorityRecord)
            .where(ShadowSessionAuthorityRecord.shadow_group_id == shadow.id)
            .order_by(ShadowSessionAuthorityRecord.ordinal)
        )
    )
    coverage = len(authorities)
    if coverage >= len(shadow.evaluation_sessions):
        raise ValueError("SHADOW_SESSION_SEQUENCE_COMPLETE")
    expected_session = shadow.evaluation_sessions[coverage]
    ordinal = coverage + 1
    if session_date < expected_session:
        raise ValueError("SHADOW_SESSION_CONFLICT")
    if session_date > expected_session:
        await _persist_attempt(
            session,
            shadow_group_id=shadow.id,
            requested_session=session_date,
            expected_session=expected_session,
            observed_at=observed_at,
            result="session_gap",
            reason="SESSION_GAP",
            authority_hash=None,
        )
        if session_date == shadow.grace_end_session:
            grace_member = await session.get(
                ExchangeCalendarSessionRecord,
                {
                    "calendar_authority_id": shadow.calendar_authority_id,
                    "session": shadow.grace_end_session,
                },
            )
            if grace_member is None:
                raise ValueError("SHADOW_CALENDAR_AUTHORITY_INVALID")
            if observed_at >= grace_member.decision_at.astimezone(UTC):
                await _expire_for_data_gap(
                    session,
                    group=group,
                    shadow=shadow,
                    coverage=coverage,
                    sealed_at=observed_at,
                )
        return _advance_result(
            group=group,
            shadow=shadow,
            session_date=session_date,
            ordinal=None,
            result="session_gap",
            reason="SESSION_GAP",
        )

    portfolios, targets_by_portfolio, instruments = await _load_frozen_targets(
        session, group.id, shadow.id
    )
    previous_by_portfolio = {
        portfolio.id: await _load_previous_ledger(
            session, portfolio=portfolio, expected_previous_ordinal=ordinal - 1
        )
        for portfolio in portfolios
    }
    required_instruments = tuple(
        instrument_id
        for instrument_id in instruments
        if ordinal == 1
        or not _instrument_permanently_untradable(instrument_id, previous_by_portfolio)
    )
    authority = await load_shadow_session_input(
        session,
        root=LockedShadowRoot.from_record(shadow),
        session_date=session_date,
        ordinal=ordinal,
        observed_at=observed_at,
        required_instruments=required_instruments,
        initial_execution_instruments=instruments if ordinal == 1 else (),
    )
    if authority.status != "complete":
        await _persist_attempt(
            session,
            shadow_group_id=shadow.id,
            requested_session=session_date,
            expected_session=expected_session,
            observed_at=observed_at,
            result=authority.status,
            reason=authority.reason or "SHADOW_AUTHORITY_INCOMPLETE",
            authority_hash=None,
        )
        return _advance_result(
            group=group,
            shadow=shadow,
            session_date=session_date,
            ordinal=ordinal,
            result=authority.status,
            reason=authority.reason,
        )

    policy = await session.get(CostPolicyRecord, shadow.cost_policy_id)
    if policy is None:
        raise ValueError("COST_POLICY_UNSUPPORTED")
    cost_model = cost_model_from_policy(policy, started_at=shadow.started_at)
    authority_record = await _persist_authority(session, shadow=shadow, authority=authority)
    for portfolio in portfolios:
        transition = simulate_shadow_portfolio_session(
            previous_state=previous_by_portfolio[portfolio.id].state,
            authority=authority,
            targets=targets_by_portfolio[portfolio.id] if ordinal == 1 else None,
            cost_model=cost_model,
            signal_session=shadow.signal_session,
        )
        await _persist_portfolio_transition(
            session,
            shadow=shadow,
            portfolio=portfolio,
            authority=authority_record,
            previous=previous_by_portfolio[portfolio.id],
            transition=transition,
        )
    await _persist_attempt(
        session,
        shadow_group_id=shadow.id,
        requested_session=session_date,
        expected_session=expected_session,
        observed_at=observed_at,
        result="applied",
        reason="SESSION_APPLIED",
        authority_hash=authority.authority_hash,
    )

    checkpoint_ordinal: int | None = None
    if ordinal in CHECKPOINT_ORDINALS:
        await _persist_complete_checkpoint(
            session,
            shadow=shadow,
            authority=authority_record,
            portfolios=portfolios,
            ordinal=cast(Literal[5, 21, 63], ordinal),
            occurred_at=observed_at,
        )
        checkpoint_ordinal = ordinal
    if ordinal == 63:
        await _finalize_complete_window(
            session,
            group=group,
            shadow=shadow,
            portfolios=portfolios,
            targets_by_portfolio=targets_by_portfolio,
            sealed_at=observed_at,
        )
    await session.flush()
    return _advance_result(
        group=group,
        shadow=shadow,
        session_date=session_date,
        ordinal=ordinal,
        result="applied",
        checkpoint_ordinal=checkpoint_ordinal,
    )


def simulate_shadow_portfolio_session(
    *,
    previous_state: SimulationState,
    authority: ShadowSessionInput,
    targets: tuple[TargetWeight, ...] | None,
    cost_model: CostModel,
    signal_session: date,
) -> ShadowPortfolioSessionResult:
    """Apply events, the one permitted initial rebalance, and raw close marks."""
    if authority.status != "complete" or authority.authority_hash is None:
        raise ValueError("SHADOW_AUTHORITY_INCOMPLETE")
    if authority.ordinal == 1:
        if targets is None:
            raise ValueError("INITIAL_TARGETS_REQUIRED")
    elif targets is not None:
        raise ValueError("TARGETS_FORBIDDEN_AFTER_INITIAL_SESSION")

    events = SessionEvents(
        session=authority.session,
        splits=tuple(event.split for event in authority.events if event.split is not None),
        dividends=tuple(event.dividend for event in authority.events if event.dividend is not None),
        delistings=tuple(
            event.delisting for event in authority.events if event.delisting is not None
        ),
    )
    state = apply_session_events(previous_state, events).state
    outcomes: tuple[SimulationOutcome, ...] = ()
    fills: tuple[SimulationFill, ...] = ()
    turnover = Decimal(0)
    if authority.ordinal == 1:
        execution = execute_target_weights(
            state,
            TargetWeightCommand(
                signal_session=signal_session,
                target_session=authority.session,
                targets=tuple(target for target in (targets or ()) if target.weight > 0),
                bars=tuple(bar.simulation_bar() for bar in authority.bars),
                fallback_marks=(),
                cost_model=cost_model,
                execution_price="open",
            ),
        )
        state = execution.state
        outcomes = execution.outcomes
        fills = execution.fills
        turnover = execution.turnover_notional

    valuation = mark_to_market(
        state,
        tuple(
            SimulationMark(instrument_id=bar.instrument_id, price=bar.close)
            for bar in authority.bars
        ),
    )
    if valuation.missing_instruments:
        raise ValueError("SHADOW_MARKS_INCOMPLETE")
    canonical_state = _canonical_state(valuation.state)
    return ShadowPortfolioSessionResult(
        authority_hash=authority.authority_hash,
        state=canonical_state,
        nav=_state_nav(canonical_state),
        outcomes=outcomes,
        fills=fills,
        turnover_notional=turnover,
        fees=sum((fill.cost for fill in fills), start=Decimal(0)),
    )


def _canonical_state(state: SimulationState) -> SimulationState:
    """Freeze every persisted ledger amount at the schema's 12-place precision."""
    return SimulationState(
        cash=canonical_decimal(state.cash),
        instruments=tuple(
            SimulationInstrumentState(
                instrument_id=item.instrument_id,
                quantity=canonical_decimal(item.quantity),
                cost_basis=canonical_decimal(item.cost_basis),
                realized_pnl=canonical_decimal(item.realized_pnl),
                cumulative_cost=canonical_decimal(item.cumulative_cost),
                last_close=(
                    None if item.last_close is None else canonical_decimal(item.last_close)
                ),
                untradable=item.untradable,
            )
            for item in state.instruments
        ),
        receivables=tuple(
            DividendReceivable(
                authority_id=item.authority_id,
                instrument_id=item.instrument_id,
                amount=canonical_decimal(item.amount),
                payable_session=item.payable_session,
            )
            for item in state.receivables
        ),
    )


def _state_nav(state: SimulationState) -> Decimal:
    nav = state.cash + sum((item.amount for item in state.receivables), start=Decimal(0))
    for item in state.instruments:
        if item.quantity > 0 and not item.untradable:
            if item.last_close is None:
                raise ValueError("SHADOW_MARKS_INCOMPLETE")
            nav += item.quantity * item.last_close
    return canonical_decimal(nav)


async def _load_frozen_targets(
    session: AsyncSession,
    proposal_group_id: UUID,
    shadow_group_id: UUID,
) -> tuple[
    tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    Mapping[UUID, tuple[TargetWeight, ...]],
    tuple[UUID, ...],
]:
    proposals, positions = await load_proposals_and_positions(session, proposal_group_id)
    portfolio_rows = tuple(
        await session.scalars(
            select(ShadowPortfolioRecord).where(
                ShadowPortfolioRecord.shadow_group_id == shadow_group_id
            )
        )
    )
    by_variant = {row.variant: row for row in portfolio_rows}
    if len(portfolio_rows) != 3 or set(by_variant) != set(SHADOW_VARIANTS):
        raise ValueError("PROPOSAL_SHADOW_INCOMPLETE")
    ordered = cast(
        tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
        tuple(by_variant[variant] for variant in SHADOW_VARIANTS),
    )
    proposal_by_variant = {proposal.variant: proposal for proposal in proposals}
    targets: dict[UUID, tuple[TargetWeight, ...]] = {}
    instrument_ids: set[UUID] = set()
    for portfolio in ordered:
        proposal = proposal_by_variant[portfolio.variant]
        variant = cast(ShadowVariant, portfolio.variant)
        values = tuple(
            TargetWeight(
                instrument_id=position.instrument_id,
                weight=target_weight_for(variant, position),
            )
            for position in positions[proposal.id]
        )
        cash_weight = Decimal(1) - sum((item.weight for item in values), start=Decimal(0))
        if (
            portfolio.proposal_id != proposal.id
            or target_weights_content_hash(values, cash_weight) != portfolio.target_weights_hash
        ):
            raise ValueError("SHADOW_PROPOSAL_AUTHORITY_INVALID")
        targets[portfolio.id] = values
        instrument_ids.update(item.instrument_id for item in values if item.weight > 0)
    return (
        ordered,
        targets,
        tuple(sorted(instrument_ids, key=lambda item: item.int)),
    )


async def _load_previous_ledger(
    session: AsyncSession,
    *,
    portfolio: ShadowPortfolioRecord,
    expected_previous_ordinal: int,
) -> _PortfolioLedger:
    if expected_previous_ordinal == 0:
        return _PortfolioLedger(
            state=SimulationState(
                cash=portfolio.initial_cash,
                instruments=(),
                receivables=(),
            ),
            cumulative_turnover=Decimal(0),
            cumulative_fees=Decimal(0),
        )
    state_record = cast(
        ShadowPortfolioStateRecord | None,
        await session.scalar(
            select(ShadowPortfolioStateRecord).where(
                ShadowPortfolioStateRecord.shadow_portfolio_id == portfolio.id,
                ShadowPortfolioStateRecord.ordinal == expected_previous_ordinal,
            )
        ),
    )
    if state_record is None:
        raise ValueError("SHADOW_STATE_SEQUENCE_INCOMPLETE")
    instrument_rows = tuple(
        await session.scalars(
            select(ShadowInstrumentStateRecord).where(
                ShadowInstrumentStateRecord.portfolio_state_id == state_record.id
            )
        )
    )
    receivable_rows = tuple(
        await session.scalars(
            select(ShadowReceivableStateRecord).where(
                ShadowReceivableStateRecord.portfolio_state_id == state_record.id
            )
        )
    )
    from nexus.simulation.models import DividendReceivable, SimulationInstrumentState

    state = SimulationState(
        cash=state_record.cash,
        instruments=tuple(
            SimulationInstrumentState(
                instrument_id=row.instrument_id,
                quantity=row.quantity,
                cost_basis=row.cost_basis,
                realized_pnl=row.realized_pnl,
                cumulative_cost=row.cumulative_cost,
                last_close=row.last_close,
                untradable=row.untradable,
            )
            for row in instrument_rows
        ),
        receivables=tuple(
            DividendReceivable(
                authority_id=row.corporate_action_id,
                instrument_id=row.instrument_id,
                amount=row.amount,
                payable_session=row.payable_session,
            )
            for row in receivable_rows
        ),
    )
    if simulation_state_content_hash(state) != state_record.state_hash:
        raise ValueError("SHADOW_STATE_HASH_MISMATCH")
    return _PortfolioLedger(
        state=state,
        cumulative_turnover=state_record.cumulative_traded_notional,
        cumulative_fees=state_record.cumulative_fees,
    )


def _instrument_permanently_untradable(
    instrument_id: UUID,
    ledgers: Mapping[UUID, _PortfolioLedger],
) -> bool:
    states = [
        item
        for ledger in ledgers.values()
        for item in ledger.state.instruments
        if item.instrument_id == instrument_id
    ]
    return len(states) == len(ledgers) and all(item.untradable for item in states)


async def _persist_authority(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    authority: ShadowSessionInput,
) -> ShadowSessionAuthorityRecord:
    if authority.authority_hash is None:
        raise ValueError("SHADOW_AUTHORITY_INCOMPLETE")
    record = ShadowSessionAuthorityRecord(
        id=new_uuid7(),
        shadow_group_id=shadow.id,
        session=authority.session,
        ordinal=authority.ordinal,
        calendar_authority_id=shadow.calendar_authority_id,
        source_policy_id=shadow.source_policy_id,
        cost_policy_id=shadow.cost_policy_id,
        content_hash=authority.authority_hash,
        available_at=authority.maximum_available_at or authority.observed_at,
        observed_at=authority.observed_at,
        late_backfill=authority.late_backfill,
        query_hash=authority.query_hash,
        source_summary={
            "queries": [
                {
                    "source": item.source,
                    "kind": item.kind,
                    "delivery_mode": item.delivery_mode,
                    "complete": item.complete,
                    "query_hash": item.query_hash,
                    "member_count": item.member_count,
                    "negative_count": item.negative_count,
                    "available_at": item.available_at.isoformat(),
                }
                for item in authority.source_summary
            ],
            "missing_execution_inputs": [
                {
                    "instrument_id": str(item.instrument_id),
                    "source": item.source,
                    "query_hash": item.query_hash,
                    "reason": item.reason,
                }
                for item in authority.missing_execution_inputs
            ],
        },
        delivery_mode_summary={"modes": list(authority.delivery_mode_summary)},
    )
    session.add(record)
    await session.flush()
    session.add_all(
        ShadowSessionPriceInputRecord(
            id=new_uuid7(),
            session_authority_id=record.id,
            shadow_group_id=shadow.id,
            price_bar_record_id=bar.price_bar_record_id,
            price_bar_set_authority_id=bar.price_bar_set_authority_id,
            instrument_id=bar.instrument_id,
            usage=bar.usage,
            source=bar.source,
            delivery_mode=bar.delivery_mode,
            revision=bar.revision,
            available_at=bar.available_at,
            content_hash=bar.content_hash,
        )
        for bar in authority.bars
    )
    session.add_all(
        ShadowSessionEventInputRecord(
            id=new_uuid7(),
            session_authority_id=record.id,
            shadow_group_id=shadow.id,
            corporate_action_id=event.corporate_action_id,
            event_kind=event.event_kind,
            provider_event_id=event.provider_event_id,
            revision=event.revision,
            available_at=event.available_at,
            evidence_hash=event.evidence_hash,
        )
        for event in authority.events
    )
    await session.flush()
    return record


async def _persist_portfolio_transition(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolio: ShadowPortfolioRecord,
    authority: ShadowSessionAuthorityRecord,
    previous: _PortfolioLedger,
    transition: ShadowPortfolioSessionResult,
) -> None:
    state = transition.state
    state_record = ShadowPortfolioStateRecord(
        id=new_uuid7(),
        shadow_group_id=shadow.id,
        shadow_portfolio_id=portfolio.id,
        session_authority_id=authority.id,
        session=authority.session,
        ordinal=authority.ordinal,
        nav=transition.nav,
        cash=state.cash,
        cumulative_traded_notional=(
            previous.cumulative_turnover
            + sum(
                (canonical_decimal(item.notional) for item in transition.fills),
                start=Decimal(0),
            )
        ),
        cumulative_fees=(
            previous.cumulative_fees
            + sum(
                (canonical_decimal(item.cost) for item in transition.fills),
                start=Decimal(0),
            )
        ),
        cumulative_realized_pnl=sum(
            (item.realized_pnl for item in state.instruments), start=Decimal(0)
        ),
        state_hash=simulation_state_content_hash(state),
    )
    session.add(state_record)
    await session.flush()
    session.add_all(
        ShadowInstrumentStateRecord(
            id=new_uuid7(),
            shadow_group_id=shadow.id,
            shadow_portfolio_id=portfolio.id,
            portfolio_state_id=state_record.id,
            instrument_id=item.instrument_id,
            quantity=item.quantity,
            cost_basis=item.cost_basis,
            realized_pnl=item.realized_pnl,
            cumulative_cost=item.cumulative_cost,
            last_close=item.last_close,
            untradable=item.untradable,
        )
        for item in state.instruments
    )
    session.add_all(
        ShadowReceivableStateRecord(
            id=new_uuid7(),
            shadow_group_id=shadow.id,
            shadow_portfolio_id=portfolio.id,
            portfolio_state_id=state_record.id,
            corporate_action_id=item.authority_id,
            instrument_id=item.instrument_id,
            amount=item.amount,
            payable_session=item.payable_session,
        )
        for item in state.receivables
    )
    outcome_ids: dict[UUID, UUID] = {}
    for outcome in transition.outcomes:
        outcome_id = new_uuid7()
        outcome_ids[outcome.instrument_id] = outcome_id
        session.add(
            ShadowSimulatedOutcomeRecord(
                id=outcome_id,
                shadow_group_id=shadow.id,
                shadow_portfolio_id=portfolio.id,
                session_authority_id=authority.id,
                instrument_id=outcome.instrument_id,
                signal_session=outcome.signal_session,
                target_session=outcome.target_session,
                ordinal=authority.ordinal,
                side=outcome.side,
                status=outcome.status,
                reason=outcome.reason,
            )
        )
    await session.flush()
    session.add_all(
        ShadowSimulatedFillRecord(
            id=new_uuid7(),
            shadow_group_id=shadow.id,
            shadow_portfolio_id=portfolio.id,
            outcome_id=outcome_ids[fill.instrument_id],
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            notional=fill.notional,
            cost=fill.cost,
            execution_price_kind="open",
        )
        for fill in transition.fills
    )
    await session.flush()


async def _persist_attempt(
    session: AsyncSession,
    *,
    shadow_group_id: UUID,
    requested_session: date,
    expected_session: date | None,
    observed_at: datetime,
    result: Literal["applied", "data_partial", "data_stale", "source_conflict", "session_gap"],
    reason: str,
    authority_hash: str | None,
) -> None:
    session.add(
        ShadowAdvanceAttemptRecord(
            id=new_uuid7(),
            shadow_group_id=shadow_group_id,
            requested_session=requested_session,
            expected_session=expected_session,
            observed_at=observed_at,
            result=result,
            reason_code=reason,
            authority_hash=authority_hash,
        )
    )
    applied = result == "applied"
    event_type = "shadow.session_applied" if applied else "shadow.session_failed"
    payload: dict[str, object] = {
        "shadow_group_id": str(shadow_group_id),
        "requested_session": requested_session.isoformat(),
        "expected_session": (None if expected_session is None else expected_session.isoformat()),
        "result": result,
        "reason": reason,
    }
    if authority_hash is not None:
        payload["authority_hash"] = authority_hash
    await append_audit_event(
        session,
        actor_id="shadow-worker",
        action=event_type,
        schema_version="1",
        payload=payload,
        occurred_at=observed_at,
    )
    session.add(
        OutboxEvent(
            event_type=event_type,
            aggregate_id=shadow_group_id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )
    await session.flush()


async def _persist_complete_checkpoint(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    authority: ShadowSessionAuthorityRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    ordinal: Literal[5, 21, 63],
    occurred_at: datetime,
) -> ShadowCheckpointRecord:
    nav_by_variant: dict[ShadowVariant, tuple[ShadowNavPoint, ...]] = {}
    turnover_by_variant: dict[ShadowVariant, Decimal] = {}
    fees_by_variant: dict[ShadowVariant, Decimal] = {}
    for portfolio in portfolios:
        variant = cast(ShadowVariant, portfolio.variant)
        states = tuple(
            await session.scalars(
                select(ShadowPortfolioStateRecord)
                .where(
                    ShadowPortfolioStateRecord.shadow_portfolio_id == portfolio.id,
                    ShadowPortfolioStateRecord.ordinal <= ordinal,
                )
                .order_by(ShadowPortfolioStateRecord.ordinal)
            )
        )
        nav_by_variant[variant] = tuple(
            ShadowNavPoint(session=state.session, nav=state.nav) for state in states
        )
        latest = states[-1]
        turnover_by_variant[variant] = latest.cumulative_traded_notional / INITIAL_NAV
        fees_by_variant[variant] = latest.cumulative_fees
    metrics = calculate_checkpoint_metrics(
        nav_by_variant=nav_by_variant,
        turnover_by_variant=turnover_by_variant,
        fees_by_variant=fees_by_variant,
        ordinal=ordinal,
    )
    return await _persist_checkpoint(
        session,
        shadow=shadow,
        portfolios=portfolios,
        ordinal=ordinal,
        status="complete",
        coverage_count=ordinal,
        reason=None,
        authority=authority,
        metrics=metrics,
        occurred_at=occurred_at,
    )


async def _persist_incomplete_checkpoint(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    ordinal: Literal[5, 21, 63],
    coverage_count: int,
    reason: str,
    occurred_at: datetime,
) -> ShadowCheckpointRecord:
    metrics = calculate_checkpoint_metrics(
        nav_by_variant={variant: () for variant in SHADOW_VARIANTS},
        turnover_by_variant={variant: Decimal(0) for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal(0) for variant in SHADOW_VARIANTS},
        ordinal=ordinal,
    )
    return await _persist_checkpoint(
        session,
        shadow=shadow,
        portfolios=portfolios,
        ordinal=ordinal,
        status="incomplete",
        coverage_count=coverage_count,
        reason=reason,
        authority=None,
        metrics=metrics,
        occurred_at=occurred_at,
    )


async def _persist_checkpoint(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    ordinal: Literal[5, 21, 63],
    status: Literal["complete", "incomplete"],
    coverage_count: int,
    reason: str | None,
    authority: ShadowSessionAuthorityRecord | None,
    metrics: tuple[VariantMetrics, VariantMetrics, VariantMetrics],
    occurred_at: datetime,
) -> ShadowCheckpointRecord:
    checkpoint = ShadowCheckpointRecord(
        id=new_uuid7(),
        shadow_group_id=shadow.id,
        session_authority_id=None if authority is None else authority.id,
        session=shadow.evaluation_sessions[ordinal - 1],
        ordinal=ordinal,
        status=status,
        coverage_count=coverage_count,
        reason=reason,
        metrics_hash=checkpoint_metrics_content_hash(metrics),
    )
    session.add(checkpoint)
    await session.flush()
    portfolio_by_variant = {portfolio.variant: portfolio for portfolio in portfolios}
    for variant_metrics in metrics:
        values = variant_metrics.metrics
        known = values.net_cumulative_return.status == "known"
        session.add(
            ShadowCheckpointMetricRecord(
                id=new_uuid7(),
                shadow_group_id=shadow.id,
                checkpoint_id=checkpoint.id,
                shadow_portfolio_id=portfolio_by_variant[variant_metrics.variant].id,
                variant=variant_metrics.variant,
                metric_status="known" if known else "unknown",
                unknown_reason=(None if known else values.net_cumulative_return.reason),
                net_cumulative_return=values.net_cumulative_return.value,
                return_delta_vs_hold=values.return_delta_vs_hold.value,
                max_drawdown=values.max_drawdown.value,
                drawdown_improvement=values.drawdown_improvement.value,
                annualized_volatility=values.annualized_volatility.value,
                volatility_improvement=values.volatility_improvement.value,
                cumulative_turnover=values.cumulative_turnover.value,
                fee_drag_index_points=values.fee_drag_index_points.value,
                worst_sessions=(
                    [
                        {
                            "session": item.session.isoformat(),
                            "return": format(item.return_value, "f"),
                        }
                        for item in values.worst_sessions
                    ]
                    if known
                    else None
                ),
            )
        )
    event_payload: dict[str, object] = {
        "shadow_group_id": str(shadow.id),
        "checkpoint_id": str(checkpoint.id),
        "session": checkpoint.session.isoformat(),
        "ordinal": ordinal,
        "status": status,
        "metrics_hash": checkpoint.metrics_hash,
        "reason": reason,
    }
    await append_audit_event(
        session,
        actor_id="shadow-worker",
        action="shadow.checkpoint_created",
        schema_version="1",
        payload=event_payload,
        occurred_at=occurred_at,
    )
    session.add(
        OutboxEvent(
            event_type="shadow.checkpoint_created",
            aggregate_id=shadow.id,
            schema_version="1",
            payload=event_payload,
            published_at=None,
        )
    )
    await session.flush()
    return checkpoint


async def _finalize_complete_window(
    session: AsyncSession,
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    targets_by_portfolio: Mapping[UUID, tuple[TargetWeight, ...]],
    sealed_at: datetime,
) -> None:
    outcomes = tuple(
        await session.scalars(
            select(ShadowSimulatedOutcomeRecord).where(
                ShadowSimulatedOutcomeRecord.shadow_group_id == shadow.id
            )
        )
    )
    filled = {
        (outcome.shadow_portfolio_id, outcome.instrument_id)
        for outcome in outcomes
        if outcome.status == "filled"
    }
    initial_complete = all(
        (portfolio.id, target.instrument_id) in filled
        for portfolio in portfolios
        for target in targets_by_portfolio[portfolio.id]
        if target.weight > 0
    ) and all(outcome.status == "filled" for outcome in outcomes)
    reason: Literal["validated", "expired"] = "validated" if initial_complete else "expired"
    reason_code = "SHADOW_VALIDATED" if initial_complete else "SHADOW_INITIAL_EXECUTION_INCOMPLETE"
    await _seal_and_transition(
        session,
        group=group,
        shadow=shadow,
        reason=reason,
        reason_code=reason_code,
        sealed_at=sealed_at,
    )


async def _expire_for_data_gap(
    session: AsyncSession,
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
    coverage: int,
    sealed_at: datetime,
) -> None:
    portfolios, _, _ = await _load_frozen_targets(session, shadow.proposal_group_id, shadow.id)
    existing_ordinals = set(
        await session.scalars(
            select(ShadowCheckpointRecord.ordinal).where(
                ShadowCheckpointRecord.shadow_group_id == shadow.id
            )
        )
    )
    for ordinal in CHECKPOINT_ORDINALS:
        if ordinal not in existing_ordinals:
            await _persist_incomplete_checkpoint(
                session,
                shadow=shadow,
                portfolios=portfolios,
                ordinal=cast(Literal[5, 21, 63], ordinal),
                coverage_count=coverage,
                reason="SHADOW_DATA_INCOMPLETE",
                occurred_at=sealed_at,
            )
    await _seal_and_transition(
        session,
        group=group,
        shadow=shadow,
        reason="expired",
        reason_code="SHADOW_DATA_INCOMPLETE",
        sealed_at=sealed_at,
    )


async def _seal_and_transition(
    session: AsyncSession,
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
    reason: Literal["validated", "expired"],
    reason_code: str,
    sealed_at: datetime,
) -> None:
    if group.status != "shadowing":
        raise ValueError("SHADOW_FINALIZE_CAS_FAILED")
    session_hashes = tuple(
        (
            authority.session,
            authority.content_hash,
        )
        for authority in await session.scalars(
            select(ShadowSessionAuthorityRecord)
            .where(ShadowSessionAuthorityRecord.shadow_group_id == shadow.id)
            .order_by(ShadowSessionAuthorityRecord.session)
        )
    )
    checkpoint_hashes = tuple(
        (checkpoint.ordinal, checkpoint.metrics_hash)
        for checkpoint in await session.scalars(
            select(ShadowCheckpointRecord)
            .where(ShadowCheckpointRecord.shadow_group_id == shadow.id)
            .order_by(ShadowCheckpointRecord.ordinal)
        )
    )
    session.add(
        ShadowSealRecord(
            id=new_uuid7(),
            shadow_group_id=shadow.id,
            reason=reason,
            reason_code=reason_code,
            sealed_at=sealed_at,
            final_coverage=len(session_hashes),
            final_authority_hash=final_authority_content_hash(session_hashes, checkpoint_hashes),
        )
    )
    await session.flush()
    group.status = reason
    group.version += 1
    await session.flush()
    payload = {
        "proposal_group_id": str(group.id),
        "shadow_group_id": str(shadow.id),
        "version": group.version,
        "reason": reason,
        "reason_code": reason_code,
    }
    await append_audit_event(
        session,
        actor_id="shadow-worker",
        action=f"shadow.{reason}",
        schema_version="1",
        payload=payload,
        occurred_at=sealed_at,
    )
    session.add(
        OutboxEvent(
            event_type=f"shadow.{reason}",
            aggregate_id=shadow.id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )
    await session.flush()


def _advance_result(
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
    session_date: date,
    ordinal: int | None,
    result: Literal[
        "applied",
        "replayed",
        "data_partial",
        "data_stale",
        "source_conflict",
        "session_gap",
        "sealed",
    ],
    reason: str | None = None,
    checkpoint_ordinal: int | None = None,
) -> ShadowAdvanceResult:
    return ShadowAdvanceResult(
        shadow_group_id=shadow.id,
        session=session_date,
        ordinal=ordinal,
        result=result,
        reason=reason,
        proposal_status=cast(ProposalGroupStatus, group.status),
        checkpoint_ordinal=checkpoint_ordinal,
    )
