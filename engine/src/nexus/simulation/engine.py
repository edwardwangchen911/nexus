"""Pure deterministic execution, event, and valuation functions."""

from dataclasses import replace
from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.simulation.canonical import RESULT_QUANTUM
from nexus.simulation.costs import CostModel
from nexus.simulation.models import (
    DividendReceivable,
    EventApplicationResult,
    ExecutionResult,
    SessionEvents,
    SimulationFill,
    SimulationInstrumentState,
    SimulationMark,
    SimulationOutcome,
    SimulationState,
    TargetWeightCommand,
    ValuationResult,
)


def execute_target_weights(
    state: SimulationState,
    command: TargetWeightCommand,
) -> ExecutionResult:
    """Return a new post-cost state, deterministic fills/outcomes, and turnover."""
    instruments = {item.instrument_id: item for item in state.instruments}
    targets = {item.instrument_id: item.weight for item in command.targets}
    bars = {item.instrument_id: item for item in command.bars}
    fallback_marks = {item.instrument_id: item.price for item in command.fallback_marks}
    execution_prices = {
        instrument_id: getattr(bar, command.execution_price) for instrument_id, bar in bars.items()
    }
    execution_ids = {
        item.instrument_id for item in state.instruments if item.quantity != 0
    } | set(targets)
    pre_cost_nav = state.cash + sum((item.amount for item in state.receivables), start=Decimal(0))
    pre_cost_nav += sum(
        (
            item.quantity
            * (
                execution_prices[item.instrument_id]
                if item.instrument_id in execution_prices
                else fallback_marks[item.instrument_id]
            )
            for item in state.instruments
            if item.quantity != 0
            and (item.instrument_id in execution_prices or item.instrument_id in fallback_marks)
        ),
        start=Decimal(0),
    )
    actionable = {
        instrument_id
        for instrument_id in execution_ids
        if not instruments.get(instrument_id, _empty_instrument(instrument_id)).untradable
        and instrument_id in execution_prices
    }
    current_values = {
        instrument_id: instruments.get(instrument_id, _empty_instrument(instrument_id)).quantity
        * execution_prices[instrument_id]
        for instrument_id in actionable
    }
    target_nav = _solve_post_cost_target_nav(
        pre_cost_nav=pre_cost_nav,
        current_values=current_values,
        target_weights=targets,
        actionable=actionable,
        cost_model=command.cost_model,
    )
    cash = state.cash
    fills: list[SimulationFill] = []
    outcomes: list[SimulationOutcome] = []
    turnover = Decimal(0)
    for instrument_id in sorted(execution_ids, key=lambda item: item.int):
        current = instruments.get(instrument_id, _empty_instrument(instrument_id))
        target_weight = targets.get(instrument_id, Decimal(0))
        if current.untradable:
            outcomes.append(
                SimulationOutcome(
                    instrument_id=instrument_id,
                    signal_session=command.signal_session,
                    target_session=command.target_session,
                    side=_missing_side(current.quantity, target_weight),
                    status="untradable",
                    reason="instrument delisted before target-session execution",
                )
            )
            continue
        price = execution_prices.get(instrument_id)
        if price is None:
            outcomes.append(
                SimulationOutcome(
                    instrument_id=instrument_id,
                    signal_session=command.signal_session,
                    target_session=command.target_session,
                    side=_missing_side(current.quantity, target_weight),
                    status="missing_bar",
                    reason="target-session execution bar unavailable; intent terminated",
                )
            )
            continue
        target_notional = target_nav * target_weight
        delta = target_notional - current.quantity * price
        if delta == 0:
            continue
        cost = command.cost_model.estimate(delta)
        quantity_delta = delta / price
        cost_basis = current.cost_basis
        realized = current.realized_pnl
        if delta > 0:
            cost_basis += delta
        else:
            sold_quantity = -quantity_delta
            removed_basis = (
                cost_basis * sold_quantity / current.quantity
                if current.quantity > 0
                else Decimal(0)
            )
            cost_basis -= removed_basis
            realized += -delta - removed_basis
        new_quantity = current.quantity + quantity_delta
        if abs(new_quantity) <= RESULT_QUANTUM:
            new_quantity = Decimal(0)
            cost_basis = Decimal(0)
        instruments[instrument_id] = SimulationInstrumentState(
            instrument_id=instrument_id,
            quantity=new_quantity,
            cost_basis=cost_basis,
            realized_pnl=realized,
            cumulative_cost=current.cumulative_cost + cost,
            last_close=current.last_close,
            untradable=False,
        )
        cash -= delta + cost
        turnover += abs(delta)
        fill = SimulationFill(
            instrument_id=instrument_id,
            signal_session=command.signal_session,
            fill_session=command.target_session,
            side="buy" if delta > 0 else "sell",
            quantity=abs(quantity_delta),
            price=price,
            notional=abs(delta),
            cost=cost,
        )
        fills.append(fill)
        outcomes.append(
            SimulationOutcome(
                instrument_id=instrument_id,
                signal_session=command.signal_session,
                target_session=command.target_session,
                side=fill.side,
                status="filled",
                reason=f"filled at target-session {command.execution_price}",
            )
        )
    return ExecutionResult(
        state=SimulationState(
            cash=cash,
            instruments=tuple(instruments.values()),
            receivables=state.receivables,
        ),
        fills=tuple(fills),
        outcomes=tuple(outcomes),
        turnover_notional=turnover,
    )


def apply_session_events(
    state: SimulationState,
    events: SessionEvents,
) -> EventApplicationResult:
    """Apply split, dividend entitlement/settlement, then resolved delisting."""
    if any(item.recovery_price is None for item in events.delistings):
        raise ValueError("unresolved delisting cannot be applied")
    instruments = {item.instrument_id: item for item in state.instruments}
    receivables = {item.authority_id: item for item in state.receivables}
    cash = state.cash
    for split_event in events.splits:
        current = instruments.get(split_event.instrument_id)
        if current is None or current.quantity == 0:
            continue
        instruments[split_event.instrument_id] = replace(
            current,
            quantity=current.quantity * split_event.ratio,
        )
    for dividend_event in events.dividends:
        if dividend_event.entitlement_session != events.session:
            continue
        current = instruments.get(dividend_event.instrument_id)
        if current is None or current.quantity == 0:
            continue
        if dividend_event.authority_id in receivables:
            raise ValueError("dividend authority was already entitled")
        receivables[dividend_event.authority_id] = DividendReceivable(
            authority_id=dividend_event.authority_id,
            instrument_id=dividend_event.instrument_id,
            amount=current.quantity * dividend_event.amount_per_share,
            payable_session=dividend_event.payable_session,
        )
    for authority_id in sorted(tuple(receivables), key=lambda item: item.int):
        receivable = receivables[authority_id]
        if receivable.payable_session != events.session:
            continue
        current = instruments.get(
            receivable.instrument_id, _empty_instrument(receivable.instrument_id)
        )
        cash += receivable.amount
        instruments[receivable.instrument_id] = replace(
            current,
            realized_pnl=current.realized_pnl + receivable.amount,
        )
        del receivables[authority_id]
    delisting_count = 0
    for delisting_event in events.delistings:
        current = instruments.get(
            delisting_event.instrument_id,
            _empty_instrument(delisting_event.instrument_id),
        )
        recovery_price = delisting_event.recovery_price
        if recovery_price is None:
            raise ValueError("unresolved delisting cannot be applied")
        realized = current.realized_pnl
        if current.quantity != 0:
            recovery = current.quantity * recovery_price
            cash += recovery
            realized += recovery - current.cost_basis
            delisting_count += 1
        instruments[delisting_event.instrument_id] = SimulationInstrumentState(
            instrument_id=delisting_event.instrument_id,
            quantity=Decimal(0),
            cost_basis=Decimal(0),
            realized_pnl=realized,
            cumulative_cost=current.cumulative_cost,
            last_close=None,
            untradable=True,
        )
    return EventApplicationResult(
        state=SimulationState(
            cash=cash,
            instruments=tuple(instruments.values()),
            receivables=tuple(receivables.values()),
        ),
        delisting_count=delisting_count,
    )


def mark_to_market(
    state: SimulationState,
    marks: tuple[SimulationMark, ...],
) -> ValuationResult:
    """Value cash, holdings, and receivables from one exact close set."""
    mark_ids = [item.instrument_id for item in marks]
    if len(mark_ids) != len(set(mark_ids)):
        raise ValueError("valuation marks must be unique")
    marks_by_id = {item.instrument_id: item.price for item in marks}
    nav = state.cash + sum((item.amount for item in state.receivables), start=Decimal(0))
    missing: list[UUID] = []
    instruments: list[SimulationInstrumentState] = []
    for current in state.instruments:
        if current.quantity == 0 or current.untradable:
            instruments.append(current)
            continue
        price = marks_by_id.get(current.instrument_id)
        if price is None:
            missing.append(current.instrument_id)
            instruments.append(current)
            continue
        nav += current.quantity * price
        instruments.append(replace(current, last_close=price))
    next_state = SimulationState(
        cash=state.cash,
        instruments=tuple(instruments),
        receivables=state.receivables,
    )
    return ValuationResult(
        state=next_state,
        nav=nav,
        missing_instruments=tuple(sorted(missing, key=lambda item: item.int)),
    )


def _empty_instrument(instrument_id: UUID) -> SimulationInstrumentState:
    return SimulationInstrumentState(
        instrument_id=instrument_id,
        quantity=Decimal(0),
        cost_basis=Decimal(0),
        realized_pnl=Decimal(0),
        cumulative_cost=Decimal(0),
        last_close=None,
        untradable=False,
    )


def _solve_post_cost_target_nav(
    *,
    pre_cost_nav: Decimal,
    current_values: dict[UUID, Decimal],
    target_weights: dict[UUID, Decimal],
    actionable: set[UUID],
    cost_model: CostModel,
) -> Decimal:
    cost_rate = cost_model.estimate(Decimal(1))
    if cost_rate == 0 or not actionable:
        return pre_cost_nav
    candidate = pre_cost_nav
    for _ in range(32):
        turnover = sum(
            (
                abs(
                    candidate * target_weights.get(instrument_id, Decimal(0))
                    - current_values[instrument_id]
                )
                for instrument_id in actionable
            ),
            start=Decimal(0),
        )
        updated = pre_cost_nav - cost_rate * turnover
        if abs(updated - candidate) <= RESULT_QUANTUM:
            return updated
        candidate = updated
    return candidate


def _missing_side(
    quantity: Decimal,
    target_weight: Decimal,
) -> Literal["buy", "sell", "rebalance"]:
    if quantity == 0 and target_weight > 0:
        return "buy"
    if quantity > 0 and target_weight == 0:
        return "sell"
    return "rebalance"
