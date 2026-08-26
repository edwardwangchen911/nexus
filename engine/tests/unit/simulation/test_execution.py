"""Target-weight execution behavior shared by backtests and Proposal Shadow."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from nexus.simulation.canonical import canonical_decimal
from nexus.simulation.costs import CostModel
from nexus.simulation.engine import execute_target_weights
from nexus.simulation.models import (
    SimulationBar,
    SimulationInstrumentState,
    SimulationState,
    TargetWeight,
    TargetWeightCommand,
)

SIGNAL_SESSION = date(2026, 1, 2)
TARGET_SESSION = date(2026, 1, 5)


def _empty_state() -> SimulationState:
    return SimulationState(cash=Decimal("100"), instruments=(), receivables=())


def _command(
    *,
    targets: tuple[TargetWeight, ...],
    bars: tuple[SimulationBar, ...],
    cost_model: CostModel | None = None,
) -> TargetWeightCommand:
    return TargetWeightCommand(
        signal_session=SIGNAL_SESSION,
        target_session=TARGET_SESSION,
        targets=targets,
        bars=bars,
        fallback_marks=(),
        cost_model=cost_model or CostModel.zero(),
        execution_price="open",
    )


def test_execute_target_weights_returns_fractional_fill_and_new_state() -> None:
    instrument = UUID(int=1)
    original = _empty_state()

    result = execute_target_weights(
        original,
        _command(
            targets=(TargetWeight(instrument_id=instrument, weight=Decimal("0.5")),),
            bars=(
                SimulationBar(
                    instrument_id=instrument,
                    open=Decimal("10"),
                    close=Decimal("11"),
                ),
            ),
        ),
    )

    assert original.cash == Decimal("100.000000000000")
    assert original.instruments == ()
    assert result.state.cash == Decimal("50.000000000000")
    assert result.state.instruments[0].quantity == Decimal("5.000000000000")
    assert result.fills[0].notional == Decimal("50.000000000000")
    assert result.fills[0].quantity == Decimal("5.000000000000")
    assert result.outcomes[0].status == "filled"
    assert result.turnover_notional == Decimal("50.000000000000")


def test_execute_target_weights_solves_post_cost_full_weight() -> None:
    instrument = UUID(int=1)

    result = execute_target_weights(
        _empty_state(),
        _command(
            targets=(TargetWeight(instrument_id=instrument, weight=Decimal("1")),),
            bars=(
                SimulationBar(
                    instrument_id=instrument,
                    open=Decimal("10"),
                    close=Decimal("10"),
                ),
            ),
            cost_model=CostModel(
                commission_bps=Decimal("100"),
                spread_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
            ),
        ),
    )

    assert result.fills[0].notional == Decimal("99.009900990099")
    assert result.fills[0].cost == Decimal("0.990099009901")
    assert result.fills[0].quantity == Decimal("9.900990099010")
    assert canonical_decimal(result.state.cash) == Decimal("0.000000000000")


def test_execute_target_weights_records_missing_bar_without_spending_cash() -> None:
    instrument = UUID(int=1)

    result = execute_target_weights(
        _empty_state(),
        _command(
            targets=(TargetWeight(instrument_id=instrument, weight=Decimal("1")),),
            bars=(),
        ),
    )

    assert result.state == _empty_state()
    assert result.fills == ()
    assert result.turnover_notional == Decimal("0.000000000000")
    assert result.outcomes[0].status == "missing_bar"
    assert result.outcomes[0].side == "buy"


def test_execute_target_weights_records_untradable_without_fabricating_fill() -> None:
    instrument = UUID(int=1)
    state = SimulationState(
        cash=Decimal("100"),
        instruments=(
            SimulationInstrumentState(
                instrument_id=instrument,
                quantity=Decimal("0"),
                cost_basis=Decimal("0"),
                realized_pnl=Decimal("0"),
                cumulative_cost=Decimal("0"),
                last_close=None,
                untradable=True,
            ),
        ),
        receivables=(),
    )

    result = execute_target_weights(
        state,
        _command(
            targets=(TargetWeight(instrument_id=instrument, weight=Decimal("1")),),
            bars=(
                SimulationBar(
                    instrument_id=instrument,
                    open=Decimal("10"),
                    close=Decimal("10"),
                ),
            ),
        ),
    )

    assert result.fills == ()
    assert result.outcomes[0].status == "untradable"
    assert result.state.cash == Decimal("100.000000000000")


def test_execute_target_weights_orders_results_by_uuid_not_input_order() -> None:
    first = UUID(int=1)
    second = UUID(int=2)

    result = execute_target_weights(
        _empty_state(),
        _command(
            targets=(
                TargetWeight(instrument_id=second, weight=Decimal("0.4")),
                TargetWeight(instrument_id=first, weight=Decimal("0.4")),
            ),
            bars=(
                SimulationBar(instrument_id=second, open=Decimal("10"), close=Decimal("10")),
                SimulationBar(instrument_id=first, open=Decimal("10"), close=Decimal("10")),
            ),
        ),
    )

    assert tuple(fill.instrument_id for fill in result.fills) == (first, second)
    assert tuple(outcome.instrument_id for outcome in result.outcomes) == (first, second)


def test_execute_target_weights_does_not_reopen_cleared_historical_instrument() -> None:
    instrument = UUID(int=1)
    state = SimulationState(
        cash=Decimal("100"),
        instruments=(
            SimulationInstrumentState(
                instrument_id=instrument,
                quantity=Decimal("0"),
                cost_basis=Decimal("0"),
                realized_pnl=Decimal("2"),
                cumulative_cost=Decimal("1"),
                last_close=Decimal("10"),
                untradable=False,
            ),
        ),
        receivables=(),
    )

    result = execute_target_weights(state, _command(targets=(), bars=()))

    assert result.state == state
    assert result.fills == ()
    assert result.outcomes == ()
