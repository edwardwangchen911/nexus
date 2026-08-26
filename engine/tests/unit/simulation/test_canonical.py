"""Canonical Decimal and input-boundary behavior for the shared simulator."""

from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.backtest.canonical import canonical_decimal as backtest_canonical_decimal
from nexus.backtest.costs import CostModel as BacktestCostModel
from nexus.simulation.canonical import canonical_decimal
from nexus.simulation.costs import CostModel
from nexus.simulation.models import SimulationBar, TargetWeight, TargetWeightCommand


def test_canonical_decimal_quantizes_to_numeric_38_12() -> None:
    assert canonical_decimal(Decimal("100.1234567890129")) == Decimal("100.123456789013")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("1e26")])
def test_canonical_decimal_rejects_values_postgresql_cannot_authorize(value: Decimal) -> None:
    with pytest.raises(ValueError, match=r"NUMERIC\(38,12\)"):
        canonical_decimal(value)


def test_cost_model_estimates_exact_decimal_cost() -> None:
    model = CostModel(
        commission_bps=Decimal("1"),
        spread_bps=Decimal("2"),
        slippage_bps=Decimal("2"),
    )

    assert model.estimate(Decimal("1000")) == Decimal("0.5")


def test_backtest_compatibility_exports_the_shared_decimal_types() -> None:
    assert BacktestCostModel is CostModel
    assert backtest_canonical_decimal is canonical_decimal


def test_simulation_boundary_rejects_binary_float() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        SimulationBar(
            instrument_id=UUID(int=1),
            open=10.0,  # type: ignore[arg-type]
            close=Decimal("10"),
        )


def test_target_command_rejects_duplicate_instruments() -> None:
    duplicate = UUID(int=1)

    with pytest.raises(ValueError, match="target instruments must be unique"):
        TargetWeightCommand(
            signal_session=date(2026, 1, 2),
            target_session=date(2026, 1, 5),
            targets=(
                TargetWeight(instrument_id=duplicate, weight=Decimal("0.4")),
                TargetWeight(instrument_id=duplicate, weight=Decimal("0.5")),
            ),
            bars=(),
            fallback_marks=(),
            cost_model=CostModel.zero(),
            execution_price="open",
        )


def test_target_command_rejects_weight_above_full_nav() -> None:
    with pytest.raises(ValueError, match="target weights must not exceed one"):
        TargetWeightCommand(
            signal_session=date(2026, 1, 2),
            target_session=date(2026, 1, 5),
            targets=(
                TargetWeight(instrument_id=UUID(int=1), weight=Decimal("0.6")),
                TargetWeight(instrument_id=UUID(int=2), weight=Decimal("0.5")),
            ),
            bars=(),
            fallback_marks=(),
            cost_model=CostModel.zero(),
            execution_price="open",
        )
