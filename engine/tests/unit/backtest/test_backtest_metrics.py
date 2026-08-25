"""Complete deterministic performance metric behavior."""

from decimal import Decimal

from nexus.backtest.metrics import calculate_metrics


def test_metrics_include_downside_tail_multiple_testing_and_costs() -> None:
    """A return-only report could hide drawdown, tail loss, attempts, and trading drag."""
    metrics = calculate_metrics(
        (Decimal("100"), Decimal("80"), Decimal("88")),
        turnover_notional=Decimal("50"),
        initial_nav=Decimal("100"),
        total_cost=Decimal("1"),
        experiment_attempt_count=4,
    )

    assert metrics.total_return == Decimal("-0.12")
    assert metrics.max_drawdown == Decimal("-0.2")
    assert metrics.expected_shortfall_95 == Decimal("-0.2")
    assert metrics.total_cost == Decimal("1")
    assert metrics.cost_ratio == Decimal("0.02")
    assert Decimal(0) <= metrics.deflated_sharpe_confidence <= Decimal(1)
    assert metrics.sortino < 0
    assert metrics.calmar < 0


def test_zero_activity_metrics_are_finite_and_zero() -> None:
    """A cash-only or empty window must not serialize NaN or infinity."""
    metrics = calculate_metrics(
        (),
        turnover_notional=Decimal(0),
        initial_nav=Decimal("100"),
        total_cost=Decimal(0),
        experiment_attempt_count=1,
    )

    assert all(value.is_finite() for value in metrics.model_dump().values())
    assert set(metrics.model_dump().values()) == {Decimal(0)}
