"""Exact 5/21/63 Proposal Shadow checkpoint metrics."""

from datetime import date, timedelta
from decimal import Decimal

import pytest
from nexus.shadow.metrics import annualized_volatility, calculate_checkpoint_metrics
from nexus.shadow.models import SHADOW_VARIANTS, ShadowNavPoint
from nexus.simulation.canonical import canonical_decimal


def _sessions() -> tuple[date, ...]:
    start = date(2026, 1, 5)
    return tuple(start + timedelta(days=index) for index in range(5))


def _points(values: tuple[str, ...]) -> tuple[ShadowNavPoint, ...]:
    return tuple(
        ShadowNavPoint(session=session, nav=Decimal(value))
        for session, value in zip(_sessions()[: len(values)], values, strict=True)
    )


def test_checkpoint_metrics_use_anchor_and_positive_improvement_semantics() -> None:
    result = calculate_checkpoint_metrics(
        nav_by_variant={
            "hold_current": _points(("100", "100", "100", "100", "100")),
            "minimum_change": _points(("110", "99", "108.9", "108.9", "108.9")),
            "strategy_target": _points(("105", "103", "106", "104", "107")),
        },
        turnover_by_variant={
            "hold_current": Decimal("0"),
            "minimum_change": Decimal("0.8"),
            "strategy_target": Decimal("1.2"),
        },
        fees_by_variant={
            "hold_current": Decimal("0"),
            "minimum_change": Decimal("0.12"),
            "strategy_target": Decimal("0.18"),
        },
        ordinal=5,
    )

    assert tuple(item.variant for item in result) == SHADOW_VARIANTS
    minimum = result[1].metrics
    assert minimum.net_cumulative_return.value == Decimal("0.089000000000")
    assert minimum.return_delta_vs_hold.value == Decimal("0.089000000000")
    assert minimum.max_drawdown.value == Decimal("-0.100000000000")
    assert minimum.drawdown_improvement.value == Decimal("-0.100000000000")
    expected_volatility = canonical_decimal(Decimal("0.007").sqrt() * Decimal(252).sqrt())
    assert minimum.annualized_volatility.value == expected_volatility
    assert minimum.volatility_improvement.value == -expected_volatility
    assert minimum.cumulative_turnover.value == Decimal("0.800000000000")
    assert minimum.fee_drag_index_points.value == Decimal("0.120000000000")


def test_worst_sessions_sort_by_return_then_session_and_include_anchor_return() -> None:
    result = calculate_checkpoint_metrics(
        nav_by_variant={
            variant: _points(("110", "99", "108.9", "108.9", "108.9"))
            for variant in SHADOW_VARIANTS
        },
        turnover_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        ordinal=5,
    )

    worst = result[0].metrics.worst_sessions
    assert tuple(item.session for item in worst) == (
        _sessions()[1],
        _sessions()[3],
        _sessions()[4],
        _sessions()[0],
        _sessions()[2],
    )
    assert worst[0].return_value == Decimal("-0.100000000000")
    assert worst[3].return_value == Decimal("0.100000000000")


def test_constant_nav_has_zero_sample_volatility_and_drawdown() -> None:
    result = calculate_checkpoint_metrics(
        nav_by_variant={
            variant: _points(("100", "100", "100", "100", "100")) for variant in SHADOW_VARIANTS
        },
        turnover_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        ordinal=5,
    )

    for item in result:
        assert item.metrics.annualized_volatility.value == Decimal("0.000000000000")
        assert item.metrics.max_drawdown.value == Decimal("0.000000000000")


@pytest.mark.parametrize("returns", [(), (Decimal("0.1"),)])
def test_fewer_than_two_returns_have_zero_volatility(returns: tuple[Decimal, ...]) -> None:
    assert annualized_volatility(returns) == Decimal("0")


@pytest.mark.parametrize("ordinal", [5, 21, 63])
def test_all_checkpoint_ordinals_use_exactly_their_observation_window(ordinal: int) -> None:
    start = date(2026, 1, 5)
    points = tuple(
        ShadowNavPoint(session=start + timedelta(days=index), nav=Decimal("100"))
        for index in range(ordinal)
    )

    result = calculate_checkpoint_metrics(
        nav_by_variant={variant: points for variant in SHADOW_VARIANTS},
        turnover_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        ordinal=ordinal,  # type: ignore[arg-type]
    )

    assert all(item.metrics.net_cumulative_return.value == Decimal("0") for item in result)


def test_incomplete_checkpoint_returns_unknown_instead_of_zero() -> None:
    incomplete = _points(("100", "101", "102", "103"))
    result = calculate_checkpoint_metrics(
        nav_by_variant={variant: incomplete for variant in SHADOW_VARIANTS},
        turnover_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        fees_by_variant={variant: Decimal("0") for variant in SHADOW_VARIANTS},
        ordinal=5,
    )

    for item in result:
        metric = item.metrics.net_cumulative_return
        assert metric.status == "unknown"
        assert metric.value is None
        assert metric.reason == "SHADOW_CHECKPOINT_INCOMPLETE"
        assert item.metrics.worst_sessions == ()
