# ruff: noqa: E501
"""Unknown-safe deterministic proposal comparison."""

import pytest
from nexus.optimizer.compare import (
    ComparableProposal,
    ComparisonDimensions,
    TypedMetric,
    compare_proposals,
)


def test_typed_metric_rejects_binary_float() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        TypedMetric(status="known", value=0.1)


def _dimensions(
    *,
    violations: int,
    top1: str,
    drawdown: str,
    cost: str,
    unknown: str | None = None,
) -> ComparisonDimensions:
    metric = TypedMetric.known
    values = {
        "top1": metric(top1),
        "top3": metric("0.6"),
        "hhi": metric("0.4"),
        "sector_exposure": metric("0.3"),
        "currency_exposure": metric("0.8"),
        "volatility": metric("0.2"),
        "drawdown": metric(drawdown),
        "correlation": metric("0.5"),
        "liquidity": metric("10"),
        "turnover": metric("0.1"),
        "cost": metric(cost),
    }
    if unknown is not None:
        values[unknown] = TypedMetric.unknown("NOT_PROVEN")
    return ComparisonDimensions(hard_violations=violations, **values)


def test_comparison_requires_every_typed_dimension_and_no_risk_deterioration() -> None:
    """A concentration improvement cannot rank with unknown or worse drawdown authority."""
    result = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal("minimum_change", _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01")),
            ComparableProposal("strategy_target", _dimensions(violations=0, top1="0.3", drawdown="0.3", cost="0.02")),
        )
    )
    assert result.recommendation == "minimum_change"
    assert result.ranked_variants == ("minimum_change", "hold_current")
    assert result.excluded_variants == ("strategy_target",)

    unknown = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal("minimum_change", _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01", unknown="liquidity")),
            ComparableProposal("strategy_target", _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.02", unknown="drawdown")),
        )
    )
    assert unknown.recommendation == "hold_current"
    assert unknown.excluded_variants == ("minimum_change", "strategy_target")
    assert unknown.tax_impact.status == "unknown"


def test_comparison_excludes_unknown_cost_from_ranking_and_can_recommend_hold() -> None:
    """Coercing unknown costs to zero would make an unsafe alternative win the ranking."""
    result = compare_proposals(
        (
            ComparableProposal(
                "hold_current",
                _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0", unknown="cost"),
            ),
            ComparableProposal(
                "minimum_change",
                _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01", unknown="cost"),
            ),
            ComparableProposal(
                "strategy_target",
                _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.01"),
                requires_fx=True,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.tax_impact.status == "unknown"
    assert result.ranked_variants == ()
    assert result.excluded_variants == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )


def test_comparison_excludes_partial_stale_and_incomplete_evidence() -> None:
    result = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal(
                "minimum_change",
                _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01"),
                stale=True,
            ),
            ComparableProposal(
                "strategy_target",
                _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.01"),
                evidence_complete=False,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.ranked_variants == ("hold_current",)
    assert result.excluded_variants == ("minimum_change", "strategy_target")
