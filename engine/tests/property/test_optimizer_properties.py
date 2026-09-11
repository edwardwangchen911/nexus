"""Property contracts for deterministic and non-executable optimization paths."""

from decimal import Decimal
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.optimizer.compare import (
    ComparableProposal,
    ComparisonDimensions,
    TypedMetric,
    compare_proposals,
)
from nexus.optimizer.minimum_change import ReductionCandidate, reduce_until_satisfied
from nexus.optimizer.strategy_target import build_strategy_target


def _dimensions(violations: int, cost: Decimal | None) -> ComparisonDimensions:
    known = TypedMetric.known
    return ComparisonDimensions(
        hard_violations=violations,
        top1=known("0.5"),
        top3=known("0.6"),
        hhi=known("0.4"),
        sector_exposure=known("0.3"),
        currency_exposure=known("0.8"),
        volatility=known("0.2"),
        drawdown=known("0.2"),
        correlation=known("0.5"),
        liquidity=known("1"),
        turnover=known("0"),
        cost=TypedMetric.unknown("COST_UNKNOWN") if cost is None else known(cost),
    )


@given(st.integers(min_value=1, max_value=10_000))
def test_one_basis_point_reductions_are_exact_and_reduce_only(step_count: int) -> None:
    """Every reduction releases exact Decimal cash and cannot increase an instrument."""
    instrument_id = UUID("0198d500-1000-7000-8000-000000000801")
    current_weight = Decimal(step_count) / Decimal("10000")
    result = reduce_until_satisfied(
        (
            ReductionCandidate(
                instrument_id, current_weight, current_weight, 1, Decimal("1")
            ),
        ),
        excess_weight=current_weight,
    )
    position = result.positions[0]
    assert position.target_weight == Decimal("0")
    assert position.target_weight <= position.current_weight
    assert result.cash_released == current_weight


@given(st.integers(min_value=1, max_value=10_000))
def test_missing_selection_authority_is_same_weight_analysis_only(step_count: int) -> None:
    """A missing baseline authority must never manufacture a selected target."""
    instrument_id = UUID("0198d500-1000-7000-8000-000000000802")
    weight = Decimal(step_count) / Decimal("10000")
    result = build_strategy_target({instrument_id: weight}, authority=None)
    assert result.evidence_complete is False
    assert result.targets == {instrument_id: weight}


@given(st.decimals(min_value=0, max_value=1, places=4))
def test_unknown_cost_is_never_ranked_as_zero(cost: Decimal) -> None:
    """Unknown-cost alternatives remain excluded regardless of known hold cost."""
    result = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(1, cost)),
            ComparableProposal("minimum_change", _dimensions(0, None)),
            ComparableProposal("strategy_target", _dimensions(0, None)),
        )
    )
    assert result.recommendation is None
    assert result.ranked_variants == ("hold_current",)
    assert result.excluded_variants == ("minimum_change", "strategy_target")
    assert result.status == "incomparable"
    assert result.reasons == (
        "COST_EVIDENCE_INCOMPLETE",
        "COMPARISON_EVIDENCE_INCOMPLETE",
    )
