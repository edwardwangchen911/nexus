"""Exact Decimal reduce-only minimum-change heuristic contracts."""

from decimal import Decimal
from uuid import UUID

from nexus.optimizer.minimum_change import ReductionCandidate, reduce_until_satisfied


def test_minimum_change_reduces_one_bp_using_severity_liquidity_then_id_tie_break() -> None:
    """Changing the tie-break or allowing an increase must fail deterministic replay."""
    first = UUID("0198d500-1000-7000-8000-000000000201")
    second = UUID("0198d500-1000-7000-8000-000000000202")
    result = reduce_until_satisfied(
        (
            ReductionCandidate(first, Decimal("0.20"), Decimal("0.20"), 2, Decimal("5")),
            ReductionCandidate(second, Decimal("0.20"), Decimal("0.20"), 2, Decimal("5")),
        ),
        excess_weight=Decimal("0.0002"),
    )

    assert result.positions[0].instrument_id == first
    assert result.positions[0].target_weight == Decimal("0.1998")
    assert result.positions[1].target_weight == Decimal("0.20")
    assert result.cash_released == Decimal("0.0002")


def test_concentration_rechecks_top_members_and_preserves_cash_for_both_limits() -> None:
    from nexus.optimizer.minimum_change import reduce_to_concentration_limits

    candidates = tuple(
        ReductionCandidate(
            UUID(int=index + 1),
            Decimal(weight),
            Decimal(weight),
            2,
            Decimal("1"),
        )
        for index, weight in enumerate(("0.5", "0.2", "0.1", "0.1", "0.1"))
    )
    limits = ((1, Decimal("0.30")), (3, Decimal("0.60")))
    result = reduce_to_concentration_limits(candidates, limits)
    weights = sorted((p.target_weight for p in result.positions), reverse=True)
    assert weights[0] <= Decimal("0.30") * sum(weights)
    assert sum(weights[:3]) <= Decimal("0.60") * sum(weights)
    assert sum(weights) + result.cash_released == 1
    assert result == reduce_to_concentration_limits(tuple(reversed(candidates)), limits)
    assert sum(p.target_weight < p.current_weight for p in result.positions) >= 2


def test_concentration_does_not_clear_tiny_portfolio_to_claim_zero_risk() -> None:
    import pytest
    from nexus.optimizer.minimum_change import reduce_to_concentration_limits

    candidates = tuple(
        ReductionCandidate(
            UUID(int=index + 1),
            Decimal(weight),
            Decimal(weight),
            2,
            Decimal("1"),
        )
        for index, weight in enumerate(("0.00009", "0.00003", "0.00003", "0.00003"))
    )
    with pytest.raises(ValueError, match="one-bp precision"):
        reduce_to_concentration_limits(candidates, ((1, Decimal("0.30")),))
