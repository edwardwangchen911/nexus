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
