# ruff: noqa: E501
"""Unknown-safe deterministic proposal comparison."""

from decimal import Decimal

from nexus.optimizer.compare import ComparableProposal, compare_proposals


def test_comparison_excludes_unknown_cost_from_ranking_and_can_recommend_hold() -> None:
    """Coercing unknown costs to zero would make an unsafe alternative win the ranking."""
    result = compare_proposals(
        (
            ComparableProposal(
                "hold_current", violations=1, turnover=Decimal("0"), cost=None, requires_fx=False
            ),
            ComparableProposal(
                "minimum_change",
                violations=0,
                turnover=Decimal("0.1"),
                cost=None,
                requires_fx=False,
            ),
            ComparableProposal(
                "strategy_target",
                violations=0,
                turnover=Decimal("0.2"),
                cost=Decimal("0.01"),
                requires_fx=True,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.tax_impact == "unknown"
    assert result.ranked_variants == ()
