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
    assert result.excluded_variants == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )


def test_comparison_excludes_partial_stale_and_incomplete_evidence() -> None:
    result = compare_proposals(
        (
            ComparableProposal("hold_current", 1, Decimal("0"), Decimal("0"), False),
            ComparableProposal(
                "minimum_change", 0, Decimal("0.1"), Decimal("0.01"), False, stale=True
            ),
            ComparableProposal(
                "strategy_target",
                0,
                Decimal("0.1"),
                Decimal("0.01"),
                False,
                evidence_complete=False,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.ranked_variants == ("hold_current",)
    assert result.excluded_variants == ("minimum_change", "strategy_target")
