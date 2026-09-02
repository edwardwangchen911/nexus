"""Research reads economic uncertainty without changing its frozen comparison facts."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.agents.proposal_context import FrozenProposalComparison


@pytest.mark.parametrize("partial", [False, True])
def test_frozen_comparison_reports_tradeoff_or_missing_authority(partial: bool) -> None:
    from nexus.agents.proposal_context import assess_frozen_comparison

    variants = []
    for variant, weight, cost in (
        ("hold_current", "0.5", "0"),
        ("minimum_change", "0.4", "0.001"),
        ("strategy_target", "0.6", "0.01"),
    ):
        dimensions = {
            name: {"status": "known", "value": "0", "reason": None}
            for name in (
                "top1",
                "top3",
                "hhi",
                "sector_exposure",
                "currency_exposure",
                "volatility",
                "drawdown",
                "correlation",
                "liquidity",
                "turnover",
                "cost",
            )
        }
        dimensions["top1"]["value"] = weight
        dimensions["cost"]["value"] = cost
        variants.append(
            {
                "variant": variant,
                "dimensions": {
                    **dimensions,
                    "hard_violations": 0,
                    "tax_impact": {"status": "unknown", "reason": "TAX_DATA_UNAVAILABLE"},
                },
                "non_executable": partial,
                "requires_fx": False,
                "cost_complete": True,
            }
        )
    frozen = FrozenProposalComparison.model_validate(
        {
            **{
                name: UUID(int=1)
                for name in (
                    "proposal_group_id",
                    "data_snapshot_id",
                    "source_policy_id",
                    "cost_policy_id",
                    "optimizer_policy_id",
                )
            },
            "valuation_time": datetime(2026, 1, 1, tzinfo=UTC),
            "frozen_at": datetime(2026, 1, 2, tzinfo=UTC),
            "partial": partial,
            "source_conflict": False,
            "authority_stale": False,
            "diagnostic_complete": not partial,
            "variants": variants,
        }
    )
    before = frozen.model_dump_json()
    result = assess_frozen_comparison(frozen)
    assert result["recommendation"] is None
    assert result["status"] == ("incomparable" if partial else "policy_required")
    assert result["reasons"] == [
        "COMPARISON_EVIDENCE_INCOMPLETE" if partial else "ECONOMIC_COMPARISON_POLICY_MISSING"
    ]
    assert frozen.model_dump_json() == before
