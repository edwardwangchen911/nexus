"""Compact typed comparison fixtures shared by optimization integration tests."""

from uuid import UUID


def complete_comparison_metrics(
    *, report_id: UUID, data_id: UUID, source_id: UUID, optimizer_id: UUID, cost_id: UUID
) -> dict[str, object]:
    known = {"status": "known", "value": "0", "reason": None}
    dimensions = {
        name: dict(known)
        for name in (
            "top1", "top3", "hhi", "sector_exposure", "currency_exposure",
            "volatility", "drawdown", "correlation", "liquidity", "turnover", "cost",
        )
    }
    dimensions.update(
        hard_violations=0,
        tax_impact={"status": "unknown", "value": None, "reason": "TAX_DATA_UNAVAILABLE"},
    )
    return {
        "cost": "0", "ranking_eligible": True, "unknown_metrics": [],
        "constraint_complete": True, "eligibility_complete": True,
        "evidence_complete": True, "stale": False, "dimensions": dimensions,
        "authority": {
            "diagnostic_report_id": str(report_id), "data_snapshot_id": str(data_id),
            "source_policy_id": str(source_id), "optimizer_policy_id": str(optimizer_id),
            "cost_policy_id": str(cost_id),
        },
    }
