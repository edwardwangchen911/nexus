"""Read-time economic labels must not turn incomplete evidence into a hold decision."""

from uuid import uuid4

import pytest
from helpers.optimization import complete_comparison_metrics
from nexus.api.routes.optimization import _economic_comparison
from nexus.api.schemas.optimization import OptimizationProposalResponse
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord


def comparison_case() -> tuple[OptimizationProposalGroupRecord, list[OptimizationProposalResponse]]:
    group = OptimizationProposalGroupRecord(
        diagnostic_report_id=uuid4(), data_snapshot_id=uuid4(), source_policy_id=uuid4(),
        policy_id=uuid4(), cost_policy_id=COST_POLICY_ID, portfolio_scope="internal_paper",
        baseline_ready=True, diagnostic_complete=True, requires_fx=False,
        cost_complete=True, authority_stale=False, partial=False, source_conflict=False,
    )
    proposals = []
    for variant in ("hold_current", "minimum_change", "strategy_target"):
        metrics = complete_comparison_metrics(
            report_id=group.diagnostic_report_id, data_id=group.data_snapshot_id,
            source_id=group.source_policy_id, optimizer_id=group.policy_id,
            cost_id=group.cost_policy_id,
        )
        metrics.pop("cost")  # Legacy persistence-only convenience field is not in the API.
        proposals.append(OptimizationProposalResponse.model_validate({
            "proposal_id": uuid4(), "variant": variant, "analysis_only": False,
            "strategy_evidence_complete": True, "portfolio_scope": "internal_paper",
            "positions": [], "comparison": {
                "metrics": metrics,
                "tax_impact": "unknown", "requires_fx": False,
                "non_executable": False, "cost_complete": True,
            },
        }))
    return group, proposals


def test_costed_risk_improvement_requires_policy_without_rewriting_persisted_flags() -> None:
    group, proposals = comparison_case()
    for index, proposal in enumerate(proposals):
        payload = proposal.model_dump()
        dimensions = payload["comparison"]["metrics"]["dimensions"]
        dimensions["top1"]["value"] = "0.5" if proposal.variant == "hold_current" else "0.2"
        dimensions["cost"]["value"] = "0" if proposal.variant == "hold_current" else "0.001"
        proposals[index] = OptimizationProposalResponse.model_validate(payload)
    before = [proposal.model_dump() for proposal in proposals]
    assert _economic_comparison(group, proposals).model_dump() == {
        "status": "policy_required", "recommendation": None,
        "reasons": ("ECONOMIC_COMPARISON_POLICY_MISSING",),
    }
    assert [proposal.model_dump() for proposal in proposals] == before


@pytest.mark.parametrize(
    "missing", ["different_authority", "strategy", "partial", "legacy_real_cost"]
)
def test_incomplete_or_foreign_evidence_does_not_recommend_hold(missing: str) -> None:
    group, proposals = comparison_case()
    if missing == "different_authority":
        payload = proposals[1].model_dump()
        payload["comparison"]["metrics"]["authority"]["data_snapshot_id"] = uuid4()
        proposals[1] = OptimizationProposalResponse.model_validate(payload)
    elif missing == "strategy":
        proposals[2] = proposals[2].model_copy(update={"strategy_evidence_complete": False})
    elif missing == "partial":
        group.partial = True
    else:
        group.portfolio_scope = "real_read_only"
        proposals = [p.model_copy(update={"portfolio_scope": "real_read_only"}) for p in proposals]
    result = _economic_comparison(group, proposals)
    assert result.status == "incomparable"
    assert result.recommendation is None
    assert "COMPARISON_EVIDENCE_INCOMPLETE" in result.reasons
    if missing == "legacy_real_cost":
        assert "COST_EVIDENCE_INCOMPLETE" in result.reasons
