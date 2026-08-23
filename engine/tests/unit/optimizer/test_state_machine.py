# ruff: noqa: E501
"""Proposal lifecycle contains no execution authorization."""

from uuid import UUID

import pytest
from nexus.optimizer.models import OptimizationProposal, OptimizationProposalGroup, ProposalPosition
from nexus.optimizer.state_machine import ProposalLifecycleError, transition_proposal_group


def test_lifecycle_requires_versions_and_rejects_incomplete_strategy_shadowing() -> None:
    """Allowing stale writes or shadowing incomplete strategy evidence would authorize unsafe work."""
    position = ProposalPosition(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000401"),
        current_weight="1",
        target_weight="1",
        action="maintain",
        reason_code="BASELINE",
    )
    baseline = UUID("0198d500-1000-7000-8000-000000000402")
    hold = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=baseline,
        positions=(position,),
        cash_current_weight="0",
        cash_target_weight="0",
        analysis_only=True,
        strategy_evidence_complete=False,
    )
    group = OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=baseline,
        proposals=(
            hold,
            hold.model_copy(
                update={
                    "variant": "minimum_change",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000403"),
                }
            ),
            hold.model_copy(
                update={
                    "variant": "strategy_target",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000404"),
                }
            ),
        ),
    )

    compared = transition_proposal_group(group, "compared", expected_version=0)
    assert compared.status == "compared"
    assert compared.version == 1
    with pytest.raises(ProposalLifecycleError, match="PROPOSAL_NOT_EXECUTABLE"):
        transition_proposal_group(compared, "shadowing", expected_version=1)
    with pytest.raises(ProposalLifecycleError, match="VERSION_CONFLICT"):
        transition_proposal_group(compared, "rejected", expected_version=0)


def test_shadow_requires_complete_fresh_and_cost_authority() -> None:
    """Strategy evidence alone must not make stale or unresolved-cost work shadowable."""
    position = ProposalPosition(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000411"),
        current_weight="1",
        target_weight="1",
        action="maintain",
        reason_code="BASELINE",
    )
    baseline = UUID("0198d500-1000-7000-8000-000000000412")
    proposal = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=baseline,
        positions=(position,),
        cash_current_weight="0",
        cash_target_weight="0",
        analysis_only=False,
        strategy_evidence_complete=True,
    )
    group = OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=baseline,
        proposals=(
            proposal,
            proposal.model_copy(
                update={
                    "variant": "minimum_change",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000413"),
                }
            ),
            proposal.model_copy(
                update={
                    "variant": "strategy_target",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000414"),
                }
            ),
        ),
        shadow_authority={
            "baseline_ready": True,
            "diagnostic_complete": True,
            "source_conflict": True,
            "partial": False,
            "requires_fx": False,
            "cost_complete": True,
        },
    )

    compared = transition_proposal_group(group, "compared", expected_version=0)
    with pytest.raises(ProposalLifecycleError, match="PROPOSAL_NOT_EXECUTABLE"):
        transition_proposal_group(compared, "shadowing", expected_version=1)
