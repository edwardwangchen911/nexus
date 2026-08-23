# ruff: noqa: E501
"""Behavioral contracts for deterministic proposal construction."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.optimizer.models import (
    OptimizationPolicy,
    OptimizationProposal,
    OptimizationProposalGroup,
    ProposalPosition,
    build_proposal_group,
)
from nexus.optimizer.strategy_target import (
    DeterministicSelectionAuthority,
    SelectionCandidate,
)
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot
from nexus.strategy.intents import ResearchIntent


def test_group_is_atomic_and_incomplete_strategy_target_is_a_noop() -> None:
    """Removing a variant or inventing a target without authority must fail this contract."""
    snapshot = PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000101"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000102"),
        status="READY",
        cash=(CashBalance(currency="USD", cash_balance=Decimal("0"), available_cash=Decimal("0")),),
        positions=(
            BrokerPosition(
                instrument_id=UUID("0198d500-1000-7000-8000-000000000103"),
                currency="USD",
                quantity=Decimal("10"),
                available_quantity=Decimal("10"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("10"),
                diluted_cost=Decimal("10"),
                current_price=Decimal("10"),
            ),
        ),
        open_orders=(),
        as_of=datetime(2026, 8, 24, tzinfo=UTC),
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=snapshot.as_of,
        policy_version="real_advisory_policy_v1",
        optimization_need="OPTIMIZATION_CANDIDATE",
        items=(),
    )

    group = build_proposal_group(snapshot, report, (), OptimizationPolicy.v1())

    assert tuple(proposal.variant for proposal in group.proposals) == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )
    assert {proposal.baseline_portfolio_snapshot_id for proposal in group.proposals} == {
        snapshot.id
    }
    assert group.proposals[0].turnover == Decimal("0")
    assert group.proposals[2].analysis_only is True
    assert group.proposals[2].positions[0].target_weight == Decimal("1")


def test_group_rejects_reused_child_identifier() -> None:
    """Three variant labels do not make a group immutable if they point at one child row."""
    baseline = UUID("0198d500-1000-7000-8000-000000000121")
    position = ProposalPosition(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000122"),
        current_weight="1",
        target_weight="1",
        action="maintain",
        reason_code="BASELINE_WEIGHT",
    )
    proposal = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=baseline,
        positions=(position,),
        cash_current_weight="0",
        cash_target_weight="0",
        analysis_only=True,
        strategy_evidence_complete=False,
    )
    with pytest.raises(ValueError, match="unique child identifiers"):
        OptimizationProposalGroup(
            baseline_portfolio_snapshot_id=baseline,
            proposals=(
                proposal,
                proposal.model_copy(update={"variant": "minimum_change"}),
                proposal.model_copy(update={"variant": "strategy_target"}),
            ),
        )


def test_research_can_veto_a_deterministic_addition_but_cannot_create_one() -> None:
    """A watch intent may remove a policy addition; its prose never supplies another target."""
    existing = UUID("0198d500-1000-7000-8000-000000000131")
    candidate = UUID("0198d500-1000-7000-8000-000000000132")
    data_snapshot = UUID("0198d500-1000-7000-8000-000000000133")
    clock = datetime(2026, 8, 24, tzinfo=UTC)
    snapshot = PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000134"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000135"),
        status="READY",
        cash=(CashBalance(currency="USD", cash_balance="0", available_cash="0"),),
        positions=(
            BrokerPosition(
                instrument_id=existing,
                currency="USD",
                quantity="1",
                available_quantity="1",
                frozen_quantity="0",
                avg_cost="1",
                diluted_cost="1",
                current_price="1",
            ),
        ),
        open_orders=(),
        as_of=clock,
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=clock,
        policy_version="real_advisory_policy_v1",
        optimization_need="OPTIMIZATION_CANDIDATE",
        items=(),
    )
    authority = DeterministicSelectionAuthority(
        policy_version="baseline_v1",
        decision_at=clock,
        data_snapshot_id=data_snapshot,
        available_at=clock,
        evidence_refs=("selection:fixture",),
        candidates=(SelectionCandidate(candidate, ("candidate:fixture",)),),
        selected_instrument_ids=(candidate,),
        evidence_hash="a" * 64,
    )
    evidence = {
        "source": "fixture",
        "object_id": "research:fixture",
        "available_at": clock,
        "location": "fixture://research",
        "content_hash": "b" * 64,
    }
    intent = ResearchIntent.model_validate(
        {
            "schema_version": "research_intent_v1",
            "decision_id": "0198d500-1000-7000-8000-000000000136",
            "instrument_id": str(candidate),
            "display_symbol": "NEW",
            "decision_time": clock,
            "data_snapshot_id": data_snapshot,
            "horizon": "medium_term",
            "stance": "watch",
            "exposure_action": "none",
            "theses": [],
            "counter_theses": [],
            "uncertainties": ["authority incomplete"],
            "invalidation_conditions": [],
            "entry_conditions": [],
            "exit_conditions": [],
            "valid_until": datetime(2026, 8, 25, tzinfo=UTC),
            "quality_status": "complete",
            "evidence_audit": "complete",
            "role_votes": {"auditor": "abstain"},
            "disagreement": "watch",
            "strategy_version": "baseline_v1",
            "model_version": "fixture_v1",
            "prompt_version": "fixture_v1",
            "data_version": "fixture_v1",
            "evidence_refs": [evidence],
        }
    )

    group = build_proposal_group(
        snapshot,
        report,
        (intent,),
        OptimizationPolicy.v1(),
        selection_authority=authority,
    )
    strategy = group.proposals[2]
    assert next(item for item in strategy.positions if item.instrument_id == candidate).target_weight == 0
