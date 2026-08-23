# ruff: noqa: E501
"""Behavioral contracts for deterministic proposal construction."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.optimizer.models import OptimizationPolicy, build_proposal_group
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot


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
