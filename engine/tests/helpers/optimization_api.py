"""Persisted three-way proposal fixtures for API contract tests."""

from datetime import UTC, date, datetime
from uuid import UUID

from helpers.optimization import complete_comparison_metrics
from helpers.portfolio_api import INSTRUMENT_ID, PORTFOLIOS
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy.ext.asyncio import AsyncSession

OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")


async def seed_readable_group(
    session: AsyncSession, *, position_count: int = 1
) -> tuple[UUID, UUID]:
    """Create one valid persisted three-way group and return its minimum-change proposal."""
    observed_at = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=PORTFOLIOS[0],
        portfolio_snapshot_id=SNAPSHOT_ID,
        snapshot_as_of=observed_at,
        trading_day=date(2026, 8, 24),
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version="real_advisory_policy_v1",
        optimization_need="OPTIMIZATION_CANDIDATE",
        diagnostic_input_hash="d" * 64,
    )
    data_snapshot = DataSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000701"),
        source="fixture",
        provider_object_id="proposal-read",
        revision="1",
        observed_at=observed_at,
        available_at=observed_at,
        received_at=observed_at,
        schema_version="1",
        payload={},
    )
    session.add_all((report, data_snapshot))
    instrument_ids = (INSTRUMENT_ID,) + tuple(
        UUID(int=9000 + index) for index in range(position_count - 1)
    )
    session.add_all(
        Instrument(
            id=instrument_id,
            asset_type="equity",
            exchange="XNYS",
            currency="USD",
            tick_size="0.01",
            risk_bucket="us_equity",
        )
        for instrument_id in instrument_ids[1:]
    )
    await session.flush()
    group = OptimizationProposalGroupRecord(
        baseline_portfolio_snapshot_id=SNAPSHOT_ID,
        portfolio_scope="internal_paper",
        status="compared",
        version=3,
        policy_id=OPTIMIZER_POLICY_ID,
        diagnostic_report_id=report.id,
        data_snapshot_id=data_snapshot.id,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        diagnostic_input_hash=report.diagnostic_input_hash,
        valuation_time=observed_at,
        baseline_ready=True,
        diagnostic_complete=True,
        source_conflict=False,
        partial=False,
        requires_fx=False,
        cost_complete=True,
        authority_stale=False,
    )
    session.add(group)
    await session.flush()
    minimum_change_id: UUID | None = None
    for variant in ("hold_current", "minimum_change", "strategy_target"):
        proposal = OptimizationProposalRecord(
            proposal_group_id=group.id,
            variant=variant,
            baseline_portfolio_snapshot_id=SNAPSHOT_ID,
            analysis_only=False,
            strategy_evidence_complete=True,
            portfolio_scope="internal_paper",
        )
        session.add(proposal)
        await session.flush()
        proposal_instruments = instrument_ids if variant != "strategy_target" else ()
        for instrument_id in proposal_instruments:
            session.add(
                ProposalPositionRecord(
                    proposal_id=proposal.id,
                    instrument_id=instrument_id,
                    current_weight="0.20",
                    target_weight="0.20",
                    action="maintain",
                    reason_code="fixture",
                )
            )
        session.add(
            ProposalComparisonRecord(
                proposal_id=proposal.id,
                metrics=complete_comparison_metrics(
                    report_id=report.id,
                    data_id=data_snapshot.id,
                    source_id=SOURCE_POLICY_ID,
                    optimizer_id=OPTIMIZER_POLICY_ID,
                    cost_id=COST_POLICY_ID,
                ),
                tax_impact="unknown",
                requires_fx=False,
                non_executable=False,
                cost_complete=True,
            )
        )
        if variant == "minimum_change":
            minimum_change_id = proposal.id
    assert minimum_change_id is not None
    return group.id, minimum_change_id
