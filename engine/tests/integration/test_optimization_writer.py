"""Real PostgreSQL persistence contracts for complete proposal groups."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    PORTFOLIOS,
    app,
    headers,
    seed_portfolio_list,
    settings,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.models import OptimizationProposal, OptimizationProposalGroup, ProposalPosition
from nexus.optimizer.persistence import (
    ProposalComparisonInput,
    ProposalPersistenceAuthority,
    persist_proposal_group,
)
from nexus.persistence.models.data import DataSnapshot, SourcePolicy
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
_OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_CLOCK = datetime(2026, 8, 24, tzinfo=UTC)


@pytest.mark.asyncio
async def test_writer_commits_exact_group_children_positions_and_comparisons_atomically(
    migrated_database_url: str,
) -> None:
    """A missing child/comparison or failed position insert must leave no partial group."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority = await _seed_authority(factory)
    group = _group()
    comparisons = tuple(
        ProposalComparisonInput(
            variant=variant,
            violation_count=0,
            turnover=Decimal("0"),
            cost=Decimal("0"),
            unknown_metrics=(),
            requires_fx=False,
            source_conflict=False,
            partial=False,
            cost_complete=True,
        )
        for variant in ("hold_current", "minimum_change", "strategy_target")
    )
    async with factory() as session, session.begin():
        await persist_proposal_group(session, group, authority, comparisons)  # type: ignore[arg-type]
    async with factory() as session:
        children = tuple(
            await session.scalars(
                select(OptimizationProposalRecord)
                .where(OptimizationProposalRecord.proposal_group_id == group.proposal_group_id)
                .order_by(OptimizationProposalRecord.variant)
            )
        )
        positions = await session.scalar(select(func.count()).select_from(ProposalPositionRecord))
        comparisons_count = await session.scalar(
            select(func.count()).select_from(ProposalComparisonRecord)
        )
    assert tuple(child.variant for child in children) == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )
    assert (positions, comparisons_count) == (3, 3)
    path = f"/internal/v1/optimization-proposals/{group.proposal_group_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["version"] == 0
    assert tuple(item["variant"] for item in body["proposals"]) == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )
    assert all(len(item["positions"]) == 1 for item in body["proposals"])
    assert all(item["comparison"]["tax_impact"] == "unknown" for item in body["proposals"])


async def _seed_authority(factory) -> ProposalPersistenceAuthority:
    data_snapshot_id = UUID("0198d500-1000-7000-8000-000000000901")
    source_policy_id = UUID("0198d500-1000-7000-8000-000000000902")
    report_id = UUID("0198d500-1000-7000-8000-000000000903")
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=data_snapshot_id,
                source="fixture",
                provider_object_id="task12-data",
                revision="1",
                observed_at=_CLOCK,
                available_at=_CLOCK,
                received_at=_CLOCK,
                schema_version="1",
                payload={},
            )
        )
        session.add(
            SourcePolicy(
                id=source_policy_id,
                key="source_policy",
                version="source_policy_v1",
                schema_version="1",
                payload={},
            )
        )
        session.add(
            PortfolioDiagnosticReportRecord(
                id=report_id,
                portfolio_id=PORTFOLIOS[0],
                portfolio_snapshot_id=_SNAPSHOT_ID,
                snapshot_as_of=_CLOCK,
                trading_day=date(2026, 8, 24),
                policy_id=REAL_ADVISORY_POLICY_ID,
                policy_version="real_advisory_policy_v1",
                optimization_need="OPTIMIZATION_CANDIDATE",
                diagnostic_input_hash="d" * 64,
            )
        )
    return ProposalPersistenceAuthority(
        diagnostic_report_id=report_id,
        data_snapshot_id=data_snapshot_id,
        source_policy_id=source_policy_id,
        optimizer_policy_id=_OPTIMIZER_POLICY_ID,
        cost_model_version="cost_model_v1",
        diagnostic_input_hash="d" * 64,
        valuation_time=_CLOCK,
        baseline_ready=True,
        diagnostic_complete=True,
        source_conflict=False,
        partial=False,
        requires_fx=False,
        cost_complete=True,
    )


def _group() -> OptimizationProposalGroup:
    position = ProposalPosition(
        instrument_id=INSTRUMENT_ID,
        current_weight=Decimal("1"),
        target_weight=Decimal("1"),
        action="maintain",
        reason_code="BASELINE_WEIGHT",
    )
    proposal = OptimizationProposal(
        variant="hold_current",
        baseline_portfolio_snapshot_id=_SNAPSHOT_ID,
        positions=(position,),
        cash_current_weight=Decimal("0"),
        cash_target_weight=Decimal("0"),
        analysis_only=False,
        strategy_evidence_complete=True,
    )
    return OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=_SNAPSHOT_ID,
        valuation_time=_CLOCK,
        proposals=(
            proposal,
            proposal.model_copy(
                update={
                    "variant": "minimum_change",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000911"),
                }
            ),
            proposal.model_copy(
                update={
                    "variant": "strategy_target",
                    "proposal_id": UUID("0198d500-1000-7000-8000-000000000912"),
                }
            ),
        ),
    )
