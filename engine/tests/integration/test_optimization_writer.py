"""Real PostgreSQL persistence contracts for complete proposal groups."""

import asyncio
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
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.optimizer.models import OptimizationProposal, OptimizationProposalGroup, ProposalPosition
from nexus.optimizer.persistence import (
    ProposalComparisonInput,
    ProposalPersistenceAuthority,
    persist_proposal_group,
)
from nexus.optimizer.production import create_proposal_group_idempotently
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

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


@pytest.mark.asyncio
async def test_writer_rolls_back_every_row_when_a_late_position_is_invalid(
    migrated_database_url: str,
) -> None:
    """An injected child failure must not leave a parent, child, position, or comparison behind."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority = await _seed_authority(factory)
    group = _group().model_copy(
        update={
            "proposals": (
                _group().proposals[0].model_copy(
                    update={
                        "positions": (
                            ProposalPosition(
                                instrument_id=UUID("0198d500-1000-7000-8000-000000009999"),
                                current_weight=Decimal("1"),
                                target_weight=Decimal("1"),
                                action="maintain",
                                reason_code="INJECTED_FAILURE",
                            ),
                        )
                    }
                ),
                _group().proposals[1],
                _group().proposals[2],
            )
        }
    )
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            await persist_proposal_group(session, group, authority, _comparisons())
    async with factory() as session:
        group_count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
        proposal_count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalRecord)
        )
        position_count = await session.scalar(
            select(func.count()).select_from(ProposalPositionRecord)
        )
        comparison_count = await session.scalar(
            select(func.count()).select_from(ProposalComparisonRecord)
        )
    assert (group_count, proposal_count, position_count, comparison_count) == (0, 0, 0, 0)


@pytest.mark.asyncio
async def test_idempotent_group_writer_has_one_concurrent_identity_and_conflicts_on_hash(
    migrated_database_url: str,
) -> None:
    """Two sessions with one key/hash commit exactly one complete three-way group."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority = await _seed_authority(factory)
    group = _group()
    barrier = asyncio.Barrier(2)

    async def create() -> object:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await create_proposal_group_idempotently(
                session,
                actor_id="optimizer-race",
                idempotency_key="group-race",
                request_hash="a" * 64,
                group=group,
                authority=authority,
                comparisons=_comparisons(),
            )

    first, second = await asyncio.wait_for(asyncio.gather(create(), create()), timeout=5)
    assert first.group_id == second.group_id == group.proposal_group_id
    assert {first.replayed, second.replayed} == {False, True}
    with pytest.raises(IdempotencyConflict):
        async with factory() as session, session.begin():
            await create_proposal_group_idempotently(
                session,
                actor_id="optimizer-race",
                idempotency_key="group-race",
                request_hash="b" * 64,
                group=group,
                authority=authority,
                comparisons=_comparisons(),
            )
    async with factory() as session:
        group_count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
        proposal_count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalRecord)
        )
        comparison_count = await session.scalar(
            select(func.count()).select_from(ProposalComparisonRecord)
        )
    assert (group_count, proposal_count, comparison_count) == (1, 3, 3)


async def _seed_authority(factory) -> ProposalPersistenceAuthority:
    data_snapshot_id = UUID("0198d500-1000-7000-8000-000000000901")
    source_policy_id = SOURCE_POLICY_ID
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
        cost_policy_id=COST_POLICY_ID,
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


def _comparisons() -> tuple[
    ProposalComparisonInput, ProposalComparisonInput, ProposalComparisonInput
]:
    return tuple(
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
    )  # type: ignore[return-value]
