import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    ORDER_INSTRUMENT_ID,
    PORTFOLIOS,
    app,
    headers,
    seed_portfolio_list,
    settings,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.compare import ComparisonDimensions, TypedMetric
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
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
_OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_CLOCK = datetime(2026, 8, 24, tzinfo=UTC)
_SELECTION_ID = UUID("0198d500-1000-7000-8000-000000000904")


def _known_dimensions() -> ComparisonDimensions:
    metric = TypedMetric.known
    return ComparisonDimensions(
        hard_violations=0,
        top1=metric("1"), top3=metric("1"), hhi=metric("1"),
        sector_exposure=metric("1"), currency_exposure=metric("1"),
        volatility=metric("1"), drawdown=metric("1"), correlation=metric("1"),
        liquidity=metric("1"), turnover=metric("0"), cost=metric("0"),
    )


@pytest.mark.asyncio
async def test_writer_commits_exact_group_children_positions_and_comparisons_atomically(
    migrated_database_url: str,
) -> None:
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
            dimensions=_known_dimensions(),
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
    for item in body["proposals"]:
        metrics = item["comparison"]["metrics"]
        assert set(metrics["dimensions"]) == {
            "hard_violations",
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
            "tax_impact",
        }
        assert metrics["authority"] == {
            "diagnostic_report_id": str(authority.diagnostic_report_id),
            "data_snapshot_id": str(authority.data_snapshot_id),
            "source_policy_id": str(authority.source_policy_id),
            "optimizer_policy_id": str(authority.optimizer_policy_id),
            "cost_policy_id": str(authority.cost_policy_id),
        }


@pytest.mark.asyncio
async def test_writer_rejects_strategy_without_bound_selection_atomically(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    seeded = await _seed_authority(factory)
    authority = replace(seeded, selection_authority_id=None, selection_evidence_hash=None)
    invalid = _group().model_copy(
        update={
            "proposals": (
                *_group().proposals[:2],
                _group().proposals[2].model_copy(update={"strategy_evidence_complete": True}),
            )
        }
    )
    with pytest.raises(ValueError, match="strategy target is not bound"):
        async with factory() as session, session.begin():
            await persist_proposal_group(session, invalid, authority, _comparisons())
    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
    assert count == 0


@pytest.mark.asyncio
async def test_writer_rejects_overweight_or_unselected_strategy_targets(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority = await _seed_authority(factory)
    strategy = _group().proposals[2]
    overweight_position = strategy.positions[0].model_copy(
        update={"target_weight": Decimal("0.126")}
    )
    overweight = strategy.model_copy(
        update={"positions": (overweight_position,), "cash_target_weight": Decimal("0.874")}
    )
    unrelated = strategy.model_copy(
        update={
            "positions": (
                strategy.positions[0].model_copy(
                    update={"target_weight": Decimal("0"), "action": "reduce"}
                ),
                ProposalPosition(
                    instrument_id=ORDER_INSTRUMENT_ID,
                    current_weight=Decimal("0"),
                    target_weight=Decimal("0.125"),
                    action="increase",
                    reason_code="UNBOUND_TARGET",
                ),
            ),
        }
    )
    negative = strategy.model_copy(
        update={"positions": (strategy.positions[0].model_copy(
            update={"target_weight": Decimal("-0.001")}
        ),)}
    )
    for invalid in (overweight, unrelated, negative):
        group = _group().model_copy(
            update={"proposals": (*_group().proposals[:2], invalid)}
        )
        with pytest.raises(ValueError, match="exceeds its bound selection"):
            async with factory() as session, session.begin():
                await persist_proposal_group(session, group, authority, _comparisons())
    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
    assert count == 0


@pytest.mark.asyncio
async def test_writer_rolls_back_every_row_when_a_late_position_is_invalid(
    migrated_database_url: str,
) -> None:
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


@pytest.mark.asyncio
async def test_composite_parent_identity_rejects_child_baseline_mismatch_and_parent_rewrite(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    authority = await _seed_authority(factory)
    group = _group()
    async with factory() as session, session.begin():
        await persist_proposal_group(session, group, authority, _comparisons())
    async with factory() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO optimization_proposals "
                        "(id, proposal_group_id, variant, baseline_portfolio_snapshot_id, "
                        "analysis_only, strategy_evidence_complete, portfolio_scope) "
                        "VALUES (:id, :group, 'hold_current', :baseline, false, true, "
                        "'internal_paper')"
                    ),
                    {
                        "id": UUID("0198d500-1000-7000-8000-000000000919"),
                        "group": group.proposal_group_id,
                        "baseline": UUID("0198d500-1000-7000-8000-000000000042"),
                    },
                )
        with pytest.raises(IntegrityError, match="optimization group authority is immutable"):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE optimization_proposal_groups "
                        "SET baseline_portfolio_snapshot_id = :baseline WHERE id = :group"
                    ),
                    {
                        "baseline": UUID("0198d500-1000-7000-8000-000000000042"),
                        "group": group.proposal_group_id,
                    },
                )
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text("DELETE FROM optimization_proposal_groups WHERE id = :group"),
                    {"group": group.proposal_group_id},
                )


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
        session.add(
            BaselineSelectionAuthorityRecord(
                id=_SELECTION_ID,
                policy_version="baseline_v1",
                data_snapshot_id=data_snapshot_id,
                decision_at=_CLOCK,
                available_at=_CLOCK,
                evidence_refs=["selection:writer"],
                candidates=[
                    {
                        "instrument_id": str(INSTRUMENT_ID),
                        "provenance_refs": ["candidate:writer"],
                    }
                ],
                selected_instrument_ids=[str(INSTRUMENT_ID)],
                evidence_hash="a" * 64,
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
        portfolio_scope="internal_paper",
        selection_authority_id=_SELECTION_ID,
        selection_evidence_hash="a" * 64,
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
        strategy_evidence_complete=False,
        portfolio_scope="internal_paper",
    )
    return OptimizationProposalGroup(
        baseline_portfolio_snapshot_id=_SNAPSHOT_ID,
        portfolio_scope="internal_paper",
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
                    "positions": (
                        position.model_copy(
                            update={
                                "target_weight": Decimal("0.125"),
                                "action": "reduce",
                                "reason_code": "BASELINE_SELECTION_AUTHORITY",
                            }
                        ),
                    ),
                    "cash_target_weight": Decimal("0.875"),
                    "strategy_evidence_complete": True,
                }
            ),
        ),
    )


def _comparisons() -> tuple[ProposalComparisonInput, ProposalComparisonInput,
                            ProposalComparisonInput]:
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
            dimensions=_known_dimensions(),
        )
        for variant in ("hold_current", "minimum_change", "strategy_target")
    )  # type: ignore[return-value]
