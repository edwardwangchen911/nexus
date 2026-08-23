"""Production composition from frozen PostgreSQL authority."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    PORTFOLIOS,
    seed_portfolio_list,
    settings,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.optimizer.production import build_and_persist_proposal_group
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

_CLOCK = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
_DATA_ID = UUID("0198d500-1000-7000-8000-000000000b01")
_SELECTION_ID = UUID("0198d500-1000-7000-8000-000000000b02")
_ADMISSION_ID = UUID("0198d500-1000-7000-8000-000000000b03")
_OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")


async def _seed_authority(database_url: str, selected_id: UUID | None = INSTRUMENT_ID) -> None:
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=_DATA_ID,
                source="fixture",
                provider_object_id="builder-data",
                revision="1",
                observed_at=_CLOCK,
                available_at=_CLOCK,
                received_at=_CLOCK,
                schema_version="1",
                payload={},
            )
        )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=_SNAPSHOT_ID,
            snapshot_as_of=_CLOCK,
            trading_day=date(2026, 8, 24),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="OPTIMIZATION_CANDIDATE",
            diagnostic_input_hash="d" * 64,
        )
        session.add(report)
        await session.flush()
        item = PortfolioDiagnosticItemRecord(
            report_id=report.id,
            diagnostic_code="CONCENTRATION_TOP1",
            metric_value=Decimal("0.3002"),
            unit="ratio",
            reference_limit=Decimal("0.3000"),
            policy_scope="currency:USD",
            severity="critical",
            actionability="reduce_only",
            unknown_reason=None,
        )
        session.add(item)
        await session.flush()
        session.add(
            PortfolioDiagnosticEvidenceRefRecord(
                item_id=item.id,
                source="nexus_db",
                object_id="diagnostic:fixture",
                available_at=_CLOCK,
                location="db://diagnostics/fixture",
                content_hash="e" * 64,
            )
        )
        if selected_id is not None:
            session.add(
                BaselineSelectionAuthorityRecord(
                    id=_SELECTION_ID,
                    policy_version="baseline_v1",
                    data_snapshot_id=_DATA_ID,
                    decision_at=_CLOCK,
                    available_at=_CLOCK,
                    evidence_refs=["selection:fixture"],
                    candidates=[
                        {
                            "instrument_id": str(selected_id),
                            "provenance_refs": ["candidate:fixture"],
                        }
                    ],
                    selected_instrument_ids=[str(selected_id)],
                    evidence_hash="a" * 64,
                )
            )
        session.add(
            OptimizationAdmissionAuthorityRecord(
                id=_ADMISSION_ID,
                portfolio_snapshot_id=_SNAPSHOT_ID,
                diagnostic_report_id=report.id,
                data_snapshot_id=_DATA_ID,
                source_policy_id=SOURCE_POLICY_ID,
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
                authority_stale=False,
                selection_authority_id=None if selected_id is None else _SELECTION_ID,
                portfolio_scope="internal_paper",
            )
        )


@pytest.mark.asyncio
async def test_production_builder_uses_frozen_diagnostics_selection_and_atomic_writer(
    migrated_database_url: str,
) -> None:
    """Replacing real heuristic/target composition with cloned hold rows breaks this test."""
    await _seed_authority(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await build_and_persist_proposal_group(
            session,
            actor_id="builder",
            idempotency_key="builder-one",
            request_hash="1" * 64,
            admission_authority_id=_ADMISSION_ID,
            research=(),
        )
    async with factory() as session, session.begin():
        replay = await build_and_persist_proposal_group(
            session,
            actor_id="builder",
            idempotency_key="builder-one",
            request_hash="1" * 64,
            admission_authority_id=_ADMISSION_ID,
            research=(),
        )
    assert replay.group_id == result.group_id
    assert replay.replayed is True
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, result.group_id)
        children = tuple(
            await session.scalars(
                select(OptimizationProposalRecord)
                .where(OptimizationProposalRecord.proposal_group_id == result.group_id)
                .order_by(OptimizationProposalRecord.variant)
            )
        )
        positions = {
            child.variant: tuple(
                await session.scalars(
                    select(ProposalPositionRecord).where(
                        ProposalPositionRecord.proposal_id == child.id
                    )
                )
            )
            for child in children
        }
    assert group is not None
    assert (group.selection_authority_id, group.data_snapshot_id) == (_SELECTION_ID, _DATA_ID)
    minimum = positions["minimum_change"][0]
    strategy = positions["strategy_target"][0]
    assert minimum.target_weight == minimum.current_weight - Decimal("0.0002")
    assert strategy.target_weight == Decimal("0.125000000000")
    assert tuple(child.strategy_evidence_complete for child in children) == (False, False, True)


@pytest.mark.asyncio
async def test_production_builder_absent_selection_is_only_strategy_noop(
    migrated_database_url: str,
) -> None:
    """Missing selection authority must not prevent reduce-only mitigation or invent a target."""
    await _seed_authority(migrated_database_url, selected_id=None)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await build_and_persist_proposal_group(
            session,
            actor_id="builder",
            idempotency_key="builder-no-selection",
            request_hash="2" * 64,
            admission_authority_id=_ADMISSION_ID,
            research=(),
        )
    async with factory() as session:
        strategy = await session.scalar(
            select(OptimizationProposalRecord).where(
                OptimizationProposalRecord.proposal_group_id == result.group_id,
                OptimizationProposalRecord.variant == "strategy_target",
            )
        )
        assert strategy is not None
        comparison = await session.scalar(
            select(ProposalComparisonRecord).where(
                ProposalComparisonRecord.proposal_id == strategy.id
            )
        )
        strategy_position = await session.scalar(
            select(ProposalPositionRecord).where(
                ProposalPositionRecord.proposal_id == strategy.id
            )
        )
    assert strategy.strategy_evidence_complete is False
    assert strategy_position is not None
    assert strategy_position.current_weight == strategy_position.target_weight
    assert comparison is not None and comparison.non_executable is True


@pytest.mark.asyncio
async def test_production_builder_rolls_back_claim_and_group_on_late_fk_failure(
    migrated_database_url: str,
) -> None:
    """A selected non-instrument fails at the writer and leaves all proposal tables empty."""
    unknown = UUID("0198d500-1000-7000-8000-000000000b99")
    await _seed_authority(migrated_database_url, selected_id=unknown)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            await build_and_persist_proposal_group(
                session,
                actor_id="builder",
                idempotency_key="builder-rollback",
                request_hash="3" * 64,
                admission_authority_id=_ADMISSION_ID,
                research=(),
            )
    async with factory() as session:
        counts = []
        for model in (
            OptimizationProposalGroupRecord,
            OptimizationProposalRecord,
            ProposalPositionRecord,
            ProposalComparisonRecord,
        ):
            counts.append(await session.scalar(select(func.count()).select_from(model)))
    assert tuple(counts) == (0, 0, 0, 0)
