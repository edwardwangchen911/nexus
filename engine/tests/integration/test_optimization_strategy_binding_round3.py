"""Raw PostgreSQL selection-hash and strategy-weight boundaries."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.portfolio_api import INSTRUMENT_ID, PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

_CLOCK = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
_BASELINE = UUID("0198d500-1000-7000-8000-000000000041")
_DATA = UUID("0198d500-1000-7000-8000-000000000f01")
_SELECTION = UUID("0198d500-1000-7000-8000-000000000f02")
_REPORT = UUID("0198d500-1000-7000-8000-000000000f03")
_GROUP = UUID("0198d500-1000-7000-8000-000000000f04")
_PROPOSAL = UUID("0198d500-1000-7000-8000-000000000f05")
_POSITION = UUID("0198d500-1000-7000-8000-000000000f06")
_POLICY = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")


@pytest.mark.asyncio
async def test_raw_group_rejects_foreign_selection_evidence_hash_atomically(
    migrated_database_url: str,
) -> None:
    factory = await _seed_authority(migrated_database_url)
    with pytest.raises(IntegrityError, match="selection evidence hash"):
        async with factory() as session, session.begin():
            await _insert_group(session, evidence_hash="b" * 64)
    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
    assert count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("target", (Decimal("-0.001"), Decimal("0.126")))
async def test_raw_strategy_rejects_out_of_bounds_target_atomically(
    migrated_database_url: str, target: Decimal,
) -> None:
    factory = await _seed_authority(migrated_database_url)
    with pytest.raises(IntegrityError, match="strategy target exceeds bound selection"):
        async with factory() as session, session.begin():
            await _insert_group(session, evidence_hash="a" * 64)
            await _insert_strategy(session)
            await _insert_position(session, target)
    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
    assert count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("target", (Decimal("0"), Decimal("0.125")))
async def test_raw_strategy_allows_selected_bounded_target(
    migrated_database_url: str, target: Decimal,
) -> None:
    factory = await _seed_authority(migrated_database_url)
    async with factory() as session:
        await _insert_group(session, evidence_hash="a" * 64)
        await _insert_strategy(session)
        await _insert_position(session, target)
        await session.flush()
        await session.rollback()


async def _seed_authority(database_url: str):
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add_all(
            (
                DataSnapshot(
                    id=_DATA, source="fixture", provider_object_id="strategy-binding",
                    revision="1", observed_at=_CLOCK, available_at=_CLOCK,
                    received_at=_CLOCK, schema_version="1", payload={},
                ),
                PortfolioDiagnosticReportRecord(
                    id=_REPORT, portfolio_id=PORTFOLIOS[0],
                    portfolio_snapshot_id=_BASELINE, snapshot_as_of=_CLOCK,
                    trading_day=date(2026, 8, 24), policy_id=REAL_ADVISORY_POLICY_ID,
                    policy_version="real_advisory_policy_v1",
                    optimization_need="OPTIMIZATION_CANDIDATE",
                    diagnostic_input_hash="d" * 64,
                ),
                BaselineSelectionAuthorityRecord(
                    id=_SELECTION, policy_version="baseline_v1", data_snapshot_id=_DATA,
                    decision_at=_CLOCK, available_at=_CLOCK,
                    evidence_refs=["selection:round3"],
                    candidates=[{
                        "instrument_id": str(INSTRUMENT_ID),
                        "provenance_refs": ["candidate:round3"],
                    }],
                    selected_instrument_ids=[str(INSTRUMENT_ID)], evidence_hash="a" * 64,
                ),
            )
        )
    return factory


async def _insert_group(session, *, evidence_hash: str) -> None:
    await session.execute(
        text(
            "INSERT INTO optimization_proposal_groups "
            "(id, baseline_portfolio_snapshot_id, portfolio_scope, selection_authority_id, "
            "selection_evidence_hash, status, version, policy_id, diagnostic_report_id, "
            "data_snapshot_id, source_policy_id, cost_policy_id, diagnostic_input_hash, "
            "valuation_time, baseline_ready, diagnostic_complete, source_conflict, partial, "
            "requires_fx, cost_complete, authority_stale) VALUES "
            "(:id, :baseline, 'internal_paper', :selection, :selection_hash, 'draft', 0, "
            ":policy, :report, :data, :source, :cost, :diagnostic_hash, :valuation, true, "
            "true, false, false, false, true, false)"
        ),
        {
            "id": _GROUP, "baseline": _BASELINE, "selection": _SELECTION,
            "selection_hash": evidence_hash, "policy": _POLICY, "report": _REPORT,
            "data": _DATA, "source": SOURCE_POLICY_ID, "cost": COST_POLICY_ID,
            "diagnostic_hash": "d" * 64, "valuation": _CLOCK,
        },
    )


async def _insert_strategy(session) -> None:
    await session.execute(
        text(
            "INSERT INTO optimization_proposals "
            "(id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, "
            "strategy_evidence_complete, portfolio_scope) VALUES "
            "(:id, :group, 'strategy_target', :baseline, false, true, 'internal_paper')"
        ),
        {"id": _PROPOSAL, "group": _GROUP, "baseline": _BASELINE},
    )


async def _insert_position(session, target: Decimal) -> None:
    await session.execute(
        text(
            "INSERT INTO proposal_positions "
            "(id, proposal_id, instrument_id, current_weight, target_weight, action, reason_code) "
            "VALUES (:id, :proposal, :instrument, 0.5, :target, 'reduce', 'ROUND3_BOUNDARY')"
        ),
        {
            "id": _POSITION, "proposal": _PROPOSAL,
            "instrument": INSTRUMENT_ID, "target": target,
        },
    )
