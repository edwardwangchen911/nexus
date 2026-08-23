import asyncio
import inspect
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from helpers.portfolio_api import ACCOUNTS, PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import _persist_report, load_history
from nexus.diagnostics.service import diagnose_and_persist
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.session import session_factory
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
INPUT_HASH = "f" * 64


def test_production_diagnosis_has_no_caller_snapshot_history_or_comparable_parameters() -> None:
    parameters = tuple(inspect.signature(diagnose_and_persist).parameters)

    assert parameters == ("session", "portfolio_id", "snapshot_id", "artifact_reader")


def test_repository_has_no_public_ready_report_writer() -> None:
    from nexus.diagnostics import repository

    assert not hasattr(repository, "persist_report")


@pytest.mark.asyncio
async def test_same_snapshot_policy_concurrent_replay_returns_one_existing_report(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    async def write() -> UUID:
        async with factory() as session, session.begin():
            policy = await load_real_advisory_policy_v1(session)
            return await _persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=_report(policy.version),
                diagnostic_input_hash=INPUT_HASH,
            )

    report_ids = await asyncio.gather(write(), write())
    assert report_ids[0] == report_ids[1]


@pytest.mark.asyncio
async def test_same_snapshot_policy_replay_rejects_different_content(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        policy = await load_real_advisory_policy_v1(session)
        report = _report(policy.version)
        original_id = await _persist_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            report=report,
            diagnostic_input_hash=INPUT_HASH,
        )
        replay_id = await _persist_report(
            session,
            portfolio_id=PORTFOLIOS[0],
            report=report,
            diagnostic_input_hash=INPUT_HASH,
        )
        assert replay_id == original_id
        with pytest.raises(ValueError, match="replay conflicts"):
            await _persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=report.model_copy(update={"optimization_need": "BLOCKED"}),
                diagnostic_input_hash=INPUT_HASH,
            )


@pytest.mark.asyncio
async def test_cross_portfolio_report_is_rejected_by_repository_and_composite_fk(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        policy = await load_real_advisory_policy_v1(session)
        with pytest.raises(ValueError, match="snapshot or policy authority"):
            await _persist_report(
                session,
                portfolio_id=PORTFOLIOS[2],
                report=_report(policy.version),
                diagnostic_input_hash=INPUT_HASH,
            )

    async with factory() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    PortfolioDiagnosticReportRecord(
                        portfolio_id=PORTFOLIOS[2],
                        portfolio_snapshot_id=SNAPSHOT_ID,
                        trading_day=date(2026, 8, 24),
                        policy_id=REAL_ADVISORY_POLICY_ID,
                        policy_version="real_advisory_policy_v1",
                        optimization_need="RESEARCH_REQUIRED",
                    )
                )
                await session.flush()


@pytest.mark.asyncio
async def test_report_rejects_evidence_unavailable_at_frozen_snapshot(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        policy = await load_real_advisory_policy_v1(session)
        report = _report(policy.version)
        future_ref = report.items[0].evidence_refs[0].model_copy(
            update={"available_at": datetime(2026, 8, 24, 4, 0, 1, tzinfo=UTC)}
        )
        future_item = report.items[0].model_copy(update={"evidence_refs": (future_ref,)})

        with pytest.raises(ValueError, match="evidence is unavailable"):
            await _persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=report.model_copy(update={"items": (future_item,)}),
                diagnostic_input_hash=INPUT_HASH,
            )


@pytest.mark.asyncio
async def test_history_excludes_current_and_future_snapshots(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    current_as_of = datetime(2026, 8, 24, 4, tzinfo=UTC)
    prior_id = UUID("0198d500-1000-7000-8000-000000000061")
    future_id = UUID("0198d500-1000-7000-8000-000000000062")
    async with factory() as session, session.begin():
        policy = await load_real_advisory_policy_v1(session)
        await _add_snapshot(session, prior_id, current_as_of - timedelta(days=1), 61)
        await _add_snapshot(session, future_id, current_as_of + timedelta(days=1), 62)
        for snapshot_id, trading_day in (
            (prior_id, date(2026, 8, 23)),
            (SNAPSHOT_ID, date(2026, 8, 24)),
            (future_id, date(2026, 8, 25)),
        ):
            report = _report(policy.version)
            ref = report.items[0].evidence_refs[0].model_copy(
                update={
                    "available_at": datetime.combine(
                        trading_day, datetime.min.time(), tzinfo=UTC
                    )
                }
            )
            await _persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=report.model_copy(
                    update={
                        "portfolio_snapshot_id": snapshot_id,
                        "trading_day": trading_day,
                        "items": (
                            report.items[0].model_copy(update={"evidence_refs": (ref,)}),
                        ),
                    }
                ),
                diagnostic_input_hash=INPUT_HASH,
            )

        history = await load_history(
            session,
            portfolio_id=PORTFOLIOS[0],
            current_snapshot_id=SNAPSHOT_ID,
            current_snapshot_as_of=current_as_of,
            policy_version=policy.version,
        )

    assert tuple(report.portfolio_snapshot_id for report in history.prior_reports) == (
        prior_id,
    )


def _report(policy_version: str) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=SNAPSHOT_ID,
        trading_day=date(2026, 8, 24),
        policy_version=policy_version,  # type: ignore[arg-type]
        optimization_need="RESEARCH_REQUIRED",
        items=(
            DiagnosticItem(
                diagnostic_code="LIQUIDITY_DAYS_TO_EXIT",
                metric_value=None,
                unit="trading_days",
                reference_limit="2",
                policy_scope="currency:HKD",
                severity="info",
                actionability="research",
                unknown_reason="ADV20_MISSING",
                evidence_refs=(
                    EvidenceRef(
                        source="fixture",
                        object_id="market-1",
                        available_at=datetime(2026, 8, 24, tzinfo=UTC),
                        location="fixture://market-1",
                        content_hash="a" * 64,
                    ),
                ),
            ),
        ),
    )


async def _add_snapshot(
    session: AsyncSession, snapshot_id: UUID, as_of: datetime, suffix: int
) -> None:
    run = BrokerSyncRun(
        id=UUID(f"0198d500-1000-7000-8000-0000000000{suffix}"),
        account_id=ACCOUNTS[0],
        portfolio_id=PORTFOLIOS[0],
        trigger_kind="AUTOMATIC",
        actor_id=f"history-{suffix}",
        route="broker-sync",
        idempotency_key=f"history-{suffix}",
        request_hash=str(suffix % 10) * 64,
        status="READY",
        accepted_at=as_of,
        completed_at=as_of,
        attempt_count=1,
        failure_reason=None,
        cash_flow_quality="NOT_RUN",
    )
    snapshot = PortfolioSnapshotRecord(
        id=snapshot_id,
        portfolio_id=PORTFOLIOS[0],
        sync_run_id=run.id,
        stale_from_snapshot_id=None,
        status="READY",
        as_of=as_of,
        position_hash_version="broker_position_hash_v1",
        position_hash="a" * 64,
        order_hash_version="broker_order_hash_v1",
        order_hash="b" * 64,
        source_request_ids=[f"history-{suffix}"],
        source_observations=[],
    )
    session.add_all((run, snapshot))
    await session.flush()
