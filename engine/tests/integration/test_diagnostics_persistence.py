import inspect
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from helpers.portfolio_api import (
    ACCOUNTS,
    CLOCK,
    PORTFOLIOS,
    seed_portfolio_list,
    settings,
    snapshot_record,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.diagnostics.repository import load_history
from nexus.diagnostics.service import diagnose_and_persist
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.session import session_factory
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
INPUT_HASH = "f" * 64


def test_production_diagnosis_has_no_caller_snapshot_history_or_comparable_parameters() -> None:
    parameters = tuple(inspect.signature(diagnose_and_persist).parameters)

    assert parameters == ("session", "portfolio_id", "snapshot_id", "artifact_reader")


def test_repository_has_no_ready_report_writer_even_by_convention() -> None:
    from nexus.diagnostics import repository

    assert not hasattr(repository, "persist_report")
    assert not hasattr(repository, "_persist_report")


@pytest.mark.asyncio
async def test_production_diagnosis_rejects_ready_snapshot_without_cash_flow_reconciliation(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    run_id = UUID("0198d500-1000-7000-8000-000000000091")
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000092")
    async with factory() as session, session.begin():
        session.add(
            BrokerSyncRun(
                id=run_id,
                account_id=ACCOUNTS[1],
                portfolio_id=PORTFOLIOS[1],
                trigger_kind="AUTOMATIC",
                actor_id="unreconciled-ready",
                route="broker-sync",
                idempotency_key="unreconciled-ready",
                request_hash="9" * 64,
                status="READY",
                accepted_at=CLOCK,
                completed_at=CLOCK,
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="NOT_RUN",
            )
        )
        session.add(snapshot_record(snapshot_id, run_id, "READY", CLOCK, None))
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="diagnostic reconciliation is incomplete"):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[1],
                snapshot_id=snapshot_id,
            )


@pytest.mark.asyncio
async def test_cross_portfolio_report_is_rejected_by_repository_and_composite_fk(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="snapshot was not found"):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[2],
                snapshot_id=SNAPSHOT_ID,
            )

    async with factory() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    PortfolioDiagnosticReportRecord(
                        portfolio_id=PORTFOLIOS[2],
                        portfolio_snapshot_id=SNAPSHOT_ID,
                        snapshot_as_of=datetime(2026, 8, 24, 3, 59, tzinfo=UTC),
                        trading_day=date(2026, 8, 24),
                        policy_id=REAL_ADVISORY_POLICY_ID,
                        policy_version="real_advisory_policy_v1",
                        optimization_need="RESEARCH_REQUIRED",
                        diagnostic_input_hash=INPUT_HASH,
                    )
                )
                await session.flush()


@pytest.mark.asyncio
async def test_same_currency_correlation_components_persist_with_distinct_scopes(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    scopes = (
        "currency:USD:correlation-cluster:component-a",
        "currency:USD:correlation-cluster:component-b",
    )
    async with factory() as session, session.begin():
        stored = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=SNAPSHOT_ID,
            snapshot_as_of=datetime(2026, 8, 24, 3, 59, tzinfo=UTC),
            trading_day=date(2026, 8, 24),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="RESEARCH_REQUIRED",
            diagnostic_input_hash=INPUT_HASH,
        )
        session.add(stored)
        await session.flush()
        session.add_all(
            PortfolioDiagnosticItemRecord(
                report_id=stored.id,
                diagnostic_code="CORRELATION_CLUSTER",
                metric_value="0.9",
                unit="ratio",
                reference_limit="0.5",
                policy_scope=scope,
                severity="warning",
                actionability="research",
                unknown_reason=None,
            )
            for scope in scopes
        )
        await session.flush()
        stored_scopes = tuple(
            await session.scalars(
                select(PortfolioDiagnosticItemRecord.policy_scope)
                .where(PortfolioDiagnosticItemRecord.report_id == stored.id)
                .order_by(PortfolioDiagnosticItemRecord.policy_scope)
            )
        )

    assert stored_scopes == scopes


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
        await _add_snapshot(session, prior_id, current_as_of - timedelta(days=1), 61)
        await _add_snapshot(session, future_id, current_as_of + timedelta(days=1), 62)
        for snapshot_id, trading_day in (
            (prior_id, date(2026, 8, 23)),
            (SNAPSHOT_ID, date(2026, 8, 24)),
            (future_id, date(2026, 8, 25)),
        ):
            session.add(
                PortfolioDiagnosticReportRecord(
                    portfolio_id=PORTFOLIOS[0],
                    portfolio_snapshot_id=snapshot_id,
                    snapshot_as_of=datetime.combine(
                        trading_day, datetime.min.time(), tzinfo=UTC
                    ),
                    trading_day=trading_day,
                    policy_id=REAL_ADVISORY_POLICY_ID,
                    policy_version="real_advisory_policy_v1",
                    optimization_need="RESEARCH_REQUIRED",
                    diagnostic_input_hash=INPUT_HASH,
                )
            )
        await session.flush()

        history = await load_history(
            session,
            portfolio_id=PORTFOLIOS[0],
            current_snapshot_id=SNAPSHOT_ID,
            current_snapshot_as_of=current_as_of,
            policy_version="real_advisory_policy_v1",
        )

    assert tuple(report.portfolio_snapshot_id for report in history.prior_reports) == (
        prior_id,
    )
    assert len(history.authority_refs) == 1
    assert history.authority_refs[0].object_id.startswith("diagnostic-report:")
    assert len(history.authority_refs[0].content_hash) == 64


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
