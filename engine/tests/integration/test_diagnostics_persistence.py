import asyncio
import inspect
from datetime import UTC, date, datetime
from uuid import UUID

import pytest
from helpers.portfolio_api import PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import persist_report
from nexus.diagnostics.service import diagnose_and_persist
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.session import session_factory
from sqlalchemy.exc import IntegrityError

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")


def test_production_diagnosis_has_no_caller_snapshot_history_or_comparable_parameters() -> None:
    parameters = tuple(inspect.signature(diagnose_and_persist).parameters)

    assert parameters == ("session", "portfolio_id", "snapshot_id")


@pytest.mark.asyncio
async def test_same_snapshot_policy_concurrent_replay_returns_one_existing_report(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    async def write() -> UUID:
        async with factory() as session, session.begin():
            policy = await load_real_advisory_policy_v1(session)
            return await persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=_report(policy.version),
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
        original_id = await persist_report(
            session, portfolio_id=PORTFOLIOS[0], report=report
        )
        replay_id = await persist_report(
            session, portfolio_id=PORTFOLIOS[0], report=report
        )
        assert replay_id == original_id
        with pytest.raises(ValueError, match="replay conflicts"):
            await persist_report(
                session,
                portfolio_id=PORTFOLIOS[0],
                report=report.model_copy(update={"optimization_need": "BLOCKED"}),
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
            await persist_report(
                session,
                portfolio_id=PORTFOLIOS[2],
                report=_report(policy.version),
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
