from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.portfolio_api import PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.diagnostics.repository import _read_report, _reports_equivalent, _stored_decimal
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import select, text

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
SNAPSHOT_AT = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
POSITIVE_TIE = "1.2345678901225"
NEGATIVE_TIE = "-1.2345678901225"


@pytest.mark.asyncio
async def test_decimal_projection_matches_postgresql_numeric_half_away_from_zero(
    migrated_database_url: str,
) -> None:
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT CAST(:positive AS NUMERIC(38,12)), "
                    "CAST(:negative AS NUMERIC(38,12))"
                ),
                {"positive": POSITIVE_TIE, "negative": NEGATIVE_TIE},
            )
        ).one()

    expected = (Decimal("1.234567890123"), Decimal("-1.234567890123"))
    assert tuple(row) == expected
    assert (_stored_decimal(POSITIVE_TIE), _stored_decimal(NEGATIVE_TIE)) == expected
    assert _stored_decimal("1.2345678901224") == Decimal("1.234567890122")
    assert _stored_decimal("-1.2345678901224") == Decimal("-1.234567890122")
    assert _stored_decimal("1.2345678901226") == Decimal("1.234567890123")
    assert _stored_decimal("-1.2345678901226") == Decimal("-1.234567890123")


@pytest.mark.asyncio
async def test_pg_stored_half_ulp_items_are_equivalent_to_same_authority_replay(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    ref = EvidenceRef(
        source="fixture",
        object_id="decimal-replay",
        available_at=SNAPSHOT_AT,
        location="fixture://decimal-replay",
        content_hash="d" * 64,
    )
    async with factory() as session, session.begin():
        record = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=SNAPSHOT_ID,
            snapshot_as_of=SNAPSHOT_AT,
            trading_day=date(2026, 8, 24),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="RESEARCH_REQUIRED",
            diagnostic_input_hash="d" * 64,
        )
        session.add(record)
        await session.flush()
        for code, value in (("POSITIVE_TIE", POSITIVE_TIE), ("NEGATIVE_TIE", NEGATIVE_TIE)):
            item = PortfolioDiagnosticItemRecord(
                report_id=record.id,
                diagnostic_code=code,
                metric_value=Decimal(value),
                unit="ratio",
                reference_limit=None,
                policy_scope="portfolio",
                severity="info",
                actionability="research",
                unknown_reason=None,
            )
            session.add(item)
            await session.flush()
            session.add(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=item.id,
                    source=ref.source,
                    object_id=ref.object_id,
                    available_at=ref.available_at,
                    location=ref.location,
                    content_hash=ref.content_hash,
                )
            )

    async with factory() as session:
        stored_record = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == SNAPSHOT_ID
            )
        )
        assert stored_record is not None
        stored_report = await _read_report(session, stored_record)

    replay = PortfolioDiagnosticReport(
        portfolio_snapshot_id=SNAPSHOT_ID,
        snapshot_as_of=SNAPSHOT_AT,
        trading_day=date(2026, 8, 24),
        policy_version="real_advisory_policy_v1",
        optimization_need="RESEARCH_REQUIRED",
        items=tuple(
            DiagnosticItem(
                diagnostic_code=code,
                metric_value=value,
                unit="ratio",
                reference_limit=None,
                policy_scope="portfolio",
                severity="info",
                actionability="research",
                unknown_reason=None,
                evidence_refs=(ref,),
            )
            for code, value in (("POSITIVE_TIE", POSITIVE_TIE), ("NEGATIVE_TIE", NEGATIVE_TIE))
        ),
    )

    assert tuple(item.metric_value for item in stored_report.items) == (
        "1.234567890123",
        "-1.234567890123",
    )
    assert _reports_equivalent(stored_report, replay)
