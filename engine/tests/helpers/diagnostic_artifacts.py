from datetime import UTC, datetime

from nexus.diagnostics.calendar_policy import (
    XNYS_CALENDAR_ADOPTION_ID,
    XNYS_CALENDAR_ADOPTION_KEY,
    XNYS_CALENDAR_ADOPTION_PAYLOAD,
    XNYS_CALENDAR_ADOPTION_VERSION,
    XnysCalendarAdoption,
    validate_xnys_calendar_adoption,
)
from nexus.persistence.models.diagnostics import DiagnosticPolicy, PortfolioDiagnosticReportRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from helpers.portfolio_api import settings


class FixtureArtifactReader:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    async def __call__(self, uri: str) -> bytes:
        return self.payloads[uri]


def fixture_xnys_calendar_adoption(
    created_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> XnysCalendarAdoption:
    return validate_xnys_calendar_adoption(
        DiagnosticPolicy(
            id=XNYS_CALENDAR_ADOPTION_ID,
            key=XNYS_CALENDAR_ADOPTION_KEY,
            version=XNYS_CALENDAR_ADOPTION_VERSION,
            schema_version="1",
            payload=XNYS_CALENDAR_ADOPTION_PAYLOAD,
            created_at=created_at,
        ),
        datetime(2026, 12, 31, tzinfo=UTC),
    )


async def diagnostic_report_count(session: AsyncSession) -> int:
    return int(
        await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
    )


async def diagnostic_report_count_from_url(database_url: str) -> int:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        return await diagnostic_report_count(session)


async def diagnostic_items_without_refs(session: AsyncSession) -> int:
    statement = text(
        "SELECT count(*) FROM portfolio_diagnostic_items item "
        "WHERE NOT EXISTS (SELECT 1 FROM portfolio_diagnostic_evidence_refs ref "
        "WHERE ref.item_id = item.id)"
    )
    return int(await session.scalar(statement))
