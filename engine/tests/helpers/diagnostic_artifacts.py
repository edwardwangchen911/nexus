from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from helpers.portfolio_api import settings


class FixtureArtifactReader:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    async def __call__(self, uri: str) -> bytes:
        return self.payloads[uri]


async def diagnostic_report_count(session: AsyncSession) -> int:
    return int(
        await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
    )


async def diagnostic_report_count_from_url(database_url: str) -> int:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        return await diagnostic_report_count(session)
