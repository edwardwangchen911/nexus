import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.paper.projection import load_paper_projection
from nexus.risk.canonical import canonical_paper_projection_hash
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")


async def _load_seeded_projection(database_url: str):
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            return await load_paper_projection(
                session,
                account_id=PAPER_ACCOUNT_ID,
                as_of=datetime(2026, 8, 26, 1, tzinfo=UTC),
            )
    finally:
        await engine.dispose()


def test_seeded_paper_account_loads_as_one_reconciled_projection(
    migrated_database_url: str,
) -> None:
    projection = asyncio.run(_load_seeded_projection(migrated_database_url))

    assert projection.account_id == PAPER_ACCOUNT_ID
    assert projection.portfolio_id == PORTFOLIO_ID
    assert projection.available_cash == Decimal("10000")
    assert projection.reserved_cash == Decimal("0")
    assert projection.positions == ()
    assert projection.open_orders == ()
    assert projection.nav == Decimal("10000")
    assert projection.peak_nav == Decimal("10000")
    assert projection.drawdown_state == "unlocked"
    assert projection.projection_hash == canonical_paper_projection_hash(projection)
