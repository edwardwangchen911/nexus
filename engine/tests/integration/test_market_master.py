from datetime import date
from uuid import UUID, uuid4

import pytest
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")


@pytest.mark.asyncio
async def test_ticker_alias_has_a_valid_dated_interval(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.commit()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2026, 8, 2),
                valid_to=date(2026, 8, 1),
            )
        )
        with pytest.raises(IntegrityError, match="instrument_alias_valid_range"):
            await session.commit()


@pytest.mark.asyncio
async def test_internal_instrument_id_cannot_be_rewritten(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.commit()
        with pytest.raises(DBAPIError, match="instrument id is immutable"):
            await session.execute(
                text("UPDATE instruments SET id = :replacement WHERE id = :instrument_id"),
                {"replacement": uuid4(), "instrument_id": INSTRUMENT_ID},
            )


@pytest.mark.asyncio
async def test_published_universe_snapshot_cannot_be_rewritten(
    migrated_database_url: str,
) -> None:
    snapshot_id = UUID("c32df4bf-f6a3-5a18-8e20-1c4fb9c39d38")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            UniverseSnapshot(
                id=snapshot_id,
                key="us_liquid",
                version="us_liquid_v1",
                effective_from=date(2026, 1, 1),
                source="versioned_fixture",
                content_hash="a" * 64,
            )
        )
        await session.commit()
        with pytest.raises(DBAPIError, match="universe snapshot is immutable"):
            await session.execute(
                text("UPDATE universe_snapshots SET version = 'us_liquid_v2' WHERE id = :id"),
                {"id": snapshot_id},
            )
