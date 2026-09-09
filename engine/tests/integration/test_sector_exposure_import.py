"""Reviewed sector CSV becomes immutable DB authority without provider calls."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from helpers.portfolio_api import settings
from nexus.market.universe import SectorExposureRecord
from nexus.persistence.models.market import Instrument, SectorExposure
from nexus.persistence.session import session_factory
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

STOCK_ID = UUID(int=9101)
ETF_ID = UUID(int=9102)


def _record(
    instrument_id: UUID,
    sector: str,
    weight: str,
    *,
    digest: str,
) -> SectorExposureRecord:
    return SectorExposureRecord(
        instrument_id=instrument_id,
        sector_code=sector,
        weight=Decimal(weight),
        as_of=date(2026, 1, 1),
        source_url=f"https://issuer.example/{instrument_id}.csv",
        content_hash=digest,
        synthetic=False,
        reviewed=True,
    )


async def _seed_instruments(session: AsyncSession) -> None:
    session.add_all(
        (
            Instrument(
                id=STOCK_ID,
                asset_type="common_stock",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="EQUITY",
            ),
            Instrument(
                id=ETF_ID,
                asset_type="equity_etf",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="EQUITY",
            ),
        )
    )


@pytest.mark.asyncio
async def test_reviewed_sector_batch_is_validated_persisted_and_idempotent(
    migrated_database_url: str,
) -> None:
    from nexus.market import sector_exposure_storage

    records = (
        _record(STOCK_ID, "industrials", "1", digest="a" * 64),
        _record(ETF_ID, "financials", "0.400", digest="b" * 64),
        _record(ETF_ID, "information_technology", "0.599", digest="b" * 64),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        first = await sector_exposure_storage.persist_reviewed_sector_exposures(session, records)
        replay = await sector_exposure_storage.persist_reviewed_sector_exposures(session, records)

    assert replay == first
    assert len(first) == 3
    async with factory() as session:
        rows = tuple(
            await session.scalars(
                select(SectorExposure).order_by(
                    SectorExposure.instrument_id, SectorExposure.sector_code
                )
            )
        )
        assert len(rows) == 3
        assert all(row.reviewed and not row.synthetic for row in rows)
        assert await session.scalar(select(func.count()).select_from(SectorExposure)) == 3


@pytest.mark.asyncio
async def test_reviewed_sector_batch_rejects_incomplete_stock_without_writing(
    migrated_database_url: str,
) -> None:
    from nexus.market import sector_exposure_storage

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        with pytest.raises(ValueError, match="SECTOR_EXPOSURE_BATCH_INVALID"):
            await sector_exposure_storage.persist_reviewed_sector_exposures(
                session,
                (_record(STOCK_ID, "industrials", "0.9", digest="c" * 64),),
            )

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(SectorExposure)) == 0


@pytest.mark.asyncio
async def test_local_entry_imports_reviewed_csv_with_aggregate_receipt(
    migrated_database_url: str,
    tmp_path: Path,
) -> None:
    from nexus.ops import sector_exposure_local

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
    csv_path = tmp_path / "reviewed-sector.csv"
    csv_path.write_text(
        "instrument_id,sector_code,weight,as_of,source_url,content_hash\n"
        f"{STOCK_ID},industrials,1,2026-01-01,https://issuer.example/stock.csv,{'d' * 64}\n"
    )

    result = await sector_exposure_local.run(
        settings(migrated_database_url),
        csv_path=csv_path,
    )

    assert result == {
        "status": "imported",
        "instrument_count": 1,
        "row_count": 1,
        "execution_locked": True,
        "model_called": False,
    }
