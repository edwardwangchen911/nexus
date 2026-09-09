"""Persist explicitly reviewed sector snapshots without inferring provider authority."""

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.universe import EQUITY_SECTOR_CODES, SectorExposureRecord
from nexus.persistence.models.market import Instrument, SectorExposure


async def persist_reviewed_sector_exposures(
    session: AsyncSession,
    records: tuple[SectorExposureRecord, ...],
) -> tuple[UUID, ...]:
    """Validate and idempotently project one or more reviewed CSV snapshots."""
    if not records:
        raise ValueError("SECTOR_EXPOSURE_BATCH_INVALID")
    cutoff = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(cutoff, datetime)
    grouped: defaultdict[UUID, list[SectorExposureRecord]] = defaultdict(list)
    for record in records:
        grouped[record.instrument_id].append(record)
    instruments = {
        instrument.id: instrument
        for instrument in await session.scalars(
            select(Instrument).where(Instrument.id.in_(grouped))
        )
    }
    if set(instruments) != set(grouped):
        raise ValueError("SECTOR_EXPOSURE_BATCH_INVALID")

    for instrument_id, snapshot in grouped.items():
        instrument = instruments[instrument_id]
        identities = {
            (record.as_of, record.source_url, record.content_hash) for record in snapshot
        }
        sectors = {record.sector_code for record in snapshot}
        total = sum((record.weight for record in snapshot), start=Decimal(0))
        if (
            instrument.currency != "USD"
            or instrument.risk_bucket.upper() not in {"EQUITY", "US_EQUITY"}
            or len(identities) != 1
            or len(sectors) != len(snapshot)
            or not sectors <= set(EQUITY_SECTOR_CODES)
            or any(
                record.as_of > cutoff.date()
                or record.synthetic
                or not record.reviewed
                or not Decimal(0) <= record.weight <= Decimal(1)
                for record in snapshot
            )
        ):
            raise ValueError("SECTOR_EXPOSURE_BATCH_INVALID")
        if instrument.asset_type in {"common_stock", "equity"}:
            valid = len(snapshot) == 1 and total == Decimal(1)
        elif instrument.asset_type == "equity_etf":
            valid = bool(snapshot) and Decimal("0.995") <= total <= Decimal("1.005")
        else:
            valid = False
        if not valid:
            raise ValueError("SECTOR_EXPOSURE_BATCH_INVALID")

    await session.execute(
        insert(SectorExposure)
        .values(
            [
                {
                    "instrument_id": record.instrument_id,
                    "sector_code": record.sector_code,
                    "weight": record.weight,
                    "as_of": record.as_of,
                    "source_url": record.source_url,
                    "content_hash": record.content_hash,
                    "synthetic": False,
                    "reviewed": True,
                }
                for record in records
            ]
        )
        .on_conflict_do_nothing(
            index_elements=["instrument_id", "sector_code", "as_of", "content_hash"]
        )
    )
    rows = tuple(
        await session.scalars(
            select(SectorExposure)
            .where(SectorExposure.instrument_id.in_(grouped))
            .order_by(SectorExposure.instrument_id, SectorExposure.sector_code)
        )
    )
    expected = {
        (record.instrument_id, record.sector_code, record.as_of, record.content_hash): record
        for record in records
    }
    actual = {
        (row.instrument_id, row.sector_code, row.as_of, row.content_hash): row
        for row in rows
        if (row.instrument_id, row.sector_code, row.as_of, row.content_hash) in expected
    }
    if set(actual) != set(expected) or any(
        (
            row.weight,
            row.source_url,
            row.synthetic,
            row.reviewed,
        )
        != (record.weight, record.source_url, False, True)
        for identity, record in expected.items()
        for row in (actual[identity],)
    ):
        raise ValueError("SECTOR_EXPOSURE_BATCH_CONFLICT")
    return tuple(row.id for row in actual.values())
