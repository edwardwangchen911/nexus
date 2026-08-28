"""PostgreSQL tests for raw-first immutable intraday ingestion."""

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.intraday_ingestion import ingest_intraday_snapshot
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar
from nexus.providers.market.snapshot_contracts import (
    MarketSnapshot,
    SnapshotBatch,
    SnapshotMinuteBar,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
RECEIVED_AT = datetime(2026, 8, 28, 14, 30, 16, tzinfo=UTC)


@dataclass
class _ObjectStore:
    fail_at: str | None = None
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)
    archived_payloads: list[bytes] = field(default_factory=list)

    async def archive_raw(self, key: str, payload: bytes) -> str:
        if self.fail_at == "archive_raw":
            raise OSError("fixture archive failure")
        if key not in self.objects:
            self.objects[key] = payload
            self.archived_payloads.append(payload)
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        if self.fail_at == "put_temporary":
            raise OSError("fixture temporary failure")
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        if self.fail_at == "promote":
            raise OSError("fixture promotion failure")
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)


def _batch(*, aapl_close: str = "227.16") -> SnapshotBatch:
    return SnapshotBatch(
        snapshots=(
            _snapshot(AAPL_ID, "AAPL", aapl_close),
            _snapshot(MSFT_ID, "MSFT", "510.25"),
        ),
        raw_payload=f'{{"aapl_close":"{aapl_close}","fixture":"intraday"}}'.encode(),
        received_at=RECEIVED_AT,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="iex",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )


def _snapshot(instrument_id: InstrumentId, symbol: str, close: str) -> MarketSnapshot:
    observed = RECEIVED_AT.replace(second=0)
    price = Decimal(close)
    return MarketSnapshot(
        instrument_id=instrument_id,
        symbol=symbol,
        trade=SnapshotTrade(
            price=price,
            observed_at=RECEIVED_AT.replace(second=14, microsecond=400_000),
            provider_trade_id=f"{symbol}-trade",
        ),
        quote=SnapshotQuote(
            bid=price - Decimal("0.02"),
            ask=price + Decimal("0.02"),
            observed_at=RECEIVED_AT.replace(second=14, microsecond=500_000),
        ),
        minute_bar=SnapshotMinuteBar(
            bar=PriceBar(
                instrument_id=instrument_id,
                session=date(2026, 8, 28),
                open=price - Decimal("0.16"),
                high=price + Decimal("0.04"),
                low=price - Decimal("0.18"),
                close=price,
                volume=Decimal("1200"),
                observed_at=observed,
                available_at=RECEIVED_AT,
                received_at=RECEIVED_AT,
                revision="snapshot-v1",
            )
        ),
    )


async def _seed_instruments(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        session.add_all(
            Instrument(
                id=instrument_id,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="large_cap",
            )
            for instrument_id in (AAPL_ID, MSFT_ID)
        )


@pytest.mark.asyncio
async def test_intraday_ingestion_is_raw_first_and_replay_idempotent(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    async with factory() as session, session.begin():
        first = await ingest_intraday_snapshot(session, store, _batch(), commit=False)
    async with factory() as session, session.begin():
        replay = await ingest_intraday_snapshot(session, store, _batch(), commit=False)

    assert first.created_trade_observations == 2
    assert first.created_quote_observations == 2
    assert first.created_bar_authorities == 2
    assert first.replayed == 0
    assert replay.created_trade_observations == 0
    assert replay.created_quote_observations == 0
    assert replay.created_bar_authorities == 0
    assert replay.replayed == 6
    assert store.archived_payloads == [_batch().raw_payload]
    assert store.temporary == {}

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 4
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 3
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 2
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 2
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 2


@pytest.mark.asyncio
async def test_same_minute_revision_creates_new_authority_without_rewrite(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(session, store, _batch(), commit=False)
    async with factory() as session, session.begin():
        changed = await ingest_intraday_snapshot(
            session, store, _batch(aapl_close="227.17"), commit=False
        )

    assert changed.created_bar_authorities == 1
    async with factory() as session:
        rows = (
            await session.scalars(
                select(IntradayBarSetAuthority).order_by(
                    IntradayBarSetAuthority.first_observed_at,
                    IntradayBarSetAuthority.created_at,
                )
            )
        ).all()
        assert len(rows) == 3
        assert len({row.content_hash for row in rows}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", ("archive_raw", "put_temporary", "promote"))
async def test_object_store_failure_leaves_no_readable_intraday_authority(
    migrated_database_url: str,
    fail_at: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore(fail_at=fail_at)
    async with factory() as session:
        with pytest.raises(OSError):
            await ingest_intraday_snapshot(session, store, _batch())

    assert store.temporary == {}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0
