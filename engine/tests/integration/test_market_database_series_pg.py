"""PostgreSQL-only market loaders preserve frozen authority and decision time."""

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from helpers.price_bar_authority import (
    INSTRUMENT_ID,
    MANIFEST_ID,
    insert_valid_authority_from_prepare,
    seed_price_candidate,
)
from nexus.market.database_series import load_daily_series, load_intraday_series
from nexus.market.intraday_ingestion import ingest_intraday_snapshot
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

DECISION_TIME = datetime(2026, 8, 28, 15, 1, 30, tzinfo=UTC)


@dataclass
class _ObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)


def _batch(*, received_at: datetime, close: str) -> SnapshotBatch:
    observed = received_at.replace(second=0, microsecond=0) - timedelta(minutes=1)
    price = Decimal(close)
    return SnapshotBatch(
        snapshots=(
            MarketSnapshot(
                instrument_id=InstrumentId(INSTRUMENT_ID),
                symbol="AAPL",
                trade=SnapshotTrade(
                    price=price,
                    observed_at=received_at - timedelta(seconds=2),
                    provider_trade_id=f"trade-{close}",
                ),
                quote=SnapshotQuote(
                    bid=price - Decimal("0.02"),
                    ask=price + Decimal("0.02"),
                    observed_at=received_at - timedelta(seconds=1),
                ),
                minute_bar=SnapshotMinuteBar(
                    bar=PriceBar(
                        instrument_id=InstrumentId(INSTRUMENT_ID),
                        session=date(2026, 8, 28),
                        open=price - Decimal("0.10"),
                        high=price + Decimal("0.10"),
                        low=price - Decimal("0.20"),
                        close=price,
                        volume=Decimal("1200"),
                        observed_at=observed,
                        available_at=received_at,
                        received_at=received_at,
                        revision="snapshot-v1",
                    )
                ),
            ),
        ),
        raw_payload=f'{{"close":"{close}"}}'.encode(),
        received_at=received_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="iex",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )


@pytest.mark.asyncio
async def test_database_loaders_return_only_visible_revalidated_authorities(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await seed_price_candidate(factory)
    await insert_valid_authority_from_prepare(factory, MANIFEST_ID)
    store = _ObjectStore()
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(
            session,
            store,
            _batch(received_at=DECISION_TIME - timedelta(seconds=10), close="103"),
            commit=False,
        )
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(
            session,
            store,
            _batch(received_at=DECISION_TIME + timedelta(seconds=10), close="999"),
            commit=False,
        )

    async with factory() as session:
        daily = await load_daily_series(session, INSTRUMENT_ID, DECISION_TIME)
        intraday = await load_intraday_series(session, INSTRUMENT_ID, DECISION_TIME)

    assert len(daily) == 1
    assert daily[0].interval == "1Day"
    assert intraday.source == "alpaca"
    assert intraday.feed == "iex"
    assert [bar.close for bar in intraday.bars] == [Decimal("103")]
    assert intraday.trade is not None and intraday.trade.last == Decimal("103")
    assert intraday.quote is not None and intraday.quote.ask == Decimal("103.02")
