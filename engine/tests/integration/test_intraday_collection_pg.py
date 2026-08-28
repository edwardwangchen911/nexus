"""PostgreSQL idempotency for intraday Snapshot scheduling."""

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.jobs.intraday_schedule import schedule_due_intraday_snapshot
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    Watchlist,
    WatchlistMember,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.providers.settings import ProviderSettings
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")
WATCHLIST_ID = UUID("8c7cc45f-9420-5ea5-a75e-7df8a8a63b58")
AAPL_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
MSFT_ID = UUID("019d0000-0000-7000-8000-0000000000a2")


def _settings() -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        intraday_monitor_mode="production",
        alpaca_key_id="key-canary",
        alpaca_secret_key="secret-canary",
        alpaca_feed="sip",
        alpaca_delivery_mode="delayed",
        alpaca_intraday_feed="iex",
        alpaca_intraday_delivery_mode="real_time",
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )


async def _seed_schedule(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=CALENDAR_ID,
                exchange="XNYS",
                version="intraday-v1",
                source="exchange_calendars",
                range_start=date(2026, 8, 28),
                range_end=date(2026, 8, 28),
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=date(2026, 8, 28),
                decision_at=datetime(2026, 8, 28, 21, tzinfo=UTC),
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
            )
        )
        session.add(Watchlist(id=WATCHLIST_ID, name="default_private"))
        for instrument_id, symbol in ((MSFT_ID, "MSFT"), (AAPL_ID, "AAPL")):
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                )
            )
            session.add(WatchlistMember(watchlist_id=WATCHLIST_ID, instrument_id=instrument_id))


@pytest.mark.asyncio
async def test_schedule_enqueues_one_canonical_job_on_replay(
    migrated_database_url: str,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def schedule() -> Job | None:
        async with factory() as session, session.begin():
            return await schedule_due_intraday_snapshot(session, now=NOW, settings=_settings())

    first, replay = await asyncio.gather(schedule(), schedule())

    assert first is not None and replay is not None
    assert first.id == replay.id
    assert first.priority == 160
    assert first.payload["instruments"] == [
        {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
        {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
    ]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "now",
    (
        datetime(2026, 8, 28, 13, 29, 59, tzinfo=UTC),
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
    ),
)
async def test_schedule_noops_before_open_and_at_half_open_close(
    migrated_database_url: str,
    now: datetime,
) -> None:
    await _seed_schedule(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(
            session,
            now=now,
            settings=_settings(),
        )
    assert job is None
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
