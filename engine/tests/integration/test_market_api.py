import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode
from uuid import UUID, uuid4

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.main import create_engine_app
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    LatestQuote,
    Watchlist,
    WatchlistMember,
)
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, PriceBar, PriceSeries
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from uuid6 import uuid7

_DECISION_TIME = datetime(2026, 8, 24, 14, 0, tzinfo=UTC)
_AAPL_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
_MSFT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")
_NVDA_ID = UUID("60d64dc1-0367-52f2-9ec1-5a2c5355c65b")
_CONCURRENT_REQUEST_TIMEOUT_SECONDS = 5.0


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _app(database_url: str):
    app = create_engine_app(_settings(database_url))
    app.state.utc_clock = lambda: _DECISION_TIME
    return app


def _headers(method: str, path_and_query: str, *, nonce: UUID | None = None) -> dict[str, str]:
    return build_test_hmac_headers(
        method,
        path_and_query,
        b"",
        nonce=str(nonce or uuid7()),
    )


@asynccontextmanager
async def _client(app) -> AsyncIterator[httpx.AsyncClient]:
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client
    finally:
        await app.state.session_factory.kw["bind"].dispose()


async def _seed_instrument(
    database_url: str,
    *,
    instrument_id: UUID,
    symbol: str,
    tick_size: Decimal | None = Decimal("0.01"),
    valid_from: date = date(2020, 1, 1),
    valid_to: date | None = None,
) -> None:
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=tick_size,
                    risk_bucket="EQUITY",
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=valid_from,
                    valid_to=valid_to,
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


async def _seed_quote(
    database_url: str,
    *,
    instrument_id: UUID,
    source: str = "fixture",
    feed: str = "fixture",
    last: Decimal | None = Decimal("100.00"),
    observed_at: datetime = _DECISION_TIME - timedelta(seconds=10),
    available_at: datetime | None = None,
    quality_flags: Sequence[str] = (),
) -> None:
    factory = session_factory(_settings(database_url))
    try:
        async with factory() as session:
            session.add(
                LatestQuote(
                    instrument_id=instrument_id,
                    source=source,
                    feed=feed,
                    bid=None,
                    ask=None,
                    last=last,
                    observed_at=observed_at,
                    available_at=available_at or observed_at + timedelta(seconds=1),
                    received_at=observed_at + timedelta(seconds=2),
                    revision="1",
                    quality_flags=list(quality_flags),
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_market_requires_hmac_authentication(migrated_database_url: str) -> None:
    """Removing the service-actor dependency would expose private market facts."""
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get("/internal/v1/market")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_market_limit_is_capped_at_100(migrated_database_url: str) -> None:
    """Removing the API bound would allow an unbounded internal market read."""
    path = "/internal/v1/market?limit=101"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_market_returns_exact_source_aware_point_in_time_rows(
    migrated_database_url: str,
) -> None:
    """Float conversion, future aliases, future quotes, or missing provenance break this."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    await _seed_instrument(
        migrated_database_url,
        instrument_id=_MSFT_ID,
        symbol="EXPIRED",
        valid_to=_DECISION_TIME.date() - timedelta(days=1),
    )
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            session.add(
                InstrumentAlias(
                    instrument_id=_AAPL_ID,
                    symbol="FUTURE",
                    alias_type="ticker",
                    valid_from=_DECISION_TIME.date() + timedelta(days=1),
                    valid_to=None,
                )
            )
            await session.commit()
    finally:
        await factory.kw["bind"].dispose()
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.4500"),
        observed_at=_DECISION_TIME - timedelta(seconds=8),
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="sip",
        last=Decimal("999.00"),
        observed_at=_DECISION_TIME + timedelta(minutes=1),
        available_at=_DECISION_TIME + timedelta(minutes=1),
    )
    path = "/internal/v1/market?limit=100"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    assert response.json()["data"] == {
        "items": [
            {
                "instrument_id": str(_AAPL_ID),
                "symbol": "AAPL",
                "price": "123.450000000000",
                "source": "fixture",
                "feed": "fixture",
                "as_of": "2026-08-24T13:59:52Z",
                "quality": "PASS",
                "quality_reasons": [],
                "watchlisted": False,
            }
        ],
        "next_cursor": None,
        "selected_series": None,
    }
    assert response.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"


@pytest.mark.asyncio
async def test_market_marks_session_quote_stale_and_never_fabricates_missing_price(
    migrated_database_url: str,
) -> None:
    """A stale or absent quote must remain evidence, not become a fresh synthetic value."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    await _seed_instrument(
        migrated_database_url, instrument_id=_MSFT_ID, symbol="MSFT"
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("100.00"),
        observed_at=_DECISION_TIME - timedelta(seconds=46),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    items = {item["symbol"]: item for item in response.json()["data"]["items"]}
    assert items["AAPL"] == {
        "instrument_id": str(_AAPL_ID),
        "symbol": "AAPL",
        "price": "100.000000000000",
        "source": "fixture",
        "feed": "fixture",
        "as_of": "2026-08-24T13:59:14Z",
        "quality": "PARTIAL",
        "quality_reasons": ["SELECTED_SOURCE_STALE"],
        "watchlisted": False,
    }
    assert items["MSFT"]["price"] is None
    assert items["MSFT"]["source"] is None
    assert items["MSFT"]["quality"] == "FAIL"
    assert items["MSFT"]["quality_reasons"] == ["QUOTE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_market_fails_closed_on_conflicting_latest_sources(
    migrated_database_url: str,
) -> None:
    """Removing the cross-source threshold would display a disputed price as trusted."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="alpaca",
        feed="sip",
        last=Decimal("100.00"),
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        source="fixture",
        feed="fixture",
        last=Decimal("101.00"),
    )
    path = "/internal/v1/market"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    item = response.json()["data"]["items"][0]
    assert item["price"] is None
    assert item["source"] is None
    assert item["quality"] == "FAIL"
    assert item["quality_reasons"] == ["SOURCE_CONFLICT"]


@pytest.mark.asyncio
async def test_market_cursor_is_stable_and_invalid_or_foreign_values_fail_closed(
    migrated_database_url: str,
) -> None:
    """An unstable order or permissive cursor decoder causes duplicates or cross-route reuse."""
    for instrument_id, symbol in (
        (_AAPL_ID, "AAPL"),
        (_MSFT_ID, "MSFT"),
        (_NVDA_ID, "NVDA"),
    ):
        await _seed_instrument(
            migrated_database_url, instrument_id=instrument_id, symbol=symbol
        )
    app = _app(migrated_database_url)

    async with _client(app) as client:
        first_path = "/internal/v1/market?limit=2"
        first = await client.get(first_path, headers=_headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        assert isinstance(cursor, str) and cursor
        assert "MSFT" not in cursor
        app.state.utc_clock = lambda: _DECISION_TIME + timedelta(minutes=1)
        second_query = urlencode({"limit": 2, "cursor": cursor})
        second_path = f"/internal/v1/market?{second_query}"
        second = await client.get(second_path, headers=_headers("GET", second_path))
        malformed_path = "/internal/v1/market?cursor=not-a-market-cursor"
        malformed = await client.get(
            malformed_path, headers=_headers("GET", malformed_path)
        )
        foreign_path = "/internal/v1/market?cursor=eyJzY29wZSI6Im90aGVyIn0"
        foreign = await client.get(foreign_path, headers=_headers("GET", foreign_path))
        cross_filter_query = urlencode({"limit": 2, "cursor": cursor, "watchlist": "true"})
        cross_filter_path = f"/internal/v1/market?{cross_filter_query}"
        cross_filter = await client.get(
            cross_filter_path, headers=_headers("GET", cross_filter_path)
        )

    assert [item["symbol"] for item in first.json()["data"]["items"]] == [
        "AAPL",
        "MSFT",
    ]
    assert [item["symbol"] for item in second.json()["data"]["items"]] == ["NVDA"]
    assert second.json()["data"]["next_cursor"] is None
    assert second.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"
    for rejected in (malformed, foreign, cross_filter):
        assert rejected.status_code == 422
        assert rejected.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_selected_series_uses_repository_authority_and_excludes_future_bars(
    migrated_database_url: str,
) -> None:
    """Bypassing Task 7 would expose a future revision through the chart payload."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    visible = PriceBar(
        instrument_id=InstrumentId(_AAPL_ID),
        session=date(2026, 8, 21),
        open="100.00",
        high="102.00",
        low="99.00",
        close="101.00",
        volume="1000",
        available_at=_DECISION_TIME - timedelta(days=1),
        revision="1",
    )
    future = visible.model_copy(
        update={
            "close": Decimal("999.00"),
            "available_at": _DECISION_TIME + timedelta(microseconds=1),
            "revision": "2",
        }
    )

    async def offline_loader(
        instrument_id: UUID, decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        assert instrument_id == _AAPL_ID
        assert decision_time == _DECISION_TIME
        return (
            PriceSeries(
                source="fixture",
                feed="fixture",
                adjustment="split",
                bars=(future, visible),
                mode="fixture",
                quality="PASS",
            ),
        )

    selected_path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(
            selected_path, headers=_headers("GET", selected_path)
        )

    series = response.json()["data"]["selected_series"]
    assert series["source"] == "fixture"
    assert series["feed"] == "fixture"
    assert series["quality"] == "PARTIAL"
    assert series["quality_reasons"] == ["FUTURE_DATA_EXCLUDED"]
    assert [bar["close"] for bar in series["bars"]] == ["101.00"]


@pytest.mark.asyncio
async def test_selected_series_missing_seam_returns_explicit_no_history(
    migrated_database_url: str,
) -> None:
    """A missing offline seam must not be filled from the latest quote or a network call."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    await _seed_quote(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        last=Decimal("123.45"),
    )
    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert selected == {
        "instrument_id": str(_AAPL_ID),
        "source": None,
        "feed": None,
        "adjustment": None,
        "interval": None,
        "quality": "FAIL",
        "quality_reasons": ["NO_HISTORY_DATA"],
        "bars": [],
    }


@pytest.mark.asyncio
async def test_selected_series_is_deterministically_bounded_to_latest_500_bars(
    migrated_database_url: str,
) -> None:
    """Removing the response cap would make one chart request return unbounded history."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    first_session = date(2025, 1, 1)
    bars = tuple(
        PriceBar(
            instrument_id=InstrumentId(_AAPL_ID),
            session=first_session + timedelta(days=index),
            open=str(100 + index),
            high=str(101 + index),
            low=str(99 + index),
            close=str(100 + index),
            volume=str(1000 + index),
            available_at=datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=index),
            revision="1",
        )
        for index in range(501)
    )

    async def offline_loader(
        _instrument_id: UUID, _decision_time: datetime
    ) -> tuple[PriceSeries, ...]:
        return (
            PriceSeries(
                source="fixture",
                feed="fixture",
                adjustment="split",
                bars=bars,
                mode="fixture",
                quality="PASS",
            ),
        )

    path = f"/internal/v1/market?selected_instrument_id={_AAPL_ID}"
    app = _app(migrated_database_url)
    app.state.market_price_series_loader = offline_loader
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    selected = response.json()["data"]["selected_series"]
    assert len(selected["bars"]) == 500
    assert selected["bars"][0]["session"] == "2025-01-02"
    assert selected["bars"][-1]["session"] == "2026-05-16"
    assert selected["quality"] == "PARTIAL"
    assert selected["quality_reasons"] == ["HISTORY_TRUNCATED"]


@pytest.mark.asyncio
async def test_selected_series_unknown_instrument_is_not_found(
    migrated_database_url: str,
) -> None:
    """Returning an empty chart for an unknown identifier would hide a caller bug."""
    unknown_id = uuid4()
    path = f"/internal/v1/market?selected_instrument_id={unknown_id}"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"
    assert response.json()["error"]["details"] == {"instrument_id": str(unknown_id)}


@pytest.mark.asyncio
async def test_watchlist_put_and_delete_have_idempotent_set_semantics(
    migrated_database_url: str,
) -> None:
    """Insert/delete row-count semantics must not leak through the final representation."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        first_put = await client.put(path, headers=_headers("PUT", path))
        duplicate_put = await client.put(path, headers=_headers("PUT", path))
        market_path = "/internal/v1/market"
        listed_present = await client.get(
            market_path, headers=_headers("GET", market_path)
        )
        first_delete = await client.delete(path, headers=_headers("DELETE", path))
        absent_delete = await client.delete(path, headers=_headers("DELETE", path))
        listed_absent = await client.get(
            market_path, headers=_headers("GET", market_path)
        )

    expected_true = {"instrument_id": str(_AAPL_ID), "watchlisted": True}
    expected_false = {"instrument_id": str(_AAPL_ID), "watchlisted": False}
    assert first_put.status_code == duplicate_put.status_code == 200
    assert first_put.json()["data"] == duplicate_put.json()["data"] == expected_true
    assert listed_present.json()["data"]["items"][0]["watchlisted"] is True
    assert first_delete.status_code == absent_delete.status_code == 200
    assert first_delete.json()["data"] == absent_delete.json()["data"] == expected_false
    assert listed_absent.json()["data"]["items"][0]["watchlisted"] is False


@pytest.mark.asyncio
async def test_watchlist_unknown_instrument_is_not_found(
    migrated_database_url: str,
) -> None:
    """A foreign key failure must be normalized instead of creating an implicit instrument."""
    unknown_id = uuid4()
    path = f"/internal/v1/watchlist/{unknown_id}"
    app = _app(migrated_database_url)
    async with _client(app) as client:
        response = await client.put(path, headers=_headers("PUT", path))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"
    assert response.json()["error"]["details"] == {"instrument_id": str(unknown_id)}


@pytest.mark.asyncio
async def test_market_watchlist_boolean_filter_is_applied_in_repository_query(
    migrated_database_url: str,
) -> None:
    """A UI-only filter would leak unfiltered pagination and inconsistent counts."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    await _seed_instrument(
        migrated_database_url, instrument_id=_MSFT_ID, symbol="MSFT"
    )
    membership_path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        added = await client.put(
            membership_path, headers=_headers("PUT", membership_path)
        )
        true_path = "/internal/v1/market?watchlist=true"
        listed_true = await client.get(
            true_path, headers=_headers("GET", true_path)
        )
        false_path = "/internal/v1/market?watchlist=false"
        listed_false = await client.get(
            false_path, headers=_headers("GET", false_path)
        )

    assert added.status_code == 200
    assert [
        item["symbol"] for item in listed_true.json()["data"]["items"]
    ] == ["AAPL"]
    assert [
        item["symbol"] for item in listed_false.json()["data"]["items"]
    ] == ["MSFT"]


@pytest.mark.asyncio
async def test_concurrent_duplicate_watchlist_puts_leave_one_membership(
    migrated_database_url: str,
) -> None:
    """Removing PostgreSQL conflict authority would allow duplicate membership or a 500."""
    await _seed_instrument(
        migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL"
    )
    path = f"/internal/v1/watchlist/{_AAPL_ID}"
    app = _app(migrated_database_url)

    async with _client(app) as client:
        responses = await asyncio.wait_for(
            asyncio.gather(
                client.put(path, headers=_headers("PUT", path)),
                client.put(path, headers=_headers("PUT", path)),
            ),
            timeout=_CONCURRENT_REQUEST_TIMEOUT_SECONDS,
        )

    assert [response.status_code for response in responses] == [200, 200]
    assert all(response.json()["data"]["watchlisted"] for response in responses)
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(WatchlistMember)
                .where(WatchlistMember.instrument_id == _AAPL_ID)
            )
            watchlist_count = await session.scalar(
                select(func.count()).select_from(Watchlist)
            )
    finally:
        await factory.kw["bind"].dispose()
    assert count == 1
    assert watchlist_count == 1
