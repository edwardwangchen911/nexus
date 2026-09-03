"""Persisted receipt-only quotes never acquire a source timestamp on refresh."""

from datetime import timedelta
from urllib.parse import urlencode

import httpx
import pytest
from integration.test_market_api import (
    _AAPL_ID,
    _DECISION_TIME,
    _MSFT_ID,
    _app,
    _client,
    _headers,
    _seed_fosun_quote_v2,
    _seed_instrument,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery", ["unknown", "delayed"])
async def test_market_returns_receipt_but_never_infers_fosun_source_time(
    migrated_database_url,
    monkeypatch,
    delivery,
):
    await _seed_instrument(migrated_database_url, instrument_id=_AAPL_ID, symbol="AAPL")
    await _seed_fosun_quote_v2(
        migrated_database_url,
        instrument_id=_AAPL_ID,
        delivery_mode=delivery,
    )

    async def forbidden(*args, **kwargs):
        pytest.fail("market GET must only read persisted data")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    application = _app(migrated_database_url)
    async with _client(application) as client:
        path = "/internal/v1/market"
        first = await client.get(path, headers=_headers("GET", path))
        application.state.utc_clock = lambda: _DECISION_TIME + timedelta(seconds=30)
        refreshed = await client.get(path, headers=_headers("GET", path))
    assert first.status_code == refreshed.status_code == 200
    for response in (first, refreshed):
        item = response.json()["data"]["items"][0]
        assert item["received_at"] == "2026-08-24T13:59:50Z"
        assert item["as_of"] == "2026-08-24T13:59:50Z"
        assert item["source_observed_at"] is None
        assert item["delivery_mode"] == delivery
        assert "persisted_at" not in item
    assert first.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"
    assert refreshed.json()["meta"]["as_of"] == "2026-08-24T14:00:30Z"


@pytest.mark.asyncio
async def test_cursor_retains_selected_quote_receipt_while_fresh_query_can_advance(
    migrated_database_url,
):
    admission = None
    for instrument, symbol in ((_AAPL_ID, "AAPL"), (_MSFT_ID, "MSFT")):
        await _seed_instrument(migrated_database_url, instrument_id=instrument, symbol=symbol)
        admission = await _seed_fosun_quote_v2(
            migrated_database_url, instrument_id=instrument, admission=admission
        )
    application = _app(migrated_database_url)
    async with _client(application) as client:
        path = "/internal/v1/market?limit=1"
        first = await client.get(path, headers=_headers("GET", path))
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        await _seed_fosun_quote_v2(
            migrated_database_url,
            instrument_id=_MSFT_ID,
            received_at=_DECISION_TIME + timedelta(seconds=10),
            admission=admission,
        )
        # A newly inserted future receipt is still invisible at the original cutoff.
        path = "/internal/v1/market?limit=2"
        unchanged = await client.get(path, headers=_headers("GET", path))
        assert unchanged.json()["data"]["items"][1]["received_at"] == "2026-08-24T13:59:50Z"
        application.state.utc_clock = lambda: _DECISION_TIME + timedelta(seconds=20)
        path = "/internal/v1/market?" + urlencode({"limit": 1, "cursor": cursor})
        second = await client.get(path, headers=_headers("GET", path))
        path = "/internal/v1/market?limit=2"
        fresh = await client.get(path, headers=_headers("GET", path))
    assert second.status_code == fresh.status_code == 200
    assert second.json()["data"]["items"][0]["received_at"] == "2026-08-24T13:59:50Z"
    assert second.json()["meta"]["as_of"] == "2026-08-24T14:00:00Z"
    assert fresh.json()["data"]["items"][1]["received_at"] == "2026-08-24T14:00:10Z"
    assert fresh.json()["data"]["items"][1]["source_observed_at"] is None
