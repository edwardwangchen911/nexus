"""Exact Alpaca request and raw-evidence transport contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nexus.providers.market.alpaca import (
    AlpacaMarketDataProvider,
    AlpacaRawResponse,
    MarketProviderPayloadError,
)
from nexus.providers.market.alpaca_http import AlpacaHttpTransport
from pydantic import SecretStr

from tests.support.provider_fakes import (
    FixedInstrumentSymbolResolver,
    RecordingFixedOriginClient,
    canonical_bars_request,
)


@pytest.mark.asyncio
async def test_transport_builds_only_the_frozen_bars_request() -> None:
    """A changed path/query/header contract must fail before production use."""
    raw = b'{ "bars": [], "next_page_token": null, "request_id": "request-1" }\n'
    recorder = RecordingFixedOriginClient(payload=raw)
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        feed="sip",
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )

    response = await transport.fetch_bars(canonical_bars_request(), None)

    assert response.raw_payload == raw
    assert response.request_id == "request-1"
    assert recorder.calls[0].path_segments == ("v2", "stocks", "AAPL", "bars")
    assert recorder.calls[0].query == (
        ("adjustment", "all"),
        ("end", "2026-08-28T00:00:00Z"),
        ("feed", "sip"),
        ("limit", "10000"),
        ("sort", "asc"),
        ("start", "2026-08-01T00:00:00Z"),
        ("timeframe", "1Day"),
    )
    assert recorder.calls[0].header_names == (
        "APCA-API-KEY-ID",
        "APCA-API-SECRET-KEY",
    )


@pytest.mark.asyncio
async def test_transport_parses_provider_numbers_as_exact_decimals() -> None:
    recorder = RecordingFixedOriginClient(
        payload=(
            b'{"bars":[{"t":"2026-08-21T20:00:00Z","o":100,"h":101,"l":99,"c":100.5,"v":1000}]}'
        )
    )
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id"),
        secret=SecretStr("secret"),
        feed="iex",
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )

    response = await transport.fetch_bars(canonical_bars_request(), None)
    bar = response.payload["bars"][0]

    assert bar["c"] == Decimal("100.5")
    assert isinstance(bar["c"], Decimal)


@pytest.mark.asyncio
async def test_transport_adds_one_validated_page_token_last() -> None:
    """An opaque cursor must not become a generic query-string forwarding seam."""
    recorder = RecordingFixedOriginClient(payload=b'{"bars": []}')
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id"),
        secret=SecretStr("secret"),
        feed="iex",
        symbol_resolver=FixedInstrumentSymbolResolver("BRK.B"),
    )

    await transport.fetch_bars(canonical_bars_request(), "page_token-1==")

    assert recorder.calls[0].path_segments == ("v2", "stocks", "BRK.B", "bars")
    assert recorder.calls[0].query[-1] == ("page_token", "page_token-1==")


@pytest.mark.parametrize(
    ("symbol", "cursor"),
    (
        ("BRK/B", None),
        ("AAPL", "https://evil.test"),
        ("AAPL", "token/escape"),
        ("AAPL", "x" * 513),
    ),
)
@pytest.mark.asyncio
async def test_symbol_and_cursor_escapes_fail_before_http(
    symbol: str,
    cursor: str | None,
) -> None:
    """Unsafe resolver/cursor output must never reach the credentialed client."""
    recorder = RecordingFixedOriginClient(payload=b"{}")
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id"),
        secret=SecretStr("secret"),
        feed="iex",
        symbol_resolver=FixedInstrumentSymbolResolver(symbol),
    )

    with pytest.raises(MarketProviderPayloadError):
        await transport.fetch_bars(canonical_bars_request(), cursor)

    assert recorder.calls == []


class _ScriptedRawTransport:
    def __init__(self, response: AlpacaRawResponse) -> None:
        self._response = response
        self.calls = 0

    async def fetch_bars(self, *_: object) -> AlpacaRawResponse:
        self.calls += 1
        return self._response


@pytest.mark.asyncio
async def test_provider_preserves_raw_bytes_and_uses_conservative_availability() -> None:
    """Re-serializing evidence or backdating availability would break point-in-time safety."""
    raw = (
        b'{ "bars": [{"t":"2026-08-21T20:00:00Z","o":"100","h":"101",'
        b'"l":"99","c":"100.5","v":"1000"}] }\n'
    )
    transport = _ScriptedRawTransport(
        AlpacaRawResponse(
            payload={
                "bars": [
                    {
                        "t": "2026-08-21T20:00:00Z",
                        "o": "100",
                        "h": "101",
                        "l": "99",
                        "c": "100.5",
                        "v": "1000",
                    }
                ]
            },
            raw_payload=raw,
            request_id=None,
        )
    )
    received_at = datetime(2026, 8, 28, 1, tzinfo=UTC)
    provider = AlpacaMarketDataProvider(
        transport,
        feed="iex",
        delivery_mode="real_time",
        now=lambda: received_at,
    )

    page = await provider.fetch_bars(canonical_bars_request())

    assert transport.calls == 1
    assert page.raw_payload == raw
    assert page.items[0].close == Decimal("100.5")
    assert page.items[0].available_at == received_at
    assert page.items[0].received_at == received_at
    assert page.provenance.feed == "iex"
    assert page.provenance.mode == "read_only"
    assert page.provenance.delivery_mode == "real_time"
