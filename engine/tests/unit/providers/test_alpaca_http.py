"""Exact Alpaca request and raw-evidence transport contracts."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
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


@pytest.mark.parametrize(
    "raw",
    (
        b'{"symbol":"AAPL","bars":[]}',
        b'{"symbol":"AAPL","bars":[],"next_page_token":""}',
        b'{"symbol":"AAPL","bars":[],"next_page_token":false}',
    ),
)
@pytest.mark.asyncio
async def test_provider_cannot_infer_page_exhaustion_from_invalid_cursor(raw: bytes) -> None:
    recorder = RecordingFixedOriginClient(payload=raw)
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        feed="sip",
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )
    provider = AlpacaMarketDataProvider(transport, feed="sip", delivery_mode="delayed")

    with pytest.raises(MarketProviderPayloadError, match="next_page_token"):
        await provider.fetch_bars(canonical_bars_request())

    assert len(recorder.calls) == 1


@pytest.mark.parametrize("cursor", (None, "page_token-1=="))
@pytest.mark.asyncio
async def test_provider_retains_explicit_terminal_or_continuation_cursor(
    cursor: str | None,
) -> None:
    raw = json.dumps({"symbol": "AAPL", "bars": [], "next_page_token": cursor}).encode()
    recorder = RecordingFixedOriginClient(payload=raw)
    provider = AlpacaMarketDataProvider(
        AlpacaHttpTransport(
            recorder,
            key_id=SecretStr("id-canary"),
            secret=SecretStr("secret-canary"),
            feed="sip",
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
        ),
        feed="sip",
        delivery_mode="delayed",
    )

    page = await provider.fetch_bars(canonical_bars_request())

    assert page.next_cursor == cursor
    assert page.raw_payload == raw
    assert page.items == ()
    assert len(recorder.calls) == 1


@pytest.mark.asyncio
async def test_transport_builds_only_the_frozen_bars_request() -> None:
    """A changed path/query/header contract must fail before production use."""
    raw = b'{ "symbol":"AAPL", "bars": [], "next_page_token": null, "request_id": "request-1" }\n'
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
        ("asof", "2026-08-28"),
        ("currency", "USD"),
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
            b'{"symbol":"AAPL","bars":[{"t":"2026-08-21T20:00:00Z",'
            b'"o":100,"h":101,"l":99,"c":100.5,"v":1000}]}'
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
    recorder = RecordingFixedOriginClient(payload=b'{"symbol":"BRK.B","bars": []}')
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
        b'"l":"99","c":"100.5","v":"1000"}], "next_page_token":null }\n'
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
                ],
                "next_page_token": None,
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


class _RenamedSymbolResolver:
    """Synthetic instrument-master boundary, not a mocked provider or normalizer."""

    def __init__(self) -> None:
        self.dates: list[date] = []

    async def resolve(self, instrument_id, as_of: date) -> str:
        assert instrument_id == canonical_bars_request().instrument_id
        self.dates.append(as_of)
        return "FB" if as_of < date(2022, 6, 9) else "META"


def _http_provider(client, resolver, received_at):
    fixed = FixedOriginHttpClient(
        provider_name="alpaca",
        origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
        policy=HttpRequestPolicy(max_attempts=1),
        client=client,
    )
    return AlpacaMarketDataProvider(
        AlpacaHttpTransport(
            fixed,
            key_id=SecretStr("id-canary"),
            secret=SecretStr("secret-canary"),
            feed="sip",
            symbol_resolver=resolver,
        ),
        feed="sip",
        delivery_mode="delayed",
        now=lambda: received_at,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("end,symbol", [("2022-06-08", "FB"), ("2022-06-11", "META")])
@pytest.mark.parametrize("adjustment", ["raw", "split", "all"])
async def test_real_http_stack_keeps_local_and_remote_rename_date_on_every_page(
    end,
    symbol,
    adjustment,
    caplog,
):
    resolver = _RenamedSymbolResolver()
    request = canonical_bars_request().model_copy(
        update={
            "start": datetime(2022, 6, 6, tzinfo=UTC),
            "end": datetime.fromisoformat(end).replace(tzinfo=UTC),
            "adjustment": adjustment,
        }
    )
    received_at = datetime(2026, 8, 28, 1, tzinfo=UTC)
    calls = []
    raws = []

    def respond(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        assert incoming.method == "GET"
        assert incoming.url.host == "data.alpaca.markets"
        assert incoming.url.path == f"/v2/stocks/{symbol}/bars"
        assert incoming.url.params["asof"] == end
        assert incoming.url.params["currency"] == "USD"
        assert incoming.url.params["adjustment"] == adjustment
        assert incoming.headers["APCA-API-KEY-ID"] == "id-canary"
        assert incoming.headers["APCA-API-SECRET-KEY"] == "secret-canary"
        assert "canary" not in str(incoming.url)
        if len(calls) == 1:
            assert "page_token" not in incoming.url.params
            # No currency/feed/adjustment echo is required by the response schema.
            raw = (
                b'{"symbol":"' + symbol.encode() + b'","bars":[{"t":"2022-06-06T04:00:00Z",'
                b'"o":100,"h":101,"l":99,"c":100.10000000000000001,"v":1000}],'
                b'"next_page_token":"next-1"}'
            )
        else:
            assert len(calls) == 2
            assert incoming.url.params["page_token"] == "next-1"
            raw = json.dumps({"symbol": symbol, "bars": [], "next_page_token": None}).encode()
        raws.append(raw)
        return httpx.Response(200, content=raw)

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        provider = _http_provider(client, resolver, received_at)
        first = await provider.fetch_bars(request)
        last = await provider.fetch_bars(request, first.next_cursor)
    assert resolver.dates == [request.end.date()] * 2
    assert first.raw_payload == raws[0] and last.raw_payload == raws[1]
    assert first.items[0].close == Decimal("100.10000000000000001")
    assert first.items[0].instrument_id == request.instrument_id
    assert first.items[0].received_at == first.items[0].available_at == received_at
    assert first.provenance.adjustment == adjustment
    assert last.items == () and last.next_cursor is None
    assert "id-canary" not in caplog.text and "secret-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "symbol_field",
    [
        {},
        {"symbol": None},
        {"symbol": False},
        {"symbol": 1},
        {"symbol": []},
        {"symbol": {}},
        {"symbol": ""},
        {"symbol": "aapl"},
        {"symbol": "OTHER"},
        {"symbol": "secret-canary"},
    ],
)
async def test_real_http_stack_rejects_unbound_response_identity_even_for_empty_page(
    symbol_field,
    caplog,
):
    calls = []
    raw = json.dumps(
        {**symbol_field, "bars": [], "next_page_token": None, "opaque": "raw-private-canary"}
    ).encode()

    def respond(request):
        calls.append(request)
        return httpx.Response(200, content=raw)

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        provider = _http_provider(
            client,
            FixedInstrumentSymbolResolver("AAPL"),
            datetime(2026, 8, 28, tzinfo=UTC),
        )
        with pytest.raises(
            MarketProviderPayloadError, match="^ALPACA_RESPONSE_SYMBOL_INVALID$"
        ) as caught:
            await provider.fetch_bars(canonical_bars_request())
    assert len(calls) == 1
    for marker in ("id-canary", "secret-canary", "raw-private-canary"):
        assert marker not in str(caught.value) and marker not in caplog.text
