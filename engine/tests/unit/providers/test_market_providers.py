from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import BarsRequest, MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.market.alpaca import (
    AlpacaMarketDataProvider,
    MarketProviderDeadlineExceeded,
    MarketProviderPayloadError,
)
from nexus.providers.market.fixture import FixtureMarketDataProvider

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")
START = datetime(2026, 8, 21, 0, tzinfo=UTC)
END = datetime(2026, 8, 22, 0, tzinfo=UTC)
REQUEST = BarsRequest(
    instrument_id=INSTRUMENT_ID,
    start=START,
    end=END,
    interval="1Day",
    adjustment="all",
)


def _page() -> ProviderPage[PriceBar]:
    return ProviderPage(
        items=(
            PriceBar(
                instrument_id=INSTRUMENT_ID,
                session=START.date(),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("1000"),
                observed_at=END,
                available_at=END,
                received_at=END,
            ),
        ),
        next_cursor="next",
        raw_payload=b'{"fixture":true}',
        provenance=MarketDataProvenance(
            source="fixture",
            feed="synthetic",
            adjustment="all",
            interval="1Day",
            mode="fixture",
        ),
    )


@pytest.mark.asyncio
async def test_fixture_provider_returns_only_injected_offline_pages() -> None:
    page = _page()
    provider = FixtureMarketDataProvider({None: page})

    assert await provider.fetch_bars(REQUEST) == page
    with pytest.raises(KeyError, match="fixture cursor"):
        await provider.fetch_bars(REQUEST, "missing")


class ScriptedTransport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls = 0

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None
    ) -> dict[str, Any]:
        del request, cursor
        response = self.responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        assert isinstance(response, dict)
        return response


def _alpaca_payload(*, close: object = "100.5") -> dict[str, Any]:
    return {
        "bars": [
            {
                "t": "2026-08-21T20:00:00Z",
                "o": "100",
                "h": "101",
                "l": "99",
                "c": close,
                "v": "1000",
                "available_at": "2026-08-21T20:15:00Z",
                "revision": "fixture-r1",
                "quality_flags": [],
            }
        ],
        "next_page_token": "next",
        "request_id": "request-1",
    }


@pytest.mark.asyncio
async def test_alpaca_adapter_owns_bounded_retry_and_normalizes_decimal_strings() -> None:
    transport = ScriptedTransport(
        [httpx.ReadTimeout("temporary"), _alpaca_payload()]
    )
    provider = AlpacaMarketDataProvider(
        transport,
        max_attempts=2,
        deadline_seconds=1,
        backoff_seconds=0,
        now=lambda: END,
    )

    page = await provider.fetch_bars(REQUEST)

    assert transport.calls == 2
    assert page.items[0].close == Decimal("100.5")
    assert page.items[0].received_at == END
    assert page.next_cursor == "next"
    assert b'"request_id":"request-1"' in page.raw_payload
    assert page.provenance == MarketDataProvenance(
        source="alpaca",
        feed="sip",
        adjustment="all",
        interval="1Day",
        mode="delayed",
    )


@pytest.mark.asyncio
async def test_alpaca_adapter_rejects_binary_float_financial_payloads() -> None:
    provider = AlpacaMarketDataProvider(
        ScriptedTransport([_alpaca_payload(close=100.5)]),
        deadline_seconds=1,
        backoff_seconds=0,
        now=lambda: END,
    )

    with pytest.raises(MarketProviderPayloadError, match="decimal string"):
        await provider.fetch_bars(REQUEST)


class NeverReturningTransport:
    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None
    ) -> dict[str, Any]:
        del request, cursor
        await __import__("asyncio").Event().wait()
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_alpaca_adapter_enforces_an_absolute_deadline() -> None:
    provider = AlpacaMarketDataProvider(
        NeverReturningTransport(),
        max_attempts=3,
        deadline_seconds=0.01,
        backoff_seconds=0,
        now=lambda: END,
    )

    with pytest.raises(MarketProviderDeadlineExceeded):
        await provider.fetch_bars(REQUEST)
