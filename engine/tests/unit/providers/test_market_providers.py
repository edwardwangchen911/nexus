from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import BarsRequest, MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.market.alpaca import (
    AlpacaMarketDataProvider,
    AlpacaRawResponse,
    MarketProviderPayloadError,
)
from nexus.providers.market.fixture import FixtureMarketDataProvider
from pydantic import ValidationError

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


@pytest.mark.parametrize(
    ("mode", "delivery_mode"),
    [
        ("delayed", "real_time"),
        ("fixture", "delayed"),
        ("read_only", "fixture"),
    ],
)
def test_provider_provenance_rejects_contradictory_delivery_authority(
    mode: str,
    delivery_mode: str,
) -> None:
    with pytest.raises(ValidationError, match="delivery mode contradicts provider mode"):
        MarketDataProvenance(
            source="alpaca",
            feed="sip",
            adjustment="all",
            interval="1Day",
            mode=mode,
            delivery_mode=delivery_mode,
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
            delivery_mode="fixture",
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
    ) -> AlpacaRawResponse:
        del request, cursor
        response = self.responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        assert isinstance(response, AlpacaRawResponse)
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


def _alpaca_response(*, close: object = "100.5") -> AlpacaRawResponse:
    return AlpacaRawResponse(
        payload=_alpaca_payload(close=close),
        raw_payload=b'{"fixture":"exact-raw"}',
        request_id="request-1",
    )


@pytest.mark.asyncio
async def test_alpaca_adapter_normalizes_one_transport_response() -> None:
    transport = ScriptedTransport([_alpaca_response()])
    provider = AlpacaMarketDataProvider(
        transport,
        feed="sip",
        delivery_mode="delayed",
        now=lambda: END,
    )

    page = await provider.fetch_bars(REQUEST)

    assert transport.calls == 1
    assert page.items[0].close == Decimal("100.5")
    assert page.items[0].received_at == END
    assert page.next_cursor == "next"
    assert page.raw_payload == b'{"fixture":"exact-raw"}'
    assert page.provenance == MarketDataProvenance(
        source="alpaca",
        feed="sip",
        adjustment="all",
        interval="1Day",
        mode="delayed",
        delivery_mode="delayed",
    )


@pytest.mark.asyncio
async def test_alpaca_adapter_rejects_binary_float_financial_payloads() -> None:
    provider = AlpacaMarketDataProvider(
        ScriptedTransport([_alpaca_response(close=100.5)]),
        feed="sip",
        delivery_mode="delayed",
        now=lambda: END,
    )

    with pytest.raises(MarketProviderPayloadError, match="decimal string"):
        await provider.fetch_bars(REQUEST)


@pytest.mark.asyncio
async def test_alpaca_adapter_never_retries_transport_errors() -> None:
    transport = ScriptedTransport(
        [httpx.ReadTimeout("temporary"), _alpaca_response()]
    )
    provider = AlpacaMarketDataProvider(
        transport,
        feed="sip",
        delivery_mode="delayed",
        now=lambda: END,
    )

    with pytest.raises(httpx.ReadTimeout):
        await provider.fetch_bars(REQUEST)
    assert transport.calls == 1
