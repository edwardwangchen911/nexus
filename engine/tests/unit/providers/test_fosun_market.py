"""Provider-owned Fosun market normalization and PIT symbol authority."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun import FosunMarketDataProvider, FosunSnapshotProvider
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
REQUEST_ID = UUID("0198d400-3000-7000-8000-000000000022")
RECEIVED_AT = datetime(2026, 8, 29, 14, 31, tzinfo=UTC)


class _Resolver:
    def __init__(self) -> None:
        self.calls: list[tuple[InstrumentId, date]] = []

    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str:
        self.calls.append((instrument_id, as_of))
        return {AAPL_ID: "AAPL", MSFT_ID: "MSFT"}[instrument_id]


@dataclass
class _Gateway:
    quote_payload: bytes
    bar_payload: bytes
    quote_calls: list[tuple[str, ...]]
    bar_calls: list[dict[str, object]]

    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse:
        self.quote_calls.append(codes)
        return RawGatewayResponse(
            operation="fosun_get_quotes",
            gateway_request_id=REQUEST_ID,
            request_body_sha256="a" * 64,
            raw_payload=self.quote_payload,
            received_at=RECEIVED_AT,
        )

    async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse:
        self.bar_calls.append(kwargs)
        return RawGatewayResponse(
            operation="fosun_get_bars",
            gateway_request_id=REQUEST_ID,
            request_body_sha256="b" * 64,
            raw_payload=self.bar_payload,
            received_at=RECEIVED_AT,
        )


def _gateway(*, bar_timestamp: datetime | None = None) -> _Gateway:
    quote_payload = json.dumps(
        {
            "items": [
                {
                    "kind": "quote",
                    "instrument_code": "usAAPL",
                    "price": "227.1600",
                    "as_of": RECEIVED_AT.isoformat(),
                },
                {
                    "kind": "quote",
                    "instrument_code": "usMSFT",
                    "price": None,
                    "as_of": RECEIVED_AT.isoformat(),
                },
            ],
            "source_request_id": "synthetic-provider-request",
            "has_more": False,
            "next_cursor": None,
            "as_of": RECEIVED_AT.isoformat(),
            "quality": "PARTIAL",
        },
        separators=(",", ":"),
    ).encode()
    timestamp = bar_timestamp or datetime(2026, 8, 28, 20, tzinfo=UTC)
    interval = "minute" if bar_timestamp is not None else "day"
    bar_payload = json.dumps(
        {
            "items": [
                {
                    "kind": "bar",
                    "instrument_code": "usAAPL",
                    "interval": interval,
                    "timestamp": timestamp.isoformat(),
                    "open": "100.10",
                    "high": "103.20",
                    "low": "99.90",
                    "close": "102.30",
                    "volume": "123456",
                }
            ],
            "source_request_id": "synthetic-provider-request",
            "has_more": False,
            "next_cursor": None,
            "as_of": RECEIVED_AT.isoformat(),
            "quality": "PARTIAL",
        },
        separators=(",", ":"),
    ).encode()
    return _Gateway(quote_payload, bar_payload, [], [])


def _daily_request(**updates: object) -> BarsRequest:
    values: dict[str, object] = {
        "instrument_id": AAPL_ID,
        "start": datetime(2026, 8, 1, tzinfo=UTC),
        "end": datetime(2026, 8, 29, tzinfo=UTC),
        "interval": "1Day",
        "adjustment": "raw",
    }
    values.update(updates)
    return BarsRequest.model_validate(values)


@pytest.mark.asyncio
async def test_daily_provider_resolves_pit_code_and_preserves_exact_bar_values() -> None:
    gateway = _gateway()
    resolver = _Resolver()
    provider = FosunMarketDataProvider(
        gateway,
        symbol_resolver=resolver,
        delivery_mode="unknown",
    )
    request = _daily_request()

    page = await provider.fetch_bars(request)

    assert resolver.calls == [
        (AAPL_ID, request.end.date()),
        (AAPL_ID, request.end.date()),
    ]
    assert gateway.bar_calls == [
        {
            "code": "usAAPL",
            "ktype": "day",
            "start_time": request.start,
            "end_time": request.end,
            "limit": 500,
        }
    ]
    assert page.items[0].close == Decimal("102.30")
    assert page.items[0].observed_at == datetime(2026, 8, 28, 20, tzinfo=UTC)
    assert page.items[0].available_at == RECEIVED_AT
    assert page.items[0].received_at == RECEIVED_AT
    assert page.raw_payload == gateway.bar_payload
    assert page.provenance.model_dump() == {
        "source": "fosun",
        "feed": "l1",
        "adjustment": "raw",
        "interval": "1Day",
        "mode": "read_only",
        "delivery_mode": "unknown",
    }


@pytest.mark.asyncio
async def test_daily_raw_fetch_precedes_contract_parsing() -> None:
    gateway = _gateway()
    gateway.bar_payload = b'{"items":[],"items":[]}'
    provider = FosunMarketDataProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _daily_request()

    raw = await provider.fetch_raw_bars(request)

    assert raw.raw_payload == gateway.bar_payload
    with pytest.raises(MarketProviderPayloadError, match="FOSUN_BAR_CONTRACT_INVALID"):
        await provider.normalize_bars(request, raw)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bar_request", "cursor"),
    [
        (_daily_request(adjustment="all"), None),
        (
            _daily_request(
                start=datetime(2025, 1, 1, tzinfo=UTC),
                end=datetime(2026, 8, 29, tzinfo=UTC),
            ),
            None,
        ),
        (_daily_request(), "cursor"),
    ],
)
async def test_invalid_daily_contract_stops_before_gateway(
    bar_request: BarsRequest,
    cursor: str | None,
) -> None:
    gateway = _gateway()
    provider = FosunMarketDataProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )

    with pytest.raises(MarketProviderPayloadError):
        await provider.fetch_bars(bar_request, cursor)

    assert gateway.bar_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("received_at", "expected_count"),
    [
        (datetime(2026, 8, 29, 14, 30, 59, tzinfo=UTC), 0),
        (datetime(2026, 8, 29, 14, 31, tzinfo=UTC), 1),
    ],
)
async def test_minute_provider_only_publishes_completed_bars(
    received_at: datetime,
    expected_count: int,
) -> None:
    gateway = _gateway(bar_timestamp=datetime(2026, 8, 29, 14, 30, tzinfo=UTC))
    gateway.bar_payload = gateway.bar_payload.replace(
        RECEIVED_AT.isoformat().encode(), received_at.isoformat().encode()
    )
    provider = FosunMarketDataProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _daily_request(
        start=datetime(2026, 8, 29, 14, 29, tzinfo=UTC),
        end=datetime(2026, 8, 29, 14, 31, tzinfo=UTC),
        interval="1Min",
    )
    raw = RawGatewayResponse(
        operation="fosun_get_bars",
        gateway_request_id=REQUEST_ID,
        request_body_sha256="b" * 64,
        raw_payload=gateway.bar_payload,
        received_at=received_at,
    )

    page = await provider.normalize_bars(request, raw)

    assert len(page.items) == expected_count


def _snapshot_request() -> IntradaySnapshotRequest:
    return IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(instrument_id=MSFT_ID, symbol="MSFT"),
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
        ),
        symbol_as_of=date(2026, 8, 29),
        feed="l1",
        delivery_mode="unknown",
    )


@pytest.mark.asyncio
async def test_snapshot_provider_maps_last_only_and_marks_receipt_limitations() -> None:
    gateway = _gateway()
    resolver = _Resolver()
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=resolver,
        delivery_mode="unknown",
    )
    request = _snapshot_request()

    envelope = await provider.fetch_raw_snapshots(request)
    batch = provider.normalize_raw_snapshots(envelope, request)

    assert gateway.quote_calls == [("usAAPL", "usMSFT")]
    assert resolver.calls == [
        (MSFT_ID, request.symbol_as_of),
        (AAPL_ID, request.symbol_as_of),
    ]
    assert tuple(snapshot.symbol for snapshot in batch.snapshots) == ("MSFT", "AAPL")
    assert batch.snapshots[0].trade is None
    assert batch.snapshots[0].quote is None
    assert batch.snapshots[1].trade is not None
    assert batch.snapshots[1].trade.price == Decimal("227.1600")
    assert batch.snapshots[1].trade.event_kind == "snapshot"
    assert batch.snapshots[1].trade.observed_at == RECEIVED_AT
    assert batch.snapshots[1].trade.quality_flags == (
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    )
    assert batch.snapshots[1].quote is None
    assert batch.snapshots[1].minute_bar is None
    assert batch.provenance.source == "fosun"
    assert batch.provenance.feed == "l1"
    assert batch.provenance.delivery_mode == "unknown"


@pytest.mark.asyncio
async def test_snapshot_request_rejects_more_than_ten_fosun_symbols() -> None:
    instruments = tuple(
        SnapshotInstrument(
            instrument_id=InstrumentId(UUID(int=index + 1)),
            symbol=f"A{index}",
        )
        for index in range(11)
    )

    with pytest.raises(ValueError):
        IntradaySnapshotRequest(
            instruments=instruments,
            symbol_as_of=date(2026, 8, 29),
            feed="l1",
            delivery_mode="unknown",
        )
