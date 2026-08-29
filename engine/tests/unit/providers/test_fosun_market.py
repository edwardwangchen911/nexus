"""Provider-owned Fosun market normalization and PIT symbol authority."""

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.fosun import (
    FosunMarketDataProvider,
    FosunSnapshotProvider,
    FosunSnapshotReadAborted,
    decode_fosun_snapshot_evidence,
)
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
    quote_received_at: datetime = RECEIVED_AT
    bar_received_at: datetime = RECEIVED_AT

    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse:
        self.quote_calls.append(codes)
        return RawGatewayResponse(
            operation="fosun_get_quotes",
            gateway_request_id=UUID(
                int=REQUEST_ID.int + len(self.quote_calls) + len(self.bar_calls)
            ),
            request_body_sha256=_request_hash({"codes": list(codes)}),
            raw_payload=self.quote_payload,
            received_at=self.quote_received_at,
        )

    async def read_bars_raw(self, **kwargs: object) -> RawGatewayResponse:
        self.bar_calls.append(kwargs)
        code = kwargs["code"]
        payload = self.bar_payload.replace(b'usAAPL"', f'{code}"'.encode())
        return RawGatewayResponse(
            operation="fosun_get_bars",
            gateway_request_id=UUID(
                int=REQUEST_ID.int + len(self.quote_calls) + len(self.bar_calls)
            ),
            request_body_sha256=_request_hash(
                {
                    "code": kwargs["code"],
                    "ktype": kwargs["ktype"],
                    "start_time": _utc_text(kwargs["start_time"]),
                    "end_time": _utc_text(kwargs["end_time"]),
                    "limit": kwargs["limit"],
                }
            ),
            raw_payload=payload,
            received_at=self.bar_received_at,
        )


@dataclass
class _ReadGuard:
    reject_after: int | None = None
    events: list[tuple[str, str, tuple[str, ...]]] = field(default_factory=list)
    after_count: int = 0

    async def before_read(
        self,
        operation: str,
        instrument_codes: tuple[str, ...],
    ) -> bool:
        self.events.append(("before", operation, instrument_codes))
        return True

    async def after_read(
        self,
        operation: str,
        instrument_codes: tuple[str, ...],
        response: RawGatewayResponse,
    ) -> bool:
        assert response.operation == operation
        self.after_count += 1
        self.events.append(("after", operation, instrument_codes))
        return self.reject_after != self.after_count

def _gateway(
    *,
    bar_timestamp: datetime | None = None,
    quote_received_at: datetime = RECEIVED_AT,
    bar_received_at: datetime = RECEIVED_AT,
) -> _Gateway:
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
    return _Gateway(
        quote_payload,
        bar_payload,
        [],
        [],
        quote_received_at,
        bar_received_at,
    )


def _request_hash(payload: dict[str, object]) -> str:
    import hashlib

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _utc_text(value: object) -> str:
    assert isinstance(value, datetime)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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


def _snapshot_request(
    *,
    bucket_start: datetime = datetime(2026, 8, 29, 14, 30, 15, tzinfo=UTC),
    include_minute_bars: bool = False,
) -> IntradaySnapshotRequest:
    return IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(instrument_id=MSFT_ID, symbol="MSFT"),
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
        ),
        symbol_as_of=date(2026, 8, 29),
        bucket_start=bucket_start,
        include_minute_bars=include_minute_bars,
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
            bucket_start=datetime(2026, 8, 29, 14, 30, tzinfo=UTC),
            include_minute_bars=True,
            feed="l1",
            delivery_mode="unknown",
        )


@pytest.mark.asyncio
async def test_minute_boundary_archives_quote_and_one_bar_response_per_symbol() -> None:
    gateway = _gateway(bar_timestamp=datetime(2026, 8, 29, 14, 29, tzinfo=UTC))
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _snapshot_request(
        bucket_start=datetime(2026, 8, 29, 14, 30, tzinfo=UTC),
        include_minute_bars=True,
    )

    envelope = await provider.fetch_raw_snapshots(request)
    evidence = decode_fosun_snapshot_evidence(
        envelope.raw_payload,
        expected_codes=("usAAPL", "usMSFT"),
        include_minute_bars=True,
        bucket_start=request.bucket_start,
    )
    batch = provider.normalize_raw_snapshots(envelope, request)

    assert [item[1].operation for item in evidence] == [
        "fosun_get_quotes",
        "fosun_get_bars",
        "fosun_get_bars",
    ]
    assert [call["code"] for call in gateway.bar_calls] == ["usAAPL", "usMSFT"]
    assert all(snapshot.minute_bar is not None for snapshot in batch.snapshots)
    assert envelope.received_at == RECEIVED_AT


@pytest.mark.asyncio
async def test_each_minute_bar_and_quote_read_has_its_own_guard_pair() -> None:
    gateway = _gateway(bar_timestamp=datetime(2026, 8, 29, 14, 29, tzinfo=UTC))
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _snapshot_request(
        bucket_start=datetime(2026, 8, 29, 14, 30, tzinfo=UTC),
        include_minute_bars=True,
    )
    guard = _ReadGuard()

    await provider.fetch_raw_snapshots(request, read_guard=guard)

    assert guard.events == [
        ("before", "fosun_get_bars", ("usAAPL",)),
        ("after", "fosun_get_bars", ("usAAPL",)),
        ("before", "fosun_get_bars", ("usMSFT",)),
        ("after", "fosun_get_bars", ("usMSFT",)),
        ("before", "fosun_get_quotes", ("usAAPL", "usMSFT")),
        ("after", "fosun_get_quotes", ("usAAPL", "usMSFT")),
    ]


@pytest.mark.asyncio
async def test_post_read_drift_returns_partial_exact_evidence_and_stops_calls() -> None:
    gateway = _gateway(bar_timestamp=datetime(2026, 8, 29, 14, 29, tzinfo=UTC))
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _snapshot_request(
        bucket_start=datetime(2026, 8, 29, 14, 30, tzinfo=UTC),
        include_minute_bars=True,
    )

    with pytest.raises(FosunSnapshotReadAborted) as raised:
        await provider.fetch_raw_snapshots(
            request,
            read_guard=_ReadGuard(reject_after=1),
        )

    assert raised.value.result_code == "INTRADAY_AUTHORITY_CHANGED_AFTER_FETCH"
    assert raised.value.envelope is not None
    evidence = decode_fosun_snapshot_evidence(raised.value.envelope.raw_payload)
    assert len(evidence) == 1
    assert evidence[0][0] == ("usAAPL",)
    assert evidence[0][1].raw_payload == gateway.bar_payload
    assert [call["code"] for call in gateway.bar_calls] == ["usAAPL"]
    assert gateway.quote_calls == []


@pytest.mark.asyncio
async def test_received_time_regression_aborts_with_all_exact_evidence() -> None:
    gateway = _gateway(
        bar_timestamp=datetime(2026, 8, 29, 14, 29, tzinfo=UTC),
        quote_received_at=RECEIVED_AT - timedelta(seconds=1),
    )
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _snapshot_request(
        bucket_start=datetime(2026, 8, 29, 14, 30, tzinfo=UTC),
        include_minute_bars=True,
    )

    with pytest.raises(FosunSnapshotReadAborted) as raised:
        await provider.fetch_raw_snapshots(request)

    assert raised.value.result_code == "INTRADAY_SNAPSHOT_EVIDENCE_INVALID"
    assert raised.value.envelope is not None
    evidence = decode_fosun_snapshot_evidence(raised.value.envelope.raw_payload)
    assert [item[1].operation for item in evidence] == [
        "fosun_get_quotes",
        "fosun_get_bars",
        "fosun_get_bars",
    ]


@pytest.mark.asyncio
async def test_snapshot_evidence_rejects_body_digest_tampering() -> None:
    gateway = _gateway()
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    request = _snapshot_request()
    envelope = await provider.fetch_raw_snapshots(request)
    tampered = envelope.raw_payload.replace(
        b'"response_body_sha256":"', b'"response_body_sha256":"f'
    )

    with pytest.raises(MarketProviderPayloadError, match="FOSUN_SNAPSHOT_EVIDENCE_INVALID"):
        provider.normalize_raw_snapshots(
            envelope.model_copy(update={"raw_payload": tampered}),
            request,
        )
