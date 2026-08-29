"""Strict, bounded Alpaca snapshot contracts."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from nexus.providers.base import InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_snapshot import (
    AlpacaSnapshotProvider,
    normalize_snapshot_envelope,
    normalize_snapshot_payload,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    RawSnapshotEnvelope,
    SnapshotInstrument,
)
from pydantic import SecretStr, ValidationError

from tests.support.provider_fakes import RecordingFixedOriginClient

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/providers/alpaca_snapshot.json"
RECEIVED_AT = datetime(2026, 8, 28, 14, 31, tzinfo=UTC)


class _SnapshotSymbolResolver:
    def __init__(self) -> None:
        self.as_of_dates: list[date] = []

    async def resolve(self, instrument_id: InstrumentId, as_of: object) -> str:
        assert isinstance(as_of, date)
        self.as_of_dates.append(as_of)
        return {AAPL_ID: "AAPL", MSFT_ID: "MSFT"}[instrument_id]


def _request() -> IntradaySnapshotRequest:
    return IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
            SnapshotInstrument(instrument_id=MSFT_ID, symbol="MSFT"),
        ),
        symbol_as_of=date(2026, 8, 28),
        feed="iex",
        delivery_mode="real_time",
    )


def _provider(
    payload: bytes | None = None,
    *,
    received_at: datetime = RECEIVED_AT,
) -> tuple[AlpacaSnapshotProvider, RecordingFixedOriginClient]:
    recorder = RecordingFixedOriginClient(payload=payload or FIXTURE.read_bytes())
    provider = AlpacaSnapshotProvider(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        symbol_resolver=_SnapshotSymbolResolver(),
        feed="iex",
        delivery_mode="real_time",
        now=lambda: received_at,
    )
    return provider, recorder


@pytest.mark.asyncio
async def test_snapshot_provider_preserves_exact_decimal_and_fixed_request() -> None:
    provider, recorder = _provider()

    request = _request()
    batch = await provider.fetch_snapshots(request)

    assert batch.provenance.source == "alpaca"
    assert batch.provenance.feed == "iex"
    assert batch.raw_payload == FIXTURE.read_bytes()
    assert batch.snapshots[0].trade is not None
    assert batch.snapshots[0].trade.price == Decimal("227.16")
    assert batch.snapshots[0].quote is not None
    assert batch.snapshots[0].quote.bid == Decimal("227.14")
    assert batch.snapshots[0].minute_bar is not None
    assert batch.snapshots[0].minute_bar.interval == "1Min"
    assert recorder.calls[0].path_segments == ("v2", "stocks", "snapshots")
    assert recorder.calls[0].query == (
        ("currency", "USD"),
        ("feed", "iex"),
        ("symbols", "AAPL,MSFT"),
    )


@pytest.mark.asyncio
async def test_snapshot_received_at_is_captured_after_transport_returns() -> None:
    resolver = _SnapshotSymbolResolver()
    recorder = RecordingFixedOriginClient(payload=FIXTURE.read_bytes())
    response_received_at = datetime(2026, 8, 28, 14, 31, tzinfo=UTC)
    provider = AlpacaSnapshotProvider(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        symbol_resolver=resolver,
        feed="iex",
        delivery_mode="real_time",
        now=lambda: response_received_at,
    )

    request = _request()
    batch = await provider.fetch_snapshots(request)

    assert resolver.as_of_dates == [request.symbol_as_of, request.symbol_as_of]
    assert batch.received_at == response_received_at
    assert batch.snapshots[0].minute_bar is not None
    assert batch.snapshots[0].minute_bar.bar.received_at == response_received_at


@pytest.mark.asyncio
async def test_snapshot_does_not_publish_an_in_progress_minute_bar() -> None:
    provider, _ = _provider(
        received_at=datetime(2026, 8, 28, 14, 30, 59, 999_999, tzinfo=UTC)
    )

    batch = await provider.fetch_snapshots(_request())

    assert batch.snapshots[0].trade is not None
    assert batch.snapshots[0].quote is not None
    assert batch.snapshots[0].minute_bar is None


@pytest.mark.asyncio
async def test_snapshot_rejects_provider_fact_after_response_receipt() -> None:
    payload = b'{"AAPL":{"latestTrade":{"p":1,"t":"2026-08-28T14:30:18Z"}}}'
    recorder = RecordingFixedOriginClient(payload=payload)
    response_received_at = datetime(2026, 8, 28, 14, 30, 17, tzinfo=UTC)
    provider = AlpacaSnapshotProvider(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        symbol_resolver=_SnapshotSymbolResolver(),
        feed="iex",
        delivery_mode="real_time",
        now=lambda: response_received_at,
    )
    request = IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
        symbol_as_of=date(2026, 8, 28),
        feed="iex",
        delivery_mode="real_time",
    )

    with pytest.raises(MarketProviderPayloadError, match="ALPACA_SNAPSHOT_FUTURE_FACT"):
        await provider.fetch_snapshots(request)


@pytest.mark.parametrize(
    "instruments",
    (
        (),
        tuple(
            SnapshotInstrument(instrument_id=InstrumentId(UUID(int=index + 1)), symbol=f"A{index}")
            for index in range(31)
        ),
        (
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="MSFT"),
        ),
        (
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
            SnapshotInstrument(instrument_id=MSFT_ID, symbol="AAPL"),
        ),
    ),
)
def test_snapshot_request_rejects_unbounded_or_duplicate_instruments(
    instruments: tuple[SnapshotInstrument, ...],
) -> None:
    with pytest.raises(ValidationError):
        IntradaySnapshotRequest(
            instruments=instruments,
            symbol_as_of=date(2026, 8, 28),
            feed="iex",
            delivery_mode="real_time",
        )


@pytest.mark.parametrize("symbol", ("aapl", "../AAPL", "AAPL?x=1", "A" * 16))
def test_snapshot_instrument_rejects_unsafe_symbols(symbol: str) -> None:
    with pytest.raises(ValidationError):
        SnapshotInstrument(instrument_id=AAPL_ID, symbol=symbol)


@pytest.mark.parametrize(
    ("feed", "delivery_mode"),
    (("iex", "delayed"), ("sip", "delayed"), ("delayed_sip", "real_time")),
)
def test_snapshot_request_rejects_feed_delivery_mismatch(
    feed: str,
    delivery_mode: str,
) -> None:
    with pytest.raises(ValidationError):
        IntradaySnapshotRequest(
            instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
            symbol_as_of=date(2026, 8, 28),
            feed=feed,
            delivery_mode=delivery_mode,
        )


@pytest.mark.parametrize(
    ("payload", "code"),
    (
        (b'{"AAPL":{},"AAPL":{}}', "ALPACA_DUPLICATE_KEY"),
        (b'{"GOOG":{}}', "ALPACA_SNAPSHOT_TICKER_MISMATCH"),
        (b'{"AAPL":{"unknown":{}}}', "ALPACA_SNAPSHOT_FIELD_UNKNOWN"),
        (
            b'{"AAPL":{"latestQuote":{"bp":2,"ap":1,"t":"2026-08-28T14:30:14Z"}}}',
            "ALPACA_SNAPSHOT_SPREAD_INVALID",
        ),
        (
            b'{"AAPL":{"minuteBar":{"o":1,"h":2,"l":0.5,"c":1,"v":-1,"t":"2026-08-28T14:30:00Z"}}}',
            "ALPACA_SNAPSHOT_VOLUME_INVALID",
        ),
        (
            b'{"AAPL":{"minuteBar":{"o":3,"h":2,"l":1,"c":1,"v":1,"t":"2026-08-28T14:30:00Z"}}}',
            "ALPACA_SNAPSHOT_OHLC_INVALID",
        ),
        (
            b'{"AAPL":{"latestTrade":{"p":1,"t":"2026-08-28T14:30:14"}}}',
            "ALPACA_SNAPSHOT_TIME_INVALID",
        ),
        (
            b'{"AAPL":{"latestTrade":{"p":NaN,"t":"2026-08-28T14:30:14Z"}}}',
            "ALPACA_JSON_CONSTANT_INVALID",
        ),
    ),
)
@pytest.mark.asyncio
async def test_snapshot_payload_contract_fails_closed(payload: bytes, code: str) -> None:
    provider, recorder = _provider(payload)
    request = IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
        symbol_as_of=date(2026, 8, 28),
        feed="iex",
        delivery_mode="real_time",
    )

    with pytest.raises(MarketProviderPayloadError, match=code):
        await provider.fetch_snapshots(request)

    assert len(recorder.calls) == 1


@pytest.mark.asyncio
async def test_snapshot_transport_returns_raw_before_contract_validation() -> None:
    payload = b'{"AAPL":{},"AAPL":{}}'
    provider, recorder = _provider(payload)
    request = IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
        symbol_as_of=date(2026, 8, 28),
        feed="iex",
        delivery_mode="real_time",
    )

    envelope = await provider.fetch_raw_snapshots(request)

    assert envelope.raw_payload == payload
    assert len(recorder.calls) == 1
    with pytest.raises(MarketProviderPayloadError, match="ALPACA_DUPLICATE_KEY"):
        normalize_snapshot_envelope(envelope, request=request)


def test_snapshot_envelope_rejects_mismatched_provenance_during_normalization() -> None:
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"AAPL":{}}',
        received_at=RECEIVED_AT,
        provenance={
            "source": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "interval": "1Min",
            "mode": "read_only",
            "delivery_mode": "real_time",
        },
    )

    with pytest.raises(MarketProviderPayloadError, match="ALPACA_INTRADAY_AUTHORITY_MISMATCH"):
        normalize_snapshot_envelope(envelope, request=_request())


@pytest.mark.asyncio
async def test_snapshot_payload_over_eight_mib_is_rejected() -> None:
    provider, _ = _provider(b"{" + b" " * (8 * 1024 * 1024) + b"}")
    with pytest.raises(MarketProviderPayloadError, match="ALPACA_SNAPSHOT_SIZE_INVALID"):
        await provider.fetch_snapshots(_request())


def test_snapshot_normalizer_rejects_binary_float_injected_after_json_boundary() -> None:
    request = IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
        symbol_as_of=date(2026, 8, 28),
        feed="iex",
        delivery_mode="real_time",
    )
    with pytest.raises(MarketProviderPayloadError, match="ALPACA_SNAPSHOT_TRADE_INVALID"):
        normalize_snapshot_payload(
            {
                "AAPL": {
                    "latestTrade": {
                        "p": 227.16,
                        "t": "2026-08-28T14:30:14Z",
                    }
                }
            },
            raw_payload=b"fixture",
            request=request,
            received_at=RECEIVED_AT,
        )
