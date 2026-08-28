"""Strict, bounded Alpaca snapshot contracts."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from nexus.providers.base import InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_snapshot import (
    AlpacaSnapshotProvider,
    normalize_snapshot_payload,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)
from pydantic import SecretStr, ValidationError

from tests.support.provider_fakes import RecordingFixedOriginClient

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/providers/alpaca_snapshot.json"
RECEIVED_AT = datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC)


class _SnapshotSymbolResolver:
    async def resolve(self, instrument_id: InstrumentId, as_of: object) -> str:
        del as_of
        return {AAPL_ID: "AAPL", MSFT_ID: "MSFT"}[instrument_id]


def _request() -> IntradaySnapshotRequest:
    return IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
            SnapshotInstrument(instrument_id=MSFT_ID, symbol="MSFT"),
        ),
        feed="iex",
        delivery_mode="real_time",
    )


def _provider(
    payload: bytes | None = None,
) -> tuple[AlpacaSnapshotProvider, RecordingFixedOriginClient]:
    recorder = RecordingFixedOriginClient(payload=payload or FIXTURE.read_bytes())
    provider = AlpacaSnapshotProvider(
        recorder,
        key_id=SecretStr("id-canary"),
        secret=SecretStr("secret-canary"),
        symbol_resolver=_SnapshotSymbolResolver(),
        feed="iex",
        delivery_mode="real_time",
        now=lambda: RECEIVED_AT,
    )
    return provider, recorder


@pytest.mark.asyncio
async def test_snapshot_provider_preserves_exact_decimal_and_fixed_request() -> None:
    provider, recorder = _provider()

    batch = await provider.fetch_snapshots(_request())

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
        feed="iex",
        delivery_mode="real_time",
    )

    with pytest.raises(MarketProviderPayloadError, match=code):
        await provider.fetch_snapshots(request)

    assert len(recorder.calls) == 1


@pytest.mark.asyncio
async def test_snapshot_payload_over_eight_mib_is_rejected() -> None:
    provider, _ = _provider(b"{" + b" " * (8 * 1024 * 1024) + b"}")
    with pytest.raises(MarketProviderPayloadError, match="ALPACA_SNAPSHOT_SIZE_INVALID"):
        await provider.fetch_snapshots(_request())


def test_snapshot_normalizer_rejects_binary_float_injected_after_json_boundary() -> None:
    request = IntradaySnapshotRequest(
        instruments=(SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),),
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
