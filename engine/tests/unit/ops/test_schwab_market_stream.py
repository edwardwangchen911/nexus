import asyncio
import base64
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.ops.schwab_market_stream import (
    build_parser,
    collect_schwab_stream,
    load_provider_market_stream_archive,
    pack_provider_market_stream_archive,
    parse_schwab_snapshot_batch,
)
from nexus.providers.base import InstrumentId
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.schwab_stream import SchwabStreamCredentials
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)


class Socket:
    def __init__(self, frames: list[object]) -> None:
        self.frames = list(frames)
        self.sent: list[dict[str, object]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def recv(self):
        if not self.frames:
            await asyncio.Event().wait()
        frame = self.frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return json.dumps(frame)

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))


SCHWAB_CREDENTIALS = SchwabStreamCredentials(
    access_token="access-token",
    customer_id="customer",
    correlation_id="correlation",
    channel="channel",
    function_id="function",
)
SCHWAB_LOGIN = {
    "response": [
        {
            "service": "ADMIN",
            "command": "LOGIN",
            "requestid": "1",
            "timestamp": 1_789_912_200_000,
            "content": {"code": 0, "msg": "LOGIN command succeeded"},
        }
    ]
}
SCHWAB_SUBSCRIBED = {
    "response": [
        {
            "service": "LEVELONE_EQUITIES",
            "command": "SUBS",
            "requestid": "2",
            "timestamp": 1_789_912_200_100,
            "content": {"code": 0, "msg": "SUBS command succeeded"},
        }
    ]
}
SCHWAB_HEARTBEAT = {"notify": [{"heartbeat": "1789912200500"}]}
SCHWAB_DATA = {
    "data": [
        {
            "service": "LEVELONE_EQUITIES",
            "timestamp": 1_789_912_201_000,
            "command": "SUBS",
            "content": [
                {
                    "key": "AAPL",
                    "1": 227.14,
                    "2": 227.18,
                    "3": 227.15,
                    "34": 1_789_912_201_000,
                    "35": 1_789_912_200_900,
                    "37": 1_789_912_200_800,
                    "38": 1_789_912_200_700,
                    "39": "ARCX",
                    "40": "XNAS",
                    "41": "XNYS",
                    "delayed": False,
                    "assetMainType": "EQUITY",
                }
            ],
        }
    ]
}

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
EVENT_AT = datetime(2026, 8, 28, 14, 30, 10, tzinfo=UTC)


def _request(*symbols: str) -> IntradaySnapshotRequest:
    ids = {"AAPL": AAPL_ID, "MSFT": MSFT_ID}
    return IntradaySnapshotRequest(
        instruments=tuple(
            SnapshotInstrument(instrument_id=ids[symbol], symbol=symbol) for symbol in symbols
        ),
        symbol_as_of=date(2026, 8, 28),
        bucket_start=datetime(2026, 8, 28, 14, 30, tzinfo=UTC),
        feed="l1",
        delivery_mode="real_time",
    )


def _data(symbol: str = "AAPL", **changes: object) -> dict[str, object]:
    millis = int(EVENT_AT.timestamp() * 1000)
    quote = {
        "key": symbol,
        "1": "227.14",
        "2": "227.18",
        "3": "227.15",
        "34": millis,
        "35": millis - 100,
        "37": millis - 200,
        "38": millis - 300,
        "delayed": False,
        "assetMainType": "EQUITY",
        **changes,
    }
    if quote.get("delayed") == "OMIT":
        del quote["delayed"]
    return {
        "data": [
            {
                "service": "LEVELONE_EQUITIES",
                "timestamp": millis,
                "command": "SUBS",
                "content": [quote],
            }
        ]
    }


async def _packed_archive(tmp_path, symbols: tuple[str, ...], frames: list[object]) -> bytes:
    directory = tmp_path / f"archive-{len(list(tmp_path.iterdir()))}"
    await collect_schwab_stream(
        symbols=symbols,
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        max_frames=len(frames),
        connector=lambda *args, **kwargs: Socket(frames),
    )
    return pack_provider_market_stream_archive(directory)


def test_schwab_stream_credentials_repr_hides_every_private_value() -> None:
    rendered = repr(SCHWAB_CREDENTIALS)

    assert all(
        value not in rendered
        for value in (
            "access-token",
            "customer",
            "correlation",
            "channel",
            "function",
        )
    )


def test_market_stream_cli_supports_only_schwab_without_command_line_credentials() -> None:
    parsed = build_parser().parse_args(
        [
            "schwab",
            "--repo-root",
            "/tmp/repo",
            "--symbol",
            "AAPL",
            "--output-dir",
            "/tmp/archive",
        ]
    )
    assert parsed.source == "schwab"
    assert parsed.symbol == ["AAPL"]
    assert all("token" not in key and "secret" not in key for key in vars(parsed))
    imported = build_parser().parse_args(
        [
            "import",
            "--repo-root",
            "/tmp/repo",
            "--binding",
            "AAPL=019d0000-0000-7000-8000-0000000000c1",
            "--stream-dir",
            "/tmp/archive",
        ]
    )
    assert imported.source == "import"
    assert all("token" not in key and "secret" not in key for key in vars(imported))
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "alltick",
                "--symbol",
                "TSLA.US",
                "--output-dir",
                "/tmp/archive",
            ]
        )


@pytest.mark.asyncio
async def test_schwab_stream_collects_one_bounded_private_archive(tmp_path) -> None:
    socket = Socket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, SCHWAB_HEARTBEAT, SCHWAB_DATA])
    calls: list[tuple[str, dict[str, object]]] = []

    def connector(url: str, **kwargs: object):
        calls.append((url, kwargs))
        return socket

    directory = tmp_path / "schwab"
    result = await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        duration=1,
        max_frames=4,
        connector=connector,
    )

    assert result == {
        "status": "bounded_complete",
        "stop_reason": "FRAME_LIMIT",
        "authenticated": True,
        "subscription_confirmed": True,
        "observation_count": 1,
        "frame_count": 4,
    }
    assert len(calls) == 1
    assert [message["requests"][0]["command"] for message in socket.sent] == [
        "LOGIN",
        "SUBS",
    ]
    archive = load_provider_market_stream_archive(directory)
    observation = archive["observations"][0]
    assert observation.delayed is False
    assert observation.last.as_tuple().exponent == -2
    assert observation.bid_time == datetime.fromtimestamp(1_789_912_200.8, UTC)
    assert observation.ask_time == datetime.fromtimestamp(1_789_912_200.7, UTC)
    assert observation.ask_mic_id == "ARCX"
    assert observation.bid_mic_id == "XNAS"
    assert observation.last_mic_id == "XNYS"
    assert not any("access-token" in item.read_text() for item in directory.iterdir())


@pytest.mark.asyncio
async def test_provider_stream_disconnects_once_and_partial_archive_cannot_be_loaded(
    tmp_path,
) -> None:
    calls = 0

    def connector(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        return Socket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, RuntimeError("private")])

    directory = tmp_path / "partial"
    result = await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        connector=connector,
    )

    assert calls == 1
    assert result["status"] == "partial"
    assert result["stop_reason"] == "DISCONNECTED"
    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_INCOMPLETE"):
        load_provider_market_stream_archive(directory)

    receipt_path = directory / "receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["status"] = "bounded_complete"
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"):
        load_provider_market_stream_archive(directory)


@pytest.mark.asyncio
async def test_cancelled_provider_stream_persists_cancelled_not_timeout(tmp_path) -> None:
    directory = tmp_path / "cancelled"
    task = asyncio.create_task(
        collect_schwab_stream(
            symbols=("AAPL",),
            directory=directory,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=SCHWAB_CREDENTIALS,
            connector=lambda *args, **kwargs: Socket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED]),
        )
    )
    while not directory.joinpath("frame-00001.json").exists():
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    receipt = json.loads((directory / "receipt.json").read_text())
    assert receipt["status"] == "partial"
    assert receipt["stop_reason"] == "CANCELLED"


@pytest.mark.asyncio
async def test_schwab_market_stream_archive_rejects_tampered_frame(tmp_path) -> None:
    directory = tmp_path / "tampered"
    await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        max_frames=3,
        connector=lambda *args, **kwargs: Socket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, SCHWAB_DATA]),
    )
    (directory / "frame-00002.json").write_text("{}")

    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"):
        load_provider_market_stream_archive(directory)


@pytest.mark.asyncio
async def test_schwab_snapshot_parser_selects_latest_trade_and_two_sided_quote(tmp_path) -> None:
    older_millis = int((EVENT_AT - timedelta(seconds=1)).timestamp() * 1000)
    older = _data(
        **{
            "1": "226.90",
            "2": "227.00",
            "3": "226.95",
            "34": older_millis,
            "35": older_millis,
            "37": older_millis,
            "38": older_millis,
        }
    )
    newer = _data(**{"1": "227.14", "2": "227.18", "3": "227.15"})
    payload = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, newer, older],
    )

    batch = parse_schwab_snapshot_batch(payload, _request("AAPL"))

    assert batch.provenance.source == "schwab"
    assert batch.provenance.delivery_mode == "real_time"
    assert batch.raw_payload == payload
    assert batch.snapshots[0].trade is not None
    assert batch.snapshots[0].trade.price == Decimal("227.15")
    assert batch.snapshots[0].quote is not None
    assert batch.snapshots[0].quote.bid == Decimal("227.14")
    assert batch.snapshots[0].quote.ask == Decimal("227.18")


@pytest.mark.asyncio
async def test_schwab_snapshot_parser_allows_observed_requested_subset_and_trade_only(
    tmp_path,
) -> None:
    payload = await _packed_archive(
        tmp_path,
        ("AAPL", "MSFT"),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data(**{"1": None, "2": None})],
    )

    batch = parse_schwab_snapshot_batch(payload, _request("AAPL", "MSFT"))

    assert [snapshot.symbol for snapshot in batch.snapshots] == ["AAPL"]
    assert batch.snapshots[0].trade is not None
    assert batch.snapshots[0].quote is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"2": None}, "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID"),
        ({"delayed": "OMIT"}, "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID"),
        ({"delayed": True}, "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID"),
        ({"35": "bad"}, "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"),
    ],
)
async def test_schwab_snapshot_parser_rejects_unsafe_observations(
    tmp_path, changes: dict[str, object], code: str
) -> None:
    frame_changes = changes
    if changes == {"35": "bad"}:
        frame_changes = {}
    payload = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data(**frame_changes)],
    )
    if changes == {"35": "bad"}:
        envelope = json.loads(payload)
        frame = _data(**changes)
        frame_raw = json.dumps(frame).encode()
        receipt_raw = base64.b64decode(envelope["receipt_base64"])
        receipt = json.loads(receipt_raw)
        receipt["frames"][2]["raw_sha256"] = hashlib.sha256(frame_raw).hexdigest()
        receipt["frames"][2]["raw_byte_length"] = len(frame_raw)
        envelope["frames_base64"][2] = base64.b64encode(frame_raw).decode()
        envelope["receipt_base64"] = base64.b64encode(
            json.dumps(receipt, sort_keys=True).encode()
        ).decode()
        payload = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()

    with pytest.raises(MarketProviderPayloadError, match=code):
        parse_schwab_snapshot_batch(payload, _request("AAPL"))


@pytest.mark.asyncio
async def test_schwab_snapshot_parser_rejects_wrong_symbol_and_incomplete_capture(tmp_path) -> None:
    wrong = await _packed_archive(
        tmp_path,
        ("MSFT",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data("MSFT")],
    )
    with pytest.raises(MarketProviderPayloadError, match="SCHWAB_MARKET_STREAM_BINDINGS_INVALID"):
        parse_schwab_snapshot_batch(wrong, _request("AAPL"))

    directory = tmp_path / "incomplete"
    await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        connector=lambda *args, **kwargs: Socket([SCHWAB_LOGIN, RuntimeError("private")]),
    )
    payload = pack_provider_market_stream_archive(directory)
    with pytest.raises(MarketProviderPayloadError, match="PROVIDER_MARKET_STREAM_INCOMPLETE"):
        parse_schwab_snapshot_batch(payload, _request("AAPL"))
