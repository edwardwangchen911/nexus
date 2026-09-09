import asyncio
import json
from decimal import Decimal

import pytest
from nexus.ops.alpaca_market_stream import collect_market_stream, load_market_stream_archive


class Socket:
    def __init__(self, frames: list[object]) -> None:
        self.frames = list(frames)
        self.sent: list[object] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def recv(self):
        if not self.frames:
            await asyncio.sleep(10)
        frame = self.frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return json.dumps(frame)

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))


class BlockingSocket(Socket):
    async def recv(self):
        if self.frames:
            return await super().recv()
        await asyncio.Event().wait()


CONNECTED = [{"T": "success", "msg": "connected"}]
AUTHENTICATED = [{"T": "success", "msg": "authenticated"}]
SUBSCRIBED = [
    {
        "T": "subscription",
        "trades": ["AAPL"],
        "quotes": ["AAPL"],
        "bars": [],
        "updatedBars": [],
        "dailyBars": [],
        "statuses": [],
        "lulds": [],
        "corrections": ["AAPL"],
        "cancelErrors": ["AAPL"],
    }
]
MINIMAL_SUBSCRIBED = [
    {
        "T": "subscription",
        "trades": ["AAPL"],
        "quotes": ["AAPL"],
        "corrections": ["AAPL"],
        "cancelErrors": ["AAPL"],
    }
]
DATA = [
    {
        "T": "t",
        "S": "AAPL",
        "i": 17,
        "x": "V",
        "p": 227.15,
        "s": 2,
        "c": ["@"],
        "t": "2026-09-09T14:30:01.123456789Z",
        "z": "C",
    },
    {
        "T": "q",
        "S": "AAPL",
        "bx": "V",
        "bp": 227.14,
        "bs": 1,
        "ax": "V",
        "ap": 227.18,
        "as": 1,
        "c": ["R"],
        "t": "2026-09-09T14:30:01.223456789Z",
        "z": "C",
    },
]


@pytest.mark.asyncio
async def test_bounded_iex_stream_roundtrips_latest_trade_and_quote(tmp_path) -> None:
    socket = Socket([CONNECTED, AUTHENTICATED, SUBSCRIBED, DATA])
    calls: list[tuple[str, dict[str, object]]] = []

    def connector(url: str, **kwargs: object):
        calls.append((url, kwargs))
        return socket

    directory = tmp_path / "run"
    result = await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        duration=1,
        max_frames=4,
        connector=connector,
    )

    assert result == {
        "status": "bounded_complete",
        "stop_reason": "FRAME_LIMIT",
        "authenticated": True,
        "subscription_confirmed": True,
        "trade_count": 1,
        "quote_count": 1,
        "frame_count": 4,
    }
    assert calls[0][0] == "wss://stream.data.alpaca.markets/v2/iex"
    assert calls[0][1]["additional_headers"] == {
        "APCA-API-KEY-ID": "example-key",
        "APCA-API-SECRET-KEY": "example-secret",
        "Content-Type": "application/json",
    }
    assert socket.sent == [{"action": "subscribe", "trades": ["AAPL"], "quotes": ["AAPL"]}]

    archive = load_market_stream_archive(directory)
    assert archive.symbol == "AAPL"
    assert archive.feed == "iex"
    assert archive.trade is not None
    assert archive.trade.price == Decimal("227.15")
    assert archive.trade.provider_trade_id == "17"
    assert archive.quote is not None
    assert archive.quote.bid == Decimal("227.14")
    assert archive.quote.ask == Decimal("227.18")
    assert archive.raw_payload
    assert b"example-secret" not in archive.raw_payload


@pytest.mark.asyncio
async def test_latest_event_uses_stream_order_when_provider_times_truncate_equal(
    tmp_path,
) -> None:
    later_trade = dict(DATA[0], i=18, p=228.25)
    directory = tmp_path / "run"
    await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=5,
        connector=lambda *args, **kwargs: Socket(
            [CONNECTED, AUTHENTICATED, SUBSCRIBED, DATA, [later_trade]]
        ),
    )

    archive = load_market_stream_archive(directory)
    assert archive.trade is not None
    assert archive.trade.price == Decimal("228.25")
    assert archive.trade.provider_trade_id == "18"


@pytest.mark.asyncio
async def test_stream_accepts_omitted_unsubscribed_channels(tmp_path) -> None:
    directory = tmp_path / "run"
    result = await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=4,
        connector=lambda *args, **kwargs: Socket(
            [CONNECTED, AUTHENTICATED, MINIMAL_SUBSCRIBED, DATA]
        ),
    )

    assert result["status"] == "bounded_complete"
    assert result["subscription_confirmed"] is True
    assert load_market_stream_archive(directory).trade is not None


@pytest.mark.asyncio
async def test_market_stream_never_reconnects_and_partial_archive_is_not_importable(
    tmp_path,
) -> None:
    calls = 0

    def connector(*args, **kwargs):
        nonlocal calls
        calls += 1
        return Socket([CONNECTED, AUTHENTICATED, SUBSCRIBED, RuntimeError("private")])

    directory = tmp_path / "run"
    result = await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        connector=connector,
    )

    assert calls == 1
    assert result["status"] == "partial"
    assert result["stop_reason"] == "DISCONNECTED"
    with pytest.raises(ValueError, match="ALPACA_MARKET_STREAM_INCOMPLETE"):
        load_market_stream_archive(directory)


@pytest.mark.asyncio
async def test_market_stream_rejects_tampered_raw_frame(tmp_path) -> None:
    directory = tmp_path / "run"
    await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=4,
        connector=lambda *args, **kwargs: Socket(
            [CONNECTED, AUTHENTICATED, SUBSCRIBED, DATA]
        ),
    )
    (directory / "frame-00003.json").write_text("[]")

    with pytest.raises(ValueError, match="ALPACA_MARKET_STREAM_ARCHIVE_INVALID"):
        load_market_stream_archive(directory)


@pytest.mark.asyncio
async def test_cancelled_stream_persists_the_real_stop_reason(tmp_path) -> None:
    directory = tmp_path / "run"
    task = asyncio.create_task(
        collect_market_stream(
            symbol="AAPL",
            directory=directory,
            key="example-key",
            secret="example-secret",
            connector=lambda *args, **kwargs: BlockingSocket(
                [CONNECTED, AUTHENTICATED, SUBSCRIBED]
            ),
        )
    )
    while not directory.joinpath("frame-00002.json").exists():
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    receipt = json.loads((directory / "receipt.json").read_text())
    assert receipt["status"] == "partial"
    assert receipt["stop_reason"] == "CANCELLED"
