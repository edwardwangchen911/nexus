import asyncio
import json

import pytest
from nexus.ops.provider_market_stream import (
    build_parser,
    collect_schwab_stream,
    load_provider_market_stream_archive,
)
from nexus.providers.market.schwab_stream import SchwabStreamCredentials


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
                    "delayed": False,
                    "assetMainType": "EQUITY",
                }
            ],
        }
    ]
}


def test_market_stream_cli_supports_only_schwab_without_command_line_credentials() -> None:
    parsed = build_parser().parse_args(
        ["schwab", "--symbol", "AAPL", "--output-dir", "/tmp/archive"]
    )
    assert parsed.source == "schwab"
    assert parsed.symbol == ["AAPL"]
    assert all("token" not in key and "secret" not in key for key in vars(parsed))
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
    assert archive["observations"][0].delayed is False
    assert archive["observations"][0].last.as_tuple().exponent == -2
    assert not any(
        "access-token" in item.read_text() for item in directory.iterdir()
    )


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
async def test_provider_market_stream_archive_rejects_tampered_frame(tmp_path) -> None:
    directory = tmp_path / "tampered"
    await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        max_frames=3,
        connector=lambda *args, **kwargs: Socket(
            [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, SCHWAB_DATA]
        ),
    )
    (directory / "frame-00002.json").write_text("{}")

    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"):
        load_provider_market_stream_archive(directory)
