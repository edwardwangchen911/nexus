import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY

import pytest
from nexus_fosun_gateway.push_stream import (
    MAX_FRAME,
    PUSH_URL,
    PushStreamError,
    collect_push_quote,
    load_push_archive,
)


class Socket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def recv(self):
        if not self.frames:
            await asyncio.sleep(10)
        frame = self.frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return frame if isinstance(frame, bytes) else json.dumps(frame, separators=(",", ":"))

    async def send(self, message):
        self.sent.append(json.loads(message))


SUB_REPLY = {
    "topic": "qt",
    "requestid": "request-123",
    "subRep": [{"topic": "qt", "scode": ["usAAPL"], "fcode": [], "invalid": [], "denied": []}],
}


def _collect(*, socket, directory, **kwargs):
    return collect_push_quote(
        code="usAAPL",
        directory=directory,
        api_key="synthetic-api-key",
        session="synthetic-session",
        duration=kwargs.pop("duration", 0.03),
        max_frames=kwargs.pop("max_frames", 100),
        connector=lambda *args, **connector_kwargs: socket,
        request_id_factory=lambda: "request-123",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_confirmed_silent_quote_window_is_bounded_and_archived_privately(tmp_path):
    socket = Socket([SUB_REPLY])
    calls = []

    def connector(url, **kwargs):
        calls.append((url, kwargs))
        return socket

    result = await collect_push_quote(
        code="usAAPL",
        directory=tmp_path / "run",
        api_key="synthetic-api-key",
        session="synthetic-session",
        duration=0.03,
        connector=connector,
        request_id_factory=lambda: "request-123",
    )

    assert result == {
        "status": "bounded_complete",
        "stop_reason": "DURATION_LIMIT",
        "subscription_confirmed": True,
        "frame_count": 1,
    }
    assert calls == [
        (
            PUSH_URL,
            {
                "additional_headers": {
                    "X-Api-Key": "synthetic-api-key",
                    "X-Session": "synthetic-session",
                },
                "open_timeout": 0.03,
                "close_timeout": 1,
                "max_size": 2 * 1024 * 1024,
                "max_queue": 1,
                "compression": None,
                "logger": ANY,
            },
        )
    ]
    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"}
    ]
    archive = load_push_archive(tmp_path / "run")
    assert archive["status"] == "bounded_complete"
    assert archive["frames"] == ((tmp_path / "run" / "frame-00000.json").read_bytes(),)
    assert "synthetic-api-key" not in (tmp_path / "run" / "receipt.json").read_text()
    assert "synthetic-session" not in (tmp_path / "run" / "receipt.json").read_text()
    assert (tmp_path / "run").stat().st_mode & 0o077 == 0
    assert all(path.stat().st_mode & 0o077 == 0 for path in (tmp_path / "run").iterdir())


@pytest.mark.asyncio
async def test_heartbeat_is_echoed_and_quote_frames_are_retained(tmp_path):
    quote = {"topic": "qt", "items": [{"code": "usAAPL", "msg": "{}"}]}
    socket = Socket([SUB_REPLY, {"topic": "hb", "hbValue": 17}, quote])

    result = await _collect(socket=socket, directory=tmp_path / "run", max_frames=3)

    assert result["status"] == "bounded_complete"
    assert result["stop_reason"] == "FRAME_LIMIT"
    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"},
        {"act": "hb", "hbValue": 17},
    ]
    assert len(load_push_archive(tmp_path / "run")["frames"]) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "reason", "stored_frames"),
    [
        ({**SUB_REPLY, "requestid": "wrong"}, "SUBSCRIPTION_INVALID", 1),
        (
            {**SUB_REPLY, "subRep": [{**SUB_REPLY["subRep"][0], "denied": ["usAAPL"]}]},
            "SUBSCRIPTION_DENIED",
            1,
        ),
        ({"topic": "qt", "items": []}, "PROTOCOL_INVALID", 1),
        ([{"topic": "qt"}, {"topic": "qt"}], "FRAME_INVALID", 0),
    ],
)
async def test_invalid_subscription_protocol_never_looks_complete(
    tmp_path, frame, reason, stored_frames
):
    socket = Socket([frame])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "failed"
    assert result["stop_reason"] == reason
    assert result["subscription_confirmed"] is False
    assert result["frame_count"] == stored_frames
    assert (tmp_path / "run" / "receipt.json").exists()


@pytest.mark.asyncio
async def test_disconnect_after_subscription_is_partial_without_reconnect(tmp_path):
    socket = Socket([SUB_REPLY, RuntimeError("private upstream text")])
    calls = []

    def connector(*args, **kwargs):
        calls.append((args, kwargs))
        return socket

    result = await collect_push_quote(
        code="usAAPL",
        directory=tmp_path / "run",
        api_key="synthetic-api-key",
        session="synthetic-session",
        connector=connector,
        request_id_factory=lambda: "request-123",
    )

    assert result["status"] == "partial"
    assert result["stop_reason"] == "DISCONNECTED"
    assert len(calls) == 1
    assert load_push_archive(tmp_path / "run")["status"] == "partial"


@pytest.mark.asyncio
async def test_secret_reflection_is_not_persisted(tmp_path):
    socket = Socket([SUB_REPLY, {"topic": "qt", "items": [{"msg": "synthetic-session"}]}])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "SECRET_REFLECTION"
    assert result["frame_count"] == 1
    assert all(
        "synthetic-session" not in path.read_text()
        for path in (tmp_path / "run").iterdir()
    )


@pytest.mark.asyncio
async def test_limits_stop_or_reject_before_an_unbounded_archive(tmp_path):
    socket = Socket([SUB_REPLY, {"topic": "qt", "items": [{"code": "usAAPL"}]}])

    result = await _collect(socket=socket, directory=tmp_path / "frames", max_frames=2)

    assert result["status"] == "bounded_complete"
    assert result["stop_reason"] == "FRAME_LIMIT"
    with pytest.raises(PushStreamError, match="PUSH_LIMIT_INVALID"):
        await _collect(socket=Socket([]), directory=tmp_path / "bad-duration", duration=301)
    with pytest.raises(PushStreamError, match="PUSH_LIMIT_INVALID"):
        await _collect(socket=Socket([]), directory=tmp_path / "bad-frames", max_frames=101)


@pytest.mark.asyncio
async def test_single_frame_limit_is_enforced_before_private_write(tmp_path):
    oversized = json.dumps({"topic": "qt", "padding": "x" * MAX_FRAME}).encode()
    socket = Socket([SUB_REPLY, oversized])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "FRAME_TOO_LARGE"
    assert result["frame_count"] == 1
    assert not (tmp_path / "run" / "frame-00001.json").exists()


@pytest.mark.asyncio
async def test_total_byte_limit_stops_before_the_overflow_frame_is_written(tmp_path):
    frame = json.dumps({"topic": "qt", "padding": "x" * 1_800_000}).encode()
    socket = Socket([SUB_REPLY, frame, frame, frame, frame, frame])

    result = await _collect(socket=socket, directory=tmp_path / "run", duration=1, max_frames=10)

    assert result["status"] == "partial"
    assert result["stop_reason"] == "BYTE_LIMIT"
    assert result["frame_count"] == 5
    assert not (tmp_path / "run" / "frame-00005.json").exists()


@pytest.mark.asyncio
async def test_loader_rejects_tampered_frame_and_terminal_state(tmp_path):
    socket = Socket([SUB_REPLY])
    directory = tmp_path / "run"
    await _collect(socket=socket, directory=directory)

    (directory / "frame-00000.json").write_text("{}")
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)

    socket = Socket([SUB_REPLY])
    directory = tmp_path / "terminal"
    await _collect(socket=socket, directory=directory)
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["status"] = "partial"
    (directory / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


def test_loader_rejects_inconsistent_time_window_without_socket(tmp_path):
    started = datetime.now(UTC) - timedelta(seconds=2)
    receipt = {
        "schema": "nexus_fosun_push_receipt_v1",
        "source": "fosun",
        "transport": "websocket",
        "origin": PUSH_URL,
        "topic": "qt",
        "code": "usAAPL",
        "started_at": started.isoformat(),
        "ended_at": (started + timedelta(seconds=2)).isoformat(),
        "requested_duration_seconds": 1.0,
        "requested_max_frames": 1,
        "elapsed_monotonic_seconds": 2.0,
        "subscription_confirmed": True,
        "status": "bounded_complete",
        "stop_reason": "DURATION_LIMIT",
        "frame_count": 0,
        "frames": [],
    }
    directory = tmp_path / "bad"
    directory.mkdir(mode=0o700)
    (directory / "receipt.json").write_text(json.dumps(receipt))
    directory.chmod(0o700)
    (directory / "receipt.json").chmod(0o600)

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)
