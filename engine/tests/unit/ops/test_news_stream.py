import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from nexus.ops.news_stream import collect_stream, load_stream_archive


class Socket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []

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

    async def send(self, message):
        self.sent.append(json.loads(message))


AUTH = [{"T": "success", "msg": "authenticated"}]
SUB = [{"T": "subscription", "news": ["EXAMPLE"]}]


@pytest.mark.asyncio
async def test_confirmed_silent_window_is_not_permission_failure(tmp_path):
    socket = Socket([AUTH, SUB])
    calls = []

    def connector(url, **kwargs):
        calls.append(url)
        return socket

    result = await collect_stream(
        symbol="EXAMPLE",
        directory=tmp_path / "run",
        key="example-key",
        secret="example-secret",
        duration=0.03,
        connector=connector,
    )
    assert result["status"] == "bounded_complete"
    assert result["article_count"] == 0 and result["subscription_confirmed"]
    assert len(calls) == 1
    assert socket.sent == [{"action": "subscribe", "news": ["EXAMPLE"]}]
    assert load_stream_archive(tmp_path / "run")["articles"] == ()
    assert "example-secret" not in (tmp_path / "run" / "receipt.json").read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tail,reason,status",
    [
        (RuntimeError("private exception"), "DISCONNECTED", "partial"),
        ([{"T": "error", "code": 409, "msg": "upstream"}], "PROVIDER_409", "partial"),
        ([{"T": "success", "msg": "example-secret"}], "SECRET_REFLECTION", "partial"),
    ],
)
async def test_stop_without_reconnect(tmp_path, tail, reason, status):
    socket = Socket([AUTH, SUB, tail])
    result = await collect_stream(
        symbol="EXAMPLE",
        directory=tmp_path / "run",
        key="example-key",
        secret="example-secret",
        connector=lambda *a, **kw: socket,
    )
    assert result["stop_reason"] == reason and result["status"] == status
    assert not any("example-secret" in path.read_text() for path in (tmp_path / "run").iterdir())


@pytest.mark.asyncio
async def test_news_frame_roundtrip_and_tampering(tmp_path):
    news = [
        {
            "T": "n",
            "id": 1,
            "headline": "Example",
            "summary": "News",
            "author": "Writer",
            "created_at": "2026-09-01T00:00:00Z",
            "updated_at": "2026-09-01T01:00:00Z",
            "symbols": ["EXAMPLE"],
            "source": "example",
            "url": "javascript:alert(1)",
        }
    ]
    socket = Socket([AUTH, SUB, news])
    directory = tmp_path / "run"
    result = await collect_stream(
        symbol="EXAMPLE",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=3,
        connector=lambda *a, **kw: socket,
    )
    assert result["article_count"] == 1
    assert load_stream_archive(directory)["articles"][0].url is None
    (directory / "frame-00002.json").write_text("[]")
    with pytest.raises(ValueError):
        load_stream_archive(directory)


@pytest.mark.asyncio
async def test_stream_archive_loader_retains_validated_originals_for_import(tmp_path):
    socket = Socket([AUTH, SUB])
    directory = tmp_path / "run"
    await collect_stream(
        symbol="EXAMPLE",
        directory=directory,
        key="example-key",
        secret="example-secret",
        duration=0.03,
        connector=lambda *a, **kw: socket,
    )
    archive = load_stream_archive(directory)
    assert archive["receipt"]["schema"] == "nexus_news_stream_receipt_v1"
    assert archive["receipt_raw"] == (directory / "receipt.json").read_bytes()
    assert archive["frames"] == (
        (directory / "frame-00000.json").read_bytes(),
        (directory / "frame-00001.json").read_bytes(),
    )


@pytest.mark.asyncio
async def test_stream_archive_rejects_forged_terminal_state_and_overlong_window(tmp_path):
    socket = Socket([AUTH, SUB])
    directory = tmp_path / "run"
    await collect_stream(
        symbol="EXAMPLE",
        directory=directory,
        key="example-key",
        secret="example-secret",
        duration=0.03,
        connector=lambda *args, **kwargs: socket,
    )
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["status"] = "partial"
    (directory / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="NEWS_STREAM_ARCHIVE_INVALID"):
        load_stream_archive(directory)
    receipt["status"] = "bounded_complete"
    ended = datetime.fromisoformat(receipt["ended_at"])
    receipt["started_at"] = (ended - timedelta(seconds=302)).astimezone(UTC).isoformat()
    (directory / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="NEWS_STREAM_ARCHIVE_INVALID"):
        load_stream_archive(directory)


@pytest.mark.asyncio
async def test_stream_archive_rejects_provider_error_frame(tmp_path):
    socket = Socket([AUTH, SUB, [{"T": "error", "code": 409, "msg": "upstream"}]])
    directory = tmp_path / "run"
    await collect_stream(
        symbol="EXAMPLE",
        directory=directory,
        key="example-key",
        secret="example-secret",
        connector=lambda *args, **kwargs: socket,
    )
    with pytest.raises(ValueError, match="NEWS_STREAM_ARCHIVE_INVALID"):
        load_stream_archive(directory)


@pytest.mark.asyncio
async def test_stream_archive_requires_boolean_authentication_state(tmp_path):
    socket = Socket([AUTH, SUB])
    directory = tmp_path / "run"
    await collect_stream(
        symbol="EXAMPLE",
        directory=directory,
        key="example-key",
        secret="example-secret",
        duration=0.03,
        connector=lambda *args, **kwargs: socket,
    )
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["authenticated"] = 1
    (directory / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="NEWS_STREAM_ARCHIVE_INVALID"):
        load_stream_archive(directory)
