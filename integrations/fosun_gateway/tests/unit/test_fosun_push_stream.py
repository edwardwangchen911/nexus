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


def _sub_reply(code="usAAPL"):
    return {
        "topic": "qt",
        "requestid": "request-123",
        "subRep": [{"topic": "qt", "scode": [code], "fcode": [], "invalid": [], "denied": []}],
    }


SUB_REPLY = _sub_reply()


def _dynamic_quote(**changes):
    quote = {
        "qtTime": 1_704_067_200_000,
        "qtDate": 20240101,
        "power": 1,
        "tradeStatus": 1,
        "price": 101,
        "chgVal": 2,
        "chgPct": 3,
        "pClose": 99,
        "open": 100,
        "high": 102,
        "low": 98,
        "vol": 10,
        "turnover": 1000,
    }
    quote.update(changes)
    return quote


def _quote_frame(code="usAAPL", **changes):
    return {"topic": "qt", "items": [{"code": code, "msg": json.dumps(_dynamic_quote(**changes))}]}


def _collect(*, socket, directory, **kwargs):
    code = kwargs.pop("code", "usAAPL")
    return collect_push_quote(
        code=code,
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
    quote = _quote_frame()
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
async def test_silent_window_sends_proactive_heartbeat_without_archiving_it(tmp_path):
    socket = Socket([SUB_REPLY])
    now = datetime(2024, 1, 1, tzinfo=UTC)

    result = await _collect(
        socket=socket,
        directory=tmp_path / "run",
        duration=0.035,
        heartbeat_interval_seconds=0.01,
        now=lambda: now,
    )

    assert result["status"] == "bounded_complete"
    assert socket.sent[0] == {
        "act": "sub",
        "items": [{"topic": "qt", "code": ["usAAPL"]}],
        "requestid": "request-123",
    }
    assert all(
        sent == {"act": "hb", "hbValue": 1_704_067_200_000}
        for sent in socket.sent[1:]
    )
    assert len(socket.sent) >= 2
    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["heartbeat_sent_count"] == len(socket.sent) - 1
    assert archive["receipt"]["quote_message_count"] == 0
    assert len(archive["frames"]) == 1


@pytest.mark.asyncio
async def test_window_shorter_than_heartbeat_interval_needs_no_proactive_heartbeat(tmp_path):
    socket = Socket([SUB_REPLY])

    await _collect(
        socket=socket,
        directory=tmp_path / "run",
        duration=0.01,
        heartbeat_interval_seconds=0.1,
    )

    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"}
    ]


@pytest.mark.asyncio
async def test_window_over_fifteen_seconds_requires_a_proactive_heartbeat(tmp_path):
    moments = iter([0.0, 0.0, 15.1, 16.0, 16.0])

    result = await _collect(
        socket=Socket([SUB_REPLY]),
        directory=tmp_path / "run",
        duration=16,
        monotonic_clock=lambda: next(moments),
        now=lambda: datetime(2024, 1, 1, tzinfo=UTC),
    )

    assert result["status"] == "bounded_complete"
    assert result["stop_reason"] == "DURATION_LIMIT"
    assert result["frame_count"] == 1
    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["heartbeat_sent_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "reason", "stored_frames"),
    [
        ({**SUB_REPLY, "requestid": "wrong"}, "SUBSCRIPTION_INVALID", 1),
        (
            {
                **SUB_REPLY,
                "subRep": [{**SUB_REPLY["subRep"][0], "scode": [], "denied": ["usAAPL"]}],
            },
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
@pytest.mark.parametrize(
    "reply",
    [
        {**SUB_REPLY, "errMsg": "provider refused"},
        {
            **SUB_REPLY,
            "subRep": [{**SUB_REPLY["subRep"][0], "fcode": ["usAAPL"]}],
        },
        {
            **SUB_REPLY,
            "subRep": [{**SUB_REPLY["subRep"][0], "denied": ["usAAPL"]}],
        },
    ],
)
async def test_subrep_requires_exactly_one_code_state_without_error_message(tmp_path, reply):
    result = await _collect(socket=Socket([reply]), directory=tmp_path / "run")

    assert result["status"] == "failed"
    assert result["stop_reason"] == "SUBSCRIPTION_INVALID"


@pytest.mark.asyncio
async def test_nonempty_error_message_in_heartbeat_is_protocol_failure(tmp_path):
    result = await _collect(
        socket=Socket([SUB_REPLY, {"topic": "hb", "hbValue": 17, "errMsg": "provider refused"}]),
        directory=tmp_path / "run",
    )

    assert result["status"] == "partial"
    assert result["stop_reason"] == "PROTOCOL_INVALID"
    assert result["frame_count"] == 2
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(tmp_path / "run")


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
    escaped_secret = "".join(f"\\u{ord(character):04x}" for character in "synthetic-session")
    nested = json.dumps(_dynamic_quote())[:-1] + f',"extra":"{escaped_secret}"}}'
    reflected = json.dumps({"topic": "qt", "items": [{"code": "usAAPL", "msg": nested}]})
    socket = Socket([SUB_REPLY, reflected.encode()])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "SECRET_REFLECTION"
    assert result["frame_count"] == 1
    assert all(
        "synthetic-session" not in path.read_text()
        for path in (tmp_path / "run").iterdir()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "quote",
    [
        {"topic": "qt", "items": []},
        {"topic": "qt", "items": [{"code": "usMSFT", "msg": json.dumps(_dynamic_quote())}]},
        {"topic": "qt", "items": [{"code": "usAAPL", "msg": "[]"}]},
        _quote_frame(price=True),
        _quote_frame(price=0),
        _quote_frame(power=13),
        {**_quote_frame(), "errMsg": "provider refused"},
    ],
)
async def test_invalid_quote_frame_is_archived_then_partial(tmp_path, quote):
    socket = Socket([SUB_REPLY, quote])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "QUOTE_INVALID"
    assert result["frame_count"] == 2
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(tmp_path / "run")


@pytest.mark.asyncio
async def test_quote_nested_duplicate_key_is_rejected_after_private_archive(tmp_path):
    duplicate = (
        '{"qtTime":1704067200000,"qtDate":20240101,"power":1,"tradeStatus":1,'
        '"price":101,"price":102,"chgVal":2,"chgPct":3,"pClose":99,"open":100,'
        '"high":102,"low":98,"vol":10,"turnover":1000}'
    )
    socket = Socket([SUB_REPLY, {"topic": "qt", "items": [{"code": "usAAPL", "msg": duplicate}]}])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "QUOTE_INVALID"
    assert result["frame_count"] == 2


@pytest.mark.asyncio
async def test_valid_quote_is_preserved_without_timezone_or_normalization(tmp_path):
    quote = _quote_frame(qtTime=1_704_067_200_123, price=101)
    socket = Socket([SUB_REPLY, quote])

    await _collect(socket=socket, directory=tmp_path / "run", max_frames=2)

    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["quote_message_count"] == 1
    assert archive["frames"][1] == json.dumps(quote, separators=(",", ":")).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["usAAPL", "hk00656"])
async def test_only_us_and_five_digit_hk_codes_are_supported(tmp_path, code):
    socket = Socket([_sub_reply(code)])

    result = await _collect(socket=socket, directory=tmp_path / code, code=code)

    assert result["status"] == "bounded_complete"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["hk656", "hkABCDE", "HK00656", "sh600519", "nasAAPL"])
async def test_other_market_code_forms_are_rejected(tmp_path, code):
    with pytest.raises(PushStreamError, match="PUSH_CODE_INVALID"):
        await _collect(socket=Socket([]), directory=tmp_path / code, code=code)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"), [("quote_message_count", 2), ("heartbeat_sent_count", 1)]
)
async def test_loader_recomputes_receipt_counters_to_reject_tampering(tmp_path, field, value):
    directory = tmp_path / "run"
    await _collect(socket=Socket([SUB_REPLY, _quote_frame()]), directory=directory, max_frames=2)
    receipt = json.loads((directory / "receipt.json").read_text())

    assert receipt["quote_message_count"] == 1
    assert receipt["heartbeat_sent_count"] == 0
    receipt[field] = value
    (directory / "receipt.json").write_text(json.dumps(receipt))

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


@pytest.mark.asyncio
async def test_limits_stop_or_reject_before_an_unbounded_archive(tmp_path):
    socket = Socket([SUB_REPLY, _quote_frame()])

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
    frame = json.dumps(_quote_frame(padding="x" * 1_800_000)).encode()
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
