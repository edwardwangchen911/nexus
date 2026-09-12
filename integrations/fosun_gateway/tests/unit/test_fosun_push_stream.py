import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import ANY

import pytest
from nexus_fosun_gateway import push_stream
from nexus_fosun_gateway.push_stream import (
    MAX_FRAME,
    PUSH_URL,
    PushStreamError,
    collect_push_quote,
    load_latest_push_quote,
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


class BlockingSocket(Socket):
    def __init__(self, frames):
        super().__init__(frames)
        self.blocked = asyncio.Event()

    async def recv(self):
        if self.frames:
            return await super().recv()
        self.blocked.set()
        await asyncio.Event().wait()


class ReceiveClock:
    """Advance only when a silent receive exhausts its real protocol timeout."""

    def __init__(self, socket):
        self.socket = socket
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    async def wait_for(self, receive, timeout):
        if receive.cr_code.co_name != "recv" or self.socket.frames:
            return await receive
        receive.close()
        self.elapsed += timeout
        raise TimeoutError


class EchoSocket(Socket):
    ack_kind = "matching"

    async def send(self, message):
        await super().send(message)
        sent = json.loads(message)
        if sent["act"] == "hb":
            value = sent["hbValue"] + (1 if self.ack_kind == "unknown" else 0)
            self.frames.append({"topic": "hb", "hbValue": value})
            if self.ack_kind == "duplicate":
                self.frames.append({"topic": "hb", "hbValue": value})


class SendBlockingSocket(Socket):
    def __init__(self, act):
        super().__init__([SUB_REPLY])
        self.act = act
        self.offset = 0.0

    def clock(self):
        return self.offset

    async def recv(self):
        result = await super().recv()
        self.offset = 15.0
        return result

    async def send(self, message):
        if json.loads(message)["act"] == self.act:
            await asyncio.Event().wait()
        await super().send(message)


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


def _controlled_collect(*, socket, directory, monkeypatch, **kwargs):
    clock = ReceiveClock(socket)
    monkeypatch.setattr(push_stream.asyncio, "wait_for", clock.wait_for)
    return _collect(
        socket=socket,
        directory=directory,
        monotonic_clock=clock.monotonic,
        now=lambda: datetime(2024, 1, 1, tzinfo=UTC) + timedelta(seconds=clock.elapsed),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_latest_push_quote_preserves_archive_and_scales_last_price(tmp_path):
    directory = tmp_path / "run"
    clock = ReceiveClock(Socket([]))
    frames = [
        SUB_REPLY,
        _quote_frame(price=12_345, power=2),
        _quote_frame(price=12_346, power=2),
    ]
    socket = Socket(frames)
    clock.socket = socket

    await _collect(
        socket=socket,
        directory=directory,
        max_frames=len(frames),
        monotonic_clock=clock.monotonic,
        now=lambda: datetime(2024, 1, 1, tzinfo=UTC)
        + timedelta(seconds=clock.elapsed),
    )

    quote = load_latest_push_quote(directory)

    assert quote.code == "usAAPL"
    assert quote.price == Decimal("123.46")
    assert quote.provider_quote_date == 20240101
    assert quote.provider_quote_time == 1_704_067_200_000
    assert quote.frame_received_at == datetime(2024, 1, 1, tzinfo=UTC)
    assert quote.archive_completed_at == datetime(2024, 1, 1, tzinfo=UTC)
    assert quote.authority_sha256 is None
    document = json.loads(quote.raw_archive_payload)
    assert document["schema"] == "fosun_push_quote_archive_v1"
    assert base64.b64decode(document["receipt_base64"]) == (
        directory / "receipt.json"
    ).read_bytes()
    assert [base64.b64decode(value) for value in document["frames_base64"]] == [
        (directory / f"frame-{index:05}.json").read_bytes() for index in range(len(frames))
    ]


@pytest.mark.asyncio
async def test_authority_digest_is_bound_into_v3_archive(tmp_path, monkeypatch):
    directory = tmp_path / "run"
    digest = "a" * 64
    await _controlled_collect(
        socket=Socket([SUB_REPLY, _quote_frame()]),
        directory=directory,
        monkeypatch=monkeypatch,
        max_frames=2,
        authority_sha256=digest,
    )

    archive = load_push_archive(directory)
    quote = load_latest_push_quote(directory)

    assert archive["receipt"]["schema"] == "nexus_fosun_push_receipt_v3"
    assert archive["receipt"]["authority_sha256"] == digest
    assert quote.authority_sha256 == digest


@pytest.mark.asyncio
async def test_latest_push_quote_rejects_complete_window_without_quote(tmp_path):
    directory = tmp_path / "run"
    await _collect(socket=Socket([SUB_REPLY]), directory=directory)

    with pytest.raises(PushStreamError, match="PUSH_QUOTE_UNAVAILABLE"):
        load_latest_push_quote(directory)


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [0, -1])
async def test_latest_push_quote_rejects_nonpositive_price(tmp_path, price):
    directory = tmp_path / "run"
    await _collect(
        socket=Socket([SUB_REPLY, _quote_frame(price=price)]),
        directory=directory,
        max_frames=2,
    )

    with pytest.raises(PushStreamError, match="PUSH_QUOTE_UNAVAILABLE"):
        load_latest_push_quote(directory)


@pytest.mark.asyncio
@pytest.mark.parametrize("power", [-1, 13])
async def test_latest_push_quote_rejects_unpublished_scale(tmp_path, power):
    directory = tmp_path / "run"
    await _collect(
        socket=Socket([SUB_REPLY, _quote_frame(power=power)]),
        directory=directory,
        max_frames=2,
    )

    with pytest.raises(PushStreamError, match="PUSH_QUOTE_UNAVAILABLE"):
        load_latest_push_quote(directory)


@pytest.mark.asyncio
async def test_latest_push_quote_rejects_partial_archive(tmp_path):
    directory = tmp_path / "run"
    result = await _collect(
        socket=Socket([SUB_REPLY, _quote_frame(), RuntimeError("disconnect")]),
        directory=directory,
    )
    assert result["status"] == "partial"

    with pytest.raises(PushStreamError, match="PUSH_QUOTE_UNAVAILABLE"):
        load_latest_push_quote(directory)


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
                "ping_interval": None,
                "proxy": None,
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
async def test_explicit_probe_proxy_is_pinned_without_library_ping(tmp_path):
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
        proxy_url="http://127.0.0.1:9090",
    )

    assert result["status"] == "bounded_complete"
    assert calls[0][1]["proxy"] == "http://127.0.0.1:9090"
    assert calls[0][1]["ping_interval"] is None


@pytest.mark.parametrize(
    "proxy_url",
    ["", "http://localhost:9090", "http://127.0.0.1:9091", "http://user:secret@127.0.0.1:9090"],
)
def test_collector_rejects_unreviewed_proxy_before_creating_archive(tmp_path, proxy_url):
    with pytest.raises(PushStreamError, match="PUSH_PROXY_INVALID"):
        asyncio.run(
            collect_push_quote(
                code="usAAPL",
                directory=tmp_path / "run",
                api_key="synthetic-api-key",
                session="synthetic-session",
                proxy_url=proxy_url,
            )
        )

    assert not (tmp_path / "run").exists()


@pytest.mark.asyncio
async def test_unsolicited_heartbeat_ack_is_rejected_without_reply(tmp_path):
    quote = _quote_frame()
    socket = Socket([SUB_REPLY, {"topic": "hb", "hbValue": 17}, quote])

    result = await _collect(socket=socket, directory=tmp_path / "run", max_frames=3)

    assert result["status"] == "partial"
    assert result["stop_reason"] == "PROTOCOL_INVALID"
    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"},
    ]
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(tmp_path / "run")


@pytest.mark.asyncio
async def test_provider_subscription_reply_topic_is_accepted(tmp_path):
    reply = {
        "topic": "subrep",
        "time": 1_704_067_200_000,
        "requestid": "request-123",
        "subRep": [{"topic": "qt", "scode": ["usAAPL"]}],
    }

    result = await _collect(socket=Socket([reply]), directory=tmp_path / "run")

    assert result == {
        "status": "bounded_complete",
        "stop_reason": "DURATION_LIMIT",
        "subscription_confirmed": True,
        "frame_count": 1,
    }
    assert load_push_archive(tmp_path / "run")["status"] == "bounded_complete"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("act", "duration", "status"), [("sub", 0.03, "failed"), ("hb", 15.03, "partial")]
)
async def test_blocked_outbound_send_cannot_outlive_deadline(tmp_path, act, duration, status):
    socket = SendBlockingSocket(act)
    task = asyncio.create_task(
        _collect(
            socket=socket,
            directory=tmp_path / "run",
            duration=duration,
            monotonic_clock=socket.clock,
        )
    )
    done, pending = await asyncio.wait({task}, timeout=0.2)
    if pending:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert done, "collector remained blocked after the requested deadline"
    result = task.result()
    assert result["stop_reason"] == "DURATION_LIMIT"
    assert result["status"] == status
    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["heartbeat_sent_count"] == 0
    assert len(socket.sent) == (0 if act == "sub" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("duration", "sent_count"), [(14, 0), (15, 0), (16, 1), (46, 3), (300, 19)]
)
async def test_silent_window_sends_bounded_heartbeats_without_archiving_them(
    tmp_path, monkeypatch, duration, sent_count
):
    socket = Socket([SUB_REPLY])
    clock = ReceiveClock(socket)
    monkeypatch.setattr(push_stream.asyncio, "wait_for", clock.wait_for)
    now = datetime(2024, 1, 1, tzinfo=UTC)

    result = await _collect(
        socket=socket,
        directory=tmp_path / "run",
        duration=duration,
        monotonic_clock=clock.monotonic,
        now=lambda: now,
    )

    assert result["status"] == "bounded_complete"
    assert socket.sent[0] == {
        "act": "sub",
        "items": [{"topic": "qt", "code": ["usAAPL"]}],
        "requestid": "request-123",
    }
    assert socket.sent[1:] == [
        {"act": "hb", "hbValue": 1_704_067_200_000 + index} for index in range(sent_count)
    ]
    assert len(socket.sent) == sent_count + 1
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
    )

    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("interval", [0.0, 1e-12, 0.01, 14.99, 15.01, True])
async def test_heartbeat_interval_cannot_create_excess_outbound_traffic(tmp_path, interval):
    socket = Socket([ConnectionError()])
    directory = tmp_path / "run"

    with pytest.raises(PushStreamError, match="^PUSH_HEARTBEAT_INTERVAL_INVALID$"):
        await _collect(socket=socket, directory=directory, heartbeat_interval_seconds=interval)

    assert socket.sent == []
    assert not directory.exists()


@pytest.mark.asyncio
async def test_window_over_fifteen_seconds_requires_a_proactive_heartbeat(tmp_path, monkeypatch):
    result = await _controlled_collect(
        socket=Socket([SUB_REPLY]),
        directory=tmp_path / "run",
        duration=16,
        monkeypatch=monkeypatch,
    )

    assert result["status"] == "bounded_complete"
    assert result["stop_reason"] == "DURATION_LIMIT"
    assert result["frame_count"] == 1
    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["heartbeat_sent_count"] == 1


@pytest.mark.asyncio
async def test_server_heartbeat_ack_does_not_add_an_outbound_message(tmp_path, monkeypatch):
    socket = EchoSocket([SUB_REPLY])

    await _controlled_collect(
        socket=socket,
        directory=tmp_path / "run",
        duration=16,
        monkeypatch=monkeypatch,
    )

    assert socket.sent == [
        {"act": "sub", "items": [{"topic": "qt", "code": ["usAAPL"]}], "requestid": "request-123"},
        {"act": "hb", "hbValue": 1_704_067_215_000},
    ]
    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["heartbeat_ack_count"] == 1
    assert archive["receipt"]["heartbeat_sent_count"] == 1
    assert archive["receipt"].get("heartbeats") == [
        {"value": 1_704_067_215_000, "after_frame_count": 1, "elapsed_monotonic_seconds": 15.0}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("value", True),
        ("value", 17),
        ("after_frame_count", 2),
        ("after_frame_count", -1),
        ("elapsed_monotonic_seconds", 14.9),
        ("elapsed_monotonic_seconds", 17.0),
    ],
)
async def test_loader_requires_consistent_heartbeat_send_evidence(
    tmp_path, monkeypatch, field, value
):
    directory = tmp_path / "run"
    await _controlled_collect(
        socket=EchoSocket([SUB_REPLY]),
        directory=directory,
        duration=16,
        monkeypatch=monkeypatch,
    )
    receipt = json.loads((directory / "receipt.json").read_text())
    assert "heartbeats" in receipt
    receipt["heartbeats"][0][field] = value
    (directory / "receipt.json").write_text(json.dumps(receipt))

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


@pytest.mark.asyncio
@pytest.mark.parametrize("sent_count", [0, 1])
async def test_legacy_archive_is_readable_only_without_missing_heartbeat_evidence(
    tmp_path, sent_count
):
    directory = tmp_path / "run"
    await _collect(socket=Socket([SUB_REPLY]), directory=directory, max_frames=1)
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["schema"] = "nexus_fosun_push_receipt_v1"
    receipt.pop("heartbeats")
    receipt.pop("outbound_send_timed_out")
    receipt["heartbeat_sent_count"] = sent_count
    (directory / "receipt.json").write_text(json.dumps(receipt))

    if sent_count:
        with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
            load_push_archive(directory)
    else:
        assert load_push_archive(directory)["receipt"] == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("ack_kind", ["unknown", "duplicate"])
async def test_heartbeat_ack_must_match_one_pending_send(tmp_path, monkeypatch, ack_kind):
    socket = EchoSocket([SUB_REPLY])
    socket.ack_kind = ack_kind
    result = await _controlled_collect(
        socket=socket,
        directory=tmp_path / "run",
        duration=16,
        monkeypatch=monkeypatch,
    )

    assert result["status"] == "partial"
    assert result["stop_reason"] == "PROTOCOL_INVALID"
    assert len(socket.sent) == 2
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(tmp_path / "run")


@pytest.mark.asyncio
async def test_loader_cannot_allow_a_heartbeat_before_the_first_interval(tmp_path):
    directory = tmp_path / "run"
    await _collect(socket=Socket([SUB_REPLY]), directory=directory, max_frames=1)
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["heartbeat_sent_count"] = 1
    # Preserve schema shape, also colluding with the send record when it exists.
    if "heartbeats" in receipt:
        receipt["heartbeats"] = [
            {"value": 1, "after_frame_count": 1, "elapsed_monotonic_seconds": 0.0}
        ]
    (directory / "receipt.json").write_text(json.dumps(receipt))

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


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
        {**SUB_REPLY, "errMsg": {"code": 1, "msg": "provider refused"}},
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
@pytest.mark.parametrize("error", [None, "", {}])
async def test_empty_official_error_message_values_are_not_failures(tmp_path, error):
    result = await _collect(
        socket=Socket([{**SUB_REPLY, "errMsg": error}]),
        directory=tmp_path / "run",
    )

    assert result["status"] == "bounded_complete"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["provider refused", {"code": 1, "msg": "provider refused"}])
async def test_nonempty_error_message_in_heartbeat_is_protocol_failure(tmp_path, error):
    result = await _collect(
        socket=Socket([SUB_REPLY, {"topic": "hb", "hbValue": 17, "errMsg": error}]),
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
async def test_cancellation_after_a_frame_persists_a_readable_partial_receipt(tmp_path):
    socket = BlockingSocket([SUB_REPLY])
    directory = tmp_path / "run"
    task = asyncio.create_task(_collect(socket=socket, directory=directory, duration=1))

    await socket.blocked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    archive = load_push_archive(directory)
    assert archive["status"] == "partial"
    assert archive["receipt"]["stop_reason"] == "CANCELLED"
    assert archive["frames"] == ((directory / "frame-00000.json").read_bytes(),)


@pytest.mark.asyncio
async def test_loader_accepts_cancelled_archive_without_a_scheduled_fifteen_second_send(tmp_path):
    moments = iter([0.0, 0.0, 0.0, 14.9, 15.1])
    socket = BlockingSocket([SUB_REPLY])
    directory = tmp_path / "run"
    task = asyncio.create_task(
        _collect(
            socket=socket,
            directory=directory,
            duration=20,
            monotonic_clock=lambda: next(moments),
            now=lambda: datetime(2024, 1, 1, tzinfo=UTC),
        )
    )

    await socket.blocked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    archive = load_push_archive(directory)
    assert archive["status"] == "partial"
    assert archive["receipt"]["stop_reason"] == "CANCELLED"
    assert archive["receipt"]["heartbeat_sent_count"] == 0


@pytest.mark.asyncio
async def test_secret_reflection_is_not_persisted(tmp_path):
    escaped_secret = "".join(f"\\u{ord(character):04x}" for character in "synthetic-session")
    nested = json.dumps(_dynamic_quote())[:-1] + f',"extra":"{escaped_secret}"}}'
    reflected = json.dumps({"topic": "qt", "items": [{"code": "usAAPL", "msg": nested}]})
    socket = Socket([SUB_REPLY, reflected.encode()])

    result = await _collect(socket=socket, directory=tmp_path / "run", duration=1)

    assert result["status"] == "partial"
    assert result["stop_reason"] == "SECRET_REFLECTION"
    assert result["frame_count"] == 1
    assert not (tmp_path / "run" / "frame-00001.json").exists()
    assert all("synthetic-session" not in path.read_text() for path in (tmp_path / "run").iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("nested_format", ['{{"extra":"{}",}}', '{{"extra":"{}"', '["{}"]'])
async def test_malformed_nested_json_cannot_archive_unicode_escaped_session(
    tmp_path, nested_format, monkeypatch
):
    escaped_secret = "".join(f"\\u{ord(character):04x}" for character in "synthetic-session")
    nested = nested_format.format(escaped_secret)
    frame = {"topic": "qt", "items": [{"code": "usAAPL", "msg": nested}]}

    result = await _controlled_collect(
        socket=Socket([SUB_REPLY, frame]),
        directory=tmp_path / "run",
        monkeypatch=monkeypatch,
    )

    assert result["status"] == "partial"
    assert result["stop_reason"] == "QUOTE_INVALID"
    assert result["frame_count"] == 1
    assert not (tmp_path / "run" / "frame-00001.json").exists()
    assert load_push_archive(tmp_path / "run")["receipt"]["quote_message_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", ["[]", "null", [], None, {}, '{"extra":"\ud800"}'])
async def test_nested_message_must_parse_as_object_before_archiving(tmp_path, nested):
    frame = {"topic": "qt", "items": [{"code": "usAAPL", "msg": nested}]}

    result = await _collect(
        socket=Socket([SUB_REPLY, frame]), directory=tmp_path / "run", duration=1
    )

    assert result["stop_reason"] == "QUOTE_INVALID"
    assert result["frame_count"] == 1
    assert not (tmp_path / "run" / "frame-00001.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "quote",
    [
        {"topic": "qt", "items": []},
        {"topic": "qt", "items": [{"code": "usMSFT", "msg": json.dumps(_dynamic_quote())}]},
        _quote_frame(price=True),
        {**_quote_frame(), "errMsg": "provider refused"},
        {**_quote_frame(), "errMsg": {"code": 1, "msg": "provider refused"}},
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
async def test_quote_nested_duplicate_key_is_rejected_before_private_archive(tmp_path):
    duplicate = (
        '{"qtTime":1704067200000,"qtDate":20240101,"power":1,"tradeStatus":1,'
        '"price":101,"price":102,"chgVal":2,"chgPct":3,"pClose":99,"open":100,'
        '"high":102,"low":98,"vol":10,"turnover":1000}'
    )
    socket = Socket([SUB_REPLY, {"topic": "qt", "items": [{"code": "usAAPL", "msg": duplicate}]}])

    result = await _collect(socket=socket, directory=tmp_path / "run")

    assert result["status"] == "partial"
    assert result["stop_reason"] == "QUOTE_INVALID"
    assert result["frame_count"] == 1
    assert not (tmp_path / "run" / "frame-00001.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(("price", "power"), [(101, 1), (0, 1), (-101, 1), (101, 13), (101, -1)])
async def test_valid_quote_is_preserved_without_timezone_or_normalization(tmp_path, price, power):
    quote = _quote_frame(qtTime=1_704_067_200_123, price=price, power=power)
    socket = Socket([SUB_REPLY, quote])

    await _collect(socket=socket, directory=tmp_path / "run", duration=1, max_frames=2)

    archive = load_push_archive(tmp_path / "run")
    assert archive["receipt"]["quote_message_count"] == 1
    assert archive["frames"][1] == json.dumps(quote, separators=(",", ":")).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", sorted(_dynamic_quote()))
@pytest.mark.parametrize("invalid", [True, 1.0, "1", None])
async def test_each_required_quote_field_must_be_a_non_bool_integer(tmp_path, field, invalid):
    result = await _collect(
        socket=Socket([SUB_REPLY, _quote_frame(**{field: invalid})]),
        directory=tmp_path / "run",
        duration=1,
        max_frames=2,
    )

    assert result["status"] == "partial"
    assert result["stop_reason"] == "QUOTE_INVALID"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["subscription", "heartbeat", "quote"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", True),
        ("id", "1"),
        ("time", 1.0),
        ("time", False),
        ("cache", None),
        ("cache", True),
        ("requestid", 1),
    ],
)
async def test_optional_s2c_metadata_rejects_wrong_types(tmp_path, kind, field, value):
    frame = {
        "subscription": SUB_REPLY,
        "heartbeat": {"topic": "hb", "hbValue": 17},
        "quote": _quote_frame(),
    }[kind]
    frames = [] if kind == "subscription" else [SUB_REPLY]
    frames.append({**frame, field: value})

    result = await _collect(
        socket=Socket(frames),
        directory=tmp_path / "run",
        duration=1,
        max_frames=len(frames),
    )

    assert result["status"] in {"partial", "failed"}
    assert result["stop_reason"] == "PROTOCOL_INVALID"
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(tmp_path / "run")


@pytest.mark.asyncio
async def test_optional_s2c_metadata_is_preserved_when_types_are_valid(tmp_path):
    metadata = {"id": 1, "time": 1_704_067_200_123, "cache": 0, "requestid": "request-123"}
    frames = [
        {**SUB_REPLY, **metadata},
        {**_quote_frame(), **metadata},
    ]
    result = await _collect(socket=Socket(frames), directory=tmp_path / "run", max_frames=2)

    assert result["status"] == "bounded_complete"
    assert load_push_archive(tmp_path / "run")["frames"] == tuple(
        json.dumps(frame, separators=(",", ":")).encode() for frame in frames
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ids", "valid"),
    [
        ([5, 5], False),
        ([5, 4], False),
        ([5, 6], True),
        ([5, None, 4], False),
        ([5, None, 5], False),
        ([5, None, 6], True),
        ([None, 4], True),
    ],
)
async def test_consecutive_quote_ids_must_increase_only_when_both_are_known(tmp_path, ids, valid):
    frames = [SUB_REPLY]
    for value in ids:
        frames.append(_quote_frame() if value is None else {**_quote_frame(), "id": value})

    result = await _collect(
        socket=Socket(frames),
        directory=tmp_path / "run",
        duration=1,
        max_frames=len(frames),
    )

    if valid:
        assert result["status"] == "bounded_complete"
        assert load_push_archive(tmp_path / "run")["receipt"]["quote_message_count"] == len(ids)
    else:
        assert result["status"] == "partial"
        assert result["stop_reason"] == "QUOTE_INVALID"
        with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
            load_push_archive(tmp_path / "run")


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
    ("field", "value"), [("quote_message_count", 2), ("heartbeat_sent_count", 999)]
)
async def test_loader_recomputes_receipt_counters_to_reject_tampering(
    tmp_path, monkeypatch, field, value
):
    directory = tmp_path / "run"
    socket = Socket([SUB_REPLY, _quote_frame()])
    await _controlled_collect(
        socket=socket,
        directory=directory,
        max_frames=2,
        monkeypatch=monkeypatch,
    )
    receipt = json.loads((directory / "receipt.json").read_text())

    assert receipt["quote_message_count"] == 1
    assert receipt["heartbeat_sent_count"] == 0
    receipt[field] = value
    (directory / "receipt.json").write_text(json.dumps(receipt))

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


@pytest.mark.asyncio
async def test_loader_recomputes_heartbeat_ack_count(tmp_path, monkeypatch):
    directory = tmp_path / "run"
    await _controlled_collect(
        socket=EchoSocket([SUB_REPLY]),
        directory=directory,
        duration=16,
        monkeypatch=monkeypatch,
    )
    receipt = json.loads((directory / "receipt.json").read_text())

    assert receipt["heartbeat_ack_count"] == 1
    receipt["heartbeat_ack_count"] = 0
    (directory / "receipt.json").write_text(json.dumps(receipt))

    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


@pytest.mark.asyncio
async def test_loader_rejects_excess_receipt_entries_before_any_frame_read(tmp_path, monkeypatch):
    directory = tmp_path / "run"
    await _collect(socket=Socket([SUB_REPLY]), directory=directory, max_frames=1)
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["frames"].append(dict(receipt["frames"][0]))
    (directory / "receipt.json").write_text(json.dumps(receipt))
    original_read_private = push_stream._read_private

    def refuse_frame_read(directory_fd, name, maximum):
        if name != "receipt.json":
            pytest.fail("loader read a frame before validating receipt frame bounds")
        return original_read_private(directory_fd, name, maximum)

    monkeypatch.setattr(push_stream, "_read_private", refuse_frame_read)
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)


@pytest.mark.asyncio
async def test_loader_stops_after_the_frame_that_exceeds_aggregate_bytes(tmp_path, monkeypatch):
    directory = tmp_path / "run"
    frame = json.dumps(_quote_frame(padding="x" * 1_800_000)).encode()
    await _collect(
        socket=Socket([SUB_REPLY, frame, frame, frame, frame, frame]),
        directory=directory,
        duration=1,
        max_frames=10,
    )
    receipt = json.loads((directory / "receipt.json").read_text())
    raw = (directory / "frame-00001.json").read_bytes()
    for index in (5, 6):
        name = f"frame-{index:05}.json"
        path = directory / name
        path.write_bytes(raw)
        path.chmod(0o600)
        receipt["frames"].append(
            {
                "file": name,
                "received_at": receipt["frames"][1]["received_at"],
                "raw_sha256": receipt["frames"][1]["raw_sha256"],
                "raw_byte_length": len(raw),
            }
        )
    receipt["frame_count"] = len(receipt["frames"])
    receipt["status"] = "partial"
    receipt["stop_reason"] = "DISCONNECTED"
    (directory / "receipt.json").write_text(json.dumps(receipt))
    original_read_private = push_stream._read_private
    frame_reads = []

    def count_frame_read(directory_fd, name, maximum):
        if name != "receipt.json":
            frame_reads.append(name)
        return original_read_private(directory_fd, name, maximum)

    monkeypatch.setattr(push_stream, "_read_private", count_frame_read)
    with pytest.raises(PushStreamError, match="PUSH_ARCHIVE_INVALID"):
        load_push_archive(directory)

    assert frame_reads == [f"frame-{index:05}.json" for index in range(6)]


@pytest.mark.asyncio
async def test_loader_rejects_received_at_that_moves_backwards(tmp_path):
    directory = tmp_path / "run"
    await _collect(
        socket=Socket([SUB_REPLY, {"topic": "hb", "hbValue": 17}]),
        directory=directory,
        max_frames=2,
    )
    receipt = json.loads((directory / "receipt.json").read_text())
    receipt["frames"][0]["received_at"] = receipt["ended_at"]
    receipt["frames"][1]["received_at"] = receipt["started_at"]
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
