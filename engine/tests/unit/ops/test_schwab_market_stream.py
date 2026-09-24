import asyncio
import base64
import hashlib
import json
import socket
import ssl
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.ops import schwab_market_stream as stream
from nexus.ops.schwab_market_stream import (
    ProviderMarketStreamError,
    SchwabMarketStreamImportResult,
    build_parser,
    capture_and_import_schwab_level_one_once,
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
from websockets.exceptions import InvalidHandshake, ProxyError


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


async def _packed_archive(
    tmp_path,
    symbols: tuple[str, ...],
    frames: list[object],
    *,
    completed_at: datetime | None = None,
) -> bytes:
    directory = tmp_path / f"archive-{len(list(tmp_path.iterdir()))}"
    await collect_schwab_stream(
        symbols=symbols,
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        max_frames=len(frames),
        connector=lambda *args, **kwargs: Socket(frames),
    )
    provider_times = [
        datetime.fromtimestamp(item["data"][0]["timestamp"] / 1000, UTC)
        for item in frames
        if isinstance(item, dict) and "data" in item
    ]
    frozen_end = completed_at or max(provider_times) + timedelta(seconds=1)
    receipt_path = directory / "receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["started_at"] = (frozen_end - timedelta(seconds=2)).isoformat()
    receipt["ended_at"] = frozen_end.isoformat()
    for entry in receipt["frames"]:
        entry["received_at"] = frozen_end.isoformat()
    receipt_path.write_text(json.dumps(receipt, sort_keys=True))
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
    paper_probe = build_parser().parse_args(
        [
            "paper-probe",
            "--repo-root",
            "/tmp/repo",
            "--binding",
            f"AAPL={AAPL_ID}",
            "--output-dir",
            "/tmp/paper-probe",
            "--duration",
            "30",
        ]
    )
    assert paper_probe.source == "paper-probe"
    assert paper_probe.binding == [f"AAPL={AAPL_ID}"]
    assert all("token" not in key and "secret" not in key for key in vars(paper_probe))
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
async def test_paper_quote_probe_requires_fresh_two_sided_quote_for_every_binding(tmp_path) -> None:
    assert hasattr(stream, "validate_paper_quote_probe")
    complete = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data()],
    )
    assert stream.validate_paper_quote_probe(complete, {"AAPL": UUID(str(AAPL_ID))}) == {
        "paper_quote_status": "passed_at_observation",
        "candidate_count": 1,
        "execution_locked": True,
    }

    trade_only = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data(**{"1": None, "2": None})],
    )
    with pytest.raises(ProviderMarketStreamError, match="SCHWAB_L1_INCOMPLETE"):
        stream.validate_paper_quote_probe(trade_only, {"AAPL": UUID(str(AAPL_ID))})


@pytest.mark.asyncio
async def test_paper_quote_probe_rejects_stale_quote_at_capture_end(tmp_path) -> None:
    assert hasattr(stream, "validate_paper_quote_probe")
    stale_millis = int((EVENT_AT - timedelta(seconds=61)).timestamp() * 1000)
    stale = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [
            SCHWAB_LOGIN,
            SCHWAB_SUBSCRIBED,
            _data(
                **{
                    "34": stale_millis,
                    "35": stale_millis,
                    "37": stale_millis,
                    "38": stale_millis,
                }
            ),
        ],
        completed_at=EVENT_AT,
    )
    with pytest.raises(ProviderMarketStreamError, match="SCHWAB_MARKET_STREAM_EVIDENCE_STALE"):
        stream.validate_paper_quote_probe(stale, {"AAPL": UUID(str(AAPL_ID))})


@pytest.mark.asyncio
async def test_paper_probe_cli_checks_same_archive_without_import(tmp_path, monkeypatch) -> None:
    await _packed_archive(
        tmp_path,
        ("AAPL",),
        [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, _data()],
    )
    source = tmp_path / "archive-0"
    output = tmp_path / "paper-probe"

    async def capture(arguments, _client, **_kwargs):
        source.rename(arguments.output_dir)
        return {
            "status": "bounded_complete",
            "stop_reason": "FRAME_LIMIT",
            "authenticated": True,
            "subscription_confirmed": True,
            "observation_count": 1,
            "frame_count": 3,
        }

    monkeypatch.setattr(stream, "run_schwab_stream", capture)
    arguments = build_parser().parse_args(
        [
            "paper-probe",
            "--repo-root",
            str(tmp_path),
            "--binding",
            f"AAPL={AAPL_ID}",
            "--output-dir",
            str(output),
        ]
    )
    async with httpx.AsyncClient() as client:
        result = await stream._run(arguments, client=client)

    assert result == {
        "status": "bounded_complete",
        "stop_reason": "FRAME_LIMIT",
        "authenticated": True,
        "subscription_confirmed": True,
        "observation_count": 1,
        "frame_count": 3,
        "paper_quote_status": "passed_at_observation",
        "candidate_count": 1,
        "execution_locked": True,
    }
    assert output.is_dir()


@pytest.mark.asyncio
async def test_paper_probe_requires_existing_output_parent_before_provider_call(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unexpected_capture(*args, **kwargs):
        raise AssertionError("missing output parent must stop before provider access")

    monkeypatch.setattr(stream, "run_schwab_stream", unexpected_capture)
    output = tmp_path / "missing-parent" / "fresh-probe"
    arguments = build_parser().parse_args(
        [
            "paper-probe",
            "--repo-root",
            str(tmp_path),
            "--binding",
            f"AAPL={AAPL_ID}",
            "--output-dir",
            str(output),
        ]
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(
            ProviderMarketStreamError, match="PROVIDER_MARKET_STREAM_OUTPUT_PARENT_REQUIRED"
        ):
            await stream._run(arguments, client=client)
    assert not output.parent.exists()


@pytest.mark.asyncio
async def test_paper_probe_rejects_symlinked_output_parent_before_provider_call(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unexpected_capture(*args, **kwargs):
        raise AssertionError("symlinked output parent must stop before provider access")

    monkeypatch.setattr(stream, "run_schwab_stream", unexpected_capture)
    real_parent = tmp_path / "private-parent"
    real_parent.mkdir(mode=0o700)
    link = tmp_path / "linked-parent"
    link.symlink_to(real_parent, target_is_directory=True)
    arguments = build_parser().parse_args(
        [
            "paper-probe",
            "--repo-root",
            str(tmp_path),
            "--binding",
            f"AAPL={AAPL_ID}",
            "--output-dir",
            str(link / "fresh-probe"),
        ]
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(
            ProviderMarketStreamError, match="PROVIDER_MARKET_STREAM_OUTPUT_PARENT_REQUIRED"
        ):
            await stream._run(arguments, client=client)
    assert not (real_parent / "fresh-probe").exists()


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
@pytest.mark.parametrize(
    ("connection_error", "expected_reason"),
    [
        (TimeoutError("private connection detail"), "CONNECT_TIMEOUT"),
        (ProxyError("private proxy detail"), "CONNECT_PROXY_FAILED"),
        (ssl.SSLError("private tls detail"), "CONNECT_TLS_FAILED"),
        (socket.gaierror("private dns detail"), "CONNECT_DNS_FAILED"),
        (ConnectionResetError("private reset detail"), "CONNECT_RESET"),
        (ConnectionRefusedError("private refused detail"), "CONNECT_REFUSED"),
        (OSError("private network detail"), "CONNECT_NETWORK_FAILED"),
        (InvalidHandshake("private handshake detail"), "CONNECT_HANDSHAKE_FAILED"),
        (EOFError("private upstream detail"), "CONNECT_EOF"),
        (RuntimeError("private client detail"), "CONNECT_RUNTIME_FAILED"),
        (TypeError("private client detail"), "CONNECT_TYPE_INVALID"),
        (ValueError("private client detail"), "CONNECT_VALUE_INVALID"),
    ],
)
async def test_schwab_stream_classifies_connect_failure_without_leaking_detail(
    tmp_path, connection_error: Exception, expected_reason: str
) -> None:
    class FailedConnection:
        async def __aenter__(self):
            raise connection_error

        async def __aexit__(self, *args):
            return None

    directory = tmp_path / "failed-connect"
    result = await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        connector=lambda *args, **kwargs: FailedConnection(),
    )

    assert result["status"] == "failed"
    assert result["stop_reason"] == expected_reason
    assert result["authenticated"] is False
    assert result["frame_count"] == 0
    receipt = (directory / "receipt.json").read_text()
    assert "private" not in receipt
    assert json.loads(receipt)["stop_reason"] == expected_reason
    with pytest.raises(ProviderMarketStreamError, match="PROVIDER_MARKET_STREAM_INCOMPLETE"):
        load_provider_market_stream_archive(directory)


@pytest.mark.asyncio
async def test_agent_decision_capture_is_one_aapl_attempt_with_fixed_limits(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[object, object, object]] = []
    imported: list[tuple[dict[str, UUID], Path, datetime]] = []

    async def capture(arguments, client, *, now=None):
        calls.append((arguments, client, now))
        arguments.output_dir.mkdir(mode=0o700)
        return {"status": "bounded_complete"}

    expected = SchwabMarketStreamImportResult(
        raw_sha256="a" * 64,
        created_observations=2,
        symbols_observed=1,
        artifact_id=UUID(int=10),
        quote_ids={"AAPL": UUID(int=11)},
        trade_ids={"AAPL": UUID(int=12)},
    )

    async def fake_import(session, store, *, bindings, directory, imported_at):
        del session, store
        imported.append((bindings, directory, imported_at))
        return expected

    monkeypatch.setattr("nexus.ops.schwab_market_stream.import_schwab_market_stream", fake_import)
    captured_at = datetime(2026, 8, 28, 14, 30, tzinfo=UTC)

    class Session:
        async def scalar(self, statement):
            del statement
            return captured_at + timedelta(seconds=1)

    result = await capture_and_import_schwab_level_one_once(
        Session(),
        object(),
        repo_root=tmp_path,
        client=object(),
        instrument_id=AAPL_ID,
        checked_at=captured_at,
        capture=capture,
    )

    assert result == expected
    assert len(calls) == 1
    arguments, _, now = calls[0]
    assert arguments.symbol == ["AAPL"]
    assert arguments.duration == 30.0
    assert arguments.max_frames == 100
    assert now == captured_at
    assert imported[0][0] == {"AAPL": AAPL_ID}
    assert imported[0][2] == captured_at + timedelta(seconds=1)
    assert not imported[0][1].exists()


@pytest.mark.asyncio
async def test_agent_decision_capture_preserves_safe_incomplete_summary(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    imported = False

    async def capture(arguments, client, *, now=None):
        del client, now
        arguments.output_dir.mkdir(mode=0o700)
        return {
            "status": "partial",
            "stop_reason": "DISCONNECTED",
            "authenticated": True,
            "subscription_confirmed": True,
            "observation_count": 0,
            "frame_count": 2,
        }

    async def fake_import(*args, **kwargs):
        nonlocal imported
        del args, kwargs
        imported = True
        raise AssertionError("an incomplete capture must not reach import")

    monkeypatch.setattr("nexus.ops.schwab_market_stream.import_schwab_market_stream", fake_import)

    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_INCOMPLETE") as caught:
        await capture_and_import_schwab_level_one_once(
            object(),
            object(),
            repo_root=tmp_path,
            client=object(),
            instrument_id=AAPL_ID,
            checked_at=EVENT_AT,
            capture=capture,
        )

    assert caught.value.capture_summary == {
        "status": "partial",
        "stop_reason": "DISCONNECTED",
        "authenticated": True,
        "subscription_confirmed": True,
        "observation_count": 0,
        "frame_count": 2,
    }
    assert imported is False


@pytest.mark.asyncio
async def test_explicit_capture_config_reaches_refresh_without_process_or_default_override(
    tmp_path,
    monkeypatch,
):
    from nexus.ops import schwab_market_stream as stream
    from nexus.ops.agent_paper_loop import load_agent_paper_service_environment
    from nexus.providers.market import schwab_oauth as oauth

    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/.env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=default-key\nNEXUS_SCHWAB_APP_SECRET=default-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    env_file = tmp_path / "infra/.env.service"
    env_file.write_text(
        "NEXUS_SCHWAB_APP_KEY=registered-key\nNEXUS_SCHWAB_APP_SECRET=registered-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    monkeypatch.setenv("NEXUS_SCHWAB_APP_KEY", "process-key")
    monkeypatch.setenv("NEXUS_SCHWAB_APP_SECRET", "process-secret")
    environment = load_agent_paper_service_environment(tmp_path, {}, env_file=env_file)
    oauth.save_schwab_token(
        tmp_path,
        oauth.SchwabOAuthToken(
            access_token="old-access",
            refresh_token="refresh-canary",
            obtained_at=EVENT_AT - timedelta(minutes=20),
            expires_at=EVENT_AT,
        ),
    )
    requests = []

    def respond(request):
        requests.append(request.url.path)
        if request.url.path == "/v1/oauth/token":
            expected = base64.b64encode(b"registered-key:registered-secret").decode()
            assert request.headers["Authorization"] == "Basic " + expected
            return httpx.Response(
                200,
                json={
                    "access_token": "registered-access",
                    "refresh_token": "rotated-refresh",
                    "expires_in": 1800,
                    "token_type": "Bearer",
                },
            )
        assert request.headers["Authorization"] == "Bearer registered-access"
        return httpx.Response(
            200,
            json={
                "streamerInfo": [
                    {
                        "streamerSocketUrl": "wss://streamer-api.schwab.com/ws",
                        "schwabClientCustomerId": "customer",
                        "schwabClientCorrelId": "correlation",
                        "schwabClientChannel": "channel",
                        "schwabClientFunctionId": "function",
                    }
                ]
            },
        )

    async def collect(**kwargs):
        assert kwargs["credentials"].access_token == "registered-access"
        assert kwargs["symbols"] == ("AAPL",)
        kwargs["directory"].mkdir(mode=0o700)
        return {"status": "bounded_complete"}

    expected = SchwabMarketStreamImportResult("a" * 64, 1, 1, UUID(int=1), {}, {})

    async def imported(*args, **kwargs):
        return expected

    class Session:
        async def scalar(self, statement):
            return EVENT_AT

    monkeypatch.setattr(stream, "import_schwab_market_stream", imported)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await stream.capture_and_import_schwab_level_one_once(
            Session(),
            object(),
            repo_root=tmp_path,
            client=client,
            instrument_id=AAPL_ID,
            checked_at=EVENT_AT,
            process_environment=environment,
            capture=partial(stream.run_schwab_stream, collector=collect),
        )
    assert result == expected
    assert requests == ["/v1/oauth/token", "/trader/v1/userPreference"]


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


@pytest.mark.parametrize(
    ("symbols", "observation_count"),
    [(("AAPL",), 1), (("AAPL", "AVGO", "NVDA"), 3)],
)
@pytest.mark.asyncio
async def test_bounded_capture_archive_uses_receive_window_not_socket_close_time(
    tmp_path, monkeypatch, symbols, observation_count
) -> None:
    from nexus.ops import schwab_market_stream as stream

    close_finished = False
    original_monotonic = stream.monotonic

    class SlowCloseSocket(Socket):
        async def __aexit__(self, *args):
            nonlocal close_finished
            close_finished = True

    def clock() -> float:
        return original_monotonic() + (40 if close_finished else 0)

    monkeypatch.setattr(stream, "monotonic", clock)
    data = json.loads(json.dumps(SCHWAB_DATA))
    row = data["data"][0]["content"][0]
    data["data"][0]["content"] = [{**row, "key": symbol} for symbol in symbols]
    directory = tmp_path / "slow-close"
    result = await collect_schwab_stream(
        symbols=symbols,
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        duration=30,
        max_frames=3,
        connector=lambda *args, **kwargs: SlowCloseSocket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, data]),
    )

    assert result["status"] == "bounded_complete"
    assert len(load_provider_market_stream_archive(directory)["observations"]) == observation_count


@pytest.mark.asyncio
async def test_capture_archive_failure_keeps_safe_phase_and_reason(tmp_path) -> None:
    from nexus.ops import schwab_market_stream as stream

    async def capture(arguments, client, *, now=None):
        del client, now
        summary = await collect_schwab_stream(
            symbols=("AAPL",),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=SCHWAB_CREDENTIALS,
            duration=30,
            max_frames=3,
            connector=lambda *args, **kwargs: Socket(
                [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, SCHWAB_DATA]
            ),
        )
        receipt_path = arguments.output_dir / "receipt.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["elapsed_monotonic_seconds"] = 40.0
        receipt_path.write_text(json.dumps(receipt))
        return summary

    with pytest.raises(
        stream.ProviderMarketStreamError, match="PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"
    ) as caught:
        await stream.capture_and_import_schwab_level_one_batch(
            object(),
            object(),
            repo_root=tmp_path,
            client=object(),
            bindings={"AAPL": UUID(int=1)},
            capture=capture,
        )
    assert caught.value.failure_stage == "archive_pack"
    assert caught.value.validation_reason == "CAPTURE_TIME_BOUND"
    assert caught.value.capture_summary["status"] == "bounded_complete"


@pytest.mark.asyncio
async def test_capture_import_failure_keeps_safe_capture_summary(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def capture(arguments, client, *, now=None):
        del client, now
        frame = json.loads(json.dumps(SCHWAB_DATA))
        observed_ms = int((datetime.now(UTC) - timedelta(seconds=2)).timestamp() * 1000)
        frame["data"][0]["timestamp"] = observed_ms
        for field in ("34", "35", "37", "38"):
            frame["data"][0]["content"][0][field] = observed_ms
        return await collect_schwab_stream(
            symbols=("AAPL",),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=SCHWAB_CREDENTIALS,
            duration=30,
            max_frames=3,
            connector=lambda *args, **kwargs: Socket([SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, frame]),
        )

    async def fail_import(*args, **kwargs):
        del args, kwargs
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")

    class Session:
        async def scalar(self, statement):
            del statement
            return datetime.now(UTC)

    monkeypatch.setattr(stream, "import_schwab_market_stream", fail_import)
    with pytest.raises(
        ProviderMarketStreamError, match="PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"
    ) as caught:
        await stream.capture_and_import_schwab_level_one_batch(
            Session(),
            object(),
            repo_root=tmp_path,
            client=object(),
            bindings={"AAPL": UUID(int=1)},
            capture=capture,
        )
    assert caught.value.failure_stage == "stream_import"
    assert caught.value.capture_summary["status"] == "bounded_complete"
    assert caught.value.capture_summary["observation_count"] == 1


@pytest.mark.asyncio
async def test_close_failure_cannot_make_capture_complete(tmp_path) -> None:
    class FailedCloseSocket(Socket):
        async def __aexit__(self, *args):
            raise RuntimeError("private close failure")

    directory = tmp_path / "close-failed"
    result = await collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=SCHWAB_CREDENTIALS,
        max_frames=3,
        connector=lambda *args, **kwargs: FailedCloseSocket(
            [SCHWAB_LOGIN, SCHWAB_SUBSCRIBED, SCHWAB_DATA]
        ),
    )
    assert result["status"] == "partial"
    assert result["stop_reason"] == "CLOSE_FAILED"
    with pytest.raises(ValueError, match="PROVIDER_MARKET_STREAM_INCOMPLETE"):
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
        ({"1": "0"}, "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID"),
        ({"1": "228", "2": "227"}, "SCHWAB_MARKET_STREAM_EVIDENCE_INVALID"),
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
async def test_schwab_snapshot_parser_rejects_quote_older_than_sixty_seconds(
    tmp_path,
) -> None:
    stale_millis = int((EVENT_AT - timedelta(seconds=61)).timestamp() * 1000)
    payload = await _packed_archive(
        tmp_path,
        ("AAPL",),
        [
            SCHWAB_LOGIN,
            SCHWAB_SUBSCRIBED,
            _data(
                **{
                    "34": stale_millis,
                    "35": stale_millis,
                    "37": stale_millis,
                    "38": stale_millis,
                }
            ),
        ],
        completed_at=EVENT_AT,
    )

    with pytest.raises(MarketProviderPayloadError, match="SCHWAB_MARKET_STREAM_EVIDENCE_STALE"):
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
