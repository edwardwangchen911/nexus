"""One bounded, private Schwab equity stream capture; never reconnects."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import getpass
import json
import logging
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import webbrowser
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake, ProxyError

from nexus.market.ingestion import MarketObjectStore, object_uri_matches_key
from nexus.market.intraday_ingestion import _get_or_create_artifact, _insert_observation
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, MarketDataProvenance
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.schwab_oauth import (
    SchwabOAuthError,
    build_schwab_authorization_url,
    exchange_schwab_authorization_code,
    fetch_schwab_stream_preferences,
    load_or_refresh_schwab_token,
    load_schwab_app_config,
    parse_schwab_callback,
    save_schwab_token,
)
from nexus.providers.market.schwab_stream import (
    SchwabLevelOneState,
    SchwabStreamCredentials,
    SchwabStreamError,
    build_level_one_subscription,
    build_login,
    validate_streamer_url,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    MarketSnapshot,
    SnapshotBatch,
    SnapshotInstrument,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.providers.news import validate_symbol
from nexus.risk.authority import require_schwab_execution_scope_capacity
from nexus.settings import EngineSettings

SCHEMA = "nexus_provider_market_stream_receipt_v1"
ARCHIVE_SCHEMA = "schwab_level_one_stream_v1"
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024
MAX_FRAMES = 100
MAX_DURATION_SECONDS = 300
CLOSE_TIMEOUT_SECONDS = 1
TOKEN_REFRESH_WINDOW = timedelta(minutes=5)


class ProviderMarketStreamError(ValueError):
    """Stable, value-free bounded market stream error."""

    def __init__(
        self,
        code: str,
        *,
        capture_summary: Mapping[str, Any] | None = None,
        failure_stage: str | None = None,
        validation_reason: str | None = None,
    ) -> None:
        super().__init__(code)
        self.capture_summary = None if capture_summary is None else dict(capture_summary)
        self.failure_stage = failure_stage
        self.validation_reason = validation_reason


@dataclass(frozen=True)
class SchwabMarketStreamImportResult:
    raw_sha256: str
    created_observations: int
    symbols_observed: int
    artifact_id: UUID
    quote_ids: dict[str, UUID]
    trade_ids: dict[str, UUID]


class _FixedConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


def _connect_failure_reason(error: Exception) -> str:
    """Classify a failed handshake without persisting URLs or exception text."""
    if isinstance(error, TimeoutError):
        return "CONNECT_TIMEOUT"
    if isinstance(error, ProxyError):
        return "CONNECT_PROXY_FAILED"
    if isinstance(error, ssl.SSLError):
        return "CONNECT_TLS_FAILED"
    if isinstance(error, socket.gaierror):
        return "CONNECT_DNS_FAILED"
    if isinstance(error, InvalidHandshake):
        return "CONNECT_HANDSHAKE_FAILED"
    if isinstance(error, ConnectionResetError):
        return "CONNECT_RESET"
    if isinstance(error, ConnectionRefusedError):
        return "CONNECT_REFUSED"
    if isinstance(error, OSError):
        return "CONNECT_NETWORK_FAILED"
    if isinstance(error, EOFError):
        return "CONNECT_EOF"
    if isinstance(error, RuntimeError):
        return "CONNECT_RUNTIME_FAILED"
    if isinstance(error, TypeError):
        return "CONNECT_TYPE_INVALID"
    if isinstance(error, ValueError):
        return "CONNECT_VALUE_INVALID"
    return "CONNECT_FAILED"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="source", required=True)
    authorize = subparsers.add_parser("authorize")
    authorize.add_argument("--repo-root", type=Path, required=True)
    refresh = subparsers.add_parser("refresh")
    refresh.add_argument("--repo-root", type=Path, required=True)
    child = subparsers.add_parser("schwab")
    child.add_argument("--repo-root", type=Path, required=True)
    child.add_argument("--symbol", action="append", required=True)
    child.add_argument("--output-dir", type=Path, required=True)
    child.add_argument("--duration", type=float, default=60)
    child.add_argument("--max-frames", type=int, default=100)
    paper_probe = subparsers.add_parser("paper-probe")
    paper_probe.add_argument("--repo-root", type=Path, required=True)
    paper_probe.add_argument("--binding", action="append", required=True)
    paper_probe.add_argument("--output-dir", type=Path, required=True)
    paper_probe.add_argument("--duration", type=float, default=30)
    import_parser = subparsers.add_parser("import")
    import_parser.add_argument("--repo-root", type=Path, required=True)
    import_parser.add_argument("--binding", action="append", required=True)
    import_parser.add_argument("--stream-dir", type=Path, required=True)
    return parser


def read_schwab_callback_dialog(
    prompt: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    """Collect the OAuth callback in a local macOS dialog without echoing it."""
    del prompt
    command = [
        "/usr/bin/osascript",
        "-e",
        'display dialog "请先在浏览器完成嘉信登录和授权。完成后回到这里。" '
        'buttons {"取消", "我已完成授权"} default button "我已完成授权" '
        'cancel button "取消" with title "NEXUS 嘉信授权"',
        "-e",
        'set callbackResult to display dialog "请粘贴浏览器地址栏中的完整 '
        'https://127.0.0.1 回调 URL。" default answer "" '
        'buttons {"取消", "继续"} default button "继续" cancel button "取消" '
        'with title "NEXUS 嘉信授权"',
        "-e",
        "return text returned of callbackResult",
    ]
    try:
        result = runner(
            command,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        raise ProviderMarketStreamError("SCHWAB_OAUTH_DIALOG_FAILED") from None
    callback = result.stdout.strip()
    if result.returncode != 0 or not callback or len(callback.encode()) > 16_384:
        raise ProviderMarketStreamError("SCHWAB_OAUTH_DIALOG_CANCELLED")
    return callback


async def authorize_schwab(
    repo_root: Path,
    client: httpx.AsyncClient,
    *,
    opener: Callable[[str], bool] = webbrowser.open,
    callback_reader: Callable[[str], str],
    process_environment: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = load_schwab_app_config(repo_root, process_environment or {})
    if not opener(build_schwab_authorization_url(config)):
        raise ProviderMarketStreamError("SCHWAB_OAUTH_BROWSER_OPEN_FAILED")
    callback = callback_reader("完成嘉信授权后，粘贴浏览器地址栏中的完整回调 URL：")
    code = parse_schwab_callback(config, callback)
    token = await exchange_schwab_authorization_code(config, code, client, now=now)
    token_path = save_schwab_token(repo_root, token)
    return {
        "status": "authorized",
        "token_path": str(token_path),
        "expires_at": token.expires_at.isoformat(),
        "secrets_printed": False,
    }


async def run_schwab_stream(
    arguments: argparse.Namespace,
    client: httpx.AsyncClient,
    *,
    collector: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    now: datetime | None = None,
    process_environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise SchwabOAuthError("SCHWAB_OAUTH_TIME_INVALID")
    token = await load_or_refresh_schwab_token(
        arguments.repo_root,
        client,
        process_environment=os.environ if process_environment is None else process_environment,
        now=checked_at,
        refresh_window=TOKEN_REFRESH_WINDOW,
    )
    streamer_url, credentials = await fetch_schwab_stream_preferences(token, client, now=checked_at)
    collect = collector or collect_schwab_stream
    return await collect(
        symbols=tuple(arguments.symbol),
        directory=arguments.output_dir,
        streamer_url=streamer_url,
        credentials=credentials,
        duration=arguments.duration,
        max_frames=arguments.max_frames,
    )


async def refresh_schwab(
    repo_root: Path,
    client: httpx.AsyncClient,
    *,
    process_environment: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    checked_at = now or datetime.now(UTC)
    refreshed = await load_or_refresh_schwab_token(
        repo_root,
        client,
        process_environment=process_environment or {},
        now=checked_at,
        refresh_window=TOKEN_REFRESH_WINDOW,
        force=True,
    )
    token_path = repo_root.resolve() / ".secrets" / "nexus-schwab-local" / "tokens.json"
    return {
        "status": "refreshed",
        "token_path": str(token_path),
        "expires_at": refreshed.expires_at.isoformat(),
        "secrets_printed": False,
    }


async def _run(
    arguments: argparse.Namespace,
    *,
    client: httpx.AsyncClient | None = None,
    opener: Callable[[str], bool] = webbrowser.open,
    callback_reader: Callable[[str], str] | None = None,
    dialog_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if arguments.source == "import":
        environment = load_local_environment(arguments.repo_root, os.environ)
        imported = await import_schwab_market_stream_local(
            EngineSettings.model_validate(environment),
            bindings=_parse_bindings(arguments.binding),
            directory=arguments.stream_dir,
        )
        return {
            "status": "stored_display_quote",
            "raw_sha256": imported.raw_sha256,
            "created_observations": imported.created_observations,
            "symbols_observed": imported.symbols_observed,
            "execution_locked": True,
            "model_called": False,
        }
    if client is None:
        async with httpx.AsyncClient(follow_redirects=False, timeout=10) as transport:
            return await _run(
                arguments,
                client=transport,
                opener=opener,
                callback_reader=callback_reader,
                dialog_runner=dialog_runner,
                now=now,
            )
    if arguments.source == "authorize":
        reader = callback_reader
        if reader is None:
            if sys.platform == "darwin":
                runner = dialog_runner or subprocess.run

                def native_dialog(prompt: str) -> str:
                    return read_schwab_callback_dialog(prompt, runner=runner)

                reader = native_dialog
            else:
                reader = getpass.getpass
        return await authorize_schwab(
            arguments.repo_root,
            client,
            opener=opener,
            callback_reader=reader,
            process_environment=os.environ,
            now=now,
        )
    if arguments.source == "refresh":
        return await refresh_schwab(
            arguments.repo_root,
            client,
            process_environment=os.environ,
            now=now,
        )
    if arguments.source == "paper-probe":
        bindings = _parse_bindings(arguments.binding)
        if len(bindings) > 5:
            raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
        _limits(arguments.duration, MAX_FRAMES)
        if arguments.output_dir.exists() or arguments.output_dir.is_symlink():
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_NEW_OUTPUT_REQUIRED")
        capture = await run_schwab_stream(
            argparse.Namespace(
                repo_root=arguments.repo_root,
                symbol=sorted(bindings),
                output_dir=arguments.output_dir,
                duration=arguments.duration,
                max_frames=MAX_FRAMES,
            ),
            client,
            now=now,
            process_environment={},
        )
        if capture.get("status") != "bounded_complete":
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_INCOMPLETE")
        return {
            **_summary(capture),
            **validate_paper_quote_probe(
                pack_provider_market_stream_archive(arguments.output_dir), bindings
            ),
        }
    return await run_schwab_stream(arguments, client, now=now)


def main() -> None:
    parser = build_parser()
    try:
        result = asyncio.run(_run(parser.parse_args()))
    except (ProviderMarketStreamError, SchwabOAuthError, SchwabStreamError) as error:
        parser.error(str(error))
    print(json.dumps(result, sort_keys=True))


async def collect_schwab_stream(
    *,
    symbols: tuple[str, ...],
    directory: Path,
    streamer_url: str,
    credentials: SchwabStreamCredentials,
    duration: float = 60,
    max_frames: int = 100,
    connector: Any = _FixedConnect,
) -> dict[str, Any]:
    url = validate_streamer_url(streamer_url)
    state = SchwabLevelOneState(symbols)
    return await _collect(
        symbols=symbols,
        directory=directory,
        url=url,
        duration=duration,
        max_frames=max_frames,
        connector=connector,
        state=state,
        credentials=credentials,
    )


async def capture_and_import_schwab_level_one_once(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    repo_root: Path,
    client: httpx.AsyncClient,
    instrument_id: UUID,
    checked_at: datetime | None = None,
    capture: Callable[..., Awaitable[dict[str, Any]]] = run_schwab_stream,
    process_environment: Mapping[str, str] | None = None,
) -> SchwabMarketStreamImportResult:
    """Capture and freeze one bounded AAPL Level One decision observation."""
    with tempfile.TemporaryDirectory(prefix="nexus-agent-schwab-") as raw_directory:
        directory = Path(raw_directory) / "capture"
        arguments = argparse.Namespace(
            repo_root=repo_root,
            symbol=["AAPL"],
            output_dir=directory,
            duration=30.0,
            max_frames=100,
        )
        capture_summary = await capture(
            arguments,
            client,
            now=checked_at,
            **(
                {"process_environment": process_environment}
                if process_environment is not None
                else {}
            ),
        )
        if capture_summary.get("status") != "bounded_complete":
            raise ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE",
                capture_summary=capture_summary,
            )
        imported_at = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(imported_at, datetime):
            raise RuntimeError("SCHWAB_MARKET_STREAM_DATABASE_CLOCK_INVALID")
        return await import_schwab_market_stream(
            session,
            store,
            bindings={"AAPL": instrument_id},
            directory=directory,
            imported_at=imported_at,
        )


async def capture_and_import_schwab_level_one_batch(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    repo_root: Path,
    client: httpx.AsyncClient,
    bindings: Mapping[str, UUID],
    max_instruments: int = 5,
    checked_at: datetime | None = None,
    capture: Callable[..., Awaitable[dict[str, Any]]] = run_schwab_stream,
    process_environment: Mapping[str, str] | None = None,
) -> SchwabMarketStreamImportResult:
    """Freeze one capture only after every explicitly bound candidate has a fresh quote."""
    try:
        require_schwab_execution_scope_capacity(
            tuple(bindings.values()), max_instruments=max_instruments
        )
    except ValueError as error:
        raise ProviderMarketStreamError(str(error)) from None
    if any(not isinstance(value, UUID) for value in bindings.values()) or len(
        set(bindings.values())
    ) != len(bindings):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
    symbols = tuple(sorted(bindings))
    for symbol in symbols:
        validate_symbol(symbol)
    with tempfile.TemporaryDirectory(prefix="nexus-agent-schwab-") as raw_directory:
        directory = Path(raw_directory) / "capture"
        summary = await capture(
            argparse.Namespace(
                repo_root=repo_root,
                symbol=list(symbols),
                output_dir=directory,
                duration=30.0,
                max_frames=100,
            ),
            client,
            now=checked_at,
            **(
                {"process_environment": process_environment}
                if process_environment is not None
                else {}
            ),
        )
        if summary.get("status") != "bounded_complete":
            raise ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE", capture_summary=summary
            )
        try:
            raw = pack_provider_market_stream_archive(directory)
        except ProviderMarketStreamError as error:
            raise ProviderMarketStreamError(
                str(error),
                capture_summary=summary,
                failure_stage="archive_pack",
                validation_reason=error.validation_reason,
            ) from error
        try:
            archive = _unpack_provider_market_stream_archive(raw)
        except MarketProviderPayloadError as error:
            raise ProviderMarketStreamError(
                str(error), capture_summary=summary, failure_stage="archive_replay"
            ) from error
        completed_at = datetime.fromisoformat(archive["receipt"]["ended_at"]).astimezone(UTC)
        try:
            batch = parse_schwab_snapshot_batch(
                raw,
                IntradaySnapshotRequest(
                    instruments=tuple(
                        SnapshotInstrument(instrument_id=InstrumentId(bindings[s]), symbol=s)
                        for s in symbols
                    ),
                    symbol_as_of=completed_at.date(),
                    feed="l1",
                    delivery_mode="real_time",
                ),
            )
        except MarketProviderPayloadError as error:
            raise ProviderMarketStreamError(
                str(error), capture_summary=summary, failure_stage="snapshot_parse"
            ) from error
        if {row.symbol for row in batch.snapshots} != set(symbols) or any(
            row.quote is None
            or not timedelta(0) <= completed_at - row.quote.observed_at <= timedelta(seconds=60)
            for row in batch.snapshots
        ):
            raise ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE", capture_summary=summary
            )
        imported_at = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(imported_at, datetime):
            raise RuntimeError("SCHWAB_MARKET_STREAM_DATABASE_CLOCK_INVALID")
        try:
            return await import_schwab_market_stream(
                session,
                store,
                bindings=dict(bindings),
                directory=directory,
                imported_at=imported_at,
            )
        except ProviderMarketStreamError as error:
            raise ProviderMarketStreamError(
                str(error), capture_summary=summary, failure_stage="stream_import"
            ) from error


async def _collect(
    *,
    symbols: tuple[str, ...],
    directory: Path,
    url: str,
    duration: float,
    max_frames: int,
    connector: Any,
    state: SchwabLevelOneState,
    credentials: SchwabStreamCredentials,
) -> dict[str, Any]:
    requested_duration, requested_frames = _limits(duration, max_frames)
    directory.mkdir(mode=0o700)
    started_at = datetime.now(UTC)
    started_monotonic = monotonic()
    deadline = started_monotonic + requested_duration
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "source": "schwab",
        "transport": "websocket",
        "origin": url,
        "symbols": list(symbols),
        "started_at": started_at.isoformat(),
        "authenticated": False,
        "subscription_confirmed": False,
        "status": "failed",
        "stop_reason": "CONNECT_FAILED",
        "frames": [],
        "observation_count": 0,
        "requested_duration_seconds": requested_duration,
        "requested_max_frames": requested_frames,
    }
    byte_count = 0
    cancelled: asyncio.CancelledError | None = None
    capture_ended_at: datetime | None = None
    capture_elapsed: float | None = None
    private_logger = logging.Logger(
        "nexus.schwab_market_stream.private", level=logging.CRITICAL + 1
    )
    private_logger.propagate = False
    try:
        async with connector(
            url,
            open_timeout=min(10, requested_duration),
            close_timeout=CLOSE_TIMEOUT_SECONDS,
            max_size=MAX_FRAME,
            max_queue=1,
            compression=None,
            logger=private_logger,
        ) as socket:
            try:
                receipt["stop_reason"] = "DISCONNECTED"
                await socket.send(json.dumps(build_login(credentials, request_id=1)))
                while len(receipt["frames"]) < requested_frames:
                    now = monotonic()
                    remaining = deadline - now
                    if remaining <= 0:
                        receipt["stop_reason"] = "DURATION_LIMIT"
                        break
                    try:
                        frame = await asyncio.wait_for(socket.recv(), timeout=remaining)
                    except TimeoutError:
                        receipt["stop_reason"] = "DURATION_LIMIT"
                        break
                    raw = frame.encode() if isinstance(frame, str) else frame
                    if not isinstance(raw, bytes):
                        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_FRAME_INVALID")
                    byte_count += len(raw)
                    if len(raw) > MAX_FRAME or byte_count > MAX_BYTES:
                        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_BYTE_LIMIT")
                    value = _json_object(raw)
                    decoded = json.dumps(value, ensure_ascii=False, default=str)
                    if (
                        credentials.access_token.encode() in raw
                        or credentials.access_token in decoded
                    ):
                        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_SECRET_REFLECTION")
                    received_at = datetime.now(UTC)
                    name = f"frame-{len(receipt['frames']):05}.json"
                    write_private(directory / name, raw)
                    receipt["frames"].append(
                        {
                            "file": name,
                            "received_at": received_at.isoformat(),
                            "raw_sha256": sha256(raw).hexdigest(),
                            "raw_byte_length": len(raw),
                        }
                    )
                    control = _schwab_control(value)
                    if control == "login":
                        receipt["authenticated"] = True
                        await socket.send(
                            json.dumps(
                                build_level_one_subscription(credentials, symbols, request_id=2)
                            )
                        )
                    elif control == "subscription":
                        if not receipt["authenticated"]:
                            raise ProviderMarketStreamError(
                                "PROVIDER_MARKET_STREAM_PROTOCOL_INVALID"
                            )
                        receipt["subscription_confirmed"] = True
                    elif control == "heartbeat":
                        continue
                    else:
                        if not receipt["subscription_confirmed"]:
                            raise ProviderMarketStreamError(
                                "PROVIDER_MARKET_STREAM_PROTOCOL_INVALID"
                            )
                        receipt["observation_count"] += len(
                            state.apply(value, received_at=received_at)
                        )
                else:
                    receipt["stop_reason"] = "FRAME_LIMIT"
            except asyncio.CancelledError as error:
                receipt["stop_reason"] = "CANCELLED"
                cancelled = error
            except (ProviderMarketStreamError, SchwabStreamError) as error:
                receipt["stop_reason"] = str(error)
            except Exception:
                pass
            finally:
                # The receipt bounds received evidence, not the WebSocket close handshake.
                capture_ended_at = datetime.now(UTC)
                capture_elapsed = monotonic() - started_monotonic
    except asyncio.CancelledError as error:
        receipt["stop_reason"] = "CANCELLED"
        cancelled = error
    except Exception as error:
        if capture_ended_at is not None:
            receipt["stop_reason"] = "CLOSE_FAILED"
        else:
            receipt["stop_reason"] = _connect_failure_reason(error)
    receipt["ended_at"] = (capture_ended_at or datetime.now(UTC)).isoformat()
    receipt["elapsed_monotonic_seconds"] = (
        capture_elapsed if capture_elapsed is not None else monotonic() - started_monotonic
    )
    receipt["frame_count"] = len(receipt["frames"])
    receipt["status"] = (
        "bounded_complete"
        if receipt["subscription_confirmed"]
        and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if receipt["subscription_confirmed"]
        else "failed"
    )
    write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    if cancelled is not None:
        raise cancelled
    return _summary(receipt)


def _read_provider_market_stream_files(directory: Path) -> tuple[bytes, tuple[bytes, ...]]:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(descriptor).st_mode & 0o077:
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
        receipt_raw = _read(descriptor, "receipt.json", 256 * 1024)
        receipt = _receipt_object(receipt_raw)
        if not isinstance(receipt.get("frames"), list):
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
        frames = [
            _read(descriptor, f"frame-{index:05}.json", MAX_FRAME)
            for index in range(len(receipt["frames"]))
        ]
        expected = {"receipt.json", *(f"frame-{index:05}.json" for index in range(len(frames)))}
        if set(os.listdir(descriptor)) != expected:
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
    finally:
        os.close(descriptor)
    return receipt_raw, tuple(frames)


def _decode_provider_market_stream_archive(
    receipt_raw: bytes,
    frames: tuple[bytes, ...],
    *,
    require_complete: bool,
    derive_observations: bool = True,
) -> dict[str, Any]:
    receipt = _receipt_object(receipt_raw)
    frame_list = list(frames)
    _validate_receipt(receipt, frame_list)
    if require_complete and receipt["status"] != "bounded_complete":
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_INCOMPLETE")
    observations: list[Any] = []
    if not derive_observations:
        return {
            "receipt": receipt,
            "receipt_raw": receipt_raw,
            "frames": frames,
            "observations": (),
        }
    state = SchwabLevelOneState(tuple(receipt["symbols"]))
    authenticated = subscribed = False
    for entry, raw in zip(receipt["frames"], frame_list, strict=True):
        value = _json_object(raw)
        received_at = datetime.fromisoformat(entry["received_at"])
        control = _schwab_control(value)
        if control == "login":
            authenticated = True
        elif control == "subscription":
            subscribed = authenticated
        elif control == "heartbeat":
            continue
        else:
            if not subscribed:
                raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
            observations.extend(state.apply(value, received_at=received_at))
    if (
        authenticated != receipt["authenticated"]
        or subscribed != receipt["subscription_confirmed"]
        or len(observations) != receipt["observation_count"]
    ):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
    return {
        "receipt": receipt,
        "receipt_raw": receipt_raw,
        "frames": frames,
        "observations": tuple(observations),
    }


def load_provider_market_stream_archive(directory: Path) -> dict[str, Any]:
    """Revalidate a complete private archive and derive observations from originals."""
    receipt_raw, frames = _read_provider_market_stream_files(directory)
    return _decode_provider_market_stream_archive(
        receipt_raw,
        frames,
        require_complete=True,
    )


def _frozen_schwab_payload(receipt_raw: bytes, frames: tuple[bytes, ...]) -> bytes:
    return json.dumps(
        {
            "schema": ARCHIVE_SCHEMA,
            "receipt_base64": base64.b64encode(receipt_raw).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def pack_provider_market_stream_archive(directory: Path) -> bytes:
    """Validate and package bounded capture files before interpreting completeness."""
    receipt_raw, frames = _read_provider_market_stream_files(directory)
    _decode_provider_market_stream_archive(
        receipt_raw,
        frames,
        require_complete=False,
        derive_observations=False,
    )
    return _frozen_schwab_payload(receipt_raw, frames)


def _unpack_provider_market_stream_archive(raw_payload: bytes) -> dict[str, Any]:
    try:
        if not raw_payload or len(raw_payload) > MAX_BYTES * 2:
            raise ValueError
        value = json.loads(raw_payload, object_pairs_hook=_unique)
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "receipt_base64",
            "frames_base64",
        }:
            raise ValueError
        if value["schema"] != ARCHIVE_SCHEMA or not isinstance(value["frames_base64"], list):
            raise ValueError
        receipt_raw = base64.b64decode(value["receipt_base64"], validate=True)
        frames = tuple(base64.b64decode(frame, validate=True) for frame in value["frames_base64"])
        return _decode_provider_market_stream_archive(
            receipt_raw,
            frames,
            require_complete=True,
        )
    except (
        TypeError,
        ValueError,
        KeyError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        binascii.Error,
        ProviderMarketStreamError,
        SchwabStreamError,
    ) as error:
        code = str(error)
        if code not in {
            "PROVIDER_MARKET_STREAM_INCOMPLETE",
            "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
        }:
            code = "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID"
        raise MarketProviderPayloadError(code) from None


def parse_schwab_snapshot_batch(
    raw_payload: bytes,
    request: IntradaySnapshotRequest,
) -> SnapshotBatch:
    """Normalize a complete real-time Schwab transcript for its exact request."""
    if request.feed != "l1" or request.delivery_mode != "real_time" or request.include_minute_bars:
        raise MarketProviderPayloadError("SCHWAB_INTRADAY_AUTHORITY_MISMATCH")
    archive = _unpack_provider_market_stream_archive(raw_payload)
    receipt = archive["receipt"]
    expected = {item.symbol: item for item in request.instruments}
    if set(receipt["symbols"]) != set(expected) or len(receipt["symbols"]) != len(expected):
        raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
    completed_at = datetime.fromisoformat(receipt["ended_at"]).astimezone(UTC)
    observations = archive["observations"]
    if any(
        observation.delayed is not False
        or observation.received_at > completed_at
        or observation.symbol not in expected
        for observation in observations
    ):
        raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")

    snapshots: list[MarketSnapshot] = []
    for symbol, instrument in expected.items():
        values = [item for item in observations if item.symbol == symbol]
        if any((item.bid is None) != (item.ask is None) for item in values):
            raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
        trades = [item for item in values if item.last is not None and item.trade_time is not None]
        quotes: list[tuple[Any, datetime]] = []
        for item in values:
            quote_time = max(
                (
                    time
                    for time in (item.bid_time, item.ask_time, item.quote_time)
                    if time is not None
                ),
                default=None,
            )
            if item.bid is not None and item.ask is not None and quote_time is not None:
                if item.bid <= 0 or item.ask <= 0 or item.bid > item.ask:
                    raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
                quotes.append((item, quote_time))
        trade = max(
            trades,
            key=lambda item: (
                item.trade_time,
                -1 if item.sequence is None else item.sequence,
                item.received_at,
            ),
            default=None,
        )
        quote_pair = max(
            quotes,
            key=lambda pair: (
                pair[1],
                -1 if pair[0].sequence is None else pair[0].sequence,
                pair[0].received_at,
            ),
            default=None,
        )
        trade_time = None if trade is None else trade.trade_time
        quote = None if quote_pair is None else quote_pair[0]
        quote_time = None if quote_pair is None else quote_pair[1]
        if trade is not None and (trade.last is None or trade.last <= 0):
            raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
        if any(time is not None and time > completed_at for time in (trade_time, quote_time)):
            raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
        if quote_time is not None and completed_at - quote_time > timedelta(seconds=60):
            raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EVIDENCE_STALE")
        if trade is None and quote is None:
            continue
        snapshots.append(
            MarketSnapshot(
                instrument_id=InstrumentId(instrument.instrument_id),
                symbol=symbol,
                trade=(
                    None
                    if trade is None or trade_time is None
                    else SnapshotTrade(price=trade.last, observed_at=trade_time)
                ),
                quote=(
                    None
                    if quote is None or quote_time is None
                    else SnapshotQuote(
                        bid=quote.bid,
                        ask=quote.ask,
                        observed_at=quote_time,
                    )
                ),
                minute_bar=None,
            )
        )
    if not snapshots:
        raise MarketProviderPayloadError("SCHWAB_MARKET_STREAM_EMPTY")
    return SnapshotBatch(
        snapshots=tuple(snapshots),
        raw_payload=raw_payload,
        received_at=completed_at,
        provenance=MarketDataProvenance(
            source="schwab",
            feed="l1",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )


def validate_paper_quote_probe(
    raw_payload: bytes, bindings: Mapping[str, UUID]
) -> dict[str, object]:
    """Check the decision quote contract without importing or claiming a cycle."""
    if not 1 <= len(bindings) <= 5 or len(set(bindings.values())) != len(bindings):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
    try:
        archive = _unpack_provider_market_stream_archive(raw_payload)
        completed_at = datetime.fromisoformat(archive["receipt"]["ended_at"]).astimezone(UTC)
        batch = parse_schwab_snapshot_batch(
            raw_payload,
            IntradaySnapshotRequest(
                instruments=tuple(
                    SnapshotInstrument(instrument_id=InstrumentId(bindings[symbol]), symbol=symbol)
                    for symbol in sorted(bindings)
                ),
                symbol_as_of=completed_at.date(),
                feed="l1",
                delivery_mode="real_time",
            ),
        )
    except MarketProviderPayloadError as error:
        raise ProviderMarketStreamError(str(error)) from None
    if {item.symbol for item in batch.snapshots} != set(bindings) or any(
        item.quote is None for item in batch.snapshots
    ):
        raise ProviderMarketStreamError("SCHWAB_L1_INCOMPLETE")
    return {
        "paper_quote_status": "passed_at_observation",
        "candidate_count": len(bindings),
        "execution_locked": True,
    }


async def import_schwab_market_stream(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    bindings: dict[str, UUID],
    directory: Path,
    imported_at: datetime,
) -> SchwabMarketStreamImportResult:
    """Import each latest verified trade or two-sided quote observed in the request."""
    if imported_at.tzinfo is None or imported_at.utcoffset() != timedelta(0):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_IMPORT_TIME_INVALID")
    raw_payload = pack_provider_market_stream_archive(directory)
    archive = _unpack_provider_market_stream_archive(raw_payload)
    receipt = archive["receipt"]
    symbols = tuple(receipt["symbols"])
    completed_at = datetime.fromisoformat(receipt["ended_at"]).astimezone(UTC)
    if (
        set(bindings) != set(symbols)
        or any(not isinstance(value, UUID) for value in bindings.values())
        or len(set(bindings.values())) != len(bindings)
    ):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
    if completed_at > imported_at:
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
    try:
        batch = parse_schwab_snapshot_batch(
            raw_payload,
            IntradaySnapshotRequest(
                instruments=tuple(
                    SnapshotInstrument(
                        instrument_id=InstrumentId(bindings[symbol]),
                        symbol=symbol,
                    )
                    for symbol in symbols
                ),
                symbol_as_of=completed_at.date(),
                feed="l1",
                delivery_mode="real_time",
            ),
        )
    except MarketProviderPayloadError as error:
        raise ProviderMarketStreamError(str(error)) from None
    selected = {
        snapshot.symbol: (
            snapshot.trade,
            None if snapshot.trade is None else snapshot.trade.observed_at,
            snapshot.quote,
            None if snapshot.quote is None else snapshot.quote.observed_at,
        )
        for snapshot in batch.snapshots
    }

    identity_rows = (
        await session.execute(
            select(InstrumentAlias, Instrument)
            .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
            .where(
                InstrumentAlias.instrument_id.in_(tuple(bindings.values())),
                InstrumentAlias.symbol.in_(symbols),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.created_at <= completed_at,
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
        )
    ).all()
    for symbol, instrument_id in bindings.items():
        event_dates = {
            timestamp.date()
            for timestamp in selected.get(symbol, (None, None, None, None))[1::2]
            if timestamp is not None
        } or {completed_at.date()}
        for event_date in event_dates:
            matches = [
                alias
                for alias, _ in identity_rows
                if alias.symbol == symbol
                and alias.instrument_id == instrument_id
                and alias.valid_from <= event_date
                and (alias.valid_to is None or alias.valid_to >= event_date)
            ]
            if len(matches) != 1:
                raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_IDENTITY_REQUIRED")

    raw_digest = sha256(raw_payload).hexdigest()
    raw_key = f"raw/market/schwab/stream/{completed_at:%Y/%m/%d}/{raw_digest}.json"
    raw_uri = await store.archive_raw(raw_key, raw_payload)
    if not object_uri_matches_key(raw_uri, raw_key):
        raise RuntimeError("SCHWAB_MARKET_STREAM_OBJECT_URI_MISMATCH")
    if await store.read_raw(raw_key, uri=raw_uri) != raw_payload:
        raise RuntimeError("SCHWAB_MARKET_STREAM_OBJECT_READBACK_MISMATCH")
    event_times = [
        timestamp
        for item in selected.values()
        for timestamp in (item[1], item[3])
        if timestamp is not None
    ]
    artifact, _ = await _get_or_create_artifact(
        session,
        uri=raw_uri,
        digest=raw_digest,
        byte_size=len(raw_payload),
        mime_type="application/json",
        schema_version=ARCHIVE_SCHEMA,
        source="schwab",
        observed_at=max(event_times),
        available_at=completed_at,
        received_at=completed_at,
    )
    if (
        artifact.observed_at != max(event_times)
        or artifact.available_at != completed_at
        or artifact.received_at != completed_at
    ):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_ARTIFACT_REPLAY_CONFLICT")

    created = 0
    for symbol, (trade, trade_time, quote, quote_time) in selected.items():
        if trade is not None and trade_time is not None:
            created += int(
                await _insert_observation(
                    session,
                    raw_artifact=artifact,
                    instrument_id=bindings[symbol],
                    source="schwab",
                    feed="l1",
                    delivery_mode="real_time",
                    event_kind="trade",
                    bid=None,
                    ask=None,
                    last=trade.price,
                    observed_at=trade_time,
                    available_at=completed_at,
                    received_at=completed_at,
                    revision=raw_digest,
                    quality_flags=(),
                    provider_binding=None,
                )
            )
        if quote is not None and quote_time is not None:
            created += int(
                await _insert_observation(
                    session,
                    raw_artifact=artifact,
                    instrument_id=bindings[symbol],
                    source="schwab",
                    feed="l1",
                    delivery_mode="real_time",
                    event_kind="quote",
                    bid=quote.bid,
                    ask=quote.ask,
                    last=None,
                    observed_at=quote_time,
                    available_at=completed_at,
                    received_at=completed_at,
                    revision=raw_digest,
                    quality_flags=(),
                    provider_binding=None,
                )
            )
    observations = tuple(
        await session.scalars(
            select(LatestQuote).where(
                LatestQuote.data_artifact_id == artifact.id,
                LatestQuote.instrument_id.in_(tuple(bindings.values())),
                LatestQuote.source == "schwab",
                LatestQuote.feed == "l1",
                LatestQuote.delivery_mode == "real_time",
                LatestQuote.event_kind.in_(("quote", "trade")),
            )
        )
    )
    by_symbol = {instrument_id: symbol for symbol, instrument_id in bindings.items()}
    quote_ids = {
        by_symbol[row.instrument_id]: row.id for row in observations if row.event_kind == "quote"
    }
    trade_ids = {
        by_symbol[row.instrument_id]: row.id for row in observations if row.event_kind == "trade"
    }
    expected_quote_symbols = {
        symbol
        for symbol, (_, _, quote, quote_time) in selected.items()
        if quote is not None and quote_time is not None
    }
    expected_trade_symbols = {
        symbol
        for symbol, (trade, trade_time, _, _) in selected.items()
        if trade is not None and trade_time is not None
    }
    if set(quote_ids) != expected_quote_symbols or set(trade_ids) != expected_trade_symbols:
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EVIDENCE_INVALID")
    return SchwabMarketStreamImportResult(
        raw_sha256=raw_digest,
        created_observations=created,
        symbols_observed=len(selected),
        artifact_id=artifact.id,
        quote_ids=quote_ids,
        trade_ids=trade_ids,
    )


def _parse_bindings(values: list[str]) -> dict[str, UUID]:
    bindings: dict[str, UUID] = {}
    try:
        for value in values:
            symbol_raw, separator, instrument_raw = value.partition("=")
            if not separator:
                raise ValueError
            symbol = validate_symbol(symbol_raw)
            if symbol in bindings:
                raise ValueError
            bindings[symbol] = UUID(instrument_raw)
    except (AttributeError, TypeError, ValueError):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID") from None
    if not bindings:
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_BINDINGS_INVALID")
    return bindings


def _require_local_services(settings: EngineSettings) -> None:
    database = make_url(settings.database_url) if settings.database_url else None
    endpoint = urlsplit(settings.object_store_endpoint or "")
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        database is None
        or database.drivername != "postgresql+asyncpg"
        or database.host not in local
        or database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_LOCAL_SERVICES_REQUIRED")


async def import_schwab_market_stream_local(
    settings: EngineSettings,
    *,
    bindings: dict[str, UUID],
    directory: Path,
) -> SchwabMarketStreamImportResult:
    _require_local_services(settings)
    store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    async with asyncio.timeout(120):
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            head = await session.scalar(
                text(
                    "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END "
                    "FROM public.alembic_version"
                )
            )
            if head != CURRENT_MIGRATION_HEAD:
                raise ProviderMarketStreamError("SCHWAB_MARKET_STREAM_SCHEMA_NOT_READY")
            imported_at = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(imported_at, datetime):
                raise RuntimeError("SCHWAB_MARKET_STREAM_DATABASE_CLOCK_INVALID")
            return await import_schwab_market_stream(
                session,
                store,
                bindings=bindings,
                directory=directory,
                imported_at=imported_at,
            )


def _schwab_control(value: dict[str, Any]) -> str | None:
    if "notify" in value:
        if (
            set(value) != {"notify"}
            or not isinstance(value["notify"], list)
            or len(value["notify"]) != 1
            or not isinstance(value["notify"][0], dict)
            or set(value["notify"][0]) != {"heartbeat"}
            or not isinstance(value["notify"][0]["heartbeat"], str)
            or not value["notify"][0]["heartbeat"].isascii()
            or not value["notify"][0]["heartbeat"].isdecimal()
        ):
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_PROTOCOL_INVALID")
        return "heartbeat"
    if "response" not in value:
        return None
    if (
        set(value) != {"response"}
        or not isinstance(value["response"], list)
        or len(value["response"]) != 1
    ):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_PROTOCOL_INVALID")
    response = value["response"][0]
    if not isinstance(response, dict) or not isinstance(response.get("content"), dict):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_PROTOCOL_INVALID")
    if response["content"].get("code") != 0:
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_PROVIDER_REJECTED")
    key = (response.get("service"), response.get("command"), response.get("requestid"))
    if key == ("ADMIN", "LOGIN", "1"):
        return "login"
    if key == ("LEVELONE_EQUITIES", "SUBS", "2"):
        return "subscription"
    raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_PROTOCOL_INVALID")


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw, parse_float=Decimal, object_pairs_hook=_unique)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_FRAME_INVALID") from None
    if not isinstance(value, dict):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_FRAME_INVALID")
    return value


def _receipt_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID") from None
    if not isinstance(value, dict):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
    return value


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_DUPLICATE_KEY")
        result[key] = value
    return result


def _limits(duration: float, max_frames: int) -> tuple[float, int]:
    if (
        type(duration) not in {int, float}
        or type(max_frames) is not int
        or not 0 < duration <= MAX_DURATION_SECONDS
        or not 1 <= max_frames <= MAX_FRAMES
    ):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_LIMIT_INVALID")
    return float(duration), max_frames


def _validate_receipt(receipt: dict[str, Any], frames: list[bytes]) -> None:
    required = {
        "schema",
        "source",
        "transport",
        "origin",
        "symbols",
        "started_at",
        "ended_at",
        "authenticated",
        "subscription_confirmed",
        "status",
        "stop_reason",
        "frames",
        "observation_count",
        "requested_duration_seconds",
        "requested_max_frames",
        "elapsed_monotonic_seconds",
        "frame_count",
    }
    if set(receipt) != required or (
        receipt.get("schema") != SCHEMA
        or receipt.get("source") != "schwab"
        or receipt.get("transport") != "websocket"
        or receipt.get("status") not in {"bounded_complete", "partial", "failed"}
        or not isinstance(receipt.get("stop_reason"), str)
        or type(receipt.get("authenticated")) is not bool
        or type(receipt.get("subscription_confirmed")) is not bool
        or type(receipt.get("observation_count")) is not int
        or receipt.get("observation_count", -1) < 0
        or type(receipt.get("frame_count")) is not int
        or receipt.get("frame_count", -1) < 0
        or type(receipt.get("requested_duration_seconds")) is not float
        or type(receipt.get("requested_max_frames")) is not int
        or type(receipt.get("elapsed_monotonic_seconds")) is not float
        or not isinstance(receipt.get("symbols"), list)
        or not isinstance(receipt.get("frames"), list)
        or len(frames) != receipt.get("frame_count")
        or len(frames) != len(receipt["frames"])
        or (receipt.get("subscription_confirmed") and not receipt.get("authenticated"))
    ):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
    duration, frame_limit = _limits(
        receipt["requested_duration_seconds"], receipt["requested_max_frames"]
    )
    expected_status = (
        "bounded_complete"
        if receipt["subscription_confirmed"]
        and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if receipt["subscription_confirmed"]
        else "failed"
    )
    expected_origin = "wss://streamer-api.schwab.com/ws"
    started = datetime.fromisoformat(receipt["started_at"])
    ended = datetime.fromisoformat(receipt["ended_at"])
    if (
        started.tzinfo is not None
        and ended.tzinfo is not None
        and (
            not started <= ended <= started + timedelta(seconds=duration + CLOSE_TIMEOUT_SECONDS)
            or not 0 <= receipt["elapsed_monotonic_seconds"] <= duration + CLOSE_TIMEOUT_SECONDS
        )
    ):
        raise ProviderMarketStreamError(
            "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID", validation_reason="CAPTURE_TIME_BOUND"
        )
    if (
        receipt["status"] != expected_status
        or receipt["origin"] != expected_origin
        or started.tzinfo is None
        or ended.tzinfo is None
        or len(frames) > frame_limit
        or sum(map(len, frames)) > MAX_BYTES
        or (
            receipt["status"] == "bounded_complete"
            and receipt["stop_reason"] == "FRAME_LIMIT"
            and len(frames) != frame_limit
        )
    ):
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
    previous: datetime | None = None
    for index, (entry, raw) in enumerate(zip(receipt["frames"], frames, strict=True)):
        if not isinstance(entry, dict) or entry != {
            "file": f"frame-{index:05}.json",
            "received_at": entry.get("received_at"),
            "raw_sha256": sha256(raw).hexdigest(),
            "raw_byte_length": len(raw),
        }:
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
        received = datetime.fromisoformat(entry["received_at"])
        if not started <= received <= ended or (previous is not None and received < previous):
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
        previous = received


def _read(descriptor: int, name: str, maximum: int) -> bytes:
    child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
    try:
        metadata = os.fstat(child)
        if metadata.st_mode & 0o077 or metadata.st_size > maximum:
            raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_ARCHIVE_INVALID")
        return os.read(child, maximum + 1)
    finally:
        os.close(child)


def _summary(receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        key: receipt[key]
        for key in (
            "status",
            "stop_reason",
            "authenticated",
            "subscription_confirmed",
            "observation_count",
            "frame_count",
        )
    }
