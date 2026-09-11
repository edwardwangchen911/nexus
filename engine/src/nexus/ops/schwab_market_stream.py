"""One bounded, private Schwab equity stream capture; never reconnects."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import logging
import os
import subprocess
import sys
import webbrowser
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any

import httpx
from websockets.asyncio.client import connect

from nexus.ops.local_recovery import write_private
from nexus.providers.market.schwab_oauth import (
    SchwabOAuthError,
    build_schwab_authorization_url,
    exchange_schwab_authorization_code,
    fetch_schwab_stream_preferences,
    load_schwab_app_config,
    load_schwab_token,
    parse_schwab_callback,
    refresh_schwab_access_token,
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

SCHEMA = "nexus_provider_market_stream_receipt_v1"
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024
MAX_FRAMES = 100
MAX_DURATION_SECONDS = 300
CLOSE_TIMEOUT_SECONDS = 1
TOKEN_REFRESH_WINDOW = timedelta(minutes=5)


class ProviderMarketStreamError(ValueError):
    """Stable, value-free bounded market stream error."""


class _FixedConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


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
    callback = callback_reader(
        "完成嘉信授权后，粘贴浏览器地址栏中的完整回调 URL："
    )
    code = parse_schwab_callback(config, callback)
    token = await exchange_schwab_authorization_code(
        config, code, client, now=now
    )
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
) -> dict[str, Any]:
    token = load_schwab_token(arguments.repo_root)
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise SchwabOAuthError("SCHWAB_OAUTH_TIME_INVALID")
    if token.expires_at - checked_at <= TOKEN_REFRESH_WINDOW:
        config = load_schwab_app_config(arguments.repo_root, {})
        token = await refresh_schwab_access_token(
            config, token, client, now=checked_at
        )
        save_schwab_token(arguments.repo_root, token)
    streamer_url, credentials = await fetch_schwab_stream_preferences(
        token, client, now=checked_at
    )
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
    config = load_schwab_app_config(repo_root, process_environment or {})
    token = load_schwab_token(repo_root)
    refreshed = await refresh_schwab_access_token(config, token, client, now=now)
    token_path = save_schwab_token(repo_root, refreshed)
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
    except asyncio.CancelledError as error:
        receipt["stop_reason"] = "CANCELLED"
        cancelled = error
    except Exception:
        pass
    receipt["ended_at"] = datetime.now(UTC).isoformat()
    receipt["elapsed_monotonic_seconds"] = monotonic() - started_monotonic
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


def load_provider_market_stream_archive(directory: Path) -> dict[str, Any]:
    """Revalidate a complete private archive and derive observations from originals."""
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
    _validate_receipt(receipt, frames)
    if receipt["status"] != "bounded_complete":
        raise ProviderMarketStreamError("PROVIDER_MARKET_STREAM_INCOMPLETE")
    state = SchwabLevelOneState(tuple(receipt["symbols"]))
    observations: list[Any] = []
    authenticated = subscribed = False
    for entry, raw in zip(receipt["frames"], frames, strict=True):
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
        "frames": tuple(frames),
        "observations": tuple(observations),
    }


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
    if set(value) != {"response"} or not isinstance(value["response"], list) or len(
        value["response"]
    ) != 1:
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
        "schema", "source", "transport", "origin", "symbols", "started_at", "ended_at",
        "authenticated", "subscription_confirmed", "status", "stop_reason", "frames",
        "observation_count", "requested_duration_seconds", "requested_max_frames",
        "elapsed_monotonic_seconds", "frame_count",
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
        receipt["status"] != expected_status
        or receipt["origin"] != expected_origin
        or started.tzinfo is None
        or ended.tzinfo is None
        or not started <= ended <= started + timedelta(seconds=duration + CLOSE_TIMEOUT_SECONDS)
        or not 0 <= receipt["elapsed_monotonic_seconds"] <= duration + CLOSE_TIMEOUT_SECONDS
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
            "status", "stop_reason", "authenticated", "subscription_confirmed",
            "observation_count", "frame_count",
        )
    }
