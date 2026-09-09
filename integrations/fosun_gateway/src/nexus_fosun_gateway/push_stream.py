"""One explicit, bounded Fosun quote subscription with a private raw archive."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from time import monotonic
from typing import Any

from websockets.asyncio.client import connect

PUSH_URL = "wss://openapi-push.fosunxcz.com/msg/v1/push"
SCHEMA = "nexus_fosun_push_receipt_v1"
TOPIC = "qt"
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024
MAX_DURATION_SECONDS = 300
MAX_FRAMES = 100
CLOSE_TIMEOUT_SECONDS = 1
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15.0
MAX_POWER = 12
DYNAMIC_QUOTE_FIELDS = frozenset(
    {
        "qtTime",
        "qtDate",
        "power",
        "tradeStatus",
        "price",
        "chgVal",
        "chgPct",
        "pClose",
        "open",
        "high",
        "low",
        "vol",
        "turnover",
    }
)


class PushStreamError(ValueError):
    """A stable, value-free collector or archive error."""


class _FixedConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _message(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique)
    except (TypeError, ValueError, UnicodeError):
        raise PushStreamError("FRAME_INVALID") from None
    if not isinstance(value, dict):
        raise PushStreamError("FRAME_INVALID")
    return value


def _validate_code(code: str) -> str:
    if (
        not isinstance(code, str)
        or not (
            (
                4 <= len(code) <= 17
                and code.startswith("us")
                and code[2:].replace(".", "").replace("-", "").isalnum()
                and code[2:].isupper()
            )
            or (
                len(code) == 7
                and code.startswith("hk")
                and code[2:].isascii()
                and code[2:].isdigit()
            )
        )
    ):
        raise PushStreamError("PUSH_CODE_INVALID")
    return code


def _validate_limits(duration: float, max_frames: int) -> tuple[float, int]:
    if (
        type(duration) not in {int, float}
        or type(max_frames) is not int
        or not 0 < duration <= MAX_DURATION_SECONDS
        or not 1 <= max_frames <= MAX_FRAMES
    ):
        raise PushStreamError("PUSH_LIMIT_INVALID")
    return float(duration), max_frames


def _validate_heartbeat_interval(value: float) -> float:
    if type(value) not in {int, float} or not 0 < value <= DEFAULT_HEARTBEAT_INTERVAL_SECONDS:
        raise PushStreamError("PUSH_HEARTBEAT_INTERVAL_INVALID")
    return float(value)


def _validate_credential(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or any(not 33 <= ord(character) <= 126 for character in value)
    ):
        raise PushStreamError("PUSH_CREDENTIALS_INVALID")
    return value


def _validate_request_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 64
        or not value.isascii()
        or any(not (character.isalnum() or character in "-_") for character in value)
    ):
        raise PushStreamError("PUSH_REQUEST_ID_INVALID")
    return value


def _validate_subscription(message: dict[str, Any], *, code: str, request_id: str) -> None:
    reply = message.get("subRep")
    if (
        message.get("topic") != TOPIC
        or message.get("requestid") != request_id
        or not isinstance(reply, list)
        or len(reply) != 1
        or not isinstance(reply[0], dict)
        or _has_nonempty_err_msg(message)
    ):
        raise PushStreamError("SUBSCRIPTION_INVALID")
    item = reply[0]
    if (
        item.get("topic") != TOPIC
        or _has_nonempty_err_msg(item)
        or any(
            not isinstance(item.get(name), list)
            for name in ("scode", "fcode", "invalid", "denied")
        )
    ):
        raise PushStreamError("SUBSCRIPTION_INVALID")
    collections = {name: item[name] for name in ("scode", "fcode", "invalid", "denied")}
    if any(any(value != code for value in values) for values in collections.values()):
        raise PushStreamError("SUBSCRIPTION_INVALID")
    if collections == {"scode": [], "fcode": [], "invalid": [], "denied": [code]}:
        raise PushStreamError("SUBSCRIPTION_DENIED")
    if collections != {"scode": [code], "fcode": [], "invalid": [], "denied": []}:
        raise PushStreamError("SUBSCRIPTION_INVALID")


def _validate_heartbeat(message: dict[str, Any]) -> int:
    value = message.get("hbValue")
    if message.get("topic") != "hb" or _has_nonempty_err_msg(message) or type(value) is not int:
        raise PushStreamError("PROTOCOL_INVALID")
    return value


def _has_nonempty_err_msg(message: dict[str, Any]) -> bool:
    if "errMsg" not in message:
        return False
    value = message["errMsg"]
    return not (value is None or value == "" or value == {})


def _validate_quote_frame(message: dict[str, Any], *, code: str) -> int:
    items = message.get("items")
    if (
        message.get("topic") != TOPIC
        or _has_nonempty_err_msg(message)
        or not isinstance(items, list)
        or not items
    ):
        raise PushStreamError("QUOTE_INVALID")
    for item in items:
        if not isinstance(item, dict) or set(item) != {"code", "msg"} or item.get("code") != code:
            raise PushStreamError("QUOTE_INVALID")
        encoded = item.get("msg")
        if not isinstance(encoded, str):
            raise PushStreamError("QUOTE_INVALID")
        try:
            quote = _message(encoded.encode())
        except PushStreamError:
            raise PushStreamError("QUOTE_INVALID") from None
        if _has_nonempty_err_msg(quote) or not DYNAMIC_QUOTE_FIELDS.issubset(quote):
            raise PushStreamError("QUOTE_INVALID")
        if any(type(quote[field]) is not int for field in DYNAMIC_QUOTE_FIELDS):
            raise PushStreamError("QUOTE_INVALID")
        if not 0 <= quote["power"] <= MAX_POWER or quote["price"] <= 0:
            raise PushStreamError("QUOTE_INVALID")
    return len(items)


def _check_secret_reflection(
    raw: bytes, message: dict[str, Any], credentials: tuple[str, str]
) -> None:
    rendered = json.dumps(message, ensure_ascii=False)
    if any(value.encode() in raw or value in rendered for value in credentials):
        raise PushStreamError("SECRET_REFLECTION")


def _check_nested_secret_reflection(
    message: dict[str, Any], credentials: tuple[str, str]
) -> None:
    items = message.get("items")
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("msg"), str):
            continue
        try:
            nested = _message(item["msg"].encode())
        except PushStreamError:
            continue
        _check_secret_reflection(b"", nested, credentials)


def _write_private(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


async def collect_push_quote(
    *,
    code: str,
    directory: Path,
    api_key: str,
    session: str,
    duration: float = 60,
    max_frames: int = MAX_FRAMES,
    connector: Any = _FixedConnect,
    request_id_factory: Any = lambda: token_hex(8),
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    monotonic_clock: Callable[[], float] = monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Open one quote subscription, archive inbound originals, and never reconnect."""
    code = _validate_code(code)
    duration, max_frames = _validate_limits(duration, max_frames)
    heartbeat_interval_seconds = _validate_heartbeat_interval(heartbeat_interval_seconds)
    api_key, session = _validate_credential(api_key), _validate_credential(session)
    request_id = _validate_request_id(request_id_factory())
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    started = now()
    started_monotonic = monotonic_clock()
    deadline = started_monotonic + duration
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "source": "fosun",
        "transport": "websocket",
        "origin": PUSH_URL,
        "topic": TOPIC,
        "code": code,
        "request_id": request_id,
        "started_at": started.isoformat(),
        "subscription_confirmed": False,
        "status": "failed",
        "stop_reason": "CONNECT_FAILED",
        "frames": [],
        "requested_duration_seconds": duration,
        "requested_max_frames": max_frames,
        "heartbeat_interval_seconds": heartbeat_interval_seconds,
        "quote_message_count": 0,
        "heartbeat_sent_count": 0,
    }
    byte_count = 0
    logger = logging.Logger("nexus.fosun_push.private", level=logging.CRITICAL + 1)
    logger.propagate = False
    try:
        async with connector(
            PUSH_URL,
            additional_headers={"X-Api-Key": api_key, "X-Session": session},
            open_timeout=min(10, duration),
            close_timeout=CLOSE_TIMEOUT_SECONDS,
            max_size=MAX_FRAME,
            max_queue=1,
            compression=None,
            logger=logger,
        ) as socket:
            try:
                await socket.send(
                    json.dumps(
                        {
                            "act": "sub",
                            "items": [{"topic": TOPIC, "code": [code]}],
                            "requestid": request_id,
                        },
                        separators=(",", ":"),
                    )
                )
                receipt["stop_reason"] = "DISCONNECTED"
                next_heartbeat = started_monotonic + heartbeat_interval_seconds
                while len(receipt["frames"]) < max_frames:
                    current_monotonic = monotonic_clock()
                    remaining = deadline - current_monotonic
                    if remaining <= 0:
                        raise TimeoutError
                    until_heartbeat = next_heartbeat - current_monotonic
                    if until_heartbeat <= 0:
                        await socket.send(
                            json.dumps(
                                {"act": "hb", "hbValue": int(now().timestamp() * 1000)},
                                separators=(",", ":"),
                            )
                        )
                        receipt["heartbeat_sent_count"] += 1
                        next_heartbeat += heartbeat_interval_seconds
                        continue
                    try:
                        frame = await asyncio.wait_for(
                            socket.recv(), timeout=min(remaining, until_heartbeat)
                        )
                    except TimeoutError:
                        if monotonic_clock() >= deadline:
                            raise
                        continue
                    raw = frame.encode() if isinstance(frame, str) else frame
                    if not isinstance(raw, bytes):
                        raise PushStreamError("FRAME_INVALID")
                    if len(raw) > MAX_FRAME:
                        raise PushStreamError("FRAME_TOO_LARGE")
                    byte_count += len(raw)
                    if byte_count > MAX_BYTES:
                        raise PushStreamError("BYTE_LIMIT")
                    message = _message(raw)
                    _check_secret_reflection(raw, message, (api_key, session))
                    _check_nested_secret_reflection(message, (api_key, session))
                    received = now()
                    name = f"frame-{len(receipt['frames']):05}.json"
                    _write_private(directory / name, raw)
                    receipt["frames"].append(
                        {
                            "file": name,
                            "received_at": received.isoformat(),
                            "raw_sha256": sha256(raw).hexdigest(),
                            "raw_byte_length": len(raw),
                        }
                    )
                    if not receipt["subscription_confirmed"]:
                        if "subRep" not in message:
                            raise PushStreamError("PROTOCOL_INVALID")
                        _validate_subscription(message, code=code, request_id=request_id)
                        receipt["subscription_confirmed"] = True
                    elif message.get("topic") == "hb":
                        await socket.send(
                            json.dumps(
                                {"act": "hb", "hbValue": _validate_heartbeat(message)},
                                separators=(",", ":"),
                            )
                        )
                    elif message.get("topic") == TOPIC and "subRep" not in message:
                        receipt["quote_message_count"] += _validate_quote_frame(
                            message, code=code
                        )
                    else:
                        raise PushStreamError("PROTOCOL_INVALID")
                receipt["stop_reason"] = "FRAME_LIMIT"
            except TimeoutError:
                receipt["stop_reason"] = "DURATION_LIMIT"
            except asyncio.CancelledError:
                receipt["stop_reason"] = "CANCELLED"
                raise
            except PushStreamError as error:
                receipt["stop_reason"] = str(error)
            except Exception:
                pass
            finally:
                receipt["ended_at"] = now().isoformat()
                receipt["elapsed_monotonic_seconds"] = monotonic_clock() - started_monotonic
    except asyncio.CancelledError:
        receipt["stop_reason"] = "CANCELLED"
        if "ended_at" not in receipt:
            receipt["ended_at"] = now().isoformat()
            receipt["elapsed_monotonic_seconds"] = monotonic_clock() - started_monotonic
        receipt["frame_count"] = len(receipt["frames"])
        receipt["status"] = "partial" if receipt["subscription_confirmed"] else "failed"
        _write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
        raise
    except Exception:
        pass
    if "ended_at" not in receipt:
        receipt["ended_at"] = now().isoformat()
        receipt["elapsed_monotonic_seconds"] = monotonic_clock() - started_monotonic
    receipt["frame_count"] = len(receipt["frames"])
    receipt["status"] = (
        "bounded_complete"
        if receipt["subscription_confirmed"]
        and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if receipt["subscription_confirmed"]
        else "failed"
    )
    _write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    return {
        key: receipt[key]
        for key in ("status", "stop_reason", "subscription_confirmed", "frame_count")
    }


def _read_private(directory_fd: int, name: str, maximum: int) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o077
            or metadata.st_size > maximum
        ):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        content = os.read(descriptor, maximum + 1)
        if len(content) > maximum:
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        return content
    except OSError:
        raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
    finally:
        os.close(descriptor)


def _require_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    return result.astimezone(UTC)


def _validate_receipt(receipt: dict[str, Any], frames: list[bytes]) -> dict[str, Any]:
    expected_keys = {
        "schema",
        "source",
        "transport",
        "origin",
        "topic",
        "code",
        "request_id",
        "started_at",
        "ended_at",
        "requested_duration_seconds",
        "requested_max_frames",
        "heartbeat_interval_seconds",
        "elapsed_monotonic_seconds",
        "subscription_confirmed",
        "status",
        "stop_reason",
        "frame_count",
        "quote_message_count",
        "heartbeat_sent_count",
        "frames",
    }
    if set(receipt) != expected_keys:
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    code = _validate_code(receipt["code"])
    request_id = _validate_request_id(receipt["request_id"])
    duration, max_frames = _validate_limits(
        receipt["requested_duration_seconds"], receipt["requested_max_frames"]
    )
    try:
        heartbeat_interval = _validate_heartbeat_interval(receipt["heartbeat_interval_seconds"])
    except PushStreamError:
        raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
    elapsed = receipt["elapsed_monotonic_seconds"]
    start, end = _require_utc(receipt["started_at"]), _require_utc(receipt["ended_at"])
    entries = receipt["frames"]
    if (
        receipt["schema"] != SCHEMA
        or receipt["source"] != "fosun"
        or receipt["transport"] != "websocket"
        or receipt["origin"] != PUSH_URL
        or receipt["topic"] != TOPIC
        or type(elapsed) is not float
        or not 0 <= elapsed <= duration + CLOSE_TIMEOUT_SECONDS
        or start > end
        or end - start > timedelta(seconds=duration + CLOSE_TIMEOUT_SECONDS)
        or type(receipt["subscription_confirmed"]) is not bool
        or receipt["status"] not in {"bounded_complete", "partial", "failed"}
        or not isinstance(receipt["stop_reason"], str)
        or not isinstance(entries, list)
        or receipt["frame_count"] != len(frames)
        or type(receipt["quote_message_count"]) is not int
        or receipt["quote_message_count"] < 0
        or type(receipt["heartbeat_sent_count"]) is not int
        or receipt["heartbeat_sent_count"] < 0
        or len(frames) > max_frames
        or len(frames) > MAX_FRAMES
        or sum(map(len, frames)) > MAX_BYTES
    ):
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    subscribed = False
    quote_message_count = 0
    previous_received: datetime | None = None
    for index, (entry, raw) in enumerate(zip(entries, frames, strict=True)):
        if (
            not isinstance(entry, dict)
            or set(entry) != {"file", "received_at", "raw_sha256", "raw_byte_length"}
            or entry["file"] != f"frame-{index:05}.json"
            or entry["raw_sha256"] != sha256(raw).hexdigest()
            or type(entry["raw_byte_length"]) is not int
            or entry["raw_byte_length"] != len(raw)
        ):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        received = _require_utc(entry["received_at"])
        if (
            not start <= received <= end
            or previous_received is not None
            and received < previous_received
        ):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        previous_received = received
        message = _message(raw)
        if not subscribed:
            if "subRep" not in message:
                raise PushStreamError("PUSH_ARCHIVE_INVALID")
            _validate_subscription(message, code=code, request_id=request_id)
            subscribed = True
        elif message.get("topic") == "hb":
            try:
                _validate_heartbeat(message)
            except PushStreamError:
                raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
        elif message.get("topic") == TOPIC and "subRep" not in message:
            try:
                quote_message_count += _validate_quote_frame(message, code=code)
            except PushStreamError:
                raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
        else:
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
    expected_status = (
        "bounded_complete"
        if subscribed and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if subscribed
        else "failed"
    )
    if receipt["subscription_confirmed"] != subscribed or receipt["status"] != expected_status:
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    heartbeat_window = duration if receipt["stop_reason"] == "DURATION_LIMIT" else elapsed
    if (
        receipt["quote_message_count"] != quote_message_count
        or receipt["heartbeat_sent_count"] != int(heartbeat_window // heartbeat_interval)
    ):
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    if receipt["status"] == "bounded_complete" and (
        (
            receipt["stop_reason"] == "DURATION_LIMIT"
            and not duration <= elapsed <= duration + CLOSE_TIMEOUT_SECONDS
        )
        or (
            receipt["stop_reason"] == "FRAME_LIMIT" and receipt["frame_count"] != max_frames
        )
    ):
        raise PushStreamError("PUSH_ARCHIVE_INVALID")
    return {"status": receipt["status"], "receipt": receipt, "frames": tuple(frames)}


def load_push_archive(directory: Path) -> dict[str, Any]:
    """Check consistency only; cannot externally trust a colluding local archive rewrite."""
    try:
        directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
    try:
        metadata = os.fstat(directory_fd)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        receipt_raw = _read_private(directory_fd, "receipt.json", 256 * 1024)
        receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
        if (
            not isinstance(receipt, dict)
            or not isinstance(receipt.get("frames"), list)
            or type(receipt.get("requested_max_frames")) is not int
            or not 1 <= receipt["requested_max_frames"] <= MAX_FRAMES
            or len(receipt["frames"]) > receipt["requested_max_frames"]
        ):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        frames = []
        byte_count = 0
        for index in range(len(receipt["frames"])):
            frame = _read_private(directory_fd, f"frame-{index:05}.json", MAX_FRAME)
            byte_count += len(frame)
            if byte_count > MAX_BYTES:
                raise PushStreamError("PUSH_ARCHIVE_INVALID")
            frames.append(frame)
        expected_names = {
            "receipt.json",
            *(f"frame-{index:05}.json" for index in range(len(frames))),
        }
        if set(os.listdir(directory_fd)) != expected_names:
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
    except (OSError, TypeError, ValueError, PushStreamError):
        raise PushStreamError("PUSH_ARCHIVE_INVALID") from None
    finally:
        os.close(directory_fd)
    return {**_validate_receipt(receipt, frames), "receipt_raw": receipt_raw}
