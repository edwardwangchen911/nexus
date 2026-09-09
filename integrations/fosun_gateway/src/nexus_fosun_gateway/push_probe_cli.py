"""One-shot, value-free CLI for a bounded Fosun quote push probe."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import stat
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from nexus_fosun_gateway.activation import MAX_ARTIFACT_BYTES, _read_bounded_or_empty
from nexus_fosun_gateway.adapter.probe_sdk import PhysicalHttpBudget, build_probe_transport
from nexus_fosun_gateway.admission import push_probe_ready_sdk_subject
from nexus_fosun_gateway.probe_cli import write_result_frame
from nexus_fosun_gateway.probe_settings import PushProbeProviderSettings
from nexus_fosun_gateway.push_stream import (
    LOCAL_PROBE_PROXY_URL,
    PushStreamError,
    _validate_code,
    _validate_limits,
    collect_push_quote,
    load_latest_push_quote,
    load_push_archive,
)
from nexus_fosun_gateway.security.provider_environment import (
    UnsafeProviderEnvironmentError,
    reject_unsafe_provider_environment,
)

_ARGUMENT_NAMES = {"--manifest", "--code", "--duration", "--max-frames", "--output-dir"}
_OPTIONAL_ARGUMENT_NAMES = {"--authority-sha256", "--normalized-output"}
_ENVIRONMENT_NAMES = (
    "FOSUN_PROVIDER_MANIFEST",
    "FOSUN_SDK_ADMITTED",
    "FOSUN_PROBE_HTTPS_PROXY_URL",
    "FSOPENAPI_BASE_URL",
    "FSOPENAPI_API_KEY",
    "FSOPENAPI_CLIENT_PRIVATE_KEY",
    "FSOPENAPI_SERVER_PUBLIC_KEY",
)
_SAFE_STOP_REASONS = {
    "DURATION_LIMIT",
    "FRAME_LIMIT",
    "DISCONNECTED",
    "SUBSCRIPTION_DENIED",
    "SUBSCRIPTION_INVALID",
    "PROTOCOL_INVALID",
    "FRAME_INVALID",
    "FRAME_TOO_LARGE",
    "BYTE_LIMIT",
    "CANCELLED",
    "CONNECT_FAILED",
}


def failure_result(code: str, physical_request_count: int = 0) -> dict[str, object]:
    count = (
        physical_request_count
        if type(physical_request_count) is int and 0 <= physical_request_count <= 1
        else 0
    )
    safe_code = (
        code
        if code in {"PUSH_PROBE_FAILED", "PUSH_PROBE_SETTINGS_INVALID"}
        else "PUSH_PROBE_FAILED"
    )
    return {
        "status": "failed",
        "code": safe_code,
        "stop_reason": None,
        "subscription_confirmed": False,
        "frame_count": 0,
        "physical_request_count": count,
    }


def main(argv: Sequence[str] | None = None, *, output_fd: int = 1) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    try:
        result = asyncio.run(_execute_once(arguments))
        exit_code = 0 if result.get("status") == "bounded_complete" else 2
    except (KeyboardInterrupt, asyncio.CancelledError):
        result = failure_result("PUSH_PROBE_FAILED")
        exit_code = 130
    except BaseException:
        result = failure_result("PUSH_PROBE_FAILED")
        exit_code = 2
    try:
        delivered = write_result_frame(result, file_descriptor=output_fd)
    except BaseException:
        return 2
    return exit_code if delivered else 2


async def _execute_once(
    arguments: tuple[str, ...],
    *,
    build_transport: Callable[..., Any] = build_probe_transport,
    collect: Callable[..., Any] = collect_push_quote,
    load: Callable[[Path], dict[str, Any]] = load_push_archive,
    load_quote: Callable[[Path], Any] = load_latest_push_quote,
) -> dict[str, object]:
    bundle: Any = None
    budget: PhysicalHttpBudget | None = None
    result: dict[str, object]
    try:
        reject_unsafe_provider_environment()
        plan = _parse_arguments(arguments)
        manifest_bytes = _read_bounded_or_empty(plan["manifest"], max_bytes=MAX_ARTIFACT_BYTES)
        manifest = _load_manifest(manifest_bytes)
        if push_probe_ready_sdk_subject(manifest) is None:
            raise ValueError
        environment_values = {
            name: os.environ[name] for name in _ENVIRONMENT_NAMES if name in os.environ
        }
        if environment_values.get("FOSUN_PROVIDER_MANIFEST") != str(plan["manifest"]):
            raise ValueError
        settings = PushProbeProviderSettings.model_validate(environment_values)
        if settings.provider_manifest != plan["manifest"]:
            raise ValueError
        budget = PhysicalHttpBudget(scope="core")
        bundle = build_transport(manifest, settings, budget)
        session = await bundle.create_push_session()
        collected = await collect(
            code=plan["code"],
            directory=plan["output_dir"],
            api_key=settings.fsopenapi_api_key.get_secret_value(),
            session=session,
            duration=plan["duration"],
            max_frames=plan["max_frames"],
            proxy_url=LOCAL_PROBE_PROXY_URL,
            authority_sha256=plan["authority_sha256"],
        )
        archive = load(plan["output_dir"])
        if archive.get("status") != collected.get("status"):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        status = collected.get("status")
        stop_reason = collected.get("stop_reason")
        confirmed = collected.get("subscription_confirmed")
        frame_count = collected.get("frame_count")
        if (
            status not in {"bounded_complete", "partial", "failed"}
            or stop_reason not in _SAFE_STOP_REASONS
            or type(confirmed) is not bool
            or type(frame_count) is not int
            or not 0 <= frame_count <= plan["max_frames"]
        ):
            raise PushStreamError("PUSH_ARCHIVE_INVALID")
        normalized_output = plan["normalized_output"]
        if status == "bounded_complete" and normalized_output is not None:
            quote = load_quote(plan["output_dir"])
            _write_normalized_quote(
                normalized_output,
                quote,
                archive_started_at=archive["receipt"]["started_at"],
            )
        result = {
            "status": status,
            "code": "OK" if status == "bounded_complete" else "PUSH_PROBE_INCOMPLETE",
            "stop_reason": stop_reason,
            "subscription_confirmed": confirmed,
            "frame_count": frame_count,
            "physical_request_count": budget.snapshot().total,
        }
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except PushStreamError:
        result = failure_result("PUSH_PROBE_FAILED", _request_count(budget))
    except (UnsafeProviderEnvironmentError, ValidationError, OSError, ValueError):
        result = failure_result("PUSH_PROBE_SETTINGS_INVALID", _request_count(budget))
    except Exception:
        result = failure_result("PUSH_PROBE_FAILED", _request_count(budget))
    finally:
        if bundle is not None:
            try:
                await bundle.aclose()
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except Exception:
                result = failure_result("PUSH_PROBE_FAILED", _request_count(budget))
    return result


def _parse_arguments(arguments: tuple[str, ...]) -> dict[str, Any]:
    if len(arguments) % 2 or not len(_ARGUMENT_NAMES) * 2 <= len(arguments) <= (
        len(_ARGUMENT_NAMES | _OPTIONAL_ARGUMENT_NAMES) * 2
    ):
        raise ValueError
    values: dict[str, str] = {}
    for index in range(0, len(arguments), 2):
        name, value = arguments[index], arguments[index + 1]
        if name not in _ARGUMENT_NAMES | _OPTIONAL_ARGUMENT_NAMES or name in values:
            raise ValueError
        values[name] = value
    names = set(values)
    if not _ARGUMENT_NAMES <= names:
        raise ValueError
    manifest = _absolute_path(values["--manifest"])
    output_dir = _absolute_path(values["--output-dir"])
    if output_dir.exists() or not output_dir.parent.is_dir():
        raise ValueError
    parent = output_dir.parent.stat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077:
        raise ValueError
    duration = _exact_integer(values["--duration"])
    max_frames = _exact_integer(values["--max-frames"])
    _validate_limits(duration, max_frames)
    if duration > 60:
        raise ValueError
    authority_sha256 = values.get("--authority-sha256")
    if authority_sha256 is not None and (
        len(authority_sha256) != 64
        or any(character not in "0123456789abcdef" for character in authority_sha256)
    ):
        raise ValueError
    normalized_output_value = values.get("--normalized-output")
    normalized_output = (
        None if normalized_output_value is None else _absolute_path(normalized_output_value)
    )
    if normalized_output is not None:
        if authority_sha256 is None or normalized_output.exists():
            raise ValueError
        normalized_parent = normalized_output.parent.stat()
        if not stat.S_ISDIR(normalized_parent.st_mode) or normalized_parent.st_mode & 0o077:
            raise ValueError
    return {
        "manifest": manifest,
        "code": _validate_code(values["--code"]),
        "duration": float(duration),
        "max_frames": max_frames,
        "output_dir": output_dir,
        "authority_sha256": authority_sha256,
        "normalized_output": normalized_output,
    }


def _write_normalized_quote(path: Path, quote: Any, *, archive_started_at: str) -> None:
    raw_archive = quote.raw_archive_payload
    document = {
        "schema": "nexus_fosun_push_quote_handoff_v1",
        "archive_started_at": archive_started_at,
        "code": quote.code,
        "price": str(quote.price),
        "provider_quote_date": quote.provider_quote_date,
        "provider_quote_time": quote.provider_quote_time,
        "frame_received_at": quote.frame_received_at.isoformat().replace("+00:00", "Z"),
        "archive_completed_at": quote.archive_completed_at.isoformat().replace("+00:00", "Z"),
        "authority_sha256": quote.authority_sha256,
        "raw_archive_sha256": hashlib.sha256(raw_archive).hexdigest(),
        "raw_archive_base64": base64.b64encode(raw_archive).decode("ascii"),
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())


def _absolute_path(value: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value or len(value.encode()) > 4096:
        raise ValueError
    path = Path(value)
    if not path.is_absolute():
        raise ValueError
    return path


def _exact_integer(value: str) -> int:
    parsed = int(value)
    if str(parsed) != value:
        raise ValueError
    return parsed


def _load_manifest(raw: bytes) -> object:
    try:
        return json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        return None


def _request_count(budget: PhysicalHttpBudget | None) -> int:
    return 0 if budget is None else budget.snapshot().total


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
