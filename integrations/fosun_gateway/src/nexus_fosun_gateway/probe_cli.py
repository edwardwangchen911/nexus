"""One-shot, fail-closed CLI for the bounded Fosun read-only probe."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import ValidationError

from nexus_fosun_gateway.activation import (
    MAX_ARTIFACT_BYTES,
    _read_bounded_or_empty,
    evaluate_activation,
)
from nexus_fosun_gateway.adapter.probe_sdk import (
    PhysicalHttpBudget,
    ProbeSdkError,
    SdkProbeExecutor,
    build_probe_transport,
)
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.probe import (
    PreparedProbeArtifact,
    ProbePlan,
    run_sanitized_probe,
    validate_sanitized_probe_artifact,
)
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from nexus_fosun_gateway.publication import (
    load_probe_signing_key,
    publish_probe_artifact,
)

_ARGUMENT_NAMES = (
    "--manifest",
    "--symbol",
    "--session-date",
    "--cash-flow-date-from",
    "--cash-flow-date-to",
    "--limit",
    "--capability-ttl-seconds",
    "--output-basename",
)
_PROBE_ENVIRONMENT_NAMES = (
    "FOSUN_PROVIDER_MANIFEST",
    "FOSUN_SDK_ADMITTED",
    "FOSUN_PROBE_OUTPUT_DIR",
    "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE",
    "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U",
    "FSOPENAPI_BASE_URL",
    "FSOPENAPI_API_KEY",
    "FSOPENAPI_CLIENT_PRIVATE_KEY",
    "FSOPENAPI_SERVER_PUBLIC_KEY",
)


def failure_result(code: str) -> dict[str, object]:
    """Return a fixed, value-free failure frame with receipt physically last."""
    return {
        "status": "probe_failed",
        "code": code,
        "artifact_sha256": None,
        "byte_length": None,
        "capability_expires_at": None,
        "activation_contract_hash": None,
        "publication_receipt": None,
    }


def write_result_frame(
    result: dict[str, object],
    *,
    writer: Callable[[int, bytes | memoryview], int] = os.write,
) -> bool:
    """Write exactly one JSON frame; callers must never emit a recovery frame."""
    try:
        frame = json.dumps(result, separators=(",", ":")).encode("utf-8") + b"\n"
    except Exception:
        return False
    view = memoryview(frame)
    offset = 0
    while offset < len(view):
        try:
            written = writer(1, view[offset:])
        except InterruptedError:
            continue
        except OSError:
            return False
        if (
            not isinstance(written, int)
            or isinstance(written, bool)
            or not 1 <= written <= len(view) - offset
        ):
            return False
        offset += written
    return True


def main(argv: Sequence[str] | None = None) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    try:
        result = asyncio.run(_execute_once(arguments))
        exit_code = 0 if result["status"] == "probe_published" else 2
    except (KeyboardInterrupt, asyncio.CancelledError):
        result = failure_result("PROBE_INTERRUPTED")
        exit_code = 130
    except BaseException:
        result = failure_result("PROBE_FAILED")
        exit_code = 2
    if not write_result_frame(result):
        return 2
    return exit_code


async def _execute_once(arguments: tuple[str, ...]) -> dict[str, object]:
    prepared: PreparedProbeArtifact | None = None
    publication_complete = False
    try:
        manifest_path, plan = _parse_arguments(arguments)
        manifest_bytes = _read_bounded_or_empty(
            manifest_path,
            max_bytes=MAX_ARTIFACT_BYTES,
        )
        manifest_document = _load_manifest(manifest_bytes)
        if admitted_sdk_subject(manifest_document) is None:
            raise ProbeSdkError("COMPLETE_SDK_ADMISSION")

        settings = ProbeProviderSettings.model_validate(
            {
                name: os.environ[name]
                for name in _PROBE_ENVIRONMENT_NAMES
                if name in os.environ
            }
        )
        if settings.provider_manifest != manifest_path:
            raise ProbeSdkError("PROBE_SETTINGS_INVALID")
        started_at = datetime.now(UTC)
        plan.validate_for_start(started_at)
        budget = PhysicalHttpBudget()
        bundle = build_probe_transport(manifest_document, settings, budget)
        executor = SdkProbeExecutor(bundle, budget)
        prepared = await run_sanitized_probe(
            manifest_bytes,
            plan,
            executor,
            started_at=started_at,
            private_key_factory=lambda: load_probe_signing_key(
                settings.publisher_private_key_file,
                settings.publisher_public_key_b64u,
            ),
            utc_clock=lambda: datetime.now(UTC),
        )
        validate_sanitized_probe_artifact(manifest_bytes, prepared.artifact_bytes)
        publication = publish_probe_artifact(
            settings.output_dir,
            plan.output_basename,
            prepared.artifact_bytes,
            expected_sha256=prepared.artifact_sha256,
            expected_byte_length=prepared.byte_length,
        )
        publication_complete = True
        receipt_bytes = prepared.sign_publication_receipt(publication)
        activation = evaluate_activation(
            manifest_bytes,
            prepared.artifact_bytes,
            receipt_bytes,
            trusted_publication_public_key_bytes=(
                settings.publisher_public_key_b64u.encode("ascii")
            ),
            checked_at=datetime.now(UTC),
        )
        if activation.status != "activated_read_only":
            raise ProbeSdkError("PROBE_OUTPUT_INVALID")
        receipt = json.loads(receipt_bytes)
        return {
            "status": "probe_published",
            "code": "OK",
            "artifact_sha256": prepared.artifact_sha256,
            "byte_length": prepared.byte_length,
            "capability_expires_at": prepared.capability_expires_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "activation_contract_hash": activation.contract_hash,
            "publication_receipt": receipt,
        }
    except (KeyboardInterrupt, asyncio.CancelledError):
        raise
    except ValidationError:
        return failure_result("PROBE_SETTINGS_INVALID")
    except ProbeSdkError as error:
        if publication_complete and error.code != "PROBE_OUTPUT_QUARANTINE_REQUIRED":
            return failure_result("PROBE_OUTPUT_QUARANTINE_REQUIRED")
        return failure_result(error.code)
    except Exception:
        return failure_result(
            "PROBE_OUTPUT_QUARANTINE_REQUIRED"
            if publication_complete
            else "PROBE_FAILED"
        )
    finally:
        if prepared is not None:
            prepared.dispose_signing_key()


def _parse_arguments(arguments: tuple[str, ...]) -> tuple[Path, ProbePlan]:
    if len(arguments) != len(_ARGUMENT_NAMES) * 2:
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    values: dict[str, str] = {}
    for index in range(0, len(arguments), 2):
        name = arguments[index]
        value = arguments[index + 1]
        if name not in _ARGUMENT_NAMES or name in values:
            raise ProbeSdkError("INVALID_PROBE_PLAN")
        values[name] = value
    if set(values) != set(_ARGUMENT_NAMES):
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    try:
        manifest_path = Path(values["--manifest"])
        if not manifest_path.is_absolute():
            raise ValueError
        plan = ProbePlan(
            symbol=values["--symbol"],
            session_date=_exact_date(values["--session-date"]),
            cash_flow_date_from=_exact_date(values["--cash-flow-date-from"]),
            cash_flow_date_to=_exact_date(values["--cash-flow-date-to"]),
            limit=_exact_integer(values["--limit"]),
            capability_ttl_seconds=_exact_integer(
                values["--capability-ttl-seconds"]
            ),
            output_basename=values["--output-basename"],
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProbeSdkError("INVALID_PROBE_PLAN") from None
    return manifest_path, plan


def _exact_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError
    return parsed


def _exact_integer(value: str) -> int:
    parsed = int(value)
    if str(parsed) != value:
        raise ValueError
    return parsed


def _load_manifest(manifest_bytes: bytes) -> object:
    try:
        return json.loads(manifest_bytes)
    except (UnicodeError, json.JSONDecodeError):
        return None


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
