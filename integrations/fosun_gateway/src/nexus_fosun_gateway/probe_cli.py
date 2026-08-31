"""One-shot, fail-closed CLI for the bounded Fosun read-only probe."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

from pydantic import ValidationError

from nexus_fosun_gateway.activation import (
    MAX_ARTIFACT_BYTES,
    _read_bounded_or_empty,
    verify_probe_publication,
)
from nexus_fosun_gateway.adapter.probe_sdk import (
    PhysicalHttpBudget,
    ProbeErrorCode,
    ProbeSdkError,
    SdkProbeExecutor,
    build_probe_transport,
)
from nexus_fosun_gateway.admission import probe_ready_sdk_subject
from nexus_fosun_gateway.probe import (
    PreparedProbeArtifact,
    ProbePlan,
    run_sanitized_probe,
    validate_sanitized_probe_artifact,
)
from nexus_fosun_gateway.probe_contract import (
    PROBE_CLI_ARGUMENT_NAMES,
    PROBE_FAILURE_CATEGORIES,
    PROBE_FAILURE_OPERATIONS,
)
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from nexus_fosun_gateway.publication import (
    load_probe_signing_key,
    publish_probe_artifact,
)
from nexus_fosun_gateway.security.provider_environment import (
    UnsafeProviderEnvironmentError,
    reject_unsafe_provider_environment,
)

_ARGUMENT_NAMES = PROBE_CLI_ARGUMENT_NAMES
_PROBE_ENVIRONMENT_NAMES = (
    "FOSUN_PROVIDER_MANIFEST",
    "FOSUN_SDK_ADMITTED",
    "FOSUN_PROBE_OUTPUT_DIR",
    "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE",
    "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U",
    "FOSUN_PROBE_HTTPS_PROXY_URL",
    "FSOPENAPI_BASE_URL",
    "FSOPENAPI_API_KEY",
    "FSOPENAPI_CLIENT_PRIVATE_KEY",
    "FSOPENAPI_SERVER_PUBLIC_KEY",
)
_MAX_MANIFEST_PATH_BYTES = 4096


def failure_result(error: str | ProbeSdkError) -> dict[str, object]:
    """Return a fixed, value-free failure frame with receipt physically last."""
    code: str
    if isinstance(error, ProbeSdkError):
        code = error.code
        operation = (
            error.operation
            if error.operation in PROBE_FAILURE_OPERATIONS
            else "unknown"
        )
        category = (
            error.category
            if error.category in PROBE_FAILURE_CATEGORIES
            else "unknown"
        )
        physical_request_count = (
            error.physical_request_count
            if isinstance(error.physical_request_count, int)
            and not isinstance(error.physical_request_count, bool)
            and 0 <= error.physical_request_count <= 24
            else 0
        )
    else:
        code = cast(ProbeErrorCode, error)
        operation = "unknown"
        category = "unknown"
        physical_request_count = 0
    return {
        "status": "probe_failed",
        "code": code,
        "failure_stage": "probe",
        "failure_operation": operation,
        "failure_category": category,
        "physical_request_count": physical_request_count,
        "artifact_sha256": None,
        "byte_length": None,
        "capability_expires_at": None,
        "activation_contract_hash": None,
        "publication_receipt": None,
    }


def write_result_frame(
    result: dict[str, object],
    *,
    file_descriptor: int = 1,
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
            written = writer(file_descriptor, view[offset:])
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


def main(argv: Sequence[str] | None = None, *, output_fd: int = 1) -> int:
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
    try:
        delivered = write_result_frame(result, file_descriptor=output_fd)
    except BaseException:
        return 2
    if not delivered:
        return 2
    return exit_code


async def _execute_once(arguments: tuple[str, ...]) -> dict[str, object]:
    prepared: PreparedProbeArtifact | None = None
    budget: PhysicalHttpBudget | None = None
    publication_complete = False
    publication_path: Path | None = None
    try:
        reject_unsafe_provider_environment()
        manifest_spelling, manifest_path = _parse_manifest_argument(arguments)
        manifest_bytes = _read_bounded_or_empty(
            manifest_path,
            max_bytes=MAX_ARTIFACT_BYTES,
        )
        manifest_document = _load_manifest(manifest_bytes)
        if probe_ready_sdk_subject(manifest_document) is None:
            raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
        plan = _parse_probe_plan(arguments, manifest_spelling)

        environment_values = {
            name: os.environ[name]
            for name in _PROBE_ENVIRONMENT_NAMES
            if name in os.environ
        }
        if environment_values.get("FOSUN_PROVIDER_MANIFEST") != manifest_spelling:
            raise ProbeSdkError("PROBE_SETTINGS_INVALID")
        settings = ProbeProviderSettings.model_validate(environment_values)
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
        publication_path = settings.output_dir / plan.output_basename
        publication = publish_probe_artifact(
            settings.output_dir,
            plan.output_basename,
            prepared.artifact_bytes,
            expected_sha256=prepared.artifact_sha256,
            expected_byte_length=prepared.byte_length,
        )
        publication_complete = True
        receipt_bytes = prepared.sign_publication_receipt(publication)
        verification_hash = verify_probe_publication(
            manifest_bytes,
            prepared.artifact_bytes,
            receipt_bytes,
            trusted_publication_public_key_bytes=(
                settings.publisher_public_key_b64u.encode("ascii")
            ),
            checked_at=datetime.now(UTC),
        )
        if verification_hash is None:
            raise ProbeSdkError("PROBE_OUTPUT_INVALID")
        receipt = json.loads(receipt_bytes)
        return {
            "status": "probe_published",
            "code": "OK",
            "failure_stage": None,
            "failure_operation": None,
            "failure_category": None,
            "physical_request_count": None,
            "artifact_sha256": prepared.artifact_sha256,
            "byte_length": prepared.byte_length,
            "capability_expires_at": prepared.capability_expires_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "activation_contract_hash": verification_hash,
            "publication_receipt": receipt,
        }
    except (KeyboardInterrupt, asyncio.CancelledError):
        if publication_complete or (
            publication_path is not None
            and prepared is not None
            and _read_bounded_or_empty(
                publication_path,
                max_bytes=MAX_ARTIFACT_BYTES,
            )
            == prepared.artifact_bytes
        ):
            return failure_result(_with_budget("PROBE_OUTPUT_QUARANTINE_REQUIRED", budget))
        raise
    except (UnsafeProviderEnvironmentError, ValidationError):
        return failure_result(_with_budget("PROBE_SETTINGS_INVALID", budget))
    except ProbeSdkError as error:
        if publication_complete and error.code != "PROBE_OUTPUT_QUARANTINE_REQUIRED":
            return failure_result(_with_budget("PROBE_OUTPUT_QUARANTINE_REQUIRED", budget))
        return failure_result(_with_budget(error, budget))
    except Exception:
        return failure_result(
            _with_budget(
                "PROBE_OUTPUT_QUARANTINE_REQUIRED"
                if publication_complete
                else "PROBE_FAILED",
                budget,
            )
        )
    finally:
        if prepared is not None:
            prepared.dispose_signing_key()


def _with_budget(
    error: str | ProbeSdkError,
    budget: PhysicalHttpBudget | None,
) -> ProbeSdkError:
    """Preserve only the already-dispatched physical facts across local failures."""
    if isinstance(error, ProbeSdkError):
        code = error.code
        category = error.category
    else:
        code = cast(ProbeErrorCode, error)
        category = "unknown"
    if budget is None:
        return ProbeSdkError(code, category=category)
    snapshot = budget.snapshot()
    return ProbeSdkError(
        code,
        operation=budget.last_dispatched_label() or "unknown",
        category=category,
        physical_request_count=snapshot.total,
    )


def _parse_manifest_argument(arguments: tuple[str, ...]) -> tuple[str, Path]:
    if not arguments or len(arguments) % 2 != 0:
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    manifest_values: list[str] = []
    for index in range(0, len(arguments), 2):
        if arguments[index] == "--manifest":
            manifest_values.append(arguments[index + 1])
    if len(manifest_values) != 1:
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    try:
        manifest_spelling = manifest_values[0]
        if (
            not manifest_spelling
            or "\x00" in manifest_spelling
            or len(manifest_spelling.encode("utf-8")) > _MAX_MANIFEST_PATH_BYTES
        ):
            raise ValueError
        manifest_path = Path(manifest_spelling)
        if not manifest_path.is_absolute():
            raise ValueError
    except (TypeError, UnicodeError, ValueError):
        raise ProbeSdkError("INVALID_PROBE_PLAN") from None
    return manifest_spelling, manifest_path


def _parse_probe_plan(arguments: tuple[str, ...], manifest_spelling: str) -> ProbePlan:
    if len(arguments) != len(_ARGUMENT_NAMES) * 2:
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    values: dict[str, str] = {}
    for index in range(0, len(arguments), 2):
        name = arguments[index]
        value = arguments[index + 1]
        if name not in _ARGUMENT_NAMES or name in values:
            raise ProbeSdkError("INVALID_PROBE_PLAN")
        values[name] = value
    if set(values) != set(_ARGUMENT_NAMES) or values["--manifest"] != manifest_spelling:
        raise ProbeSdkError("INVALID_PROBE_PLAN")
    try:
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
    return plan


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
