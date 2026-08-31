"""Transport-free Fosun read-only activation decision and sanitized contract."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    model_validator,
)

from nexus_fosun_gateway.admission import sdk_admission_document_is_valid
from nexus_fosun_gateway.probe_contract import (
    BROKER_PROBE_OPERATIONS,
    MAX_BROKER_REQUESTS,
    REQUIRED_PROBE_OPERATIONS,
    probe_response_contract_sha256,
)

RESULT_SCHEMA_VERSION = "nexus.fosun.activation-result.v1"
PROBE_SCHEMA_VERSION = "nexus.fosun.sanitized-probe.v2"
PUBLICATION_RECEIPT_SCHEMA_VERSION = "nexus.fosun.probe-publication.v1"
PUBLICATION_SIGNING_DOMAIN = b"NEXUS-FOSUN-PROBE-PUBLICATION-V1\0"
MAX_REQUESTS_PER_OPERATION = 3
MAX_PROBE_REQUESTS = 24
MAX_CAPABILITY_TTL = timedelta(hours=24)
MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_PUBLICATION_RECEIPT_BYTES = 16 * 1024
MAX_TRUSTED_PUBLISHER_KEY_BYTES = 43
_Hex40 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_Hex64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_Base64Url32 = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{43}$")]
_Base64Url64 = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{86}$")]
_OperationName = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,63}$"),
]
_FORBIDDEN_OPERATION_MARKERS = (
    "placeorder",
    "createorder",
    "submitorder",
    "cancelorder",
    "replaceorder",
    "simulation",
    "simulate",
    "subscription",
    "webhook",
    "trade",
)


class FosunActivationResult(BaseModel):
    """Only the two externally meaningful activation outcomes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["activated_read_only", "fixture_with_next_action"]
    contract_hash: _Hex64
    checked_at: datetime
    capability_expires_at: datetime | None
    next_action: str | None

    @model_validator(mode="after")
    def _validate_result(self) -> FosunActivationResult:
        _require_aware(self.checked_at, "checked_at")
        if self.capability_expires_at is not None:
            _require_aware(self.capability_expires_at, "capability_expires_at")
        if self.status == "activated_read_only":
            if self.next_action is not None or self.capability_expires_at is None:
                raise ValueError("activated result requires an expiry and no next action")
        elif (
            not self.next_action
            or self.capability_expires_at is not None
            and (self.capability_expires_at > self.checked_at)
        ):
            raise ValueError("fixture result requires one next action and no active capability")
        return self


class ProbeOperationEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: _OperationName
    request_count: int = Field(ge=1, le=MAX_PROBE_REQUESTS)
    passed: StrictBool
    response_contract_sha256: _Hex64


class ProbeChecks(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    permission_read_only: StrictBool
    history_window: StrictBool
    adjustment_semantics: StrictBool
    pagination: StrictBool
    rate_limit: StrictBool
    response_contract: StrictBool

    def all_passed(self) -> bool:
        return all(
            (
                self.permission_read_only,
                self.history_window,
                self.adjustment_semantics,
                self.pagination,
                self.rate_limit,
                self.response_contract,
            )
        )


class SanitizedProbeArtifact(BaseModel):
    """Metadata-only proof; it has no fields for account or response data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["nexus.fosun.sanitized-probe.v2"]
    provider: Literal["fosun"]
    captured_at: datetime
    capability_expires_at: datetime
    manifest_sha256: _Hex64
    sdk_tag: Annotated[str, StringConstraints(pattern=r"^v[0-9]+\.[0-9]+\.[0-9]+$")]
    sdk_commit: _Hex40
    publication_public_key_b64u: _Base64Url32
    operations: tuple[ProbeOperationEvidence, ...] = Field(min_length=1, max_length=32)
    checks: ProbeChecks

    @model_validator(mode="after")
    def _validate_window(self) -> SanitizedProbeArtifact:
        _require_aware(self.captured_at, "captured_at")
        _require_aware(self.capability_expires_at, "capability_expires_at")
        ttl = self.capability_expires_at - self.captured_at
        if ttl <= timedelta(0) or ttl > MAX_CAPABILITY_TTL:
            raise ValueError("capability window must be positive and no longer than 24 hours")
        _decode_base64url(self.publication_public_key_b64u, expected_bytes=32)
        return self


class ProbePublicationReceipt(BaseModel):
    """Detached proof produced only after durable probe publication."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["nexus.fosun.probe-publication.v1"]
    artifact_sha256: _Hex64
    signature_b64u: _Base64Url64

    @model_validator(mode="after")
    def _validate_signature_encoding(self) -> ProbePublicationReceipt:
        _decode_base64url(self.signature_b64u, expected_bytes=64)
        return self


def evaluate_activation(
    manifest_bytes: bytes,
    probe_bytes: bytes | None = None,
    publication_receipt_bytes: bytes | None = None,
    *,
    trusted_publication_public_key_bytes: bytes | None = None,
    checked_at: datetime | None = None,
) -> FosunActivationResult:
    """Evaluate only supplied metadata; this function has no provider transport."""
    now = checked_at or datetime.now(UTC)
    _require_aware(now, "checked_at")
    manifest_sha256 = _sha256(manifest_bytes)
    manifest_document = _load_mapping(manifest_bytes)
    sdk_tag, sdk_commit = _sdk_subject(manifest_document)
    probe_sha256 = _sha256(probe_bytes) if probe_bytes is not None else None
    publication_receipt_sha256 = (
        _sha256(publication_receipt_bytes)
        if publication_receipt_bytes is not None
        else None
    )
    trusted_publication_public_key_sha256 = (
        _sha256(trusted_publication_public_key_bytes)
        if trusted_publication_public_key_bytes is not None
        else None
    )
    contract_hash = _contract_hash(
        manifest_sha256=manifest_sha256,
        sdk_tag=sdk_tag,
        sdk_commit=sdk_commit,
        probe_sha256=probe_sha256,
        publication_receipt_sha256=publication_receipt_sha256,
        trusted_publication_public_key_sha256=(
            trusted_publication_public_key_sha256
        ),
    )

    if manifest_document is None:
        return _blocked(contract_hash, now, "REVIEW_PROVIDER_MANIFEST")
    if not sdk_admission_document_is_valid(manifest_document):
        return _blocked(contract_hash, now, "COMPLETE_SDK_ADMISSION")
    if trusted_publication_public_key_bytes is None:
        return _blocked(contract_hash, now, "PROVIDE_TRUSTED_PROBE_PUBLISHER")
    trusted_publisher = _load_trusted_publication_key(
        trusted_publication_public_key_bytes
    )
    if trusted_publisher is None:
        return _blocked(contract_hash, now, "REVIEW_TRUSTED_PROBE_PUBLISHER")
    trusted_public_key_b64u, trusted_public_key = trusted_publisher
    if probe_bytes is None:
        return _blocked(contract_hash, now, "SCHEDULE_GUARDED_READ_ONLY_PROBE")

    try:
        probe = SanitizedProbeArtifact.model_validate_json(probe_bytes)
    except (ValidationError, ValueError):
        return _blocked(contract_hash, now, "REVIEW_PROBE_ARTIFACT")
    if not hmac.compare_digest(
        _canonical_json(probe.model_dump(mode="json")),
        probe_bytes,
    ):
        return _blocked(contract_hash, now, "REVIEW_PROBE_ARTIFACT")

    if (
        probe.manifest_sha256 != manifest_sha256
        or probe.sdk_tag != sdk_tag
        or probe.sdk_commit != sdk_commit
    ):
        return _blocked(contract_hash, now, "REVIEW_PROBE_SUBJECT")
    if not hmac.compare_digest(
        probe.publication_public_key_b64u,
        trusted_public_key_b64u,
    ):
        return _blocked(contract_hash, now, "REVIEW_PROBE_PUBLISHER_BINDING")
    if probe.capability_expires_at <= now:
        return _blocked(
            contract_hash,
            now,
            "REFRESH_GUARDED_READ_ONLY_PROBE",
            expired_at=probe.capability_expires_at,
        )
    if probe.captured_at > now:
        return _blocked(contract_hash, now, "REVIEW_PROBE_EVIDENCE")

    operation_names = tuple(operation.name for operation in probe.operations)
    if any(_is_forbidden_operation(name) for name in operation_names):
        return _blocked(contract_hash, now, "REMOVE_FORBIDDEN_PROBE_OPERATION")
    if len(operation_names) != len(set(operation_names)) or set(operation_names) != set(
        REQUIRED_PROBE_OPERATIONS
    ):
        return _blocked(contract_hash, now, "FIX_PROBE_OPERATION_SET")
    if operation_names != REQUIRED_PROBE_OPERATIONS:
        return _blocked(contract_hash, now, "FIX_PROBE_OPERATION_SEQUENCE")
    if any(
        (expected := probe_response_contract_sha256(operation.name)) is None
        or not hmac.compare_digest(operation.response_contract_sha256, expected)
        for operation in probe.operations
    ):
        return _blocked(contract_hash, now, "REVIEW_PROBE_RESPONSE_CONTRACT")
    if (
        any(operation.request_count > MAX_REQUESTS_PER_OPERATION for operation in probe.operations)
        or sum(operation.request_count for operation in probe.operations) > MAX_PROBE_REQUESTS
        or sum(
            operation.request_count
            for operation in probe.operations
            if operation.name in BROKER_PROBE_OPERATIONS
        )
        > MAX_BROKER_REQUESTS
    ):
        return _blocked(contract_hash, now, "REDUCE_PROBE_REQUEST_COUNT")
    if not probe.checks.all_passed() or not all(operation.passed for operation in probe.operations):
        return _blocked(contract_hash, now, "REVIEW_PROBE_EVIDENCE")
    if publication_receipt_bytes is None:
        return _blocked(contract_hash, now, "PROVIDE_PROBE_PUBLICATION_RECEIPT")
    if not _publication_receipt_is_valid(
        probe_bytes,
        publication_receipt_bytes,
        trusted_public_key,
    ):
        return _blocked(contract_hash, now, "REVIEW_PROBE_PUBLICATION_RECEIPT")

    return FosunActivationResult(
        status="activated_read_only",
        contract_hash=contract_hash,
        checked_at=now,
        capability_expires_at=probe.capability_expires_at,
        next_action=None,
    )


def evaluate_activation_paths(
    manifest_path: Path,
    probe_path: Path | None = None,
    publication_receipt_path: Path | None = None,
    *,
    trusted_publication_public_key_bytes: bytes | None = None,
    checked_at: datetime | None = None,
) -> FosunActivationResult:
    """Read bounded local artifacts and evaluate them without provider or credential access."""
    return evaluate_activation(
        _read_bounded_or_empty(manifest_path, max_bytes=MAX_ARTIFACT_BYTES),
        (
            _read_bounded_or_empty(probe_path, max_bytes=MAX_ARTIFACT_BYTES)
            if probe_path is not None
            else None
        ),
        (
            _read_bounded_or_empty(
                publication_receipt_path,
                max_bytes=MAX_PUBLICATION_RECEIPT_BYTES,
            )
            if publication_receipt_path is not None
            else None
        ),
        trusted_publication_public_key_bytes=(
            trusted_publication_public_key_bytes
        ),
        checked_at=checked_at,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate Fosun activation from local metadata without provider transport."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("contracts/providers/fosun/manifest.json"),
    )
    parser.add_argument("--probe-artifact", type=Path)
    parser.add_argument("--publication-receipt", type=Path)
    parser.add_argument(
        "--trusted-publisher-public-key-b64u",
        type=_publisher_public_key_argument,
    )
    parser.add_argument("--require-activated", action="store_true")
    args = parser.parse_args(argv)

    result = evaluate_activation_paths(
        args.manifest,
        args.probe_artifact,
        args.publication_receipt,
        trusted_publication_public_key_bytes=(
            args.trusted_publisher_public_key_b64u
        ),
    )
    print(result.model_dump_json())
    if args.require_activated and result.status != "activated_read_only":
        return 2
    return 0


def _blocked(
    contract_hash: str,
    checked_at: datetime,
    next_action: str,
    *,
    expired_at: datetime | None = None,
) -> FosunActivationResult:
    return FosunActivationResult(
        status="fixture_with_next_action",
        contract_hash=contract_hash,
        checked_at=checked_at,
        capability_expires_at=expired_at,
        next_action=next_action,
    )


def _contract_hash(
    *,
    manifest_sha256: str,
    sdk_tag: str | None,
    sdk_commit: str | None,
    probe_sha256: str | None,
    publication_receipt_sha256: str | None,
    trusted_publication_public_key_sha256: str | None,
) -> str:
    material = {
        "manifest_sha256": manifest_sha256,
        "probe_artifact_sha256": probe_sha256,
        "probe_publication_receipt_sha256": publication_receipt_sha256,
        "trusted_publication_public_key_sha256": (
            trusted_publication_public_key_sha256
        ),
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "sdk_subject": {"commit": sdk_commit, "tag": sdk_tag},
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    return _sha256(encoded)


def _load_mapping(document_bytes: bytes) -> dict[str, object] | None:
    try:
        document = json.loads(document_bytes)
    except (UnicodeError, json.JSONDecodeError):
        return None
    return document if isinstance(document, dict) else None


def _sdk_subject(document: dict[str, object] | None) -> tuple[str | None, str | None]:
    if document is None:
        return None, None
    sdk = document.get("sdk")
    if not isinstance(sdk, dict):
        return None, None
    tag = sdk.get("tag")
    commit = sdk.get("commit")
    safe_tag = (
        tag if isinstance(tag, str) and re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag) else None
    )
    safe_commit = (
        commit if isinstance(commit, str) and re.fullmatch(r"[0-9a-f]{40}", commit) else None
    )
    return safe_tag, safe_commit


def _is_forbidden_operation(name: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", name.lower())
    return any(marker in normalized for marker in _FORBIDDEN_OPERATION_MARKERS)


def _read_bounded_or_empty(path: Path, *, max_bytes: int) -> bytes:
    file_descriptor: int | None = None
    try:
        file_descriptor = _open_evidence_file(path)
        before = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in {0, os.geteuid()}
            or before.st_mode & 0o022
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > max_bytes
        ):
            return b""
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            try:
                chunk = os.read(file_descriptor, remaining)
            except InterruptedError:
                continue
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        after = os.fstat(file_descriptor)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_mode",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            len(content) != before.st_size
            or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        ):
            return b""
        return content
    except (OSError, ValueError):
        return b""
    finally:
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass


def _open_evidence_file(path: Path) -> int:
    components = path.parts
    if not components:
        raise OSError("invalid evidence path")
    if path.is_absolute():
        directory_fd = os.open(
            os.path.sep,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
        components = components[1:]
    else:
        directory_fd = os.open(
            ".",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
    try:
        starting_directory = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(starting_directory.st_mode)
            or starting_directory.st_uid not in {0, os.geteuid()}
            or starting_directory.st_mode & 0o022
        ):
            raise OSError("untrusted evidence root")
        if not components or any(component in {"", ".", ".."} for component in components):
            raise OSError("invalid evidence path")
        for component in components[:-1]:
            next_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
            try:
                directory = os.fstat(next_fd)
                if (
                    not stat.S_ISDIR(directory.st_mode)
                    or directory.st_uid not in {0, os.geteuid()}
                    or directory.st_mode & 0o022
                ):
                    raise OSError("untrusted evidence parent")
            except BaseException:
                os.close(next_fd)
                raise
            os.close(directory_fd)
            directory_fd = next_fd
        return os.open(
            components[-1],
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=directory_fd,
        )
    finally:
        os.close(directory_fd)


def _publication_receipt_is_valid(
    probe_bytes: bytes,
    publication_receipt_bytes: bytes,
    trusted_public_key: Ed25519PublicKey,
) -> bool:
    try:
        receipt = ProbePublicationReceipt.model_validate_json(publication_receipt_bytes)
        if not hmac.compare_digest(
            _canonical_json(receipt.model_dump(mode="json")),
            publication_receipt_bytes,
        ):
            return False
        artifact_sha256 = _sha256(probe_bytes)
        if not hmac.compare_digest(receipt.artifact_sha256, artifact_sha256):
            return False
        trusted_public_key.verify(
            _decode_base64url(receipt.signature_b64u, expected_bytes=64),
            PUBLICATION_SIGNING_DOMAIN + bytes.fromhex(artifact_sha256),
        )
    except (InvalidSignature, ValidationError, ValueError):
        return False
    return True


def _load_trusted_publication_key(
    trusted_publication_public_key_bytes: bytes,
) -> tuple[str, Ed25519PublicKey] | None:
    if len(trusted_publication_public_key_bytes) != MAX_TRUSTED_PUBLISHER_KEY_BYTES:
        return None
    try:
        encoded = trusted_publication_public_key_bytes.decode("ascii")
        raw = _decode_base64url(encoded, expected_bytes=32)
        public_key = Ed25519PublicKey.from_public_bytes(raw)
    except (UnicodeError, ValueError):
        return None
    return encoded, public_key


def _publisher_public_key_argument(value: str) -> bytes:
    try:
        return value.encode("ascii")
    except UnicodeEncodeError:
        raise argparse.ArgumentTypeError(
            "trusted publisher public key must be canonical base64url"
        ) from None


def _decode_base64url(value: str, *, expected_bytes: int) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (ValueError, TypeError):
        raise ValueError("base64url value is invalid") from None
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode()
    if len(decoded) != expected_bytes or not hmac.compare_digest(canonical, value):
        raise ValueError("base64url value is invalid")
    return decoded


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _require_aware(value: datetime, field_name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"{field_name} must be UTC")


if __name__ == "__main__":
    raise SystemExit(main())
