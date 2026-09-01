"""Strict, transport-free extraction of one detached probe publication receipt."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    model_validator,
)

from nexus_fosun_gateway.activation import (
    MAX_ARTIFACT_BYTES,
    MAX_PUBLICATION_RECEIPT_BYTES,
    ScopedProbePublicationReceipt,
    _open_evidence_file,
)
from nexus_fosun_gateway.probe_contract import PROBE_RESULT_FIELD_NAMES
from nexus_fosun_gateway.publication import publish_probe_artifact

_ARGUMENT_NAMES = (
    "--capture-exit",
    "--frame-dir",
    "--frame-basename",
    "--artifact-dir",
    "--artifact-basename",
)
_FRAME_BASENAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,56}\.frame\.json")
_ARTIFACT_BASENAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,62}\.json")
_MAX_FRAME_BYTES = 16 * 1024
_PUBLICATION_RECEIPT_BASENAME = "publication-receipt.json"
_Hex64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_RunId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{15,63}$")]
_UtcTimestamp = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$"
    ),
]


class _ProbeSuccessFrame(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
    )

    status: Literal["probe_published"]
    code: Literal["OK"]
    failure_stage: None
    failure_operation: None
    failure_category: None
    physical_request_count: None
    run_id: _RunId
    scope: Literal["core"]
    scope_contract_version: Literal["nexus.fosun.read-only-scope.v1.core"]
    artifact_sha256: _Hex64
    byte_length: StrictInt = Field(ge=1, le=MAX_ARTIFACT_BYTES)
    capability_expires_at: _UtcTimestamp
    activation_contract_hash: _Hex64
    publication_receipt: ScopedProbePublicationReceipt

    @model_validator(mode="after")
    def _validate_scope_identity(self) -> _ProbeSuccessFrame:
        receipt = self.publication_receipt
        if (
            receipt.run_id != self.run_id
            or receipt.scope != self.scope
            or receipt.scope_contract_version != self.scope_contract_version
        ):
            raise ValueError("probe publication scope identity mismatch")
        return self


def main(argv: Sequence[str] | None = None) -> int:
    """Publish a canonical receipt or return one value-free failure status."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    try:
        if (
            len(arguments) != len(_ARGUMENT_NAMES) * 2
            or tuple(arguments[::2]) != _ARGUMENT_NAMES
        ):
            return 2
        capture_exit = arguments[1]
        frame_dir = Path(arguments[3])
        frame_basename = arguments[5]
        artifact_dir = Path(arguments[7])
        artifact_basename = arguments[9]
        if (
            capture_exit != "0"
            or not frame_dir.is_absolute()
            or not artifact_dir.is_absolute()
            or _FRAME_BASENAME.fullmatch(frame_basename) is None
            or _ARTIFACT_BASENAME.fullmatch(artifact_basename) is None
            or artifact_basename == _PUBLICATION_RECEIPT_BASENAME
        ):
            return 2
        frame_bytes = _read_current_user_file(
            frame_dir / frame_basename,
            max_bytes=_MAX_FRAME_BYTES,
        )
        if (
            not frame_bytes.endswith(b"\n")
            or frame_bytes.count(b"\n") != 1
        ):
            return 2
        frame_document = json.loads(frame_bytes)
        if (
            not isinstance(frame_document, dict)
            or tuple(frame_document) != PROBE_RESULT_FIELD_NAMES
        ):
            return 2
        frame = _ProbeSuccessFrame.model_validate(frame_document)
        artifact_bytes = _read_current_user_file(
            artifact_dir / artifact_basename,
            max_bytes=MAX_ARTIFACT_BYTES,
        )
        artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        if (
            not hmac.compare_digest(artifact_sha256, frame.artifact_sha256)
            or len(artifact_bytes) != frame.byte_length
            or not hmac.compare_digest(
                frame.publication_receipt.artifact_sha256,
                artifact_sha256,
            )
        ):
            return 2
        receipt_bytes = json.dumps(
            frame.publication_receipt.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if len(receipt_bytes) > MAX_PUBLICATION_RECEIPT_BYTES:
            return 2
        publication = publish_probe_artifact(
            artifact_dir,
            _PUBLICATION_RECEIPT_BASENAME,
            receipt_bytes,
            expected_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
            expected_byte_length=len(receipt_bytes),
        )
        summary = json.dumps(
            {
                "status": "receipt_published",
                "artifact_sha256": artifact_sha256,
                "receipt_sha256": publication.artifact_sha256,
            },
            separators=(",", ":"),
        ).encode() + b"\n"
        _write_all(1, summary)
        return 0
    except BaseException:
        return 2


def run() -> None:
    raise SystemExit(main())


def _read_current_user_file(path: Path, *, max_bytes: int) -> bytes:
    file_descriptor: int | None = None
    try:
        file_descriptor = _open_evidence_file(path)
        before = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or not 1 <= before.st_size <= max_bytes
        ):
            raise OSError("invalid probe evidence")
        content = _read_exact(file_descriptor, max_bytes)
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
            raise OSError("unstable probe evidence")
        return content
    finally:
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass


def _read_exact(file_descriptor: int, max_bytes: int) -> bytes:
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
    return b"".join(chunks)


def _write_all(file_descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        try:
            written = os.write(file_descriptor, view[offset:])
        except InterruptedError:
            continue
        if (
            not isinstance(written, int)
            or isinstance(written, bool)
            or not 1 <= written <= len(view) - offset
        ):
            raise OSError("short receipt status write")
        offset += written


if __name__ == "__main__":
    run()
