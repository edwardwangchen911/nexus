"""Byte-verified, non-executable evidence references."""

import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc

_NON_EXECUTABLE_LOCATION_SCHEMES = frozenset({"memory", "s3", "package"})


class EvidencePayloadError(ValueError):
    """The archived bytes cannot safely be represented as evidence."""


@dataclass(frozen=True)
class VerifiedEvidencePayload:
    """An immutable consumer reference bound to the bytes that produced it."""

    reference: EvidenceRef
    byte_length: int


def evidence_ref_from_payload(
    payload: bytes,
    *,
    source: str,
    object_id: str,
    available_at: datetime,
    location: str,
    expected_sha256: str,
    expected_byte_length: int,
) -> VerifiedEvidencePayload:
    """Verify archival metadata before exposing an evidence reference to consumers."""
    if not isinstance(payload, bytes):
        raise EvidencePayloadError("PAYLOAD_MUST_BE_BYTES")
    if expected_byte_length < 0:
        raise EvidencePayloadError("BYTE_LENGTH_MISMATCH")
    if len(payload) != expected_byte_length:
        raise EvidencePayloadError("BYTE_LENGTH_MISMATCH")
    digest = sha256(payload).hexdigest()
    if digest != expected_sha256:
        raise EvidencePayloadError("CONTENT_HASH_MISMATCH")
    if not _is_pinned_non_executable_location(location, digest):
        raise EvidencePayloadError("NON_EXECUTABLE_LOCATION")
    return VerifiedEvidencePayload(
        reference=EvidenceRef(
            source=source,
            object_id=object_id,
            available_at=require_utc(available_at),
            location=location,
            content_hash=digest,
        ),
        byte_length=len(payload),
    )


def _is_pinned_non_executable_location(location: str, digest: str) -> bool:
    scheme, separator, stored_object = location.partition("://")
    if (
        separator != "://"
        or scheme not in _NON_EXECUTABLE_LOCATION_SCHEMES
        or not stored_object
    ):
        return False
    if scheme == "memory":
        return stored_object.startswith(f"sha256/{digest}")
    if scheme == "package":
        return f"@sha256:{digest}" in stored_object
    if f"/sha256/{digest}" in stored_object:
        return True
    return re.search(r"[?&]versionId=[^&#]+", stored_object) is not None
