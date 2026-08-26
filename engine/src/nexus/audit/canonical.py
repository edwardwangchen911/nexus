"""Strict RFC 8785-compatible subset used by the NEXUS audit chain."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_MAX_SAFE_INTEGER = 9_007_199_254_740_991


def _strict_json_value(value: object) -> object:
    if value is None or isinstance(value, str | bool):
        return value
    if isinstance(value, int):
        if not -_MAX_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            raise ValueError("audit integers must stay within the JavaScript safe range")
        return value
    if isinstance(value, float):
        raise TypeError("audit canonical JSON rejects binary floats")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("audit canonical JSON requires string keys")
        return {key: _strict_json_value(value[key]) for key in sorted(value)}
    if isinstance(value, tuple | list):
        return [_strict_json_value(item) for item in value]
    raise TypeError(f"audit canonical JSON rejects {type(value).__name__}")


def canonical_json(value: object) -> str:
    """Return deterministic JCS bytes for the deliberately strict JSON subset."""
    return json.dumps(
        _strict_json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


class AuditChainInput(BaseModel):
    """The exact event fields covered by one audit-chain hash."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_seq: int = Field(ge=1)
    occurred_at: datetime
    actor_id: str = Field(min_length=1, max_length=255)
    action: str = Field(min_length=1, max_length=255)
    schema_version: str = Field(min_length=1, max_length=32)
    payload: dict[str, object]

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("audit occurred_at must use UTC")
        return value

    @field_validator("payload")
    @classmethod
    def _payload_is_strict_json(cls, value: dict[str, object]) -> dict[str, object]:
        canonical_json(value)
        return value


def canonical_audit_event(event: AuditChainInput) -> str:
    """Canonicalize one complete, hash-covered event without chain hashes."""
    return canonical_json(
        {
            "event_seq": event.event_seq,
            "occurred_at": event.occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "actor_id": event.actor_id,
            "action": event.action,
            "schema_version": event.schema_version,
            "payload": event.payload,
        }
    )


def audit_event_hash(previous_hash: str, canonical_event: str) -> str:
    """Hash the exact previous lowercase hash and canonical event text."""
    if len(previous_hash) != 64 or any(
        character not in "0123456789abcdef" for character in previous_hash
    ):
        raise ValueError("previous hash must be 64 lowercase hexadecimal characters")
    return hashlib.sha256((previous_hash + canonical_event).encode("utf-8")).hexdigest()
