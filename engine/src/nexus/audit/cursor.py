"""Opaque filter-bound cursor for the redacted risk-audit event stream."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from nexus.api.errors import ErrorCode, NexusError
from nexus.audit.redaction import AuditKind
from nexus.domain.time import require_utc

_DOMAIN = b"nexus-risk-audit-cursor-v1\0"
_MAX_AGE = timedelta(minutes=5)
_MAX_LENGTH = 2048


@dataclass(frozen=True)
class RiskAuditCursorState:
    decision_time: datetime
    from_at: datetime
    to_at: datetime
    kind: AuditKind | None
    after_occurred_at: datetime
    after_event_id: UUID


class RiskAuditCursorCodec:
    def __init__(self, signing_secrets: Sequence[str], request_time: datetime) -> None:
        self._secrets = tuple(secret for secret in signing_secrets if secret)
        if not self._secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._request_time = require_utc(request_time)

    def encode(self, state: RiskAuditCursorState) -> str:
        payload = json.dumps(
            {
                "after_event_id": str(state.after_event_id),
                "after_occurred_at": _time(state.after_occurred_at),
                "decision_time": _time(state.decision_time),
                "from_at": _time(state.from_at),
                "kind": state.kind,
                "scope": "nexus.risk_audit.v1",
                "to_at": _time(state.to_at),
                "version": 1,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        signature = hmac.new(self._secrets[0].encode(), _DOMAIN + payload, hashlib.sha256).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor) > _MAX_LENGTH:
            raise RuntimeError("risk-audit cursor exceeded its fixed bound")
        return cursor

    def decode(
        self,
        cursor: str,
        *,
        requested_from: datetime | None,
        requested_to: datetime | None,
        requested_kind: AuditKind | None,
    ) -> RiskAuditCursorState:
        try:
            if not 1 <= len(cursor) <= _MAX_LENGTH:
                raise ValueError
            parts = cursor.split(".")
            if len(parts) != 2:
                raise ValueError
            payload = _b64decode(parts[0])
            signature = _b64decode(parts[1])
            if len(signature) != hashlib.sha256().digest_size or not any(
                hmac.compare_digest(
                    signature,
                    hmac.new(secret.encode(), _DOMAIN + payload, hashlib.sha256).digest(),
                )
                for secret in self._secrets
            ):
                raise ValueError
            decoded = json.loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {
                "after_event_id",
                "after_occurred_at",
                "decision_time",
                "from_at",
                "kind",
                "scope",
                "to_at",
                "version",
            }:
                raise ValueError
            if decoded["scope"] != "nexus.risk_audit.v1" or decoded["version"] != 1:
                raise ValueError
            kind = decoded["kind"]
            if kind not in {
                None,
                "quality",
                "risk",
                "agent_cost",
                "job",
                "reconciliation",
                "audit",
            }:
                raise ValueError
            state = RiskAuditCursorState(
                decision_time=_parse_time(decoded["decision_time"]),
                from_at=_parse_time(decoded["from_at"]),
                to_at=_parse_time(decoded["to_at"]),
                kind=cast(AuditKind | None, kind),
                after_occurred_at=_parse_time(decoded["after_occurred_at"]),
                after_event_id=UUID(cast(str, decoded["after_event_id"])),
            )
            age = self._request_time - state.decision_time
            if age < timedelta(0) or age > _MAX_AGE:
                raise ValueError
            if (
                state.kind != requested_kind
                or (requested_from is not None and state.from_at != require_utc(requested_from))
                or (requested_to is not None and state.to_at != require_utc(requested_to))
            ):
                raise ValueError
            return state
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error):
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The request could not be completed.",
                status_code=422,
            ) from None


def _time(value: datetime) -> str:
    return require_utc(value).isoformat().replace("+00:00", "Z")


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    return require_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
