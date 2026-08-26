"""Opaque HMAC keyset cursor for one internal-paper order status filter."""

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
from nexus.domain.time import require_utc
from nexus.paper.models import InternalOrderStatus

_CURSOR_DOMAIN = b"nexus-paper-order-cursor-v1\0"
_CURSOR_MAX_AGE = timedelta(minutes=5)
_CURSOR_MAX_LENGTH = 2048


@dataclass(frozen=True)
class PaperOrderCursorState:
    decision_time: datetime
    after_created_at: datetime
    after_order_id: UUID
    status: InternalOrderStatus | None


class PaperOrderCursorCodec:
    def __init__(self, signing_secrets: Sequence[str], request_time: datetime) -> None:
        self._secrets = tuple(secret for secret in signing_secrets if secret)
        if not self._secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._request_time = require_utc(request_time)

    def encode(self, state: PaperOrderCursorState) -> str:
        payload = json.dumps(
            {
                "after_created_at": require_utc(state.after_created_at)
                .isoformat()
                .replace("+00:00", "Z"),
                "after_order_id": str(state.after_order_id),
                "decision_time": require_utc(state.decision_time)
                .isoformat()
                .replace("+00:00", "Z"),
                "scope": "nexus.paper.orders.v1",
                "status": state.status,
                "version": 1,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        signature = hmac.new(
            self._secrets[0].encode(),
            _CURSOR_DOMAIN + payload,
            hashlib.sha256,
        ).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor) > _CURSOR_MAX_LENGTH:
            raise RuntimeError("paper order cursor exceeded its fixed bound")
        return cursor

    def decode(
        self,
        cursor: str,
        *,
        requested_status: InternalOrderStatus | None,
    ) -> PaperOrderCursorState:
        try:
            if not 1 <= len(cursor) <= _CURSOR_MAX_LENGTH:
                raise ValueError
            parts = cursor.split(".")
            if len(parts) != 2:
                raise ValueError
            payload = _b64decode(parts[0])
            signature = _b64decode(parts[1])
            if len(signature) != hashlib.sha256().digest_size or not any(
                hmac.compare_digest(
                    signature,
                    hmac.new(
                        secret.encode(),
                        _CURSOR_DOMAIN + payload,
                        hashlib.sha256,
                    ).digest(),
                )
                for secret in self._secrets
            ):
                raise ValueError
            decoded = json.loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {
                "after_created_at",
                "after_order_id",
                "decision_time",
                "scope",
                "status",
                "version",
            }:
                raise ValueError
            if decoded["scope"] != "nexus.paper.orders.v1" or decoded["version"] != 1:
                raise ValueError
            status = decoded["status"]
            allowed_statuses = {
                None,
                "proposed",
                "risk_rejected",
                "risk_approved",
                "awaiting_approval",
                "submitted",
                "partially_filled",
                "filled",
                "cancelled",
                "rejected",
                "reconciling",
            }
            if status not in allowed_statuses or status != requested_status:
                raise ValueError
            decision_time = _parse_time(decoded["decision_time"])
            age = self._request_time - decision_time
            if age < timedelta(0) or age > _CURSOR_MAX_AGE:
                raise ValueError
            return PaperOrderCursorState(
                decision_time=decision_time,
                after_created_at=_parse_time(decoded["after_created_at"]),
                after_order_id=UUID(cast(str, decoded["after_order_id"])),
                status=cast(InternalOrderStatus | None, status),
            )
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error):
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The request could not be completed.",
                status_code=422,
            ) from None


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    return require_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
