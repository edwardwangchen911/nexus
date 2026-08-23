"""Opaque HMAC keyset cursor bound to one portfolio-list filter."""

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
from nexus.api.schemas.portfolio import PortfolioKind
from nexus.domain.time import require_utc

_CURSOR_DOMAIN = b"nexus-portfolio-cursor-v1\0"
_CURSOR_MAX_AGE = timedelta(minutes=5)
_CURSOR_MAX_LENGTH = 2048


@dataclass(frozen=True)
class PortfolioCursorState:
    decision_time: datetime
    after_name: str
    after_portfolio_id: UUID
    kind: PortfolioKind | None


class PortfolioCursorCodec:
    def __init__(
        self,
        signing_secrets: Sequence[str],
        request_time: datetime,
    ) -> None:
        self._secrets = tuple(secret for secret in signing_secrets if secret)
        if not self._secrets:
            raise ValueError("at least one cursor signing secret is required")
        self._request_time = require_utc(request_time)

    def encode(self, state: PortfolioCursorState) -> str:
        payload = json.dumps(
            {
                "after_name": state.after_name,
                "after_portfolio_id": str(state.after_portfolio_id),
                "decision_time": require_utc(state.decision_time)
                .isoformat()
                .replace("+00:00", "Z"),
                "kind": state.kind,
                "scope": "nexus.portfolios.v1",
                "version": 1,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        signature = hmac.new(
            self._secrets[0].encode(), _CURSOR_DOMAIN + payload, hashlib.sha256
        ).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor) > _CURSOR_MAX_LENGTH:
            raise RuntimeError("portfolio cursor exceeded its fixed bound")
        return cursor

    def decode(
        self,
        cursor: str,
        *,
        requested_kind: PortfolioKind | None,
    ) -> PortfolioCursorState:
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
                    hmac.new(secret.encode(), _CURSOR_DOMAIN + payload, hashlib.sha256).digest(),
                )
                for secret in self._secrets
            ):
                raise ValueError
            decoded = json.loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {
                "after_name",
                "after_portfolio_id",
                "decision_time",
                "kind",
                "scope",
                "version",
            }:
                raise ValueError
            if decoded["scope"] != "nexus.portfolios.v1" or decoded["version"] != 1:
                raise ValueError
            kind = decoded["kind"]
            if kind not in (None, "real_read_only", "internal_paper", "shadow"):
                raise ValueError
            if kind != requested_kind:
                raise ValueError
            name = decoded["after_name"]
            if not isinstance(name, str) or not 1 <= len(name) <= 100:
                raise ValueError
            decision_time = require_utc(
                datetime.fromisoformat(
                    cast(str, decoded["decision_time"]).replace("Z", "+00:00")
                )
            )
            age = self._request_time - decision_time
            if age < timedelta(0) or age > _CURSOR_MAX_AGE:
                raise ValueError
            return PortfolioCursorState(
                decision_time=decision_time,
                after_name=name,
                after_portfolio_id=UUID(cast(str, decoded["after_portfolio_id"])),
                kind=cast(PortfolioKind | None, kind),
            )
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error):
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The request could not be completed.",
                status_code=422,
            ) from None


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
