"""Strict response and input contracts for the read-only broker gateway."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

MAX_RESPONSE_BYTES = 1_048_576
MAX_ITEMS = 100


class GatewayClientError(RuntimeError):
    """Sanitized transport or contract failure without response or secret material."""


class GatewayResponsePage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[dict[str, object], ...]
    source_request_id: str
    has_more: bool
    next_cursor: str | None
    as_of: datetime
    quality: Literal["PASS", "PARTIAL"]

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("gateway as_of must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _bounded_consistent_page(self) -> GatewayResponsePage:
        if not self.source_request_id or len(self.source_request_id.encode("utf-8")) > 128:
            raise ValueError("gateway source request ID is invalid")
        if len(self.items) > MAX_ITEMS:
            raise ValueError("gateway page exceeds fixed item limit")
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("gateway cursor state is inconsistent")
        validate_cursor(self.next_cursor)
        return self


def validate_account_id(value: str) -> None:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value.encode("utf-8")) > 128
    ):
        raise GatewayClientError("gateway account scope rejected")


def validate_pagination(cursor: str | None, limit: int) -> None:
    if type(limit) is not int or limit != MAX_ITEMS:
        raise GatewayClientError("gateway pagination rejected")
    try:
        validate_cursor(cursor)
    except ValueError:
        raise GatewayClientError("gateway pagination rejected") from None


def validate_cursor(cursor: str | None) -> None:
    if cursor is not None and (
        not isinstance(cursor, str)
        or not cursor.isascii()
        or not cursor.isdecimal()
        or len(cursor) > 12
    ):
        raise ValueError("gateway cursor is invalid")


def is_exact_json_media_type(value: str | None) -> bool:
    if value is None:
        return False
    parts = [part.strip().lower() for part in value.split(";")]
    if parts[0] != "application/json":
        return False
    return all(part == "charset=utf-8" for part in parts[1:]) and len(parts) <= 2


def validate_content_length(value: str | None) -> None:
    if value is None:
        return
    if not value.isascii() or not value.isdecimal() or int(value) > MAX_RESPONSE_BYTES:
        raise GatewayClientError("gateway response rejected")
