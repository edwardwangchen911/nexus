"""Strict publisher registry and stable delivery envelope models."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,254}$")
_SCHEMA = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")


@dataclass(frozen=True)
class OutboxMessage:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    schema_version: str
    payload: Mapping[str, object]


Publisher = Callable[[OutboxMessage], Awaitable[None]]


@dataclass(frozen=True)
class PublisherSpec:
    event_type: str
    schema_version: str
    consumer_idempotent: bool
    publisher: Publisher

    def __post_init__(self) -> None:
        if _NAME.fullmatch(self.event_type) is None:
            raise ValueError("publisher event_type is invalid")
        if _SCHEMA.fullmatch(self.schema_version) is None:
            raise ValueError("publisher schema_version is invalid")
        if not self.consumer_idempotent:
            raise ValueError("publisher consumer must be idempotent by event_id")
