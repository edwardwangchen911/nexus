"""Current action observations: local revisions, never ledger or PIT authority."""

import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.providers.corporate_actions.alpaca_http import CorporateActionsRequest

SOURCE = "alpaca_action_observation"
SCHEMA = "alpaca_action_observation_v1"
MAX_RAW_BYTES = 32 * 1024 * 1024
MAX_EVENTS = 10000


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class ActionPageReference(_Frozen):
    page_number: int = Field(strict=True, ge=1, le=100)
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_byte_length: int = Field(strict=True, gt=0, le=8 * 1024 * 1024)
    receipt_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_byte_length: int = Field(strict=True, gt=0, le=64 * 1024)
    received_at: AwareDatetime

    @field_validator("received_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class ActionEventObservation(_Frozen):
    category: str = Field(min_length=1, max_length=255)
    provider_event_id: str = Field(min_length=1, max_length=255)
    page_number: int = Field(strict=True, ge=1, le=100)
    index: int = Field(strict=True, ge=0, lt=1000)
    symbol: str | None = Field(default=None, max_length=255)
    old_symbol: str | None = Field(default=None, max_length=255)
    new_symbol: str | None = Field(default=None, max_length=255)
    process_date: date | None = None
    ex_date: date | None = None
    record_date: date | None = None
    payable_date: date | None = None
    declaration_date: date | None = None
    rate: Decimal | None = Field(default=None, allow_inf_nan=False)
    old_rate: Decimal | None = Field(default=None, allow_inf_nan=False)
    new_rate: Decimal | None = Field(default=None, allow_inf_nan=False)
    currency: str | None = Field(default=None, max_length=32)

    @field_validator("rate", "old_rate", "new_rate", mode="before")
    @classmethod
    def _decimal(cls, value: object) -> object:
        if isinstance(value, float | bool) or (
            value is not None and not isinstance(value, str | Decimal | int)
        ):
            raise ValueError("ALPACA_ACTIONS_NUMBER_INVALID")
        return value

    @field_validator(
        "process_date", "ex_date", "record_date", "payable_date", "declaration_date", mode="before"
    )
    @classmethod
    def _date(cls, value: object) -> object:
        if value is None or type(value) is date:
            return value
        if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
            raise ValueError("ALPACA_ACTIONS_DATE_INVALID")
        return date.fromisoformat(value)


class AlpacaActionObservationDocument(_Frozen):
    instrument_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    query_start: date
    query_end: date
    available_at: AwareDatetime
    manifest_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_byte_length: int = Field(strict=True, gt=0, le=128 * 1024)
    pages: tuple[ActionPageReference, ...] = Field(min_length=1, max_length=100)
    events: tuple[ActionEventObservation, ...] = Field(max_length=MAX_EVENTS)
    total_event_count: int = Field(strict=True, ge=0, le=MAX_EVENTS)
    unbound_event_count: int = Field(strict=True, ge=0, le=MAX_EVENTS)
    pagination_exhausted: Literal[True] = True
    source_completeness: Literal["unknown"] = "unknown"
    historical_backtest_eligible: Literal[False] = False
    ledger_eligible: Literal[False] = False
    revision_semantics: Literal["local_content_hash"] = "local_content_hash"
    execution_locked: Literal[True] = True

    @field_validator("available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _validate(self) -> "AlpacaActionObservationDocument":
        CorporateActionsRequest(self.symbol, self.query_start, self.query_end)
        times = [page.received_at for page in self.pages]
        positions = [(event.page_number, event.category, event.index) for event in self.events]
        if (
            [page.page_number for page in self.pages] != list(range(1, len(self.pages) + 1))
            or sum(page.raw_byte_length for page in self.pages) > MAX_RAW_BYTES
            or times != sorted(times)
            or self.available_at != max(times)
            or len(self.events) + self.unbound_event_count != self.total_event_count
            or len(positions) != len(set(positions))
            or any(event.page_number > len(self.pages) for event in self.events)
            or any(
                self.symbol not in (event.symbol, event.old_symbol, event.new_symbol)
                for event in self.events
            )
        ):
            raise ValueError("ALPACA_ACTIONS_DOCUMENT_INVALID")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()

    @property
    def provider_object_id(self) -> str:
        return f"actions:{self.instrument_id}:{self.query_start}:{self.query_end}"
