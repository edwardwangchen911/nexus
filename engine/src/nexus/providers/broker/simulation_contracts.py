"""Strict wire contracts for the isolated broker-simulation gateway."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

DecimalString = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^(0|[1-9]\d*)(\.\d+)?$", max_length=40),
]
Cursor = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9]+$", max_length=12)]
MAX_SIMULATION_REQUEST_BYTES = 65_536
MAX_SIMULATION_RESPONSE_BYTES = 1_048_576


class SimulationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SimulationCreateOrderRequest(SimulationContract):
    command_id: UUID
    account_binding_id: UUID
    intent_order_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)
    instrument_code: str = Field(min_length=1, max_length=32)
    side: Literal["buy", "sell"]
    quantity: DecimalString
    limit_price: DecimalString


class SimulationModifyOrderRequest(SimulationContract):
    command_id: UUID
    account_binding_id: UUID
    order_binding_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)
    quantity: DecimalString
    limit_price: DecimalString


class SimulationCancelOrderRequest(SimulationContract):
    command_id: UUID
    account_binding_id: UUID
    order_binding_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)


class SimulationQueryRequest(SimulationContract):
    account_binding_id: UUID
    cursor: Cursor | None = None
    limit: int = Field(default=100, ge=1, le=100)


class SimulationPortfolioRequest(SimulationContract):
    account_binding_id: UUID


class SimulationCapabilityResponse(SimulationContract):
    mode: Literal["offline_fixture"]
    external_activation: Literal["external_activation_blocked"]
    operations: tuple[str, ...]
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("simulation as_of must be timezone-aware")
        return value.astimezone(UTC)


class SimulationCommandResponse(SimulationContract):
    command_id: UUID
    outcome: Literal["acknowledged", "rejected", "transport_failed", "result_unknown"]
    safe_code: str = Field(min_length=1, max_length=64)
    provider_order_ref: str | None = Field(default=None, max_length=128)
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("simulation as_of must be timezone-aware")
        return value.astimezone(UTC)


class SimulationSnapshotPage(SimulationContract):
    items: tuple[dict[str, object], ...]
    source_request_id: str = Field(min_length=1, max_length=128)
    has_more: bool
    next_cursor: Cursor | None
    as_of: datetime
    quality: Literal["PASS", "PARTIAL", "CONFLICT"]

    @field_validator("as_of")
    @classmethod
    def _page_as_of_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("simulation as_of must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _cursor_is_consistent(self) -> SimulationSnapshotPage:
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("simulation cursor state is inconsistent")
        if len(self.items) > 100:
            raise ValueError("simulation page exceeds fixed item limit")
        return self
