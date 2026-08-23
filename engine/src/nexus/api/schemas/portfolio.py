"""Strict redacted contracts for broker-backed portfolio reads and sync admission."""

from typing import Annotated, Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

type PortfolioKind = Literal["real_read_only", "internal_paper", "shadow"]
type SnapshotStatus = Literal["READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"]
type SyncRunStatus = Literal[
    "ACTIVE", "READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED", "FAILED"
]
type CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BrokerSyncCreateRequest(_StrictSchema):
    portfolio_id: UUID


class PortfolioListItemResponse(_StrictSchema):
    portfolio_id: UUID
    name: str = Field(min_length=1, max_length=100)
    kind: Literal["real_read_only"] = "real_read_only"
    coverage_scope: Literal["broker_account"] = "broker_account"
    provider: str = Field(min_length=1, max_length=50)
    latest_snapshot_status: SnapshotStatus | None
    latest_snapshot_as_of: AwareDatetime | None
    original_currencies: tuple[CurrencyCode, ...]
    last_sync_run_status: SyncRunStatus | None


class PortfolioPageResponse(_StrictSchema):
    items: tuple[PortfolioListItemResponse, ...] = Field(max_length=50)
    next_cursor: str | None


class PortfolioPageEnvelope(_StrictSchema):
    data: PortfolioPageResponse
    meta: SuccessMetaResponse


class CashBucketResponse(_StrictSchema):
    currency: CurrencyCode
    cash_balance: DecimalString
    available_cash: DecimalString
    provider_as_of: AwareDatetime | None


class PositionResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    exchange: str = Field(min_length=1, max_length=20)
    currency: CurrencyCode
    quantity: DecimalString
    available_quantity: DecimalString
    frozen_quantity: DecimalString
    avg_cost: DecimalString | None
    diluted_cost: DecimalString | None
    current_price: DecimalString | None
    provider_as_of: AwareDatetime | None
    read_only: Literal[True] = True


class OpenOrderResponse(_StrictSchema):
    broker_order_snapshot_id: UUID
    instrument_id: UUID
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    currency: CurrencyCode
    side: str = Field(min_length=1, max_length=10)
    quantity: DecimalString
    filled_quantity: DecimalString
    price: DecimalString | None
    status: str = Field(min_length=1, max_length=30)
    updated_at: AwareDatetime | None
    read_only: Literal[True] = True


class PortfolioSnapshotResponse(_StrictSchema):
    portfolio_snapshot_id: UUID
    portfolio_id: UUID
    coverage_scope: Literal["broker_account"] = "broker_account"
    status: SnapshotStatus
    cash: tuple[CashBucketResponse, ...] = Field(max_length=32)
    positions: tuple[PositionResponse, ...] = Field(max_length=5000)
    open_orders: tuple[OpenOrderResponse, ...] = Field(max_length=5000)
    converted_view: None = None
    as_of: AwareDatetime | None


class PortfolioSnapshotEnvelope(_StrictSchema):
    data: PortfolioSnapshotResponse
    meta: SuccessMetaResponse


class BrokerSyncRunResponse(_StrictSchema):
    broker_sync_run_id: UUID
    portfolio_id: UUID
    status: SyncRunStatus
    trigger_kind: Literal["MANUAL", "AUTOMATIC"]
    accepted_at: AwareDatetime
    completed_at: AwareDatetime | None
    attempt_count: int = Field(ge=0)
    cash_flow_quality: Literal["PASS", "PARTIAL", "NOT_RUN"]
    failure_category: Literal["SYNC_FAILED"] | None


class BrokerSyncRunEnvelope(_StrictSchema):
    data: BrokerSyncRunResponse
    meta: SuccessMetaResponse
