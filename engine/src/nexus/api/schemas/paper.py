"""Strict redacted contracts for the internal-paper console."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.paper.models import InternalOrderStatus
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PaperPerformanceResponse(_StrictSchema):
    as_of: AwareDatetime
    cash: DecimalString
    available_cash: DecimalString
    reserved_cash: DecimalString
    market_value: DecimalString
    nav: DecimalString
    peak_nav: DecimalString
    realized_pnl: DecimalString
    unrealized_pnl: DecimalString
    fees: DecimalString
    drawdown_ratio: DecimalString


class PaperPositionResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    quantity: DecimalString
    reserved_quantity: DecimalString
    available_quantity: DecimalString
    cost_basis: DecimalString
    average_cost: DecimalString
    reference_close: DecimalString
    market_value: DecimalString
    unrealized_pnl: DecimalString
    price_source: str = Field(min_length=1, max_length=100)
    price_feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    price_as_of: AwareDatetime


class PaperAttributionResponse(_StrictSchema):
    dimension: Literal["cash", "instrument", "sector"]
    dimension_key: str = Field(min_length=1, max_length=100)
    market_value: DecimalString
    realized_pnl: DecimalString
    unrealized_pnl: DecimalString
    fees: DecimalString
    slippage: DecimalString
    completeness: Literal["FULL", "PARTIAL"]


class PaperPortfolioResponse(_StrictSchema):
    portfolio_id: UUID
    name: str = Field(min_length=1, max_length=100)
    mode: Literal["internal_paper"]
    currency: Literal["USD"]
    initial_capital: DecimalString
    data_state: Literal["EMPTY", "READY", "PARTIAL", "STALE"]
    performance: PaperPerformanceResponse | None
    positions: tuple[PaperPositionResponse, ...] = Field(max_length=500)
    attributions: tuple[PaperAttributionResponse, ...] = Field(max_length=500)


class PaperPortfolioEnvelope(_StrictSchema):
    data: PaperPortfolioResponse
    meta: SuccessMetaResponse


class PaperOrderEventResponse(_StrictSchema):
    event_seq: int = Field(ge=1)
    from_status: InternalOrderStatus | None
    to_status: InternalOrderStatus
    actor_kind: Literal["task18", "task19", "user", "reconciliation"]
    reason_code: str = Field(min_length=1, max_length=100)
    occurred_at: AwareDatetime


class PaperFillResponse(_StrictSchema):
    fill_id: UUID
    fill_seq: int = Field(ge=1)
    quantity: DecimalString
    price: DecimalString
    notional: DecimalString
    fee: DecimalString
    occurred_at: AwareDatetime
    source: str = Field(min_length=1, max_length=100)
    feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    source_as_of: AwareDatetime
    execution_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    slippage_bps: Literal[10, 25]
    slippage: DecimalString


class PaperOrderResponse(_StrictSchema):
    order_id: UUID
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    side: Literal["buy", "sell"]
    quantity: DecimalString
    filled_quantity: DecimalString
    remaining_quantity: DecimalString
    worst_price: DecimalString
    status: InternalOrderStatus
    cancellable: bool
    current_event_seq: int = Field(ge=1)
    submitted_at: AwareDatetime
    strategy_version: str = Field(min_length=1, max_length=50)
    risk_policy_version: str = Field(min_length=1, max_length=50)
    events: tuple[PaperOrderEventResponse, ...] = Field(max_length=128)
    fills: tuple[PaperFillResponse, ...] = Field(max_length=128)


class PaperOrderPageResponse(_StrictSchema):
    items: tuple[PaperOrderResponse, ...] = Field(max_length=50)
    next_cursor: str | None


class PaperOrderPageEnvelope(_StrictSchema):
    data: PaperOrderPageResponse
    meta: SuccessMetaResponse


class PaperCancelRequest(_StrictSchema):
    expected_event_seq: int = Field(ge=1)


class PaperCancelResponse(_StrictSchema):
    order_id: UUID
    status: Literal["cancelled"]
    filled_quantity: DecimalString
    remaining_quantity: DecimalString
    result_event_seq: int = Field(ge=1)
    replayed: bool


class PaperCancelEnvelope(_StrictSchema):
    data: PaperCancelResponse
    meta: SuccessMetaResponse
