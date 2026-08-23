"""Reviewed financial fact schemas returned by the Fosun gateway."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


class GatewayFact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("*", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("binary floating-point financial facts are forbidden")
        return value

    @field_validator("*", mode="after")
    @classmethod
    def _reject_non_finite_decimal(cls, value: object) -> object:
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("non-finite financial fact")
        return value


class CashItem(GatewayFact):
    kind: Literal["cash"]
    external_account_id: str
    currency: str
    cash_balance: Decimal
    available_cash: Decimal
    provider_as_of: datetime | None


class PositionItem(GatewayFact):
    kind: Literal["position"]
    external_account_id: str
    instrument_code: str
    currency: str
    quantity: Decimal
    available_quantity: Decimal
    frozen_quantity: Decimal
    average_cost: Decimal | None
    diluted_cost: Decimal | None
    current_price: Decimal | None
    provider_as_of: datetime | None


class OrderItem(GatewayFact):
    kind: Literal["order"]
    external_order_id: str
    instrument_code: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    filled_quantity: Decimal
    price: Decimal | None
    status: str
    submitted_at: datetime
    updated_at: datetime | None


class CashFlowItem(GatewayFact):
    kind: Literal["cash_flow"]
    external_cash_flow_id: str
    currency: str
    amount: Decimal
    occurred_at: datetime
    flow_type: str
