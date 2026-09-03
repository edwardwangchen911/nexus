"""Normalized public gateway models; provider objects never cross this module."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)

SyntheticSafeId = Annotated[str, StringConstraints(min_length=1, max_length=128)]
# Local transport bounds, not a claim about the provider's enum or symbol universe.
CashFlowBusinessType = Annotated[int, Field(strict=True, ge=-(2**63), le=2**63 - 1)]
CashFlowProductCode = Annotated[
    str,
    StringConstraints(
        strict=True, min_length=1, max_length=128, pattern=r"\A[A-Za-z0-9][A-Za-z0-9._:/-]*\z"
    ),
]


class GatewayModel(BaseModel):
    """Strict base class for all public gateway data."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class CapabilityItem(GatewayModel):
    kind: Literal["capability"] = "capability"
    provider: Literal["fosun"] = "fosun"
    mode: Literal["fixture", "read_only"]
    operations: tuple[str, ...]


class AccountItem(GatewayModel):
    kind: Literal["account"] = "account"
    external_account_id: SyntheticSafeId
    status: str
    base_currency: str | None = None


class CashItem(GatewayModel):
    kind: Literal["cash"] = "cash"
    external_account_id: SyntheticSafeId
    currency: str
    cash_balance: Decimal
    available_cash: Decimal
    provider_as_of: datetime | None = None

    @field_validator("provider_as_of")
    @classmethod
    def cash_provider_as_of_is_aware_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value)


class PositionItem(GatewayModel):
    kind: Literal["position"] = "position"
    external_account_id: SyntheticSafeId
    instrument_code: str
    currency: str
    quantity: Decimal
    available_quantity: Decimal
    frozen_quantity: Decimal
    average_cost: Decimal | None = None
    diluted_cost: Decimal | None = None
    current_price: Decimal | None = None
    provider_as_of: datetime | None = None

    @field_validator("provider_as_of")
    @classmethod
    def position_provider_as_of_is_aware_utc(
        cls, value: datetime | None
    ) -> datetime | None:
        return None if value is None else _as_utc(value)


class OrderItem(GatewayModel):
    kind: Literal["order"] = "order"
    external_order_id: SyntheticSafeId
    instrument_code: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    filled_quantity: Decimal
    price: Decimal | None = None
    status: str
    submitted_at: datetime
    updated_at: datetime | None = None

    @field_validator("submitted_at", "updated_at")
    @classmethod
    def order_times_are_aware_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value)


class CashFlowItem(GatewayModel):
    kind: Literal["cash_flow"] = "cash_flow"
    external_cash_flow_id: SyntheticSafeId
    currency: str
    amount: Decimal
    occurred_at: datetime | None
    provider_business_date: date | None = None
    direction: Literal[1, 2] | None = None
    flow_type: str
    business_type: CashFlowBusinessType | None = None
    product_code: CashFlowProductCode | None = None

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_is_aware_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value)

    @model_validator(mode="after")
    def cash_flow_has_date(self) -> "CashFlowItem":
        if self.occurred_at is None and self.provider_business_date is None:
            raise ValueError("cash flow requires a business date or an aware timestamp")
        return self


class QuoteItem(GatewayModel):
    kind: Literal["quote"] = "quote"
    instrument_code: str
    price: Decimal | None
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def as_of_is_aware_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class BarItem(GatewayModel):
    kind: Literal["bar"] = "bar"
    instrument_code: str
    interval: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @field_validator("timestamp")
    @classmethod
    def timestamp_is_aware_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


GatewayItem = Annotated[
    CapabilityItem
    | AccountItem
    | CashItem
    | PositionItem
    | OrderItem
    | CashFlowItem
    | QuoteItem
    | BarItem,
    Field(discriminator="kind"),
]

class GatewayPage[ItemT: BaseModel](GatewayModel):
    items: tuple[ItemT, ...]
    source_request_id: SyntheticSafeId
    has_more: bool
    next_cursor: str | None
    as_of: datetime
    quality: Literal["PASS", "PARTIAL"] = "PASS"

    @field_validator("as_of")
    @classmethod
    def page_as_of_is_aware_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class GatewayBarPage(GatewayPage[GatewayItem]):
    # Adapter-owned request fact, not a claim that the provider verified adjustment.
    sdk_requested_adjustment: Literal["noward"] | None = None
    provider_reported_delay: StrictBool | None = None


class GatewayError(Exception):
    """Sanitized error that is safe to serialize across the gateway boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int,
        next_action: str,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.next_action = next_action
        self.retry_after_seconds = retry_after_seconds


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
