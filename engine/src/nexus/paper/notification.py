"""Closed, event-time facts used to render internal-paper notifications."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from nexus.paper.models import exact_decimal

NotificationStatus = Literal["risk_rejected", "filled", "cancelled"]
NotificationPriceKind = Literal["reference", "worst_case", "average_fill"]
NotificationBalanceKind = Literal["projected_after", "actual_after", "unchanged"]
NotificationSymbol = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Z0-9][A-Z0-9.\-]{0,19}$"),
]

_CANONICAL_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?$")


class PaperNotificationError(ValueError):
    """Value-free notification contract error safe for outbox recording."""


def _exact_notification_decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, str):
        if _CANONICAL_DECIMAL.fullmatch(value) is None:
            raise ValueError(f"{field} must be a canonical decimal string")
        try:
            value = Decimal(value)
        except InvalidOperation as error:
            raise ValueError(f"{field} must be a canonical decimal string") from error
    return exact_decimal(value, field=field)


def _require_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value


def _decimal_text(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"-0", ""} else rendered


class _FrozenNotificationModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PaperNotificationPrice(_FrozenNotificationModel):
    kind: NotificationPriceKind
    value: Decimal
    currency: Literal["USD"]

    @field_validator("value", mode="before")
    @classmethod
    def _exact_value(cls, value: object) -> Decimal:
        result = _exact_notification_decimal(value, field="price value")
        if result <= 0:
            raise ValueError("price value must be positive")
        return result

    @field_serializer("value", when_used="json")
    def _serialize_value(self, value: Decimal) -> str:
        return _decimal_text(value)


class PaperNotificationCash(_FrozenNotificationModel):
    kind: NotificationBalanceKind
    available: Decimal

    @field_validator("available", mode="before")
    @classmethod
    def _exact_available(cls, value: object) -> Decimal:
        result = _exact_notification_decimal(value, field="available cash")
        if result < 0:
            raise ValueError("available cash must not be negative")
        return result

    @field_serializer("available", when_used="json")
    def _serialize_available(self, value: Decimal) -> str:
        return _decimal_text(value)


class PaperNotificationPosition(_FrozenNotificationModel):
    kind: NotificationBalanceKind
    quantity: Decimal
    portfolio_ratio: Decimal | None

    @field_validator("quantity", "portfolio_ratio", mode="before")
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return _exact_notification_decimal(value, field=info.field_name or "position value")

    @model_validator(mode="after")
    def _valid_values(self) -> PaperNotificationPosition:
        if self.quantity < 0 or self.quantity != self.quantity.to_integral_value():
            raise ValueError("position quantity must be non-negative whole shares")
        if self.portfolio_ratio is not None and not (
            Decimal(0) <= self.portfolio_ratio <= Decimal(1)
        ):
            raise ValueError("portfolio ratio must be between zero and one")
        return self

    @field_serializer("quantity", "portfolio_ratio", when_used="json")
    def _serialize_values(self, value: Decimal | None) -> str | None:
        return None if value is None else _decimal_text(value)


class PaperOrderNotificationSnapshot(_FrozenNotificationModel):
    """Frozen business facts sufficient to render one order outcome."""

    schema_version: Literal["paper_notification_v1"]
    mode: Literal["internal_paper"]
    order_id: UUID
    agent_cycle_id: UUID | None
    occurred_at: datetime
    symbol: NotificationSymbol
    side: Literal["buy", "sell"]
    status: NotificationStatus
    requested_quantity: Decimal
    approved_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    price: PaperNotificationPrice
    notional: Decimal
    fee: Decimal | None
    cash: PaperNotificationCash
    position: PaperNotificationPosition
    agent_reason: str | None = Field(default=None, min_length=1, max_length=500)
    invalidation: str | None = Field(default=None, min_length=1, max_length=500)
    risk_reason: str | None = Field(default=None, min_length=1, max_length=500)

    @field_validator(
        "requested_quantity",
        "approved_quantity",
        "filled_quantity",
        "remaining_quantity",
        "notional",
        "fee",
        mode="before",
    )
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return _exact_notification_decimal(value, field=info.field_name or "notification value")

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        return _require_utc(value, field="occurred_at")

    @field_serializer(
        "requested_quantity",
        "approved_quantity",
        "filled_quantity",
        "remaining_quantity",
        "notional",
        "fee",
        when_used="json",
    )
    def _serialize_values(self, value: Decimal | None) -> str | None:
        return None if value is None else _decimal_text(value)

    @model_validator(mode="after")
    def _valid_outcome(self) -> PaperOrderNotificationSnapshot:
        quantities = (
            self.requested_quantity,
            self.approved_quantity,
            self.filled_quantity,
            self.remaining_quantity,
        )
        if any(value < 0 or value != value.to_integral_value() for value in quantities):
            raise ValueError("notification quantities must be non-negative whole shares")
        if self.requested_quantity <= 0:
            raise ValueError("requested quantity must be positive")
        if self.approved_quantity > self.requested_quantity:
            raise ValueError("approved quantity must not exceed requested quantity")
        if self.filled_quantity + self.remaining_quantity != self.approved_quantity:
            raise ValueError("filled and remaining quantity must equal approved quantity")
        if self.notional < 0 or self.fee is not None and self.fee < 0:
            raise ValueError("notional and fee must not be negative")

        if self.status == "filled":
            if (
                self.price.kind != "average_fill"
                or self.cash.kind != "actual_after"
                or self.position.kind != "actual_after"
                or self.remaining_quantity != 0
                or self.filled_quantity <= 0
                or self.fee is None
                or self.notional <= 0
            ):
                raise ValueError("filled notification requires actual terminal values")
        elif self.status == "risk_rejected":
            if (
                self.price.kind not in {"reference", "worst_case"}
                or self.cash.kind != "unchanged"
                or self.position.kind != "unchanged"
                or any(
                    value != 0
                    for value in (
                        self.approved_quantity,
                        self.filled_quantity,
                        self.remaining_quantity,
                    )
                )
                or self.fee is not None
                or self.risk_reason is None
                or self.notional <= 0
            ):
                raise ValueError("risk rejection requires unchanged account values")
        else:
            if self.cash.kind != "actual_after" or self.position.kind != "actual_after":
                raise ValueError("cancelled notification requires actual account values")
            if self.filled_quantity > 0 and (
                self.price.kind != "average_fill" or self.fee is None or self.notional <= 0
            ):
                raise ValueError("partially filled cancellation requires fill values")
            if self.filled_quantity == 0 and (
                self.price.kind not in {"reference", "worst_case"}
                or self.fee is not None
                or self.notional != 0
            ):
                raise ValueError("unfilled cancellation requires reference values")
        return self


def notification_snapshot_payload(snapshot: PaperOrderNotificationSnapshot) -> dict[str, object]:
    """Serialize a validated snapshot for the existing outbox payload."""
    return {"notification_snapshot": snapshot.model_dump(mode="json")}


def parse_notification_snapshot(payload: Mapping[str, object]) -> PaperOrderNotificationSnapshot:
    """Parse a closed snapshot without leaking rejected values into the error."""
    raw = payload.get("notification_snapshot")
    if not isinstance(raw, Mapping):
        raise PaperNotificationError("PAPER_NOTIFICATION_INVALID")
    try:
        return PaperOrderNotificationSnapshot.model_validate(dict(raw))
    except (TypeError, ValueError, ValidationError):
        raise PaperNotificationError("PAPER_NOTIFICATION_INVALID") from None
