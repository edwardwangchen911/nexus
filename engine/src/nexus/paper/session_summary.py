"""Versioned, frozen end-of-session facts for the internal-paper notification."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, ValidationInfo, field_serializer, field_validator, model_validator

from nexus.paper.notification import (
    _decimal_text,
    _exact_notification_decimal,
    _FrozenNotificationModel,
    _require_utc,
)


class PaperSessionSummarySnapshot(_FrozenNotificationModel):
    schema_version: Literal["paper_session_summary_v1"]
    mode: Literal["internal_paper"]
    account_id: UUID
    market: Literal["XNYS"]
    session_date: date
    as_of: datetime
    frozen_at: datetime
    currency: Literal["USD"]
    cycle_count: int = Field(gt=0, strict=True)
    completed_count: int = Field(ge=0, strict=True)
    unavailable_count: int = Field(ge=0, strict=True)
    pending_count: int = Field(ge=0, strict=True)
    no_order_count: int = Field(ge=0, strict=True)
    order_count: int = Field(ge=0, strict=True)
    fill_count: int = Field(ge=0, strict=True)
    risk_rejected_count: int = Field(ge=0, strict=True)
    nav: Decimal = Field(gt=0)
    daily_pnl: Decimal | None
    daily_return_ratio: Decimal | None
    daily_fees: Decimal = Field(ge=0)
    drawdown_ratio: Decimal = Field(ge=0, le=1)
    comparison_start: date
    hold_current_nav: Decimal | None
    hold_current_return_ratio: Decimal | None
    spy_nav: Decimal | None
    spy_return_ratio: Decimal | None

    @field_validator(
        "nav",
        "daily_pnl",
        "daily_return_ratio",
        "daily_fees",
        "drawdown_ratio",
        "hold_current_nav",
        "hold_current_return_ratio",
        "spy_nav",
        "spy_return_ratio",
        mode="before",
    )
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal | None:
        return (
            None
            if value is None
            else _exact_notification_decimal(value, field=info.field_name or "summary value")
        )

    @field_serializer(
        "nav",
        "daily_pnl",
        "daily_return_ratio",
        "daily_fees",
        "drawdown_ratio",
        "hold_current_nav",
        "hold_current_return_ratio",
        "spy_nav",
        "spy_return_ratio",
        when_used="json",
    )
    def _serialize_values(self, value: Decimal | None) -> str | None:
        return None if value is None else _decimal_text(value)

    @field_validator("as_of", "frozen_at")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        return _require_utc(value, field="as_of")

    @model_validator(mode="after")
    def _consistent_counts(self) -> "PaperSessionSummarySnapshot":
        if (
            self.frozen_at < self.as_of
            or self.as_of.astimezone(ZoneInfo("America/New_York")).date() != self.session_date
        ):
            raise ValueError("summary time bounds are inconsistent")
        if (
            self.completed_count + self.unavailable_count + self.pending_count != self.cycle_count
            or self.no_order_count > self.completed_count
            or self.risk_rejected_count > self.order_count
        ):
            raise ValueError("summary counts are inconsistent")
        return self
