"""Source-aware market and private watchlist wire contracts."""

from datetime import UTC, date, datetime
from typing import Annotated, Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.providers.base import DeliveryMode
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

QualityReason = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]{0,99}$")]


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MarketItemResponse(_StrictSchema):
    """One point-in-time instrument row with explicit source quality."""

    instrument_id: UUID
    symbol: str
    price: DecimalString | None
    source: str | None
    feed: str | None
    delivery_mode: DeliveryMode
    as_of: datetime | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    quality_reasons: tuple[str, ...]
    watchlisted: bool

    @field_validator("as_of")
    @classmethod
    def _aware_as_of(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime must be timezone-aware")
        return None if value is None else value.astimezone(UTC)


class MarketBarResponse(_StrictSchema):
    """Exact selected-source OHLCV evidence for a semantic chart table."""

    session: date
    open: DecimalString
    high: DecimalString
    low: DecimalString
    close: DecimalString
    volume: DecimalString
    available_at: datetime
    observed_at: datetime | None
    quality_flags: tuple[str, ...]

    @field_validator("available_at", "observed_at")
    @classmethod
    def _aware_bar_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime must be timezone-aware")
        return None if value is None else value.astimezone(UTC)


class SelectedMarketSeriesResponse(_StrictSchema):
    """At most one repository-selected historical series in a market page."""

    instrument_id: UUID
    source: str | None
    feed: str | None
    delivery_mode: DeliveryMode | None
    adjustment: str | None
    interval: Literal["1Day"] | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    quality_reasons: tuple[str, ...]
    bars: tuple[MarketBarResponse, ...]


class IntradayBarResponse(_StrictSchema):
    """One complete 1Min fact from the selected intraday source authority."""

    observed_at: datetime
    open: DecimalString
    high: DecimalString
    low: DecimalString
    close: DecimalString
    volume: DecimalString
    available_at: datetime
    quality_flags: tuple[QualityReason, ...] = Field(max_length=16)

    @field_validator("observed_at", "available_at")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class IntradayMonitorResponse(_StrictSchema):
    """Display-only facts and deterministic metrics for one selected instrument."""

    instrument_id: UUID
    status: Literal["ready", "partial", "stale", "outside_session", "unavailable"]
    source: str | None
    feed: str | None
    delivery_mode: DeliveryMode | None
    last: DecimalString | None
    bid: DecimalString | None
    ask: DecimalString | None
    trade_as_of: datetime | None
    quote_as_of: datetime | None
    bar_as_of: datetime | None
    return_5m: DecimalString | None
    return_30m: DecimalString | None
    range_30m: DecimalString | None
    spread_bps: DecimalString | None
    quote_age_seconds: int | None = Field(default=None, ge=0)
    bar_age_seconds: int | None = Field(default=None, ge=0)
    sample_count: int = Field(ge=0, le=90)
    quality_reasons: tuple[QualityReason, ...] = Field(max_length=32)
    bars: tuple[IntradayBarResponse, ...] = Field(max_length=60)

    @field_validator("trade_as_of", "quote_as_of", "bar_as_of")
    @classmethod
    def _aware_fact_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime must be timezone-aware")
        return None if value is None else value.astimezone(UTC)


class MarketPageResponse(_StrictSchema):
    """A bounded deterministic page plus one optional chart series."""

    items: tuple[MarketItemResponse, ...]
    next_cursor: str | None
    selected_series: SelectedMarketSeriesResponse | None
    intraday_monitor: IntradayMonitorResponse | None


class MarketPageEnvelope(_StrictSchema):
    data: MarketPageResponse
    meta: SuccessMetaResponse


class WatchlistMembershipResponse(_StrictSchema):
    """Final set representation shared by idempotent PUT and DELETE."""

    instrument_id: UUID
    watchlisted: bool


class WatchlistMembershipEnvelope(_StrictSchema):
    data: WatchlistMembershipResponse
    meta: SuccessMetaResponse
