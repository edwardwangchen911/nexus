"""Source-aware market and private watchlist wire contracts."""

from datetime import UTC, date, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from pydantic import BaseModel, ConfigDict, field_validator


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MarketItemResponse(_StrictSchema):
    """One point-in-time instrument row with explicit source quality."""

    instrument_id: UUID
    symbol: str
    price: DecimalString | None
    source: str | None
    feed: str | None
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
    adjustment: str | None
    interval: Literal["1Day"] | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    quality_reasons: tuple[str, ...]
    bars: tuple[MarketBarResponse, ...]


class MarketPageResponse(_StrictSchema):
    """A bounded deterministic page plus one optional chart series."""

    items: tuple[MarketItemResponse, ...]
    next_cursor: str | None
    selected_series: SelectedMarketSeriesResponse | None


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
