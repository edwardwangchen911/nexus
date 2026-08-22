"""Provider-neutral market data contracts."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, NewType, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.domain.time import require_utc

InstrumentId = NewType("InstrumentId", UUID)


class BarsRequest(BaseModel):
    """One bounded request for an instrument's bars."""

    model_config = ConfigDict(frozen=True)

    instrument_id: InstrumentId
    start: datetime
    end: datetime
    interval: Literal["1Min", "1Day"]
    adjustment: Literal["raw", "split", "all"]

    @field_validator("start", "end")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class PriceBar(BaseModel):
    """Point-in-time normalized OHLCV record with exact decimal values."""

    model_config = ConfigDict(frozen=True)

    instrument_id: InstrumentId
    session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    available_at: datetime
    observed_at: datetime | None = None
    received_at: datetime | None = None
    revision: str = "1"
    quality_flags: tuple[str, ...] = ()

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("financial values must not be binary floats")
        return value

    @field_validator("available_at", "observed_at", "received_at")
    @classmethod
    def _bar_aware_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class MarketDataProvenance(BaseModel):
    """Inseparable provider identity and normalization contract for one page."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Literal["fosun", "alpaca", "fixture"]
    feed: str
    adjustment: Literal["raw", "split", "all"]
    interval: Literal["1Min", "1Day"]
    mode: Literal["fixture", "read_only", "delayed"]


class ProviderPage[T](BaseModel):
    """A recoverable provider page plus its exact raw archive payload."""

    model_config = ConfigDict(frozen=True)

    items: tuple[T, ...]
    next_cursor: str | None
    raw_payload: bytes = b""
    source_request_id: str | None = None
    as_of: datetime | None = None
    provenance: MarketDataProvenance

    @field_validator("as_of")
    @classmethod
    def _page_aware_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class PriceSeries(BaseModel):
    """A complete, never-field-merged series from one explicit source."""

    model_config = ConfigDict(frozen=True)

    source: str
    feed: str
    adjustment: str
    bars: tuple[PriceBar, ...]
    interval: Literal["1Min", "1Day"] = "1Day"
    mode: Literal["fixture", "read_only", "delayed"]
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    contract_hash: str | None = None


class MarketDataProvider(Protocol):
    """Only interface the ingestion layer uses for historical market bars."""

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None = None
    ) -> ProviderPage[PriceBar]: ...
