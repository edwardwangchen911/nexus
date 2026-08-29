"""Immutable, provider-normalized intraday Snapshot contracts."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar

_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


def _exact_decimal(value: object) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("financial values must not be binary floats")
    if not isinstance(value, Decimal | int | str):
        raise TypeError("financial values must be Decimal, int, or string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except Exception as error:
        raise ValueError("financial value must be decimal") from error
    if not result.is_finite():
        raise ValueError("financial value must be finite")
    return result


class SnapshotInstrument(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: InstrumentId
    symbol: str

    @field_validator("symbol")
    @classmethod
    def _safe_symbol(cls, value: str) -> str:
        if _SYMBOL.fullmatch(value) is None:
            raise ValueError("snapshot symbol is invalid")
        return value


class IntradaySnapshotRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instruments: tuple[SnapshotInstrument, ...]
    feed: Literal["iex", "sip", "delayed_sip"]
    delivery_mode: Literal["real_time", "delayed"]

    @model_validator(mode="after")
    def _bounded_and_consistent(self) -> IntradaySnapshotRequest:
        if not 1 <= len(self.instruments) <= 30:
            raise ValueError("snapshot instruments must contain 1 to 30 items")
        ids = tuple(item.instrument_id for item in self.instruments)
        symbols = tuple(item.symbol for item in self.instruments)
        if len(set(ids)) != len(ids) or len(set(symbols)) != len(symbols):
            raise ValueError("snapshot instruments must be unique")
        expected = "delayed" if self.feed == "delayed_sip" else "real_time"
        if self.delivery_mode != expected:
            raise ValueError("snapshot feed contradicts delivery mode")
        return self


class SnapshotTrade(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    price: Decimal
    observed_at: datetime
    provider_trade_id: str | None = None

    @field_validator("price", mode="before")
    @classmethod
    def _price_decimal(cls, value: object) -> Decimal:
        result = _exact_decimal(value)
        if result <= 0:
            raise ValueError("trade price must be positive")
        return result

    @field_validator("observed_at")
    @classmethod
    def _observed_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class SnapshotQuote(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bid: Decimal
    ask: Decimal
    observed_at: datetime

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def _quote_decimal(cls, value: object) -> Decimal:
        result = _exact_decimal(value)
        if result <= 0:
            raise ValueError("quote prices must be positive")
        return result

    @field_validator("observed_at")
    @classmethod
    def _observed_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _spread_order(self) -> SnapshotQuote:
        if self.bid > self.ask:
            raise ValueError("quote bid must not exceed ask")
        return self


class SnapshotMinuteBar(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    interval: Literal["1Min"] = "1Min"
    bar: PriceBar


class MarketSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: InstrumentId
    symbol: str
    trade: SnapshotTrade | None
    quote: SnapshotQuote | None
    minute_bar: SnapshotMinuteBar | None


class RawSnapshotEnvelope(BaseModel):
    """Transport-complete bytes captured before any payload interpretation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_payload: bytes
    received_at: datetime
    provenance: MarketDataProvenance

    @field_validator("received_at")
    @classmethod
    def _received_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class SnapshotBatch(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    snapshots: tuple[MarketSnapshot, ...]
    raw_payload: bytes
    received_at: datetime
    provenance: MarketDataProvenance

    @field_validator("received_at")
    @classmethod
    def _received_utc(cls, value: datetime) -> datetime:
        return require_utc(value)
