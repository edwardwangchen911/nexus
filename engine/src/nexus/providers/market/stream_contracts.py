"""Provider-neutral observations emitted by bounded real-time market streams."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class MarketStreamObservation:
    source: Literal["schwab"]
    symbol: str
    received_at: datetime
    sequence: int | None
    quote_time: datetime | None = None
    trade_time: datetime | None = None
    bid_time: datetime | None = None
    ask_time: datetime | None = None
    provider_time_unit: Literal["seconds", "milliseconds"] | None = None
    delayed: bool | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    last_size: Decimal | None = None
    total_volume: Decimal | None = None
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    last_exchange: str | None = None
    bid_mic_id: str | None = None
    ask_mic_id: str | None = None
    last_mic_id: str | None = None
