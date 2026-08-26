"""Pure deterministic fill calculations for the internal paper account."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    Decimal,
    localcontext,
)
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.paper.models import PaperOrder, exact_decimal

_QUANTUM = Decimal("0.000000000001")
_BPS = Decimal("10000")


def _exact(value: object, *, field: str) -> Decimal:
    return exact_decimal(value, field=field)


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value


def _quantize(value: Decimal, *, rounding: str = ROUND_HALF_EVEN) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return value.quantize(_QUANTUM, rounding=rounding)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExecutionModel(_FrozenModel):
    version: Literal["execution_model_v1"]
    schema_version: Literal["paper_execution_model_v1"]
    bar_interval: Literal["1Min"]
    window_minutes: Literal[5]
    first_evaluation_offset_minutes: Literal[25]
    missing_data_cancel_offset_minutes: Literal[45]
    volume_participation: Decimal
    price_method: Literal["close_volume_vwap_v1"]
    quoted_slippage_bps: Literal[10]
    unquoted_slippage_bps: Literal[25]
    commission_bps: Literal[0]
    regulatory_fee_policy: Literal["zero_v1"]
    cash_reserve_markup_bps: Literal[100]
    remainder_cancel: Literal["calendar_market_close"]

    @field_validator("volume_participation", mode="before")
    @classmethod
    def _participation_is_exact(cls, value: object) -> Decimal:
        result = _exact(value, field="volume participation")
        if not Decimal(0) < result <= Decimal(1):
            raise ValueError("volume participation must be between zero and one")
        return result


EXECUTION_MODEL_V1 = ExecutionModel(
    version="execution_model_v1",
    schema_version="paper_execution_model_v1",
    bar_interval="1Min",
    window_minutes=5,
    first_evaluation_offset_minutes=25,
    missing_data_cancel_offset_minutes=45,
    volume_participation=Decimal("0.01"),
    price_method="close_volume_vwap_v1",
    quoted_slippage_bps=10,
    unquoted_slippage_bps=25,
    commission_bps=0,
    regulatory_fee_policy="zero_v1",
    cash_reserve_markup_bps=100,
    remainder_cancel="calendar_market_close",
)


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def execution_model_hash(model: ExecutionModel) -> str:
    """Return the canonical content address for a complete execution model."""
    return hashlib.sha256(_canonical_json(model.model_dump(mode="json"))).hexdigest()


class ExecutionCalendarSession(_FrozenModel):
    session: date
    market_open_at: datetime
    market_close_at: datetime

    @field_validator("market_open_at", "market_close_at")
    @classmethod
    def _times_are_utc(cls, value: datetime, info: object) -> datetime:
        field_name = getattr(info, "field_name", "calendar time")
        return _utc(value, field=field_name)

    @model_validator(mode="after")
    def _bounds_are_ordered(self) -> ExecutionCalendarSession:
        if self.market_close_at <= self.market_open_at:
            raise ValueError("market close must be after market open")
        new_york = ZoneInfo("America/New_York")
        if (
            self.market_open_at.astimezone(new_york).date() != self.session
            or self.market_close_at.astimezone(new_york).date() != self.session
        ):
            raise ValueError("calendar bounds must belong to the New York session date")
        return self


class ExecutionBar(_FrozenModel):
    bar_id: UUID
    authority_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    observed_at: datetime
    available_at: datetime
    close: Decimal
    volume: Decimal

    @field_validator("observed_at", "available_at")
    @classmethod
    def _times_are_utc(cls, value: datetime, info: object) -> datetime:
        field_name = getattr(info, "field_name", "bar time")
        return _utc(value, field=field_name)

    @field_validator("close", "volume", mode="before")
    @classmethod
    def _numbers_are_exact(cls, value: object, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "bar value")
        return _exact(value, field=field_name)

    @model_validator(mode="after")
    def _bar_is_valid(self) -> ExecutionBar:
        if self.close <= 0:
            raise ValueError("bar close must be positive")
        if self.volume < 0:
            raise ValueError("bar volume must be nonnegative")
        if self.available_at < self.observed_at:
            raise ValueError("bar cannot be available before it is observed")
        return self


class ExecutionQuote(_FrozenModel):
    quote_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    feed: str = Field(min_length=1, max_length=100)
    bid: Decimal
    ask: Decimal
    observed_at: datetime
    available_at: datetime
    revision: str = Field(min_length=1, max_length=255)
    quality_flags: tuple[str, ...]

    @field_validator("observed_at", "available_at")
    @classmethod
    def _times_are_utc(cls, value: datetime, info: object) -> datetime:
        field_name = getattr(info, "field_name", "quote time")
        return _utc(value, field=field_name)

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def _prices_are_exact(cls, value: object, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "quote price")
        return _exact(value, field=field_name)


class QualifiedExecutionWindow(_FrozenModel):
    authority_id: UUID
    instrument_id: UUID
    source: str
    feed: str
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    bar_ids: tuple[UUID, ...]
    window_start_at: datetime
    window_end_at: datetime
    evaluated_at: datetime
    data_available_at: datetime
    vwap: Decimal
    volume: Decimal
    capacity: Decimal
    quote_id: UUID | None
    quote_revision: str | None
    bid: Decimal | None
    ask: Decimal | None
    half_spread: Decimal
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "vwap",
        "volume",
        "capacity",
        "bid",
        "ask",
        "half_spread",
        mode="before",
    )
    @classmethod
    def _numbers_are_exact(cls, value: object, info: object) -> Decimal | None:
        if value is None:
            return None
        field_name = getattr(info, "field_name", "window value")
        return _exact(value, field=field_name)


class FillCalculation(_FrozenModel):
    order_id: UUID
    account_id: UUID
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: Decimal
    price: Decimal
    notional: Decimal
    window_vwap: Decimal
    half_spread: Decimal
    slippage_bps: int
    slippage_amount: Decimal
    commission: Decimal
    regulatory_fee: Decimal
    fee: Decimal
    source_authority_id: UUID
    execution_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    window_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "quantity",
        "price",
        "notional",
        "window_vwap",
        "half_spread",
        "slippage_amount",
        "commission",
        "regulatory_fee",
        "fee",
        mode="before",
    )
    @classmethod
    def _numbers_are_exact(cls, value: object, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "fill value")
        return _exact(value, field=field_name)


def _qualified_quote(
    quote: ExecutionQuote | None,
    *,
    first: ExecutionBar,
    window_start: datetime,
    window_end: datetime,
    evaluated_at: datetime,
) -> ExecutionQuote | None:
    if quote is None:
        return None
    if (
        quote.instrument_id != first.instrument_id
        or quote.source != first.source
        or quote.feed != first.feed
        or not window_start <= quote.observed_at < window_end
        or quote.available_at > evaluated_at
        or quote.available_at < quote.observed_at
        or quote.quality_flags
        or quote.bid <= 0
        or quote.ask < quote.bid
    ):
        return None
    return quote


def qualify_execution_window(
    *,
    model: ExecutionModel,
    calendar: ExecutionCalendarSession,
    bars: tuple[ExecutionBar, ...],
    quote: ExecutionQuote | None,
    evaluated_at: datetime,
) -> QualifiedExecutionWindow:
    """Validate and content-address one complete five-minute fill window."""
    evaluated_at = _utc(evaluated_at, field="evaluated_at")
    if len(bars) != model.window_minutes:
        raise ValueError("execution window requires exactly five bars")
    first = bars[0]
    window_start = first.observed_at
    window_end = window_start + timedelta(minutes=model.window_minutes)
    expected_times = tuple(
        window_start + timedelta(minutes=index) for index in range(model.window_minutes)
    )
    if tuple(bar.observed_at for bar in bars) != expected_times:
        raise ValueError("execution bars must be continuous and ordered")
    if window_start < calendar.market_open_at or window_end > calendar.market_close_at:
        raise ValueError("execution window is outside the calendar session")
    first_evaluation_at = calendar.market_open_at + timedelta(
        minutes=model.first_evaluation_offset_minutes
    )
    if evaluated_at < first_evaluation_at:
        raise ValueError("execution window is before the first evaluation time")
    if any(bar.available_at > evaluated_at for bar in bars):
        raise ValueError("execution bar is not yet available")
    authority_scope = {
        (
            bar.authority_id,
            bar.instrument_id,
            bar.source,
            bar.feed,
            bar.delivery_mode,
        )
        for bar in bars
    }
    if len(authority_scope) != 1:
        raise ValueError("execution bars must share one authority and source scope")
    total_volume = sum((bar.volume for bar in bars), Decimal(0))
    if total_volume <= 0:
        raise ValueError("execution window requires positive volume")
    with localcontext() as context:
        context.prec = 50
        vwap = _quantize(
            sum((bar.close * bar.volume for bar in bars), Decimal(0)) / total_volume
        )
        capacity = _quantize(
            (total_volume * model.volume_participation).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
    accepted_quote = _qualified_quote(
        quote,
        first=first,
        window_start=window_start,
        window_end=window_end,
        evaluated_at=evaluated_at,
    )
    half_spread = (
        _quantize((accepted_quote.ask - accepted_quote.bid) / Decimal(2))
        if accepted_quote is not None
        else Decimal("0.000000000000")
    )
    data_available_at = max(bar.available_at for bar in bars)
    if accepted_quote is not None:
        data_available_at = max(data_available_at, accepted_quote.available_at)
    payload = {
        "schema": "paper_execution_window_sha256_v1",
        "authority_id": str(first.authority_id).lower(),
        "instrument_id": str(first.instrument_id).lower(),
        "source": first.source,
        "feed": first.feed,
        "delivery_mode": first.delivery_mode,
        "bars": [
            {
                "bar_id": str(bar.bar_id).lower(),
                "observed_at": bar.observed_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "available_at": bar.available_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "close": format(_quantize(bar.close), ".12f"),
                "volume": format(_quantize(bar.volume), ".12f"),
            }
            for bar in bars
        ],
        "evaluated_at": evaluated_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "vwap": format(vwap, ".12f"),
        "volume": format(_quantize(total_volume), ".12f"),
        "capacity": format(capacity, ".12f"),
        "quote": (
            {
                "quote_id": str(accepted_quote.quote_id).lower(),
                "revision": accepted_quote.revision,
                "bid": format(_quantize(accepted_quote.bid), ".12f"),
                "ask": format(_quantize(accepted_quote.ask), ".12f"),
                "observed_at": accepted_quote.observed_at.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "available_at": accepted_quote.available_at.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
            }
            if accepted_quote is not None
            else None
        ),
    }
    return QualifiedExecutionWindow(
        authority_id=first.authority_id,
        instrument_id=first.instrument_id,
        source=first.source,
        feed=first.feed,
        delivery_mode=first.delivery_mode,
        bar_ids=tuple(bar.bar_id for bar in bars),
        window_start_at=window_start,
        window_end_at=window_end,
        evaluated_at=evaluated_at,
        data_available_at=data_available_at,
        vwap=vwap,
        volume=_quantize(total_volume),
        capacity=capacity,
        quote_id=accepted_quote.quote_id if accepted_quote is not None else None,
        quote_revision=accepted_quote.revision if accepted_quote is not None else None,
        bid=_quantize(accepted_quote.bid) if accepted_quote is not None else None,
        ask=_quantize(accepted_quote.ask) if accepted_quote is not None else None,
        half_spread=half_spread,
        content_hash=hashlib.sha256(_canonical_json(payload)).hexdigest(),
    )


def _execution_price(
    *,
    model: ExecutionModel,
    side: Literal["buy", "sell"],
    window: QualifiedExecutionWindow,
    tick_size: Decimal | None,
) -> tuple[Decimal, int, Decimal]:
    slippage_bps = (
        model.quoted_slippage_bps
        if window.quote_id is not None
        else model.unquoted_slippage_bps
    )
    with localcontext() as context:
        context.prec = 50
        directional_cost = window.half_spread + (
            window.vwap * Decimal(slippage_bps) / _BPS
        )
        raw_price = (
            window.vwap + directional_cost
            if side == "buy"
            else window.vwap - directional_cost
        )
        if raw_price <= 0:
            raise ValueError("execution cost would create a nonpositive price")
        increment = (
            _exact(tick_size, field="tick_size")
            if tick_size is not None
            else _QUANTUM
        )
        if increment <= 0:
            raise ValueError("tick_size must be positive")
        units = raw_price / increment
        rounding = ROUND_CEILING if side == "buy" else ROUND_FLOOR
        price = _quantize(units.to_integral_value(rounding=rounding) * increment)
        return price, slippage_bps, directional_cost


def calculate_fill(
    *,
    model: ExecutionModel,
    order: PaperOrder,
    existing_filled_quantity: Decimal,
    reservation_outstanding: Decimal,
    window: QualifiedExecutionWindow,
    tick_size: Decimal | None,
) -> FillCalculation:
    """Calculate one bounded fill; a zero quantity is a deterministic no-op."""
    existing = _exact(existing_filled_quantity, field="existing filled quantity")
    reservation = _exact(reservation_outstanding, field="reservation outstanding")
    if existing < 0 or existing > order.quantity:
        raise ValueError("existing fill quantity is outside the order")
    if reservation < 0:
        raise ValueError("reservation outstanding must be nonnegative")
    if window.instrument_id != order.instrument_id:
        raise ValueError("execution window belongs to another instrument")
    price, slippage_bps, directional_cost = _execution_price(
        model=model,
        side=order.side,
        window=window,
        tick_size=tick_size,
    )
    remaining = order.quantity - existing
    reservation_capacity = (
        (reservation / price).to_integral_value(rounding=ROUND_FLOOR)
        if order.side == "buy"
        else reservation.to_integral_value(rounding=ROUND_FLOOR)
    )
    quantity = _quantize(min(remaining, window.capacity, reservation_capacity))
    if quantity < 0:
        raise ValueError("fill quantity cannot be negative")
    notional = _quantize(quantity * price)
    commission = Decimal("0.000000000000")
    regulatory_fee = Decimal("0.000000000000")
    fee = commission + regulatory_fee
    return FillCalculation(
        order_id=order.order_id,
        account_id=order.account_id,
        instrument_id=order.instrument_id,
        side=order.side,
        quantity=quantity,
        price=price,
        notional=notional,
        window_vwap=window.vwap,
        half_spread=window.half_spread,
        slippage_bps=slippage_bps,
        slippage_amount=_quantize(quantity * directional_cost),
        commission=commission,
        regulatory_fee=regulatory_fee,
        fee=fee,
        source_authority_id=window.authority_id,
        execution_model_hash=execution_model_hash(model),
        window_content_hash=window.content_hash,
    )
