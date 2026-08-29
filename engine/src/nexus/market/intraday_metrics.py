"""Deterministic, display-only metrics over one intraday source authority."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.providers.base import DeliveryMode, PriceBar
from nexus.quality.market import xnys_session_window

_TWELVE_PLACES = Decimal("0.000000000001")
_QUOTE_STALE_AFTER = timedelta(seconds=45)
_BAR_STALE_AFTER = timedelta(seconds=90)
_SOURCE_BLOCKERS = frozenset(
    {
        "INTRADAY_SOURCE_CONFLICT",
        "INTRADAY_SOURCE_POLICY_UNAVAILABLE",
        "INTRADAY_AUTHORITY_INVALID",
    }
)


class IntradayTradeView(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    last: Decimal
    observed_at: datetime
    available_at: datetime
    quality_flags: tuple[str, ...]

    @field_validator("last", mode="before")
    @classmethod
    def _exact_positive_price(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("intraday prices must not use binary floats")
        if Decimal(value) <= 0:  # type: ignore[arg-type]
            raise ValueError("intraday price must be positive")
        return value

    @field_validator("observed_at", "available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _time_order(self) -> Self:
        if self.available_at < self.observed_at:
            raise ValueError("intraday trade availability precedes observation")
        return self


class IntradayQuoteView(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bid: Decimal
    ask: Decimal
    observed_at: datetime
    available_at: datetime
    quality_flags: tuple[str, ...]

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def _exact_positive_price(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("intraday prices must not use binary floats")
        if Decimal(value) <= 0:  # type: ignore[arg-type]
            raise ValueError("intraday price must be positive")
        return value

    @field_validator("observed_at", "available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _consistent_quote(self) -> Self:
        if self.bid > self.ask:
            raise ValueError("intraday quote is crossed")
        if self.available_at < self.observed_at:
            raise ValueError("intraday quote availability precedes observation")
        return self


class IntradayAuthorityView(BaseModel):
    """One whole-source, point-in-time view; fields are never merged across sources."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    source: str | None
    feed: str | None
    delivery_mode: DeliveryMode | None
    bars: tuple[PriceBar, ...]
    trade: IntradayTradeView | None
    quote: IntradayQuoteView | None
    quality_reasons: tuple[str, ...] = ()


class IntradayMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    return_5m: Decimal | None
    return_30m: Decimal | None
    range_30m: Decimal | None
    spread_bps: Decimal | None
    trade_age_seconds: int | None
    quote_age_seconds: int | None
    bar_age_seconds: int | None
    sample_count: int
    quality_reasons: tuple[str, ...]


def compute_intraday_metrics(
    view: IntradayAuthorityView,
    decision_time: datetime,
) -> IntradayMetrics:
    """Compute exact metrics or explicit nulls; never interpolate or synthesize facts."""
    decision = require_utc(decision_time)
    reasons = set(view.quality_reasons)
    if view.delivery_mode == "unknown":
        reasons.add("INTRADAY_DELIVERY_UNVERIFIED")
    if view.trade is not None:
        reasons.update(view.trade.quality_flags)
    usable, bar_invalid = _usable_bars(view, decision, reasons)
    sample_count = len(usable)
    session = xnys_session_window(decision.date())
    outside_session = session is None or not session.open_at <= decision < session.close_at
    if outside_session:
        reasons.add("INTRADAY_OUTSIDE_SESSION")

    trade_age = _age(view.trade.observed_at, decision) if view.trade is not None else None
    quote_age = _age(view.quote.observed_at, decision) if view.quote is not None else None
    bar_age = _age(usable[-1].observed_at, decision) if usable else None
    if outside_session:
        trade_age = quote_age = bar_age = None

    composite_blocked = bool(reasons & _SOURCE_BLOCKERS)
    bar_blocked = bar_invalid or composite_blocked or outside_session
    return_5m: Decimal | None = None
    return_30m: Decimal | None = None
    range_30m: Decimal | None = None
    if not bar_blocked:
        return_5m = _return_window(usable, 6, reasons, "INTRADAY_RETURN_5M_INCOMPLETE")
        return_30m = _return_window(
            usable,
            31,
            reasons,
            "INTRADAY_RETURN_30M_INCOMPLETE",
        )
        range_30m = _range_window(
            usable,
            30,
            reasons,
            "INTRADAY_RANGE_30M_INCOMPLETE",
        )

    spread_bps: Decimal | None = None
    if not composite_blocked and not outside_session:
        quote = view.quote
        if quote is None:
            reasons.add("INTRADAY_QUOTE_UNAVAILABLE")
        elif quote.available_at > decision or quote.observed_at > decision:
            reasons.add("INTRADAY_QUOTE_NOT_VISIBLE")
        elif quote.quality_flags:
            reasons.add("INTRADAY_QUOTE_QUALITY_FLAGS")
        elif decision - quote.observed_at > _QUOTE_STALE_AFTER:
            reasons.add("INTRADAY_QUOTE_STALE")
        else:
            midpoint = (quote.ask + quote.bid) / Decimal(2)
            spread_bps = _quantize((quote.ask - quote.bid) / midpoint * Decimal(10_000))

    if not outside_session:
        if view.trade is not None and trade_age is not None and trade_age > 45:
            reasons.add("INTRADAY_TRADE_STALE")
        if bar_age is not None and bar_age > int(_BAR_STALE_AFTER.total_seconds()):
            reasons.add("INTRADAY_BAR_STALE")

    return IntradayMetrics(
        return_5m=return_5m,
        return_30m=return_30m,
        range_30m=range_30m,
        spread_bps=spread_bps,
        trade_age_seconds=trade_age,
        quote_age_seconds=quote_age,
        bar_age_seconds=bar_age,
        sample_count=sample_count,
        quality_reasons=_ordered_reasons(reasons),
    )


def _usable_bars(
    view: IntradayAuthorityView,
    decision: datetime,
    reasons: set[str],
) -> tuple[tuple[PriceBar, ...], bool]:
    invalid = False
    visible: list[PriceBar] = []
    for bar in view.bars:
        observed = bar.observed_at
        if bar.instrument_id != view.instrument_id:
            reasons.add("INTRADAY_INSTRUMENT_MISMATCH")
            invalid = True
            continue
        if observed is None:
            reasons.add("INTRADAY_OBSERVED_AT_MISSING")
            invalid = True
            continue
        if bar.available_at > decision or observed > decision:
            reasons.add("INTRADAY_BAR_NOT_VISIBLE")
            invalid = True
            continue
        if observed + timedelta(minutes=1) > decision:
            reasons.add("INTRADAY_BAR_INCOMPLETE")
            invalid = True
            continue
        if bar.volume < 0:
            reasons.add("INTRADAY_VOLUME_INVALID")
            invalid = True
            continue
        if bar.quality_flags:
            reasons.add("INTRADAY_BAR_QUALITY_FLAGS")
            invalid = True
        market_session = xnys_session_window(bar.session)
        if (
            market_session is None
            or not market_session.open_at <= observed < market_session.close_at
        ):
            reasons.add("INTRADAY_BAR_OUTSIDE_SESSION")
            invalid = True
        visible.append(bar)
    ordered = tuple(
        sorted(
            visible,
            key=lambda bar: (_observed(bar), bar.revision),
        )
    )
    times = tuple(_observed(bar) for bar in ordered)
    if len(set(times)) != len(times):
        reasons.add("INTRADAY_TIMESTAMP_DUPLICATE")
        invalid = True
    if any(
        later - earlier != timedelta(minutes=1)
        for earlier, later in zip(times, times[1:], strict=False)
    ):
        reasons.add("INTRADAY_WINDOW_GAP")
        invalid = True
    return ordered, invalid


def _return_window(
    bars: tuple[PriceBar, ...],
    size: int,
    reasons: set[str],
    incomplete_reason: str,
) -> Decimal | None:
    if len(bars) < size:
        reasons.add(incomplete_reason)
        return None
    window = bars[-size:]
    if not _continuous(window):
        reasons.add("INTRADAY_WINDOW_GAP")
        return None
    return _quantize(window[-1].close / window[0].close - Decimal(1))


def _range_window(
    bars: tuple[PriceBar, ...],
    size: int,
    reasons: set[str],
    incomplete_reason: str,
) -> Decimal | None:
    if len(bars) < size:
        reasons.add(incomplete_reason)
        return None
    window = bars[-size:]
    if not _continuous(window):
        reasons.add("INTRADAY_WINDOW_GAP")
        return None
    return _quantize(max(bar.high for bar in window) / min(bar.low for bar in window) - Decimal(1))


def _continuous(bars: tuple[PriceBar, ...]) -> bool:
    return all(
        _observed(later) - _observed(earlier) == timedelta(minutes=1)
        for earlier, later in zip(bars, bars[1:], strict=False)
    )


def _observed(bar: PriceBar) -> datetime:
    if bar.observed_at is None:
        raise AssertionError("usable intraday bar must have observed_at")
    return bar.observed_at


def _age(observed_at: datetime | None, decision: datetime) -> int | None:
    if observed_at is None or observed_at > decision:
        return None
    return int((decision - observed_at).total_seconds())


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_TWELVE_PLACES)


def _ordered_reasons(reasons: set[str]) -> tuple[str, ...]:
    priority = (
        "INTRADAY_AUTHORITY_INVALID",
        "INTRADAY_SOURCE_POLICY_UNAVAILABLE",
        "INTRADAY_SOURCE_CONFLICT",
        "INTRADAY_DELIVERY_UNVERIFIED",
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
        "INTRADAY_OUTSIDE_SESSION",
        "INTRADAY_BAR_NOT_VISIBLE",
        "INTRADAY_BAR_INCOMPLETE",
        "INTRADAY_TIMESTAMP_DUPLICATE",
        "INTRADAY_WINDOW_GAP",
        "INTRADAY_BAR_OUTSIDE_SESSION",
        "INTRADAY_BAR_QUALITY_FLAGS",
        "INTRADAY_QUOTE_STALE",
        "INTRADAY_TRADE_STALE",
        "INTRADAY_BAR_STALE",
    )
    return tuple(reason for reason in priority if reason in reasons) + tuple(
        sorted(reasons.difference(priority))
    )
