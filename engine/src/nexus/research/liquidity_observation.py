"""Public close-times-volume proxy on a fixed window, without trading authority."""

from collections.abc import Sequence
from datetime import date
from decimal import Decimal, localcontext
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.domain.decimal import DecimalString, decimal_to_string
from nexus.providers.base import PriceBar


class LiquidityObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    method: Literal["mean_20_session_close_times_volume"]
    status: Literal["available", "insufficient_sessions", "missing_sessions"]
    value: DecimalString | None
    currency: Literal["USD"]
    unit: Literal["USD_per_trading_session"]
    window_sessions: Literal[20]
    observed_sessions: int = Field(ge=0, le=20)
    window_start: date
    window_end: date
    missing_sessions: tuple[date, ...] = Field(max_length=20)

    @model_validator(mode="after")
    def consistent_window(self) -> Self:
        if (
            self.window_start > self.window_end
            or self.missing_sessions != tuple(sorted(set(self.missing_sessions)))
            or any(not self.window_start <= day <= self.window_end for day in self.missing_sessions)
            or self.observed_sessions + len(self.missing_sessions) > 20
        ):
            raise ValueError("LIQUIDITY_WINDOW_INVALID")
        if self.status == "available":
            if (
                self.value is None
                or Decimal(self.value) < 0
                or self.observed_sessions != 20
                or self.missing_sessions
            ):
                raise ValueError("LIQUIDITY_VALUE_INVALID")
        elif self.value is not None or self.observed_sessions >= 20:
            raise ValueError("LIQUIDITY_INCOMPLETE_VALUE")
        if self.status == "missing_sessions" and (
            not self.missing_sessions or self.observed_sessions + len(self.missing_sessions) != 20
        ):
            raise ValueError("LIQUIDITY_WINDOW_INVALID")
        if (
            self.status == "insufficient_sessions"
            and self.observed_sessions + len(self.missing_sessions) >= 20
        ):
            raise ValueError("LIQUIDITY_WINDOW_INVALID")
        return self


def calculate_liquidity_observation(
    bars: Sequence[PriceBar], sessions: Sequence[date]
) -> LiquidityObservation:
    expected = tuple(sessions)
    by_session = {bar.session: bar for bar in bars}
    if (
        not expected
        or expected != tuple(sorted(set(expected)))
        or len(by_session) != len(bars)
        or not set(by_session) <= set(expected)
    ):
        raise ValueError("LIQUIDITY_WINDOW_INVALID")
    window = expected[-20:]
    sample = [by_session[day] for day in window if day in by_session]
    missing = tuple(day for day in window if day not in by_session)
    for bar in sample:
        decimal_to_string(bar.close)
        decimal_to_string(bar.volume)
        if bar.close <= 0 or bar.volume < 0 or bar.quality_flags:
            raise ValueError("LIQUIDITY_BARS_INVALID")
    value = None
    status: Literal["available", "insufficient_sessions", "missing_sessions"] = "available"
    if len(window) < 20:
        status = "insufficient_sessions"
    elif missing:
        status = "missing_sessions"
    else:
        # Each input is bounded to 40 decimal characters; retain product/sum precision.
        with localcontext() as context:
            context.prec = 100
            value = decimal_to_string(
                sum((bar.close * bar.volume for bar in sample), Decimal(0)) / 20
            )
    return LiquidityObservation(
        method="mean_20_session_close_times_volume",
        currency="USD",
        unit="USD_per_trading_session",
        window_sessions=20,
        status=status,
        value=value,
        observed_sessions=len(sample),
        window_start=window[0],
        window_end=window[-1],
        missing_sessions=missing,
    )
