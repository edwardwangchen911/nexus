"""Immutable whole-series market source selection policy."""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from nexus.domain.time import require_utc
from nexus.providers.base import PriceBar, PriceSeries


class SourcePolicy(BaseModel):
    """Published source_policy_v1 inputs for one instrument decision."""

    model_config = ConfigDict(frozen=True)

    version: Literal["source_policy_v1"] = "source_policy_v1"
    instrument_tick_size: Decimal | None
    priority: tuple[str, ...] = ("fosun", "alpaca", "fixture")

    @classmethod
    def v1(cls, *, instrument_tick_size: Decimal | None) -> "SourcePolicy":
        return cls(instrument_tick_size=instrument_tick_size)


class SourceSelection(BaseModel):
    """One selected series and its order-eligibility quality."""

    model_config = ConfigDict(frozen=True)

    selected: PriceSeries | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    reasons: tuple[str, ...]


def select_price_series(
    candidates: Sequence[PriceSeries],
    policy: SourcePolicy,
    decision_time: datetime,
) -> SourceSelection:
    """Choose exactly one complete point-in-time series under source_policy_v1."""
    decision_time = require_utc(decision_time)
    reasons: set[str] = set()
    eligible: list[PriceSeries] = []
    point_in_time_candidate_seen = False

    for candidate in candidates:
        if candidate.source not in policy.priority or candidate.quality == "FAIL":
            continue
        if candidate.source == "alpaca" and (
            candidate.feed != "sip" or candidate.mode not in {"fixture", "delayed"}
        ):
            continue
        if candidate.source == "fixture" and candidate.mode != "fixture":
            continue
        if candidate.source == "fosun" and (
            candidate.capability_expires_at is None
            or candidate.capability_expires_at < decision_time
        ):
            reasons.add("FOSUN_CAPABILITY_EXPIRED")
            continue
        point_in_time_candidate_seen = True
        visible = tuple(bar for bar in candidate.bars if bar.available_at <= decision_time)
        if not visible:
            continue
        if len(visible) != len(candidate.bars):
            reasons.add("FUTURE_DATA_EXCLUDED")
        eligible.append(candidate.model_copy(update={"bars": visible}))

    if not eligible:
        return SourceSelection(
            selected=None,
            quality="FAIL",
            reasons=(
                "NO_POINT_IN_TIME_DATA" if point_in_time_candidate_seen else "NO_ELIGIBLE_SOURCE",
            ),
        )

    ordered = sorted(eligible, key=lambda series: _series_sort_key(series, policy))
    selected = ordered[0]
    cross_check = _cross_check(ordered, policy)
    if cross_check == "SOURCE_CONFLICT":
        return SourceSelection(selected=None, quality="FAIL", reasons=(cross_check,))
    if cross_check is not None:
        reasons.add(cross_check)

    quality: Literal["PASS", "PARTIAL", "FAIL"] = selected.quality
    if "TICK_SIZE_UNKNOWN" in reasons or "FUTURE_DATA_EXCLUDED" in reasons:
        quality = "PARTIAL"
    return SourceSelection(
        selected=selected,
        quality=quality,
        reasons=_ordered_reasons(reasons),
    )


def _series_sort_key(series: PriceSeries, policy: SourcePolicy) -> tuple[object, ...]:
    try:
        priority = policy.priority.index(series.source)
    except ValueError:
        priority = len(policy.priority)
    bar_key = tuple(
        (
            bar.session.isoformat(),
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
            str(bar.volume),
            bar.available_at.isoformat(),
        )
        for bar in series.bars
    )
    return priority, series.source, series.feed, series.adjustment, bar_key


def _cross_check(ordered: Sequence[PriceSeries], policy: SourcePolicy) -> str | None:
    sources = {series.source: series for series in ordered}
    if "fosun" not in sources or "alpaca" not in sources:
        return None
    primary = sources["fosun"]
    secondary = sources["alpaca"]
    if primary.interval != secondary.interval:
        return "SOURCE_CONFLICT"
    if primary.interval == "1Min":
        return (
            "INTRADAY_DIVERGENCE_ALERT"
            if _relative_close_difference(primary, secondary) > Decimal("0.005")
            else None
        )
    if primary.adjustment != secondary.adjustment:
        return "SOURCE_CONFLICT"
    primary_bars = {bar.session: bar for bar in primary.bars}
    secondary_bars = {bar.session: bar for bar in secondary.bars}
    if primary_bars.keys() != secondary_bars.keys():
        return "SOURCE_CONFLICT"
    if policy.instrument_tick_size is None:
        return "TICK_SIZE_UNKNOWN"
    for session in primary_bars:
        first = primary_bars[session].close
        second = secondary_bars[session].close
        relative_limit = min(abs(first), abs(second)) * Decimal("0.002")
        if abs(first - second) > max(policy.instrument_tick_size, relative_limit):
            return "SOURCE_CONFLICT"
    return None


def _relative_close_difference(first: PriceSeries, second: PriceSeries) -> Decimal:
    first_bar: PriceBar = first.bars[-1]
    second_bar: PriceBar = second.bars[-1]
    denominator = min(abs(first_bar.close), abs(second_bar.close))
    if denominator == 0:
        return Decimal("Infinity") if first_bar.close != second_bar.close else Decimal(0)
    return abs(first_bar.close - second_bar.close) / denominator


def _ordered_reasons(reasons: set[str]) -> tuple[str, ...]:
    order = (
        "FOSUN_CAPABILITY_EXPIRED",
        "FUTURE_DATA_EXCLUDED",
        "TICK_SIZE_UNKNOWN",
        "INTRADAY_DIVERGENCE_ALERT",
    )
    return tuple(reason for reason in order if reason in reasons) + tuple(
        sorted(reasons.difference(order))
    )
