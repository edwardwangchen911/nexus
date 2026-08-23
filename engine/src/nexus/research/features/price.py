"""Exact price features with a Decimal boundary."""

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal

import polars as pl

from nexus.domain.time import require_utc
from nexus.providers.base import PriceBar


def momentum_126_skip5(closes: Sequence[Decimal]) -> Decimal | None:
    """Return close[t-5] / close[t-126] - 1 for a complete 127-close window."""
    if len(closes) < 127:
        return None
    denominator = closes[-127]
    if denominator == 0:
        return None
    return closes[-6] / denominator - Decimal(1)


def point_in_time_closes(
    bars: Sequence[PriceBar], decision_time: datetime
) -> tuple[Decimal, ...]:
    """Filter on available_at before using Polars only for deterministic ordering."""
    decision_time = require_utc(decision_time)
    visible = [
        bar
        for bar in bars
        if bar.available_at <= decision_time and bar.session <= decision_time.date()
    ]
    if not visible:
        return ()
    latest_by_session: dict[date, PriceBar] = {}
    for bar in visible:
        prior = latest_by_session.get(bar.session)
        bar_key = (bar.available_at, bar.received_at or bar.available_at, bar.revision)
        if prior is None:
            latest_by_session[bar.session] = bar
            continue
        prior_key = (
            prior.available_at,
            prior.received_at or prior.available_at,
            prior.revision,
        )
        if bar_key > prior_key:
            latest_by_session[bar.session] = bar
    latest = list(latest_by_session.values())
    frame = pl.DataFrame(
        {
            "session": [bar.session for bar in latest],
            "close": [str(bar.close) for bar in latest],
        }
    ).sort("session")
    return tuple(Decimal(value) for value in frame.get_column("close").to_list())
