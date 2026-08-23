"""Deterministic point-in-time price revision selection."""

from collections.abc import Sequence

from nexus.providers.base import PriceBar, PriceSeries


def canonical_price_bars(
    series: PriceSeries, bars: Sequence[PriceBar]
) -> tuple[PriceBar, ...]:
    """Select the latest stable revision for each series observation identity."""
    latest: dict[tuple[object, ...], PriceBar] = {}
    for bar in bars:
        identity: tuple[object, ...] = (bar.instrument_id, bar.session)
        if series.interval == "1Min":
            identity += (bar.observed_at,)
        current = latest.get(identity)
        if current is None or price_revision_key(bar) > price_revision_key(current):
            latest[identity] = bar
    return tuple(sorted(latest.values(), key=price_bar_sort_key))


def price_revision_key(bar: PriceBar) -> tuple[object, ...]:
    return (
        bar.available_at,
        bar.received_at or bar.available_at,
        bar.observed_at or bar.available_at,
        bar.revision,
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        tuple(sorted(bar.quality_flags)),
    )


def price_bar_sort_key(bar: PriceBar) -> tuple[object, ...]:
    return (
        str(bar.instrument_id),
        bar.session.isoformat(),
        bar.observed_at.isoformat() if bar.observed_at else "",
        bar.available_at.isoformat(),
        bar.received_at.isoformat() if bar.received_at else "",
        bar.revision,
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        tuple(sorted(bar.quality_flags)),
    )
