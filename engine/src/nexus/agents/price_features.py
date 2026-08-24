"""Deterministic Decimal features over one frozen daily price series."""

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Literal, cast

from nexus.persistence.models.data import PriceBarRecord

type PriceFeatureName = Literal[
    "close", "sma_20", "sma_200", "rsi_14", "volatility_20"
]

_SCALE = Decimal("0.000000000001")


@dataclass(frozen=True)
class PriceFeatureSet:
    values: dict[str, str]
    missing: tuple[str, ...]


def compute_price_features(
    bars: Sequence[PriceBarRecord], requested: Sequence[str]
) -> PriceFeatureSet:
    """Compute only allowlisted features; insufficient warmup remains explicit."""
    ordered = tuple(sorted(bars, key=lambda bar: (bar.observed_at, bar.id)))
    values: dict[str, str] = {}
    missing: list[str] = []
    for raw_name in requested:
        name = cast(PriceFeatureName, raw_name)
        value = _feature(name, ordered)
        if value is None:
            missing.append(name)
        else:
            values[name] = _format(value)
    return PriceFeatureSet(values=values, missing=tuple(missing))


def _feature(name: PriceFeatureName, bars: tuple[PriceBarRecord, ...]) -> Decimal | None:
    closes = tuple(bar.close for bar in bars)
    if name == "close":
        return None if not closes else closes[-1]
    if name == "sma_20":
        return _sma(closes, 20)
    if name == "sma_200":
        return _sma(closes, 200)
    if name == "rsi_14":
        return _rsi_14(closes)
    return _volatility_20(closes)


def _sma(closes: tuple[Decimal, ...], window: int) -> Decimal | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:], Decimal(0)) / Decimal(window)


def _rsi_14(closes: tuple[Decimal, ...]) -> Decimal | None:
    if len(closes) < 15:
        return None
    changes = tuple(current - previous for previous, current in zip(closes[-15:-1], closes[-14:]))
    gain = sum((max(change, Decimal(0)) for change in changes), Decimal(0))
    loss = sum((max(-change, Decimal(0)) for change in changes), Decimal(0))
    if gain == 0 and loss == 0:
        return Decimal(50)
    if loss == 0:
        return Decimal(100)
    if gain == 0:
        return Decimal(0)
    relative_strength = gain / loss
    return Decimal(100) - Decimal(100) / (Decimal(1) + relative_strength)


def _volatility_20(closes: tuple[Decimal, ...]) -> Decimal | None:
    if len(closes) < 21:
        return None
    window = closes[-21:]
    returns = tuple(
        current / previous - Decimal(1)
        for previous, current in zip(window, window[1:])
    )
    mean = sum(returns, Decimal(0)) / Decimal(20)
    variance = sum(((value - mean) ** 2 for value in returns), Decimal(0)) / Decimal(20)
    with localcontext() as context:
        context.prec = 50
        return variance.sqrt() * Decimal(252).sqrt()


def _format(value: Decimal) -> str:
    return format(value.quantize(_SCALE), "f")
