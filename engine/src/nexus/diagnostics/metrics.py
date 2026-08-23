"""Exact portfolio metric primitives; threshold callers receive Decimal or unknown."""

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from math import ceil
from uuid import UUID


def security_hhi(values: Sequence[Decimal]) -> Decimal | None:
    """Return normalized security-only HHI, excluding cash at the call boundary."""
    if any(value < 0 for value in values):
        raise ValueError("security values must not be negative")
    total = sum(values, Decimal(0))
    if total == 0:
        return None
    return sum(((value / total) ** 2 for value in values), Decimal(0))


def top_weights(values: Sequence[Decimal]) -> tuple[Decimal, ...] | None:
    """Return descending security-only weights, leaving cash outside this input."""
    if any(value < 0 for value in values):
        raise ValueError("security values must not be negative")
    total = sum(values, Decimal(0))
    if total == 0:
        return None
    return tuple(sorted((value / total for value in values), reverse=True))


def annualized_volatility(closes: Sequence[Decimal], window: int) -> Decimal | None:
    """Use exact population standard deviation over qualified daily returns."""
    returns = _returns(closes[-(window + 1) :])
    if len(returns) < _coverage_minimum(window):
        return None
    mean = sum(returns, Decimal(0)) / Decimal(len(returns))
    variance = sum(((value - mean) ** 2 for value in returns), Decimal(0)) / Decimal(len(returns))
    return variance.sqrt() * Decimal(252).sqrt()


def maximum_drawdown(closes: Sequence[Decimal], window: int = 252) -> Decimal | None:
    """Return the deepest peak-to-trough loss over sufficiently covered closes."""
    sample = closes[-window:]
    if len(sample) < _coverage_minimum(window) or any(value <= 0 for value in sample):
        return None
    peak = sample[0]
    drawdowns = (
        Decimal(1) - value / (peak := max(peak, value)) for value in sample
    )
    return max(drawdowns, default=Decimal(0))


def beta_and_correlation(
    asset_closes: Sequence[Decimal],
    benchmark_closes: Sequence[Decimal],
    window: int = 126,
    *,
    minimum_returns: int | None = None,
) -> tuple[Decimal, Decimal] | None:
    """Return beta and correlation for aligned exact price observations."""
    count = min(len(asset_closes), len(benchmark_closes), window + 1)
    if count < 2:
        return None
    asset_returns = _returns(asset_closes[-count:])
    benchmark_returns = _returns(benchmark_closes[-count:])
    pairs = tuple(zip(asset_returns, benchmark_returns, strict=True))
    required = minimum_returns if minimum_returns is not None else _coverage_minimum(window)
    if len(pairs) < required:
        return None
    asset_mean = sum((pair[0] for pair in pairs), Decimal(0)) / Decimal(len(pairs))
    benchmark_mean = sum((pair[1] for pair in pairs), Decimal(0)) / Decimal(len(pairs))
    covariance = sum(
        ((asset - asset_mean) * (benchmark - benchmark_mean) for asset, benchmark in pairs),
        Decimal(0),
    ) / Decimal(len(pairs))
    asset_variance = sum(
        ((asset - asset_mean) ** 2 for asset, _ in pairs), Decimal(0)
    ) / Decimal(len(pairs))
    benchmark_variance = sum(
        ((benchmark - benchmark_mean) ** 2 for _, benchmark in pairs), Decimal(0)
    ) / Decimal(len(pairs))
    if asset_variance == 0 or benchmark_variance == 0:
        return None
    correlation = covariance / (asset_variance * benchmark_variance).sqrt()
    return covariance / benchmark_variance, correlation


def beta_and_correlation_by_session(
    asset_closes: Mapping[date, Decimal],
    benchmark_closes: Mapping[date, Decimal],
    window: int = 126,
) -> tuple[Decimal, Decimal] | None:
    """Align returns by their actual sessions before applying the 126-session window."""
    asset_returns = _returns_by_session(asset_closes)
    benchmark_returns = _returns_by_session(benchmark_closes)
    sessions = tuple(sorted(set(asset_returns) & set(benchmark_returns)))[-window:]
    required = _coverage_minimum(window)
    if len(sessions) < required:
        return None
    return _beta_from_pairs(
        tuple((asset_returns[session], benchmark_returns[session]) for session in sessions)
    )


def correlation_components(
    returns_by_instrument: Mapping[UUID, Sequence[Decimal]],
) -> tuple[tuple[UUID, ...], ...]:
    """Build deterministic connected components for pairs correlated at least 0.80."""
    identifiers = tuple(sorted(returns_by_instrument, key=str))
    neighbours: dict[UUID, set[UUID]] = {identifier: set() for identifier in identifiers}
    for index, left in enumerate(identifiers):
        for right in identifiers[index + 1 :]:
            correlation = _correlation(returns_by_instrument[left], returns_by_instrument[right])
            if correlation is not None and correlation >= Decimal("0.80"):
                neighbours[left].add(right)
                neighbours[right].add(left)
    components: list[tuple[UUID, ...]] = []
    remaining = set(identifiers)
    while remaining:
        start = min(remaining, key=str)
        stack = [start]
        component: set[UUID] = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(neighbours[current] - component)
        remaining -= component
        if len(component) > 1:
            components.append(tuple(sorted(component, key=str)))
    return tuple(components)


def days_to_exit(position_market_value: Decimal, adv20: Decimal | None) -> Decimal | None:
    """Use a fixed ten-percent ADV participation cap without binary floats."""
    if position_market_value < 0:
        raise ValueError("position market value must not be negative")
    if adv20 is None or adv20 <= 0:
        return None
    return position_market_value / (adv20 * Decimal("0.10"))


def _returns(closes: Sequence[Decimal]) -> tuple[Decimal, ...]:
    return tuple(
        current / prior - Decimal(1)
        for prior, current in zip(closes, closes[1:])
        if prior > 0 and current > 0
    )


def _coverage_minimum(window: int) -> int:
    if window < 1:
        raise ValueError("window must be positive")
    return ceil(Decimal(window) * Decimal("0.80"))


def _correlation(left: Sequence[Decimal], right: Sequence[Decimal]) -> Decimal | None:
    count = min(len(left), len(right))
    if count < 60:
        return None
    pairs = tuple(zip(left[-count:], right[-count:], strict=True))
    left_mean = sum((pair[0] for pair in pairs), Decimal(0)) / Decimal(count)
    right_mean = sum((pair[1] for pair in pairs), Decimal(0)) / Decimal(count)
    covariance = sum(
        ((first - left_mean) * (second - right_mean) for first, second in pairs), Decimal(0)
    ) / Decimal(count)
    left_variance = sum(
        ((first - left_mean) ** 2 for first, _ in pairs), Decimal(0)
    ) / Decimal(count)
    right_variance = sum(
        ((second - right_mean) ** 2 for _, second in pairs), Decimal(0)
    ) / Decimal(count)
    if left_variance == 0 or right_variance == 0:
        return None
    return covariance / (left_variance * right_variance).sqrt()


def _returns_by_session(closes: Mapping[date, Decimal]) -> dict[date, Decimal]:
    sessions = tuple(sorted(closes))
    return {
        current: closes[current] / closes[prior] - Decimal(1)
        for prior, current in zip(sessions, sessions[1:])
        if closes[prior] > 0 and closes[current] > 0
    }


def _beta_from_pairs(pairs: tuple[tuple[Decimal, Decimal], ...]) -> tuple[Decimal, Decimal] | None:
    count = len(pairs)
    asset_mean = sum((asset for asset, _ in pairs), Decimal(0)) / Decimal(count)
    benchmark_mean = sum((benchmark for _, benchmark in pairs), Decimal(0)) / Decimal(count)
    covariance = sum(
        ((asset - asset_mean) * (benchmark - benchmark_mean) for asset, benchmark in pairs),
        Decimal(0),
    ) / Decimal(count)
    asset_variance = sum(
        ((asset - asset_mean) ** 2 for asset, _ in pairs), Decimal(0)
    ) / Decimal(count)
    benchmark_variance = sum(
        ((benchmark - benchmark_mean) ** 2 for _, benchmark in pairs), Decimal(0)
    ) / Decimal(count)
    if asset_variance == 0 or benchmark_variance == 0:
        return None
    correlation = covariance / (asset_variance * benchmark_variance).sqrt()
    return covariance / benchmark_variance, correlation
