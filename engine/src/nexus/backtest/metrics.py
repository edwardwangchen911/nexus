"""Deterministic performance, downside, tail, and multiple-testing metrics."""

import math
from decimal import ROUND_CEILING, Decimal, localcontext
from statistics import NormalDist

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.backtest.canonical import canonical_decimal


class BacktestMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    total_return: Decimal
    annualized_return: Decimal
    annualized_volatility: Decimal
    sharpe: Decimal
    sortino: Decimal
    max_drawdown: Decimal
    calmar: Decimal
    expected_shortfall_95: Decimal
    deflated_sharpe_confidence: Decimal
    turnover: Decimal
    total_cost: Decimal
    cost_ratio: Decimal

    @field_validator("*", mode="after")
    @classmethod
    def _canonical_values(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


def calculate_metrics(
    nav_values: tuple[Decimal, ...],
    *,
    turnover_notional: Decimal,
    initial_nav: Decimal,
    total_cost: Decimal = Decimal(0),
    experiment_attempt_count: int = 1,
) -> BacktestMetrics:
    if experiment_attempt_count < 1:
        raise ValueError("experiment attempt count must be positive")
    if not nav_values:
        return _zero_metrics()
    total_return = nav_values[-1] / initial_nav - Decimal(1)
    returns = tuple(
        nav_values[index] / nav_values[index - 1] - Decimal(1)
        for index in range(1, len(nav_values))
        if nav_values[index - 1] != 0
    )
    with localcontext() as context:
        context.prec = 38
        observation_count = max(len(nav_values) - 1, 0)
        years = Decimal(observation_count) / Decimal(252)
        annualized_return = (
            (nav_values[-1] / initial_nav) ** (Decimal(1) / years) - Decimal(1)
            if nav_values[-1] > 0 and years > 0
            else Decimal(0)
            if years == 0
            else Decimal(-1)
        )
        if len(returns) > 1:
            mean = sum(returns, start=Decimal(0)) / Decimal(len(returns))
            variance = sum((value - mean) ** 2 for value in returns) / Decimal(len(returns) - 1)
            annualized_volatility = variance.sqrt() * Decimal(252).sqrt()
            sharpe = (
                mean * Decimal(252) / annualized_volatility
                if annualized_volatility != 0
                else Decimal(0)
            )
            downside = tuple(value for value in returns if value < 0)
            downside_deviation = (
                (sum(value**2 for value in downside) / Decimal(len(downside))).sqrt()
                * Decimal(252).sqrt()
                if downside
                else Decimal(0)
            )
            sortino = (
                mean * Decimal(252) / downside_deviation if downside_deviation != 0 else Decimal(0)
            )
        else:
            annualized_volatility = Decimal(0)
            sharpe = Decimal(0)
            sortino = Decimal(0)
    peak = nav_values[0]
    max_drawdown = Decimal(0)
    for nav in nav_values:
        peak = max(peak, nav)
        if peak > 0:
            max_drawdown = min(max_drawdown, nav / peak - Decimal(1))
    calmar = annualized_return / abs(max_drawdown) if max_drawdown != 0 else Decimal(0)
    expected_shortfall = _expected_shortfall_95(returns)
    deflated_confidence = _deflated_sharpe_confidence(
        returns,
        experiment_attempt_count=experiment_attempt_count,
    )
    return BacktestMetrics(
        total_return=total_return,
        annualized_return=annualized_return,
        annualized_volatility=annualized_volatility,
        sharpe=sharpe,
        sortino=sortino,
        max_drawdown=max_drawdown,
        calmar=calmar,
        expected_shortfall_95=expected_shortfall,
        deflated_sharpe_confidence=deflated_confidence,
        turnover=turnover_notional / initial_nav if initial_nav else Decimal(0),
        total_cost=total_cost,
        cost_ratio=total_cost / turnover_notional if turnover_notional else Decimal(0),
    )


def _zero_metrics() -> BacktestMetrics:
    return BacktestMetrics(
        total_return=Decimal(0),
        annualized_return=Decimal(0),
        annualized_volatility=Decimal(0),
        sharpe=Decimal(0),
        sortino=Decimal(0),
        max_drawdown=Decimal(0),
        calmar=Decimal(0),
        expected_shortfall_95=Decimal(0),
        deflated_sharpe_confidence=Decimal(0),
        turnover=Decimal(0),
        total_cost=Decimal(0),
        cost_ratio=Decimal(0),
    )


def _expected_shortfall_95(returns: tuple[Decimal, ...]) -> Decimal:
    if not returns:
        return Decimal(0)
    tail_count = int(
        (Decimal(len(returns)) * Decimal("0.05")).to_integral_value(rounding=ROUND_CEILING)
    )
    tail = sorted(returns)[: max(tail_count, 1)]
    return sum(tail, start=Decimal(0)) / Decimal(len(tail))


def _deflated_sharpe_confidence(
    returns: tuple[Decimal, ...], *, experiment_attempt_count: int
) -> Decimal:
    """Return the Bailey/Lopez de Prado deflated Sharpe probability."""
    if len(returns) < 2:
        return Decimal(0)
    values = [float(value) for value in returns]
    count = len(values)
    mean = sum(values) / count
    variance = sum((value - mean) ** 2 for value in values) / (count - 1)
    if variance <= 0:
        return Decimal(0)
    standard_deviation = math.sqrt(variance)
    sharpe = mean / standard_deviation
    centered = [(value - mean) / standard_deviation for value in values]
    skewness = sum(value**3 for value in centered) / count
    kurtosis = sum(value**4 for value in centered) / count

    expected_maximum = 0.0
    if experiment_attempt_count > 1:
        normal = NormalDist()
        gamma = 0.5772156649015329
        attempts = float(experiment_attempt_count)
        variance_sharpe = (1.0 + 0.5 * sharpe**2) / max(count - 1, 1)
        expected_maximum = math.sqrt(max(variance_sharpe, 0.0)) * (
            (1.0 - gamma) * normal.inv_cdf(1.0 - 1.0 / attempts)
            + gamma * normal.inv_cdf(1.0 - 1.0 / (attempts * math.e))
        )

    denominator_squared = 1.0 - skewness * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe**2
    if denominator_squared <= 0:
        return Decimal(0)
    statistic = (
        (sharpe - expected_maximum) * math.sqrt(max(count - 1, 1)) / math.sqrt(denominator_squared)
    )
    probability = min(max(NormalDist().cdf(statistic), 0.0), 1.0)
    return Decimal(str(round(probability, 12)))
