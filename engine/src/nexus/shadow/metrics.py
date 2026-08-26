"""Exact Decimal metrics for Proposal Shadow checkpoints."""

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, localcontext
from typing import Literal

from nexus.shadow.models import (
    INITIAL_NAV,
    SHADOW_VARIANTS,
    ShadowMetrics,
    ShadowMetricValue,
    ShadowNavPoint,
    ShadowVariant,
    VariantMetrics,
    WorstSession,
)
from nexus.simulation.canonical import validated_decimal

_INCOMPLETE = "SHADOW_CHECKPOINT_INCOMPLETE"


def annualized_volatility(returns: tuple[Decimal, ...]) -> Decimal:
    """Return sample daily volatility annualized by sqrt(252)."""
    values = tuple(
        validated_decimal(value, field="daily return") for value in returns
    )
    if len(values) < 2:
        return Decimal(0)
    with localcontext() as context:
        context.prec = 50
        mean = sum(values, start=Decimal(0)) / Decimal(len(values))
        variance = sum((value - mean) ** 2 for value in values) / Decimal(
            len(values) - 1
        )
        return variance.sqrt() * Decimal(252).sqrt()


def calculate_checkpoint_metrics(
    *,
    nav_by_variant: Mapping[ShadowVariant, tuple[ShadowNavPoint, ...]],
    turnover_by_variant: Mapping[ShadowVariant, Decimal],
    fees_by_variant: Mapping[ShadowVariant, Decimal],
    ordinal: Literal[5, 21, 63],
) -> tuple[VariantMetrics, VariantMetrics, VariantMetrics]:
    """Return hold/minimum/strategy metrics in fixed order."""
    if ordinal not in (5, 21, 63):
        raise ValueError("shadow checkpoint ordinal must be 5, 21, or 63")
    if (
        set(nav_by_variant) != set(SHADOW_VARIANTS)
        or set(turnover_by_variant) != set(SHADOW_VARIANTS)
        or set(fees_by_variant) != set(SHADOW_VARIANTS)
        or any(len(nav_by_variant[variant]) != ordinal for variant in SHADOW_VARIANTS)
    ):
        return _unknown_variants(_INCOMPLETE)
    sessions = tuple(point.session for point in nav_by_variant["hold_current"])
    if any(
        tuple(point.session for point in nav_by_variant[variant]) != sessions
        for variant in SHADOW_VARIANTS
    ):
        raise ValueError("checkpoint variants must share identical sessions")
    if any(left >= right for left, right in zip(sessions, sessions[1:], strict=False)):
        raise ValueError("checkpoint sessions must be strictly increasing")
    turnover = {
        variant: validated_decimal(turnover_by_variant[variant], field="turnover")
        for variant in SHADOW_VARIANTS
    }
    fees = {
        variant: validated_decimal(fees_by_variant[variant], field="fees")
        for variant in SHADOW_VARIANTS
    }
    if any(value < 0 for value in (*turnover.values(), *fees.values())):
        raise ValueError("checkpoint turnover and fees must be nonnegative")
    raw = {variant: _raw_metrics(nav_by_variant[variant]) for variant in SHADOW_VARIANTS}
    hold = raw["hold_current"]
    return tuple(
        VariantMetrics(
            variant=variant,
            metrics=ShadowMetrics(
                net_cumulative_return=ShadowMetricValue.known(raw[variant].net_return),
                return_delta_vs_hold=ShadowMetricValue.known(
                    raw[variant].net_return - hold.net_return
                ),
                max_drawdown=ShadowMetricValue.known(raw[variant].max_drawdown),
                drawdown_improvement=ShadowMetricValue.known(
                    abs(hold.max_drawdown) - abs(raw[variant].max_drawdown)
                ),
                annualized_volatility=ShadowMetricValue.known(raw[variant].volatility),
                volatility_improvement=ShadowMetricValue.known(
                    hold.volatility - raw[variant].volatility
                ),
                worst_sessions=raw[variant].worst_sessions,
                cumulative_turnover=ShadowMetricValue.known(turnover[variant]),
                fee_drag_index_points=ShadowMetricValue.known(fees[variant]),
            ),
        )
        for variant in SHADOW_VARIANTS
    )  # type: ignore[return-value]


class _RawMetrics:
    def __init__(
        self,
        *,
        net_return: Decimal,
        max_drawdown: Decimal,
        volatility: Decimal,
        worst_sessions: tuple[WorstSession, ...],
    ) -> None:
        self.net_return = net_return
        self.max_drawdown = max_drawdown
        self.volatility = volatility
        self.worst_sessions = worst_sessions


def _raw_metrics(points: tuple[ShadowNavPoint, ...]) -> _RawMetrics:
    previous = INITIAL_NAV
    returns: list[tuple[date, Decimal]] = []
    peak = INITIAL_NAV
    max_drawdown = Decimal(0)
    for point in points:
        daily_return = point.nav / previous - Decimal(1)
        returns.append((point.session, daily_return))
        previous = point.nav
        peak = max(peak, point.nav)
        max_drawdown = min(max_drawdown, point.nav / peak - Decimal(1))
    volatility = annualized_volatility(tuple(item[1] for item in returns))
    worst = tuple(
        WorstSession(session=session, return_value=value)
        for session, value in sorted(returns, key=lambda item: (item[1], item[0]))[:5]
    )
    return _RawMetrics(
        net_return=points[-1].nav / INITIAL_NAV - Decimal(1),
        max_drawdown=max_drawdown,
        volatility=volatility,
        worst_sessions=worst,
    )


def _unknown_metrics(reason: str) -> ShadowMetrics:
    unknown = ShadowMetricValue.unknown
    return ShadowMetrics(
        net_cumulative_return=unknown(reason),
        return_delta_vs_hold=unknown(reason),
        max_drawdown=unknown(reason),
        drawdown_improvement=unknown(reason),
        annualized_volatility=unknown(reason),
        volatility_improvement=unknown(reason),
        worst_sessions=(),
        cumulative_turnover=unknown(reason),
        fee_drag_index_points=unknown(reason),
    )


def _unknown_variants(
    reason: str,
) -> tuple[VariantMetrics, VariantMetrics, VariantMetrics]:
    return (
        VariantMetrics(variant="hold_current", metrics=_unknown_metrics(reason)),
        VariantMetrics(variant="minimum_change", metrics=_unknown_metrics(reason)),
        VariantMetrics(variant="strategy_target", metrics=_unknown_metrics(reason)),
    )
