"""Deterministic in-memory what-if evaluation without portfolio persistence."""

from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AggregateMetric(BaseModel):
    """Bounded integer aggregate with an explicit known/unknown state."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    status: Literal["known", "unknown"]
    value: int | None = Field(default=None, ge=0)
    unit: Literal["basis_points", "parts_per_million", "count"]
    reason: str | None = None

    @model_validator(mode="after")
    def _state_matches_payload(self) -> "AggregateMetric":
        if self.status == "known" and (self.value is None or self.reason is not None):
            raise ValueError("known aggregate requires only a value")
        if self.status == "unknown" and (self.value is not None or not self.reason):
            raise ValueError("unknown aggregate requires only a reason")
        return self

    @classmethod
    def known(
        cls, value: int, unit: Literal["basis_points", "parts_per_million", "count"]
    ) -> "AggregateMetric":
        return cls(status="known", value=value, unit=unit)

    @classmethod
    def unknown(
        cls, reason: str, unit: Literal["basis_points", "parts_per_million", "count"]
    ) -> "AggregateMetric":
        return cls(status="unknown", reason=reason, unit=unit)


class WhatIfDiagnostics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    top1: AggregateMetric
    top3: AggregateMetric
    hhi: AggregateMetric
    sector_exposure: AggregateMetric
    currency_exposure: AggregateMetric
    volatility: AggregateMetric
    drawdown: AggregateMetric
    correlation: AggregateMetric
    liquidity: AggregateMetric


class WhatIfConstraints(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    total_weight: AggregateMetric
    unallocated_cash: AggregateMetric
    single_name_warning: AggregateMetric
    single_name_critical: AggregateMetric
    top3_warning: AggregateMetric
    cash_minimum: AggregateMetric
    violation_count: AggregateMetric


class WhatIfEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    diagnostics: WhatIfDiagnostics
    constraints: WhatIfConstraints


def evaluate_what_if(weights: tuple[Decimal, ...]) -> WhatIfEvaluation:
    """Return fixed-size diagnostic/constraint aggregates; never retain identities or weights."""
    ordered = sorted(weights, reverse=True)
    total = sum(ordered, Decimal("0"))
    normalized = [weight / total for weight in ordered] if total > 0 else []
    top1 = normalized[0] if normalized else Decimal("0")
    top3 = sum(normalized[:3], Decimal("0"))
    hhi = sum((weight * weight for weight in normalized), Decimal("0"))
    warning = sum(Decimal("0.15") < weight <= Decimal("0.30") for weight in normalized)
    critical = sum(weight > Decimal("0.30") for weight in normalized)
    top3_breach = int(top3 > Decimal("0.60"))
    cash = max(Decimal("0"), Decimal("1") - total)
    cash_breach = int(cash < Decimal("0.10"))
    total_breach = int(total > Decimal("1"))
    return WhatIfEvaluation(
        diagnostics=WhatIfDiagnostics(
            top1=AggregateMetric.known(_scaled(top1, 10_000), "basis_points"),
            top3=AggregateMetric.known(_scaled(top3, 10_000), "basis_points"),
            hhi=AggregateMetric.known(_scaled(hhi, 1_000_000), "parts_per_million"),
            sector_exposure=AggregateMetric.unknown(
                "SECTOR_EXPOSURE_REQUIRES_IDENTITIES", "basis_points"
            ),
            currency_exposure=AggregateMetric.unknown(
                "CURRENCY_EXPOSURE_REQUIRES_IDENTITIES", "basis_points"
            ),
            volatility=AggregateMetric.unknown(
                "VOLATILITY_REQUIRES_PRICE_HISTORY", "basis_points"
            ),
            drawdown=AggregateMetric.unknown(
                "DRAWDOWN_REQUIRES_PRICE_HISTORY", "basis_points"
            ),
            correlation=AggregateMetric.unknown(
                "CORRELATION_REQUIRES_PRICE_HISTORY", "basis_points"
            ),
            liquidity=AggregateMetric.unknown("LIQUIDITY_REQUIRES_ADV20", "count"),
        ),
        constraints=WhatIfConstraints(
            total_weight=AggregateMetric.known(_scaled(total, 10_000), "basis_points"),
            unallocated_cash=AggregateMetric.known(
                _scaled(cash, 10_000), "basis_points"
            ),
            single_name_warning=AggregateMetric.known(warning, "count"),
            single_name_critical=AggregateMetric.known(critical, "count"),
            top3_warning=AggregateMetric.known(top3_breach, "count"),
            cash_minimum=AggregateMetric.known(cash_breach, "count"),
            violation_count=AggregateMetric.known(
                warning + critical + top3_breach + cash_breach + total_breach, "count"
            ),
        ),
    )


def _scaled(value: Decimal, multiplier: int) -> int:
    return int((value * multiplier).to_integral_value(rounding=ROUND_HALF_UP))
