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


class WhatIfDiagnostics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    top1: AggregateMetric
    top3: AggregateMetric
    hhi: AggregateMetric


class WhatIfConstraints(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    total_weight: AggregateMetric
    unallocated_cash: AggregateMetric
    violation_count: AggregateMetric


class WhatIfEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    diagnostics: WhatIfDiagnostics
    constraints: WhatIfConstraints


def evaluate_what_if(weights: tuple[Decimal, ...]) -> WhatIfEvaluation:
    """Return fixed-size diagnostic/constraint aggregates; never retain identities or weights."""
    ordered = sorted(weights, reverse=True)
    total = sum(ordered, Decimal("0"))
    top1 = ordered[0] if ordered else Decimal("0")
    top3 = sum(ordered[:3], Decimal("0"))
    hhi = sum((weight * weight for weight in ordered), Decimal("0"))
    return WhatIfEvaluation(
        diagnostics=WhatIfDiagnostics(
            top1=AggregateMetric.known(_scaled(top1, 10_000), "basis_points"),
            top3=AggregateMetric.known(_scaled(top3, 10_000), "basis_points"),
            hhi=AggregateMetric.known(_scaled(hhi, 1_000_000), "parts_per_million"),
        ),
        constraints=WhatIfConstraints(
            total_weight=AggregateMetric.known(_scaled(total, 10_000), "basis_points"),
            unallocated_cash=AggregateMetric.known(
                _scaled(max(Decimal("0"), Decimal("1") - total), 10_000), "basis_points"
            ),
            violation_count=AggregateMetric.known(int(total > 1), "count"),
        ),
    )


def _scaled(value: Decimal, multiplier: int) -> int:
    return int((value * multiplier).to_integral_value(rounding=ROUND_HALF_UP))
