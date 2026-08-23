"""Typed deterministic comparison with unknown-safe ranking."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.diagnostics.models import PortfolioDiagnosticReport
from nexus.optimizer.models import OptimizationProposal, ProposalVariant

MetricStatus = Literal["known", "unknown"]


class TypedMetric(BaseModel):
    """One dimension is explicitly known or carries a stable unknown reason."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    status: MetricStatus
    value: Decimal | None = None
    reason: str | None = None

    @field_validator("value", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("comparison metrics must not use binary floats")
        return value

    @model_validator(mode="after")
    def _status_matches_payload(self) -> "TypedMetric":
        if self.status == "known" and (self.value is None or self.reason is not None):
            raise ValueError("known metric requires only a Decimal value")
        if self.status == "unknown" and (self.value is not None or not self.reason):
            raise ValueError("unknown metric requires only a reason")
        return self

    @classmethod
    def known(cls, value: Decimal | str) -> "TypedMetric":
        return cls(status="known", value=Decimal(value))

    @classmethod
    def unknown(cls, reason: str) -> "TypedMetric":
        return cls(status="unknown", reason=reason)


class ComparisonDimensions(BaseModel):
    """Every required Task 12 comparison dimension, including typed tax unknown."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    hard_violations: int
    top1: TypedMetric
    top3: TypedMetric
    hhi: TypedMetric
    sector_exposure: TypedMetric
    currency_exposure: TypedMetric
    volatility: TypedMetric
    drawdown: TypedMetric
    correlation: TypedMetric
    liquidity: TypedMetric
    turnover: TypedMetric
    cost: TypedMetric
    tax_impact: TypedMetric = TypedMetric.unknown("TAX_DATA_UNAVAILABLE")

    @model_validator(mode="after")
    def _valid_dimensions(self) -> "ComparisonDimensions":
        if self.hard_violations < 0:
            raise ValueError("hard violations must be nonnegative")
        if self.tax_impact.status != "unknown":
            raise ValueError("tax impact must remain unknown")
        return self

    @property
    def all_ranked_dimensions_known(self) -> bool:
        return all(getattr(self, name).status == "known" for name in _RANKED_METRIC_NAMES)

    @property
    def unknown_ranked_dimensions(self) -> tuple[str, ...]:
        return tuple(
            name for name in _RANKED_METRIC_NAMES if getattr(self, name).status == "unknown"
        )


_RISK_SMALLER_IS_BETTER = (
    "top1",
    "top3",
    "hhi",
    "sector_exposure",
    "currency_exposure",
    "volatility",
    "drawdown",
    "correlation",
)
_RANKED_METRIC_NAMES = (*_RISK_SMALLER_IS_BETTER, "liquidity", "turnover", "cost")


@dataclass(frozen=True)
class ComparableProposal:
    variant: ProposalVariant
    dimensions: ComparisonDimensions
    requires_fx: bool = False
    constraint_complete: bool = True
    eligibility_complete: bool = True
    evidence_complete: bool = True
    stale: bool = False
    partial: bool = False
    source_conflict: bool = False
    cost_complete: bool = True

    @property
    def authority_complete(self) -> bool:
        return (
            self.dimensions.all_ranked_dimensions_known
            and self.cost_complete
            and self.constraint_complete
            and self.eligibility_complete
            and self.evidence_complete
            and not self.requires_fx
            and not self.stale
            and not self.partial
            and not self.source_conflict
        )


@dataclass(frozen=True)
class ProposalComparison:
    recommendation: ProposalVariant
    ranked_variants: tuple[ProposalVariant, ...]
    tax_impact: TypedMetric = TypedMetric.unknown("TAX_DATA_UNAVAILABLE")
    excluded_variants: tuple[ProposalVariant, ...] = ()


def compare_proposals(proposals: tuple[ComparableProposal, ...]) -> ProposalComparison:
    """Rank only complete alternatives that prove improvement without risk deterioration."""
    if tuple(item.variant for item in proposals) != (
        "hold_current",
        "minimum_change",
        "strategy_target",
    ):
        raise ValueError("comparison requires exactly the ordered three variants")
    hold = proposals[0]
    eligible = [hold] if hold.authority_complete else []
    if hold.authority_complete:
        eligible.extend(
            proposal
            for proposal in proposals[1:]
            if proposal.authority_complete and _proves_safe_improvement(proposal, hold)
        )
    ranked = tuple(item.variant for item in sorted(eligible, key=_rank_key))
    recommendation = ranked[0] if ranked and ranked[0] != "hold_current" else "hold_current"
    return ProposalComparison(
        recommendation=recommendation,
        ranked_variants=ranked,
        excluded_variants=tuple(item.variant for item in proposals if item not in eligible),
    )


def evaluate_dimensions(
    proposal: OptimizationProposal,
    report: PortfolioDiagnosticReport,
    *,
    hard_violations: int,
    cost: Decimal | None,
) -> ComparisonDimensions:
    """Evaluate proposal weights in memory and retain unavailable risk inputs as unknown."""
    weights = sorted(
        (position.target_weight for position in proposal.positions if position.target_weight > 0),
        reverse=True,
    )
    top1 = weights[0] if weights else Decimal("0")
    top3 = sum(weights[:3], Decimal("0"))
    hhi = sum((weight * weight for weight in weights), Decimal("0"))
    liquidity = _report_metric(report, "LIQUIDITY_DAYS_TO_EXIT", invert=True)
    return ComparisonDimensions(
        hard_violations=hard_violations,
        top1=TypedMetric.known(top1),
        top3=TypedMetric.known(top3),
        hhi=TypedMetric.known(hhi),
        sector_exposure=_report_metric(report, "SECTOR_EXPOSURE"),
        currency_exposure=_report_metric(report, "CURRENCY_EXPOSURE"),
        volatility=_report_metric(report, "VOLATILITY"),
        drawdown=_report_metric(report, "DRAWDOWN"),
        correlation=_report_metric(report, "CORRELATION_CLUSTER"),
        liquidity=liquidity,
        turnover=TypedMetric.known(proposal.turnover),
        cost=TypedMetric.unknown("COST_NOT_PROVEN")
        if cost is None
        else TypedMetric.known(cost),
    )


def _report_metric(
    report: PortfolioDiagnosticReport,
    prefix: str,
    *,
    invert: bool = False,
    unknown_reason: str | None = None,
) -> TypedMetric:
    values = tuple(
        Decimal(item.metric_value)
        for item in report.items
        if item.diagnostic_code.startswith(prefix)
        and item.metric_value is not None
        and item.unknown_reason is None
    )
    if not values:
        return TypedMetric.unknown(unknown_reason or f"{prefix}_NOT_PROVEN")
    value = max(values)
    return TypedMetric.known(Decimal("1") / (Decimal("1") + value) if invert else value)


def _proves_safe_improvement(candidate: ComparableProposal, hold: ComparableProposal) -> bool:
    left, right = candidate.dimensions, hold.dimensions
    if left.hard_violations > right.hard_violations:
        return False
    for name in _RISK_SMALLER_IS_BETTER:
        if _value(left, name) > _value(right, name):
            return False
    if _value(left, "liquidity") < _value(right, "liquidity"):
        return False
    return (
        left.hard_violations < right.hard_violations
        or any(
            _value(left, name) < _value(right, name)
            for name in _RISK_SMALLER_IS_BETTER
        )
        or _value(left, "liquidity") > _value(right, "liquidity")
        or _value(left, "cost") < _value(right, "cost")
    )


def _rank_key(proposal: ComparableProposal) -> tuple[object, ...]:
    dimensions = proposal.dimensions
    return (
        dimensions.hard_violations,
        *(_value(dimensions, name) for name in _RISK_SMALLER_IS_BETTER),
        -_value(dimensions, "liquidity"),
        _value(dimensions, "cost"),
        _value(dimensions, "turnover"),
        proposal.variant,
    )


def _value(dimensions: ComparisonDimensions, name: str) -> Decimal:
    value = cast(TypedMetric, getattr(dimensions, name)).value
    if value is None:
        raise ValueError("unknown dimensions must be excluded before ranking")
    return value
