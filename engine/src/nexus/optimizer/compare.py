"""Typed deterministic comparison with unknown-safe ranking."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

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
    cost: Decimal | None,
    instrument_currencies: Mapping[UUID, str],
) -> ComparisonDimensions:
    """Evaluate proposal weights in memory and retain unavailable risk inputs as unknown."""
    positive = tuple(
        position for position in proposal.positions if position.target_weight > 0
    )
    raw_by_id = {
        position.instrument_id: position.target_weight for position in positive
    }
    security_total = sum((position.target_weight for position in positive), Decimal("0"))
    normalized = (
        tuple(weight / security_total for weight in raw_by_id.values())
        if security_total > 0 else ()
    )
    weights = sorted(raw_by_id.values(), reverse=True)
    top1 = weights[0] if weights else Decimal("0")
    top3 = sum(weights[:3], Decimal("0"))
    hhi = sum((weight * weight for weight in normalized), Decimal("0"))
    liquidity = _weighted_liquidity(report, raw_by_id)
    currency_exposure = _currency_exposure(raw_by_id, instrument_currencies)
    return ComparisonDimensions(
        hard_violations=sum(weight > Decimal("0.30") for weight in weights),
        top1=TypedMetric.known(top1),
        top3=TypedMetric.known(top3),
        hhi=TypedMetric.known(hhi),
        sector_exposure=TypedMetric.unknown("SECTOR_EXPOSURE_NOT_PROVEN"),
        currency_exposure=currency_exposure,
        volatility=TypedMetric.unknown("VOLATILITY_POST_PROPOSAL_NOT_PROVEN"),
        drawdown=TypedMetric.unknown("DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"),
        correlation=TypedMetric.unknown("CORRELATION_POST_PROPOSAL_NOT_PROVEN"),
        liquidity=liquidity,
        turnover=TypedMetric.known(proposal.turnover),
        cost=TypedMetric.unknown("COST_NOT_PROVEN")
        if cost is None
        else TypedMetric.known(cost),
    )


def _currency_exposure(
    weights: Mapping[UUID, Decimal], currencies: Mapping[UUID, str]
) -> TypedMetric:
    if any(instrument_id not in currencies for instrument_id in weights):
        return TypedMetric.unknown("CURRENCY_EXPOSURE_NOT_PROVEN")
    totals: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for instrument_id, weight in weights.items():
        totals[currencies[instrument_id]] += weight
    return TypedMetric.known(max(totals.values(), default=Decimal("0")))


def _weighted_liquidity(
    report: PortfolioDiagnosticReport, weights: Mapping[UUID, Decimal]
) -> TypedMetric:
    weighted_days = Decimal("0")
    for instrument_id, weight in weights.items():
        matches = tuple(
            item for item in report.items
            if item.diagnostic_code == f"LIQUIDITY_DAYS_TO_EXIT:{instrument_id}"
            and item.metric_value is not None
            and item.unknown_reason is None
        )
        if len(matches) != 1:
            return TypedMetric.unknown("LIQUIDITY_POST_PROPOSAL_NOT_PROVEN")
        metric_value = matches[0].metric_value
        assert metric_value is not None
        weighted_days += weight * Decimal(metric_value)
    return TypedMetric.known(Decimal("1") / (Decimal("1") + weighted_days))


def _proves_safe_improvement(candidate: ComparableProposal, hold: ComparableProposal) -> bool:
    left, right = candidate.dimensions, hold.dimensions
    if left.hard_violations > right.hard_violations:
        return False
    for name in _RISK_SMALLER_IS_BETTER:
        if _value(left, name) > _value(right, name):
            return False
    if _value(left, "liquidity") < _value(right, "liquidity"):
        return False
    if _value(left, "cost") > _value(right, "cost"):
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
