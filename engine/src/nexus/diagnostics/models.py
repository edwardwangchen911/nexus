"""Frozen inputs and outputs for deterministic portfolio diagnostics."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.providers.base import PriceSeries

type OptimizationNeed = Literal[
    "BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"
]
type DiagnosticSeverity = Literal["info", "warning", "critical"]
type DiagnosticActionability = Literal["monitor", "research", "reduce_only", "blocked"]


class _DiagnosticModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("diagnostic values must not be binary floats")
        return value

    @model_validator(mode="before")
    @classmethod
    def _reject_nested_binary_floats(cls, value: object) -> object:
        _assert_no_binary_float(value)
        return value


def _assert_no_binary_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("diagnostic values must not be binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _assert_no_binary_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _assert_no_binary_float(nested)


class MarketEvidence(_DiagnosticModel):
    prices: Mapping[UUID, PriceSeries] = {}
    adv20: Mapping[UUID, Decimal] = {}
    quality: Literal["PASS", "PARTIAL", "FAIL"] = "PARTIAL"
    risk_buckets: Mapping[UUID, Mapping[str, Decimal]] = {}
    trading_sessions: tuple[date, ...] = ()
    symbols: Mapping[UUID, str] = {}
    benchmark: PriceSeries | None = None
    evidence_refs: tuple[EvidenceRef, ...] = ()
    input_evidence: Mapping[str, tuple[EvidenceRef, ...]] = {}
    source_conflict: bool = False

    @model_validator(mode="after")
    def _bucket_weights_are_complete(self) -> "MarketEvidence":
        sessions_are_unique = len(set(self.trading_sessions)) == len(self.trading_sessions)
        if tuple(sorted(self.trading_sessions)) != self.trading_sessions or not sessions_are_unique:
            raise ValueError("trading sessions must be strictly increasing")
        for weights in self.risk_buckets.values():
            if (
                not weights
                or any(value < 0 for value in weights.values())
                or sum(weights.values(), Decimal(0)) != Decimal(1)
            ):
                raise ValueError("risk bucket weights must be nonnegative and sum to one")
        return self

    def refs_for(self, input_kind: str, *, as_of: datetime) -> tuple[EvidenceRef, ...]:
        refs = self.input_evidence.get(input_kind, ())
        if not refs or any(ref.available_at > as_of for ref in refs):
            return ()
        return refs

    @classmethod
    def missing_adv(cls) -> "MarketEvidence":
        return cls(quality="PASS")


class ComparableVariantEvidence(_DiagnosticModel):
    """A persisted non-hold comparison, never a caller-supplied readiness flag."""

    variant: Literal["minimum_change", "strategy_target"]
    proposal_group_id: UUID
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1)


class DiagnosticItem(_DiagnosticModel):
    diagnostic_code: str
    metric_value: DecimalString | None
    unit: str
    reference_limit: DecimalString | None
    policy_scope: str
    severity: DiagnosticSeverity
    actionability: DiagnosticActionability
    unknown_reason: str | None
    evidence_refs: tuple[EvidenceRef, ...] = ()


class PortfolioDiagnosticReport(_DiagnosticModel):
    portfolio_snapshot_id: UUID
    trading_day: date | None = None
    policy_version: Literal["real_advisory_policy_v1"]
    optimization_need: OptimizationNeed
    items: tuple[DiagnosticItem, ...]


class DiagnosticHistory(_DiagnosticModel):
    prior_reports: tuple[PortfolioDiagnosticReport, ...] = ()
    comparable_variants: tuple[ComparableVariantEvidence, ...] = ()

    @classmethod
    def empty(cls) -> "DiagnosticHistory":
        return cls()
