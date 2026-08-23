"""Frozen inputs and outputs for deterministic portfolio diagnostics."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
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


class EvidenceBinding(_DiagnosticModel):
    """Point-in-time metadata binding one input to immutable source refs."""

    available_at: datetime
    as_of: datetime
    session: date | None = None
    refs: tuple[EvidenceRef, ...] = Field(min_length=1)

    @field_validator("available_at", "as_of")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class DiagnosticEvidence(_DiagnosticModel):
    snapshot: EvidenceBinding | None = None
    cash: Mapping[str, EvidenceBinding] = {}
    position_prices: Mapping[UUID, EvidenceBinding] = {}
    adv20: Mapping[UUID, EvidenceBinding] = {}
    risk_buckets: Mapping[UUID, EvidenceBinding] = {}
    price_series: Mapping[UUID, EvidenceBinding] = {}
    benchmark: EvidenceBinding | None = None
    trading_calendar: EvidenceBinding | None = None


class InstrumentIdentity(_DiagnosticModel):
    """Repository-minted point-in-time instrument identity for scope enforcement."""

    instrument_id: UUID
    asset_type: str | None
    exchange: str | None
    currency: str | None
    risk_bucket: str | None
    canonical_ticker: str | None
    restriction: Literal["STANDARD", "FOSUN_READ_ONLY", "UNRESOLVED"]
    instrument_record_id: UUID | None = None
    alias_record_id: UUID | None = None
    instrument_created_at: datetime | None = None
    alias_created_at: datetime | None = None
    authority_refs: tuple[EvidenceRef, ...] = ()
    minted_by: Literal["instrument_repository"] = "instrument_repository"

    @field_validator("instrument_created_at", "alias_created_at")
    @classmethod
    def _identity_time_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _restriction_matches_identity(self) -> "InstrumentIdentity":
        is_fosun = (
            self.asset_type == "equity"
            and self.exchange == "XHKG"
            and self.currency == "HKD"
            and self.canonical_ticker == "00656.HK"
        )
        if is_fosun != (self.restriction == "FOSUN_READ_ONLY"):
            raise ValueError("00656.HK identity must use the read-only restriction")
        if self.restriction == "UNRESOLVED" and self.canonical_ticker is not None:
            raise ValueError("unresolved identity cannot claim a canonical ticker")
        return self

    @classmethod
    def fosun_read_only(cls, instrument_id: UUID) -> "InstrumentIdentity":
        return cls(
            instrument_id=instrument_id,
            asset_type="equity",
            exchange="XHKG",
            currency="HKD",
            risk_bucket="hk_equity",
            canonical_ticker="00656.HK",
            restriction="FOSUN_READ_ONLY",
        )

    @classmethod
    def standard(cls, instrument_id: UUID) -> "InstrumentIdentity":
        return cls(
            instrument_id=instrument_id,
            asset_type="equity",
            exchange="XNAS",
            currency="USD",
            risk_bucket="us_equity",
            canonical_ticker="FIXTURE",
            restriction="STANDARD",
        )

    @classmethod
    def unresolved(
        cls,
        instrument_id: UUID,
        *,
        authority_refs: tuple[EvidenceRef, ...] = (),
    ) -> "InstrumentIdentity":
        return cls(
            instrument_id=instrument_id,
            asset_type=None,
            exchange=None,
            currency=None,
            risk_bucket=None,
            canonical_ticker=None,
            restriction="UNRESOLVED",
            authority_refs=authority_refs,
        )


class MarketEvidence(_DiagnosticModel):
    prices: Mapping[UUID, PriceSeries] = {}
    adv20: Mapping[UUID, Decimal] = {}
    quality: Literal["PASS", "PARTIAL", "FAIL"] = "PARTIAL"
    risk_buckets: Mapping[UUID, Mapping[str, Decimal]] = {}
    trading_sessions: tuple[date, ...] = ()
    identities: Mapping[UUID, InstrumentIdentity] = {}
    benchmark: PriceSeries | None = None
    evidence: DiagnosticEvidence = DiagnosticEvidence()
    source_conflict: bool = False
    conflict_refs: tuple[EvidenceRef, ...] = ()
    incomplete_refs: tuple[EvidenceRef, ...] = ()

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
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1)


class PortfolioDiagnosticReport(_DiagnosticModel):
    portfolio_snapshot_id: UUID
    snapshot_as_of: datetime | None = None
    trading_day: date | None = None
    policy_version: Literal["real_advisory_policy_v1"]
    optimization_need: OptimizationNeed
    items: tuple[DiagnosticItem, ...]

    @field_validator("snapshot_as_of")
    @classmethod
    def _snapshot_as_of_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class DiagnosticHistory(_DiagnosticModel):
    prior_reports: tuple[PortfolioDiagnosticReport, ...] = ()
    authority_refs: tuple[EvidenceRef, ...] = ()
    comparable_variants: tuple[ComparableVariantEvidence, ...] = ()

    @classmethod
    def empty(cls) -> "DiagnosticHistory":
        return cls()
