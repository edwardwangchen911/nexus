"""Immutable value objects for static three-way Proposal Shadow."""

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.simulation.canonical import RESULT_QUANTUM, canonical_decimal, validated_decimal
from nexus.simulation.models import TargetWeight

ShadowVariant = Literal["hold_current", "minimum_change", "strategy_target"]
SHADOW_VARIANTS: tuple[ShadowVariant, ShadowVariant, ShadowVariant] = (
    "hold_current",
    "minimum_change",
    "strategy_target",
)
CHECKPOINT_ORDINALS = (5, 21, 63)
EVALUATION_SESSION_COUNT = 63
GRACE_SESSION_COUNT = 5
INITIAL_NAV = Decimal("100.000000000000")
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _canonical(value: Decimal | int, field: str) -> Decimal:
    return canonical_decimal(validated_decimal(value, field=field))


def _exact(value: Decimal | int, field: str) -> Decimal:
    return validated_decimal(value, field=field)


def _hash_value(value: str, field: str) -> None:
    if _HASH.fullmatch(value) is None:
        raise ValueError(f"{field} must be a canonical SHA-256")


def _utc(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must be UTC")


@dataclass(frozen=True)
class ShadowNavPoint:
    session: date
    nav: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "nav", _exact(self.nav, "nav"))
        if self.nav <= 0:
            raise ValueError("shadow NAV must be positive")


@dataclass(frozen=True)
class ShadowMetricValue:
    status: Literal["known", "unknown"]
    value: Decimal | None
    reason: str | None

    def __post_init__(self) -> None:
        if self.status == "known":
            if self.value is None or self.reason is not None:
                raise ValueError("known metric requires only a value")
            object.__setattr__(self, "value", _canonical(self.value, "metric value"))
        elif self.value is not None or not self.reason:
            raise ValueError("unknown metric requires only a reason")

    @classmethod
    def known(cls, value: Decimal | int) -> "ShadowMetricValue":
        return cls(status="known", value=_canonical(value, "metric value"), reason=None)

    @classmethod
    def unknown(cls, reason: str) -> "ShadowMetricValue":
        return cls(status="unknown", value=None, reason=reason)


@dataclass(frozen=True)
class WorstSession:
    session: date
    return_value: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "return_value",
            _canonical(self.return_value, "worst-session return"),
        )


@dataclass(frozen=True)
class ShadowMetrics:
    net_cumulative_return: ShadowMetricValue
    return_delta_vs_hold: ShadowMetricValue
    max_drawdown: ShadowMetricValue
    drawdown_improvement: ShadowMetricValue
    annualized_volatility: ShadowMetricValue
    volatility_improvement: ShadowMetricValue
    worst_sessions: tuple[WorstSession, ...]
    cumulative_turnover: ShadowMetricValue
    fee_drag_index_points: ShadowMetricValue

    def __post_init__(self) -> None:
        if len(self.worst_sessions) > 5:
            raise ValueError("worst sessions are limited to five")


@dataclass(frozen=True)
class VariantMetrics:
    variant: ShadowVariant
    metrics: ShadowMetrics


@dataclass(frozen=True)
class CalendarHashBinding:
    authority_id: UUID
    version: str
    content_hash: str

    def __post_init__(self) -> None:
        if not self.version:
            raise ValueError("calendar version is required")
        _hash_value(self.content_hash, "calendar content hash")


@dataclass(frozen=True)
class PriceInputHash:
    instrument_id: UUID
    authority_id: UUID
    source: str
    delivery_mode: str
    revision: str
    available_at: datetime
    content_hash: str
    purpose: Literal["execution_and_mark", "mark"]

    def __post_init__(self) -> None:
        if not self.source or not self.delivery_mode or not self.revision:
            raise ValueError("price identity fields are required")
        _utc(self.available_at, "price available_at")
        _hash_value(self.content_hash, "price content hash")


@dataclass(frozen=True)
class MissingExecutionInputHash:
    instrument_id: UUID
    source: str
    query_hash: str
    reason: str

    def __post_init__(self) -> None:
        if not self.source or not self.reason:
            raise ValueError("negative execution evidence requires source and reason")
        _hash_value(self.query_hash, "negative query hash")


@dataclass(frozen=True)
class EventInputHash:
    instrument_id: UUID
    authority_id: UUID
    event_kind: Literal["split", "dividend", "delisting"]
    source: str
    provider_event_id: str
    revision: str
    available_at: datetime
    evidence_hash: str

    def __post_init__(self) -> None:
        if not self.source or not self.provider_event_id or not self.revision:
            raise ValueError("event identity fields are required")
        _utc(self.available_at, "event available_at")
        _hash_value(self.evidence_hash, "event evidence hash")


@dataclass(frozen=True)
class ShadowSessionHashInput:
    shadow_group_id: UUID
    session: date
    ordinal: int
    calendar: CalendarHashBinding
    source_policy_id: UUID
    cost_policy_id: UUID
    bars: tuple[PriceInputHash, ...]
    missing_execution_inputs: tuple[MissingExecutionInputHash, ...]
    events: tuple[EventInputHash, ...]
    record_id: UUID | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.ordinal <= EVALUATION_SESSION_COUNT:
            raise ValueError("shadow session ordinal must be between 1 and 63")
        if self.created_at is not None:
            _utc(self.created_at, "created_at")
        bar_ids = [item.instrument_id for item in self.bars]
        if len(bar_ids) != len(set(bar_ids)):
            raise ValueError("shadow session price inputs must be unique by instrument")
        missing_ids = [item.instrument_id for item in self.missing_execution_inputs]
        if len(missing_ids) != len(set(missing_ids)):
            raise ValueError("missing execution inputs must be unique by instrument")
        if set(bar_ids) & set(missing_ids):
            raise ValueError("an instrument cannot have both a Bar and negative evidence")
        event_ids = [item.authority_id for item in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("shadow session event authorities must be unique")
        object.__setattr__(
            self,
            "bars",
            tuple(
                sorted(
                    self.bars,
                    key=lambda item: (
                        item.instrument_id.int,
                        item.source,
                        item.revision,
                        item.authority_id.int,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "missing_execution_inputs",
            tuple(
                sorted(
                    self.missing_execution_inputs,
                    key=lambda item: (item.instrument_id.int, item.source, item.query_hash),
                )
            ),
        )
        object.__setattr__(
            self,
            "events",
            tuple(
                sorted(
                    self.events,
                    key=lambda item: (
                        item.instrument_id.int,
                        item.source,
                        item.provider_event_id,
                        item.revision,
                        item.authority_id.int,
                    ),
                )
            ),
        )


@dataclass(frozen=True)
class ProposalVariantHashInput:
    proposal_id: UUID
    variant: ShadowVariant
    targets: tuple[TargetWeight, ...]
    cash_weight: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "cash_weight", _exact(self.cash_weight, "cash weight"))
        if not Decimal(0) <= self.cash_weight <= Decimal(1):
            raise ValueError("cash weight must be between zero and one")
        target_ids = [item.instrument_id for item in self.targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("proposal targets must be unique")
        total = sum((item.weight for item in self.targets), self.cash_weight)
        if abs(total - Decimal(1)) > RESULT_QUANTUM:
            raise ValueError("proposal target and cash weights must sum to one")
        object.__setattr__(
            self,
            "targets",
            tuple(sorted(self.targets, key=lambda item: item.instrument_id.int)),
        )


@dataclass(frozen=True)
class ShadowFinalGate:
    completed_sessions: int
    complete_checkpoint_ordinals: tuple[int, ...]
    checkpoint_metric_rows: int
    initial_execution_complete: bool
    authority_binding_valid: bool
    has_data_gap: bool
    grace_exhausted: bool

    def __post_init__(self) -> None:
        if not 0 <= self.completed_sessions <= EVALUATION_SESSION_COUNT:
            raise ValueError("completed sessions must be between zero and 63")
        if self.checkpoint_metric_rows < 0 or self.checkpoint_metric_rows > 9:
            raise ValueError("checkpoint metric rows must be between zero and nine")
        ordered = tuple(sorted(set(self.complete_checkpoint_ordinals)))
        if ordered != self.complete_checkpoint_ordinals or any(
            ordinal not in CHECKPOINT_ORDINALS for ordinal in ordered
        ):
            raise ValueError("complete checkpoint ordinals are invalid")
