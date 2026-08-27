"""Strict immutable value objects for the isolated Strategy Shadow."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.backtest.canonical import canonical_decimal
from nexus.evolution.models import _reject_binary_float
from nexus.strategy.baseline import PositionRankState, StrategyDataSnapshot

EVALUATION_SESSION_COUNT = 63
GRACE_SESSION_COUNT = 5
CHECKPOINT_ORDINALS = (5, 21, 63)
INITIAL_NAV = Decimal("100.000000000000")

StrategyShadowRole = Literal["champion", "challenger"]
StrategyShadowGateStatus = Literal["PASS", "FAIL", "UNKNOWN"]
StrategyShadowFinalStatus = Literal["PASS", "FAIL", "UNKNOWN", "STALE"]
StrategyShadowGateKey = Literal[
    "complete_sessions",
    "minimum_full_fills",
    "hard_risk_clear",
    "nonnegative_excess_return",
    "maximum_drawdown_degradation",
]
_GATE_ORDER: tuple[StrategyShadowGateKey, ...] = (
    "complete_sessions",
    "minimum_full_fills",
    "hard_risk_clear",
    "nonnegative_excess_return",
    "maximum_drawdown_degradation",
)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _no_binary_float(cls, value: object) -> object:
        _reject_binary_float(value)
        return value


class StrategyShadowPositionV1(_FrozenModel):
    instrument_id: UUID
    quantity: Decimal = Field(ge=0)
    cost_basis: Decimal = Field(ge=0)
    realized_pnl: Decimal
    cumulative_fees: Decimal = Field(ge=0)
    last_close: Decimal | None = Field(default=None, gt=0)
    untradable: bool

    @field_validator(
        "quantity",
        "cost_basis",
        "realized_pnl",
        "cumulative_fees",
        "last_close",
        mode="after",
    )
    @classmethod
    def _canonical_amount(cls, value: Decimal | None) -> Decimal | None:
        return None if value is None else canonical_decimal(value)


class StrategyShadowStateV1(_FrozenModel):
    cash: Decimal = Field(ge=0)
    nav: Decimal = Field(gt=0)
    positions: tuple[StrategyShadowPositionV1, ...]
    position_rank_states: tuple[PositionRankState, ...]
    hard_risk_violation: bool

    @field_validator("cash", "nav", mode="after")
    @classmethod
    def _canonical_amount(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _canonical_members(self) -> StrategyShadowStateV1:
        instrument_ids = [item.instrument_id for item in self.positions]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("Strategy Shadow positions must be unique")
        rank_ids = [item.instrument_id for item in self.position_rank_states]
        if len(rank_ids) != len(set(rank_ids)) or not set(rank_ids) <= set(instrument_ids):
            raise ValueError("Strategy Shadow rank states must be unique positions")
        object.__setattr__(
            self,
            "positions",
            tuple(sorted(self.positions, key=lambda item: item.instrument_id.int)),
        )
        object.__setattr__(
            self,
            "position_rank_states",
            tuple(
                sorted(
                    self.position_rank_states,
                    key=lambda item: item.instrument_id.int,
                )
            ),
        )
        return self


class StrategyShadowSessionPointV1(_FrozenModel):
    session: date
    ordinal: int = Field(ge=1, le=EVALUATION_SESSION_COUNT)
    nav: Decimal = Field(gt=0)
    turnover: Decimal = Field(ge=0)
    fees: Decimal = Field(ge=0)
    filled_order_count: int = Field(ge=0)
    hard_risk_violation: bool
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("nav", "turnover", "fees", mode="after")
    @classmethod
    def _canonical_amount(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class StrategyShadowExecutionBarV1(_FrozenModel):
    instrument_id: UUID
    raw_price_authority_id: UUID
    adjusted_price_authority_id: UUID
    open: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)

    @field_validator("open", "close", mode="after")
    @classmethod
    def _canonical_price(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class StrategyShadowSessionEvidenceV1(_FrozenModel):
    schema_version: Literal["strategy_shadow_evidence_v1"] = "strategy_shadow_evidence_v1"
    group_id: UUID
    session: date
    session_ordinal: int = Field(ge=1, le=EVALUATION_SESSION_COUNT)
    decision_time: datetime
    calendar_authority_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    universe_snapshot_id: UUID
    strategy_available_at: datetime
    strategy_snapshot: StrategyDataSnapshot
    execution_bars: tuple[StrategyShadowExecutionBarV1, ...]
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _exact_session_scope(self) -> StrategyShadowSessionEvidenceV1:
        if self.strategy_snapshot.next_session != self.session:
            raise ValueError("strategy snapshot must target the exact Shadow session")
        if self.strategy_snapshot.decision_session >= self.session:
            raise ValueError("strategy decision must precede the execution session")
        if self.strategy_available_at > self.decision_time:
            raise ValueError("strategy evidence was unavailable at decision time")
        instrument_ids = [item.instrument_id for item in self.execution_bars]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("Strategy Shadow execution bars must be unique")
        strategy_ids = {item.instrument_id for item in self.strategy_snapshot.instruments}
        if set(instrument_ids) != strategy_ids:
            raise ValueError("execution bars must exactly cover the strategy snapshot")
        object.__setattr__(
            self,
            "execution_bars",
            tuple(sorted(self.execution_bars, key=lambda item: item.instrument_id.int)),
        )
        return self


class StrategyShadowWorstSessionV1(_FrozenModel):
    session: date
    return_value: Decimal

    @field_validator("return_value", mode="after")
    @classmethod
    def _canonical_return(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class StrategyShadowLegMetricsV1(_FrozenModel):
    schema_version: Literal["strategy_shadow_metrics_v1"] = "strategy_shadow_metrics_v1"
    checkpoint_ordinal: Literal[5, 21, 63]
    session_count: int = Field(ge=0, le=EVALUATION_SESSION_COUNT)
    net_return: Decimal | None
    max_drawdown: Decimal | None
    annualized_volatility: Decimal | None
    worst_sessions: tuple[StrategyShadowWorstSessionV1, ...] = Field(max_length=5)
    cumulative_turnover: Decimal
    cumulative_fees: Decimal
    full_fill_count: int = Field(ge=0)
    hard_risk_violation_count: int = Field(ge=0)
    session_authority_hashes: tuple[str, ...] = Field(max_length=EVALUATION_SESSION_COUNT)
    metrics_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "net_return",
        "max_drawdown",
        "annualized_volatility",
        "cumulative_turnover",
        "cumulative_fees",
        mode="after",
    )
    @classmethod
    def _canonical_metric(cls, value: Decimal | None) -> Decimal | None:
        return None if value is None else canonical_decimal(value)

    @model_validator(mode="after")
    def _session_evidence_matches_count(self) -> StrategyShadowLegMetricsV1:
        if len(self.session_authority_hashes) != self.session_count:
            raise ValueError("metric session authority evidence is incomplete")
        return self


class StrategyShadowGateResultV1(_FrozenModel):
    gate: StrategyShadowGateKey
    status: StrategyShadowGateStatus
    observed: tuple[str, ...]
    thresholds: tuple[str, ...]
    evidence_ids: tuple[UUID, ...]
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")


class StrategyShadowFinalGateV1(_FrozenModel):
    schema_version: Literal["strategy_shadow_final_gate_v1"] = "strategy_shadow_final_gate_v1"
    status: StrategyShadowFinalStatus
    champion_generation: int = Field(ge=1)
    active_generation: int = Field(ge=1)
    gates: tuple[StrategyShadowGateResultV1, ...]
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    evidence_ids: tuple[UUID, ...]
    final_authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    finalization_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _fixed_gate_order(self) -> StrategyShadowFinalGateV1:
        if tuple(item.gate for item in self.gates) != _GATE_ORDER:
            raise ValueError("Strategy Shadow gates must use the fixed order")
        return self

    def gate(self, key: StrategyShadowGateKey) -> StrategyShadowGateResultV1:
        for item in self.gates:
            if item.gate == key:
                return item
        raise KeyError(key)


class StrategyShadowStartResultV1(_FrozenModel):
    group_id: UUID
    experiment_id: UUID
    status: Literal["running"] = "running"
    start_session: date
    evaluation_end_session: date
    grace_end_session: date
    initial_nav: Decimal = INITIAL_NAV
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    live_execution_enabled: Literal[False] = False


class StrategyShadowAdvanceResultV1(_FrozenModel):
    group_id: UUID
    requested_session: date
    ordinal: int | None
    status: Literal["applied", "duplicate", "failed", "sealed"]
    reason_code: str
    checkpoint_ordinal: Literal[5, 21, 63] | None = None
    live_execution_enabled: Literal[False] = False


class StrategyShadowFinalizationResultV1(_FrozenModel):
    group_id: UUID
    finalization_id: UUID
    status: StrategyShadowFinalStatus
    family_state: Literal["PROMOTION_ELIGIBLE", "SHADOW_FAILED", "STALE_CHAMPION_BASELINE"]
    reason_code: str
    live_execution_enabled: Literal[False] = False


def require_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if normalized.utcoffset() != value.utcoffset():
        raise ValueError(f"{field} must use UTC")
    return normalized
