"""Immutable forward strategy decisions consumed by deterministic risk."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")


def _exact_decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain a binary float")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"{field} must be a Decimal")
    result = Decimal(value)
    if not result.is_finite() or abs(result) >= _MAX_ABSOLUTE:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)")
    try:
        with localcontext() as context:
            context.prec = 50
            canonical = result.quantize(_QUANTUM)
    except InvalidOperation as error:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)") from error
    if canonical != result:
        raise ValueError(f"{field} must have no more than 12 decimal places")
    return result


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ForwardStrategyTarget(_FrozenModel):
    instrument_id: UUID
    target_weight: Decimal
    exit: bool
    strategy_priority: int = Field(ge=0, le=2)
    reason: str = Field(min_length=1, max_length=100)

    @field_validator("target_weight", mode="before")
    @classmethod
    def _weight_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="target_weight")

    @model_validator(mode="after")
    def _valid_target(self) -> ForwardStrategyTarget:
        if not Decimal(0) <= self.target_weight <= Decimal(1):
            raise ValueError("target weight must be between zero and one")
        if self.exit and (self.target_weight != Decimal(0) or self.strategy_priority != 0):
            raise ValueError("exit targets require zero weight and priority zero")
        if not self.exit and self.strategy_priority == 0:
            raise ValueError("non-exit targets cannot use exit priority")
        return self


class ForwardStrategyDecision(_FrozenModel):
    account_id: UUID
    portfolio_id: UUID
    strategy_id: Literal["baseline"]
    strategy_version: Literal["baseline_v1"]
    decision_session: date
    decision_time: datetime
    execution_session: date
    source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID
    portfolio_projection_hash: Hash64
    authority_hash: Hash64
    strategy_parameters_hash: Hash64
    decision_hash: Hash64
    cash_weight: Decimal
    reason: Literal["WEEKLY_REBALANCE"]
    targets: tuple[ForwardStrategyTarget, ...]

    @field_validator("cash_weight", mode="before")
    @classmethod
    def _cash_weight_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="cash_weight")

    @field_validator("decision_time")
    @classmethod
    def _decision_time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("decision time must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _valid_decision(self) -> ForwardStrategyDecision:
        if self.execution_session <= self.decision_session:
            raise ValueError("execution session must follow decision session")
        instrument_ids = [target.instrument_id for target in self.targets]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("forward decision targets must be unique")
        total_weight = sum(
            (target.target_weight for target in self.targets),
            start=self.cash_weight,
        )
        if total_weight != Decimal(1):
            raise ValueError("target and cash weights must equal one")
        return self
