"""Closed contracts for deterministic, read-only portfolio scenarios."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.optimizer.compare import TypedMetric

ScenarioObjective = Literal["reduce_security_concentration"]
ScenarioObjectiveStatus = Literal["met", "improved_not_met", "unchanged", "infeasible"]

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HASH_PATTERN = r"^[0-9a-f]{64}$"


class _StrictScenarioModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def _reject_binary_floats(cls, value: object) -> object:
        _reject_float(value)
        return value


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("scenario contracts reject binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_float(nested)


class AdjustmentScenarioConstraintsV1(_StrictScenarioModel):
    schema_version: Literal["adjustment_scenario_constraints_v1"]
    objective: ScenarioObjective
    max_turnover: Decimal = Field(ge=_ZERO, le=_ONE)
    minimum_cash: Decimal = Field(ge=_ZERO, le=_ONE)
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]


class PortfolioScenarioRequestV1(_StrictScenarioModel):
    schema_version: Literal["portfolio_scenario_request_v1"]
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    as_of: datetime
    constraints: AdjustmentScenarioConstraintsV1

    @field_validator("as_of")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class ScenarioPositionV1(_StrictScenarioModel):
    instrument_id: UUID
    current_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    target_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    action: Literal["reduce", "maintain"]
    reason_code: Literal["CONCENTRATION_REDUCTION", "BASELINE_WEIGHT"]

    @model_validator(mode="after")
    def _weights_match_action(self) -> ScenarioPositionV1:
        if self.target_weight > self.current_weight:
            raise ValueError("scenario position cannot increase weight")
        expected_action = "reduce" if self.target_weight < self.current_weight else "maintain"
        expected_reason = (
            "CONCENTRATION_REDUCTION" if expected_action == "reduce" else "BASELINE_WEIGHT"
        )
        if self.action != expected_action or self.reason_code != expected_reason:
            raise ValueError("scenario position action and reason must match weights")
        return self


class ScenarioMetricSetV1(_StrictScenarioModel):
    security_top1: TypedMetric
    security_top3: TypedMetric
    security_hhi: TypedMetric
    cash_weight: TypedMetric
    one_way_turnover: TypedMetric
    estimated_cost: TypedMetric


class PortfolioScenarioEvaluationV1(_StrictScenarioModel):
    schema_version: Literal["portfolio_scenario_evaluation_v1"]
    request_hash: str = Field(pattern=_HASH_PATTERN)
    result_hash: str = Field(pattern=_HASH_PATTERN)
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    diagnostic_input_hash: str = Field(pattern=_HASH_PATTERN)
    valuation_time: datetime
    objective: ScenarioObjective
    analysis_status: Literal["partial"]
    objective_status: ScenarioObjectiveStatus
    baseline: ScenarioMetricSetV1
    candidate: ScenarioMetricSetV1
    positions: tuple[ScenarioPositionV1, ...] = Field(min_length=1, max_length=100)
    cash_current_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    cash_target_weight: Decimal = Field(ge=_ZERO, le=_ONE)
    hard_constraint_reasons: tuple[str, ...]
    unknown_metrics: tuple[str, ...]
    execution_locked: Literal[True]

    @field_validator("valuation_time")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _balanced_reduce_only_result(self) -> PortfolioScenarioEvaluationV1:
        if len({position.instrument_id for position in self.positions}) != len(self.positions):
            raise ValueError("scenario positions require unique instruments")
        current_total = sum(
            (position.current_weight for position in self.positions), self.cash_current_weight
        )
        target_total = sum(
            (position.target_weight for position in self.positions), self.cash_target_weight
        )
        if current_total != _ONE or target_total != _ONE:
            raise ValueError("scenario current and target weights must each sum to one")
        if (
            self.baseline.cash_weight.status != "known"
            or self.baseline.cash_weight.value != self.cash_current_weight
            or self.candidate.cash_weight.status != "known"
            or self.candidate.cash_weight.value != self.cash_target_weight
        ):
            raise ValueError("scenario cash metrics must match cash weights")
        return self


def canonical_scenario_hash(payload: BaseModel | Mapping[str, object]) -> str:
    """Hash sorted compact JSON with stable financial and identity rendering."""
    value: object = payload.model_dump(mode="python") if isinstance(payload, BaseModel) else payload
    if not isinstance(value, Mapping):
        raise TypeError("canonical scenario documents must be mappings")
    root = {key: item for key, item in value.items() if key != "result_hash"}
    encoded = json.dumps(
        _canonical_value(root), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_value(value: object) -> Any:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        raise TypeError("canonical scenario documents reject binary floats")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical scenario decimals must be finite")
        rendered = format(value, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return "0" if rendered in {"", "-0"} else rendered
    if isinstance(value, UUID):
        return str(value).lower()
    if isinstance(value, datetime):
        return require_utc(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical scenario document keys must be strings")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical scenario value: {type(value).__name__}")
