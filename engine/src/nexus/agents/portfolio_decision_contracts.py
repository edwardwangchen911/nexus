"""Closed contracts for read-only portfolio decision research."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.domain.decimal import RatioDecimalString
from nexus.domain.time import require_utc
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import AdjustmentScenarioConstraintsV1, ScenarioObjective

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ObjectiveStatus = Literal["met", "improved_not_met", "unchanged", "infeasible"]
HardConstraintReason = Literal[
    "TURNOVER_BUDGET_EXHAUSTED",
    "STRUCTURAL_CONCENTRATION_FLOOR",
    "SELLABLE_QUANTITY_EXHAUSTED",
]
CostGapReason = Literal[
    "BROKER_COMMISSION_SCHEDULE_UNAVAILABLE",
    "BID_ASK_EVIDENCE_UNAVAILABLE",
    "SLIPPAGE_MODEL_UNAVAILABLE",
    "REGULATORY_FEE_SCHEDULE_UNAVAILABLE",
    "BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE",
    "TAX_DATA_UNAVAILABLE",
]
CitationIds = Annotated[tuple[Hash64, ...], Field(min_length=1, max_length=100)]

_UUID_TEXT = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_RATIO_TEXT = re.compile(r"^(?:0(?:\.\d{1,38})?|1(?:\.0{1,38})?)$")
_UNSAFE_TEXT = re.compile(
    r"(?:https?://|file://|\.\.[/\\]|(?:^|\s)/(?:Users|home|tmp|etc)/|"
    r"\b(?:select|insert|update|delete|drop|alter)\s+|"
    r"\b(?:weight|quantity|amount|order)\b|"
    r"(?:^|\s)(?:sudo|curl|wget|bash|zsh|sh)\s+|`|\$\()",
    re.IGNORECASE,
)


class _StrictPortfolioDecisionModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def _reject_binary_floats(cls, value: object) -> object:
        _reject_float(value)
        return value


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("portfolio decision contracts reject binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_float(nested)


def _safe_research_text(value: str) -> str:
    if _UUID_TEXT.search(value) or _UNSAFE_TEXT.search(value):
        raise ValueError("research text contains forbidden executable or identity material")
    return value


def _require_unique(values: Iterable[str], *, label: str) -> None:
    items = tuple(values)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must be unique")


def canonical_portfolio_decision_hash(value: BaseModel | Mapping[str, object]) -> str:
    """Hash one portfolio decision document without lossy numeric conversion."""
    payload: object = value.model_dump(mode="python") if isinstance(value, BaseModel) else value
    if not isinstance(payload, Mapping):
        raise TypeError("portfolio decision documents must be mappings")
    encoded = json.dumps(
        _canonical_value(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_value(value: object) -> Any:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        raise TypeError("portfolio decision documents reject binary floats")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("portfolio decision decimals must be finite")
        rendered = format(value, "f")
        rendered = rendered.rstrip("0").rstrip(".") if "." in rendered else rendered
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
            raise TypeError("portfolio decision document keys must be strings")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported portfolio decision value: {type(value).__name__}")


class PortfolioDecisionInputV1(_StrictPortfolioDecisionModel):
    schema_version: Literal["portfolio_decision_input_v1"]
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    objective: ScenarioObjective
    max_turnover: RatioDecimalString
    minimum_cash: RatioDecimalString
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]
    scenario_request_hash: Hash64
    expected_scenario_result_hash: Hash64
    diagnostic_input_hash: Hash64
    policy_version: str = Field(min_length=1, max_length=100)
    evidence_hash: Hash64

    @field_validator("decision_time")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class EvaluateAdjustmentScenarioToolInputV1(_StrictPortfolioDecisionModel):
    objective: ScenarioObjective


class AgentScenarioMetricSetV1(_StrictPortfolioDecisionModel):
    security_top1: TypedMetric
    security_top3: TypedMetric
    security_hhi: TypedMetric
    cash_weight: TypedMetric
    one_way_turnover: TypedMetric
    estimated_cost: TypedMetric


class AgentScenarioProjectionV1(_StrictPortfolioDecisionModel):
    schema_version: Literal["agent_scenario_projection_v1"]
    objective: ScenarioObjective
    objective_status: ObjectiveStatus
    analysis_status: Literal["partial"]
    baseline: AgentScenarioMetricSetV1
    candidate: AgentScenarioMetricSetV1
    unknown_metrics: tuple[Literal["estimated_cost"], ...] = Field(max_length=1)
    cost_gap_codes: tuple[CostGapReason, ...] = Field(default=(), max_length=5)
    hard_constraint_reasons: tuple[HardConstraintReason, ...] = Field(max_length=20)
    changed_position_count: int = Field(ge=0, le=100)
    result_hash: Hash64
    citation_ids: CitationIds
    execution_locked: Literal[True]

    @model_validator(mode="after")
    def _validate_projection(self) -> AgentScenarioProjectionV1:
        _require_unique(self.unknown_metrics, label="unknown metrics")
        _require_unique(self.cost_gap_codes, label="cost gap codes")
        _require_unique(self.citation_ids, label="citation ids")
        _require_unique(self.hard_constraint_reasons, label="hard constraint reasons")
        for metrics in (self.baseline, self.candidate):
            for name in (
                "security_top1",
                "security_top3",
                "security_hhi",
                "cash_weight",
                "one_way_turnover",
            ):
                metric = getattr(metrics, name)
                if metric.status != "known" or metric.value is None:
                    raise ValueError("projected risk and allocation metrics must be known")
                if metric.value < 0 or metric.value > 1:
                    raise ValueError("projected risk and allocation metrics must be ratios")
            cost = metrics.estimated_cost
            if cost.status == "known" and (cost.value is None or cost.value < 0):
                raise ValueError("known estimated cost must be nonnegative")
            if cost.status == "unknown" and cost.reason != "COST_EVIDENCE_INCOMPLETE":
                raise ValueError("unknown estimated cost requires the allowlisted reason")
        actual_unknowns = {
            name
            for name in ("estimated_cost",)
            if getattr(self.baseline, name).status == "unknown"
            or getattr(self.candidate, name).status == "unknown"
        }
        if set(self.unknown_metrics) != actual_unknowns:
            raise ValueError("unknown metrics must match projected metric status")
        return self


class PortfolioDiagnosticSummaryV1(_StrictPortfolioDecisionModel):
    code: Literal["CONCENTRATION_TOP1", "CONCENTRATION_TOP3", "CONCENTRATION_HHI"]
    severity: Literal["info", "warning", "critical"]
    metric: TypedMetric
    unit: Literal["ratio"]
    citation_ids: CitationIds

    @model_validator(mode="after")
    def _require_known_ratio(self) -> PortfolioDiagnosticSummaryV1:
        if self.metric.status != "known" or self.metric.value is None:
            raise ValueError("portfolio decision diagnostics must be known")
        if self.metric.value < 0 or self.metric.value > 1:
            raise ValueError("portfolio decision diagnostics must be ratios")
        _require_unique(self.citation_ids, label="citation ids")
        return self


class PortfolioDecisionModelContextV1(_StrictPortfolioDecisionModel):
    schema_version: Literal["portfolio_decision_model_context_v1"]
    run_id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    objective: ScenarioObjective
    constraints: AdjustmentScenarioConstraintsV1
    current_diagnostics: tuple[PortfolioDiagnosticSummaryV1, ...] = Field(
        min_length=3, max_length=3
    )
    citation_ids: CitationIds
    execution_locked: Literal[True]

    @field_validator("decision_time")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _validate_diagnostics(self) -> PortfolioDecisionModelContextV1:
        if self.constraints.objective != self.objective:
            raise ValueError("context objective must match constraints")
        expected = {"CONCENTRATION_TOP1", "CONCENTRATION_TOP3", "CONCENTRATION_HHI"}
        if {item.code for item in self.current_diagnostics} != expected:
            raise ValueError("context requires all concentration diagnostics")
        _require_unique(self.citation_ids, label="citation ids")
        allowed = set(self.citation_ids)
        if any(not set(item.citation_ids) <= allowed for item in self.current_diagnostics):
            raise ValueError("diagnostic citation must be in context authority")
        return self


class PortfolioResearchClaimV1(_StrictPortfolioDecisionModel):
    text: str = Field(min_length=1, max_length=500)
    citation_ids: Annotated[tuple[Hash64, ...], Field(min_length=1, max_length=5)]

    @field_validator("text")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _safe_research_text(value)


class PortfolioInvalidationConditionV1(_StrictPortfolioDecisionModel):
    signal: Literal[
        "portfolio.security_top1",
        "portfolio.cash_weight",
        "portfolio.scenario_objective_status",
        "portfolio.estimated_cost_status",
    ]
    operator: Literal["lt", "lte", "eq", "gte", "gt"]
    threshold: str = Field(min_length=1, max_length=40)
    unit: Literal["ratio", "state"]
    citation_ids: Annotated[tuple[Hash64, ...], Field(min_length=1, max_length=5)]

    @model_validator(mode="after")
    def _validate_closed_matrix(self) -> PortfolioInvalidationConditionV1:
        if self.signal in {"portfolio.security_top1", "portfolio.cash_weight"}:
            if self.unit != "ratio":
                raise ValueError("numeric invalidation signals require ratio unit")
            if _RATIO_TEXT.fullmatch(self.threshold) is None:
                raise ValueError("numeric invalidation threshold must be decimal ratio text")
            threshold = Decimal(self.threshold)
            if not threshold.is_finite() or threshold < 0 or threshold > 1:
                raise ValueError("numeric invalidation threshold must be a ratio")
            return self

        allowed_states = (
            {"met", "improved_not_met", "unchanged", "infeasible"}
            if self.signal == "portfolio.scenario_objective_status"
            else {"known", "unknown"}
        )
        if self.operator != "eq" or self.unit != "state" or self.threshold not in allowed_states:
            raise ValueError("state invalidation signal requires matching equality state")
        return self


class PortfolioRatioInvalidationConditionV1(PortfolioInvalidationConditionV1):
    signal: Literal["portfolio.security_top1", "portfolio.cash_weight"]
    threshold: Annotated[
        str,
        StringConstraints(min_length=1, max_length=40, pattern=_RATIO_TEXT.pattern),
    ]
    unit: Literal["ratio"]


class PortfolioScenarioStatusInvalidationConditionV1(PortfolioInvalidationConditionV1):
    signal: Literal["portfolio.scenario_objective_status"]
    operator: Literal["eq"]
    threshold: Literal["met", "improved_not_met", "unchanged", "infeasible"]
    unit: Literal["state"]


class PortfolioEstimatedCostStatusInvalidationConditionV1(PortfolioInvalidationConditionV1):
    signal: Literal["portfolio.estimated_cost_status"]
    operator: Literal["eq"]
    threshold: Literal["known", "unknown"]
    unit: Literal["state"]


PortfolioInvalidationConditionSchemaV1 = (
    PortfolioRatioInvalidationConditionV1
    | PortfolioScenarioStatusInvalidationConditionV1
    | PortfolioEstimatedCostStatusInvalidationConditionV1
)


class PortfolioLimitationV1(_StrictPortfolioDecisionModel):
    code: Literal["estimated_cost"]
    text: str = Field(min_length=1, max_length=300)

    @field_validator("text")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _safe_research_text(value)


class PortfolioResearchIntentV1(_StrictPortfolioDecisionModel):
    schema_version: Literal["portfolio_research_intent_v1"]
    run_id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    assessment: Literal["explore_adjustment", "observe"]
    objective: ScenarioObjective
    horizon: Literal["short_term", "medium_term", "long_term"]
    confidence: Literal["low", "medium", "high"]
    rationale: tuple[PortfolioResearchClaimV1, ...] = Field(min_length=1, max_length=4)
    invalidation_conditions: tuple[PortfolioInvalidationConditionSchemaV1, ...] = Field(
        min_length=1, max_length=5
    )
    limitations: tuple[PortfolioLimitationV1, ...] = Field(max_length=8)
    scenario_result_hash: Hash64
    citation_ids: CitationIds
    execution_locked: Literal[True]

    @field_validator("decision_time")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _validate_internal_citations(self) -> PortfolioResearchIntentV1:
        _require_unique(self.citation_ids, label="citation ids")
        _require_unique((item.code for item in self.limitations), label="limitation codes")
        allowed = set(self.citation_ids)
        if any(not set(item.citation_ids) <= allowed for item in self.rationale) or any(
            not set(item.citation_ids) <= allowed for item in self.invalidation_conditions
        ):
            raise ValueError("nested citation must be in intent citation ids")
        return self


def validate_portfolio_intent(
    intent: PortfolioResearchIntentV1,
    *,
    frozen: PortfolioDecisionInputV1,
    projection: AgentScenarioProjectionV1,
    allowed_citation_ids: frozenset[str],
) -> PortfolioResearchIntentV1:
    """Bind a model intent to one frozen run and its deterministic projection."""
    if (
        intent.portfolio_id != frozen.portfolio_id
        or intent.portfolio_snapshot_id != frozen.portfolio_snapshot_id
        or intent.diagnostic_report_id != frozen.diagnostic_report_id
        or intent.decision_time != frozen.decision_time
        or intent.objective != frozen.objective
        or projection.objective != frozen.objective
    ):
        raise ValueError("intent identity does not match frozen authority")
    if (
        intent.scenario_result_hash != projection.result_hash
        or intent.scenario_result_hash != frozen.expected_scenario_result_hash
    ):
        raise ValueError("scenario result hash does not match")
    if intent.assessment == "explore_adjustment" and projection.objective_status not in {
        "met",
        "improved_not_met",
    }:
        raise ValueError("assessment contradicts scenario status")
    if {item.code for item in intent.limitations} != set(projection.unknown_metrics):
        raise ValueError("limitations must exactly cover unknown metrics")
    cited = set(intent.citation_ids)
    projection_citations = set(projection.citation_ids)
    if not cited <= allowed_citation_ids or not projection_citations <= allowed_citation_ids:
        raise ValueError("citation is not in frozen authority")
    return intent
