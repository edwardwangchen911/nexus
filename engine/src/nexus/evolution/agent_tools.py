"""Fixed typed read-only tools for the bounded Strategy R&D Agent."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.evolution.models import RegisteredMutationAxisV1

EXPECTED_STRATEGY_RND_TOOL_NAMES: Final[tuple[str, ...]] = (
    "get_strategy_parent",
    "get_evolution_search_space",
    "get_training_validation_summary",
    "get_sealed_experiment_summary",
)


class StrategyRNDToolError(ValueError):
    """A model tool request or result crossed the fixed Strategy R&D boundary."""


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class StrategyRNDToolContext(_FrozenModel):
    """Server-owned identities that every model tool request must match exactly."""

    parent_strategy_definition_id: UUID
    champion_generation: int = Field(ge=1)
    research_lineage_id: UUID
    evolution_policy_id: UUID
    training_validation_summary_id: UUID
    as_of: datetime
    sealed_experiment_ids: tuple[UUID, ...]
    current_family_experiment_ids: tuple[UUID, ...]

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _experiment_sets_are_unique_and_disjoint(self) -> StrategyRNDToolContext:
        if len(self.sealed_experiment_ids) != len(set(self.sealed_experiment_ids)):
            raise ValueError("sealed experiment identities must be unique")
        if len(self.current_family_experiment_ids) != len(
            set(self.current_family_experiment_ids)
        ):
            raise ValueError("current-family experiment identities must be unique")
        if set(self.sealed_experiment_ids) & set(self.current_family_experiment_ids):
            raise ValueError("current-family experiments cannot be sealed inputs")
        return self


class _ToolInput(_FrozenModel):
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class _StrategyParentInput(_ToolInput):
    strategy_definition_id: UUID
    champion_generation: int = Field(ge=1)


class _EvolutionSearchSpaceInput(_ToolInput):
    policy_id: UUID


class _TrainingValidationSummaryInput(_ToolInput):
    summary_id: UUID


class _SealedExperimentSummaryInput(_ToolInput):
    experiment_id: UUID


class _StrategyParentOutput(_FrozenModel):
    strategy_definition_id: UUID
    strategy_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    expression_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    registered_parameters: dict[
        Literal["min_adv20_usd", "min_close", "target_weight"], str
    ]
    ranking_feature: Literal["momentum_126_skip5", "return_126"]
    positive_return_filter_enabled: bool

    @model_validator(mode="after")
    def _exact_parameters(self) -> _StrategyParentOutput:
        if set(self.registered_parameters) != {
            "min_adv20_usd",
            "min_close",
            "target_weight",
        }:
            raise ValueError("parent summary requires exact registered parameters")
        return self


class _EvolutionSearchSpaceOutput(_FrozenModel):
    policy_id: UUID
    policy_version: Literal["evolution_search_space_v1"]
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    axes: tuple[RegisteredMutationAxisV1, ...] = Field(min_length=1, max_length=5)


class _TrainingValidationSummaryOutput(_FrozenModel):
    summary_id: UUID
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    quality_status: Literal["PASS", "FAIL"]
    training_window_count: int = Field(ge=1, le=100)
    validation_window_count: int = Field(ge=1, le=100)
    training_return: str
    validation_return: str
    training_sharpe: str
    validation_sharpe: str
    maximum_drawdown: str
    evidence_refs: tuple[UUID, ...] = Field(min_length=1, max_length=32)


class _SealedExperimentSummaryOutput(_FrozenModel):
    experiment_id: UUID
    candidate_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    summary_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal[
        "BACKTEST_FAILED",
        "BACKTEST_PASSED",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    ]
    gate_status: Literal["PASS", "FAIL", "UNKNOWN"]
    evidence_refs: tuple[UUID, ...] = Field(min_length=1, max_length=32)


_TOOL_INPUTS: Final[Mapping[str, type[_ToolInput]]] = {
    "get_strategy_parent": _StrategyParentInput,
    "get_evolution_search_space": _EvolutionSearchSpaceInput,
    "get_training_validation_summary": _TrainingValidationSummaryInput,
    "get_sealed_experiment_summary": _SealedExperimentSummaryInput,
}
_TOOL_OUTPUTS: Final[Mapping[str, type[_FrozenModel]]] = {
    "get_strategy_parent": _StrategyParentOutput,
    "get_evolution_search_space": _EvolutionSearchSpaceOutput,
    "get_training_validation_summary": _TrainingValidationSummaryOutput,
    "get_sealed_experiment_summary": _SealedExperimentSummaryOutput,
}

type StrategyRNDToolExecutor = Callable[
    [dict[str, object]], Mapping[str, object] | Awaitable[Mapping[str, object]]
]


class StrategyRNDTools:
    """Validate frozen requests and typed results around injected local read lookups."""

    names: tuple[str, ...] = EXPECTED_STRATEGY_RND_TOOL_NAMES

    def __init__(
        self,
        *,
        context: StrategyRNDToolContext,
        executors: Mapping[str, StrategyRNDToolExecutor] | None = None,
    ) -> None:
        unknown = set(executors or ()) - set(self.names)
        if unknown:
            raise StrategyRNDToolError("executor is not in the fixed Strategy R&D catalogue")
        self.context = context
        self._executors = dict(executors or {})

    def validate(self, name: str, payload: Mapping[str, object]) -> dict[str, object]:
        schema = _TOOL_INPUTS.get(name)
        if schema is None:
            raise StrategyRNDToolError("tool is not in the fixed Strategy R&D catalogue")
        try:
            validated = schema.model_validate(payload)
        except ValidationError as error:
            raise StrategyRNDToolError("tool payload is not a strict typed request") from error
        if validated.as_of != self.context.as_of:
            raise StrategyRNDToolError("tool request does not use the frozen as_of")
        self._validate_context_binding(name, validated)
        return validated.model_dump(mode="python")

    def _validate_context_binding(self, name: str, value: _ToolInput) -> None:
        if name == "get_strategy_parent":
            parent = value
            if not isinstance(parent, _StrategyParentInput) or (
                parent.strategy_definition_id != self.context.parent_strategy_definition_id
                or parent.champion_generation != self.context.champion_generation
            ):
                raise StrategyRNDToolError("strategy parent identity is not frozen")
            return
        if name == "get_evolution_search_space":
            if not isinstance(value, _EvolutionSearchSpaceInput) or (
                value.policy_id != self.context.evolution_policy_id
            ):
                raise StrategyRNDToolError("evolution search-space identity is not frozen")
            return
        if name == "get_training_validation_summary":
            if not isinstance(value, _TrainingValidationSummaryInput) or (
                value.summary_id != self.context.training_validation_summary_id
            ):
                raise StrategyRNDToolError("training-validation identity is not frozen")
            return
        if not isinstance(value, _SealedExperimentSummaryInput) or (
            value.experiment_id not in self.context.sealed_experiment_ids
            or value.experiment_id in self.context.current_family_experiment_ids
        ):
            raise StrategyRNDToolError("sealed experiment is not an admitted historical input")

    async def execute(
        self, name: str, payload: Mapping[str, object]
    ) -> Mapping[str, object]:
        validated = self.validate(name, payload)
        executor = self._executors.get(name)
        if executor is None:
            raise StrategyRNDToolError("tool has no configured read-only executor")
        result = executor(validated)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, Mapping):
            raise StrategyRNDToolError("tool result must be a mapping")
        try:
            output = _TOOL_OUTPUTS[name].model_validate(result)
        except ValidationError as error:
            raise StrategyRNDToolError("tool result is not a strict typed response") from error
        return output.model_dump(mode="json")


def strategy_rnd_function_tools() -> Mapping[str, dict[str, object]]:
    """Return four strict custom-function schemas and no built-in capability."""
    return {
        name: {
            "type": "function",
            "name": name,
            "description": "Read-only frozen Strategy R&D authority lookup.",
            "parameters": schema.model_json_schema(),
            "strict": True,
        }
        for name, schema in _TOOL_INPUTS.items()
    }
