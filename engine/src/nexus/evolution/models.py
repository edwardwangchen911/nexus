"""Strict data-only contracts at the strategy R&D Agent boundary."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictStr,
    StringConstraints,
    field_validator,
    model_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
RegisteredMutationAxisName = Literal[
    "min_adv20_usd",
    "min_close",
    "target_weight",
    "ranking_feature",
    "positive_return_filter_enabled",
]
MutationValue = StrictStr | StrictBool

_UNSAFE_TEXT = re.compile(
    r"(?:https?://|file://|\.\./|\.\.\\|~/|"
    r"\b(?:python|sql|shell|import|exec|eval|subprocess|broker|credential|api[_ -]?key)\b|"
    r"\b(?:place|submit|cancel|modify)\s+(?:an?\s+)?(?:live\s+)?order\b|"
    r"\b(?:change|modify|override|bypass)\s+(?:the\s+)?(?:hard\s+)?risk\s+limits?\b|"
    r"os\.system)",
    re.IGNORECASE,
)


def _reject_binary_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("strategy evolution values must not contain binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_binary_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_binary_float(nested)


def _require_safe_text(value: str) -> str:
    if _UNSAFE_TEXT.search(value):
        raise ValueError("strategy evolution text contains executable, URL, or path content")
    return value


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _exact_values_only(cls, value: object) -> object:
        _reject_binary_float(value)
        return value


class RegisteredMutationAxisV1(_StrictFrozenModel):
    """Agent-selected values for one server-registered mutation axis."""

    axis: RegisteredMutationAxisName
    values: tuple[MutationValue, ...] = Field(min_length=1, max_length=8)

    @field_validator("values")
    @classmethod
    def _safe_unique_values(
        cls, values: tuple[MutationValue, ...]
    ) -> tuple[MutationValue, ...]:
        identities: list[tuple[str, str | bool]] = []
        for value in values:
            if isinstance(value, str):
                _require_safe_text(value)
                if not value or len(value) > 100:
                    raise ValueError("mutation string values must contain 1 to 100 characters")
                identities.append(("str", value))
            else:
                identities.append(("bool", value))
        if len(identities) != len(set(identities)):
            raise ValueError("mutation values must be unique")
        return values


class RegisteredMutationV1(_StrictFrozenModel):
    """One normalized server-registered mutation value."""

    axis: RegisteredMutationAxisName
    value: MutationValue

    @field_validator("value")
    @classmethod
    def _safe_value(cls, value: MutationValue) -> MutationValue:
        if isinstance(value, str):
            _require_safe_text(value)
            if not value or len(value) > 100:
                raise ValueError("mutation string values must contain 1 to 100 characters")
        return value


class StrategyExperimentProposalV1(_StrictFrozenModel):
    """The complete data-only output allowed from the fixed Strategy R&D Agent."""

    schema_version: Literal["strategy_experiment_proposal_v1"]
    parent_strategy_definition_id: UUID
    champion_generation: int = Field(ge=1)
    research_lineage_id: UUID
    hypothesis: str = Field(min_length=8, max_length=500)
    expected_mechanism: str = Field(min_length=8, max_length=500)
    invalidation_conditions: tuple[str, ...] = Field(min_length=1, max_length=8)
    evidence_refs: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    mutation_axes: tuple[RegisteredMutationAxisV1, ...] = Field(min_length=1, max_length=3)

    @field_validator("hypothesis", "expected_mechanism")
    @classmethod
    def _safe_narrative(cls, value: str) -> str:
        return _require_safe_text(value)

    @field_validator("invalidation_conditions")
    @classmethod
    def _safe_invalidation_conditions(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        for value in values:
            if not value or len(value) > 250:
                raise ValueError("invalidation conditions must contain 1 to 250 characters")
            _require_safe_text(value)
        if len(values) != len(set(values)):
            raise ValueError("invalidation conditions must be unique")
        return values

    @model_validator(mode="after")
    def _unique_references_and_axes(self) -> StrategyExperimentProposalV1:
        if len(self.evidence_refs) != len(set(self.evidence_refs)):
            raise ValueError("evidence references must be unique")
        axis_names = [axis.axis for axis in self.mutation_axes]
        if len(axis_names) != len(set(axis_names)):
            raise ValueError("mutation axes must be unique")
        return self


class StrategyCandidateDeltaV1(_StrictFrozenModel):
    """A service-generated candidate identity; it contains no arbitrary paths or AST."""

    schema_version: Literal["strategy_candidate_delta_v1"] = "strategy_candidate_delta_v1"
    parent_expression_hash: Hash64
    ordinal: int = Field(ge=1, le=24)
    mutations: tuple[RegisteredMutationV1, ...] = Field(min_length=1, max_length=3)
    candidate_hash: Hash64

    @model_validator(mode="after")
    def _normalized_unique_mutations(self) -> StrategyCandidateDeltaV1:
        names = tuple(mutation.axis for mutation in self.mutations)
        if len(names) != len(set(names)):
            raise ValueError("candidate mutation axes must be unique")
        if names != tuple(sorted(names)):
            raise ValueError("candidate mutations must use canonical axis order")
        return self
