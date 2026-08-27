"""Immutable built-in authorities for bounded strategy evolution."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import Decimal
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, model_validator

from nexus.audit.canonical import canonical_json
from nexus.evolution.models import MutationValue, RegisteredMutationAxisName, _reject_binary_float


class EvolutionPolicyError(ValueError):
    """A persisted strategy-evolution policy does not match the released authority."""


class _StrictFrozenPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _exact_values_only(cls, value: object) -> object:
        _reject_binary_float(value)
        return value


class CandidateBudgetPolicyV1(_StrictFrozenPolicy):
    version: Literal["candidate_budget_policy_v1"] = "candidate_budget_policy_v1"
    max_axes: Literal[3] = 3
    max_candidates: Literal[24] = 24
    max_oos_candidates: Literal[3] = 3


class MutationAxisPolicyV1(_StrictFrozenPolicy):
    axis: RegisteredMutationAxisName
    value_kind: Literal["decimal_string", "feature_name", "boolean"]
    allowed_values: tuple[StrictStr | StrictBool, ...] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def _values_match_kind(self) -> MutationAxisPolicyV1:
        if self.value_kind == "boolean" and any(
            not isinstance(value, bool) for value in self.allowed_values
        ):
            raise ValueError("boolean mutation axes require strict boolean values")
        if self.value_kind != "boolean" and any(
            not isinstance(value, str) for value in self.allowed_values
        ):
            raise ValueError("string mutation axes require strict string values")
        if len(self.allowed_values) != len(set(self.allowed_values)):
            raise ValueError("search-space values must be unique")
        return self

    def allows(self, value: MutationValue) -> bool:
        return any(
            type(value) is type(allowed) and value == allowed for allowed in self.allowed_values
        )


class EvolutionSearchSpaceV1(_StrictFrozenPolicy):
    version: Literal["evolution_search_space_v1"] = "evolution_search_space_v1"
    axes: tuple[MutationAxisPolicyV1, ...]

    @model_validator(mode="after")
    def _exact_registered_axes(self) -> EvolutionSearchSpaceV1:
        names = tuple(axis.axis for axis in self.axes)
        if len(names) != len(set(names)):
            raise ValueError("search-space axes must be unique")
        return self

    def axis(self, name: RegisteredMutationAxisName) -> MutationAxisPolicyV1:
        for definition in self.axes:
            if definition.axis == name:
                return definition
        raise EvolutionPolicyError("MUTATION_AXIS_NOT_REGISTERED")


class PromotionPolicyV1(_StrictFrozenPolicy):
    version: Literal["promotion_policy_v1"] = "promotion_policy_v1"
    minimum_oos_windows: Literal[8] = 8
    minimum_deflated_sharpe_confidence: Decimal = Decimal("0.95")
    minimum_sharpe_improvement: Decimal = Decimal("0.15")
    minimum_annual_return_improvement: Decimal = Decimal("0.01")
    maximum_drawdown_degradation: Decimal = Decimal("0.02")
    cost_stress_multiplier: Decimal = Decimal("2")


@lru_cache(maxsize=1)
def candidate_budget_policy_v1() -> CandidateBudgetPolicyV1:
    return CandidateBudgetPolicyV1()


@lru_cache(maxsize=1)
def evolution_search_space_v1() -> EvolutionSearchSpaceV1:
    return EvolutionSearchSpaceV1(
        axes=(
            MutationAxisPolicyV1(
                axis="min_adv20_usd",
                value_kind="decimal_string",
                allowed_values=("25000000", "50000000", "75000000", "100000000"),
            ),
            MutationAxisPolicyV1(
                axis="min_close",
                value_kind="decimal_string",
                allowed_values=("3", "5", "10"),
            ),
            MutationAxisPolicyV1(
                axis="target_weight",
                value_kind="decimal_string",
                allowed_values=("0.075", "0.10", "0.125"),
            ),
            MutationAxisPolicyV1(
                axis="ranking_feature",
                value_kind="feature_name",
                allowed_values=("momentum_126_skip5", "return_126"),
            ),
            MutationAxisPolicyV1(
                axis="positive_return_filter_enabled",
                value_kind="boolean",
                allowed_values=(False, True),
            ),
        )
    )


@lru_cache(maxsize=1)
def promotion_policy_v1() -> PromotionPolicyV1:
    return PromotionPolicyV1()


def policy_content_hash(policy: BaseModel) -> str:
    payload = policy.model_dump(mode="json")
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _load_exact_policy[PolicyT: BaseModel](
    document: Mapping[str, object], content_hash: str, expected: PolicyT
) -> PolicyT:
    try:
        actual = canonical_json(dict(document))
        released = canonical_json(expected.model_dump(mode="json"))
    except (TypeError, ValueError) as error:
        raise EvolutionPolicyError("POLICY_DOCUMENT_MISMATCH") from error
    if actual != released:
        raise EvolutionPolicyError("POLICY_DOCUMENT_MISMATCH")
    if content_hash != policy_content_hash(expected):
        raise EvolutionPolicyError("POLICY_HASH_MISMATCH")
    return expected


def load_candidate_budget_policy_v1(
    document: Mapping[str, object], content_hash: str
) -> CandidateBudgetPolicyV1:
    return _load_exact_policy(document, content_hash, candidate_budget_policy_v1())


def load_evolution_search_space_v1(
    document: Mapping[str, object], content_hash: str
) -> EvolutionSearchSpaceV1:
    return _load_exact_policy(document, content_hash, evolution_search_space_v1())


def load_promotion_policy_v1(
    document: Mapping[str, object], content_hash: str
) -> PromotionPolicyV1:
    return _load_exact_policy(document, content_hash, promotion_policy_v1())
