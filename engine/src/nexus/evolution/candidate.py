"""Stable server-owned enumeration and application of challenger mutations."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from itertools import product
from typing import cast

from nexus.audit.canonical import canonical_json
from nexus.evolution.models import (
    RegisteredMutationAxisName,
    RegisteredMutationV1,
    StrategyCandidateDeltaV1,
    StrategyExperimentProposalV1,
)
from nexus.evolution.policies import (
    CandidateBudgetPolicyV1,
    EvolutionSearchSpaceV1,
    candidate_budget_policy_v1,
    evolution_search_space_v1,
)
from nexus.strategy.dsl import baseline_feature_registry, compile_strategy


class EvolutionCandidateError(ValueError):
    """A proposal or delta escaped the released bounded search space."""


_POSITIVE_RETURN_FILTER = {"op": "gt", "feature": "return_126", "value": "0"}
_IMMUTABLE_DOCUMENT_FIELDS = (
    "universe",
    "rebalance",
    "holding_period",
    "portfolio",
    "exits",
)


def _candidate_hash(
    parent_expression_hash: str, mutations: tuple[RegisteredMutationV1, ...]
) -> str:
    payload = {
        "schema_version": "strategy_candidate_identity_v1",
        "parent_expression_hash": parent_expression_hash,
        "mutations": [mutation.model_dump(mode="json") for mutation in mutations],
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _strict_dict(value: object, *, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise EvolutionCandidateError(f"{field.upper()}_INVALID")
    return cast(dict[str, object], value)


def _strict_list(value: object, *, field: str) -> list[object]:
    if not isinstance(value, list):
        raise EvolutionCandidateError(f"{field.upper()}_INVALID")
    return value


def _set_parameter(document: dict[str, object], name: str, value: str) -> None:
    parameters = _strict_list(document.get("parameters"), field="parameters")
    matches = [
        _strict_dict(item, field="parameter")
        for item in parameters
        if isinstance(item, dict) and item.get("name") == name
    ]
    if len(matches) != 1:
        raise EvolutionCandidateError("PARAMETER_BINDING_INVALID")
    matches[0]["value"] = value


def _apply_mutation(document: dict[str, object], mutation: RegisteredMutationV1) -> None:
    if mutation.axis in {"min_adv20_usd", "min_close"}:
        if not isinstance(mutation.value, str):
            raise EvolutionCandidateError("MUTATION_VALUE_TYPE_INVALID")
        root = _strict_dict(document.get("root"), field="root")
        args = _strict_list(root.get("args"), field="root_args")
        index = 0 if mutation.axis == "min_adv20_usd" else 1
        if len(args) <= index:
            raise EvolutionCandidateError("REGISTERED_BINDING_MISSING")
        node = _strict_dict(args[index], field="registered_binding")
        expected_feature = "adv20_usd" if index == 0 else "raw_close"
        if node.get("feature") != expected_feature:
            raise EvolutionCandidateError("REGISTERED_BINDING_MISMATCH")
        node["value"] = mutation.value
        _set_parameter(document, mutation.axis, mutation.value)
        return
    if mutation.axis == "target_weight":
        if not isinstance(mutation.value, str):
            raise EvolutionCandidateError("MUTATION_VALUE_TYPE_INVALID")
        target = _strict_dict(document.get("target_weight"), field="target_weight")
        if target.get("method") != "fixed_equal_weight":
            raise EvolutionCandidateError("REGISTERED_BINDING_MISMATCH")
        target["weight"] = mutation.value
        _set_parameter(document, mutation.axis, mutation.value)
        return
    if mutation.axis == "ranking_feature":
        if not isinstance(mutation.value, str):
            raise EvolutionCandidateError("MUTATION_VALUE_TYPE_INVALID")
        ranking = _strict_dict(document.get("ranking"), field="ranking")
        ranking["feature"] = mutation.value
        return
    if mutation.axis == "positive_return_filter_enabled":
        if not isinstance(mutation.value, bool):
            raise EvolutionCandidateError("MUTATION_VALUE_TYPE_INVALID")
        root = _strict_dict(document.get("root"), field="root")
        args = _strict_list(root.get("args"), field="root_args")
        filtered = [
            item
            for item in args
            if not (
                isinstance(item, dict)
                and item.get("op") == "gt"
                and item.get("feature") == "return_126"
                and item.get("value") == "0"
            )
        ]
        if mutation.value:
            filtered.append(deepcopy(_POSITIVE_RETURN_FILTER))
        root["args"] = filtered
        return
    raise EvolutionCandidateError("MUTATION_AXIS_NOT_REGISTERED")


def _validated_mutations(
    mutations: tuple[RegisteredMutationV1, ...], search_space: EvolutionSearchSpaceV1
) -> None:
    for mutation in mutations:
        if not search_space.axis(mutation.axis).allows(mutation.value):
            raise EvolutionCandidateError("MUTATION_VALUE_NOT_REGISTERED")


def apply_candidate_delta(
    parent_document: Mapping[str, object],
    delta: StrategyCandidateDeltaV1,
    search_space: EvolutionSearchSpaceV1,
) -> dict[str, object]:
    """Apply only released bindings, verify identity, and return a compilable copy."""
    if search_space != evolution_search_space_v1():
        raise EvolutionCandidateError("SEARCH_SPACE_AUTHORITY_MISMATCH")
    parent = compile_strategy(parent_document, baseline_feature_registry())
    if delta.parent_expression_hash != parent.expression_hash:
        raise EvolutionCandidateError("PARENT_EXPRESSION_HASH_MISMATCH")
    _validated_mutations(delta.mutations, search_space)
    expected_hash = _candidate_hash(parent.expression_hash, delta.mutations)
    if delta.candidate_hash != expected_hash:
        raise EvolutionCandidateError("CANDIDATE_HASH_MISMATCH")

    document = deepcopy(dict(parent_document))
    immutable = {field: deepcopy(document.get(field)) for field in _IMMUTABLE_DOCUMENT_FIELDS}
    for mutation in delta.mutations:
        _apply_mutation(document, mutation)
    document["version"] = f"challenger_{delta.candidate_hash[:16]}"
    for field, expected in immutable.items():
        if document.get(field) != expected:
            raise EvolutionCandidateError("IMMUTABLE_STRATEGY_FIELD_CHANGED")
    compile_strategy(document, baseline_feature_registry())
    return document


def _semantic_hash(document: Mapping[str, object], parent_version: object) -> str:
    semantic = deepcopy(dict(document))
    semantic["version"] = parent_version
    return compile_strategy(semantic, baseline_feature_registry()).expression_hash


def generate_candidate_deltas(
    proposal: StrategyExperimentProposalV1,
    parent_document: Mapping[str, object],
    policy: CandidateBudgetPolicyV1,
) -> tuple[StrategyCandidateDeltaV1, ...]:
    """Enumerate, hash-sort, semantic-deduplicate, and cap registered candidates."""
    if policy != candidate_budget_policy_v1():
        raise EvolutionCandidateError("CANDIDATE_BUDGET_AUTHORITY_MISMATCH")
    parent = compile_strategy(parent_document, baseline_feature_registry())
    search_space = evolution_search_space_v1()
    axes = tuple(sorted(proposal.mutation_axes, key=lambda item: item.axis))
    if len(axes) > policy.max_axes:
        raise EvolutionCandidateError("CANDIDATE_AXIS_BUDGET_EXCEEDED")

    normalized: list[tuple[RegisteredMutationAxisName, tuple[str | bool, ...]]] = []
    for axis in axes:
        definition = search_space.axis(axis.axis)
        values = tuple(sorted(axis.values, key=canonical_json))
        if any(not definition.allows(value) for value in values):
            raise EvolutionCandidateError("MUTATION_VALUE_NOT_REGISTERED")
        normalized.append((axis.axis, values))

    candidates: list[tuple[str, tuple[RegisteredMutationV1, ...]]] = []
    semantic_hashes: set[str] = {parent.expression_hash}
    parent_version = parent_document.get("version")
    for combination in product(*(values for _, values in normalized)):
        mutations = tuple(
            RegisteredMutationV1(axis=axis_name, value=value)
            for (axis_name, _), value in zip(normalized, combination, strict=True)
        )
        digest = _candidate_hash(parent.expression_hash, mutations)
        provisional = StrategyCandidateDeltaV1(
            parent_expression_hash=parent.expression_hash,
            ordinal=1,
            mutations=mutations,
            candidate_hash=digest,
        )
        document = apply_candidate_delta(parent_document, provisional, search_space)
        semantic_hash = _semantic_hash(document, parent_version)
        if semantic_hash in semantic_hashes:
            continue
        semantic_hashes.add(semantic_hash)
        candidates.append((digest, mutations))

    selected = sorted(candidates, key=lambda item: item[0])[: policy.max_candidates]
    return tuple(
        StrategyCandidateDeltaV1(
            parent_expression_hash=parent.expression_hash,
            ordinal=ordinal,
            mutations=mutations,
            candidate_hash=digest,
        )
        for ordinal, (digest, mutations) in enumerate(selected, start=1)
    )
