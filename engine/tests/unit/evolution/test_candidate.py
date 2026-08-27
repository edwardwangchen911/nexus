"""Deterministic bounded challenger enumeration and patch closure."""

from __future__ import annotations

from copy import deepcopy

import pytest
from nexus.evolution.candidate import (
    EvolutionCandidateError,
    apply_candidate_delta,
    generate_candidate_deltas,
)
from nexus.evolution.models import StrategyCandidateDeltaV1, StrategyExperimentProposalV1
from nexus.evolution.policies import candidate_budget_policy_v1, evolution_search_space_v1
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy


def _proposal(*axes: dict[str, object]) -> StrategyExperimentProposalV1:
    return StrategyExperimentProposalV1.model_validate(
        {
            "schema_version": "strategy_experiment_proposal_v1",
            "parent_strategy_definition_id": "019d0000-0000-7000-8000-000000000001",
            "champion_generation": 1,
            "research_lineage_id": "019d0000-0000-7000-8000-000000000002",
            "hypothesis": "Registered variation may improve robustness.",
            "expected_mechanism": "The frozen validation protocol measures the effect.",
            "invalidation_conditions": ["The paired validation comparison is not positive."],
            "evidence_refs": ["019d0000-0000-7000-8000-000000000003"],
            "mutation_axes": list(axes),
        }
    )


def test_candidate_generation_is_stable_across_axis_and_value_order() -> None:
    first = _proposal(
        {"axis": "min_close", "values": ["3", "10"]},
        {
            "axis": "ranking_feature",
            "values": ["momentum_126_skip5", "return_126"],
        },
    )
    second = _proposal(
        {
            "axis": "ranking_feature",
            "values": ["return_126", "momentum_126_skip5"],
        },
        {"axis": "min_close", "values": ["10", "3"]},
    )

    first_deltas = generate_candidate_deltas(
        first, baseline_v1_document(), candidate_budget_policy_v1()
    )
    second_deltas = generate_candidate_deltas(
        second, baseline_v1_document(), candidate_budget_policy_v1()
    )

    assert [item.candidate_hash for item in first_deltas] == [
        item.candidate_hash for item in second_deltas
    ]
    assert [item.ordinal for item in first_deltas] == list(
        range(1, len(first_deltas) + 1)
    )
    assert all(
        tuple(mutation.axis for mutation in item.mutations)
        == tuple(sorted(mutation.axis for mutation in item.mutations))
        for item in first_deltas
    )


def test_candidate_generation_caps_the_cartesian_product_at_24() -> None:
    proposal = _proposal(
        {"axis": "min_adv20_usd", "values": ["25000000", "50000000", "75000000"]},
        {"axis": "min_close", "values": ["3", "5", "10"]},
        {"axis": "target_weight", "values": ["0.075", "0.10", "0.125"]},
    )

    candidates = generate_candidate_deltas(
        proposal, baseline_v1_document(), candidate_budget_policy_v1()
    )

    assert len(candidates) == 24
    assert len({item.candidate_hash for item in candidates}) == 24


def test_apply_candidate_delta_changes_only_server_owned_strategy_fields() -> None:
    parent = baseline_v1_document()
    proposal = _proposal(
        {"axis": "min_adv20_usd", "values": ["75000000"]},
        {"axis": "target_weight", "values": ["0.10"]},
        {"axis": "ranking_feature", "values": ["return_126"]},
    )
    delta = generate_candidate_deltas(
        proposal, parent, candidate_budget_policy_v1()
    )[0]

    document = apply_candidate_delta(parent, delta, evolution_search_space_v1())
    compiled = compile_strategy(document, baseline_feature_registry())

    assert document["root"]["args"][0]["value"] == "75000000"
    assert document["parameters"][0]["value"] == "75000000"
    assert document["target_weight"]["weight"] == "0.10"
    assert document["parameters"][2]["value"] == "0.10"
    assert document["ranking"]["feature"] == "return_126"
    assert document["version"] == f"challenger_{delta.candidate_hash[:16]}"
    assert compiled.expression_hash != delta.parent_expression_hash
    for immutable in ("universe", "rebalance", "holding_period", "portfolio", "exits"):
        assert document[immutable] == parent[immutable]


def test_apply_candidate_delta_can_only_toggle_the_registered_filter_template() -> None:
    proposal = _proposal(
        {"axis": "positive_return_filter_enabled", "values": [False]}
    )
    delta = generate_candidate_deltas(
        proposal, baseline_v1_document(), candidate_budget_policy_v1()
    )[0]

    document = apply_candidate_delta(
        baseline_v1_document(), delta, evolution_search_space_v1()
    )

    assert not any(
        node.get("feature") == "return_126" for node in document["root"]["args"]
    )
    assert all("offset" not in node for node in document["root"]["args"])
    compile_strategy(document, baseline_feature_registry())


def test_generation_rejects_values_outside_the_builtin_search_space() -> None:
    proposal = _proposal({"axis": "target_weight", "values": ["0.20"]})

    with pytest.raises(EvolutionCandidateError, match="MUTATION_VALUE_NOT_REGISTERED"):
        generate_candidate_deltas(
            proposal, baseline_v1_document(), candidate_budget_policy_v1()
        )


def test_apply_rejects_forged_hash_and_does_not_mutate_the_parent() -> None:
    parent = baseline_v1_document()
    before = deepcopy(parent)
    proposal = _proposal({"axis": "min_close", "values": ["10"]})
    delta = generate_candidate_deltas(
        proposal, parent, candidate_budget_policy_v1()
    )[0]
    forged = StrategyCandidateDeltaV1.model_validate(
        {**delta.model_dump(mode="json"), "candidate_hash": "0" * 64}
    )

    with pytest.raises(EvolutionCandidateError, match="CANDIDATE_HASH_MISMATCH"):
        apply_candidate_delta(parent, forged, evolution_search_space_v1())
    assert parent == before
