"""Property checks for deterministic strategy-evolution closure."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st
from nexus.evolution.candidate import apply_candidate_delta, generate_candidate_deltas
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import candidate_budget_policy_v1, evolution_search_space_v1
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy

AXES = {
    "min_adv20_usd": ("25000000", "50000000", "75000000", "100000000"),
    "min_close": ("3", "5", "10"),
    "target_weight": ("0.075", "0.10", "0.125"),
    "ranking_feature": ("momentum_126_skip5", "return_126"),
    "positive_return_filter_enabled": (False, True),
}


@st.composite
def registered_axes(draw):
    names = draw(
        st.lists(st.sampled_from(tuple(AXES)), min_size=1, max_size=3, unique=True)
    )
    axes: list[dict[str, object]] = []
    for name in names:
        values = draw(
            st.lists(
                st.sampled_from(AXES[name]),
                min_size=1,
                max_size=min(3, len(AXES[name])),
                unique=True,
            )
        )
        axes.append({"axis": name, "values": values})
    return axes


def _proposal(axes: list[dict[str, object]]) -> StrategyExperimentProposalV1:
    return StrategyExperimentProposalV1.model_validate(
        {
            "schema_version": "strategy_experiment_proposal_v1",
            "parent_strategy_definition_id": "019d0000-0000-7000-8000-000000000001",
            "champion_generation": 1,
            "research_lineage_id": "019d0000-0000-7000-8000-000000000002",
            "hypothesis": "Registered parameter variation may improve robustness.",
            "expected_mechanism": "The frozen validation protocol measures the effect.",
            "invalidation_conditions": ["The paired validation comparison is not positive."],
            "evidence_refs": ["019d0000-0000-7000-8000-000000000003"],
            "mutation_axes": axes,
        }
    )


@given(registered_axes())
@settings(max_examples=30, deadline=None)
def test_every_emitted_candidate_stays_in_the_compiler_and_immutable_boundary(axes) -> None:
    parent = baseline_v1_document()
    candidates = generate_candidate_deltas(
        _proposal(axes), parent, candidate_budget_policy_v1()
    )

    assert len(candidates) <= 24
    for candidate in candidates:
        document = apply_candidate_delta(parent, candidate, evolution_search_space_v1())
        compile_strategy(document, baseline_feature_registry())
        assert document["universe"] == parent["universe"]
        assert document["rebalance"] == parent["rebalance"]
        assert document["holding_period"] == parent["holding_period"]
        assert document["portfolio"] == parent["portfolio"]
        assert document["exits"] == parent["exits"]
        assert all("offset" not in node for node in document["root"]["args"])


@given(registered_axes())
@settings(max_examples=30, deadline=None)
def test_axis_order_does_not_change_candidate_identities(axes) -> None:
    reversed_axes = [
        {"axis": item["axis"], "values": list(reversed(item["values"]))}
        for item in reversed(axes)
    ]

    first = generate_candidate_deltas(
        _proposal(axes), baseline_v1_document(), candidate_budget_policy_v1()
    )
    second = generate_candidate_deltas(
        _proposal(reversed_axes), baseline_v1_document(), candidate_budget_policy_v1()
    )

    assert tuple(item.candidate_hash for item in first) == tuple(
        item.candidate_hash for item in second
    )
