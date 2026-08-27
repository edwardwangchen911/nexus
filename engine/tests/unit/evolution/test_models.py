"""Strict Phase 2 strategy-evolution contracts and built-in authorities."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import (
    EvolutionPolicyError,
    candidate_budget_policy_v1,
    evolution_search_space_v1,
    load_candidate_budget_policy_v1,
    load_evolution_search_space_v1,
    load_promotion_policy_v1,
    policy_content_hash,
    promotion_policy_v1,
)
from pydantic import ValidationError

PARENT_ID = UUID("019d0000-0000-7000-8000-000000000001")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000000002")
EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000000003")


def proposal_payload() -> dict[str, object]:
    return {
        "schema_version": "strategy_experiment_proposal_v1",
        "parent_strategy_definition_id": str(PARENT_ID),
        "champion_generation": 1,
        "research_lineage_id": str(LINEAGE_ID),
        "hypothesis": "Stricter liquidity may improve cost-adjusted persistence.",
        "expected_mechanism": "Lower impact and more reliable next-open fills.",
        "invalidation_conditions": ["Validation Sharpe is not positive."],
        "evidence_refs": [str(EVIDENCE_ID)],
        "mutation_axes": [
            {"axis": "min_adv20_usd", "values": ["50000000", "75000000"]}
        ],
    }


def test_proposal_is_frozen_and_normalizes_sequences() -> None:
    proposal = StrategyExperimentProposalV1.model_validate(proposal_payload())

    assert proposal.parent_strategy_definition_id == PARENT_ID
    assert proposal.mutation_axes[0].values == ("50000000", "75000000")
    with pytest.raises(ValidationError):
        proposal.champion_generation = 2


@pytest.mark.parametrize(
    "mutate",
    (
        lambda payload: payload.update({"python": "import os"}),
        lambda payload: payload.update({"hypothesis": "run python"}),
        lambda payload: payload.update({"expected_mechanism": "read https://example.com"}),
        lambda payload: payload.update({"hypothesis": "load ../../private-key"}),
        lambda payload: payload.update({"champion_generation": 0}),
        lambda payload: payload.update({"evidence_refs": []}),
        lambda payload: payload.update({"mutation_axes": []}),
        lambda payload: payload.update(
            {
                "mutation_axes": [
                    {"axis": "min_adv20_usd", "values": ["25000000"]},
                    {"axis": "min_close", "values": ["3"]},
                    {"axis": "target_weight", "values": ["0.10"]},
                    {"axis": "ranking_feature", "values": ["return_126"]},
                ]
            }
        ),
        lambda payload: payload.update(
            {"mutation_axes": [{"axis": "python", "values": ["import os"]}]}
        ),
        lambda payload: payload.update(
            {
                "mutation_axes": [
                    {"axis": "min_close", "values": ["3"]},
                    {"axis": "min_close", "values": ["10"]},
                ]
            }
        ),
        lambda payload: payload.update(
            {
                "mutation_axes": [
                    {"axis": "target_weight", "values": [0.1]},
                ]
            }
        ),
        lambda payload: payload.update(
            {
                "mutation_axes": [
                    {
                        "axis": "target_weight",
                        "values": ["0.10"],
                        "minimum": "0",
                    }
                ]
            }
        ),
    ),
)
def test_proposal_rejects_unsafe_or_unbounded_input(mutate) -> None:
    payload = proposal_payload()
    mutate(payload)

    with pytest.raises((ValidationError, TypeError)):
        StrategyExperimentProposalV1.model_validate(payload)


def test_v1_policies_have_non_overridable_budget_and_thresholds() -> None:
    budget = candidate_budget_policy_v1()
    promotion = promotion_policy_v1()

    assert (budget.max_axes, budget.max_candidates, budget.max_oos_candidates) == (3, 24, 3)
    assert promotion.minimum_oos_windows == 8
    assert str(promotion.minimum_deflated_sharpe_confidence) == "0.95"
    assert str(promotion.minimum_sharpe_improvement) == "0.15"
    assert str(promotion.minimum_annual_return_improvement) == "0.01"
    assert str(promotion.maximum_drawdown_degradation) == "0.02"
    assert str(promotion.cost_stress_multiplier) == "2"


@pytest.mark.parametrize(
    ("factory", "loader"),
    (
        (candidate_budget_policy_v1, load_candidate_budget_policy_v1),
        (evolution_search_space_v1, load_evolution_search_space_v1),
        (promotion_policy_v1, load_promotion_policy_v1),
    ),
)
def test_policy_loader_requires_the_exact_builtin_document_and_hash(factory, loader) -> None:
    policy = factory()
    document = policy.model_dump(mode="json")
    content_hash = policy_content_hash(policy)

    assert loader(document, content_hash) == policy
    with pytest.raises(EvolutionPolicyError, match="POLICY_DOCUMENT_MISMATCH"):
        loader({**document, "version": "forged_v2"}, content_hash)
    with pytest.raises(EvolutionPolicyError, match="POLICY_HASH_MISMATCH"):
        loader(document, "0" * 64)


def test_contract_schema_matches_the_strict_proposal_boundary() -> None:
    root = Path(__file__).resolve().parents[4]
    schema = json.loads(
        (root / "contracts/strategy/strategy-evolution.schema.json").read_text()
    )
    validator = Draft202012Validator(schema)
    valid = proposal_payload()

    assert list(validator.iter_errors(valid)) == []
    assert list(
        validator.iter_errors(
            {**valid, "mutation_axes": [{"axis": "target_weight", "values": [0.1]}]}
        )
    )
    assert list(validator.iter_errors({**valid, "sql": "select 1"}))
