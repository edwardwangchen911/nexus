"""Generic deterministic interpretation of verified strategy documents."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.evolution.candidate import apply_candidate_delta, generate_candidate_deltas
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import candidate_budget_policy_v1, evolution_search_space_v1
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    StrategyDataSnapshot,
    evaluate_baseline_v1,
)
from nexus.strategy.dsl import (
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
)
from nexus.strategy.interpreter import (
    CompiledStrategyEvaluationError,
    evaluate_compiled_strategy,
)


def _instrument(
    number: int,
    *,
    raw_close: str = "210",
    latest_close: str = "210",
    skip5_close: str = "180",
    start_close: str = "100",
    dollar_volume: str = "60000000",
) -> BaselineInstrumentSnapshot:
    closes = [Decimal(start_close)] * 200
    closes[-127] = Decimal(start_close)
    closes[-6] = Decimal(skip5_close)
    closes[-1] = Decimal(latest_close)
    return BaselineInstrumentSnapshot(
        instrument_id=UUID(int=number),
        symbol=f"S{number}",
        asset_type="stock",
        sector_exposures=(
            BaselineSectorExposure(sector="information_technology", weight=Decimal(1)),
        ),
        raw_close=Decimal(raw_close),
        closes=tuple(closes),
        dollar_volumes=tuple(Decimal(dollar_volume) for _ in range(20)),
    )


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


def _compiled_challenger(*axes: dict[str, object]):
    parent = baseline_v1_document()
    delta = generate_candidate_deltas(
        _proposal(*axes), parent, candidate_budget_policy_v1()
    )[0]
    document = apply_candidate_delta(parent, delta, evolution_search_space_v1())
    return compile_strategy(document, evolution_feature_registry_v1())


def test_interpreter_is_baseline_v1_equivalent() -> None:
    friday = date(2026, 8, 21)
    snapshot = StrategyDataSnapshot(
        decision_session=friday,
        next_session=friday + timedelta(days=3),
        instruments=(
            _instrument(1, skip5_close="195"),
            _instrument(2, skip5_close="194"),
            _instrument(3, raw_close="4.99", skip5_close="999"),
            _instrument(4, dollar_volume="49999999", skip5_close="999"),
        ),
        current_positions=(UUID(int=2), UUID(int=99)),
    )
    compiled = compile_strategy(baseline_v1_document(), baseline_feature_registry())

    generic = evaluate_compiled_strategy(compiled, snapshot, friday)
    legacy = evaluate_baseline_v1(snapshot, friday)

    assert generic.as_legacy_baseline() == legacy


def test_interpreter_uses_challenger_threshold_ranking_and_target_weight() -> None:
    friday = date(2026, 8, 21)
    strategy = _compiled_challenger(
        {"axis": "min_close", "values": ["10"]},
        {"axis": "target_weight", "values": ["0.10"]},
        {"axis": "ranking_feature", "values": ["return_126"]},
    )
    snapshot = StrategyDataSnapshot(
        decision_session=friday,
        next_session=friday + timedelta(days=3),
        instruments=(
            _instrument(1, raw_close="9", latest_close="210", skip5_close="200"),
            _instrument(2, raw_close="12", latest_close="220", skip5_close="150"),
            _instrument(3, raw_close="12", latest_close="210", skip5_close="200"),
        ),
    )

    decision = evaluate_compiled_strategy(strategy, snapshot, friday)

    assert tuple(target.instrument_id.int for target in decision.targets) == (2, 3)
    assert tuple(target.weight for target in decision.targets) == (Decimal("0.10"),) * 2
    assert decision.cash_weight == Decimal("0.80")
    assert decision.strategy_version.startswith("challenger_")


def test_interpreter_rejects_forged_compiled_identity() -> None:
    valid = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    friday = date(2026, 8, 21)
    snapshot = StrategyDataSnapshot(
        decision_session=friday,
        next_session=friday + timedelta(days=3),
        instruments=(_instrument(1),),
    )
    evaluate_compiled_strategy(valid, snapshot, friday)
    forged = valid.model_copy(update={"expression_hash": "f" * 64})

    with pytest.raises(CompiledStrategyEvaluationError, match="INTEGRITY"):
        evaluate_compiled_strategy(
            forged,
            snapshot,
            friday,
        )


def test_evolution_registry_adds_metadata_without_changing_baseline_identity() -> None:
    baseline = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    evolution = evolution_feature_registry_v1()

    assert baseline.expression_hash == (
        "f643694ad6d31f0dd504061fbd06068a93f9fb679aba9ba5b75337e2e169d13d"
    )
    assert all(
        definition.compatibility_class
        and definition.inputs
        and definition.unit
        and definition.minimum_history_sessions is not None
        and definition.availability_delay
        and definition.missing_policy
        and definition.implementation_hash
        for definition in evolution.definitions()
    )
