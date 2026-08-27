"""Deterministic validation ranking and historical promotion gates."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.audit.canonical import canonical_json
from nexus.evolution.evaluation import (
    ChampionComparisonRunV1,
    EvolutionEvaluationError,
    ValidationCandidateResultV1,
    _comparison_model,
    _promotion_decision_model,
    evaluate_historical_promotion,
    select_oos_candidates,
)
from nexus.evolution.policies import candidate_budget_policy_v1, promotion_policy_v1
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyPromotionDecisionRecord,
)

EXPERIMENT_ID = UUID("019d0000-0000-7000-8000-000000005101")
COMPARISON_ID = UUID("019d0000-0000-7000-8000-000000005102")
EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000005103")


def _validation(
    suffix: str,
    *,
    sharpe: str,
    drawdown: str,
    cost: str,
    complete: bool = True,
) -> ValidationCandidateResultV1:
    return ValidationCandidateResultV1(
        experiment_id=UUID(f"019d0000-0000-7000-8000-00000000520{suffix}"),
        expression_hash=(suffix * 64)[:64],
        validation_sharpe=Decimal(sharpe),
        validation_max_drawdown=Decimal(drawdown),
        validation_cost=Decimal(cost),
        authority_complete=complete,
    )


def _comparison(**updates: object) -> ChampionComparisonRunV1:
    values: dict[str, object] = {
        "schema_version": "champion_comparison_run_v1",
        "comparison_id": COMPARISON_ID,
        "experiment_id": EXPERIMENT_ID,
        "research_lineage_id": UUID("019d0000-0000-7000-8000-000000005104"),
        "champion_definition_id": UUID("019d0000-0000-7000-8000-000000005105"),
        "challenger_definition_id": UUID("019d0000-0000-7000-8000-000000005106"),
        "champion_backtest_run_id": UUID("019d0000-0000-7000-8000-000000005107"),
        "challenger_backtest_run_id": UUID("019d0000-0000-7000-8000-000000005108"),
        "authority_bundle_id": UUID("019d0000-0000-7000-8000-000000005109"),
        "cost_policy_id": UUID("019d0000-0000-7000-8000-000000005110"),
        "champion_generation": 1,
        "cumulative_oos_trial_count": 3,
        "valid_oos_window_count": 8,
        "challenger_deflated_sharpe_confidence": Decimal("0.95"),
        "challenger_sharpe": Decimal("1.15"),
        "champion_sharpe": Decimal("1.00"),
        "challenger_annualized_return": Decimal("0.11"),
        "champion_annualized_return": Decimal("0.10"),
        "challenger_max_drawdown": Decimal("-0.22"),
        "champion_max_drawdown": Decimal("-0.20"),
        "challenger_double_cost_return": Decimal("0.06"),
        "champion_double_cost_return": Decimal("0.05"),
        "strategy_data_complete": True,
        "benchmark_comparison_complete": True,
        "history_requirement_met": True,
        "authority_complete": True,
        "cost_sensitivity_complete": True,
        "parameter_sensitivity_complete": True,
        "evidence_ids": (EVIDENCE_ID,),
        "authority_hash": "a" * 64,
        "protocol_hash": "b" * 64,
        "cost_policy_hash": "c" * 64,
        "comparison_hash": "0" * 64,
    }
    values.update(updates)
    provisional = ChampionComparisonRunV1.model_validate(values)
    comparison_hash = hashlib.sha256(
        canonical_json(provisional.model_dump(mode="json", exclude={"comparison_hash"})).encode()
    ).hexdigest()
    return provisional.model_copy(update={"comparison_hash": comparison_hash})


def _comparison_record(
    report: ChampionComparisonRunV1,
) -> StrategyChampionComparisonRecord:
    return StrategyChampionComparisonRecord(
        id=report.comparison_id,
        experiment_id=report.experiment_id,
        champion_backtest_run_id=report.champion_backtest_run_id,
        challenger_backtest_run_id=report.challenger_backtest_run_id,
        champion_generation=report.champion_generation,
        cumulative_oos_trial_count=report.cumulative_oos_trial_count,
        metric_report=report.model_dump(mode="json"),
        authority_hash=report.authority_hash,
        protocol_hash=report.protocol_hash,
        cost_policy_hash=report.cost_policy_hash,
        comparison_hash=report.comparison_hash,
    )


def test_validation_shortlist_uses_registered_stable_ranking_and_limit() -> None:
    results = (
        _validation("1", sharpe="1.2", drawdown="-0.20", cost="4"),
        _validation("2", sharpe="1.2", drawdown="-0.10", cost="5"),
        _validation("3", sharpe="1.2", drawdown="-0.10", cost="3"),
        _validation("4", sharpe="1.1", drawdown="-0.05", cost="1"),
        _validation("5", sharpe="9", drawdown="0", cost="0", complete=False),
    )

    selected = select_oos_candidates(results, candidate_budget_policy_v1())

    assert [item.expression_hash[0] for item in selected] == ["3", "2", "1"]


@pytest.mark.parametrize(
    ("field", "failing", "passing", "gate"),
    [
        ("valid_oos_window_count", 7, 8, "minimum_oos_windows"),
        (
            "challenger_deflated_sharpe_confidence",
            Decimal("0.949999"),
            Decimal("0.95"),
            "deflated_sharpe_confidence",
        ),
        (
            "challenger_max_drawdown",
            Decimal("-0.220001"),
            Decimal("-0.22"),
            "maximum_drawdown_degradation",
        ),
    ],
)
def test_historical_gate_boundaries(
    field: str, failing: object, passing: object, gate: str
) -> None:
    failed = evaluate_historical_promotion(_comparison(**{field: failing}), promotion_policy_v1())
    passed = evaluate_historical_promotion(_comparison(**{field: passing}), promotion_policy_v1())

    assert failed.gate(gate).status == "FAIL"
    assert passed.gate(gate).status == "PASS"


def test_improvement_gate_accepts_exact_sharpe_or_return_boundary() -> None:
    sharpe = evaluate_historical_promotion(
        _comparison(
            challenger_sharpe=Decimal("1.15"),
            challenger_annualized_return=Decimal("0.109999"),
        ),
        promotion_policy_v1(),
    )
    annual_return = evaluate_historical_promotion(
        _comparison(
            challenger_sharpe=Decimal("1.149999"),
            challenger_annualized_return=Decimal("0.11"),
        ),
        promotion_policy_v1(),
    )
    failed = evaluate_historical_promotion(
        _comparison(
            challenger_sharpe=Decimal("1.149999"),
            challenger_annualized_return=Decimal("0.109999"),
        ),
        promotion_policy_v1(),
    )

    assert sharpe.gate("sharpe_or_return_improvement").status == "PASS"
    assert annual_return.gate("sharpe_or_return_improvement").status == "PASS"
    assert failed.gate("sharpe_or_return_improvement").status == "FAIL"


def test_double_cost_gate_compares_challenger_to_champion() -> None:
    report = evaluate_historical_promotion(
        _comparison(
            challenger_double_cost_return=Decimal("0.04"),
            champion_double_cost_return=Decimal("0.05"),
        ),
        promotion_policy_v1(),
    )

    assert report.gate("double_cost_excess_positive").status == "FAIL"


def test_missing_authority_benchmark_or_trial_count_is_unknown_and_fail_closed() -> None:
    report = evaluate_historical_promotion(
        _comparison(
            cumulative_oos_trial_count=0,
            authority_complete=False,
            benchmark_comparison_complete=None,
        ),
        promotion_policy_v1(),
    )

    assert report.status == "UNKNOWN"
    assert report.gate("evidence_completeness").status == "UNKNOWN"
    assert all(gate.evidence_ids == (EVIDENCE_ID,) for gate in report.gates)
    assert all(
        not isinstance(value, float)
        for gate in report.gates
        for value in (*gate.observed, *gate.thresholds)
    )


def test_comparison_loader_recomputes_hash_before_trusting_metrics() -> None:
    report = _comparison()
    record = _comparison_record(report)
    assert _comparison_model(record) == report
    record.metric_report = {
        **record.metric_report,
        "challenger_sharpe": "9.0000000000",
    }

    with pytest.raises(EvolutionEvaluationError, match="COMPARISON_HASH_MISMATCH"):
        _comparison_model(record)


def test_decision_loader_recomputes_policy_bound_result_before_replay() -> None:
    comparison = _comparison()
    policy = promotion_policy_v1()
    decision = evaluate_historical_promotion(comparison, policy)
    record = StrategyPromotionDecisionRecord(
        id=UUID("019d0000-0000-7000-8000-000000005111"),
        comparison_id=comparison.comparison_id,
        promotion_policy_id=UUID("019d0000-0000-7000-8000-000000005112"),
        stage="historical",
        status=decision.status,
        gate_report=decision.model_dump(mode="json"),
        reason_code=decision.reason_code,
        decision_hash=decision.decision_hash,
    )
    assert _promotion_decision_model(record, comparison, policy) == decision
    record.reason_code = "TAMPERED_DECISION"

    with pytest.raises(EvolutionEvaluationError, match="DECISION_AUTHORITY_INCOMPLETE"):
        _promotion_decision_model(record, comparison, policy)
