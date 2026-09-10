"""Closed contracts for deterministic, read-only portfolio scenarios."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    PortfolioScenarioEvaluationV2,
    PortfolioScenarioRequestV1,
    ScenarioMetricSetV1,
    ScenarioPositionV1,
    canonical_scenario_hash,
)
from pydantic import ValidationError

_SNAPSHOT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701")
_REPORT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702")
_FIRST_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703")
_SECOND_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e704")
_AS_OF = datetime(2026, 9, 10, 8, 0, tzinfo=UTC)
_HASH = "a" * 64


def _constraints(**updates: object) -> AdjustmentScenarioConstraintsV1:
    payload: dict[str, object] = {
        "schema_version": "adjustment_scenario_constraints_v1",
        "objective": "reduce_security_concentration",
        "max_turnover": "0.10",
        "minimum_cash": "0.10",
        "universe_policy": "current_holdings_only",
        "allow_new_positions": False,
    }
    payload.update(updates)
    return AdjustmentScenarioConstraintsV1.model_validate(payload)


@pytest.mark.parametrize(
    ("updates", "error_fragment"),
    [
        ({"max_turnover": 0.1}, "binary floats"),
        ({"minimum_cash": "-0.01"}, "greater than or equal"),
        ({"max_turnover": "1.01"}, "less than or equal"),
        ({"allow_new_positions": True}, "False"),
        ({"positions": []}, "Extra inputs"),
    ],
)
def test_constraints_reject_unsafe_or_out_of_scope_values(
    updates: dict[str, object], error_fragment: str
) -> None:
    """Removing strict validation must make unsafe scenario inputs observable."""
    with pytest.raises((TypeError, ValidationError), match=error_fragment):
        _constraints(**updates)


def test_request_canonicalizes_aware_time_and_rejects_naive_time() -> None:
    shifted = _AS_OF.astimezone(timezone(timedelta(hours=8)))
    request = PortfolioScenarioRequestV1(
        schema_version="portfolio_scenario_request_v1",
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        as_of=shifted,
        constraints=_constraints(),
    )

    assert request.as_of == _AS_OF
    assert request.as_of.tzinfo is UTC
    with pytest.raises(ValidationError, match="timezone-aware"):
        request.model_copy(update={"as_of": _AS_OF.replace(tzinfo=None)}).model_validate(
            {
                **request.model_dump(),
                "as_of": _AS_OF.replace(tzinfo=None),
            }
        )


@pytest.mark.parametrize(
    ("target", "action", "reason"),
    [
        ("0.31", "maintain", "CONCENTRATION_REDUCTION"),
        ("0.29", "maintain", "CONCENTRATION_REDUCTION"),
        ("0.30", "reduce", "BASELINE_WEIGHT"),
    ],
)
def test_position_rejects_weight_action_or_reason_mismatch(
    target: str, action: str, reason: str
) -> None:
    """A caller must not disguise an increase or mislabel an allocation action."""
    with pytest.raises(ValidationError, match="scenario position"):
        ScenarioPositionV1(
            instrument_id=_FIRST_ID,
            current_weight="0.30",
            target_weight=target,
            action=action,
            reason_code=reason,
        )


def _metrics(*, cash: str, turnover: str, cost: TypedMetric) -> ScenarioMetricSetV1:
    return ScenarioMetricSetV1(
        security_top1=TypedMetric.known("0.60"),
        security_top3=TypedMetric.known("1"),
        security_hhi=TypedMetric.known("0.52"),
        cash_weight=TypedMetric.known(cash),
        one_way_turnover=TypedMetric.known(turnover),
        estimated_cost=cost,
    )


def _evaluation_payload() -> dict[str, object]:
    return {
        "schema_version": "portfolio_scenario_evaluation_v2",
        "request_hash": _HASH,
        "result_hash": "b" * 64,
        "portfolio_snapshot_id": _SNAPSHOT_ID,
        "diagnostic_report_id": _REPORT_ID,
        "diagnostic_input_hash": "c" * 64,
        "valuation_time": _AS_OF,
        "valuation_scope": "currency:USD",
        "objective": "reduce_security_concentration",
        "analysis_status": "partial",
        "objective_status": "improved_not_met",
        "baseline": _metrics(cash="0.10", turnover="0", cost=TypedMetric.known("0")),
        "candidate": _metrics(
            cash="0.15",
            turnover="0.05",
            cost=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
        ),
        "positions": (
            ScenarioPositionV1(
                instrument_id=_FIRST_ID,
                current_weight="0.60",
                target_weight="0.55",
                action="reduce",
                reason_code="CONCENTRATION_REDUCTION",
            ),
            ScenarioPositionV1(
                instrument_id=_SECOND_ID,
                current_weight="0.30",
                target_weight="0.30",
                action="maintain",
                reason_code="BASELINE_WEIGHT",
            ),
        ),
        "cash_current_weight": "0.10",
        "cash_target_weight": "0.15",
        "hard_constraint_reasons": ("TURNOVER_BUDGET_EXHAUSTED",),
        "unknown_metrics": ("estimated_cost",),
        "cost_breakdown": {
            "currency": "USD",
            "valuation_time": _AS_OF,
            "basis": "portfolio_value_ratio",
            "evidence_version": None,
            "evidence_hash": None,
            "available_at": None,
            "commission": TypedMetric.unknown("BROKER_COMMISSION_SCHEDULE_UNAVAILABLE"),
            "spread": TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE"),
            "slippage": TypedMetric.unknown("SLIPPAGE_MODEL_UNAVAILABLE"),
            "regulatory_fees": TypedMetric.unknown("REGULATORY_FEE_SCHEDULE_UNAVAILABLE"),
            "tax_impact": TypedMetric.unknown("TAX_DATA_UNAVAILABLE"),
            "total_estimated_cost_ex_tax": TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
        },
        "execution_locked": True,
    }


def test_evaluation_accepts_one_balanced_reduce_only_candidate() -> None:
    evaluation = PortfolioScenarioEvaluationV2.model_validate(_evaluation_payload())

    assert evaluation.cash_target_weight == Decimal("0.15")
    assert evaluation.positions[0].action == "reduce"
    assert evaluation.analysis_status == "partial"
    assert evaluation.execution_locked is True


@pytest.mark.parametrize(
    ("update", "error_fragment"),
    [
        ({"cash_target_weight": "0.14"}, "sum to one"),
        ({"execution_locked": False}, "True"),
        ({"positions": ()}, "at least 1 item"),
        ({"request_hash": "not-a-hash"}, "String should match pattern"),
    ],
)
def test_evaluation_rejects_unbalanced_unlocked_or_unbound_results(
    update: dict[str, object], error_fragment: str
) -> None:
    payload = _evaluation_payload()
    payload.update(update)

    with pytest.raises(ValidationError, match=error_fragment):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_evaluation_rejects_duplicate_instruments() -> None:
    payload = _evaluation_payload()
    first = payload["positions"][0]  # type: ignore[index]
    payload["positions"] = (first, first)

    with pytest.raises(ValidationError, match="unique instruments"):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_cost_breakdown_rejects_known_component_without_evidence_authority() -> None:
    payload = _evaluation_payload()
    breakdown = payload["cost_breakdown"]  # type: ignore[assignment]
    breakdown["commission"] = TypedMetric.known("0.001")  # type: ignore[index]

    with pytest.raises(ValidationError, match="known scenario costs require evidence authority"):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_cost_breakdown_rejects_known_tax_without_evidence_authority() -> None:
    payload = _evaluation_payload()
    breakdown = payload["cost_breakdown"]  # type: ignore[assignment]
    breakdown["tax_impact"] = TypedMetric.known("-0.001")  # type: ignore[index]

    with pytest.raises(ValidationError, match="known scenario costs require evidence authority"):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_cost_breakdown_rejects_total_that_does_not_sum_known_components() -> None:
    payload = _evaluation_payload()
    breakdown = payload["cost_breakdown"]  # type: ignore[assignment]
    breakdown.update(  # type: ignore[union-attr]
        {
            "evidence_version": "broker_cost_evidence_v1",
            "evidence_hash": "e" * 64,
            "available_at": _AS_OF,
            "commission": TypedMetric.known("0.001"),
            "spread": TypedMetric.known("0.002"),
            "slippage": TypedMetric.known("0.003"),
            "regulatory_fees": TypedMetric.known("0.004"),
            "total_estimated_cost_ex_tax": TypedMetric.known("0.009"),
        }
    )
    payload["candidate"] = _metrics(cash="0.15", turnover="0.05", cost=TypedMetric.known("0.009"))

    with pytest.raises(ValidationError, match="total must equal known components"):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_cost_breakdown_rejects_negative_known_cost() -> None:
    payload = _evaluation_payload()
    breakdown = payload["cost_breakdown"]  # type: ignore[assignment]
    breakdown.update(  # type: ignore[union-attr]
        {
            "evidence_version": "broker_cost_evidence_v1",
            "evidence_hash": "e" * 64,
            "available_at": _AS_OF,
            "commission": TypedMetric.known("-0.001"),
            "spread": TypedMetric.known("0.002"),
            "slippage": TypedMetric.known("0.003"),
            "regulatory_fees": TypedMetric.known("0.004"),
            "total_estimated_cost_ex_tax": TypedMetric.known("0.008"),
        }
    )
    payload["candidate"] = _metrics(cash="0.15", turnover="0.05", cost=TypedMetric.known("0.008"))

    with pytest.raises(ValidationError, match="scenario costs cannot be negative"):
        PortfolioScenarioEvaluationV2.model_validate(payload)


def test_canonical_hash_normalizes_equivalent_financial_values() -> None:
    left = {
        "schema_version": "portfolio_scenario_request_v1",
        "as_of": _AS_OF,
        "portfolio_snapshot_id": _SNAPSHOT_ID,
        "constraints": {"minimum_cash": Decimal("0.1000"), "enabled": True},
    }
    right = {
        "constraints": {"enabled": True, "minimum_cash": Decimal("0.1")},
        "portfolio_snapshot_id": _SNAPSHOT_ID,
        "as_of": "2026-09-10T08:00:00.000000Z",
        "schema_version": "portfolio_scenario_request_v1",
    }

    assert canonical_scenario_hash(left) == canonical_scenario_hash(right)
    assert canonical_scenario_hash(left) == (
        "55b38ac49c058c89e9138d6aca7f615b9479ecc82183880784044650bcc970fd"
    )


def test_canonical_hash_excludes_only_the_root_result_hash() -> None:
    payload = _evaluation_payload()
    changed = {**payload, "result_hash": "d" * 64}

    assert canonical_scenario_hash(payload) == canonical_scenario_hash(changed)


def test_canonical_hash_binds_each_cost_evidence_authority_field() -> None:
    payload = _evaluation_payload()
    breakdown = payload["cost_breakdown"]  # type: ignore[assignment]
    breakdown.update(  # type: ignore[union-attr]
        {
            "evidence_version": "broker_cost_evidence_v1",
            "evidence_hash": "e" * 64,
            "available_at": _AS_OF,
        }
    )
    evaluation = PortfolioScenarioEvaluationV2.model_validate(payload)
    original = canonical_scenario_hash(evaluation)
    changed_hashes = {
        canonical_scenario_hash(
            evaluation.model_copy(
                update={
                    "cost_breakdown": evaluation.cost_breakdown.model_copy(
                        update={field: value}
                    )
                }
            )
        )
        for field, value in (
            ("evidence_version", "broker_cost_evidence_v2"),
            ("evidence_hash", "f" * 64),
            ("available_at", _AS_OF - timedelta(seconds=1)),
        )
    }

    assert original not in changed_hashes
    assert len(changed_hashes) == 3


@pytest.mark.parametrize("unsafe", [0.1, Decimal("NaN"), {_FIRST_ID: "bad-key"}])
def test_canonical_hash_rejects_noncanonical_values(unsafe: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        canonical_scenario_hash({"unsafe": unsafe})
