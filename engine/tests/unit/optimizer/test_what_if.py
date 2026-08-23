"""Pure, bounded what-if aggregates."""

from decimal import Decimal

from nexus.optimizer.what_if import evaluate_what_if


def test_what_if_evaluator_returns_deterministic_typed_aggregates() -> None:
    result = evaluate_what_if((Decimal("0.1250"), Decimal("0.3750")))
    assert result.model_dump(mode="json") == {
        "diagnostics": {
            "top1": {"status": "known", "value": 3750, "unit": "basis_points", "reason": None},
            "top3": {"status": "known", "value": 5000, "unit": "basis_points", "reason": None},
            "hhi": {
                "status": "known",
                "value": 156250,
                "unit": "parts_per_million",
                "reason": None,
            },
        },
        "constraints": {
            "total_weight": {
                "status": "known",
                "value": 5000,
                "unit": "basis_points",
                "reason": None,
            },
            "unallocated_cash": {
                "status": "known",
                "value": 5000,
                "unit": "basis_points",
                "reason": None,
            },
            "violation_count": {"status": "known", "value": 0, "unit": "count", "reason": None},
        },
    }


def test_what_if_evaluator_reports_overweight_without_identifiers() -> None:
    result = evaluate_what_if((Decimal("0.75"), Decimal("0.50")))
    assert result.constraints.total_weight.value == 12500
    assert result.constraints.unallocated_cash.value == 0
    assert result.constraints.violation_count.value == 1
