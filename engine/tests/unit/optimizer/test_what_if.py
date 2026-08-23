# ruff: noqa: E501
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
                "value": 625000,
                "unit": "parts_per_million",
                "reason": None,
            },
            "sector_exposure": {"status": "unknown", "value": None, "unit": "basis_points", "reason": "SECTOR_EXPOSURE_REQUIRES_IDENTITIES"},
            "currency_exposure": {"status": "unknown", "value": None, "unit": "basis_points", "reason": "CURRENCY_EXPOSURE_REQUIRES_IDENTITIES"},
            "volatility": {"status": "unknown", "value": None, "unit": "basis_points", "reason": "VOLATILITY_REQUIRES_PRICE_HISTORY"},
            "drawdown": {"status": "unknown", "value": None, "unit": "basis_points", "reason": "DRAWDOWN_REQUIRES_PRICE_HISTORY"},
            "correlation": {"status": "unknown", "value": None, "unit": "basis_points", "reason": "CORRELATION_REQUIRES_PRICE_HISTORY"},
            "liquidity": {"status": "unknown", "value": None, "unit": "count", "reason": "LIQUIDITY_REQUIRES_ADV20"},
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
            "single_name_warning": {"status": "known", "value": 0, "unit": "count", "reason": None},
            "single_name_critical": {"status": "known", "value": 1, "unit": "count", "reason": None},
            "top3_warning": {"status": "known", "value": 0, "unit": "count", "reason": None},
            "cash_minimum": {"status": "known", "value": 0, "unit": "count", "reason": None},
            "violation_count": {"status": "known", "value": 1, "unit": "count", "reason": None},
        },
    }


def test_what_if_evaluator_reports_overweight_without_identifiers() -> None:
    result = evaluate_what_if((Decimal("0.75"), Decimal("0.50")))
    assert result.constraints.total_weight.value == 12500
    assert result.constraints.unallocated_cash.value == 0
    assert result.constraints.violation_count.value == 5


def test_ten_percent_security_is_not_normalized_into_critical_concentration() -> None:
    result = evaluate_what_if((Decimal("0.10"),))
    assert result.diagnostics.top1.value == result.diagnostics.top3.value == 1000
    assert result.diagnostics.hhi.value == 1_000_000
    assert result.constraints.unallocated_cash.value == 9000
    assert result.constraints.single_name_warning.value == 0
    assert result.constraints.single_name_critical.value == 0
    assert result.constraints.top3_warning.value == 0
    assert result.constraints.violation_count.value == 0
