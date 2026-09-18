from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.api.schemas.paper import (
    AgentPaperCycleResponse,
    AgentVersionEvaluationResponse,
    PaperCancelRequest,
    PaperCancelResultResponse,
    PaperComparisonRowResponse,
    PaperOrderPageResponse,
    PaperPlanActualResponse,
    PaperPlanResponse,
    PaperPortfolioResponse,
)
from nexus.main import create_engine_app
from nexus.settings import EngineSettings
from pydantic import ValidationError

PORTFOLIO_ID = UUID("67000000-0000-0000-0000-000000000001")


def test_paper_cancel_request_is_strict_and_versioned() -> None:
    request = PaperCancelRequest.model_validate({"expected_event_seq": 4})
    assert request.expected_event_seq == 4

    with pytest.raises(ValidationError):
        PaperCancelRequest.model_validate(
            {"expected_event_seq": 4, "order_id": "broker-order-canary"}
        )
    with pytest.raises(ValidationError):
        PaperCancelRequest.model_validate({"expected_event_seq": 0})


def test_available_paper_comparison_requires_every_metric() -> None:
    with pytest.raises(ValidationError):
        PaperComparisonRowResponse(
            key="agent",
            data_state="READY",
            nav="10000",
            total_return_ratio=None,
            max_drawdown_ratio="0",
            turnover_ratio="0",
            total_cost="0",
            reason_code="PERSISTED_AGENT_PERFORMANCE",
        )


def test_cancel_result_requires_committed_fact_pair() -> None:
    base = {
        "status": "COMMITTED",
        "order": {
            "order_id": UUID(int=91),
            "status": "cancelled",
            "current_event_seq": 3,
            "filled_quantity": "1",
            "remaining_quantity": "2",
            "as_of": datetime(2026, 9, 18, 15, tzinfo=UTC),
        },
        "reservation": None,
        "reservation_released": None,
        "result_event_seq": None,
        "committed_at": None,
    }
    with pytest.raises(ValidationError, match="committed cancellation"):
        PaperCancelResultResponse.model_validate(base)


def test_plan_association_requires_exact_target_object() -> None:
    with pytest.raises(ValidationError, match="target association"):
        PaperPlanResponse.model_validate(
            {
                "batch_id": UUID(int=92),
                "currency": "USD",
                "association_state": "target_associated",
                "target": None,
                "proposal": {
                    "strategy_decision_id": UUID(int=93),
                    "decision_hash": "a" * 64,
                    "cash_weight": "1",
                    "targets": [],
                    "as_of": datetime(2026, 9, 18, 14, tzinfo=UTC),
                },
                "plan": {
                    "plan_created_at": datetime(2026, 9, 18, 14, 1, tzinfo=UTC),
                    "reconciliation_as_of": datetime(2026, 9, 18, 14, 1, tzinfo=UTC),
                    "occupation_state": "released",
                    "risk": [],
                    "items": [],
                },
                "actual": {
                    "data_state": "READY",
                    "as_of": datetime(2026, 9, 18, 15, tzinfo=UTC),
                    "available_cash": "10000",
                    "reserved_cash": "0",
                    "nav": "10000",
                    "positions": [],
                },
            }
        )


def test_plan_actual_unknown_does_not_fabricate_zeroes() -> None:
    actual = PaperPlanActualResponse.model_validate(
        {
            "data_state": "UNKNOWN",
            "as_of": None,
            "available_cash": None,
            "reserved_cash": None,
            "nav": None,
            "positions": None,
        }
    )
    assert actual.available_cash is None
    assert actual.as_of is None
    assert actual.positions is None


def test_persisted_market_benchmark_is_an_available_comparison_row() -> None:
    row = PaperComparisonRowResponse(
        key="market_benchmark",
        data_state="READY",
        nav="9988.182159",
        total_return_ratio="-0.0011817841",
        max_drawdown_ratio="0.0198412698",
        turnover_ratio="0",
        total_cost="0",
        reason_code="PERSISTED_MARKET_BENCHMARK",
    )

    assert row.reason_code == "PERSISTED_MARKET_BENCHMARK"


def test_agent_version_evaluation_rejects_inconsistent_counts_and_pass() -> None:
    base = {
        "evaluation_id": UUID(int=99),
        "status": "observing",
        "incumbent_prompt_hash": "a" * 64,
        "incumbent_model_hash": "b" * 64,
        "challenger_prompt_hash": "c" * 64,
        "challenger_model_hash": "d" * 64,
        "train_completed": 0,
        "oos_completed": 0,
        "oos_missing": 20,
        "oos_incomparable": 0,
        "slots": tuple(
            {
                "phase": "train" if index < 4 else "oos",
                "decision_slot_at": datetime(2026, 9, 18, 14, 5, tzinfo=UTC)
                + timedelta(minutes=15 * index),
                "status": "scheduled",
                "incumbent": {"status": "pending", "result_code": None},
                "challenger": {"status": "pending", "result_code": None},
                "reason_code": "REGISTERED_SLOT_SCHEDULED",
            }
            for index in range(24)
        ),
        "incumbent_metrics": None,
        "challenger_metrics": None,
        "comparisons": (
            {"key": "hold_current", "data_state": "READY", "reason_code": "READY"},
            {"key": "baseline_v1", "data_state": "UNKNOWN", "reason_code": "UNKNOWN"},
            {"key": "market_benchmark", "data_state": "UNKNOWN", "reason_code": "UNKNOWN"},
        ),
        "gates": (),
        "reason_code": "EVALUATION_OBSERVING",
        "execution_locked": True,
        "broker_write": False,
        "live_write": False,
    }
    with pytest.raises(ValidationError, match="lifecycle counts"):
        AgentVersionEvaluationResponse.model_validate({**base, "oos_missing": 19})
    with pytest.raises(ValidationError, match="PASS.*every gate"):
        gate_keys = (
            "authority",
            "oos_pairs",
            "cost_authority",
            "comparison_authority",
            "hard_risk",
            "ledger_conservation",
            "duplicate_execution",
            "action_divergence",
            "challenger_fills",
            "excess_return",
            "positive_excess_sessions",
            "drawdown_degradation",
        )
        AgentVersionEvaluationResponse.model_validate(
            {
                **base,
                "status": "PASS",
                "reason_code": "OOS_IMPROVEMENT_PROVEN",
                "gates": tuple(
                    {
                        "key": key,
                        "status": "UNKNOWN" if key == "authority" else "PASS",
                        "reason_code": (
                            "OOS_AUTHORITY_INCOMPLETE" if key == "authority" else "GATE_PASSED"
                        ),
                    }
                    for key in gate_keys
                ),
            }
        )


def test_paper_read_contracts_are_bounded_and_do_not_expose_account_or_trade_creation() -> None:
    portfolio_schema = PaperPortfolioResponse.model_json_schema()
    page_schema = PaperOrderPageResponse.model_json_schema()

    assert portfolio_schema["properties"]["positions"]["maxItems"] == 500
    assert portfolio_schema["properties"]["attributions"]["maxItems"] == 500
    comparison = portfolio_schema["$defs"]["PaperComparisonResponse"]
    assert comparison["properties"]["rows"]["maxItems"] == 4
    comparison_row = portfolio_schema["$defs"]["PaperComparisonRowResponse"]
    assert {
        "nav",
        "total_return_ratio",
        "max_drawdown_ratio",
        "turnover_ratio",
        "total_cost",
        "data_state",
    } <= set(comparison_row["required"])
    performance = portfolio_schema["$defs"]["PaperPerformanceResponse"]
    assert {"cash", "available_cash", "reserved_cash"} <= set(performance["required"])
    assert page_schema["properties"]["items"]["maxItems"] == 50
    wire = str({"portfolio": portfolio_schema, "orders": page_schema}).lower()
    assert "account_id" not in wire
    assert "broker_order" not in wire
    assert "create_order" not in wire

    cycle_schema = AgentPaperCycleResponse.model_json_schema()
    assert cycle_schema["properties"]["candidates"]["maxItems"] == 5
    assert cycle_schema["properties"]["order_ids"]["maxItems"] == 50
    cycle_wire = str(cycle_schema).lower()
    assert "prompt" not in cycle_wire
    assert "account_id" not in cycle_wire

    evaluation_schema = AgentVersionEvaluationResponse.model_json_schema()
    assert evaluation_schema["properties"]["comparisons"]["maxItems"] == 3
    assert evaluation_schema["properties"]["gates"]["maxItems"] == 12
    assert evaluation_schema["properties"]["slots"]["minItems"] == 24
    assert evaluation_schema["properties"]["slots"]["maxItems"] == 24
    evaluation_wire = str(evaluation_schema).lower()
    assert "account_id" not in evaluation_wire
    assert "prompt_content" not in evaluation_wire
    assert "create" not in evaluation_wire
    assert "promote" not in evaluation_wire
    assert "broker_order" not in evaluation_wire

    cancel_result_schema = PaperCancelResultResponse.model_json_schema()
    assert set(cancel_result_schema["properties"]["status"]["enum"]) == {
        "NOT_RECORDED",
        "COMMITTED",
        "CONFLICT",
        "UNKNOWN",
    }
    assert "idempotency_key" not in str(cancel_result_schema).lower()

    plan_schema = PaperPlanResponse.model_json_schema()
    assert {"target", "proposal", "plan", "actual"} <= set(plan_schema["properties"])
    assert "account_id" not in str(plan_schema).lower()


def test_paper_openapi_exposes_reads_and_cancel_but_no_fill_or_order_creation() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = document["paths"]

    assert set(paths["/internal/v1/paper/portfolio"]) == {"get"}
    assert set(paths["/internal/v1/paper/cycles/latest"]) == {"get"}
    assert set(paths["/internal/v1/paper/orders"]) == {"get"}
    assert set(paths["/internal/v1/paper/orders/{order_id}/cancel"]) == {"post"}
    assert set(paths["/internal/v1/paper/orders/{order_id}/cancel-result"]) == {"get"}
    assert set(paths["/internal/v1/paper/plans/{batch_id}"]) == {"get"}
    assert not any(
        forbidden in path
        for path in paths
        for forbidden in ("/fill", "/advance", "/execute", "/paper/orders/create")
    )
    cancel = paths["/internal/v1/paper/orders/{order_id}/cancel"]["post"]
    headers = {
        parameter["name"]: parameter
        for parameter in cancel["parameters"]
        if parameter["in"] == "header"
    }
    assert headers["Idempotency-Key"]["required"] is True
    cancel_result = paths["/internal/v1/paper/orders/{order_id}/cancel-result"]["get"]
    result_headers = {
        parameter["name"]: parameter
        for parameter in cancel_result["parameters"]
        if parameter["in"] == "header"
    }
    assert result_headers["Idempotency-Key"]["required"] is True
    assert not any(parameter["in"] == "query" for parameter in cancel_result["parameters"])
