from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from uuid import UUID

import pytest
from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema import validate as validate_json_schema
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    PortfolioDiagnosticSummaryV1,
    PortfolioInvalidationConditionV1,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
    validate_portfolio_intent,
)
from pydantic import ValidationError

RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("018f0000-0000-7000-8000-000000000002")
SNAPSHOT_ID = UUID("018f0000-0000-7000-8000-000000000003")
REPORT_ID = UUID("018f0000-0000-7000-8000-000000000004")
NOW = datetime(2026, 9, 10, 1, 2, 3, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _known(value: str) -> dict[str, object]:
    return {"status": "known", "value": value, "reason": None}


def _unknown(reason: str) -> dict[str, object]:
    return {"status": "unknown", "value": None, "reason": reason}


def _metrics(*, estimated_cost: dict[str, object]) -> dict[str, object]:
    return {
        "security_top1": _known("0.4"),
        "security_top3": _known("0.8"),
        "security_hhi": _known("0.22"),
        "cash_weight": _known("0.1"),
        "one_way_turnover": _known("0.05"),
        "estimated_cost": estimated_cost,
    }


def _frozen_payload() -> dict[str, object]:
    return {
        "schema_version": "portfolio_decision_input_v1",
        "portfolio_id": str(PORTFOLIO_ID),
        "portfolio_snapshot_id": str(SNAPSHOT_ID),
        "diagnostic_report_id": str(REPORT_ID),
        "decision_time": NOW.isoformat(),
        "objective": "reduce_security_concentration",
        "max_turnover": "0.05",
        "minimum_cash": "0.1",
        "universe_policy": "current_holdings_only",
        "allow_new_positions": False,
        "scenario_request_hash": HASH_A,
        "expected_scenario_result_hash": HASH_B,
        "diagnostic_input_hash": HASH_C,
        "policy_version": "real-advisory-v1",
        "evidence_hash": "d" * 64,
    }


def _projection_payload() -> dict[str, object]:
    return {
        "schema_version": "agent_scenario_projection_v1",
        "objective": "reduce_security_concentration",
        "objective_status": "improved_not_met",
        "analysis_status": "partial",
        "baseline": _metrics(estimated_cost=_unknown("COST_EVIDENCE_INCOMPLETE")),
        "candidate": _metrics(estimated_cost=_unknown("COST_EVIDENCE_INCOMPLETE")),
        "unknown_metrics": ["estimated_cost"],
        "hard_constraint_reasons": ["TURNOVER_BUDGET_EXHAUSTED"],
        "changed_position_count": 2,
        "result_hash": HASH_B,
        "citation_ids": [HASH_A],
        "execution_locked": True,
    }


def _intent_payload() -> dict[str, object]:
    return {
        "schema_version": "portfolio_research_intent_v1",
        "run_id": str(RUN_ID),
        "portfolio_id": str(PORTFOLIO_ID),
        "portfolio_snapshot_id": str(SNAPSHOT_ID),
        "diagnostic_report_id": str(REPORT_ID),
        "decision_time": NOW.isoformat(),
        "assessment": "explore_adjustment",
        "objective": "reduce_security_concentration",
        "horizon": "medium_term",
        "confidence": "medium",
        "rationale": [
            {
                "text": "Concentration is lower in the bounded scenario.",
                "citation_ids": [HASH_A],
            }
        ],
        "invalidation_conditions": [
            {
                "signal": "portfolio.security_top1",
                "operator": "lte",
                "threshold": "0.3",
                "unit": "ratio",
                "citation_ids": [HASH_A],
            }
        ],
        "limitations": [{"code": "estimated_cost", "text": "Estimated cost is unknown."}],
        "scenario_result_hash": HASH_B,
        "citation_ids": [HASH_A],
        "execution_locked": True,
    }


def test_contracts_reject_binary_floats_and_extra_fields() -> None:
    binary_float = _frozen_payload()
    binary_float["max_turnover"] = 0.05
    with pytest.raises((TypeError, ValidationError)):
        PortfolioDecisionInputV1.model_validate(binary_float)

    extra = _frozen_payload()
    extra["target_weight"] = "0.2"
    with pytest.raises(ValidationError):
        PortfolioDecisionInputV1.model_validate(extra)


def test_input_requires_utc_and_hashing_is_canonical() -> None:
    naive = _frozen_payload()
    naive["decision_time"] = "2026-09-10T01:02:03"
    with pytest.raises(ValidationError):
        PortfolioDecisionInputV1.model_validate(naive)

    frozen = PortfolioDecisionInputV1.model_validate(_frozen_payload())
    assert canonical_portfolio_decision_hash(frozen) == canonical_portfolio_decision_hash(
        frozen.model_dump(mode="python")
    )
    with pytest.raises(TypeError):
        canonical_portfolio_decision_hash({"value": 0.1})


@pytest.mark.parametrize(
    ("signal", "operator", "threshold", "unit"),
    [
        ("portfolio.security_top1", "eq", "met", "state"),
        ("portfolio.security_top1", "lte", "1e-1", "ratio"),
        ("portfolio.scenario_objective_status", "gt", "met", "state"),
        ("portfolio.estimated_cost_status", "eq", "0.2", "ratio"),
    ],
)
def test_invalidation_condition_rejects_meaningless_combinations(
    signal: str, operator: str, threshold: str, unit: str
) -> None:
    with pytest.raises(ValidationError):
        PortfolioInvalidationConditionV1.model_validate(
            {
                "signal": signal,
                "operator": operator,
                "threshold": threshold,
                "unit": unit,
                "citation_ids": [HASH_A],
            }
        )


def test_intent_json_schema_encodes_state_signal_operator_matrix() -> None:
    payload = _intent_payload()
    schema = PortfolioResearchIntentV1.model_json_schema(mode="serialization")
    validate_json_schema(instance=payload, schema=schema)
    payload["invalidation_conditions"][0] = {  # type: ignore[index]
        "signal": "portfolio.scenario_objective_status",
        "operator": "gt",
        "threshold": "unchanged",
        "unit": "state",
        "citation_ids": [HASH_A],
    }

    with pytest.raises(JsonSchemaValidationError):
        validate_json_schema(instance=payload, schema=schema)


def test_validate_intent_requires_one_limitation_per_unknown() -> None:
    frozen = PortfolioDecisionInputV1.model_validate(_frozen_payload())
    projection = AgentScenarioProjectionV1.model_validate(_projection_payload())
    payload = _intent_payload()
    payload["limitations"] = []
    intent = PortfolioResearchIntentV1.model_validate(payload)

    with pytest.raises(ValueError, match="limitations must exactly cover unknown metrics"):
        validate_portfolio_intent(
            intent,
            frozen=frozen,
            projection=projection,
            allowed_citation_ids=frozenset({HASH_A}),
        )


def test_projection_rejects_unknown_risk_or_unbounded_reason_text() -> None:
    unknown_risk = _projection_payload()
    unknown_risk["baseline"]["security_top1"] = _unknown("RISK_UNKNOWN")  # type: ignore[index]
    with pytest.raises(ValidationError):
        AgentScenarioProjectionV1.model_validate(unknown_risk)

    unsafe_reason = _projection_payload()
    unsafe_reason["candidate"]["estimated_cost"] = _unknown(  # type: ignore[index]
        "See https://example.test"
    )
    with pytest.raises(ValidationError):
        AgentScenarioProjectionV1.model_validate(unsafe_reason)

    invalid_ratio = _projection_payload()
    invalid_ratio["candidate"]["security_hhi"] = _known("1.1")  # type: ignore[index]
    with pytest.raises(ValidationError):
        AgentScenarioProjectionV1.model_validate(invalid_ratio)

    unknown_cost_gap = _projection_payload()
    unknown_cost_gap["cost_gap_codes"] = ["UNVERIFIED_ACCOUNT_FEE"]
    with pytest.raises(ValidationError):
        AgentScenarioProjectionV1.model_validate(unknown_cost_gap)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("hard_constraint_reasons", ["POSITION_AAPL_BLOCKED"]),
        ("estimated_cost_reason", "AAPL"),
    ],
)
def test_projection_rejects_identity_material_disguised_as_reason_codes(
    field: str, value: object
) -> None:
    payload = _projection_payload()
    if field == "hard_constraint_reasons":
        payload[field] = value
    else:
        payload["candidate"]["estimated_cost"] = _unknown(str(value))  # type: ignore[index]

    with pytest.raises(ValidationError):
        AgentScenarioProjectionV1.model_validate(payload)


def test_model_context_diagnostic_requires_known_ratio() -> None:
    with pytest.raises(ValidationError):
        PortfolioDiagnosticSummaryV1.model_validate(
            {
                "code": "CONCENTRATION_TOP1",
                "severity": "warning",
                "metric": _unknown("UNVERIFIED"),
                "unit": "ratio",
                "citation_ids": [HASH_A],
            }
        )


def test_validate_intent_binds_authority_status_and_citations() -> None:
    frozen = PortfolioDecisionInputV1.model_validate(_frozen_payload())
    projection = AgentScenarioProjectionV1.model_validate(_projection_payload())
    intent = PortfolioResearchIntentV1.model_validate(_intent_payload())

    assert (
        validate_portfolio_intent(
            intent,
            frozen=frozen,
            projection=projection,
            allowed_citation_ids=frozenset({HASH_A}),
        )
        is intent
    )

    stale = deepcopy(_intent_payload())
    stale["scenario_result_hash"] = HASH_C
    with pytest.raises(ValueError, match="scenario result hash does not match"):
        validate_portfolio_intent(
            PortfolioResearchIntentV1.model_validate(stale),
            frozen=frozen,
            projection=projection,
            allowed_citation_ids=frozenset({HASH_A}),
        )

    infeasible = AgentScenarioProjectionV1.model_validate(
        {**_projection_payload(), "objective_status": "infeasible"}
    )
    with pytest.raises(ValueError, match="assessment contradicts scenario status"):
        validate_portfolio_intent(
            intent,
            frozen=frozen,
            projection=infeasible,
            allowed_citation_ids=frozenset({HASH_A}),
        )

    unknown_citation = deepcopy(_intent_payload())
    unknown_citation["citation_ids"] = [HASH_C]
    unknown_citation["rationale"][0]["citation_ids"] = [HASH_C]  # type: ignore[index]
    unknown_citation["invalidation_conditions"][0]["citation_ids"] = [HASH_C]  # type: ignore[index]
    with pytest.raises(ValueError, match="citation is not in frozen authority"):
        validate_portfolio_intent(
            PortfolioResearchIntentV1.model_validate(unknown_citation),
            frozen=frozen,
            projection=projection,
            allowed_citation_ids=frozenset({HASH_A}),
        )


@pytest.mark.parametrize(
    "text",
    [
        "Inspect https://example.test or 018f0000-0000-7000-8000-000000000099",
        "Submit an order after review.",
    ],
)
def test_intent_rejects_executable_or_identity_text(text: str) -> None:
    payload = _intent_payload()
    payload["rationale"] = [
        {
            "text": text,
            "citation_ids": [HASH_A],
        }
    ]
    with pytest.raises(ValidationError):
        PortfolioResearchIntentV1.model_validate(payload)
