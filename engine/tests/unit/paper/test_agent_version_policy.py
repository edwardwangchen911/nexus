"""Frozen policy tests for paired Agent-version evaluation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.agent_version_models import (
    MAX_DRAWDOWN_DEGRADATION,
    MIN_ACTION_DIVERGENCES,
    MIN_CHALLENGER_FILLS,
    MIN_EXCESS_RETURN,
    MIN_POSITIVE_EXCESS_SESSIONS,
    MIN_SAME_SESSION_SLOT_GAP,
    OOS_SLOT_COUNT,
    POLICY_VERSION,
    TRAIN_SLOT_COUNT,
    AgentVersionEvaluationRequest,
    AgentVersionMetricFacts,
    agent_version_market_evidence_hash,
    evaluate_agent_version_metrics,
    evaluation_policy_hash,
    evaluation_policy_payload,
    evaluation_request_hash,
)
from pydantic import ValidationError

INCUMBENT_RUN_ID = UUID("71000000-0000-0000-0000-000000000001")
CHALLENGER_PROMPT_ID = UUID("71000000-0000-0000-0000-000000000002")
MODEL_ID = UUID("71000000-0000-0000-0000-000000000003")
INSTRUMENT_ID = UUID("71000000-0000-0000-0000-000000000004")

PASS_FACTS = AgentVersionMetricFacts(
    oos_pair_count=20,
    action_divergence_count=3,
    challenger_fill_count=1,
    cumulative_excess_return=Decimal("0.005"),
    positive_excess_session_count=7,
    drawdown_degradation=Decimal("0.0025"),
    hard_risk_violation_count=0,
    ledger_conserved=True,
    duplicate_execution_fact_count=0,
    cost_authority_complete=True,
    comparison_authority_complete=True,
    authority_complete=True,
)


def _slot(day: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, 14, minute, tzinfo=UTC)


TRAIN_SLOTS = (_slot(1), _slot(1, 15), _slot(2), _slot(2, 15))
OOS_SLOTS = tuple(
    slot
    for day in (5, 6, 7, 8, 9, 12, 13, 14, 15, 16)
    for slot in (_slot(day), _slot(day, 15))
)


def _request(**changes: object) -> AgentVersionEvaluationRequest:
    values: dict[str, object] = {
        "instrument_id": INSTRUMENT_ID,
        "incumbent_version_source_run_id": INCUMBENT_RUN_ID,
        "challenger_prompt_version_id": CHALLENGER_PROMPT_ID,
        "challenger_model_config_id": MODEL_ID,
        "initial_capital": Decimal("10000.00"),
        "train_slots": TRAIN_SLOTS,
        "oos_slots": OOS_SLOTS,
    }
    values.update(changes)
    return AgentVersionEvaluationRequest.model_validate(values)


def test_v1_policy_is_frozen() -> None:
    assert POLICY_VERSION == "agent_version_evaluation_v1"
    assert TRAIN_SLOT_COUNT == 4
    assert OOS_SLOT_COUNT == 20
    assert MIN_ACTION_DIVERGENCES == 3
    assert MIN_CHALLENGER_FILLS == 1
    assert MIN_EXCESS_RETURN == Decimal("0.005")
    assert MIN_POSITIVE_EXCESS_SESSIONS == 7
    assert MAX_DRAWDOWN_DEGRADATION == Decimal("0.0025")
    assert MIN_SAME_SESSION_SLOT_GAP == timedelta(minutes=15)
    assert evaluation_policy_payload()["min_same_session_slot_gap_seconds"] == 900
    assert len(evaluation_policy_hash()) == 64


def test_v1_policy_rejects_five_minute_same_session_gap() -> None:
    train_slots = (_slot(1), _slot(1, 5), _slot(2), _slot(2, 5))

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(train_slots=train_slots)


def test_v1_policy_accepts_fifteen_minute_same_session_gap() -> None:
    request = _request()

    assert request.train_slots[1] - request.train_slots[0] == timedelta(minutes=15)


def test_agent_version_v1_passes_only_complete_gate() -> None:
    report = evaluate_agent_version_metrics(PASS_FACTS)

    assert (report.status, report.reason_code) == (
        "PASS",
        "OOS_IMPROVEMENT_PROVEN",
    )
    assert tuple(gate.key for gate in report.gates) == (
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
    assert all(gate.status == "PASS" for gate in report.gates)


@pytest.mark.parametrize(
    ("change", "status", "reason"),
    [
        ({"authority_complete": False}, "UNKNOWN", "OOS_AUTHORITY_INCOMPLETE"),
        ({"oos_pair_count": 19}, "UNKNOWN", "OOS_PAIR_INCOMPLETE"),
        (
            {"cost_authority_complete": False},
            "UNKNOWN",
            "OOS_COST_AUTHORITY_INCOMPLETE",
        ),
        (
            {"comparison_authority_complete": False},
            "UNKNOWN",
            "OOS_COMPARISON_AUTHORITY_INCOMPLETE",
        ),
        ({"hard_risk_violation_count": 1}, "FAIL", "HARD_RISK_VIOLATION"),
        ({"ledger_conserved": False}, "FAIL", "LEDGER_CONSERVATION_FAILED"),
        (
            {"duplicate_execution_fact_count": 1},
            "FAIL",
            "DUPLICATE_EXECUTION_FACT",
        ),
        (
            {"action_divergence_count": 2},
            "UNKNOWN",
            "OOS_ACTION_DIVERGENCE_INSUFFICIENT",
        ),
        (
            {"challenger_fill_count": 0},
            "UNKNOWN",
            "OOS_CHALLENGER_FILL_INSUFFICIENT",
        ),
        (
            {"cumulative_excess_return": Decimal("0.0049")},
            "UNKNOWN",
            "OOS_EXCESS_RETURN_INSUFFICIENT",
        ),
        (
            {"positive_excess_session_count": 6},
            "UNKNOWN",
            "OOS_POSITIVE_SESSIONS_INSUFFICIENT",
        ),
        (
            {"drawdown_degradation": Decimal("0.0026")},
            "UNKNOWN",
            "OOS_DRAWDOWN_DEGRADATION_EXCESSIVE",
        ),
    ],
)
def test_agent_version_v1_gate_truth_table(
    change: dict[str, object], status: str, reason: str
) -> None:
    report = evaluate_agent_version_metrics(PASS_FACTS.model_copy(update=change))

    assert (report.status, report.reason_code) == (status, reason)


def test_agent_version_metric_facts_reject_binary_float() -> None:
    with pytest.raises(ValidationError, match="AGENT_VERSION_METRIC_DECIMAL_INVALID"):
        AgentVersionMetricFacts.model_validate(
            {
                **PASS_FACTS.model_dump(),
                "cumulative_excess_return": 0.005,
            }
        )


def test_request_hash_is_stable_for_decimal_and_utc_slots() -> None:
    request = _request()

    assert evaluation_request_hash(request) == evaluation_request_hash(
        request.model_copy(update={"initial_capital": Decimal("10000.000")})
    )
    assert len(evaluation_request_hash(request)) == 64


def test_market_evidence_hash_is_account_neutral_and_domain_separated() -> None:
    assert agent_version_market_evidence_hash("a" * 64) == (
        "297387ed7a079d973bc7e37938f69c3f9136cc38c385b12135d181b665885a88"
    )
    with pytest.raises(ValueError, match="AGENT_VERSION_MARKET_EVIDENCE_INVALID"):
        agent_version_market_evidence_hash("not-a-hash")


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"train_slots": TRAIN_SLOTS[:-1]}, "AGENT_VERSION_WINDOW_INVALID"),
        ({"oos_slots": OOS_SLOTS[:-1]}, "AGENT_VERSION_WINDOW_INVALID"),
        (
            {"oos_slots": TRAIN_SLOTS + OOS_SLOTS[4:]},
            "AGENT_VERSION_WINDOW_INVALID",
        ),
        (
            {"oos_slots": tuple(reversed(OOS_SLOTS))},
            "AGENT_VERSION_WINDOW_INVALID",
        ),
        (
            {"initial_capital": Decimal("0")},
            "AGENT_VERSION_CAPITAL_INVALID",
        ),
    ],
)
def test_v1_policy_rejects_invalid_requests(change: dict[str, object], reason: str) -> None:
    with pytest.raises((ValidationError, ValueError), match=reason):
        _request(**change)


def test_v1_policy_rejects_more_than_two_oos_slots_per_session() -> None:
    invalid = list(OOS_SLOTS)
    invalid[2] = _slot(5, 10)

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(oos_slots=tuple(sorted(invalid)))


def test_v1_policy_rejects_non_utc_slots() -> None:
    invalid = list(TRAIN_SLOTS)
    invalid[0] = invalid[0].replace(tzinfo=None)

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(train_slots=tuple(invalid))


def test_v1_policy_rejects_train_and_oos_on_same_new_york_session() -> None:
    train_slots = (
        datetime(2026, 9, 30, 14, 0, tzinfo=UTC),
        datetime(2026, 9, 30, 14, 15, tzinfo=UTC),
        datetime(2026, 10, 1, 14, 0, tzinfo=UTC),
        datetime(2026, 10, 1, 14, 15, tzinfo=UTC),
    )
    oos_slots = tuple(
        slot
        for day in (1, 2, 5, 6, 7, 8, 9, 12, 13, 14)
        for slot in (_slot(day, 15), _slot(day, 30))
    )

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(train_slots=train_slots, oos_slots=oos_slots)
