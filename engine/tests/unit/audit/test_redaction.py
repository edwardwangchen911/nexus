import json
from datetime import UTC, datetime

import pytest
from nexus.audit.redaction import AuditExportSource, classify_audit_action, redact_audit_event


@pytest.mark.parametrize(
    ("action", "expected"),
    (
        ("quality.freeze.failed", "quality"),
        ("paper.risk.rejected", "risk"),
        ("agent.budget.settled", "agent_cost"),
        ("job.failed", "job"),
        ("strategy.shadow.finalized", "evolution"),
        ("strategy.registry.promoted", "evolution"),
        ("broker.reconciliation.mismatch", "reconciliation"),
        ("shadow.started", "audit"),
    ),
)
def test_audit_action_classification_is_closed(action: str, expected: str) -> None:
    assert classify_audit_action(action) == expected


def test_export_redacts_identifiers_amounts_raw_input_and_actor_identity() -> None:
    source = AuditExportSource(
        event_seq=9,
        occurred_at=datetime(2026, 8, 27, 2, tzinfo=UTC),
        actor_id="user:private-user-123",
        action="job.failed",
        schema_version="job_event_v1",
        payload={
            "status": "failed",
            "reason_code": "QUALITY_STALE",
            "retryable": True,
            "account_id": "secret-account",
            "portfolio_id": "secret-portfolio",
            "order_id": "secret-order",
            "cash": "9123.45",
            "quantity": "27",
            "request_body": {"prompt": "private input"},
            "api_key": "secret-key",
        },
        previous_hash="a" * 64,
        event_hash="b" * 64,
    )

    exported = redact_audit_event(source)
    serialized = json.dumps(exported, sort_keys=True)

    assert exported == {
        "event_seq": 9,
        "occurred_at": "2026-08-27T02:00:00.000000Z",
        "kind": "job",
        "actor_kind": "user",
        "action": "job.failed",
        "schema_version": "job_event_v1",
        "outcome": {"reason_code": "QUALITY_STALE", "retryable": True, "status": "failed"},
        "previous_hash": "a" * 64,
        "event_hash": "b" * 64,
    }
    for secret in (
        "private-user-123",
        "secret-account",
        "secret-portfolio",
        "secret-order",
        "9123.45",
        "private input",
        "secret-key",
    ):
        assert secret not in serialized


@pytest.mark.parametrize(
    ("actor_id", "expected"),
    (
        ("user:one", "user"),
        ("system:task20", "system"),
        ("shadow-worker", "service"),
        ("engine-bff", "service"),
    ),
)
def test_actor_is_reduced_to_a_non_identifying_kind(actor_id: str, expected: str) -> None:
    source = AuditExportSource(
        event_seq=1,
        occurred_at=datetime(2026, 8, 27, tzinfo=UTC),
        actor_id=actor_id,
        action="audit.checked",
        schema_version="1",
        payload={},
        previous_hash="0" * 64,
        event_hash="c" * 64,
    )

    assert redact_audit_event(source)["actor_kind"] == expected


def test_evolution_export_keeps_only_safe_type_hash_gate_and_reason() -> None:
    source = AuditExportSource(
        event_seq=3,
        occurred_at=datetime(2026, 8, 27, tzinfo=UTC),
        actor_id="strategy-rnd-service",
        action="strategy.promotion.gated",
        schema_version="strategy_evolution_event_v1",
        payload={
            "status": "PASS",
            "event_type": "PROMOTION_GATED",
            "policy_version": "promotion_v1",
            "authority_hash": "a" * 64,
            "decision_hash": "b" * 64,
            "gate_code": "HISTORICAL_GATES_PASSED",
            "reason_code": "HISTORICAL_GATES_PASSED",
            "strategy_document": {"rebalance": "WEEKLY_REBALANCE"},
            "hypothesis": "private Agent content",
            "symbol": "PRIVATE",
            "account_id": "private-account",
            "amount": "90000",
        },
        previous_hash="c" * 64,
        event_hash="d" * 64,
    )

    exported = redact_audit_event(source)

    assert exported["kind"] == "evolution"
    assert exported["outcome"] == {
        "authority_hash": "a" * 64,
        "decision_hash": "b" * 64,
        "event_type": "PROMOTION_GATED",
        "gate_code": "HISTORICAL_GATES_PASSED",
        "policy_version": "promotion_v1",
        "reason_code": "HISTORICAL_GATES_PASSED",
        "status": "PASS",
    }
    serialized = json.dumps(exported, sort_keys=True)
    for private in ("rebalance", "private Agent content", "PRIVATE", "private-account", "90000"):
        assert private not in serialized
