from datetime import UTC, datetime

import pytest
from nexus.paper import agent_cycle


def test_general_manual_mode_is_distinct_from_special_recovery():
    from uuid import UUID

    from nexus.ops.agent_paper_once import AgentPaperTriggerRequest

    try:
        value = AgentPaperTriggerRequest(
            action="trigger",
            trigger_mode="manual",
            account_id=UUID(int=1),
            candidate_batch_id=UUID(int=2),
            idempotency_key="manual-new",
            formal_stage_capacity=True,
            formal_model_config_id=UUID(int=3),
            manual_request_id=UUID(int=4),
        )
    except ValueError:
        pytest.fail("formal v8 rejects the approved independent manual mode")
    assert value.trigger_mode == "manual"


def test_manual_slot_is_not_the_scheduled_slot():
    resolve = getattr(agent_cycle, "manual_decision_slot", None)
    assert callable(resolve), "manual cutoff must be server-derived rather than reusing 10:00"
    at = datetime(2026, 10, 12, 15, 17, 31, tzinfo=UTC)
    assert resolve(at) == at


def test_manual_cutoff_rejects_naive_time():
    resolve = getattr(agent_cycle, "manual_decision_slot", None)
    assert callable(resolve)
    with pytest.raises(ValueError, match="AGENT_PAPER_REQUEST_INVALID"):
        resolve(datetime(2026, 10, 12, 15))


def test_manual_mode_requires_persisted_request_identity():
    from uuid import UUID

    from nexus.ops.agent_paper_once import AgentPaperTriggerRequest

    with pytest.raises(ValueError, match="MANUAL_REQUEST_INVALID"):
        AgentPaperTriggerRequest(
            action="trigger",
            trigger_mode="manual",
            account_id=UUID(int=1),
            candidate_batch_id=UUID(int=2),
            idempotency_key="x",
            formal_stage_capacity=True,
            formal_model_config_id=UUID(int=3),
        )
