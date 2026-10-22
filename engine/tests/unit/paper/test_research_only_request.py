from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.paper.manual_request import _validate_event, manual_request_hash

AT = datetime(2026, 10, 24, 2, tzinfo=UTC)


def test_v2_research_started_does_not_claim_a_paper_cycle():
    row = SimpleNamespace(mode="research_only", contract_version=2, requested_at=AT)
    prior = SimpleNamespace(status="preparing", occurred_at=AT, cycle_id=None)
    event = SimpleNamespace(
        status="active", code="MANUAL_RESEARCH_STARTED", occurred_at=AT, cycle_id=None
    )
    _validate_event(row, event, prior)
    event.cycle_id = UUID(int=1)
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        _validate_event(row, event, prior)


def test_legacy_research_keeps_cycle_requirement():
    row = SimpleNamespace(mode="research_only", contract_version=1, requested_at=AT)
    prior = SimpleNamespace(status="preparing", occurred_at=AT, cycle_id=None)
    event = SimpleNamespace(
        status="active", code="MANUAL_CYCLE_CREATED", occurred_at=AT, cycle_id=None
    )
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        _validate_event(row, event, prior)
    event.cycle_id = UUID(int=1)
    _validate_event(row, event, prior)


def test_contract_versions_cannot_share_request_identity():
    args = dict(account_id=UUID(int=1), batch_id=UUID(int=2), mode="research_only")
    old = manual_request_hash(**args)
    assert manual_request_hash(**args, contract_version=1) == old
    assert manual_request_hash(**args, contract_version=2) != old
    with pytest.raises(ValueError):
        manual_request_hash(**(args | {"mode": "research_and_paper"}), contract_version=2)


def test_pure_queue_bypasses_inapplicable_market_window_only():
    from nexus.ops.agent_paper_loop import (
        AgentPaperLoopAction,
        AgentPaperLoopSnapshot,
        choose_loop_action,
    )
    from nexus.ops.manual_paper_host import bind_manual_queue
    from nexus.paper.manual_request import ManualRequestView

    snapshot = AgentPaperLoopSnapshot(now=AT, enabled=True)
    request = ManualRequestView(
        request_id=UUID(int=3),
        mode="research_only",
        requested_at=AT,
        status="queued",
        code="MANUAL_REQUEST_QUEUED",
        cycle_id=None,
        research_contract="research_only_v2",
    )
    bound = bind_manual_queue(snapshot, request)
    assert choose_loop_action(bound) == AgentPaperLoopAction.TRIGGER_MANUAL
    assert (
        choose_loop_action(
            bind_manual_queue(
                replace(snapshot, enabled=False),
                request,
            )
        )
        == AgentPaperLoopAction.STOP_DISABLED
    )


def test_restarted_pure_active_routes_only_to_close():
    from nexus.ops.agent_paper_loop import (
        AgentPaperLoopAction,
        AgentPaperLoopSnapshot,
        choose_loop_action,
    )
    from nexus.ops.manual_paper_host import bind_manual_queue
    from nexus.paper.manual_request import ManualRequestView

    view = ManualRequestView(
        request_id=UUID(int=3),
        mode="research_only",
        requested_at=AT,
        status="active",
        code="MANUAL_RESEARCH_STARTED",
        cycle_id=None,
        research_contract="research_only_v2",
    )
    assert (
        choose_loop_action(bind_manual_queue(AgentPaperLoopSnapshot(now=AT), view))
        == AgentPaperLoopAction.FINISH_MANUAL
    )


def test_bounded_research_entry_has_no_scope_or_scheduler_switch():
    from nexus.ops.manual_paper import parser

    args = parser().parse_args(["run-research", "--idempotency-key", "independent"])
    assert args.action == "run-research"
    with pytest.raises(SystemExit):
        parser().parse_args(
            ["run-research", "--idempotency-key", "independent", "--account-id", str(UUID(int=1))]
        )


def test_version_one_manual_row_retains_pre_0084_audit_identity():
    from nexus.jobs.models import job_input_hash
    from nexus.paper.history_seal import _row_hash
    from nexus.persistence.models.manual_paper import ManualPaperRequest

    row = ManualPaperRequest(
        id=UUID(int=1),
        account_id=UUID(int=2),
        candidate_batch_id=UUID(int=3),
        model_config_id=UUID(int=4),
        model_hash="a" * 64,
        batch_hash="b" * 64,
        key_hash="c" * 64,
        request_hash="d" * 64,
        mode="research_only",
        requested_at=AT,
        created_at=AT,
        contract_version=1,
        research_profile=None,
    )
    # Literal pre-migration column set: a new mapper field must not rewrite old proofs.
    old = {
        name: getattr(row, name)
        for name in (
            "id",
            "account_id",
            "candidate_batch_id",
            "model_config_id",
            "model_hash",
            "batch_hash",
            "key_hash",
            "request_hash",
            "mode",
            "requested_at",
            "created_at",
        )
    }
    import json

    old = json.loads(json.dumps(old, default=str))
    assert _row_hash(row) == job_input_hash(old)
    row.contract_version = 2
    assert _row_hash(row) != job_input_hash(old)
    prior = _row_hash(row)
    row.research_profile = {"changed": True}
    assert _row_hash(row) != prior
    row.contract_version = 1
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        _row_hash(row)
