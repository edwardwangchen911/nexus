import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops.manual_paper_host import bind_manual_close, bind_manual_queue
from nexus.paper.manual_request import ManualRequestView


def test_waiting_host_reinspects_pending_work_after_one_heartbeat():
    at = datetime(2026, 10, 12, 15, tzinfo=UTC)
    state = loop.AgentPaperLoopSnapshot(
        now=at,
        calendar_authority_id=UUID(int=1),
        decision_window_start_at=at.replace(hour=11),
        decision_window_end_at=at.replace(hour=20),
        next_decision_window_start_at=at + timedelta(days=1),
        cycle_status="completed",
        cycle_slot_at=at.replace(hour=14),
    )
    inspected = []
    elapsed = [0.0]

    async def inspect():
        inspected.append(elapsed[0])
        return replace(state, enabled=len(inspected) == 1)

    async def sleep(seconds):
        elapsed[0] += seconds

    async def pending():
        return True

    async def heartbeat(_):
        pass

    async def execute(*_):
        raise AssertionError("waiting itself must not execute a business action")

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=sleep,
            heartbeat=heartbeat,
            wake_for_pending_work=pending,
            clock=lambda: at + timedelta(seconds=elapsed[0]),
            monotonic=lambda: elapsed[0],
        )
    )
    assert result == loop.AgentPaperLoopAction.STOP_DISABLED
    assert inspected == [0.0, 15.0]


def test_pending_inspection_error_stops_without_retry_or_execution():
    at = datetime(2026, 10, 12, 15, tzinfo=UTC)
    calls = []

    async def inspect():
        return loop.AgentPaperLoopSnapshot(
            now=at,
            calendar_authority_id=UUID(int=1),
            decision_window_start_at=at.replace(hour=11),
            decision_window_end_at=at.replace(hour=20),
            next_decision_window_start_at=at + timedelta(days=1),
            cycle_status="completed",
            cycle_slot_at=at.replace(hour=14),
        )

    async def pending():
        calls.append("peek")
        raise ValueError("sensitive-canary")

    async def sleep(_):
        pass

    async def heartbeat(_):
        pass

    async def execute(*_):
        raise AssertionError("no execute after unknown inspection")

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=sleep,
            heartbeat=heartbeat,
            wake_for_pending_work=pending,
            clock=lambda: at,
        )
    )
    assert result == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN and calls == ["peek"]


def test_completed_manual_projection_closes_before_new_slot_or_performance():
    at = datetime(2026, 10, 12, 14, tzinfo=UTC)
    snapshot = loop.AgentPaperLoopSnapshot(
        now=at, cycle_id=UUID(int=2), cycle_status="completed", performance_due_at=at
    )
    request = ManualRequestView(
        request_id=UUID(int=3),
        mode="research_and_paper",
        requested_at=at,
        status="active",
        code="MANUAL_ACTIVE",
        cycle_id=UUID(int=2),
    )
    bound = bind_manual_close(snapshot, request)
    assert loop.choose_loop_action(bound) == loop.AgentPaperLoopAction.FINISH_MANUAL
    assert bound.manual_finish_request_id == request.request_id
    assert snapshot.manual_finish_request_id is None
    assert (
        loop.choose_loop_action(replace(bound, enabled=False))
        == loop.AgentPaperLoopAction.STOP_DISABLED
    )


def test_manual_close_rejects_wrong_cycle_and_never_finishes_running_or_only_research():
    at = datetime(2026, 10, 12, 14, tzinfo=UTC)
    snapshot = loop.AgentPaperLoopSnapshot(now=at, cycle_id=UUID(int=2), cycle_status="researching")
    request = ManualRequestView(
        request_id=UUID(int=3),
        mode="research_and_paper",
        requested_at=at,
        status="active",
        code="MANUAL_ACTIVE",
        cycle_id=UUID(int=2),
    )
    assert bind_manual_close(snapshot, request) == snapshot
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        bind_manual_close(replace(snapshot, cycle_id=UUID(int=4)), request)
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        bind_manual_close(
            replace(snapshot, cycle_status="completed"),
            request.model_copy(update={"mode": "research_only"}),
        )
    assert (
        bind_manual_close(snapshot, request.model_copy(update={"status": "preparing"})) == snapshot
    )


def test_manual_queue_runs_between_slots_but_never_preempts_due_scheduled_trigger():
    at = datetime(2026, 10, 12, 14, tzinfo=UTC)
    state = loop.AgentPaperLoopSnapshot(
        now=at,
        calendar_authority_id=UUID(int=1),
        market_open_at=at.replace(hour=13, minute=30),
        decision_window_start_at=at,
        decision_window_end_at=at.replace(hour=20),
        next_decision_window_start_at=at + timedelta(days=1),
    )
    request = ManualRequestView(
        request_id=UUID(int=3),
        mode="research_only",
        requested_at=at,
        status="queued",
        code="MANUAL_REQUEST_QUEUED",
        cycle_id=None,
    )
    assert (
        loop.choose_loop_action(bind_manual_queue(state, request))
        == loop.AgentPaperLoopAction.TRIGGER
    )
    idle = replace(
        state, now=at + timedelta(minutes=10), cycle_status="completed", cycle_slot_at=at
    )
    assert (
        loop.choose_loop_action(bind_manual_queue(idle, request))
        == loop.AgentPaperLoopAction.TRIGGER_MANUAL
    )
    running = replace(idle, cycle_status="researching", research_job_status="queued")
    assert (
        loop.choose_loop_action(bind_manual_queue(running, request))
        == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
    )
    for status in ("preparing", "unknown"):
        bound = bind_manual_queue(idle, request.model_copy(update={"status": status}))
        assert loop.choose_loop_action(bound) == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN


@pytest.mark.parametrize("status", ["failed", "unavailable"])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
def test_manual_native_failure_schedules_explicit_reconciliation(status, mode):
    at = datetime(2026, 10, 12, 14, tzinfo=UTC)
    snapshot = loop.AgentPaperLoopSnapshot(now=at, cycle_id=UUID(int=2), cycle_status=status)
    request = ManualRequestView(
        request_id=UUID(int=3),
        mode=mode,
        requested_at=at,
        status="active",
        code="MANUAL_CYCLE_CREATED",
        cycle_id=UUID(int=2),
    )
    bound = bind_manual_close(snapshot, request)
    assert loop.choose_loop_action(bound) == loop.AgentPaperLoopAction.FINISH_MANUAL
