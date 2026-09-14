import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop

NOW = datetime(2026, 9, 14, 14, 5, tzinfo=UTC)
SLOT = datetime(2026, 9, 14, 14, 5, tzinfo=UTC)
PREVIOUS_SLOT = datetime(2026, 9, 14, 14, 0, tzinfo=UTC)
AFTER_CLOSE = datetime(2026, 9, 14, 20, 1, tzinfo=UTC)
LAST_SLOT = datetime(2026, 9, 14, 19, 55, tzinfo=UTC)
A = loop.AgentPaperLoopAction


def snapshot(**changes: object) -> loop.AgentPaperLoopSnapshot:
    return replace(loop.AgentPaperLoopSnapshot(now=NOW), **changes)


@pytest.mark.parametrize(
    ("changes", "expected"),
    # fmt: off -- a compact policy matrix is easier to audit than repeated test bodies.
    [
        ({"enabled": False, "cycle_status": "researching"}, A.STOP_DISABLED),
        (
            {
                "now": NOW - timedelta(minutes=2),
                "cycle_status": "completed",
                "cycle_slot_at": PREVIOUS_SLOT,
            },
            A.WAIT_FOR_SLOT,
        ),
        ({"cycle_status": "completed", "cycle_slot_at": PREVIOUS_SLOT}, A.TRIGGER),
        ({"cycle_status": "unavailable", "cycle_slot_at": SLOT}, A.STOP_CYCLE_UNAVAILABLE),
        ({"cycle_status": "failed", "cycle_slot_at": SLOT}, A.STOP_CYCLE_UNAVAILABLE),
        (
            {
                "now": NOW + timedelta(days=1),
                "cycle_status": "unavailable",
                "cycle_slot_at": LAST_SLOT,
            },
            A.TRIGGER,
        ),
        ({"cycle_status": "researching", "research_job_status": "queued"}, A.RUN_RESEARCH_JOB),
        ({"cycle_status": "researching", "research_job_status": "succeeded"}, A.RESUME),
        ({"cycle_status": "researching", "research_job_status": "failed"}, A.RESUME),
        ({"cycle_status": "researching", "research_job_status": "dead_letter"}, A.RESUME),
        ({"cycle_status": "researching", "research_job_status": "skipped"}, A.RESUME),
        ({"cycle_status": "researching", "research_job_status": "running"}, A.STOP_RESULT_UNKNOWN),
        ({"cycle_status": "researching"}, A.STOP_RESULT_UNKNOWN),
        ({"cycle_status": "decision_recorded"}, A.RESUME),
        ({"cycle_status": "risk_evaluated"}, A.RESUME),
        ({"cycle_status": "partially_filled"}, A.STOP_RESULT_UNKNOWN),
        ({"cycle_status": "awaiting_market", "has_open_order": True}, A.STOP_RESULT_UNKNOWN),
        (
            {
                "cycle_status": "awaiting_market",
                "has_open_order": True,
                "market_window_end_at": NOW + timedelta(seconds=30),
            },
            A.WAIT_FOR_MARKET,
        ),
        (
            {
                "cycle_status": "awaiting_market",
                "has_open_order": True,
                "market_window_end_at": NOW,
            },
            A.COLLECT_WINDOW,
        ),
        (
            {
                "cycle_status": "awaiting_market",
                "has_open_order": True,
                "market_window_ready": True,
            },
            A.ADVANCE_ORDER,
        ),
        (
            {"now": AFTER_CLOSE, "cycle_status": "completed", "cycle_slot_at": LAST_SLOT},
            A.STOP_SESSION_CLOSED,
        ),
        (
            {
                "now": AFTER_CLOSE,
                "cycle_status": "awaiting_market",
                "has_open_order": True,
                "market_window_ready": True,
            },
            A.ADVANCE_ORDER,
        ),
    ],
    # fmt: on
)
def test_policy_is_slot_based_restart_safe_and_fail_closed(
    changes: dict[str, object], expected: loop.AgentPaperLoopAction
) -> None:
    assert loop.choose_loop_action(snapshot(**changes)) == expected


def test_foreground_loop_executes_persisted_action_then_stops() -> None:
    states = iter(
        (
            snapshot(
                cycle_status="researching", research_job_status="queued", research_job_id="job-1"
            ),
            snapshot(
                now=datetime(2026, 9, 14, 20, 1, tzinfo=UTC),
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
            ),
        )
    )
    actions: list[object] = []

    async def inspect():
        return next(states)

    async def execute(action, state):
        actions.append((action, state.research_job_id))

    async def no_sleep(_seconds: float) -> None:
        raise AssertionError("persisted actions do not sleep")

    result = asyncio.run(loop.run_foreground_loop(inspect=inspect, execute=execute, sleep=no_sleep))
    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert actions == [(loop.AgentPaperLoopAction.RUN_RESEARCH_JOB, "job-1")]


@pytest.mark.parametrize(
    ("first", "expected_sleep"),
    [
        (
            snapshot(
                now=datetime(2026, 9, 14, 14, 3, 30, tzinfo=UTC),
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 14, 0, tzinfo=UTC),
            ),
            90.0,
        ),
        (
            snapshot(
                cycle_status="awaiting_market",
                has_open_order=True,
                market_window_end_at=NOW + timedelta(seconds=30),
            ),
            30.0,
        ),
    ],
)
def test_foreground_loop_waits_without_product_side_effects(
    first: loop.AgentPaperLoopSnapshot, expected_sleep: float
) -> None:
    states = iter(
        (
            first,
            snapshot(
                now=datetime(2026, 9, 14, 20, 1, tzinfo=UTC),
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
            ),
        )
    )
    sleeps: list[float] = []

    async def inspect():
        return next(states)

    async def execute(_action, _state):
        raise AssertionError("waiting must not execute a product action")

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    result = asyncio.run(loop.run_foreground_loop(inspect=inspect, execute=execute, sleep=sleep))
    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert sleeps == [expected_sleep]


def enabled_environment() -> dict[str, str]:
    return {
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
    }


def test_loop_command_is_default_off(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: {})
    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "AGENT_PAPER_CYCLE_DISABLED"}


@pytest.mark.parametrize(
    ("reason", "exit_code", "output"),
    [
        (
            loop.AgentPaperLoopAction.STOP_SESSION_CLOSED,
            0,
            {"execution_locked": True, "status": "stopped", "reason": "stop_session_closed"},
        ),
        (
            loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
            1,
            {"error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN"},
        ),
        (
            loop.AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE,
            1,
            {"error_code": "AGENT_PAPER_LOOP_CYCLE_UNAVAILABLE"},
        ),
    ],
)
def test_loop_command_exposes_only_safe_stop_reasons(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reason: loop.AgentPaperLoopAction,
    exit_code: int,
    output: dict[str, object],
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: enabled_environment())

    async def stop(_settings, *, account_id, repo_root):
        return reason

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", stop)
    assert loop.main(["--account-id", str(UUID(int=1))]) == exit_code
    assert json.loads(capsys.readouterr().out) == output


def test_loop_command_reports_interrupt_without_leaking_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: enabled_environment())

    async def interrupt(_settings, *, account_id, repo_root):
        raise KeyboardInterrupt

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", interrupt)
    try:
        exit_code = loop.main(["--account-id", str(UUID(int=1))])
    except KeyboardInterrupt:
        exit_code = None

    assert exit_code == 130
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "AGENT_PAPER_LOOP_INTERRUPTED"
    }


def test_research_job_resolution_accepts_only_cycle_bound_job() -> None:
    run_id, job_id = UUID(int=10), UUID(int=11)
    event = SimpleNamespace(fact_ids={"research_run_ids": [str(run_id)]})
    job = SimpleNamespace(id=job_id, status="queued", payload={"run_id": str(run_id)})
    assert loop.resolve_cycle_research_job(events=(event,), jobs=(job,)) == (job_id, "queued")
    assert loop.resolve_cycle_research_job(events=(event,), jobs=()) is None
    wrong = SimpleNamespace(id=job_id, status="queued", payload={"run_id": str(UUID(int=12))})
    assert loop.resolve_cycle_research_job(events=(event,), jobs=(wrong,)) is None


def test_executor_claims_only_persisted_job(monkeypatch: pytest.MonkeyPatch) -> None:
    job_id = UUID(int=11)
    received: list[tuple[UUID, ...] | None] = []

    async def run_worker(_settings, *, job_ids=None):
        received.append(job_ids)
        return True

    monkeypatch.setattr(loop, "local_settings", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(loop, "run_agent_worker_once", run_worker)
    asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            snapshot=snapshot(
                cycle_id=UUID(int=2),
                cycle_status="researching",
                research_job_id=job_id,
                research_job_status="queued",
            ),
        )
    )
    assert received == [(job_id,)]


def test_executor_uses_slot_derived_trigger_key(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[object] = []
    environment = {
        **enabled_environment(),
        "ALPACA_API_KEY_ID": "key",
        "ALPACA_API_SECRET_KEY": "secret",
    }
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: environment)

    async def trigger(_settings, request, *, repo_root):
        requests.append(request)

    monkeypatch.setattr(loop, "trigger_once", trigger)
    asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(now=datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC)),
        )
    )
    request = requests[0]
    assert request.account_id == UUID(int=1)
    assert request.idempotency_key == "agent-paper-loop:2026-09-14T14:05:00+00:00"


def test_cycle_view_maps_exact_order_and_delayed_collection_time() -> None:
    order_id, window_end = UUID(int=3), datetime(2026, 9, 14, 14, 12, tzinfo=UTC)
    view = SimpleNamespace(
        cycle_id=UUID(int=2),
        status="awaiting_market",
        decision_slot_at=SLOT,
        order_ids=(order_id,),
        execution_market=SimpleNamespace(status="pending"),
    )
    result = loop.snapshot_from_cycle_view(
        now=NOW,
        view=view,
        research_job=None,
        market_scope=SimpleNamespace(order_id=order_id, window_end=window_end),
    )
    assert result.order_id == order_id
    assert result.market_window_end_at == window_end + timedelta(seconds=60)
    assert result.has_open_order is True
    with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
        loop.snapshot_from_cycle_view(
            now=NOW,
            view=view,
            research_job=None,
            market_scope=SimpleNamespace(order_id=UUID(int=4), window_end=window_end),
        )
