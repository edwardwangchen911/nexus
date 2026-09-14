import asyncio
import json
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace
from uuid import UUID

import pytest


def _loop_module():
    try:
        return import_module("nexus.ops.agent_paper_loop")
    except ModuleNotFoundError:
        pytest.fail("agent paper loop policy is not implemented")


def test_terminal_cycle_waits_in_same_slot_and_triggers_in_next_slot() -> None:
    loop = _loop_module()
    cycle_slot = datetime(2026, 9, 14, 14, 0, tzinfo=UTC)

    same_slot = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 3, tzinfo=UTC),
            cycle_status="completed",
            cycle_slot_at=cycle_slot,
        )
    )
    next_slot = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            cycle_status="completed",
            cycle_slot_at=cycle_slot,
        )
    )

    assert same_slot == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
    assert next_slot == loop.AgentPaperLoopAction.TRIGGER


def test_disabled_snapshot_stops_before_any_persisted_action() -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            enabled=False,
            cycle_status="researching",
            research_job_status="queued",
            research_job_id=UUID(int=1),
        )
    )

    assert action == loop.AgentPaperLoopAction.STOP_DISABLED


@pytest.mark.parametrize("cycle_status", ["unavailable", "failed"])
def test_unsuccessful_terminal_cycle_trips_the_session_circuit_breaker(
    cycle_status: str,
) -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 10, tzinfo=UTC),
            cycle_status=cycle_status,
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        )
    )

    assert action == loop.AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE


def test_previous_session_unavailable_cycle_does_not_block_a_new_day() -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 15, 14, 5, tzinfo=UTC),
            cycle_status="unavailable",
            cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
        )
    )

    assert action == loop.AgentPaperLoopAction.TRIGGER


@pytest.mark.parametrize("job_status", ["succeeded", "failed", "dead_letter", "skipped"])
def test_terminal_research_job_is_resumed_without_new_trigger(job_status: str) -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 7, tzinfo=UTC),
            cycle_status="researching",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            research_job_status=job_status,
        )
    )

    assert action == loop.AgentPaperLoopAction.RESUME


def test_only_the_cycle_queued_job_may_run() -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 7, tzinfo=UTC),
            cycle_status="researching",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            research_job_status="queued",
        )
    )

    assert action == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB


@pytest.mark.parametrize("job_status", ["running", None])
def test_unknown_or_running_research_job_stops_without_replay(
    job_status: str | None,
) -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 7, tzinfo=UTC),
            cycle_status="researching",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            research_job_status=job_status,
        )
    )

    assert action == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN


def test_active_order_finishes_after_close_but_terminal_cycle_does_not_retrigger() -> None:
    loop = _loop_module()
    after_close = datetime(2026, 9, 14, 20, 1, tzinfo=UTC)
    slot = datetime(2026, 9, 14, 19, 55, tzinfo=UTC)

    finish_order = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=after_close,
            cycle_status="awaiting_market",
            cycle_slot_at=slot,
            market_window_ready=True,
            has_open_order=True,
        )
    )
    stop_session = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=after_close,
            cycle_status="completed",
            cycle_slot_at=slot,
        )
    )

    assert finish_order == loop.AgentPaperLoopAction.ADVANCE_ORDER
    assert stop_session == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED


def test_awaiting_order_waits_for_its_window_then_collects_once() -> None:
    loop = _loop_module()
    window_end = datetime(2026, 9, 14, 14, 12, tzinfo=UTC)

    early = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 11, 59, tzinfo=UTC),
            cycle_status="awaiting_market",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            has_open_order=True,
            market_window_end_at=window_end,
        )
    )
    due = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=window_end,
            cycle_status="awaiting_market",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            has_open_order=True,
            market_window_end_at=window_end,
        )
    )

    assert early == loop.AgentPaperLoopAction.WAIT_FOR_MARKET
    assert due == loop.AgentPaperLoopAction.COLLECT_WINDOW


@pytest.mark.parametrize("cycle_status", ["decision_recorded", "risk_evaluated"])
def test_persisted_decision_states_resume_without_trigger(cycle_status: str) -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 7, tzinfo=UTC),
            cycle_status=cycle_status,
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        )
    )

    assert action == loop.AgentPaperLoopAction.RESUME


@pytest.mark.parametrize("cycle_status", ["partially_filled", "awaiting_market"])
def test_unresolvable_order_state_stops_without_another_fill(cycle_status: str) -> None:
    loop = _loop_module()

    action = loop.choose_loop_action(
        loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 14, 20, tzinfo=UTC),
            cycle_status=cycle_status,
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            has_open_order=cycle_status == "awaiting_market",
        )
    )

    assert action == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN


def test_foreground_loop_executes_one_persisted_action_then_stops_closed_session() -> None:
    loop = _loop_module()
    snapshots = iter(
        (
            loop.AgentPaperLoopSnapshot(
                now=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                cycle_status="researching",
                cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                research_job_status="queued",
                research_job_id="job-1",
            ),
            loop.AgentPaperLoopSnapshot(
                now=datetime(2026, 9, 14, 20, 1, tzinfo=UTC),
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
            ),
        )
    )
    actions: list[object] = []

    async def inspect():
        return next(snapshots)

    async def execute(action, snapshot):
        actions.append((action, snapshot.research_job_id))

    async def sleep(_seconds: float) -> None:
        raise AssertionError("the loop must not sleep between persisted actions")

    result = asyncio.run(
        loop.run_foreground_loop(inspect=inspect, execute=execute, sleep=sleep)
    )

    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert actions == [(loop.AgentPaperLoopAction.RUN_RESEARCH_JOB, "job-1")]


def test_foreground_loop_waits_until_the_next_five_minute_slot() -> None:
    loop = _loop_module()
    now = datetime(2026, 9, 14, 14, 3, 30, tzinfo=UTC)
    sleeps: list[float] = []
    calls = 0

    async def inspect():
        nonlocal calls
        calls += 1
        if calls == 1:
            return loop.AgentPaperLoopSnapshot(
                now=now,
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 14, 0, tzinfo=UTC),
            )
        return loop.AgentPaperLoopSnapshot(
            now=datetime(2026, 9, 14, 20, 1, tzinfo=UTC),
            cycle_status="completed",
            cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
        )

    async def execute(_action, _snapshot):
        raise AssertionError("waiting must not execute a product action")

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    result = asyncio.run(
        loop.run_foreground_loop(inspect=inspect, execute=execute, sleep=sleep)
    )

    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert sleeps == [(now.replace(minute=5, second=0) - now).total_seconds()]


def test_foreground_loop_waits_for_market_deadline_without_provider_polling() -> None:
    loop = _loop_module()
    now = datetime(2026, 9, 14, 14, 11, 30, tzinfo=UTC)
    deadline = now + timedelta(seconds=30)
    sleeps: list[float] = []
    calls = 0

    async def inspect():
        nonlocal calls
        calls += 1
        if calls == 1:
            return loop.AgentPaperLoopSnapshot(
                now=now,
                cycle_status="awaiting_market",
                cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                has_open_order=True,
                market_window_end_at=deadline,
            )
        return loop.AgentPaperLoopSnapshot(
            now=deadline,
            cycle_status="partially_filled",
            cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        )

    async def execute(_action, _snapshot):
        raise AssertionError("waiting must not call the provider")

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    result = asyncio.run(
        loop.run_foreground_loop(inspect=inspect, execute=execute, sleep=sleep)
    )

    assert result == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    assert sleeps == [30.0]


def test_loop_command_is_default_off_and_emits_a_fixed_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    loop = _loop_module()
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: {})

    exit_code = loop.main(["--account-id", str(UUID(int=1))])

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "AGENT_PAPER_CYCLE_DISABLED"
    }


def test_loop_command_reports_a_safe_terminal_reason(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    loop = _loop_module()
    account_id = UUID(int=1)
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
        },
    )

    async def stop(_settings, *, account_id: UUID, repo_root):
        assert account_id == UUID(int=1)
        assert repo_root == loop.Path.cwd()
        return loop.AgentPaperLoopAction.STOP_SESSION_CLOSED

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", stop)

    exit_code = loop.main(["--account-id", str(account_id)])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "execution_locked": True,
        "status": "stopped",
        "reason": "stop_session_closed",
    }


def test_loop_command_fails_closed_on_unknown_persisted_state(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    loop = _loop_module()
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
        },
    )

    async def stop(_settings, *, account_id: UUID, repo_root):
        return loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", stop)

    exit_code = loop.main(["--account-id", str(UUID(int=1))])

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN"
    }


def test_research_job_resolution_accepts_only_the_cycle_bound_job() -> None:
    loop = _loop_module()
    run_id = UUID(int=10)
    job_id = UUID(int=11)

    resolved = loop.resolve_cycle_research_job(
        events=(SimpleNamespace(fact_ids={"research_run_ids": [str(run_id)]}),),
        jobs=(SimpleNamespace(id=job_id, status="queued", payload={"run_id": str(run_id)}),),
    )

    assert resolved == (job_id, "queued")


@pytest.mark.parametrize(
    ("events", "jobs"),
    [
        ((), ()),
        (
            (SimpleNamespace(fact_ids={"research_run_ids": [str(UUID(int=10))]}),),
            (),
        ),
        (
            (SimpleNamespace(fact_ids={"research_run_ids": [str(UUID(int=10))]}),),
            (
                SimpleNamespace(
                    id=UUID(int=11),
                    status="queued",
                    payload={"run_id": str(UUID(int=12))},
                ),
            ),
        ),
    ],
)
def test_research_job_resolution_rejects_missing_or_mismatched_facts(
    events: tuple[object, ...], jobs: tuple[object, ...]
) -> None:
    loop = _loop_module()

    assert loop.resolve_cycle_research_job(events=events, jobs=jobs) is None


def test_local_loop_uses_persisted_inspection_and_scoped_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _loop_module()
    account_id = UUID(int=1)
    settings = loop.EngineSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
        agent_paper_cycle_enabled=True,
    )
    snapshots = iter(
        (
            loop.AgentPaperLoopSnapshot(
                now=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                cycle_status=None,
            ),
            loop.AgentPaperLoopSnapshot(
                now=datetime(2026, 9, 14, 20, 1, tzinfo=UTC),
                cycle_status="completed",
                cycle_slot_at=datetime(2026, 9, 14, 19, 55, tzinfo=UTC),
            ),
        )
    )
    actions: list[object] = []

    async def inspect(_settings, *, account_id: UUID):
        assert account_id == UUID(int=1)
        return next(snapshots)

    async def execute(_settings, *, account_id: UUID, repo_root, action, snapshot):
        actions.append((account_id, repo_root, action, snapshot.now))

    async def no_sleep(_seconds: float) -> None:
        raise AssertionError("this scenario has no wait action")

    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect, raising=False)
    monkeypatch.setattr(loop, "execute_agent_paper_loop_action", execute, raising=False)
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
        },
    )
    monkeypatch.setattr(loop.asyncio, "sleep", no_sleep)

    result = asyncio.run(
        loop.run_local_agent_paper_loop(
            settings,
            account_id=account_id,
            repo_root=loop.Path("/repo"),
        )
    )

    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert actions == [
        (
            account_id,
            loop.Path("/repo"),
            loop.AgentPaperLoopAction.TRIGGER,
            datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        )
    ]


def test_executor_claims_only_the_persisted_cycle_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _loop_module()
    job_id = UUID(int=11)
    received: list[tuple[UUID, ...] | None] = []

    async def run_worker(_settings, *, job_ids=None):
        received.append(job_ids)
        return True

    monkeypatch.setattr(loop, "local_settings", lambda *_args, **_kwargs: object(), raising=False)
    monkeypatch.setattr(loop, "run_agent_worker_once", run_worker, raising=False)

    asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            snapshot=loop.AgentPaperLoopSnapshot(
                now=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                cycle_id=UUID(int=2),
                cycle_status="researching",
                research_job_id=job_id,
                research_job_status="queued",
            ),
        )
    )

    assert received == [(job_id,)]


def test_executor_uses_a_slot_derived_trigger_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _loop_module()
    requests: list[object] = []
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )

    async def trigger(_settings, request, *, repo_root):
        requests.append(request)

    monkeypatch.setattr(loop, "trigger_once", trigger, raising=False)
    now = datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC)

    asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=loop.AgentPaperLoopSnapshot(now=now),
        )
    )

    request = requests[0]
    assert request.account_id == UUID(int=1)
    assert request.idempotency_key == "agent-paper-loop:2026-09-14T14:05:00+00:00"


def test_cycle_view_maps_only_its_order_and_window_into_the_loop_snapshot() -> None:
    loop = _loop_module()
    now = datetime(2026, 9, 14, 14, 11, tzinfo=UTC)
    cycle_id = UUID(int=2)
    order_id = UUID(int=3)
    window_end = datetime(2026, 9, 14, 14, 12, tzinfo=UTC)
    view = SimpleNamespace(
        cycle_id=cycle_id,
        status="awaiting_market",
        decision_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        order_ids=(order_id,),
        execution_market=SimpleNamespace(status="pending"),
    )

    snapshot = loop.snapshot_from_cycle_view(
        now=now,
        view=view,
        research_job=None,
        market_scope=SimpleNamespace(order_id=order_id, window_end=window_end),
    )

    assert snapshot == loop.AgentPaperLoopSnapshot(
        now=now,
        cycle_id=cycle_id,
        cycle_status="awaiting_market",
        cycle_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        market_window_ready=False,
        market_window_end_at=window_end + timedelta(seconds=60),
        has_open_order=True,
        order_id=order_id,
    )


def test_cycle_view_rejects_a_market_scope_bound_to_another_order() -> None:
    loop = _loop_module()
    view = SimpleNamespace(
        cycle_id=UUID(int=2),
        status="awaiting_market",
        decision_slot_at=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
        order_ids=(UUID(int=3),),
        execution_market=SimpleNamespace(status="pending"),
    )

    with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
        loop.snapshot_from_cycle_view(
            now=datetime(2026, 9, 14, 14, 11, tzinfo=UTC),
            view=view,
            research_job=None,
            market_scope=SimpleNamespace(
                order_id=UUID(int=4),
                window_end=datetime(2026, 9, 14, 14, 12, tzinfo=UTC),
            ),
        )
