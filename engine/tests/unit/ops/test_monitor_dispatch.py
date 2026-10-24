"""Normal host prioritizes daily/manual work without losing incremental identity."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop

AT = datetime(2026, 10, 9, 16, tzinfo=UTC)


def waiting(**changes):
    return loop.AgentPaperLoopSnapshot(
        now=AT,
        calendar_authority_id=UUID(int=1),
        market_open_at=AT - timedelta(hours=2),
        decision_window_start_at=AT - timedelta(hours=1),
        decision_window_end_at=AT + timedelta(hours=4),
        next_market_open_at=AT + timedelta(days=3),
        next_decision_window_start_at=AT + timedelta(days=3),
        **changes,
    )


@pytest.mark.parametrize("status", ["pending", "admitted"])
def test_waiting_host_services_one_original_monitor_identity(status):
    snapshot = waiting(monitor_trigger_id=UUID(int=2), monitor_trigger_status=status)
    assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
    assert (
        loop.choose_loop_action(replace(snapshot, enabled=False))
        == loop.AgentPaperLoopAction.STOP_DISABLED
    )


def test_monitor_does_not_override_daily_manual_or_unknown_boundaries():
    snapshot = waiting(monitor_trigger_id=UUID(int=2), monitor_trigger_status="pending")
    daily = replace(snapshot, cycle_status="researching", research_job_status="queued")
    assert loop.choose_loop_action(daily) == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
    assert (
        loop.choose_loop_action(
            replace(snapshot, manual_pending_request_id=UUID(int=3), manual_pending_status="queued")
        )
        == loop.AgentPaperLoopAction.TRIGGER_MANUAL
    )
    assert (
        loop.choose_loop_action(replace(snapshot, monitor_trigger_status="unknown"))
        == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    )
    assert (
        loop.choose_loop_action(
            replace(snapshot, cycle_status="researching", research_job_status="running")
        )
        == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    )


@pytest.mark.parametrize(
    "minute,expected",
    [
        (40, "run_monitor_increment"),
        (51, "wait_for_session"),
        (59, "wait_for_session"),
        (60, "trigger"),
    ],
)
def test_new_increment_reserves_the_daily_slot_worst_case_time(minute, expected):
    opening = AT.replace(hour=13, minute=30)
    snapshot = replace(
        waiting(monitor_trigger_id=UUID(int=2), monitor_trigger_status="pending"),
        now=opening.replace(minute=0) + timedelta(minutes=minute),
        market_open_at=opening,
        decision_window_start_at=opening,
        host_prepared_market_open_at=opening,
    )
    assert loop.choose_loop_action(snapshot).value == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["allowed", "daily_due", "disabled", "changed_batch", "unknown"])
async def test_dispatch_reloads_scope_and_rechecks_current_priority(monkeypatch, tmp_path, mode):
    from nexus.ops import manual_paper_host, monitor_dispatch, research_monitor
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_cycle_enabled=True,
        agent_runtime_mode="codex_sdk",
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
    )
    current = settings.model_copy(
        update={
            "agent_paper_cycle_enabled": mode != "disabled",
            "agent_paper_candidate_batch_id": UUID(int=99)
            if mode == "changed_batch"
            else UUID(int=2),
        }
    )
    selected = waiting(
        monitor_trigger_id=UUID(int=4),
        monitor_trigger_status="unknown" if mode == "unknown" else "pending",
    )
    if mode == "daily_due":
        selected = replace(selected, cycle_status="researching", research_job_status="queued")
    calls = []

    async def load(*args, **kwargs):
        assert kwargs["account_id"] == UUID(int=1)
        return current

    async def evaluation(*args, **kwargs):
        return None

    async def inspect(*args, **kwargs):
        return selected

    async def unchanged(*args, **kwargs):
        return kwargs["snapshot"]

    async def dispatch(*args, **kwargs):
        calls.append(kwargs["trigger_id"])
        return {"status": "completed"}

    monkeypatch.setattr(research_monitor, "load_host_settings", load)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", evaluation)
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_close", unchanged)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", unchanged)
    monkeypatch.setattr(monitor_dispatch, "inspect_monitor_queue", unchanged)
    monkeypatch.setattr(monitor_dispatch, "dispatch_configured_once", dispatch)
    if mode in {"disabled", "changed_batch"}:
        with pytest.raises(ValueError, match="MONITOR_HOST_"):
            await monitor_dispatch.dispatch_host_once(
                settings,
                account_id=UUID(int=1),
                trigger_id=UUID(int=4),
                calendar_authority_id=UUID(int=1),
                repo_root=tmp_path,
            )
    else:
        result = await monitor_dispatch.dispatch_host_once(
            settings,
            account_id=UUID(int=1),
            trigger_id=UUID(int=4),
            calendar_authority_id=UUID(int=1),
            repo_root=tmp_path,
        )
        assert result["status"] == ("completed" if mode == "allowed" else "deferred")
    assert calls == ([UUID(int=4)] if mode == "allowed" else [])


@pytest.mark.asyncio
async def test_frontend_loop_carries_preparation_fact_and_drains_increment_on_exit(monkeypatch):
    import asyncio

    opening = AT.replace(hour=13, minute=30)
    initial = replace(
        waiting(monitor_trigger_id=UUID(int=2), monitor_trigger_status="pending"),
        now=opening + timedelta(minutes=5),
        market_open_at=opening,
        decision_window_start_at=opening,
    )
    shutdown = asyncio.Event()
    release = asyncio.Event()
    started = asyncio.Event()
    actions = []

    async def inspect():
        return initial

    async def execute(action, snapshot):
        actions.append(action)
        if action == loop.AgentPaperLoopAction.PREPARE_INPUTS:
            return None
        assert action == loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
        assert snapshot.host_prepared_market_open_at == opening
        assert loop.choose_loop_action(snapshot) == action
        started.set()
        shutdown.set()
        await release.wait()
        return None

    async def sleep(_seconds):
        await started.wait()

    async def heartbeat(_at):
        if started.is_set():
            release.set()

    async def confirm(action, snapshot, result):
        assert action == loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
        assert snapshot.monitor_trigger_id == UUID(int=2) and result is None
        return True

    async with asyncio.timeout(10):
        result = await loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=sleep,
            heartbeat=heartbeat,
            shutdown_requested=shutdown.is_set,
            confirm_shutdown=confirm,
            clock=lambda: initial.now,
        )
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    assert actions == [
        loop.AgentPaperLoopAction.PREPARE_INPUTS,
        loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT,
    ]


@pytest.mark.asyncio
async def test_legacy_host_without_formal_monitor_model_keeps_existing_route(monkeypatch):
    from nexus.ops import monitor_dispatch
    from nexus.settings import EngineSettings

    def forbidden(*args, **kwargs):
        raise AssertionError("unconfigured monitor connected to database")

    monkeypatch.setattr(monitor_dispatch, "session_factory", forbidden)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_cycle_enabled=True,
    )
    original = waiting()
    assert (
        await monitor_dispatch.inspect_monitor_queue(
            settings, account_id=UUID(int=1), snapshot=original
        )
        is original
    )
