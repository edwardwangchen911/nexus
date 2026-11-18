"""An opening-preparation fact reaches queue selection before ordinary-action checks."""

from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import manual_paper_host, monitor_dispatch, research_monitor
from nexus.settings import EngineSettings

OPEN = datetime(2026, 11, 12, 14, 30, tzinfo=UTC)
ACCOUNT, CALENDAR, TRIGGER = UUID(int=1), UUID(int=2), UUID(int=3)


def settings():
    return EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_cycle_enabled=True,
        agent_runtime_mode="codex_sdk",
        agent_paper_account_id=ACCOUNT,
        agent_paper_candidate_batch_id=UUID(int=4),
        agent_paper_formal_model_config_id=UUID(int=5),
    )


def snapshot():
    return loop.AgentPaperLoopSnapshot(
        now=OPEN + timedelta(seconds=1),
        calendar_authority_id=CALENDAR,
        market_open_at=OPEN,
        decision_window_start_at=OPEN,
        decision_window_end_at=OPEN + timedelta(hours=6),
        next_decision_window_start_at=OPEN + timedelta(days=1),
        next_market_open_at=OPEN + timedelta(days=1),
    )


async def unchanged(*_args, **kwargs):
    return kwargs["snapshot"]


async def no_evaluation(*_args, **_kwargs):
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [True, False])
async def test_concrete_host_passes_only_successful_own_preparation_to_queue(
    monkeypatch,
    tmp_path,
    prepared,
):
    configured = settings()
    state = snapshot()
    seen = []

    async def calendar(_settings):
        return CALENDAR

    async def inspect(*_args, **_kwargs):
        return state

    async def queue(*_args, **kwargs):
        seen.append(kwargs["snapshot"].host_prepared_market_open_at)
        return kwargs["snapshot"]

    async def execute(*_args, **kwargs):
        assert kwargs["action"] == loop.AgentPaperLoopAction.PREPARE_INPUTS
        return None if prepared else loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    async def foreground(**kwargs):
        nonlocal state
        first = await kwargs["inspect"]()
        assert seen == [None]
        await kwargs["execute"](loop.AgentPaperLoopAction.PREPARE_INPUTS, first)
        await kwargs["inspect"]()
        assert seen[-1] == (OPEN if prepared else None)
        state = replace(
            state,
            now=state.now + timedelta(days=1),
            market_open_at=OPEN + timedelta(days=1),
            decision_window_start_at=state.decision_window_start_at + timedelta(days=1),
            decision_window_end_at=state.decision_window_end_at + timedelta(days=1),
        )
        later = await kwargs["inspect"]()
        assert loop.choose_loop_action(later) == loop.AgentPaperLoopAction.PREPARE_INPUTS
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(
        loop,
        "load_agent_paper_service_environment",
        lambda *_a, **_k: {
            "DATABASE_URL": configured.database_url,
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(ACCOUNT),
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(configured.agent_paper_candidate_batch_id),
            "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(
                configured.agent_paper_formal_model_config_id
            ),
            "AGENT_RUNTIME_MODE": "codex_sdk",
        },
    )
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", no_evaluation)
    monkeypatch.setattr(loop, "execute_agent_paper_loop_action", execute)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_close", unchanged)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", unchanged)
    monkeypatch.setattr(monitor_dispatch, "inspect_monitor_queue", queue)
    assert (
        await loop.run_local_agent_paper_loop(
            configured,
            account_id=ACCOUNT,
            repo_root=tmp_path,
            clock=lambda: state.now,
        )
        == loop.AgentPaperLoopAction.STOP_REQUESTED
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_open", [None, OPEN, OPEN - timedelta(days=1)])
async def test_dispatch_reinspection_receives_exact_host_opening_before_queue(
    monkeypatch,
    tmp_path,
    prepared_open,
):
    configured = settings()
    state = snapshot()
    dispatched = []

    async def load(*_args, **_kwargs):
        return configured

    async def inspect(*_args, **_kwargs):
        return state

    async def queue(*_args, **kwargs):
        seen = kwargs["snapshot"]
        assert seen.host_prepared_market_open_at == prepared_open
        if loop.choose_loop_action(seen) == loop.AgentPaperLoopAction.WAIT_FOR_SESSION:
            return replace(seen, monitor_trigger_id=TRIGGER, monitor_trigger_status="pending")
        return seen

    async def dispatch(*_args, **_kwargs):
        dispatched.append(TRIGGER)
        return {"status": "completed"}

    monkeypatch.setattr(research_monitor, "load_host_settings", load)
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", no_evaluation)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_close", unchanged)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", unchanged)
    monkeypatch.setattr(monitor_dispatch, "inspect_monitor_queue", queue)
    monkeypatch.setattr(monitor_dispatch, "dispatch_configured_once", dispatch)
    result = await monitor_dispatch.dispatch_host_once(
        configured,
        account_id=ACCOUNT,
        trigger_id=TRIGGER,
        calendar_authority_id=CALENDAR,
        repo_root=tmp_path,
        prepared_market_open_at=prepared_open,
    )
    assert result["status"] == ("completed" if prepared_open == OPEN else "deferred")
    assert dispatched == ([TRIGGER] if prepared_open == OPEN else [])
