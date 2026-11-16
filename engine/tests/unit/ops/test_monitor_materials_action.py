"""Material-only work preserves the daily slot and never admits a child."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop


def pending(seconds=900, state="pending"):
    opening = datetime(2026, 11, 16, 14, 30, tzinfo=UTC)
    return loop.AgentPaperLoopSnapshot(
        now=opening + timedelta(minutes=30) - timedelta(seconds=seconds),
        calendar_authority_id=UUID(int=1),
        market_open_at=opening,
        decision_window_start_at=opening,
        decision_window_end_at=opening + timedelta(hours=5),
        next_market_open_at=opening + timedelta(days=1),
        next_decision_window_start_at=opening + timedelta(days=1),
        host_prepared_market_open_at=opening,
        monitor_trigger_id=UUID(int=2),
        monitor_trigger_status="pending",
        monitor_batch_id=UUID(int=3),
        monitor_materials_status=state,
    )


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (1020, "run_monitor_increment"),
        (1019, "prepare_monitor_materials"),
        (900, "prepare_monitor_materials"),
        (600, "prepare_monitor_materials"),
        (420, "prepare_monitor_materials"),
        (419, "wait_for_session"),
        (300, "wait_for_session"),
    ],
)
def test_material_only_uses_420_seconds_without_reducing_full_increment_margin(seconds, expected):
    assert loop.choose_loop_action(pending(seconds)).value == expected


@pytest.mark.parametrize("state", [None, "prepared", "blocked", "unknown"])
def test_material_only_does_not_loop_prepared_or_unavailable_batches(state):
    assert loop.choose_loop_action(pending(state=state)).value == "wait_for_session"


def test_material_only_preserves_daily_manual_and_unknown_priority():
    snapshot = pending()
    assert (
        loop.choose_loop_action(
            replace(snapshot, cycle_status="researching", research_job_status="queued")
        ).value
        == "run_research_job"
    )
    assert (
        loop.choose_loop_action(
            replace(snapshot, manual_pending_request_id=UUID(int=8), manual_pending_status="queued")
        ).value
        == "trigger_manual"
    )
    assert (
        loop.choose_loop_action(replace(snapshot, monitor_trigger_status="unknown")).value
        == "stop_result_unknown"
    )


@pytest.mark.asyncio
async def test_material_action_has_its_own_shutdown_proof(monkeypatch):
    from nexus.ops import monitor_dispatch
    from nexus.settings import EngineSettings

    proof = AsyncMock(return_value=True)
    forbidden = AsyncMock(side_effect=AssertionError("full increment proof used"))
    monkeypatch.setattr(monitor_dispatch, "confirm_monitor_materials_shutdown", proof)
    monkeypatch.setattr(monitor_dispatch, "confirm_monitor_shutdown", forbidden)
    snapshot = pending()
    result = await loop.confirm_agent_paper_shutdown(
        EngineSettings(),
        account_id=UUID(int=1),
        expected_calendar_authority_id=UUID(int=1),
        action=loop.AgentPaperLoopAction.PREPARE_MONITOR_MATERIALS,
        before_snapshot=snapshot,
        action_result=None,
    )
    assert result and proof.await_count == 1
    forbidden.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["prepared", "preparing", "unknown"])
async def test_material_dispatch_keeps_original_fence_and_returns_before_market_or_child(
    monkeypatch, tmp_path, state
):
    from types import SimpleNamespace

    from nexus import worker
    from nexus.ops import monitor_dispatch, monitor_market_prepare, research_monitor
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
    from nexus.settings import EngineSettings

    account, trigger_id, batch_id = UUID(int=1), UUID(int=2), UUID(int=3)
    trigger = SimpleNamespace(
        id=trigger_id,
        account_id=account,
        batch_id=batch_id,
        kind="disclosure",
        research_run_id=None,
    )

    class Context:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            return False

    session = SimpleNamespace(
        execute=AsyncMock(),
        get=AsyncMock(return_value=trigger),
        scalar=AsyncMock(side_effect=[CURRENT_MIGRATION_HEAD, pending().now, True]),
        begin=Context,
    )
    factory = SimpleNamespace(kw={"bind": SimpleNamespace(dispose=AsyncMock())})

    class Factory:
        kw = factory.kw

        def __call__(self):
            return Context()

    monkeypatch.setattr(monitor_dispatch, "session_factory", lambda _: Factory())
    monkeypatch.setattr(monitor_dispatch, "require_host_admitting", AsyncMock())
    monkeypatch.setattr(monitor_dispatch, "daily_receipt", AsyncMock(return_value=None))
    import nexus.audit.service as audit

    recorded = AsyncMock()
    monkeypatch.setattr(audit, "append_audit_event", recorded)
    runner = AsyncMock(return_value={"status": state, "model_requests": 0, "job_created": False})
    monkeypatch.setattr(research_monitor, "run", runner)
    forbidden = AsyncMock(side_effect=AssertionError("material action spent child resources"))
    monkeypatch.setattr(monitor_market_prepare, "prepare_batch_market_inputs", forbidden)
    monkeypatch.setattr(worker, "run_agent_worker_once", forbidden)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=account,
    )
    result = await monitor_dispatch.dispatch_configured_once(
        settings,
        trigger_id=trigger_id,
        calendar_authority_id=UUID(int=1),
        repo_root=tmp_path,
        prepare_materials_only=True,
    )
    assert result["status"] == state
    assert runner.call_args.args[1].action == "prepare-pending"
    assert recorded.call_args.kwargs["action"] == monitor_dispatch.DISPATCH_ACTION
    assert session.scalar.await_count == 3  # schema, fresh DB time, same five-minute fence
    forbidden.assert_not_awaited()
