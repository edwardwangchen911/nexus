"""The sole normal host scans independently without creating a second host/model."""

import asyncio
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import research_monitor
from nexus.settings import EngineSettings

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
ACCOUNT = UUID(int=1)
CALENDAR = UUID(int=2)


@pytest.mark.asyncio
async def test_host_heartbeat_does_not_wait_for_scan_and_shutdown_collects_it(
    monkeypatch, tmp_path
):
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []
    heartbeats = []

    async def scan(settings, **kwargs):
        assert kwargs["account_id"] == ACCOUNT
        assert kwargs["calendar_authority_id"] == CALENDAR
        calls.append("scan")
        started.set()
        await release.wait()
        finished.set()

    async def calendar(_settings):
        return CALENDAR

    async def heartbeat(_settings, **kwargs):
        heartbeats.append(kwargs["seen_at"])

    async def foreground(**kwargs):
        await kwargs["heartbeat"](AT)
        await started.wait()
        await kwargs["heartbeat"](AT + timedelta(seconds=15))
        assert not finished.is_set() and len(heartbeats) == 2
        # A new bucket while busy is consumed; it must not queue a catch-up scan.
        await kwargs["heartbeat"](AT + timedelta(minutes=5))
        release.set()
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    monkeypatch.setattr(research_monitor, "scan_host_once", scan, raising=False)
    result = await loop.run_local_agent_paper_loop(
        EngineSettings(
            database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
            agent_paper_account_id=ACCOUNT,
            agent_paper_cycle_enabled=True,
        ),
        account_id=ACCOUNT,
        repo_root=tmp_path,
        clock=lambda: AT,
    )
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    assert calls == ["scan"] and finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["disabled", "account_changed", "calendar_changed", "database_changed", "enabled"]
)
async def test_host_scan_reloads_protected_scope_without_external_preparation(
    monkeypatch, tmp_path, mode
):
    calls = []
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_account_id=ACCOUNT,
        agent_paper_cycle_enabled=True,
    )

    async def bound(*args, **kwargs):
        return {
            "DATABASE_URL": settings.database_url + "_other"
            if mode == "database_changed"
            else settings.database_url,
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "false" if mode == "disabled" else "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(
                UUID(int=99) if mode == "account_changed" else ACCOUNT
            ),
        }, str(UUID(int=99) if mode == "calendar_changed" else CALENDAR)

    async def scan(current, **kwargs):
        assert current.agent_paper_account_id == ACCOUNT
        assert kwargs["calendar_authority_id"] == CALENDAR
        calls.append("scan")
        return {"status": "scanned"}

    monkeypatch.setattr(loop, "_load_bound_loop_environment", bound)
    monkeypatch.setattr(research_monitor, "scan_configured_once", scan)
    action = research_monitor.scan_host_once(
        settings,
        account_id=ACCOUNT,
        calendar_authority_id=CALENDAR,
        repo_root=tmp_path,
        env_file=tmp_path / "private.env",
    )
    if mode in {"account_changed", "calendar_changed", "database_changed"}:
        with pytest.raises(ValueError, match="MONITOR_HOST_SCOPE_CHANGED"):
            await action
    else:
        result = await action
        assert result["status"] == ("disabled" if mode == "disabled" else "scanned")
    assert calls == (["scan"] if mode == "enabled" else [])


@pytest.mark.asyncio
async def test_host_keeps_one_safe_diagnostic_per_failed_bucket_including_exit(
    monkeypatch, tmp_path, capsys
):
    import json

    first, second = asyncio.Event(), asyncio.Event()
    scans = []

    async def scan(*_args, **_kwargs):
        scans.append(True)
        (first if len(scans) == 1 else second).set()
        raise RuntimeError("private-provider-canary")

    async def calendar(_settings):
        return CALENDAR

    async def heartbeat(*_args, **_kwargs):
        pass

    async def foreground(**kwargs):
        await kwargs["heartbeat"](AT)
        await first.wait()
        await kwargs["heartbeat"](AT + timedelta(seconds=15))
        await kwargs["heartbeat"](AT + timedelta(seconds=30))
        await kwargs["heartbeat"](AT + timedelta(minutes=5))
        await second.wait()
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    monkeypatch.setattr(research_monitor, "scan_host_once", scan)
    result = await loop.run_local_agent_paper_loop(
        EngineSettings(
            database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
            agent_paper_account_id=ACCOUNT,
            agent_paper_cycle_enabled=True,
        ),
        account_id=ACCOUNT,
        repo_root=tmp_path,
        clock=lambda: AT,
    )
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    captured = capsys.readouterr()
    assert "private-provider-canary" not in captured.out + captured.err
    assert [json.loads(line) for line in captured.err.splitlines()] == [
        {
            "event": "research.monitor.scan_failed",
            "error_code": "MONITOR_SCAN_FAILED",
            "scheduled_at": stamp.isoformat(),
        }
        for stamp in (AT, AT + timedelta(minutes=5))
    ]


@pytest.mark.asyncio
async def test_cancelled_host_collects_owned_scan_before_releasing_ownership(monkeypatch, tmp_path):
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def scan(*_args, **_kwargs):
        started.set()
        try:
            await release.wait()
        finally:
            finished.set()

    async def calendar(_settings):
        return CALENDAR

    async def heartbeat(*_args, **_kwargs):
        pass

    async def foreground(**kwargs):
        await kwargs["heartbeat"](AT)
        await asyncio.Event().wait()

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    monkeypatch.setattr(research_monitor, "scan_host_once", scan)
    task = asyncio.create_task(
        loop.run_local_agent_paper_loop(
            EngineSettings(
                database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
                agent_paper_account_id=ACCOUNT,
                agent_paper_cycle_enabled=True,
            ),
            account_id=ACCOUNT,
            repo_root=tmp_path,
            clock=lambda: AT,
        )
    )
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert not any(t.get_name() == "nexus-price-monitor" for t in asyncio.all_tasks())
