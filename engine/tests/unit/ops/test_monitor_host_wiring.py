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


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["database", "account", "calendar", "model", "batch"])
async def test_running_host_cannot_rebind_monitor_identity_after_inspection(
    monkeypatch, tmp_path, changed
):
    from nexus.ops import formal_budget_protection, monitor_dispatch

    env = {
        "DATABASE_URL": "postgresql+asyncpg://localhost/nexus_test_monitor",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "AGENT_RUNTIME_MODE": "codex_sdk",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(ACCOUNT),
        "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(UUID(int=3)),
        "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=4)),
    }
    settings = EngineSettings.model_validate(env)
    scans = []
    seen = []
    did_scan = asyncio.Event()
    mutated = False

    async def bound(*args, **kwargs):
        return dict(env), str(UUID(int=99) if mutated and changed == "calendar" else CALENDAR)

    async def calendar(_settings):
        return CALENDAR

    async def evaluation(*args, **kwargs):
        return None

    async def inspect(current, **kwargs):
        seen.append(current.database_url)
        return loop.AgentPaperLoopSnapshot(now=AT, enabled=True)

    async def queue(_settings, **kwargs):
        return kwargs["snapshot"]

    async def scan(current, **kwargs):
        scans.append(current.database_url)
        did_scan.set()

    async def heartbeat(current, **kwargs):
        assert current.database_url == settings.database_url

    async def foreground(**kwargs):
        nonlocal mutated
        await kwargs["inspect"]()
        await kwargs["heartbeat"](AT)
        await did_scan.wait()
        mutated = True
        key = {
            "database": "DATABASE_URL",
            "account": "NEXUS_AGENT_PAPER_ACCOUNT_ID",
            "model": "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID",
            "batch": "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID",
        }.get(changed)
        if key:
            env[key] = env[key] + "_other" if changed == "database" else str(UUID(int=99))
        with pytest.raises(ValueError, match="MONITOR_HOST_SCOPE_CHANGED"):
            await kwargs["inspect"]()
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(
        loop, "load_agent_paper_service_environment", lambda *args, **kwargs: dict(env)
    )
    monkeypatch.setattr(formal_budget_protection, "bind_runtime_environment", bound)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", evaluation)
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(monitor_dispatch, "inspect_monitor_queue", queue)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(research_monitor, "scan_configured_once", scan)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    result = await loop.run_local_agent_paper_loop(
        settings, account_id=ACCOUNT, repo_root=tmp_path, clock=lambda: AT
    )
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    assert seen == [settings.database_url] and scans == [settings.database_url]


@pytest.mark.asyncio
async def test_normal_host_observes_sources_before_its_read_only_scan(monkeypatch, tmp_path):
    events = []

    async def observe(_settings, **kwargs):
        assert kwargs["scheduled_at"] == AT
        events.append("observe")
        return {"status": "refreshed"}

    async def scan(_settings, **kwargs):
        assert events == ["observe"]
        events.append("scan")

    async def calendar(_settings):
        return CALENDAR

    async def heartbeat(_settings, **kwargs):
        pass

    async def foreground(**kwargs):
        await kwargs["heartbeat"](AT)
        for _ in range(20):
            if events == ["observe", "scan"]:
                break
            await asyncio.sleep(0)
        assert events == ["observe", "scan"]
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(loop, "run_foreground_loop", foreground)
    monkeypatch.setattr(research_monitor, "refresh_host_observations", observe, raising=False)
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
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED and events == ["observe", "scan"]


def test_observe_is_a_normal_protected_cli_action():
    args = research_monitor.parser().parse_args(
        ["observe", "--calendar-authority-id", str(CALENDAR)]
    )
    assert args.action == "observe" and args.calendar_authority_id == CALENDAR


@pytest.mark.asyncio
@pytest.mark.parametrize("increment", [False, True], ids=["daily", "increment"])
async def test_real_foreground_keeps_five_minute_checks_during_research(
    monkeypatch, tmp_path, increment
):
    from nexus.ops import formal_budget_protection, manual_paper_host, monitor_dispatch
    from unit.ops.test_agent_paper_loop import snapshot

    stamps = [AT, AT + timedelta(minutes=5), AT + timedelta(minutes=15)]
    clock = [AT]
    started, release, scanned = asyncio.Event(), asyncio.Event(), asyncio.Event()
    preparations, scans, beats, executions = [], [], [], []
    env = {
        "DATABASE_URL": "postgresql+asyncpg://localhost/nexus_test_monitor",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(ACCOUNT),
    }
    settings = EngineSettings.model_validate(env)
    expected = (
        loop.AgentPaperLoopAction.RUN_MONITOR_INCREMENT
        if increment
        else loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
    )
    state = snapshot(
        now=AT,
        calendar_authority_id=CALENDAR,
        cycle_slot_at=AT,
        cycle_status="completed" if increment else "researching",
        research_job_status="queued",
        research_job_id=UUID(int=30),
        monitor_trigger_id=UUID(int=40) if increment else None,
        monitor_trigger_status="admitted" if increment else None,
    )
    assert loop.choose_loop_action(state) == expected

    async def calendar(_):
        return CALENDAR

    async def bind(_settings, environment):
        return environment, None

    async def evaluation(*_a, **_k):
        return None

    async def inspect(*_a, **_k):
        return state

    async def passthrough(*_a, **kwargs):
        return kwargs["snapshot"]

    async def heartbeat(_settings, **kwargs):
        beats.append(kwargs["seen_at"])

    async def execute(_settings, **kwargs):
        executions.append(kwargs["action"])
        started.set()
        await release.wait()
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    async def prepare(_settings, **kwargs):
        preparations.append(kwargs["scheduled_at"])

    async def scan(*_a, **_k):
        await started.wait()
        assert not release.is_set()
        scans.append(clock[0])
        scanned.set()

    async def sleep(_seconds):
        await scanned.wait()
        scanned.clear()
        if len(scans) < len(stamps):
            clock[0] = stamps[len(scans)]
        else:
            release.set()
            await asyncio.Event().wait()  # cancelled by the real foreground cleanup

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", calendar)
    monkeypatch.setattr(loop, "load_agent_paper_service_environment", lambda *_a, **_k: env)
    monkeypatch.setattr(formal_budget_protection, "bind_runtime_environment", bind)
    monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", evaluation)
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_close", passthrough)
    monkeypatch.setattr(manual_paper_host, "inspect_manual_queue", passthrough)
    monkeypatch.setattr(monitor_dispatch, "inspect_monitor_queue", passthrough)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(loop, "execute_agent_paper_loop_action", execute)
    monkeypatch.setattr(research_monitor, "refresh_host_observations", prepare)
    monkeypatch.setattr(research_monitor, "scan_host_once", scan)
    async with asyncio.timeout(10):
        result = await loop.run_local_agent_paper_loop(
            settings,
            account_id=ACCOUNT,
            repo_root=tmp_path,
            sleep=sleep,
            clock=lambda: clock[0],
            monotonic=lambda: (clock[0] - AT).total_seconds(),
        )
    assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    assert executions == [expected]
    assert preparations == scans == stamps  # missing 14:10 stays unobserved
    assert all(stamp in beats for stamp in stamps)
    assert not any(t.get_name() == "nexus-price-monitor" for t in asyncio.all_tasks())
