import asyncio
import gc
import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.paper.agent_market import AgentPaperMarketScope
from nexus.persistence.models.paper import PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
from nexus.providers.http import HttpPolicyError

NOW = datetime(2026, 9, 14, 14, 5, tzinfo=UTC)
SLOT = datetime(2026, 9, 14, 14, 5, tzinfo=UTC)
PREVIOUS_SLOT = datetime(2026, 9, 14, 14, 0, tzinfo=UTC)
AFTER_CLOSE = datetime(2026, 9, 14, 20, 1, tzinfo=UTC)
LAST_SLOT = datetime(2026, 9, 14, 19, 55, tzinfo=UTC)
A = loop.AgentPaperLoopAction


async def noop_heartbeat(_seen_at: datetime) -> None:
    return None


@pytest.fixture
def codex_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)


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
        (
            {
                "cycle_status": "completed",
                "cycle_slot_at": PREVIOUS_SLOT,
                "performance_due_at": datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
            },
            A.PUBLISH_PERFORMANCE,
        ),
        ({"cycle_status": "unavailable", "cycle_slot_at": SLOT}, A.STOP_CYCLE_UNAVAILABLE),
        ({"cycle_status": "failed", "cycle_slot_at": SLOT}, A.STOP_CYCLE_UNAVAILABLE),
        (
            {
                "now": NOW + timedelta(minutes=5),
                "cycle_status": "unavailable",
                "cycle_slot_at": SLOT,
            },
            A.TRIGGER,
        ),
        (
            {
                "now": NOW + timedelta(minutes=5),
                "cycle_status": "failed",
                "cycle_slot_at": SLOT,
            },
            A.TRIGGER,
        ),
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

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=no_sleep,
            heartbeat=noop_heartbeat,
        )
    )
    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert actions == [(loop.AgentPaperLoopAction.RUN_RESEARCH_JOB, "job-1")]


def test_foreground_loop_honors_known_executor_stop_without_reinspection() -> None:
    inspections = 0

    async def inspect():
        nonlocal inspections
        inspections += 1
        if inspections > 1:
            return snapshot(
                now=AFTER_CLOSE,
                cycle_status="completed",
                cycle_slot_at=LAST_SLOT,
            )
        return snapshot(
            cycle_id=UUID(int=2),
            cycle_status="awaiting_market",
            market_window_ready=True,
            has_open_order=True,
            order_id=UUID(int=3),
        )

    async def execute(_action, _state):
        return A.STOP_ORDER_UNAVAILABLE

    async def no_sleep(_seconds: float) -> None:
        raise AssertionError("a known no-trade result must stop without sleeping")

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=no_sleep,
            heartbeat=noop_heartbeat,
        )
    )

    assert result == A.STOP_ORDER_UNAVAILABLE
    assert inspections == 1


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

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=sleep,
            heartbeat=noop_heartbeat,
        )
    )
    assert result == loop.AgentPaperLoopAction.STOP_SESSION_CLOSED
    assert sum(sleeps) == expected_sleep
    assert all(seconds <= 15 for seconds in sleeps)


def test_foreground_loop_heartbeats_before_inspection_and_after_action() -> None:
    events: list[str] = []
    heartbeat_times: list[datetime] = []
    times = iter((NOW, NOW + timedelta(seconds=1)))
    states = iter(
        (
            snapshot(
                cycle_status="researching",
                research_job_status="queued",
                research_job_id="job-1",
            ),
            snapshot(now=AFTER_CLOSE, cycle_status="completed", cycle_slot_at=LAST_SLOT),
        )
    )

    async def heartbeat(seen_at: datetime) -> None:
        events.append("heartbeat")
        heartbeat_times.append(seen_at)

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        events.append("inspect")
        return next(states)

    async def execute(_action, _state) -> None:
        events.append("execute")

    async def no_sleep(_seconds: float) -> None:
        raise AssertionError("persisted actions do not sleep")

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=no_sleep,
            heartbeat=heartbeat,
            clock=lambda: next(times),
        )
    )

    assert result == A.STOP_SESSION_CLOSED
    assert events == ["heartbeat", "inspect", "execute", "heartbeat", "inspect"]
    assert heartbeat_times == [NOW, NOW + timedelta(seconds=1)]


def test_foreground_loop_keeps_heartbeat_live_during_long_action() -> None:
    heartbeat_count = 0
    execute_started = asyncio.Event()
    release_execute = asyncio.Event()
    states = iter(
        (
            snapshot(
                cycle_status="researching",
                research_job_status="queued",
                research_job_id="job-1",
            ),
            snapshot(now=AFTER_CLOSE, cycle_status="completed", cycle_slot_at=LAST_SLOT),
        )
    )

    async def heartbeat(_seen_at: datetime) -> None:
        nonlocal heartbeat_count
        heartbeat_count += 1
        if heartbeat_count == 3:
            release_execute.set()

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        return next(states)

    async def execute(_action, _state) -> None:
        execute_started.set()
        await release_execute.wait()

    async def controlled_sleep(_seconds: float) -> None:
        await execute_started.wait()

    async def run() -> loop.AgentPaperLoopAction:
        return await asyncio.wait_for(
            loop.run_foreground_loop(
                inspect=inspect,
                execute=execute,
                sleep=controlled_sleep,
                heartbeat=heartbeat,
                clock=lambda: NOW,
            ),
            timeout=1,
        )

    assert asyncio.run(run()) == A.STOP_SESSION_CLOSED
    assert heartbeat_count == 4


def test_foreground_loop_uses_fixed_heartbeat_cadence_during_long_action() -> None:
    heartbeat_count = 0
    monotonic_now = 0.0
    release_execute = asyncio.Event()
    sleeps: list[float] = []
    states = iter(
        (
            snapshot(
                cycle_status="researching",
                research_job_status="queued",
                research_job_id="job-1",
            ),
            snapshot(now=AFTER_CLOSE, cycle_status="completed", cycle_slot_at=LAST_SLOT),
        )
    )

    async def heartbeat(_seen_at: datetime) -> None:
        nonlocal heartbeat_count, monotonic_now
        heartbeat_count += 1
        if heartbeat_count in {2, 3}:
            monotonic_now += 4
        if heartbeat_count == 3:
            release_execute.set()
            await asyncio.sleep(0)

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        return next(states)

    async def execute(_action, _state) -> None:
        await release_execute.wait()

    async def controlled_sleep(seconds: float) -> None:
        nonlocal monotonic_now
        sleeps.append(seconds)
        monotonic_now += seconds

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=controlled_sleep,
            heartbeat=heartbeat,
            heartbeat_interval=timedelta(seconds=10),
            clock=lambda: NOW,
            monotonic=lambda: monotonic_now,
        )
    )

    assert result == A.STOP_SESSION_CLOSED
    assert sleeps == [10.0, 6.0]


def test_foreground_loop_stops_long_action_when_heartbeat_fails() -> None:
    heartbeat_count = 0
    execute_started = asyncio.Event()
    execute_cancelled = asyncio.Event()

    async def heartbeat(_seen_at: datetime) -> None:
        nonlocal heartbeat_count
        heartbeat_count += 1
        if heartbeat_count == 2:
            raise RuntimeError("database unavailable")

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        return snapshot(
            cycle_status="researching",
            research_job_status="queued",
            research_job_id="job-1",
        )

    async def execute(_action, _state) -> None:
        execute_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            execute_cancelled.set()

    async def controlled_sleep(_seconds: float) -> None:
        await execute_started.wait()

    async def run() -> loop.AgentPaperLoopAction:
        return await asyncio.wait_for(
            loop.run_foreground_loop(
                inspect=inspect,
                execute=execute,
                sleep=controlled_sleep,
                heartbeat=heartbeat,
                clock=lambda: NOW,
            ),
            timeout=1,
        )

    assert asyncio.run(run()) == A.STOP_RESULT_UNKNOWN
    assert heartbeat_count == 2
    assert execute_cancelled.is_set()


def test_foreground_loop_retrieves_action_failure_when_heartbeat_also_fails() -> None:
    heartbeat_count = 0
    execute_started = asyncio.Event()
    release_execute = asyncio.Event()
    unhandled: list[dict[str, object]] = []

    async def heartbeat(_seen_at: datetime) -> None:
        nonlocal heartbeat_count
        heartbeat_count += 1
        if heartbeat_count == 2:
            release_execute.set()
            await asyncio.sleep(0)
            raise RuntimeError("database unavailable")

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        return snapshot(
            cycle_status="researching",
            research_job_status="queued",
            research_job_id="job-1",
        )

    async def execute(_action, _state) -> None:
        execute_started.set()
        await release_execute.wait()
        raise RuntimeError("action failed")

    async def controlled_sleep(_seconds: float) -> None:
        await execute_started.wait()

    async def run() -> loop.AgentPaperLoopAction:
        running_loop = asyncio.get_running_loop()
        running_loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
        result = await loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=controlled_sleep,
            heartbeat=heartbeat,
            clock=lambda: NOW,
        )
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        return result

    assert asyncio.run(run()) == A.STOP_RESULT_UNKNOWN
    assert unhandled == []


def test_foreground_loop_heartbeats_during_long_waits() -> None:
    sleeps: list[float] = []
    heartbeat_times: list[datetime] = []
    times = iter(
        (
            NOW,
            NOW + timedelta(seconds=15),
            NOW + timedelta(seconds=30),
            NOW + timedelta(seconds=31),
        )
    )
    states = iter(
        (
            snapshot(
                cycle_status="awaiting_market",
                has_open_order=True,
                market_window_end_at=NOW + timedelta(seconds=31),
            ),
            snapshot(now=AFTER_CLOSE, cycle_status="completed", cycle_slot_at=LAST_SLOT),
        )
    )

    async def heartbeat(seen_at: datetime) -> None:
        heartbeat_times.append(seen_at)

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def execute(_action, _state) -> None:
        raise AssertionError("waiting must not execute a product action")

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        return next(states)

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=sleep,
            heartbeat=heartbeat,
            clock=lambda: next(times),
        )
    )

    assert result == A.STOP_SESSION_CLOSED
    assert sleeps == [15.0, 15.0, 1.0]
    assert heartbeat_times == list(
        (
            NOW,
            NOW + timedelta(seconds=15),
            NOW + timedelta(seconds=30),
            NOW + timedelta(seconds=31),
        )
    )


def test_foreground_loop_stops_before_inspection_when_heartbeat_fails() -> None:
    async def heartbeat(_seen_at: datetime) -> None:
        raise RuntimeError("database unavailable")

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        raise AssertionError("heartbeat failure must fence inspection")

    async def execute(_action, _state) -> None:
        raise AssertionError("heartbeat failure must fence product actions")

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=asyncio.sleep,
            heartbeat=heartbeat,
            clock=lambda: NOW,
        )
    )

    assert result == A.STOP_RESULT_UNKNOWN


def test_foreground_loop_stops_after_action_when_followup_heartbeat_fails() -> None:
    heartbeat_count = 0
    inspection_count = 0

    async def heartbeat(_seen_at: datetime) -> None:
        nonlocal heartbeat_count
        heartbeat_count += 1
        if heartbeat_count == 2:
            raise RuntimeError("database unavailable")

    async def inspect() -> loop.AgentPaperLoopSnapshot:
        nonlocal inspection_count
        inspection_count += 1
        return snapshot(
            cycle_status="researching",
            research_job_status="queued",
            research_job_id="job-1",
        )

    async def execute(_action, _state) -> None:
        return None

    result = asyncio.run(
        loop.run_foreground_loop(
            inspect=inspect,
            execute=execute,
            sleep=asyncio.sleep,
            heartbeat=heartbeat,
            clock=lambda: NOW,
        )
    )

    assert result == A.STOP_RESULT_UNKNOWN
    assert heartbeat_count == 2
    assert inspection_count == 1


def enabled_environment() -> dict[str, str]:
    return {
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
        "AGENT_RUNTIME_MODE": "codex_sdk",
    }


def service_ready_environment() -> dict[str, str]:
    return {
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=1)),
        "NEXUS_ENV": "development",
        "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
        "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "nexus",
        "OBJECT_STORE_ACCESS_KEY_ID": "object-access",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "object-secret",
        "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID": "current",
        "NEXUS_ENGINE_HMAC_CURRENT_SECRET": "hmac-secret",
        "AGENT_RUNTIME_MODE": "codex_sdk",
        "ALPACA_API_KEY_ID": "alpaca-key",
        "ALPACA_API_SECRET_KEY": "alpaca-secret",
        "NEXUS_SCHWAB_APP_KEY": "schwab-key",
        "NEXUS_SCHWAB_APP_SECRET": "schwab-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }


def test_service_environment_localizes_compose_endpoints_after_process_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "DATABASE_URL": "postgresql+asyncpg://u:p@postgres:5432/nexus",
            "OBJECT_STORE_ENDPOINT": "http://minio:9000",
            "NEXUS_POSTGRES_HOST_PORT": "58000",
            "NEXUS_MINIO_HOST_PORT": "59000",
        },
    )

    environment = loop.load_agent_paper_service_environment(tmp_path, {})

    assert environment["DATABASE_URL"] == ("postgresql+asyncpg://u:p@127.0.0.1:58000/nexus")
    assert environment["OBJECT_STORE_ENDPOINT"] == "http://127.0.0.1:59000"


def test_local_loop_reinspection_uses_service_environment_loader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    environment = enabled_environment()
    service_loads: list[object] = []

    def load_service(repo_root, process_environment):
        assert repo_root == tmp_path
        service_loads.append(process_environment)
        return environment

    async def inspect(_settings, *, account_id):
        assert account_id == UUID(int=1)
        return snapshot()

    async def run_loop(*, inspect, **_kwargs):
        await inspect()
        return A.STOP_SESSION_CLOSED

    monkeypatch.setattr(loop, "load_agent_paper_service_environment", load_service)
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("the loop must not reload unlocalized Compose endpoints")
        ),
    )
    monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect)
    monkeypatch.setattr(loop, "run_foreground_loop", run_loop)

    result = asyncio.run(
        loop.run_local_agent_paper_loop(
            loop.EngineSettings(database_url=environment["DATABASE_URL"]),
            account_id=UUID(int=1),
            repo_root=tmp_path,
        )
    )

    assert result == A.STOP_SESSION_CLOSED
    assert service_loads == [os.environ]


def test_service_preflight_aggregates_safe_blockers_without_external_requests(
    tmp_path,
) -> None:
    result = loop.agent_paper_service_preflight({}, repo_root=tmp_path, now=NOW)

    assert result == {
        "status": "blocked",
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": [
            "AGENT_PAPER_ACCOUNT_INVALID",
            "AGENT_PAPER_CYCLE_DISABLED",
            "AGENT_PAPER_ENGINE_CONFIG_INVALID",
            "AGENT_PAPER_MARKET_SOURCE_CONFIG_REQUIRED",
            "AGENT_PAPER_MODEL_CONFIG_REQUIRED",
            "AGENT_PAPER_OUTPUT_DIRECTORY_INVALID",
            "AGENT_PAPER_SCHWAB_APP_CONFIG_INVALID",
            "SCHWAB_OAUTH_TOKEN_STORE_INVALID",
        ],
        "optional_codes": ["AGENT_PAPER_FRED_OPTIONAL_UNCONFIGURED"],
    }


def test_service_preflight_accepts_complete_local_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: "access-token-canary",
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: True)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)
    environment = service_ready_environment() | {"FRED_API_KEY": "fred-secret"}

    assert loop.agent_paper_service_preflight(environment, repo_root=tmp_path, now=NOW) == {
        "status": "ready",
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": [],
        "optional_codes": [],
    }


def test_service_preflight_rejects_responses_for_codex_actionable_research(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: "access-token-canary",
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: True)
    environment = service_ready_environment() | {
        "AGENT_RUNTIME_MODE": "responses",
        "OPENAI_API_KEY": "openai-secret",
        "OPENAI_EXTRACTION_MODEL": "extract-model",
        "OPENAI_REASONING_MODEL": "reason-model",
        "FRED_API_KEY": "fred-secret",
    }

    result = loop.agent_paper_service_preflight(environment, repo_root=tmp_path, now=NOW)

    assert result["status"] == "blocked"
    assert result["blocking_codes"] == ["AGENT_PAPER_MODEL_RUNTIME_MISMATCH"]


def test_service_preflight_rejects_runtime_configuration_startup_would_reject(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: "access-token-canary",
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: True)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)
    environment = service_ready_environment() | {
        "AGENT_DAILY_BUDGET_USD": "0",
        "FRED_API_KEY": "fred-secret",
    }

    result = loop.agent_paper_service_preflight(environment, repo_root=tmp_path, now=NOW)

    assert result["status"] == "blocked"
    assert result["blocking_codes"] == ["AGENT_PAPER_RUNTIME_CONFIG_INVALID"]


def test_service_preflight_treats_expired_access_token_as_refreshable_warning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            loop.SchwabOAuthError("SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED")
        ),
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: True)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)
    environment = service_ready_environment() | {"FRED_API_KEY": "fred-secret"}

    result = loop.agent_paper_service_preflight(environment, repo_root=tmp_path, now=NOW)

    assert result == {
        "status": "ready",
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": [],
        "optional_codes": ["SCHWAB_OAUTH_ACCESS_TOKEN_REFRESH_REQUIRED"],
    }


def test_service_preflight_blocks_read_only_schwab_token_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: "access-token-canary",
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: False)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)
    environment = service_ready_environment() | {"FRED_API_KEY": "fred-secret"}

    result = loop.agent_paper_service_preflight(environment, repo_root=tmp_path, now=NOW)

    assert result["status"] == "blocked"
    assert result["blocking_codes"] == ["AGENT_PAPER_SCHWAB_TOKEN_STORE_READ_ONLY"]


def test_service_preflight_blocks_missing_codex_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_directory = tmp_path / ".secrets" / "nexus-agent-paper"
    output_directory.mkdir(parents=True, mode=0o700)
    output_directory.chmod(0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *_args: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *_args: object())
    monkeypatch.setattr(
        loop,
        "require_live_schwab_access_token",
        lambda *_args, **_kwargs: "access-token-canary",
    )
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *_args: True)
    monkeypatch.setattr(
        loop,
        "_codex_sdk_preflight_blocker",
        lambda: "CODEX_CHATGPT_LOGIN_REQUIRED",
    )

    result = loop.agent_paper_service_preflight(
        service_ready_environment() | {"FRED_API_KEY": "fred-secret"},
        repo_root=tmp_path,
        now=NOW,
    )

    assert result["status"] == "blocked"
    assert result["blocking_codes"] == ["CODEX_CHATGPT_LOGIN_REQUIRED"]


@pytest.mark.parametrize(
    ("status_result", "expected"),
    [
        ({"ready_for_model_requests": True, "blocker": None}, None),
        (
            {
                "ready_for_model_requests": False,
                "blocker": "CODEX_CHATGPT_LOGIN_REQUIRED",
            },
            "CODEX_CHATGPT_LOGIN_REQUIRED",
        ),
        (
            {"ready_for_model_requests": False, "blocker": "unexpected"},
            "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE",
        ),
    ],
)
def test_codex_sdk_preflight_blocker_maps_only_safe_status(
    monkeypatch: pytest.MonkeyPatch,
    status_result: dict[str, object],
    expected: str | None,
) -> None:
    async def status() -> dict[str, object]:
        return status_result

    monkeypatch.setattr(loop, "sdk_status", status)

    assert loop._codex_sdk_preflight_blocker() == expected


def test_codex_sdk_preflight_blocker_masks_status_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def status() -> dict[str, object]:
        raise RuntimeError("private-codex-status-canary")

    monkeypatch.setattr(loop, "sdk_status", status)

    assert loop._codex_sdk_preflight_blocker() == "AGENT_PAPER_CODEX_STATUS_UNAVAILABLE"


def test_service_preflight_cli_masks_configuration_values(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    canary = "private-preflight-canary"
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {"OPENAI_API_KEY": canary},
    )

    assert loop.main(["--service-preflight"]) == 1
    output = capsys.readouterr().out
    assert canary not in output
    assert json.loads(output)["status"] == "blocked"


def test_loop_command_reads_account_and_runtime_settings_from_process_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
) -> None:
    account_id = UUID(int=1)
    captured_process_environment: list[dict[str, str]] = []

    def load(_root, process_environment):
        captured_process_environment.append(dict(process_environment))
        return enabled_environment() | {
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": process_environment[
                "NEXUS_AGENT_PAPER_ACCOUNT_ID"
            ]
        }

    async def stop(_settings, *, account_id: UUID, repo_root):
        assert account_id == UUID(int=1)
        return loop.AgentPaperLoopAction.STOP_SESSION_CLOSED

    monkeypatch.setenv("NEXUS_AGENT_PAPER_ACCOUNT_ID", str(account_id))
    monkeypatch.setattr(loop, "load_local_environment", load)
    monkeypatch.setattr(loop, "run_local_agent_paper_loop", stop)

    assert loop.main([]) == 0
    assert captured_process_environment == [dict(os.environ)]
    assert json.loads(capsys.readouterr().out)["reason"] == "stop_session_closed"


def test_loop_command_reads_account_from_loaded_local_configuration(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
) -> None:
    account_id = UUID(int=1)
    environment = enabled_environment() | {"NEXUS_AGENT_PAPER_ACCOUNT_ID": str(account_id)}
    monkeypatch.delenv("NEXUS_AGENT_PAPER_ACCOUNT_ID", raising=False)
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: environment)

    async def stop(_settings, *, account_id: UUID, repo_root):
        assert account_id == UUID(int=1)
        return loop.AgentPaperLoopAction.STOP_SESSION_CLOSED

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", stop)

    assert loop.main([]) == 0
    assert json.loads(capsys.readouterr().out)["reason"] == "stop_session_closed"


def test_loop_command_is_default_off(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: {})
    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "AGENT_PAPER_CYCLE_DISABLED"}


def test_loop_command_stays_disabled_before_optional_model_config_validation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "false",
            "AGENT_RUNTIME_MODE": "responses",
        },
    )

    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "AGENT_PAPER_CYCLE_DISABLED"}


def test_loop_command_rejects_responses_runtime_before_running(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    environment = enabled_environment() | {"AGENT_RUNTIME_MODE": "responses"}
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: environment)

    async def must_not_run(*_args, **_kwargs):
        raise AssertionError("loop must not run")

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", must_not_run)

    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "AGENT_PAPER_MODEL_RUNTIME_MISMATCH"
    }


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
        (
            loop.AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE,
            1,
            {"error_code": "AGENT_PAPER_LOOP_PERFORMANCE_UNAVAILABLE"},
        ),
        (
            loop.AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE,
            1,
            {"error_code": "AGENT_PAPER_LOOP_PREREQUISITE_UNAVAILABLE"},
        ),
    ],
)
def test_loop_command_exposes_only_safe_stop_reasons(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
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


@pytest.mark.parametrize(
    "error_code",
    [
        "AGENT_PAPER_ACCOUNT_INVALID",
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "RISK_INPUT_REQUIRED",
        "SCHWAB_L1_FRESHNESS_UNPROVEN",
        "SCHWAB_L1_INCOMPLETE",
    ],
)
def test_loop_command_exposes_persisted_trigger_failure_codes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
    error_code: str,
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: enabled_environment())

    async def fail(_settings, *, account_id, repo_root):
        raise ValueError(error_code)

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", fail)

    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": error_code}


def test_loop_command_masks_unknown_failure_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
) -> None:
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: enabled_environment())

    async def fail(_settings, *, account_id, repo_root):
        raise RuntimeError("private-failure-canary")

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", fail)

    assert loop.main(["--account-id", str(UUID(int=1))]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN"}
    assert "private-failure-canary" not in captured.out + captured.err


def test_loop_command_rejects_invalid_account_without_echo(
    capsys: pytest.CaptureFixture[str],
) -> None:
    canary = "private-account-canary"

    exit_code = loop.main(["--account-id", canary])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == '{"error_code": "AGENT_PAPER_ACCOUNT_INVALID"}\n'
    assert captured.err == ""
    assert canary not in captured.out + captured.err


def test_loop_command_reports_interrupt_without_leaking_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    codex_ready: None,
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
    assert json.loads(capsys.readouterr().out) == {"error_code": "AGENT_PAPER_LOOP_INTERRUPTED"}


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
    preparations: list[tuple[UUID, datetime]] = []
    trend_preparations: list[tuple[UUID, datetime]] = []
    macro_preparations: list[tuple[UUID, datetime]] = []
    received_settings: list[object] = []
    environment = {
        **enabled_environment(),
        "ALPACA_API_KEY_ID": "key",
        "ALPACA_API_SECRET_KEY": "secret",
    }
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: environment)

    async def prepare(settings, *, account_id, repo_root, now):
        assert repo_root == loop.Path("/repo")
        received_settings.append(settings)
        preparations.append((account_id, now))
        return "ready"

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare, raising=False)

    async def prepare_macro(settings, *, fred_key, account_id, repo_root, now):
        assert repo_root == loop.Path("/repo")
        assert fred_key.get_secret_value() == ""
        assert isinstance(settings, loop.DailyOnceSettings)
        macro_preparations.append((account_id, now))
        return "ready"

    monkeypatch.setattr(loop, "prepare_macro_once", prepare_macro, raising=False)

    async def prepare_trends(settings, *, account_id, repo_root, now):
        assert repo_root == loop.Path("/repo")
        received_settings.append(settings)
        trend_preparations.append((account_id, now))
        return "ready"

    monkeypatch.setattr(loop, "prepare_current_trends_once", prepare_trends, raising=False)

    async def trigger(settings, request, *, repo_root):
        received_settings.append(settings)
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
    assert len(received_settings) == 3
    assert received_settings[0] is received_settings[1]
    assert received_settings[1] is received_settings[2]
    assert isinstance(received_settings[0], loop.AgentPaperTriggerSettings)
    assert preparations == [(UUID(int=1), datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC))]
    assert macro_preparations == [(UUID(int=1), datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC))]
    assert trend_preparations == [(UUID(int=1), datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC))]
    assert request.account_id == UUID(int=1)
    assert request.idempotency_key == "agent-paper-loop:2026-09-14T14:05:00+00:00"


def test_executor_reinspects_after_preparing_missing_prior_daily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = {
        **enabled_environment(),
        "ALPACA_API_KEY_ID": "key",
        "ALPACA_API_SECRET_KEY": "secret",
    }
    monkeypatch.setattr(loop, "load_local_environment", lambda *_args: environment)

    async def prepare(_settings, *, account_id, repo_root, now):
        return "prepared"

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare, raising=False)

    async def unexpected_trigger(*_args, **_kwargs):
        raise AssertionError("a newly imported daily authority must be read back first")

    monkeypatch.setattr(loop, "trigger_once", unexpected_trigger)
    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(now=datetime(2026, 9, 14, 14, 7, 12, tzinfo=UTC)),
        )
    )

    assert result is None


def test_executor_stops_before_cycle_when_prior_daily_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            **enabled_environment(),
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )

    async def prepare(_settings, *, account_id, repo_root, now):
        return "unavailable"

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare, raising=False)

    async def unexpected_trigger(*_args, **_kwargs):
        raise AssertionError("missing prior daily evidence must not create a cycle")

    monkeypatch.setattr(loop, "trigger_once", unexpected_trigger)
    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(),
        )
    )

    assert result == "stop_prerequisite_unavailable"


@pytest.mark.parametrize(
    ("trend_status", "expected"),
    [
        ("prepared", None),
        ("unavailable", A.STOP_PREREQUISITE_UNAVAILABLE),
    ],
)
def test_executor_does_not_trigger_until_current_trends_are_ready(
    monkeypatch: pytest.MonkeyPatch,
    trend_status: str,
    expected: loop.AgentPaperLoopAction | None,
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            **enabled_environment(),
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )

    async def prepare_daily(_settings, *, account_id, repo_root, now):
        return "ready"

    async def prepare_trends(_settings, *, account_id, repo_root, now):
        return trend_status

    async def prepare_macro(_settings, *, fred_key, account_id, repo_root, now):
        return "ready"

    async def unexpected_trigger(*_args, **_kwargs):
        raise AssertionError("research must wait for persisted current-trends readback")

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare_daily)
    monkeypatch.setattr(loop, "prepare_macro_once", prepare_macro)
    monkeypatch.setattr(loop, "prepare_current_trends_once", prepare_trends)
    monkeypatch.setattr(loop, "trigger_once", unexpected_trigger)

    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(),
        )
    )

    assert result == expected


def test_executor_continues_when_daily_macro_refresh_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            **enabled_environment(),
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )
    events: list[str] = []

    async def prepare_daily(_settings, *, account_id, repo_root, now):
        events.append("daily")
        return "ready"

    async def prepare_macro(_settings, *, fred_key, account_id, repo_root, now):
        events.append("macro")
        return "unavailable"

    async def prepare_trends(_settings, *, account_id, repo_root, now):
        events.append("trends")
        return "ready"

    async def trigger(_settings, _request, *, repo_root):
        events.append("trigger")

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare_daily)
    monkeypatch.setattr(loop, "prepare_macro_once", prepare_macro, raising=False)
    monkeypatch.setattr(loop, "prepare_current_trends_once", prepare_trends)
    monkeypatch.setattr(loop, "trigger_once", trigger)

    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(),
        )
    )

    assert result is None
    assert events == ["daily", "macro", "trends", "trigger"]


def test_executor_reinspects_after_binding_macro_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        loop,
        "load_local_environment",
        lambda *_args: {
            **enabled_environment(),
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )

    async def prepare_daily(_settings, *, account_id, repo_root, now):
        return "ready"

    async def prepare_macro(_settings, *, fred_key, account_id, repo_root, now):
        return "prepared"

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("newly bound macro evidence must be read back before triggering")

    monkeypatch.setattr(loop, "prepare_prior_daily_once", prepare_daily)
    monkeypatch.setattr(loop, "prepare_macro_once", prepare_macro)
    monkeypatch.setattr(loop, "prepare_current_trends_once", unexpected)
    monkeypatch.setattr(loop, "trigger_once", unexpected)

    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=snapshot(),
        )
    )

    assert result is None


def test_prior_daily_preparation_collects_imports_and_reads_back_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    instrument_id = UUID(int=20)
    prior_session = date(2026, 9, 11)
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=instrument_id,
                prior_session=prior_session,
                authority_ready=False,
            ),
            SimpleNamespace(
                instrument_id=instrument_id,
                prior_session=prior_session,
                authority_ready=True,
            ),
        )
    )
    calls: list[tuple[object, ...]] = []

    async def inspect(_settings, *, account_id, now):
        calls.append(("inspect", account_id, now))
        return next(scopes)

    def window(received_instrument_id, start, end, adjustment, *, now):
        calls.append(("window", received_instrument_id, start, end, adjustment, now))
        return "request", {"session": prior_session.isoformat()}

    async def collect(request, **kwargs):
        calls.append(("collect", request, kwargs["identity_asof"], kwargs["max_pages"]))
        return {"status": "collected", "readback_verified": True}

    async def import_authority(_settings, **kwargs):
        calls.append(("import", kwargs["instrument_id"], kwargs["purpose"]))
        return {"status": "stored_authority"}

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collection_window", window, raising=False)
    monkeypatch.setattr(loop, "collect_prices", collect, raising=False)
    monkeypatch.setattr(loop, "import_adjusted_authority", import_authority, raising=False)
    settings = SimpleNamespace(alpaca_key=object(), alpaca_secret=object())

    result = asyncio.run(
        loop.prepare_prior_daily_once(
            settings,
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "prepared"
    assert [call[0] for call in calls] == ["inspect", "window", "collect", "import", "inspect"]
    assert calls[1] == ("window", instrument_id, prior_session, prior_session, "all", NOW)
    assert calls[2] == ("collect", "request", prior_session, 10)
    assert calls[3] == ("import", instrument_id, "agent-paper-decision")


def test_prior_daily_preparation_reuses_existing_authority_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    async def inspect(_settings, *, account_id, now):
        return loop.PriorDailyScope(
            instrument_id=UUID(int=20),
            prior_session=date(2026, 9, 11),
            authority_ready=True,
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("existing authority must not call a provider or importer")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect)
    monkeypatch.setattr(loop, "collect_prices", unexpected)
    monkeypatch.setattr(loop, "import_adjusted_authority", unexpected)

    result = asyncio.run(
        loop.prepare_prior_daily_once(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "ready"
    assert not (tmp_path / ".secrets").exists()


def test_prior_daily_preparation_does_not_repeat_an_existing_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prior_session = date(2026, 9, 11)
    attempt = tmp_path / ".secrets" / "nexus-agent-paper" / "daily" / prior_session.isoformat()
    attempt.mkdir(mode=0o700, parents=True)

    async def inspect(_settings, *, account_id, now):
        return loop.PriorDailyScope(
            instrument_id=UUID(int=20),
            prior_session=prior_session,
            authority_ready=False,
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("an existing attempt directory must prevent a duplicate call")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect)
    monkeypatch.setattr(loop, "collect_prices", unexpected)
    monkeypatch.setattr(loop, "import_adjusted_authority", unexpected)

    result = asyncio.run(
        loop.prepare_prior_daily_once(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"


def test_prior_daily_preparation_stops_after_known_collection_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prior_session = date(2026, 9, 11)

    async def inspect(_settings, *, account_id, now):
        return loop.PriorDailyScope(
            instrument_id=UUID(int=20),
            prior_session=prior_session,
            authority_ready=False,
        )

    async def collect(*_args, **_kwargs):
        return {"status": "failed", "code": "ALPACA_PRICES_STATUS_REJECTED"}

    async def unexpected_import(*_args, **_kwargs):
        raise AssertionError("a failed collection must not be imported")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *_args, **_kwargs: ("request", {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_adjusted_authority", unexpected_import)

    result = asyncio.run(
        loop.prepare_prior_daily_once(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"


def test_macro_preparation_reuses_complete_visible_vintage_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    async def inspect(_settings, *, account_id, now):
        assert account_id == UUID(int=1)
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            vintage=date(2026, 9, 13),
            ready_series=frozenset({"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "VIXCLS"}),
            snapshot_ready=True,
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("complete visible macro evidence must not call a provider or importer")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collect_fred", unexpected, raising=False)
    monkeypatch.setattr(loop, "import_fred_collection", unexpected, raising=False)

    result = asyncio.run(
        loop.prepare_macro_once(
            SimpleNamespace(),
            fred_key=object(),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "ready"
    assert not (tmp_path / ".secrets").exists()


def test_macro_snapshot_is_not_ready_without_bound_calendar_evidence() -> None:
    observations = tuple(
        SimpleNamespace(
            series=series,
            evidence_ref_id=UUID(int=index),
            calendar_evidence_ref_id=UUID(int=index + 100),
        )
        for index, series in enumerate(sorted(loop._PAPER_MACRO_SERIES), start=1)
    )
    observation_evidence_ids = {observation.evidence_ref_id for observation in observations}

    assert not loop._macro_snapshot_contains_required_observations(
        observations,
        bound_ids=observation_evidence_ids,
    )


def test_macro_preparation_binds_complete_evidence_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    all_series = frozenset({"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "VIXCLS"})
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=UUID(int=20),
                vintage=date(2026, 9, 13),
                ready_series=all_series,
                snapshot_ready=False,
            ),
            SimpleNamespace(
                instrument_id=UUID(int=20),
                vintage=date(2026, 9, 13),
                ready_series=all_series,
                snapshot_ready=True,
            ),
        )
    )
    snapshots: list[UUID] = []

    async def inspect(_settings, *, account_id, now):
        return next(scopes)

    async def prepare_snapshot(_settings, *, instrument_id, now, macro_vintage):
        snapshots.append(instrument_id)
        assert macro_vintage == date(2026, 9, 13)

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("persisted macro evidence must not call the provider")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "_prepare_current_trends_research_snapshot", prepare_snapshot)
    monkeypatch.setattr(loop, "collect_fred", unexpected)

    result = asyncio.run(
        loop.prepare_macro_once(
            SimpleNamespace(),
            fred_key=object(),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "prepared"
    assert snapshots == [UUID(int=20)]
    assert not (tmp_path / ".secrets").exists()


def test_macro_preparation_rejects_missing_archive_config_before_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            vintage=date(2026, 9, 13),
            ready_series=frozenset(),
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("invalid archive config must not call FRED")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "collect_fred", unexpected)

    result = asyncio.run(
        loop.prepare_macro_once(
            loop.DailyOnceSettings(database_url="postgresql+asyncpg://test:test@127.0.0.1/test"),
            fred_key=loop.SecretStr("test-key"),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"
    assert not (tmp_path / ".secrets").exists()


@pytest.mark.parametrize(
    ("decision_time", "expected_vintage"),
    [
        (datetime(2026, 9, 16, 11, 0, tzinfo=UTC), date(2026, 9, 14)),
        (datetime(2026, 9, 16, 14, 0, tzinfo=UTC), date(2026, 9, 15)),
    ],
)
def test_macro_vintage_advances_only_after_its_next_xnys_open(
    decision_time: datetime, expected_vintage: date
) -> None:
    assert loop.latest_visible_macro_vintage(decision_time) == expected_vintage


def test_macro_preparation_collects_imports_rebuilds_and_reads_back_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    instrument_id = UUID(int=20)
    vintage = date(2026, 9, 13)
    all_series = frozenset({"DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "VIXCLS"})
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=instrument_id,
                vintage=vintage,
                ready_series=frozenset(),
            ),
            SimpleNamespace(
                instrument_id=instrument_id,
                vintage=vintage,
                ready_series=all_series,
                snapshot_ready=True,
            ),
        )
    )
    requests: list[object] = []
    imported: list[str] = []
    snapshots: list[UUID] = []

    async def inspect(_settings, *, account_id, now):
        assert account_id == UUID(int=1)
        return next(scopes)

    async def collect(request, *, key, directory):
        assert directory.name == request.series
        requests.append(request)
        return {"status": "collected", "observation_count": 1}

    def load(directory):
        return SimpleNamespace(series=directory.name)

    async def import_collection(_settings, *, collection):
        imported.append(collection.series)
        return {"status": "imported", "visible_observation_count": 1}

    async def prepare_snapshot(_settings, *, instrument_id, now, macro_vintage):
        snapshots.append(instrument_id)
        assert macro_vintage == vintage

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collect_fred", collect, raising=False)
    monkeypatch.setattr(loop, "load_fred_collection", load, raising=False)
    monkeypatch.setattr(loop, "import_fred_collection", import_collection, raising=False)
    monkeypatch.setattr(loop, "validate_local_import", lambda _settings: None)
    monkeypatch.setattr(loop, "_prepare_current_trends_research_snapshot", prepare_snapshot)

    result = asyncio.run(
        loop.prepare_macro_once(
            SimpleNamespace(),
            fred_key=object(),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "prepared"
    assert {request.series for request in requests} == all_series
    daily = [request for request in requests if request.series not in {"CPIAUCSL", "UNRATE"}]
    monthly = [request for request in requests if request.series in {"CPIAUCSL", "UNRATE"}]
    assert all(
        request.observation_start == date(2026, 8, 13)
        and request.observation_end == vintage
        and request.vintage == vintage
        for request in daily
    )
    assert all(
        request.observation_start == date(2026, 8, 1)
        and request.observation_end == date(2026, 8, 31)
        and request.vintage == vintage
        for request in monthly
    )
    assert set(imported) == all_series
    assert snapshots == [instrument_id]


def test_macro_preparation_attempts_provider_only_once_per_trading_day(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=UUID(int=20),
                vintage=date(2026, 9, 12),
                ready_series=frozenset(),
            ),
            SimpleNamespace(
                instrument_id=UUID(int=20),
                vintage=date(2026, 9, 13),
                ready_series=frozenset(),
            ),
        )
    )
    attempts: list[str] = []

    async def inspect(_settings, *, account_id, now):
        return next(scopes)

    async def collect(request, *, key, directory):
        attempts.append(request.series)
        raise ValueError("known provider failure")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "validate_local_import", lambda _settings: None)
    monkeypatch.setattr(loop, "collect_fred", collect)

    for _ in range(2):
        result = asyncio.run(
            loop.prepare_macro_once(
                SimpleNamespace(),
                fred_key=object(),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
            )
        )
        assert result == "unavailable"

    assert len(attempts) == 1


def test_macro_preparation_degrades_only_known_provider_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            vintage=date(2026, 9, 13),
            ready_series=frozenset(),
        )

    async def collect(request, *, key, directory):
        raise HttpPolicyError("RETRY_BUDGET_EXHAUSTED")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "validate_local_import", lambda _settings: None)
    monkeypatch.setattr(loop, "collect_fred", collect)

    result = asyncio.run(
        loop.prepare_macro_once(
            SimpleNamespace(),
            fred_key=object(),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"


def test_macro_preparation_propagates_unknown_provider_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            vintage=date(2026, 9, 13),
            ready_series=frozenset(),
        )

    async def collect(request, *, key, directory):
        raise RuntimeError("unknown provider result")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "validate_local_import", lambda _settings: None)
    monkeypatch.setattr(loop, "collect_fred", collect)

    with pytest.raises(RuntimeError, match="unknown provider result"):
        asyncio.run(
            loop.prepare_macro_once(
                SimpleNamespace(),
                fred_key=object(),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
            )
        )


@pytest.mark.parametrize(
    "imported",
    (
        {"status": "rejected", "visible_observation_count": 0},
        {"status": "imported", "visible_observation_count": 0},
    ),
)
def test_macro_preparation_fails_closed_on_import_or_readback_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: loop.Path,
    imported: dict[str, object],
) -> None:
    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            vintage=date(2026, 9, 13),
            ready_series=frozenset(),
        )

    async def collect(request, *, key, directory):
        return {"status": "collected", "observation_count": 1}

    async def import_collection(_settings, *, collection):
        return imported

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "validate_local_import", lambda _settings: None)
    monkeypatch.setattr(loop, "collect_fred", collect)
    monkeypatch.setattr(loop, "load_fred_collection", lambda directory: object())
    monkeypatch.setattr(loop, "import_fred_collection", import_collection)

    with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
        asyncio.run(
            loop.prepare_macro_once(
                SimpleNamespace(),
                fred_key=object(),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
            )
        )


def test_current_trends_preparation_collects_imports_and_reads_back_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prepare = getattr(loop, "prepare_current_trends_once", None)
    assert prepare is not None, "the loop must prepare current trends before triggering research"
    instrument_id = UUID(int=20)
    start_session = date(2025, 9, 12)
    prior_session = date(2026, 9, 11)
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=instrument_id,
                start_session=start_session,
                prior_session=prior_session,
                evidence_ready=False,
                snapshot_ready=False,
            ),
            SimpleNamespace(
                instrument_id=instrument_id,
                start_session=start_session,
                prior_session=prior_session,
                evidence_ready=True,
                snapshot_ready=True,
            ),
        )
    )
    calls: list[tuple[object, ...]] = []

    async def inspect(_settings, *, account_id, now):
        calls.append(("inspect", account_id, now))
        return next(scopes)

    def window(received_instrument_id, start, end, adjustment, *, now):
        calls.append(("window", received_instrument_id, start, end, adjustment, now))
        return "request", {"session": prior_session.isoformat()}

    async def collect(request, **kwargs):
        calls.append(("collect", request, kwargs["identity_asof"], kwargs["max_pages"]))
        return {"status": "collected", "readback_verified": True}

    async def import_context(_settings, **kwargs):
        calls.append(("import", kwargs["instrument_id"], kwargs["collection_dir"]))
        return {
            "status": "stored_context",
            "last_observed_on": prior_session.isoformat(),
        }

    async def prepare_snapshot(_settings, *, instrument_id, now):
        calls.append(("snapshot", instrument_id, now))

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collection_window", window)
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_trends", import_context, raising=False)
    monkeypatch.setattr(
        loop, "_prepare_current_trends_research_snapshot", prepare_snapshot, raising=False
    )
    settings = SimpleNamespace(alpaca_key=object(), alpaca_secret=object())

    result = asyncio.run(
        prepare(
            settings,
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "prepared"
    assert [call[0] for call in calls] == [
        "inspect",
        "window",
        "collect",
        "import",
        "snapshot",
        "inspect",
    ]
    assert calls[1] == (
        "window",
        instrument_id,
        start_session,
        prior_session,
        "split",
        NOW,
    )
    assert calls[2] == ("collect", "request", prior_session, 10)


def test_current_trends_window_uses_a_full_year_not_the_short_execution_calendar() -> None:
    start_session = getattr(loop, "_current_trends_start_session", None)
    assert start_session is not None, "current trends need their own full-year calendar window"

    assert start_session(date(2026, 9, 14)) == date(2025, 9, 15)


def test_current_trends_preparation_reuses_current_evidence_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prepare = getattr(loop, "prepare_current_trends_once", None)
    assert prepare is not None, "the loop must prepare current trends before triggering research"

    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            start_session=date(2025, 9, 12),
            prior_session=date(2026, 9, 11),
            evidence_ready=True,
            snapshot_ready=True,
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("current evidence must not call a provider or importer")

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collect_prices", unexpected)
    monkeypatch.setattr(loop, "import_trends", unexpected, raising=False)

    result = asyncio.run(
        prepare(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "ready"
    assert not (tmp_path / ".secrets").exists()


def test_current_trends_preparation_builds_missing_snapshot_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    instrument_id = UUID(int=20)
    prior_session = date(2026, 9, 11)
    scopes = iter(
        (
            SimpleNamespace(
                instrument_id=instrument_id,
                start_session=date(2025, 9, 12),
                prior_session=prior_session,
                evidence_ready=True,
                snapshot_ready=False,
            ),
            SimpleNamespace(
                instrument_id=instrument_id,
                start_session=date(2025, 9, 12),
                prior_session=prior_session,
                evidence_ready=True,
                snapshot_ready=True,
            ),
        )
    )
    calls: list[tuple[object, ...]] = []

    async def inspect(_settings, *, account_id, now):
        calls.append(("inspect", account_id, now))
        return next(scopes)

    async def prepare_snapshot(_settings, *, instrument_id, now):
        calls.append(("snapshot", instrument_id, now))

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("persisted trend evidence must not be collected or imported again")

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect)
    monkeypatch.setattr(loop, "_prepare_current_trends_research_snapshot", prepare_snapshot)
    monkeypatch.setattr(loop, "collect_prices", unexpected)
    monkeypatch.setattr(loop, "import_trends", unexpected)

    result = asyncio.run(
        loop.prepare_current_trends_once(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "prepared"
    assert [call[0] for call in calls] == ["inspect", "snapshot", "inspect"]
    assert not (tmp_path / ".secrets").exists()


def test_current_trends_preparation_does_not_repeat_an_existing_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prepare = getattr(loop, "prepare_current_trends_once", None)
    assert prepare is not None, "the loop must prepare current trends before triggering research"
    prior_session = date(2026, 9, 11)
    attempt = tmp_path / ".secrets" / "nexus-agent-paper" / "trends" / prior_session.isoformat()
    attempt.mkdir(mode=0o700, parents=True)

    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            start_session=date(2025, 9, 12),
            prior_session=prior_session,
            evidence_ready=False,
            snapshot_ready=False,
        )

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("an existing attempt directory must prevent a duplicate call")

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect, raising=False)
    monkeypatch.setattr(loop, "collect_prices", unexpected)
    monkeypatch.setattr(loop, "import_trends", unexpected, raising=False)

    result = asyncio.run(
        prepare(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"


def test_current_trends_preparation_stops_after_known_collection_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: loop.Path
) -> None:
    prior_session = date(2026, 9, 11)

    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            start_session=date(2025, 9, 12),
            prior_session=prior_session,
            evidence_ready=False,
            snapshot_ready=False,
        )

    async def collect(*_args, **_kwargs):
        return {"status": "failed", "code": "ALPACA_PRICES_STATUS_REJECTED"}

    async def unexpected_import(*_args, **_kwargs):
        raise AssertionError("a failed collection must not be imported")

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *_args, **_kwargs: ("request", {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_trends", unexpected_import)

    result = asyncio.run(
        loop.prepare_current_trends_once(
            SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
            account_id=UUID(int=1),
            repo_root=tmp_path,
            now=NOW,
        )
    )

    assert result == "unavailable"


@pytest.mark.parametrize("failure_boundary", ["collector", "importer"])
def test_current_trends_preparation_preserves_unknown_external_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: loop.Path,
    failure_boundary: str,
) -> None:
    prior_session = date(2026, 9, 11)

    async def inspect(_settings, *, account_id, now):
        return SimpleNamespace(
            instrument_id=UUID(int=20),
            start_session=date(2025, 9, 12),
            prior_session=prior_session,
            evidence_ready=False,
            snapshot_ready=False,
        )

    async def collect(*_args, **_kwargs):
        if failure_boundary == "collector":
            raise ValueError("collection outcome is unknown")
        return {"status": "collected", "readback_verified": True}

    async def import_context(*_args, **_kwargs):
        if failure_boundary == "importer":
            raise ValueError("import outcome is unknown")
        raise AssertionError("the importer must not run after a collector exception")

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *_args, **_kwargs: ("request", {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_trends", import_context)

    with pytest.raises(ValueError, match="outcome is unknown"):
        asyncio.run(
            loop.prepare_current_trends_once(
                SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
            )
        )


@pytest.mark.parametrize(
    ("published", "expected"),
    [(True, None), (False, A.STOP_PERFORMANCE_UNAVAILABLE)],
)
def test_performance_executor_publishes_exact_due_close_once(
    monkeypatch: pytest.MonkeyPatch,
    published: bool,
    expected: loop.AgentPaperLoopAction | None,
) -> None:
    close_at = datetime(2026, 9, 11, 20, 0, tzinfo=UTC)
    received: list[tuple[UUID, datetime]] = []

    async def publish(_settings, *, account_id, as_of):
        received.append((account_id, as_of))
        return published

    monkeypatch.setattr(loop, "publish_due_performance_once", publish)
    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=A.PUBLISH_PERFORMANCE,
            snapshot=snapshot(performance_due_at=close_at),
        )
    )

    assert result == expected
    assert received == [(UUID(int=1), close_at)]


@pytest.mark.parametrize(
    ("paper_action", "expected"),
    [
        ("no_data", A.STOP_ORDER_UNAVAILABLE),
        ("no_capacity", A.STOP_ORDER_UNAVAILABLE),
        ("filled", None),
    ],
)
def test_order_executor_surfaces_only_known_no_trade_stop(
    monkeypatch: pytest.MonkeyPatch,
    paper_action: str,
    expected: loop.AgentPaperLoopAction | None,
) -> None:
    async def advance(_settings, request):
        return {
            "status": "submitted",
            "action": paper_action,
            "order_id": str(request.order_id),
        }

    monkeypatch.setattr(loop, "run_paper_once", advance)
    result = asyncio.run(
        loop.execute_agent_paper_loop_action(
            loop.EngineSettings(),
            account_id=UUID(int=1),
            repo_root=loop.Path("/repo"),
            action=loop.AgentPaperLoopAction.ADVANCE_ORDER,
            snapshot=snapshot(
                cycle_id=UUID(int=2),
                cycle_status="awaiting_market",
                market_window_ready=True,
                has_open_order=True,
                order_id=UUID(int=3),
            ),
        )
    )

    assert result == expected


def test_cycle_view_accepts_real_cycle_bound_scope_and_delays_collection() -> None:
    order_id, window_end = UUID(int=3), datetime(2026, 9, 14, 14, 12, tzinfo=UTC)
    cycle_id = UUID(int=2)
    view = SimpleNamespace(
        cycle_id=cycle_id,
        status="awaiting_market",
        decision_slot_at=SLOT,
        order_ids=(order_id,),
        execution_market=SimpleNamespace(status="pending"),
    )
    market_scope = AgentPaperMarketScope(
        cycle_id=cycle_id,
        instrument_id=UUID(int=4),
        execution_source_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
        window_start=window_end - timedelta(minutes=5),
        window_end=window_end,
    )
    result = loop.snapshot_from_cycle_view(
        now=NOW,
        view=view,
        research_job=None,
        market_scope=market_scope,
    )
    assert result.order_id == order_id
    assert result.market_window_end_at == window_end + timedelta(seconds=60)
    assert result.has_open_order is True
    with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_RESULT_UNKNOWN"):
        loop.snapshot_from_cycle_view(
            now=NOW,
            view=view,
            research_job=None,
            market_scope=market_scope.model_copy(update={"cycle_id": UUID(int=5)}),
        )
