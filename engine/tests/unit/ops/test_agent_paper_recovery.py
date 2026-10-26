"""Recovery activation must reject before any business heartbeat or monitor."""

import asyncio
import json
from contextlib import contextmanager
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop


@pytest.mark.parametrize("protected_binding", [False, True])
def test_recovery_guard_runs_under_process_lock_before_business_activation(
    monkeypatch, tmp_path, protected_binding
):
    events = []

    @contextmanager
    def lock():
        events.append("locked")
        try:
            yield
        finally:
            events.append("unlocked")

    async def reject(effective):
        assert events == ["locked"]
        assert effective.agent_paper_account_id == (UUID(int=2) if protected_binding else None)
        events.append("rejected")
        raise ValueError("AGENT_PAPER_RECOVERY_OCCUPIED")

    async def forbidden(*args, **kwargs):
        pytest.fail("rejected recovery activated business work")

    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lock)
    monkeypatch.setattr(loop, "_select_initial_service_calendar_authority_id", forbidden)
    monkeypatch.setattr(loop, "heartbeat_agent_paper_loop", forbidden)
    monkeypatch.setattr(loop, "run_foreground_loop", forbidden)

    async def bind(*args, **kwargs):
        return {
            "DATABASE_URL": "postgresql+asyncpg://local@127.0.0.1/test",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=2)),
        }, str(UUID(int=3))

    monkeypatch.setattr(loop, "_load_bound_loop_environment", bind)
    with pytest.raises(ValueError, match="^AGENT_PAPER_RECOVERY_OCCUPIED$"):
        asyncio.run(
            loop.run_local_agent_paper_loop(
                loop.EngineSettings(
                    database_url="postgresql+asyncpg://local@127.0.0.1/test",
                    formal_budget_protection_enabled=protected_binding,
                ),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                recovery_guard=reject,
            )
        )
    assert events == ["locked", "rejected", "unlocked"]


@pytest.mark.parametrize("fault", ["none", "process", "config", "process_lock"])
def test_recovery_check_cli_is_explicit_read_only_and_masks_failures(
    monkeypatch, tmp_path, capsys, fault
):
    from nexus.ops import agent_paper_recovery as recovery

    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").touch()
    config = tmp_path / "infra/.env.test"
    config.touch()
    original_lock = loop.acquire_agent_paper_lock
    runtime = tmp_path / "runtime"
    monkeypatch.setattr(
        loop, "acquire_agent_paper_lock", lambda: original_lock(runtime_root=runtime)
    )
    monkeypatch.setattr(loop, "_validate_local_env", lambda *_: config)
    monkeypatch.setattr(
        loop,
        "load_agent_paper_service_environment",
        lambda *_a, **_k: {"NEXUS_AGENT_PAPER_ACCOUNT_ID": "private-config-canary"}
        if fault == "config"
        else {},
    )

    def processes():
        assert fault != "process_lock", "contending recovery must not inspect or activate work"
        if fault == "process":
            raise RuntimeError("private-config-canary")

    async def inspect(settings):
        assert fault == "none"
        return {
            "status": "ready",
            "blocking_codes": [],
            "checked_at": "2026-10-26T03:00:00+00:00",
            "execution_locked": True,
            "external_requests": 0,
        }

    def forbidden(*args, **kwargs):
        pytest.fail("recovery inspection activated the business loop")

    monkeypatch.setattr(recovery, "require_recovery_process_quiescence", processes)
    monkeypatch.setattr(recovery, "inspect_recovery_readiness", inspect)
    monkeypatch.setattr(loop, "run_local_agent_paper_loop", forbidden)
    from contextlib import nullcontext

    with original_lock(runtime_root=runtime) if fault == "process_lock" else nullcontext():
        result = loop.main(
            ["--service-recovery-check", "--repo-root", str(tmp_path), "--env-file", str(config)]
        )
    output = capsys.readouterr().out
    assert "private-config-canary" not in output
    value = json.loads(output)
    assert result == (0 if fault == "none" else 1)
    assert value["status"] == ("ready" if fault == "none" else "blocked")
    assert value["execution_locked"] is True and value["external_requests"] == 0
    if fault != "none":
        assert value["blocking_codes"] == [
            "AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED"
            if fault == "process"
            else "AGENT_PAPER_RECOVERY_HOST_PRESENT"
            if fault == "process_lock"
            else "AGENT_PAPER_RECOVERY_SCOPE_INVALID"
        ]


def test_recovery_check_requires_explicit_protected_configuration(capsys):
    assert loop.main(["--service-recovery-check"]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "AGENT_PAPER_SERVICE_CONFIG_REQUIRED"
    }


def test_recovery_dependency_error_is_blocked_without_exposing_configuration(monkeypatch):
    from nexus.ops import agent_paper_recovery as recovery

    def unavailable(*args, **kwargs):
        raise RuntimeError("private-database-credential-canary")

    monkeypatch.setattr(recovery, "session_factory", unavailable)
    settings = loop.EngineSettings(
        database_url="postgresql+asyncpg://local@127.0.0.1/test",
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
        release_revision="a" * 40,
    )
    result = asyncio.run(recovery.inspect_recovery_readiness(settings))
    assert result["status"] == "blocked"
    assert result["blocking_codes"] == ["AGENT_PAPER_RECOVERY_UNAVAILABLE"]
    assert result["external_requests"] == 0
    assert "canary" not in json.dumps(result)
