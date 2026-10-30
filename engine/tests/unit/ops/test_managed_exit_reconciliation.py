"""Lifecycle reconciliation holds real isolated locks and never starts business work."""

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import agent_paper_recovery as recovery
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.settings import EngineSettings


def settings():
    return EngineSettings(
        database_url="postgresql+asyncpg://local@127.0.0.1/test",
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
        release_revision="a" * 40,
    )


@pytest.fixture
def boundaries(monkeypatch, tmp_path):
    from nexus.ops import managed_exit_reconciliation as adapter
    from nexus.paper.manual_entry import manual_host_scope

    source = Path(__file__).resolve().parents[4] / "scripts/macos/agent-paper-loop-service.py"
    spec = importlib.util.spec_from_file_location("managed_exit_test_service", source)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    events = []
    config = settings()
    ready = {
        "status": "ready",
        "migration_head": CURRENT_MIGRATION_HEAD,
        "scope_hash": manual_host_scope(config),
        "execution_locked": True,
        "external_requests": 0,
        "checked_at": "2026-10-27T14:00:00+00:00",
        "blocking_codes": [],
    }

    def held():
        with pytest.raises(RuntimeError, match="AGENT_PAPER_SERVICE_CONTROL_BUSY"):
            with service._service_control_lock():
                pytest.fail("manager lock was not held")
        with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_ALREADY_RUNNING"):
            with loop.acquire_agent_paper_lock():
                pytest.fail("lifetime lock was not held")

    def quiet():
        held()
        events.append("quiescence")

    def capture(*args):
        held()
        events.append("capture")
        return {"opaque": "old immutable identity"}

    async def inspect(effective):
        held()
        assert effective is config
        events.append("canonical")
        return ready

    def publish(*args, **kwargs):
        held()
        events.append("publish")
        assert kwargs["capture"] == {"opaque": "old immutable identity"}
        assert kwargs["readiness"] is ready
        return {
            "status": "eligible_at_check" if kwargs["check_only"] else "reconciled",
            "proof_id": str(UUID(int=7)),
            "proof_hash": "b" * 64,
            "private": "must-not-leak",
        }

    helper = SimpleNamespace(capture_exit=capture, publish_exit=publish, SAFE_CODES=frozenset())
    monkeypatch.setattr(service, "_recovery_module", lambda: object())
    monkeypatch.setattr(service, "_reconciliation_module", lambda: helper, raising=False)
    monkeypatch.setattr(adapter, "_load_service", lambda _: service)
    monkeypatch.setattr(recovery, "require_recovery_process_quiescence", quiet)
    monkeypatch.setattr(recovery, "inspect_recovery_readiness", inspect)
    return adapter, service, helper, config, ready, events


@pytest.mark.parametrize("check_only", [True, False])
def test_reconciliation_keeps_both_locks_through_capture_canonical_check_and_publish(
    boundaries, tmp_path, check_only
):
    adapter, _, _, config, _, events = boundaries
    result = adapter.reconcile_managed_exit(
        config,
        repo_root=tmp_path,
        env_file=tmp_path / "env",
        project="nexus",
        check_only=check_only,
    )
    assert events == ["quiescence", "capture", "canonical", "quiescence", "publish"]
    assert result["status"] == ("eligible_at_check" if check_only else "reconciled")
    assert result["execution_locked"] is True and result["external_requests"] == 0
    assert "private" not in result


@pytest.mark.parametrize(
    "fault",
    ["blocked", "db_error", "schema", "scope", "first_quiet", "second_quiet", "capture", "publish"],
)
def test_failure_never_publishes_or_exposes_raw_errors(boundaries, monkeypatch, tmp_path, fault):
    adapter, _, helper, config, ready, events = boundaries
    if fault == "blocked":
        ready.update(status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    elif fault == "schema":
        ready["migration_head"] = "old-schema"
    elif fault == "scope":
        ready["scope_hash"] = "c" * 64
    elif fault == "db_error":

        async def fail(_):
            events.append("db_error")
            raise RuntimeError("private-canary")

        monkeypatch.setattr(recovery, "inspect_recovery_readiness", fail)
    elif fault in {"first_quiet", "second_quiet"}:
        original = recovery.require_recovery_process_quiescence
        calls = []

        def quiet():
            original()
            calls.append(True)
            if len(calls) == (1 if fault == "first_quiet" else 2):
                raise RuntimeError("private-canary")

        monkeypatch.setattr(recovery, "require_recovery_process_quiescence", quiet)
    else:

        def fail(*a, **kw):
            raise ValueError("private-canary")

        monkeypatch.setattr(helper, "capture_exit" if fault == "capture" else "publish_exit", fail)
    result = adapter.reconcile_managed_exit(
        config, repo_root=tmp_path, env_file=tmp_path / "env", project="nexus", check_only=False
    )
    assert result["status"] == "blocked" and result["current_eligible"] is False
    assert "publish" not in events
    assert "private" not in str(result)
    if fault == "blocked":
        assert result["blocking_codes"] == ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]


@pytest.mark.parametrize("which", ["control", "lifetime"])
def test_contended_real_locks_refuse_without_capture_or_database(boundaries, tmp_path, which):
    adapter, service, _, config, _, events = boundaries
    lock = service._service_control_lock if which == "control" else loop.acquire_agent_paper_lock
    with lock():
        result = adapter.reconcile_managed_exit(
            config, repo_root=tmp_path, env_file=tmp_path / "env", project="nexus", check_only=True
        )
    assert result["blocking_codes"] == ["AGENT_PAPER_EXIT_RECONCILIATION_LOCK_BUSY"]
    assert events == []


@pytest.mark.parametrize("commit_fails", [False, True])
def test_actual_canonical_entry_is_read_only_and_commits_before_publish(
    boundaries, monkeypatch, tmp_path, commit_fails
):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime

    adapter, _, _, config, ready, events = boundaries
    # Restore the real canonical function; only its database boundary and domain
    # readers use isolated doubles. The adapter cannot substitute a weaker check.
    monkeypatch.setattr(recovery, "inspect_recovery_readiness", _CANONICAL)
    at = datetime(2026, 10, 27, 14, tzinfo=UTC)
    values = iter([at, CURRENT_MIGRATION_HEAD, None, None])
    statements = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, statement):
            statements.append(str(statement))

        async def scalar(self, statement):
            return next(values)

        async def scalars(self, statement):
            statements.append(str(statement))
            return ()  # No unresolved research reservations in this ready fixture.

        async def get(self, *args):
            return SimpleNamespace(created_at=at)

        @asynccontextmanager
        async def begin(self):
            yield
            events.append("canonical_commit")
            if commit_fails:
                raise RuntimeError("private-commit-canary")

    class Factory:
        def __call__(self):
            return Session()

        kw = {"bind": SimpleNamespace(dispose=lambda: asyncio.sleep(0))}

    class Read:
        def __init__(self, *args):
            pass

        async def account(self):
            return SimpleNamespace(id=config.agent_paper_account_id), None

    async def batch(*a, **kw):
        return SimpleNamespace(account_id=config.agent_paper_account_id, created_at=at)

    async def admit(*a, **kw):
        assert kw["read_only"] is True and kw["allow_scheduled_completion"] is True
        events.append("canonical_account_admission")

    monkeypatch.setattr(recovery, "session_factory", lambda _: Factory())
    monkeypatch.setattr(recovery, "PaperReadService", Read)
    monkeypatch.setattr(recovery, "load_agent_paper_candidate_batch", batch)
    monkeypatch.setattr(recovery, "require_manual_account_available", admit)
    # Real canonical returns a newly built result, not the fixture's shared dict.
    helper = boundaries[2]

    def publish(*a, **kw):
        assert "canonical_commit" in events
        events.append("publish")
        return {"status": "eligible_at_check"}

    monkeypatch.setattr(helper, "publish_exit", publish)
    result = adapter.reconcile_managed_exit(
        config, repo_root=tmp_path, env_file=tmp_path / "env", project="nexus", check_only=True
    )
    assert "SET TRANSACTION READ ONLY" in statements
    assert "canonical_account_admission" in events
    assert result["status"] == ("blocked" if commit_fails else "eligible_at_check")
    assert ("publish" in events) is not commit_fails


_CANONICAL = recovery.inspect_recovery_readiness


@pytest.mark.parametrize("mode", ["check", "apply"])
def test_cli_uses_protected_config_and_adapter_without_starting_loop(
    monkeypatch, tmp_path, capsys, mode
):
    from nexus.ops import managed_exit_reconciliation as adapter

    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").touch()
    env_file = tmp_path / "infra/.env.test"
    env_file.touch()
    monkeypatch.setattr(loop, "_validate_local_env", lambda *_: env_file)
    environment = {
        "DATABASE_URL": "postgresql+asyncpg://local@127.0.0.1/test",
        "AGENT_RUNTIME_MODE": "codex_sdk",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=1)),
        "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(UUID(int=2)),
        "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=3)),
        "NEXUS_RELEASE_REVISION": "a" * 40,
    }
    monkeypatch.setattr(
        loop, "load_agent_paper_service_environment", lambda *_a, **_kw: environment
    )
    calls = []

    def reconcile(effective, **kwargs):
        assert effective.agent_paper_account_id == UUID(int=1)
        assert kwargs == {
            "repo_root": tmp_path,
            "env_file": env_file,
            "project": "nexus",
            "check_only": mode == "check",
        }
        calls.append(True)
        return {"status": "eligible_at_check" if mode == "check" else "reconciled"}

    monkeypatch.setattr(adapter, "reconcile_managed_exit", reconcile)
    monkeypatch.setattr(
        loop, "run_local_agent_paper_loop", lambda *a, **kw: pytest.fail("started host")
    )
    assert (
        loop.main(
            [
                "--service-exit-reconciliation",
                mode,
                "--service-project-name",
                "nexus",
                "--repo-root",
                str(tmp_path),
                "--env-file",
                str(env_file),
            ]
        )
        == 0
    )
    assert len(calls) == 1
    assert json.loads(capsys.readouterr().out)["status"] in {"eligible_at_check", "reconciled"}


@pytest.mark.parametrize(
    "args,code",
    [
        (
            ["--service-exit-reconciliation", "check"],
            "AGENT_PAPER_EXIT_RECONCILIATION_SCOPE_INVALID",
        ),
        (
            ["--service-exit-reconciliation", "check", "--service-project-name", "nexus"],
            "AGENT_PAPER_SERVICE_CONFIG_REQUIRED",
        ),
        (["--service-project-name", "nexus"], "AGENT_PAPER_EXIT_RECONCILIATION_SCOPE_INVALID"),
        (
            [
                "--service-exit-reconciliation",
                "apply",
                "--service-project-name",
                "nexus",
                "--account-id",
                str(UUID(int=1)),
            ],
            "AGENT_PAPER_EXIT_RECONCILIATION_SCOPE_INVALID",
        ),
        (
            [
                "--service-exit-reconciliation",
                "apply",
                "--service-project-name",
                "nexus",
                "--recovery-generation",
                str(UUID(int=1)),
                "--recovery-attempt",
                str(UUID(int=2)),
            ],
            "AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED",
        ),
    ],
)
def test_cli_rejects_missing_scope_and_activation_combinations(args, code, capsys):
    assert loop.main(args) == 1
    assert json.loads(capsys.readouterr().out)["error_code"] == code


def test_cli_rejects_mixing_diagnostics():
    with pytest.raises(SystemExit) as captured:
        loop.main(["--service-exit-reconciliation", "check", "--service-status"])
    assert captured.value.code == 2


def test_declared_helper_rejection_is_fixed_and_does_not_publish(boundaries, monkeypatch, tmp_path):
    adapter, _, helper, config, _, events = boundaries
    code = "AGENT_PAPER_EXIT_RECONCILIATION_BINDING_CHANGED"
    helper.SAFE_CODES = frozenset({code})

    def reject(*args, **kwargs):
        raise ValueError(code)

    monkeypatch.setattr(helper, "publish_exit", reject)
    result = adapter.reconcile_managed_exit(
        config, repo_root=tmp_path, env_file=tmp_path / "env", project="nexus", check_only=False
    )
    assert result["blocking_codes"] == [code]
    assert "publish" not in events


def test_empty_activation_arguments_cannot_mix_with_reconciliation(capsys):
    assert (
        loop.main(
            [
                "--service-exit-reconciliation",
                "check",
                "--service-project-name",
                "nexus",
                "--recovery-generation",
                "",
                "--recovery-attempt",
                "",
            ]
        )
        == 1
    )
    assert (
        json.loads(capsys.readouterr().out)["error_code"]
        == "AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED"
    )
