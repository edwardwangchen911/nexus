"""A recovery token grants one checked activation, never a business replay."""

import hashlib
import importlib
import json
import shutil
from pathlib import Path
from threading import Event, Thread
from uuid import uuid4

import pytest
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.settings import EngineSettings


@pytest.fixture(autouse=True)
def isolated_boot(monkeypatch):
    import nexus.ops.agent_paper_recovery_activation as activation

    monkeypatch.setattr(
        activation, "_boot_id", lambda: "11111111-1111-1111-1111-111111111111", raising=False
    )


@pytest.fixture(autouse=True)
def isolated_service_control(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    source = Path(__file__).resolve().parents[4] / "scripts/macos/agent-paper-loop-service.py"
    target = tmp_path / "scripts/macos/agent-paper-loop-service.py"
    target.parent.mkdir(parents=True)
    shutil.copyfile(source, target)
    spec = importlib.util.spec_from_file_location("isolated_service_control", target)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def state(tmp_path):
    env = tmp_path / "service.env"
    env.write_text("local-fixture\n")
    directory = tmp_path / ".secrets/nexus-agent-paper"
    directory.mkdir(parents=True, mode=0o700)
    generation, attempt = str(uuid4()), str(uuid4())
    value = {
        "version": 1,
        "desired": "enabled",
        "root": str(tmp_path),
        "env_file": str(env),
        "revision": "a" * 40,
        "schema": CURRENT_MIGRATION_HEAD,
        "config_hash": hashlib.sha256(env.read_bytes()).hexdigest(),
        "generation": generation,
        "attempt": {
            "id": attempt,
            "generation": generation,
            "boot": "11111111-1111-1111-1111-111111111111",
        },
    }
    target = directory / "recovery.json"
    target.write_text(json.dumps(value))
    target.chmod(0o600)
    return env, generation, attempt, target


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "disabled",
        "generation",
        "config",
        "scope",
        "late_unknown",
        "late_process",
        "late_process_during_check",
        "boot",
        "schema",
        "disabled_during_check",
        "none",
    ],
)
async def test_activation_rechecks_identity_and_late_occupancy_before_business(
    tmp_path, monkeypatch, change
):
    activation = importlib.import_module("nexus.ops.agent_paper_recovery_activation")
    readiness = importlib.import_module("nexus.ops.agent_paper_recovery")
    env, generation, attempt, target = state(tmp_path)
    settings = EngineSettings(release_revision="a" * 40)
    inspected = []

    def quiescent():
        if change == "late_process" or (change == "late_process_during_check" and inspected):
            raise RuntimeError("private-process-details")

    monkeypatch.setattr(readiness, "require_recovery_process_quiescence", quiescent)

    async def inspect(effective):
        inspected.append(effective)
        if change == "disabled_during_check":
            data = json.loads(target.read_text())
            data["desired"] = "disabled"
            target.write_text(json.dumps(data))
        return {
            "status": "blocked" if change == "late_unknown" else "ready",
            "blocking_codes": ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
            if change == "late_unknown"
            else [],
        }

    monkeypatch.setattr(readiness, "inspect_recovery_readiness", inspect)
    guard = activation.make_recovery_guard(
        settings, repo_root=tmp_path, env_file=env, generation=generation, attempt_id=attempt
    )
    if change in {"disabled", "generation"}:
        data = json.loads(target.read_text())
        data["desired" if change == "disabled" else "generation"] = (
            "disabled" if change == "disabled" else str(uuid4())
        )
        target.write_text(json.dumps(data))
    elif change in {"boot", "schema"}:
        data = json.loads(target.read_text())
        if change == "boot":
            data["attempt"]["boot"] = str(uuid4())
        else:
            data["schema"] = "old-schema"
        target.write_text(json.dumps(data))
    elif change == "config":
        env.write_text("changed\n")
    effective = (
        settings.model_copy(update={"agent_paper_account_id": uuid4()})
        if change == "scope"
        else settings
    )
    if change == "none":
        await guard(effective)
        assert len(inspected) == 1
        with pytest.raises(ValueError, match="RECOVERY"):
            await guard(effective)
    else:
        with pytest.raises(ValueError, match="RECOVERY"):
            await guard(effective)
        assert len(inspected) == (
            1
            if change in {"late_unknown", "disabled_during_check", "late_process_during_check"}
            else 0
        )
        if change == "late_process":
            receipt = json.loads(guard.receipt_path.read_text())
            assert receipt["phase"] == "rejected"
            assert "private-process-details" not in json.dumps(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["disconnected", "integrity", "programming", "other"])
async def test_only_confirmed_database_disconnect_receives_recoverable_exit(
    tmp_path, monkeypatch, kind
):
    from nexus.ops import agent_paper_recovery as readiness
    from nexus.ops.agent_paper_recovery_activation import make_recovery_guard
    from sqlalchemy.exc import DBAPIError, IntegrityError, ProgrammingError, SQLAlchemyError

    env, generation, attempt, _ = state(tmp_path)
    settings = EngineSettings(release_revision="a" * 40)

    async def ready(_):
        return {"status": "ready"}

    monkeypatch.setattr(readiness, "inspect_recovery_readiness", ready)
    monkeypatch.setattr(readiness, "require_recovery_process_quiescence", lambda: None)
    guard = make_recovery_guard(
        settings, repo_root=tmp_path, env_file=env, generation=generation, attempt_id=attempt
    )
    await guard(settings)
    errors = {
        "disconnected": DBAPIError(None, None, Exception("private"), connection_invalidated=True),
        "integrity": IntegrityError(None, None, Exception("private")),
        "programming": ProgrammingError(None, None, Exception("private")),
        "other": SQLAlchemyError("private"),
    }
    guard.finish_database_error(errors[kind])
    receipt = json.loads(guard.receipt_path.read_text())
    assert receipt["phase"] == "exited"
    assert receipt["reason"] == (
        "database_unavailable" if kind == "disconnected" else "unclassified"
    )
    assert "private" not in json.dumps(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order", ["disable_first", "disable_after_check", "activation_first", "contended"]
)
async def test_activation_commit_serializes_with_disable(
    tmp_path, monkeypatch, isolated_service_control, order
):
    from nexus.ops import agent_paper_recovery as readiness
    from nexus.ops.agent_paper_recovery_activation import make_recovery_guard

    env, generation, attempt, target = state(tmp_path)
    settings = EngineSettings(release_revision="a" * 40)
    guard = make_recovery_guard(
        settings, repo_root=tmp_path, env_file=env, generation=generation, attempt_id=attempt
    )
    manager = isolated_service_control
    attempted, complete = Event(), Event()
    changes = []

    def disable():
        attempted.wait(5)
        try:
            with manager._service_control_lock():
                data = json.loads(target.read_text())
                data.update(desired="disabled", generation=str(uuid4()))
                target.write_text(json.dumps(data))
                changes.append("disabled")
        except RuntimeError:
            changes.append("busy")
        finally:
            complete.set()

    worker = Thread(target=disable)
    worker.start()

    async def ready(_):
        if order == "disable_first":
            attempted.set()
            assert complete.wait(5)
        return {"status": "ready"}

    checks = []

    def quiescent():
        checks.append("checked")
        if order == "disable_after_check" and len(checks) == 2:
            attempted.set()
            assert complete.wait(5)

    monkeypatch.setattr(readiness, "inspect_recovery_readiness", ready)
    monkeypatch.setattr(readiness, "require_recovery_process_quiescence", quiescent)
    original_save = guard._save

    def save(phase, reason):
        if phase == "activated":
            attempted.set()
            assert complete.wait(5)
        original_save(phase, reason)

    monkeypatch.setattr(guard, "_save", save)
    try:
        if order == "contended":
            with manager._service_control_lock():
                with pytest.raises(ValueError, match="ACTIVATION_BLOCKED"):
                    await guard(settings)
        elif order in {"disable_first", "disable_after_check"}:
            with pytest.raises(ValueError, match="ACTIVATION_BLOCKED"):
                await guard(settings)
        else:
            await guard(settings)
            assert changes == ["busy"]
            assert json.loads(guard.receipt_path.read_text())["phase"] == "activated"
        if order != "activation_first":
            assert json.loads(guard.receipt_path.read_text())["phase"] != "activated"
    finally:
        attempted.set()
        worker.join(5)
        assert not worker.is_alive()


def test_main_preserves_database_exception_when_exit_receipt_fails(tmp_path, monkeypatch, capsys):
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_recovery_activation as activation
    from sqlalchemy.exc import SQLAlchemyError

    env, generation, attempt, _ = state(tmp_path)
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").write_text("fixture")
    monkeypatch.setattr(loop, "_validate_local_env", lambda *_: env)
    monkeypatch.setattr(
        loop,
        "load_agent_paper_service_environment",
        lambda *_a, **_k: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(uuid4()),
            "AGENT_RUNTIME_MODE": "codex_sdk",
        },
    )
    guard = activation.make_recovery_guard(
        EngineSettings(),
        repo_root=tmp_path,
        env_file=env,
        generation=generation,
        attempt_id=attempt,
    )
    guard.receipt = {"phase": "activated"}
    original = SQLAlchemyError("private-database-message")
    observed = []

    def fail_save(*_):
        raise OSError("private-filesystem-message")

    monkeypatch.setattr(guard, "_save", fail_save)
    monkeypatch.setattr(activation, "make_recovery_guard", lambda *_a, **_k: guard)

    async def fail_loop(*_a, **_k):
        raise original

    monkeypatch.setattr(loop, "run_local_agent_paper_loop", fail_loop)

    # str() is called by main's final safe-error projection: retain object identity.
    class ObservedError(SQLAlchemyError):
        def __str__(self):
            observed.append(self)
            return super().__str__()

    original = ObservedError("private-database-message")
    assert (
        loop.main(
            [
                "--repo-root",
                str(tmp_path),
                "--env-file",
                str(env),
                "--recovery-generation",
                generation,
                "--recovery-attempt",
                attempt,
            ]
        )
        == 1
    )
    assert observed == [original]
    assert original.__notes__ == ["AGENT_PAPER_RECOVERY_EXIT_RECEIPT_WRITE_FAILED"]
    output = capsys.readouterr().out
    assert "private" not in output
    assert json.loads(output) == {"error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN"}
    assert guard.receipt == {"phase": "activated"}
    with pytest.raises(OSError, match="private-filesystem-message"):
        guard.finish("stop_requested")
