"""A recovery token grants one checked activation, never a business replay."""

import hashlib
import importlib
import json
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
