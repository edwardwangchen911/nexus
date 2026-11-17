"""Only the reviewed 0125 -> 0133 ordinary exit edge may consume original proof."""

import json
import subprocess
import sys
from uuid import UUID

import pytest
from unit.ops import test_cross_schema_exit_reconciliation as cross
from unit.ops import test_managed_exit_reconciliation_control as lifecycle
from unit.ops.test_agent_paper_host_service import _invoke
from unit.ops.test_agent_paper_recovery_control import register_pid, starts

SOURCE = "0125_sec_positive_chain"
TARGET = "0133_sec_send_scope"
_BASE_HOST = cross._ended_host


def _config(revision="a" * 40):
    from nexus.settings import EngineSettings

    return EngineSettings(
        database_url="postgresql+asyncpg://local@127.0.0.1/test",
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
        release_revision=revision,
    )


def _scoped_host(tmp_path, *, schema):
    from nexus.paper.manual_entry import manual_host_scope

    root, env, state, receipt = _BASE_HOST(tmp_path, schema=schema)
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text().replace(
            "'blocking_codes':options.get('blocking_codes',[])}));",
            "'blocking_codes':options.get('blocking_codes',[]), "
            + "'scope_hash':options.get('scope_hash',"
            + repr(manual_host_scope(_config()))
            + ")}));",
        )
    )
    return root, env, state, receipt


@pytest.fixture(autouse=True)
def approved_versions(monkeypatch):
    monkeypatch.setattr(cross, "_ended_host", _scoped_host)
    monkeypatch.setattr(cross, "SOURCE_SCHEMA", SOURCE)
    monkeypatch.setattr(cross, "TARGET_SCHEMA", TARGET)
    monkeypatch.setattr("nexus.persistence.migrations.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr(lifecycle, "ended_activated_host", cross.cross_schema_host)


def test_durable_edge_check_apply_keeps_originals(tmp_path):
    cross.test_cross_schema_check_and_publication_keep_old_source(tmp_path)


@pytest.mark.parametrize("path", ("consume", "child", "archive"))
def test_durable_edge_complete_one_use_lifecycle(tmp_path, monkeypatch, path):
    cross.test_cross_schema_reuses_complete_lifecycle_assertions(tmp_path, monkeypatch, path)


@pytest.mark.parametrize("case", ("orphan", "revoked", "concurrent"))
def test_durable_edge_interruption_and_shared_locks(tmp_path, monkeypatch, case):
    if case == "orphan":
        _orphan_with_actual_scope(tmp_path, monkeypatch)
    else:
        cross.test_cross_schema_interruption_and_competing_consumers(tmp_path, monkeypatch, case)


@pytest.mark.parametrize("fault", ("code", "receipt", "pending", "unknown", "version", "target"))
def test_durable_edge_late_drift_never_consumes(tmp_path, fault):
    cross.test_cross_schema_late_change_cannot_authorize_start(tmp_path, fault)


@pytest.mark.parametrize(
    "source,target",
    (
        ("0124_sec_recent_positive", TARGET),
        ("0105_comparison_preclaim", TARGET),
        (TARGET, SOURCE),
        (SOURCE, "0132_sec_daily_pending"),
    ),
)
def test_unapproved_reverse_transitive_and_wrong_edges(tmp_path, monkeypatch, source, target):
    monkeypatch.setattr(cross, "SOURCE_SCHEMA", source)
    monkeypatch.setattr(cross, "TARGET_SCHEMA", target)
    root, env, state_path, receipt = cross.cross_schema_host(tmp_path)
    original = state_path.read_bytes(), receipt.read_bytes()
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode != 0 and "SCHEMA_MISMATCH" in result.stdout
    assert (state_path.read_bytes(), receipt.read_bytes()) == original
    assert not list(receipt.parent.glob("recovery-reconciliation-*.json"))
    assert starts(root) == []


@pytest.mark.parametrize("fault", ("enabled", "pending", "exit75", "receipt", "unknown"))
def test_durable_edge_initial_readiness_fails_closed(tmp_path, fault):
    lifecycle.test_unsafe_reconciliation_does_not_publish_or_start(tmp_path, fault)


@pytest.mark.parametrize("fault", ("env", "scope", "active_process"))
def test_durable_edge_bound_identity_drift_never_consumes(tmp_path, fault):
    root, env, state_path, receipt = cross.cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt.parent.glob("recovery-reconciliation-*.json"))
    child = None
    if fault == "env":
        path = root / "infra/.env.test"
        path.write_text(path.read_text() + "# changed exact environment\n")
    elif fault == "scope":
        path = root / "infra/.env.test"
        path.write_text(
            path.read_text() + "NEXUS_AGENT_PAPER_ACCOUNT_ID=00000000-0000-0000-0000-000000000099\n"
        )
    else:
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        register_pid(root, child.pid)
    original = state_path.read_bytes(), receipt.read_bytes(), proof.read_bytes()
    try:
        result = _invoke(root, env, "enable-recovery")
        if fault != "active_process":
            assert result.returncode != 0
            assert state_path.read_bytes() == original[0]
        assert (receipt.read_bytes(), proof.read_bytes()) == original[1:]
        assert json.loads(state_path.read_text())["exit_reconciliation"].get("consumed_by") is None
        assert starts(root) == []
    finally:
        if child is not None:
            child.terminate()
            child.wait(timeout=10)


def test_durable_edge_actual_migration_lineage():
    cross.test_supported_edge_is_in_the_actual_migration_lineage()


def test_unchanged_environment_actual_readiness_scope_drift_blocks_enable(tmp_path):
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state, receipt = cross.cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt.parent.glob("recovery-reconciliation-*.json"))
    original = state.read_bytes(), receipt.read_bytes(), proof.read_bytes()
    env_bytes = (root / "infra/.env.test").read_bytes()
    options(root, scope_hash="b" * 64)
    result = _invoke(root, env, "enable-recovery")
    assert result.returncode != 0
    assert (state.read_bytes(), receipt.read_bytes(), proof.read_bytes()) == original
    assert (root / "infra/.env.test").read_bytes() == env_bytes
    assert starts(root) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_drift", (False, True))
async def test_actual_child_readiness_scope_drift_cannot_activate(
    tmp_path, monkeypatch, scope_drift
):
    from nexus.ops import agent_paper_recovery as readiness
    from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
    from nexus.ops.agent_paper_recovery_activation import RecoveryActivation
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt = cross.cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "enable-recovery").returncode == 0
    state = json.loads(state_path.read_text())
    proof = next(receipt.parent.glob("recovery-reconciliation-*.json"))
    original = state_path.read_bytes(), receipt.read_bytes(), proof.read_bytes()
    cross.local_manager(root, env, monkeypatch)
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation._boot_id", lambda: state["boot"])
    monkeypatch.setattr(readiness, "require_recovery_process_quiescence", lambda: None)

    async def inspect(settings):
        from nexus.paper.manual_entry import manual_host_scope

        return {
            "status": "ready",
            "scope_hash": "b" * 64 if scope_drift else manual_host_scope(settings),
            "migration_head": TARGET,
            "blocking_codes": [],
            "execution_locked": True,
            "external_requests": 0,
        }

    monkeypatch.setattr(readiness, "inspect_recovery_readiness", inspect)
    config = _config(state["revision"])
    guard = RecoveryActivation(
        config,
        repo_root=root,
        env_file=root / "infra/.env.test",
        generation=state["generation"],
        attempt_id=state["attempt"]["id"],
    )
    with acquire_agent_paper_lock(runtime_root=tmp_path / "lifetime"):
        if scope_drift:
            with pytest.raises(ValueError, match="RECOVERY|RECONCILIATION"):
                await guard(config)
        else:
            await guard(config)
    if scope_drift:
        assert (
            not guard.receipt_path.exists()
            or json.loads(guard.receipt_path.read_text())["phase"] != "activated"
        )
    else:
        assert json.loads(guard.receipt_path.read_text())["phase"] == "activated"
    assert (state_path.read_bytes(), receipt.read_bytes(), proof.read_bytes()) == original
    assert len(starts(root)) == 1


def _orphan_with_actual_scope(tmp_path, monkeypatch):
    from nexus.paper.manual_entry import manual_host_scope
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state, receipt = cross.cross_schema_host(tmp_path)
    service, recovery = cross.local_manager(root, env, monkeypatch)
    helper = service._reconciliation_module()
    before = state.read_bytes(), receipt.read_bytes()
    capture = helper.capture_exit(service, recovery, root, root / "infra/.env.test", "nexus-test")
    ready = {
        "status": "ready",
        "blocking_codes": [],
        "execution_locked": True,
        "external_requests": 0,
        "migration_head": TARGET,
        "scope_hash": manual_host_scope(_config()),
        "checked_at": "2026-10-27T14:00:00Z",
    }

    def fail_pointer(*args):
        raise OSError("synthetic pointer failure")

    monkeypatch.setattr(recovery, "write", fail_pointer)
    with pytest.raises(OSError, match="synthetic pointer"):
        helper.publish_exit(
            service,
            recovery,
            root,
            root / "infra/.env.test",
            "nexus-test",
            capture=capture,
            readiness=ready,
            check_only=False,
        )
    assert (state.read_bytes(), receipt.read_bytes()) == before
    proof = next(receipt.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes()
    options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    assert _invoke(root, env, "reconcile-exit").returncode != 0
    assert (state.read_bytes(), receipt.read_bytes()) == before
    options(root)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "already_reconciled"
    assert proof.read_bytes() == frozen and receipt.read_bytes() == before[1]
    assert starts(root) == []
