"""Price renewal preserves the old exit while consuming one exact new configuration."""

import hashlib
import importlib.util
import json
import subprocess
import sys
from uuid import UUID

import pytest
from unit.ops.test_agent_paper_host_service import _invoke, _register
from unit.ops.test_agent_paper_recovery_control import options, register_pid, starts
from unit.ops.test_managed_exit_reconciliation_control import ended_activated_host


def setup(tmp_path, monkeypatch):
    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    for key in ("HOME", "PATH"):
        monkeypatch.setenv(key, env[key])
    spec = importlib.util.spec_from_file_location(
        "price_exit_service", root / "scripts/macos/agent-paper-loop-service.py"
    )
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    recovery, helper = service._recovery_module(), service._reconciliation_module()
    config = root / "infra/.env.test"
    source = config.read_bytes()
    changed = (
        source
        + b"\nNEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=00000000-0000-0000-0000-000000000004\n"
    )
    capture = helper.capture_exit(service, recovery, root, config, "nexus-test")
    target = dict(
        schema=capture["producer"]["schema"],
        revision=capture["reconciliation_revision"],
        policy="same_schema_price_renewal_v1",
        config_hash=hashlib.sha256(changed).hexdigest(),
        scope_hash="a" * 64,
        old_model_id=str(UUID(int=3)),
        new_model_id=str(UUID(int=4)),
        old_model_hash="b" * 64,
        new_model_hash="c" * 64,
        old_price_hash="d" * 64,
        new_price_hash="e" * 64,
    )
    ready = dict(
        status="ready",
        blocking_codes=[],
        execution_locked=True,
        external_requests=0,
        migration_head=target["schema"],
        scope_hash=target["scope_hash"],
    )

    def publish(check=False, selected=None):
        return helper.publish_exit(
            service,
            recovery,
            root,
            config,
            "nexus-test",
            capture=capture,
            readiness=ready,
            check_only=check,
            price_target=target if selected is None else selected,
            source_environment=source,
        )

    # Existing fake engine represents a full ready check; give it its target scope.
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text().replace(
            "'blocking_codes':options.get('blocking_codes',[])}",
            "'blocking_codes':options.get('blocking_codes',[]),'scope_hash':'" + "a" * 64 + "'}",
        )
    )
    return (
        root,
        env,
        state_path,
        receipt_path,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    )


def test_check_apply_switch_enable_and_child_binding(tmp_path, monkeypatch):
    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    old_receipt = receipt.read_bytes()
    old_state = state_path.read_bytes()
    assert publish(True)["status"] == "eligible_at_check"
    assert state_path.read_bytes() == old_state
    assert not list(receipt.parent.glob("recovery-source-env-*"))
    result = publish()
    proof = helper._proof_path(root, result["proof_id"])
    frozen = proof.read_bytes()
    assert b"source_environment_base64" not in frozen
    assert helper.price_source_environment(root, config) == source
    assert _invoke(root, env, "enable-recovery").returncode != 0
    assert starts(root) == []
    config.write_bytes(changed)
    assert publish()["status"] == "already_reconciled"
    options(root, kickstart_state="inactive", exit=1)
    _invoke(root, env, "enable-recovery")
    state = json.loads(state_path.read_text())
    assert len(starts(root)) == 1
    assert state["config_hash"] == target["config_hash"]
    assert state["exit_reconciliation"]["consumed_by"] == state["attempt"]["id"]
    helper.validate_pending_activation(root, state, state["attempt"])
    _invoke(root, env, "enable-recovery")
    _invoke(root, env, "recover")
    assert len(starts(root)) == 1
    assert proof.read_bytes() == frozen and receipt.read_bytes() == old_receipt
    # The source remains immutable while the new activation is pending.
    source_path = helper._source_environment_path(root, result["proof_id"])
    source_path.write_bytes(source + b"changed")
    with pytest.raises(ValueError, match="PROOF_INVALID"):
        helper.validate_pending_activation(root, state, state["attempt"])


@pytest.mark.parametrize("fault", ["target", "receipt", "source", "unrelated_config"])
def test_changed_binding_cannot_enable(tmp_path, monkeypatch, fault):
    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    result = publish()
    config.write_bytes(changed)
    if fault == "target":
        with pytest.raises(ValueError):
            publish(selected=dict(target, new_model_hash="f" * 64))
        config.write_bytes(changed + b"changed")
    elif fault == "receipt":
        receipt.write_bytes(receipt.read_bytes() + b" ")
    elif fault == "source":
        helper._source_environment_path(root, result["proof_id"]).write_bytes(source + b" ")
    else:
        config.write_bytes(changed + b"X=1\n")
    assert _invoke(root, env, "enable-recovery").returncode != 0
    assert starts(root) == []


def test_pointer_interruption_reuses_exact_snapshot_and_proof(tmp_path, monkeypatch):
    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    write = recovery.write
    monkeypatch.setattr(
        recovery, "write", lambda *a: (_ for _ in ()).throw(OSError("pointer failure"))
    )
    with pytest.raises(OSError):
        publish()
    proof = helper._proof_path(root, capture["producer"]["active_attempt"]["id"])
    frozen = proof.read_bytes()
    snapshot = helper._source_environment_path(root, capture["producer"]["active_attempt"]["id"])
    assert snapshot.read_bytes() == source
    monkeypatch.setattr(recovery, "write", write)
    assert publish()["status"] == "already_reconciled"
    assert proof.read_bytes() == frozen and snapshot.read_bytes() == source and starts(root) == []


def test_consumed_proof_archives_only_after_confirmed_successor_and_survives_toggle_price(
    tmp_path, monkeypatch
):
    from unit.ops.test_agent_paper_recovery_control import options

    (
        root,
        env,
        state_path,
        old_receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    publish()
    config.write_bytes(changed)
    proof = next(old_receipt.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes(), old_receipt.read_bytes()
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "enable-recovery").returncode == 0
    pending = json.loads(state_path.read_text())["attempt"]
    pointer = json.loads(state_path.read_text())["exit_reconciliation"]
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "recover").returncode == 0
        assert json.loads(state_path.read_text())["exit_reconciliation"] == pointer
        started = subprocess.check_output(
            ["/bin/ps", "-p", str(child.pid), "-o", "lstart="], text=True
        ).strip()
        receipt = json.loads(old_receipt.read_text())
        receipt.update(
            attempt_id=pending["id"],
            generation=pending["generation"],
            revision=pending["revision"],
            boot=pending["boot"],
            pid=child.pid,
            process_started=started,
        )
        target = old_receipt.with_name("recovery-activation-" + pending["id"] + ".json")
        target.write_text(json.dumps(receipt))
        target.chmod(0o600)
        options(root, health_revision="b" * 40)
        assert _invoke(root, env, "recover").returncode == 0
        assert json.loads(state_path.read_text())["exit_reconciliation"] == pointer
        options(root)
        result = _invoke(root, env, "recover")
        assert result.returncode == 0, result.stdout + result.stderr
        state = json.loads(state_path.read_text())
        assert state["reason"] == "HEALTHY" and state["active_attempt"] == pending
        assert state.get("exit_reconciliation") is None
        history = state["exit_reconciliation_history"]
        assert len(history) == 1
        assert history[0]["consumed_by"] == pending["id"]
        assert history[0]["consumer_attempt"] == pending
        assert history[0]["confirmed_instance"] == {"pid": child.pid, "started": started}
        assert history[0]["evidence_sha256"] == pointer["evidence_sha256"]
        for action in ("recover", "maintenance", "enable-recovery"):
            result = _invoke(root, env, action)
            assert result.returncode == 0, result.stdout + result.stderr
            assert json.loads(state_path.read_text())["exit_reconciliation_history"] == history
        assert len(starts(root)) == 1
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    assert _invoke(root, env, "maintenance").returncode == 0
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["proof_id"] == pending["id"]
    assert (proof.read_bytes(), old_receipt.read_bytes()) == frozen
    assert len(starts(root)) == 1


@pytest.mark.parametrize("ending", ["revoked", "prior_boot"])
def test_consumed_proof_archives_with_its_confirmed_pending_end_price(
    tmp_path, monkeypatch, ending
):
    from unit.ops.test_agent_paper_host_service import _executable
    from unit.ops.test_agent_paper_recovery_control import options

    (
        root,
        env,
        state_path,
        receipt_path,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    publish()
    config.write_bytes(changed)
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "enable-recovery").returncode == 0
    state = json.loads(state_path.read_text())
    pending = state["attempt"]
    pointer = state["exit_reconciliation"]
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes(), receipt_path.read_bytes()
    if ending == "revoked":
        assert _invoke(root, env, "disable-recovery").returncode == 0
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            started = subprocess.check_output(
                ["/bin/ps", "-p", str(child.pid), "-o", "lstart="], text=True
            ).strip()
        finally:
            child.terminate()
            child.wait(timeout=10)
        receipt = json.loads(receipt_path.read_text())
        receipt.update(
            attempt_id=pending["id"],
            generation=pending["generation"],
            revision=pending["revision"],
            phase="rejected",
            reason="PRE_ACTIVATION_REJECTED",
            pid=child.pid,
            process_started=started,
        )
        target = receipt_path.with_name("recovery-activation-" + pending["id"] + ".json")
        target.write_text(json.dumps(receipt))
        target.chmod(0o600)
    else:
        _executable(root.parent / "bin/sysctl", "print('22222222-2222-2222-2222-222222222222')\n")
        state["next_start_at"] = 0
        state_path.write_text(json.dumps(state))
    result = _invoke(root, env, "recover")
    assert result.returncode == 0, result.stdout + result.stderr
    after = json.loads(state_path.read_text())
    assert after.get("exit_reconciliation") is None
    assert after["exit_reconciliation_history"][0]["consumed_by"] == pending["id"]
    assert after["exit_reconciliation_history"][0]["evidence_sha256"] == pointer["evidence_sha256"]
    if ending == "revoked":
        assert after["desired"] == "disabled" and after["attempt"] is None
        assert len(starts(root)) == 1
    else:
        assert after["attempt"]["id"] != pending["id"]
        assert "exit_reconciliation_sha256" not in after["attempt"]
        assert len(starts(root)) == 2
    assert (proof.read_bytes(), receipt_path.read_bytes()) == frozen


@pytest.mark.parametrize("fault", ["schema", "scope", "hash", "model_same", "source_bytes"])
def test_invalid_target_check_does_not_publish(tmp_path, monkeypatch, fault):
    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    bad = dict(target)
    if fault == "schema":
        bad["schema"] = "0119_compact_budget_obligations"
    elif fault == "scope":
        bad["scope_hash"] = "f" * 64
    elif fault == "hash":
        bad["config_hash"] = "invalid"
    elif fault == "model_same":
        bad["new_model_id"] = bad["old_model_id"]
    else:
        bad["config_hash"] = capture["producer"]["config_hash"]
    old = state_path.read_bytes()
    with pytest.raises(ValueError):
        publish(True, selected=bad)
    assert state_path.read_bytes() == old and starts(root) == []
    assert not list(receipt.parent.glob("recovery-source-env-*"))
    assert not list(receipt.parent.glob("recovery-reconciliation-*"))


@pytest.mark.parametrize("fault", ["partial", "symlink", "permission"])
def test_invalid_private_snapshot_is_never_overwritten(tmp_path, monkeypatch, fault):
    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    snapshot = helper._source_environment_path(root, capture["producer"]["active_attempt"]["id"])
    if fault == "symlink":
        snapshot.symlink_to(config)
    else:
        snapshot.write_bytes(source[:1] if fault == "partial" else source)
        snapshot.chmod(0o600 if fault == "partial" else 0o644)
    original = snapshot.read_bytes()
    with pytest.raises((ValueError, OSError)):
        publish()
    assert snapshot.read_bytes() == original and starts(root) == []
    assert not list(receipt.parent.glob("recovery-reconciliation-*"))


def test_interrupted_snapshot_link_publication_reuses_exact_bytes(tmp_path, monkeypatch):
    import os

    (
        root,
        env,
        state_path,
        receipt,
        config,
        source,
        changed,
        capture,
        target,
        helper,
        recovery,
        publish,
    ) = setup(tmp_path, monkeypatch)
    snapshot = helper._source_environment_path(root, capture["producer"]["active_attempt"]["id"])
    orphan = snapshot.with_name("source-env.interrupted")
    orphan.write_bytes(source)
    orphan.chmod(0o600)
    os.link(orphan, snapshot)
    assert publish()["status"] == "reconciled"
    assert snapshot.read_bytes() == source and snapshot.stat().st_nlink == 1 and not orphan.exists()
    assert starts(root) == []
