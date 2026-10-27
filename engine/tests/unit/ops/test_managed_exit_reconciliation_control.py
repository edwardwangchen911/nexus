"""Explicit exit reconciliation never changes an old activation or starts work."""

import json
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest
from unit.ops.test_agent_paper_host_service import _invoke, _register
from unit.ops.test_agent_paper_recovery_control import (
    add_activation,
    recovery_root,
    register_pid,
    starts,
)


def ended_activated_host(tmp_path):
    root, env = recovery_root(tmp_path)
    (root / ".venv/bin/python3").symlink_to(Path(sys.executable).resolve())
    source = Path(__file__).resolve().parents[4]
    shutil.copy2(source / "scripts/macos/agent-paper-reconciliation.py", root / "scripts/macos")
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text().replace(
            "if '--service-status' in sys.argv:",
            "if '--service-exit-reconciliation' in sys.argv:\n"
            + "    sys.path.insert(0, "
            + repr(str(source / "engine/src"))
            + ")\n"
            + "    sys.path.insert(0, "
            + repr(sysconfig.get_path("purelib"))
            + ")\n"
            + "    from uuid import UUID\n"
            + "    from nexus.settings import EngineSettings\n"
            + "    from nexus.ops.managed_exit_reconciliation import reconcile_managed_exit\n"
            + "    from nexus.ops import agent_paper_recovery as checks\n"
            + "    from nexus.paper.manual_entry import manual_host_scope\n"
            + "    config=EngineSettings(database_url='postgresql+asyncpg://local@127.0.0.1/test', "
            + "agent_runtime_mode='codex_sdk', agent_paper_cycle_enabled=True, "
            + "agent_paper_account_id=UUID(int=1),agent_paper_candidate_batch_id=UUID(int=2), "
            + "agent_paper_formal_model_config_id=UUID(int=3),release_revision='a'*40)\n"
            + "    async def inspect(settings):\n"
            + "        return {'status':options.get('recovery_status','ready'), "
            + "'blocking_codes':options.get('blocking_codes',[]), "
            + "'migration_head':'0098_independent_monitor','scope_hash':manual_host_scope(config), "
            + "'execution_locked':True,'external_requests':0,'checked_at':'2026-10-27T14:00:00Z'}\n"
            + "    checks.inspect_recovery_readiness=inspect\n"
            + "    checks.require_recovery_process_quiescence=lambda:None\n"
            + "    outcome=reconcile_managed_exit(config,repo_root=Path(__file__).parents[2], "
            + "env_file=Path(__file__).parents[2]/'infra/.env.test',project='nexus-test', "
            + "check_only=sys.argv[sys.argv.index('--service-exit-reconciliation')+1]=='check')\n"
            + "    print(json.dumps(outcome));sys.exit(1 if outcome['status']=='blocked' else 0)\n"
            + "if '--service-status' in sys.argv:",
        )
    )
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        state_path, receipt_path = add_activation(root)
        state = json.loads(state_path.read_text())
        state["active_attempt"] = state["attempts"][-1]
        state_path.write_text(json.dumps(state))
        receipt = json.loads(receipt_path.read_text())
        receipt.update(phase="activated", reason="READY")
        receipt_path.write_text(json.dumps(receipt))
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    assert _invoke(root, env, "maintenance").returncode == 0
    return root, env, state_path, receipt_path


def test_reconciliation_check_does_not_rewrite_old_activation_or_start(tmp_path):
    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    original_state = state_path.read_bytes()
    original_activation = receipt_path.read_bytes()
    result = _invoke(root, env, "reconcile-exit", "--check-only")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "eligible_at_check"
    assert state_path.read_bytes() == original_state
    assert receipt_path.read_bytes() == original_activation
    assert not list(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    assert starts(root) == []


def test_apply_is_immutable_and_explicit_enable_consumes_once(tmp_path):
    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    original_activation = receipt_path.read_bytes()
    old = json.loads(state_path.read_text())
    first = _invoke(root, env, "reconcile-exit")
    assert first.returncode == 0, first.stdout + first.stderr
    value = json.loads(first.stdout)
    assert value["status"] == "reconciled"
    proof = receipt_path.with_name("recovery-reconciliation-" + value["proof_id"] + ".json")
    frozen = proof.read_bytes()
    assert _invoke(root, env, "recover").returncode == 0
    assert starts(root) == []
    again = _invoke(root, env, "reconcile-exit")
    assert again.returncode == 0, again.stdout + again.stderr
    assert json.loads(again.stdout)["status"] == "already_reconciled"
    assert proof.read_bytes() == frozen and receipt_path.read_bytes() == original_activation
    state = json.loads(state_path.read_text())
    assert state["instance"] == old["instance"]
    assert state.get("stop_proof") == old.get("stop_proof")
    assert state.get("stopped_confirmed") == old.get("stopped_confirmed")
    assert state["desired"] == "maintenance"
    from unit.ops.test_agent_paper_recovery_control import options

    options(root, kickstart_state="inactive", exit=1)
    _invoke(root, env, "enable-recovery")
    state = json.loads(state_path.read_text())
    assert len(starts(root)) == 1
    assert state["attempt"]["exit_reconciliation_sha256"] == value["proof_hash"]
    assert state["exit_reconciliation"]["consumed_by"] == state["attempt"]["id"]
    _invoke(root, env, "enable-recovery")
    _invoke(root, env, "recover")
    assert len(starts(root)) == 1
    assert proof.read_bytes() == frozen and receipt_path.read_bytes() == original_activation


def test_retirement_preserves_nonzero_exit_and_allows_upgrade(tmp_path):
    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    activation = receipt_path.read_bytes()
    old = json.loads(state_path.read_text())
    stopped = _invoke(root, env, "stop")
    assert stopped.returncode == 0, stopped.stdout + stopped.stderr
    after = json.loads(state_path.read_text())
    assert after.get("stop_proof") == old.get("stop_proof")
    assert after.get("stopped_confirmed") == old.get("stopped_confirmed")
    assert after["exit_reconciliation"]["registration_retired"] is True
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "upgrade",
        ],
        check=True,
        capture_output=True,
    )
    from unit.ops.test_agent_paper_recovery_control import options

    options(root, kickstart_state="inactive", exit=1)
    _invoke(root, env, "enable-recovery")
    assert len(starts(root)) == 1
    assert receipt_path.read_bytes() == activation


@pytest.mark.parametrize("fault", ["enabled", "pending", "exit75", "receipt", "unknown"])
def test_unsafe_reconciliation_does_not_publish_or_start(tmp_path, fault):
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    state = json.loads(state_path.read_text())
    if fault == "enabled":
        state["desired"] = "enabled"
    elif fault == "pending":
        state["attempt"] = state["active_attempt"]
    elif fault == "exit75":
        _register(root, state="inactive", exit_code=75)
    elif fault == "receipt":
        receipt = json.loads(receipt_path.read_text())
        receipt["pid"] += 1
        receipt_path.write_text(json.dumps(receipt))
    elif fault in {"unknown"}:
        options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    state_path.write_text(json.dumps(state))
    before = state_path.read_bytes(), receipt_path.read_bytes()
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode != 0
    assert json.loads(result.stdout)["status"] == "blocked"
    assert not list(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == before
    assert starts(root) == []


def test_late_business_unknown_and_receipt_change_block_enable(tmp_path):
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    assert _invoke(root, env, "enable-recovery").returncode != 0
    assert starts(root) == []
    options(root)
    receipt = json.loads(receipt_path.read_text())
    receipt["late_field"] = True
    receipt_path.write_text(json.dumps(receipt))
    assert _invoke(root, env, "enable-recovery").returncode != 0
    assert starts(root) == []


def test_child_activation_rechecks_consumed_original_proof(tmp_path, monkeypatch):
    from uuid import UUID

    from nexus.ops.agent_paper_recovery_activation import RecoveryActivation
    from nexus.settings import EngineSettings
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    options(root, kickstart_state="inactive", exit=1)
    _invoke(root, env, "enable-recovery")
    state = json.loads(state_path.read_text())
    config = EngineSettings(
        database_url="postgresql+asyncpg://local@127.0.0.1/test",
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
        release_revision=state["revision"],
    )
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation._boot_id", lambda: state["boot"])
    guard = RecoveryActivation(
        config,
        repo_root=root,
        env_file=root / "infra/.env.test",
        generation=state["generation"],
        attempt_id=state["attempt"]["id"],
    )
    guard._validate(config)
    old = json.loads(receipt_path.read_text())
    old["late_change"] = True
    receipt_path.write_text(json.dumps(old))
    with pytest.raises(ValueError, match="BINDING_CHANGED"):
        guard._validate(config)


def test_orphan_proof_requires_revalidation_before_pointer_link(tmp_path, monkeypatch):
    import importlib.util

    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    for name in ("HOME", "PATH"):
        monkeypatch.setenv(name, env[name])
    spec = importlib.util.spec_from_file_location(
        "reconcile_failure_service", root / "scripts/macos/agent-paper-loop-service.py"
    )
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    recovery = service._recovery_module()
    helper = service._reconciliation_module()
    original_state = state_path.read_bytes()
    capture = helper.capture_exit(service, recovery, root, root / "infra/.env.test", "nexus-test")
    ready = {
        "status": "ready",
        "blocking_codes": [],
        "execution_locked": True,
        "external_requests": 0,
        "migration_head": CURRENT_MIGRATION_HEAD,
        "scope_hash": "a" * 64,
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
    assert state_path.read_bytes() == original_state
    proofs = list(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    assert len(proofs) == 1
    frozen = proofs[0].read_bytes()
    from unit.ops.test_agent_paper_recovery_control import options

    options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    assert _invoke(root, env, "reconcile-exit").returncode != 0
    assert state_path.read_bytes() == original_state
    options(root)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "already_reconciled"
    assert proofs[0].read_bytes() == frozen
    assert starts(root) == []


def test_old_boot_proof_does_not_block_original_prior_boot_recovery(tmp_path):
    from uuid import uuid4

    from unit.ops.test_agent_paper_host_service import _executable
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, _ = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    # Exact crash boundary: explicit enable published its new generation, but
    # no attempt was consumed or launched before the machine's boot changed.
    state = json.loads(state_path.read_text())
    state.update(desired="enabled", generation=str(uuid4()))
    state["exit_reconciliation"]["enable_generation"] = state["generation"]
    state_path.write_text(json.dumps(state))
    _executable(root.parent / "bin/sysctl", "print('22222222-2222-2222-2222-222222222222')\n")
    options(root, kickstart_state="inactive", exit=1)
    result = _invoke(root, env, "recover")
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(starts(root)) == 1
    after = json.loads(state_path.read_text())
    assert after.get("exit_reconciliation") is None
    assert after["exit_reconciliation_history"][0]["consumed_by"] is None
    assert "exit_reconciliation_sha256" not in after["attempt"]


def test_unconsumed_proof_remains_historical_across_two_code_upgrades(tmp_path):
    import time

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes()
    state = json.loads(state_path.read_text())
    state["next_start_at"] = time.time() + 600
    state_path.write_text(json.dumps(state))
    for version in ("B", "C"):
        subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                version,
            ],
            check=True,
            capture_output=True,
        )
        if version == "B":
            assert _invoke(root, env, "enable-recovery").returncode == 0
            assert _invoke(root, env, "maintenance").returncode == 0
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "already_reconciled"
    assert proof.read_bytes() == frozen
    assert starts(root) == []
    assert json.loads(state_path.read_text())["next_start_at"] == state["next_start_at"]


def test_consumed_proof_archives_only_after_confirmed_successor_and_survives_toggle(tmp_path):
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, old_receipt = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
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


def test_reconciliation_entry_timeout_is_bounded_and_keeps_result_unknown(
    tmp_path, monkeypatch, capsys
):
    import importlib.util

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    before = state_path.read_bytes(), receipt_path.read_bytes()
    spec = importlib.util.spec_from_file_location(
        "bounded_reconciliation_service", root / "scripts/macos/agent-paper-loop-service.py"
    )
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    for name in ("HOME", "PATH"):
        monkeypatch.setenv(name, env[name])
    original_run = subprocess.run
    calls = []

    def timeout(command, **kwargs):
        if "--service-exit-reconciliation" in command:
            calls.append(command)
            assert kwargs.get("timeout") == 45
            raise subprocess.TimeoutExpired(command, 45, output="sensitive raw response")
        return original_run(command, **kwargs)

    monkeypatch.setattr(subprocess, "run", timeout)
    result = service.main(
        [
            "reconcile-exit",
            "--repo-root",
            str(root),
            "--env-file",
            str(root / "infra/.env.test"),
            "--project-name",
            "nexus-test",
        ]
    )
    assert result == 1
    output = capsys.readouterr().out
    value = json.loads(output)
    assert value["status"] == "result_unknown"
    assert value["error_code"] == "AGENT_PAPER_EXIT_RECONCILIATION_TIMEOUT"
    assert "sensitive raw response" not in output
    assert len(calls) == 1 and starts(root) == []
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == before


@pytest.mark.parametrize("extra_link", [False, True])
def test_interrupted_link_publication_reenters_without_replacing_proof(tmp_path, extra_link):
    import os

    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text().replace(
            "    outcome=reconcile_managed_exit(",
            "    if options.get('interrupt_link'):\n"
            "        import os\n"
            "        original_link=os.link\n"
            "        def interrupted_link(*args, **kwargs):\n"
            "            original_link(*args, **kwargs)\n"
            "            os._exit(86)\n"
            "        os.link=interrupted_link\n"
            "    outcome=reconcile_managed_exit(",
        )
    )
    original = state_path.read_bytes(), receipt_path.read_bytes()
    options(root, interrupt_link=True)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 86
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen, inode = proof.read_bytes(), proof.stat().st_ino
    assert proof.stat().st_nlink == 2
    temporary = list(proof.parent.glob("reconciliation.*"))
    assert len(temporary) == 1
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == original
    options(root)
    if extra_link:
        os.link(proof, root.parent / "foreign-link")
        result = _invoke(root, env, "reconcile-exit")
        assert result.returncode != 0
        assert proof.stat().st_nlink == 3 and temporary[0].exists()
        assert (state_path.read_bytes(), receipt_path.read_bytes()) == original
    else:
        checked = _invoke(root, env, "reconcile-exit", "--check-only")
        assert checked.returncode == 0
        assert proof.stat().st_nlink == 2 and temporary[0].exists()
        result = _invoke(root, env, "reconcile-exit")
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(result.stdout)["status"] == "already_reconciled"
        assert proof.stat().st_nlink == 1 and not temporary[0].exists()
    assert proof.read_bytes() == frozen and proof.stat().st_ino == inode
    assert receipt_path.read_bytes() == original[1] and starts(root) == []


@pytest.mark.parametrize("stage", ["inspection", "publication"])
def test_real_diagnostic_timeout_releases_locks_and_preserves_partial_proof(tmp_path, stage):
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text()
        .replace(
            "    async def inspect(settings):\n",
            "    async def inspect(settings):\n"
            "        if options.get('pause_stage')=='inspection':\n"
            "            import asyncio\n"
            "            await asyncio.sleep(120)\n",
        )
        .replace(
            "    outcome=reconcile_managed_exit(",
            "    if options.get('pause_stage')=='publication':\n"
            "        import os, time\n"
            "        original_link=os.link\n"
            "        def paused_link(*args, **kwargs):\n"
            "            original_link(*args, **kwargs)\n"
            "            time.sleep(120)\n"
            "        os.link=paused_link\n"
            "    outcome=reconcile_managed_exit(",
        )
    )
    before = state_path.read_bytes(), receipt_path.read_bytes()
    options(root, pause_stage=stage)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 1
    value = json.loads(result.stdout)
    assert value["status"] == "result_unknown"
    assert value["error_code"] == "AGENT_PAPER_EXIT_RECONCILIATION_TIMEOUT"
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == before
    proofs = list(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    assert len(proofs) == (1 if stage == "publication" else 0)
    frozen = proofs[0].read_bytes() if proofs else None
    options(root)
    checked = _invoke(root, env, "reconcile-exit", "--check-only")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == before
    if proofs:
        result = _invoke(root, env, "reconcile-exit")
        assert result.returncode == 0, result.stdout + result.stderr
        assert proofs[0].read_bytes() == frozen
    assert starts(root) == []


def test_unused_prior_boot_proof_is_archived_without_being_consumed(tmp_path):
    from uuid import uuid4

    from unit.ops.test_agent_paper_host_service import _executable
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    original = json.loads(state_path.read_text())["exit_reconciliation"]
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes(), receipt_path.read_bytes()
    state = json.loads(state_path.read_text())
    state.update(desired="enabled", generation=str(uuid4()))
    state["exit_reconciliation"]["enable_generation"] = state["generation"]
    state_path.write_text(json.dumps(state))
    _executable(root.parent / "bin/sysctl", "print('22222222-2222-2222-2222-222222222222')\n")
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "recover").returncode == 0
    state = json.loads(state_path.read_text())
    assert state.get("exit_reconciliation") is None
    archived = state["exit_reconciliation_history"][0]
    assert archived["evidence_sha256"] == original["evidence_sha256"]
    assert archived["consumed_by"] is None
    assert "exit_reconciliation_sha256" not in state["attempt"]
    assert (proof.read_bytes(), receipt_path.read_bytes()) == frozen
    assert len(starts(root)) == 1

    pending = state["attempt"]
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        started = subprocess.check_output(
            ["/bin/ps", "-p", str(child.pid), "-o", "lstart="], text=True
        ).strip()
        receipt = json.loads(receipt_path.read_text())
        receipt.update(
            attempt_id=pending["id"],
            generation=pending["generation"],
            revision=pending["revision"],
            boot=pending["boot"],
            pid=child.pid,
            process_started=started,
        )
        target = receipt_path.with_name("recovery-activation-" + pending["id"] + ".json")
        target.write_text(json.dumps(receipt))
        target.chmod(0o600)
        options(root)
        result = _invoke(root, env, "recover")
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(state_path.read_text())["reason"] == "HEALTHY"
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    assert _invoke(root, env, "maintenance").returncode == 0
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["proof_id"] == pending["id"]
    assert (proof.read_bytes(), receipt_path.read_bytes()) == frozen
    assert len(starts(root)) == 1


@pytest.mark.parametrize("ending", ["revoked", "prior_boot"])
def test_consumed_proof_archives_with_its_confirmed_pending_end(tmp_path, ending):
    from unit.ops.test_agent_paper_host_service import _executable
    from unit.ops.test_agent_paper_recovery_control import options

    root, env, state_path, receipt_path = ended_activated_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
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
