"""Pending consumer exit closes B explicitly without promoting it to healthy A."""

import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID

import pytest
from nexus.ops.agent_paper_recovery_activation import RecoveryActivation
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.settings import EngineSettings
from unit.ops.test_agent_paper_host_service import _invoke, _register
from unit.ops.test_agent_paper_recovery_control import options, register_pid, starts
from unit.ops.test_managed_exit_reconciliation_control import ended_activated_host


def _upgrade(root):
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
            "new target",
        ],
        check=True,
        capture_output=True,
    )


def _ended_pending_consumer(
    tmp_path, *, cross_schema=False, source_schema="0099_research_stages", target_schema=None
):
    schema = target_schema or (
        "0105_comparison_preclaim" if cross_schema else CURRENT_MIGRATION_HEAD
    )
    root, env, state_path, old_receipt = ended_activated_host(tmp_path, schema=schema)
    state = json.loads(state_path.read_text())
    if cross_schema:
        state["schema"] = source_schema
        state_path.write_text(json.dumps(state))
    original_active, original_instance = state["active_attempt"], state["instance"]
    published = _invoke(root, env, "reconcile-exit")
    assert published.returncode == 0, published.stdout + published.stderr
    prior_proof = old_receipt.with_name(f"recovery-reconciliation-{original_active['id']}.json")
    assert json.loads(prior_proof.read_text())["version"] == (2 if cross_schema else 1)
    options(root, kickstart_state="inactive", exit=1)
    enabled = _invoke(root, env, "enable-recovery")
    assert enabled.returncode == 0, enabled.stdout + enabled.stderr
    state = json.loads(state_path.read_text())
    pending = state["attempt"]
    assert pending["id"] != original_active["id"]
    assert state["exit_reconciliation"]["consumed_by"] == pending["id"]
    target = old_receipt.with_name(f"recovery-activation-{pending['id']}.json")
    child = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read(); sys.exit(1)"],
        stdin=subprocess.PIPE,
        text=True,
    )
    try:
        register_pid(root, child.pid)
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
            phase="activated",
            reason="READY",
        )
        target.write_text(json.dumps(receipt))
        target.chmod(0o600)
        # No parent recover/health observation occurs while B is alive.
        child.communicate(input="", timeout=10)
        assert child.returncode == 1
        receipt.update(phase="exited", reason="unclassified")
        target.write_text(json.dumps(receipt))
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    assert _invoke(root, env, "maintenance").returncode == 0
    state = json.loads(state_path.read_text())
    assert state["attempt"] == pending
    assert state["active_attempt"] == original_active and state["instance"] == original_instance
    assert state["generation"] != pending["generation"]
    assert state["desired"] == "maintenance"
    # Represent an elapsed backoff without sleeping or changing B's identity.
    state["next_start_at"] = state["checked_at"] - 1
    state_path.write_text(json.dumps(state))
    assert len(starts(root)) == 1
    return root, env, state_path, old_receipt, prior_proof, target


@pytest.mark.parametrize("cross_schema", [False, True])
def test_pending_consumer_closes_once_and_next_enable_consumes_its_proof(tmp_path, cross_schema):
    root, env, state_path, old_receipt, prior_proof, receipt = _ended_pending_consumer(
        tmp_path, cross_schema=cross_schema
    )
    before = json.loads(state_path.read_text())
    pending = before["attempt"]
    pointer = before["exit_reconciliation"]
    frozen = (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes())
    _upgrade(root)
    original_files = {p.name: p.read_bytes() for p in receipt.parent.glob("recovery*.json")}
    checked = _invoke(root, env, "reconcile-exit", "--check-only")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert json.loads(checked.stdout)["status"] == "eligible_at_check"
    assert {p.name: p.read_bytes() for p in receipt.parent.glob("recovery*.json")} == original_files
    applied = _invoke(root, env, "reconcile-exit")
    assert applied.returncode == 0, applied.stdout + applied.stderr
    result = json.loads(applied.stdout)
    assert result["status"] == "reconciled" and result["proof_id"] == pending["id"]
    proof = receipt.with_name(f"recovery-reconciliation-{pending['id']}.json")
    proof_bytes = proof.read_bytes()
    proof_hash = hashlib.sha256(proof_bytes).hexdigest()
    assert result["proof_hash"] == proof_hash
    evidence = json.loads(proof_bytes)
    assert evidence["version"] == 3 and evidence["kind"] == "pending_consumer_exit"
    assert evidence["historical_effects"] == "not_determined"
    assert evidence["business_disposition"] == "unchanged"
    assert evidence["execution_locked"] is True and evidence["replay_authorized"] is False
    after = json.loads(state_path.read_text())
    assert after["attempt"] is None
    for field in (
        "active_attempt",
        "instance",
        "attempts",
        "desired",
        "generation",
        "failure_streak",
        "next_start_at",
        "healthy_since",
    ):
        assert after[field] == before[field], field
    history = after["exit_reconciliation_history"]
    assert len(history) == 1
    assert history[0]["consumed_by"] == pending["id"]
    assert history[0]["evidence_sha256"] == pointer["evidence_sha256"]
    assert history[0]["consumer_attempt"] == pending
    assert history[0]["archive_reason"] == "PENDING_CONSUMER_EXIT_CONFIRMED"
    resolutions = after["attempt_resolutions"]
    assert len(resolutions) == len(before["attempt_resolutions"]) + 1
    assert resolutions[-1]["attempt_id"] == pending["id"]
    assert resolutions[-1]["code"] == "PENDING_EXIT_RECONCILED"
    assert resolutions[-1]["proof_hash"] == proof_hash
    assert after["exit_reconciliation"]["attempt_id"] == pending["id"]
    assert after["exit_reconciliation"]["evidence_sha256"] == proof_hash
    assert after["exit_reconciliation"]["consumed_by"] is None
    assert len(starts(root)) == 1
    again = _invoke(root, env, "reconcile-exit")
    assert again.returncode == 0, again.stdout + again.stderr
    assert json.loads(again.stdout)["status"] == "already_reconciled"
    repeated = json.loads(state_path.read_text())
    for field in (
        "attempts",
        "exit_reconciliation_history",
        "attempt_resolutions",
        "next_start_at",
        "failure_streak",
        "active_attempt",
        "instance",
    ):
        assert repeated[field] == after[field], field
    assert len(starts(root)) == 1
    assert proof.read_bytes() == proof_bytes
    assert (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes()) == frozen
    enabled = _invoke(root, env, "enable-recovery")
    assert enabled.returncode == 0, enabled.stdout + enabled.stderr
    consumer = json.loads(state_path.read_text())
    successor = consumer["attempt"]
    assert successor["id"] not in {pending["id"], before["active_attempt"]["id"]}
    assert successor["exit_reconciliation_sha256"] == proof_hash
    assert consumer["exit_reconciliation"]["consumed_by"] == successor["id"]
    assert consumer["attempts"][:-1] == before["attempts"]
    assert consumer["exit_reconciliation_history"] == history
    assert consumer["active_attempt"] == before["active_attempt"]
    assert consumer["instance"] == before["instance"]
    assert len(starts(root)) == 2
    _invoke(root, env, "enable-recovery")
    _invoke(root, env, "recover")
    assert len(starts(root)) == 2
    assert proof.read_bytes() == proof_bytes

    child = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read(); sys.exit(1)"],
        stdin=subprocess.PIPE,
        text=True,
    )
    try:
        register_pid(root, child.pid)
        started = subprocess.check_output(
            ["/bin/ps", "-p", str(child.pid), "-o", "lstart="], text=True
        ).strip()
        activation = json.loads(old_receipt.read_text())
        activation.update(
            attempt_id=successor["id"],
            generation=successor["generation"],
            revision=successor["revision"],
            boot=successor["boot"],
            pid=child.pid,
            process_started=started,
            phase="activated",
            reason="READY",
        )
        successor_receipt = receipt.with_name(f"recovery-activation-{successor['id']}.json")
        successor_receipt.write_text(json.dumps(activation))
        successor_receipt.chmod(0o600)
        options(root)
        healthy = _invoke(root, env, "recover")
        assert healthy.returncode == 0, healthy.stdout + healthy.stderr
        active = json.loads(state_path.read_text())
        assert active["reason"] == "HEALTHY" and active["attempt"] is None
        assert active["active_attempt"] == successor
        assert active["instance"] == {"pid": child.pid, "started": started}
        assert active.get("exit_reconciliation") is None
        assert active["exit_reconciliation_history"][:-1] == history
        archived_q = active["exit_reconciliation_history"][-1]
        assert archived_q["attempt_id"] == pending["id"]
        assert archived_q["evidence_sha256"] == proof_hash
        assert archived_q["consumed_by"] == successor["id"]
        assert archived_q["consumer_attempt"] == successor
        assert archived_q["archive_reason"] == "SUCCESSOR_ACTIVATED"
        assert archived_q["confirmed_instance"] == active["instance"]
        assert proof.read_bytes() == proof_bytes
        assert (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes()) == frozen
        child.communicate(input="", timeout=10)
        assert child.returncode == 1
        activation.update(phase="exited", reason="unclassified")
        successor_receipt.write_text(json.dumps(activation))
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    assert _invoke(root, env, "maintenance").returncode == 0
    before_check = {p.name: p.read_bytes() for p in receipt.parent.glob("recovery*.json")}
    successor_check = _invoke(root, env, "reconcile-exit", "--check-only")
    assert successor_check.returncode == 0, successor_check.stdout + successor_check.stderr
    assert json.loads(successor_check.stdout)["status"] == "eligible_at_check"
    assert {p.name: p.read_bytes() for p in receipt.parent.glob("recovery*.json")} == before_check
    # The check API does not expose producer identity; the same normal apply
    # makes its selected C identity observable without consulting a helper.
    successor_closed = _invoke(root, env, "reconcile-exit")
    assert successor_closed.returncode == 0, successor_closed.stdout + successor_closed.stderr
    assert json.loads(successor_closed.stdout)["proof_id"] == successor["id"]
    assert proof.read_bytes() == proof_bytes
    assert (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes()) == frozen
    assert len(starts(root)) == 2


@pytest.mark.parametrize("fault", ["enabled", "exit75", "write_pending", "unknown"])
def test_pending_consumer_refusal_preserves_original_chain_and_does_not_start(tmp_path, fault):
    root, env, state_path, old_receipt, prior_proof, receipt = _ended_pending_consumer(tmp_path)
    state = json.loads(state_path.read_text())
    if fault == "enabled":
        state["desired"] = "enabled"
        state_path.write_text(json.dumps(state))
    elif fault == "exit75":
        _register(root, state="inactive", exit_code=75)
    elif fault == "write_pending":
        receipt.with_suffix(".write-pending").write_text("unconfirmed\n")
    else:
        options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    frozen = {p.name: p.read_bytes() for p in receipt.parent.glob("recovery*") if p.is_file()}
    for flags in (("--check-only",), ()):
        result = _invoke(root, env, "reconcile-exit", *flags)
        assert result.returncode != 0
        assert json.loads(result.stdout)["status"] == "blocked"
        assert {
            p.name: p.read_bytes() for p in receipt.parent.glob("recovery*") if p.is_file()
        } == frozen
    assert prior_proof.exists() and old_receipt.exists()
    assert len(starts(root)) == 1


@pytest.mark.parametrize("fault", ["late_receipt", "changed_target"])
def test_pending_end_proof_rejects_late_source_or_target_change_before_next_enable(tmp_path, fault):
    root, env, state_path, _, _, receipt = _ended_pending_consumer(tmp_path)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    before = json.loads(state_path.read_text())
    if fault == "late_receipt":
        value = json.loads(receipt.read_text())
        value["late_fact"] = True
        receipt.write_text(json.dumps(value))
    else:
        _upgrade(root)
    assert _invoke(root, env, "enable-recovery").returncode != 0
    after = json.loads(state_path.read_text())
    assert after["attempt"] is None and after["attempts"] == before["attempts"]
    assert after["exit_reconciliation"] == before["exit_reconciliation"]
    assert after["exit_reconciliation_history"] == before["exit_reconciliation_history"]
    assert len(starts(root)) == 1


def test_pending_end_proof_is_rechecked_by_successor_child(tmp_path, monkeypatch):
    root, env, state_path, _, _, receipt = _ended_pending_consumer(tmp_path)
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _invoke(root, env, "enable-recovery").returncode == 0
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
    late = json.loads(receipt.read_text())
    late["late_fact"] = True
    receipt.write_text(json.dumps(late))
    with pytest.raises(ValueError, match="BINDING_CHANGED"):
        guard._validate(config)
    assert len(starts(root)) == 2


def _assert_single_pending_close(state_path, before, proof_hash):
    after = json.loads(state_path.read_text())
    pending = before["attempt"]
    assert after["attempt"] is None and after["attempts"] == before["attempts"]
    for field in (
        "active_attempt",
        "instance",
        "desired",
        "generation",
        "failure_streak",
        "next_start_at",
    ):
        assert after[field] == before[field], field
    assert len(after["exit_reconciliation_history"]) == 1
    archived = after["exit_reconciliation_history"][0]
    assert archived["consumed_by"] == pending["id"]
    assert archived["evidence_sha256"] == before["exit_reconciliation"]["evidence_sha256"]
    assert archived["archive_reason"] == "PENDING_CONSUMER_EXIT_CONFIRMED"
    assert len(after["attempt_resolutions"]) == len(before["attempt_resolutions"]) + 1
    resolution = after["attempt_resolutions"][-1]
    assert resolution["attempt_id"] == pending["id"]
    assert resolution["code"] == "PENDING_EXIT_RECONCILED"
    assert resolution["proof_hash"] == proof_hash
    assert after["exit_reconciliation"]["attempt_id"] == pending["id"]
    assert after["exit_reconciliation"]["evidence_sha256"] == proof_hash
    assert after["exit_reconciliation"]["consumed_by"] is None


@pytest.mark.parametrize("management_committed", [False, True])
def test_pending_close_interrupted_management_requires_same_proof_readback(
    tmp_path, management_committed
):
    root, env, state_path, old_receipt, prior_proof, receipt = _ended_pending_consumer(tmp_path)
    before = json.loads(state_path.read_text())
    original_state = state_path.read_bytes()
    frozen_sources = (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes())
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    source = engine.read_text()
    injected = source.replace(
        "    outcome=reconcile_managed_exit(",
        "    if options.get('interrupt_management'):\n"
        "        import os\n"
        "        original_replace=os.replace\n"
        "        def interrupted_replace(source, target):\n"
        "            if Path(target).name == 'recovery.json':\n"
        "                if options['interrupt_management']=='after':\n"
        "                    original_replace(source,target)\n"
        "                os._exit(86)\n"
        "            return original_replace(source,target)\n"
        "        os.replace=interrupted_replace\n"
        "    outcome=reconcile_managed_exit(",
    )
    assert injected != source
    engine.write_text(injected)
    options(root, interrupt_management="after" if management_committed else "before")
    interrupted = _invoke(root, env, "reconcile-exit")
    assert interrupted.returncode == 86, interrupted.stdout + interrupted.stderr
    proof = receipt.with_name(f"recovery-reconciliation-{before['attempt']['id']}.json")
    proof_bytes, inode = proof.read_bytes(), proof.stat().st_ino
    proof_hash = hashlib.sha256(proof_bytes).hexdigest()
    assert json.loads(proof_bytes)["version"] == 3
    assert (
        old_receipt.read_bytes(),
        prior_proof.read_bytes(),
        receipt.read_bytes(),
    ) == frozen_sources
    assert len(starts(root)) == 1
    if management_committed:
        _assert_single_pending_close(state_path, before, proof_hash)
    else:
        assert state_path.read_bytes() == original_state
    interrupted_state = state_path.read_bytes()
    # An orphan Q is not a substitute for fresh business qualification; an
    # already-committed Q also cannot bypass a newly observed unknown.
    options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    for flags in (("--check-only",), ()):
        blocked = _invoke(root, env, "reconcile-exit", *flags)
        assert blocked.returncode != 0
        assert json.loads(blocked.stdout)["status"] == "blocked"
        assert state_path.read_bytes() == interrupted_state
        assert proof.read_bytes() == proof_bytes and proof.stat().st_ino == inode
    options(root)
    checked = _invoke(root, env, "reconcile-exit", "--check-only")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert json.loads(checked.stdout)["status"] == "eligible_at_check"
    assert state_path.read_bytes() == interrupted_state
    completed = _invoke(root, env, "reconcile-exit")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    result = json.loads(completed.stdout)
    assert result["status"] == "already_reconciled"
    assert result["proof_id"] == before["attempt"]["id"] and result["proof_hash"] == proof_hash
    _assert_single_pending_close(state_path, before, proof_hash)
    completed_state = state_path.read_bytes()
    if management_committed:
        assert completed_state == interrupted_state
    repeated = _invoke(root, env, "reconcile-exit")
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    assert json.loads(repeated.stdout)["proof_hash"] == proof_hash
    assert state_path.read_bytes() == completed_state
    assert proof.read_bytes() == proof_bytes and proof.stat().st_ino == inode
    assert (
        old_receipt.read_bytes(),
        prior_proof.read_bytes(),
        receipt.read_bytes(),
    ) == frozen_sources
    assert len(starts(root)) == 1


def test_concurrent_pending_close_produces_one_management_transition(tmp_path):
    root, env, state_path, old_receipt, prior_proof, receipt = _ended_pending_consumer(tmp_path)
    before = json.loads(state_path.read_text())
    frozen_sources = (old_receipt.read_bytes(), prior_proof.read_bytes(), receipt.read_bytes())
    gate = Barrier(2)

    def close():
        gate.wait(timeout=15)
        return _invoke(root, env, "reconcile-exit")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(close) for _ in range(2)]
        outcomes = [future.result(timeout=60) for future in futures]
    successful = [json.loads(result.stdout) for result in outcomes if result.returncode == 0]
    assert successful and sum(item["status"] == "reconciled" for item in successful) == 1
    assert all(item["status"] in {"reconciled", "already_reconciled"} for item in successful)
    for result in outcomes:
        if result.returncode != 0:
            assert json.loads(result.stdout)["blocking_codes"] == [
                "AGENT_PAPER_EXIT_RECONCILIATION_LOCK_BUSY"
            ]
    proof = receipt.with_name(f"recovery-reconciliation-{before['attempt']['id']}.json")
    proof_bytes, inode = proof.read_bytes(), proof.stat().st_ino
    proof_hash = hashlib.sha256(proof_bytes).hexdigest()
    assert {item["proof_hash"] for item in successful} == {proof_hash}
    _assert_single_pending_close(state_path, before, proof_hash)
    committed = state_path.read_bytes()
    assert len(list(receipt.parent.glob("recovery-reconciliation-*.json"))) == 2
    again = _invoke(root, env, "reconcile-exit")
    assert again.returncode == 0, again.stdout + again.stderr
    assert json.loads(again.stdout)["status"] == "already_reconciled"
    assert state_path.read_bytes() == committed
    assert proof.read_bytes() == proof_bytes and proof.stat().st_ino == inode
    assert (
        old_receipt.read_bytes(),
        prior_proof.read_bytes(),
        receipt.read_bytes(),
    ) == frozen_sources
    assert len(starts(root)) == 1
