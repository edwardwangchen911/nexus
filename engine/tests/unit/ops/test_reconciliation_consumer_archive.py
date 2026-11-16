"""One original exit may have distinct, fully validated activation consumers."""

import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest


def module():
    path = Path(__file__).resolve().parents[4] / "scripts/macos/agent-paper-reconciliation.py"
    spec = importlib.util.spec_from_file_location("consumer_archive_control", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_distinct_consumers_archive_without_rewriting_old_history(tmp_path, monkeypatch):
    control = module()
    original, first, second = (str(uuid4()) for _ in range(3))
    pointer = {"attempt_id": original, "consumed_by": first, "evidence_sha256": "a" * 64}
    prior = {"id": first, "generation": str(uuid4()), "boot": str(uuid4()), "revision": "a" * 40}
    prior["exit_reconciliation_sha256"] = "a" * 64
    pointer["enable_generation"] = prior["generation"]
    state = {
        "exit_reconciliation": pointer,
        "attempts": [prior],
        "attempt_resolutions": [
            {"attempt_id": first, "reason": "REVOKED_BEFORE_ACTIVATION", "exit": 1}
        ],
    }
    directory = tmp_path / ".secrets/nexus-agent-paper"
    directory.mkdir(parents=True)
    receipt = {
        "version": 1,
        "attempt_id": first,
        "generation": prior["generation"],
        "boot": prior["boot"],
        "revision": prior["revision"],
        "pid": 12345,
        "process_started": "fixed original process identity",
        "phase": "rejected",
        "reason": "PRE_ACTIVATION_REJECTED",
    }
    path = directory / f"recovery-activation-{first}.json"
    path.write_text(json.dumps(receipt))
    path.chmod(0o600)
    control._archive_pointer(state, pointer, "REVOKED_BEFORE_ACTIVATION", 1, consumer_attempt=prior)
    old = deepcopy(state["exit_reconciliation_history"][0])
    next_pointer = {**pointer, "consumed_by": second}
    state["exit_reconciliation"] = next_pointer
    monkeypatch.setattr(control, "validate_pending_activation", lambda *args: None)
    control.archive_consumed(
        tmp_path, state, {"id": second}, reason="SUCCESSOR_ACTIVATED", observed_at=2
    )
    assert state["exit_reconciliation_history"][0] == old
    assert len(state["exit_reconciliation_history"]) == 2
    assert "exit_reconciliation" not in state


@pytest.mark.parametrize("change", [{}, {"evidence_sha256": "b" * 64}, {"consumed_by": None}])
def test_repeated_consumer_or_changed_evidence_still_rejected(change):
    control = module()
    pointer = {"attempt_id": str(uuid4()), "consumed_by": str(uuid4()), "evidence_sha256": "a" * 64}
    state = {"exit_reconciliation_history": [{**pointer, **change}], "exit_reconciliation": pointer}
    before = deepcopy(state)
    with pytest.raises(ValueError, match="PROOF_INVALID"):
        control._archive_pointer(state, pointer, "SUCCESSOR_ACTIVATED", 2)
    assert state == before


def test_failed_activation_binding_cannot_archive(monkeypatch, tmp_path):
    control = module()
    consumer = str(uuid4())
    pointer = {"attempt_id": str(uuid4()), "consumed_by": consumer, "evidence_sha256": "a" * 64}
    state = {"exit_reconciliation": pointer}

    def reject(*args):
        raise ValueError("AGENT_PAPER_EXIT_RECONCILIATION_PROOF_INVALID")

    monkeypatch.setattr(control, "validate_pending_activation", reject)
    with pytest.raises(ValueError, match="PROOF_INVALID"):
        control.archive_consumed(
            tmp_path, state, {"id": consumer}, reason="SUCCESSOR_ACTIVATED", observed_at=2
        )
    assert state == {"exit_reconciliation": pointer}


def test_normal_reconcile_after_rejected_consumer_reaches_healthy(tmp_path):
    import os
    import subprocess
    import sys

    from unit.ops.test_agent_paper_host_service import _invoke
    from unit.ops.test_agent_paper_recovery_control import options, register_pid, starts
    from unit.ops.test_managed_exit_reconciliation_control import (
        test_consumed_proof_archives_with_its_confirmed_pending_end,
    )

    test_consumed_proof_archives_with_its_confirmed_pending_end(tmp_path, "revoked")
    root = tmp_path / "repo"
    env = {
        **os.environ,
        "HOME": str(tmp_path / "home"),
        "PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin",
    }
    state_path = root / ".secrets/nexus-agent-paper/recovery.json"
    before = json.loads(state_path.read_text())
    original_history = deepcopy(before["exit_reconciliation_history"])
    directory = state_path.parent
    frozen = {
        p.name: p.read_bytes()
        for p in directory.glob("recovery-*.json")
        if p.name != "recovery.json"
    }
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    value = json.loads(state_path.read_text())
    value["next_start_at"] = 0
    state_path.write_text(json.dumps(value))
    assert _invoke(root, env, "enable-recovery").returncode == 0
    pending = json.loads(state_path.read_text())["attempt"]
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        register_pid(root, child.pid)
        started = subprocess.check_output(
            ["/bin/ps", "-p", str(child.pid), "-o", "lstart="], text=True
        ).strip()
        receipt = {
            **pending,
            "version": 1,
            "attempt_id": pending["id"],
            "phase": "activated",
            "reason": "READY",
            "pid": child.pid,
            "process_started": started,
        }
        target = directory / f"recovery-activation-{pending['id']}.json"
        target.write_text(json.dumps(receipt))
        target.chmod(0o600)
        options(root)
        result = _invoke(root, env, "recover")
        assert result.returncode == 0, result.stdout + result.stderr
        after = json.loads(state_path.read_text())
        assert after["reason"] == "HEALTHY" and after["attempt"] is None
        assert after["exit_reconciliation_history"][:1] == original_history
        assert len(after["exit_reconciliation_history"]) == 2
        assert all((directory / name).read_bytes() == content for name, content in frozen.items())
        assert len(starts(root)) == 2
        assert _invoke(root, env, "recover").returncode == 0
        assert (
            json.loads(state_path.read_text())["exit_reconciliation_history"]
            == after["exit_reconciliation_history"]
        )
        assert len(starts(root)) == 2
    finally:
        child.terminate()
        child.wait(timeout=10)


@pytest.mark.parametrize(
    "fault",
    [
        "receipt_version",
        "receipt_phase",
        "receipt_pid",
        "receipt_started",
        "receipt_generation",
        "resolution_exit",
        "resolution_duplicate",
        "negative_symlink",
        "prior_hash",
    ],
)
def test_prior_rejection_corruption_never_archives(tmp_path, monkeypatch, fault):
    control = module()
    first, second, producer, generation, boot = (str(uuid4()) for _ in range(5))
    attempt = {
        "id": first,
        "generation": generation,
        "boot": boot,
        "revision": "a" * 40,
        "exit_reconciliation_sha256": "a" * 64,
    }
    old = {
        "attempt_id": producer,
        "consumed_by": first,
        "enable_generation": generation,
        "evidence_sha256": "a" * 64,
        "consumer_attempt": attempt,
        "archive_reason": "REVOKED_BEFORE_ACTIVATION",
    }
    pointer = {**old, "consumed_by": second}
    state = {
        "exit_reconciliation": pointer,
        "exit_reconciliation_history": [old],
        "attempts": [attempt],
        "attempt_resolutions": [
            {"attempt_id": first, "reason": "REVOKED_BEFORE_ACTIVATION", "exit": 1}
        ],
    }
    receipt = {
        "version": 1,
        "attempt_id": first,
        "generation": generation,
        "boot": boot,
        "revision": "a" * 40,
        "pid": 12345,
        "process_started": "original identity",
        "phase": "rejected",
        "reason": "PRE_ACTIVATION_REJECTED",
    }
    if fault.startswith("receipt_"):
        field = fault.removeprefix("receipt_")
        field = "process_started" if field == "started" else field
        receipt[field] = None
    elif fault == "resolution_exit":
        state["attempt_resolutions"][0]["exit"] = 75
    elif fault == "resolution_duplicate":
        state["attempt_resolutions"] *= 2
    elif fault == "prior_hash":
        attempt["exit_reconciliation_sha256"] = "b" * 64
    directory = tmp_path / ".secrets/nexus-agent-paper"
    directory.mkdir(parents=True)
    path = directory / f"recovery-activation-{first}.json"
    path.write_text(json.dumps(receipt))
    path.chmod(0o600)
    if fault == "negative_symlink":
        path.with_suffix(".write-pending").symlink_to(directory / "missing")
    before = deepcopy(state)
    monkeypatch.setattr(control, "validate_pending_activation", lambda *args: None)
    with pytest.raises(ValueError, match="PROOF_INVALID"):
        control.archive_consumed(
            tmp_path, state, {"id": second}, reason="SUCCESSOR_ACTIVATED", observed_at=2
        )
    assert state == before
