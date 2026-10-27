"""Real manager processes with isolated launchd and engine boundaries."""

import json
import os
import shutil
import subprocess
import sys

import pytest
from unit.ops.test_agent_paper_host_service import (
    ROOT,
    _calls,
    _executable,
    _invoke,
    _register,
    _service_root,
)


def recovery_root(tmp_path):
    root, environment = _service_root(tmp_path)
    source = ROOT / "scripts/macos/agent-paper-recovery.py"
    if source.exists():
        shutil.copy2(source, root / "scripts/macos" / source.name)
    subprocess.run(["git", "-C", str(root), "add", "."], check=True, capture_output=True)
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
            "-qm",
            "fixture",
        ],
        check=True,
        capture_output=True,
    )
    launchctl = tmp_path / "bin/launchctl"
    launchctl.write_text(
        launchctl.read_text()
        .replace(
            "job = json.loads(state.read_text())",
            "if any('com.nexus.agent-paper-recovery' in arg for arg in sys.argv):\n"
            "    state = state.with_name('recovery-registration')\n"
            "    if not state.exists(): state.write_text('{}')\n"
            "job = json.loads(state.read_text())",
        )
        .replace("elif action == 'kill':\n", "elif action == 'kill':\n    job.pop('pid', None)\n")
    )
    _executable(tmp_path / "bin/sysctl", "print('11111111-1111-1111-1111-111111111111')\n")
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    engine.write_text(
        engine.read_text().replace(
            "if '--service-startup-preflight' in sys.argv:",
            "if '--service-status' in sys.argv:\n"
            "    import subprocess\n"
            "    revision = subprocess.check_output(['git','-C',str(Path(__file__).parents[2]),"
            "'rev-parse','HEAD'],text=True).strip()\n"
            "    heartbeat = {'status':'fresh','release_revision':"
            "options.get('health_revision',revision),'migration_head':'0098_independent_monitor'}\n"
            "    print(json.dumps({'heartbeat': heartbeat, 'preflight':{'status':'ready'}})); "
            "sys.exit(0)\n"
            "if '--service-recovery-check' in sys.argv:\n"
            "    print(json.dumps({'status':options.get('recovery_status','ready'), "
            "'migration_head':'0098_independent_monitor', "
            "'blocking_codes':options.get('blocking_codes',[])})); sys.exit(0)\n"
            "if '--service-startup-preflight' in sys.argv:",
        )
    )
    return root, environment


def options(root, **values):
    (root.parent / "control").write_text(json.dumps(values))


def register_pid(root, pid):
    job = _register(root)
    job["pid"] = pid
    (root.parent / "state").write_text(json.dumps(job))


def starts(root):
    return [call for call in _calls(root) if call[:2] == ["launchctl", "kickstart"]]


def test_enable_observes_healthy_host_without_restarting_it(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    result = _invoke(root, env, "enable-recovery")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _invoke(root, env, "recover").returncode == 0
    assert starts(root) == []
    state = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
    assert state["desired"] == "enabled" and state["attempts"] == []


def test_manual_stop_disables_recovery_before_failed_stop(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    options(root, stop_exit=75)
    assert _invoke(root, env, "stop").returncode == 1
    state = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
    assert state["desired"] == "disabled"
    assert _invoke(root, env, "recover").returncode == 0
    assert starts(root) == []


def test_fresh_wrong_revision_cannot_enable_recovery(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    options(root, health_revision="b" * 40)
    assert _invoke(root, env, "enable-recovery").returncode == 1
    assert starts(root) == []


def test_existing_observer_registration_must_match_exact_owner(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    target = root.parent / "recovery-registration"
    job = json.loads(target.read_text())
    job["ProgramArguments"][1] = "/other/agent-paper-loop-service.py"
    target.write_text(json.dumps(job))
    assert _invoke(root, env, "enable-recovery").returncode == 1
    assert starts(root) == []


@pytest.mark.parametrize("action", ["disable-recovery", "maintenance"])
def test_delayed_wrapper_never_downgrades_after_management_is_disabled(tmp_path, action):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    assert _invoke(root, env, action).returncode == 0
    result = _invoke(root, env, "run")
    assert result.returncode == 1
    assert not any(call[0] == "environment" for call in _calls(root))


@pytest.mark.parametrize("action", ["recover", "enable-recovery"])
def test_pending_start_is_not_cleared_by_old_healthy_instance(tmp_path, action):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    pending = {
        "id": str(uuid4()),
        "at": value["checked_at"],
        "boot": value["boot"],
        "generation": value["generation"],
        "old_instance": value["instance"],
        "old_exit": 0,
    }
    value["attempt"] = pending
    value["attempts"].append(pending)
    target.write_text(json.dumps(value))
    _invoke(root, env, action)
    after = json.loads(target.read_text())
    assert after["attempt"] == pending and after["generation"] == value["generation"]
    assert starts(root) == []


def test_nonfinite_management_time_fails_closed(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    value["checked_at"] = float("nan")
    target.write_text(json.dumps(value))
    assert _invoke(root, env, "recover").returncode == 1
    assert starts(root) == []


def test_explicit_restart_after_verified_stop_keeps_managed_activation(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        child.terminate()
        child.wait(timeout=10)
        assert _invoke(root, env, "stop").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    _invoke(root, env, "start")
    value = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
    assert value["desired"] == "enabled" and value["attempt"] is not None
    assert len(starts(root)) == 1


def test_classified_dependency_exit_is_rechecked_without_erasing_original_exit(tmp_path):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    attempt = {
        "id": str(uuid4()),
        "at": value["checked_at"] - 3601,
        "generation": value["generation"],
        "boot": value["boot"],
        "old_instance": None,
        "old_exit": 0,
    }
    value["attempts"].append(attempt)
    value["attempt"] = attempt
    target.write_text(json.dumps(value))
    receipt = target.with_name("recovery-activation-" + attempt["id"] + ".json")
    receipt.write_text(
        json.dumps(
            {
                "version": 1,
                "attempt_id": attempt["id"],
                "generation": value["generation"],
                "revision": value["revision"],
                "boot": value["boot"],
                "pid": child.pid,
                "process_started": value["instance"]["started"],
                "phase": "exited",
                "reason": "database_unavailable",
            }
        )
    )
    receipt.chmod(0o600)
    _register(root, state="inactive", exit_code=1)
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=1)
    _invoke(root, env, "recover")
    assert len(starts(root)) == 1
    assert json.loads((root.parent / "state").read_text())["exit"] == 1
    assert json.loads(target.read_text())["attempts"][0] == attempt


@pytest.mark.parametrize("block", ["unknown", "nonzero", "safe"])
def test_exited_owned_process_requires_current_eligibility_and_never_repeats_start(tmp_path, block):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    job = _register(root, state="inactive", exit_code=1 if block == "nonzero" else 0)
    (root.parent / "state").write_text(json.dumps(job))
    if block == "unknown":
        options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    else:
        options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    _invoke(root, env, "recover")
    first = len(starts(root))
    assert first == (1 if block == "safe" else 0)
    _invoke(root, env, "recover")
    assert len(starts(root)) == first
    assert json.loads((root.parent / "state").read_text())["exit"] == (
        1 if block == "nonzero" else 0
    )


def test_managed_start_keeps_normal_running_status_contract(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    result = _invoke(root, env, "start")
    assert result.returncode == 0
    assert json.loads(result.stdout)["status"] == "running"
    assert starts(root) == []


def test_managed_start_never_reports_success_for_an_unconfirmed_launch(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        child.terminate()
        child.wait(timeout=10)
        assert _invoke(root, env, "stop").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_START_FAILED"
    assert len(starts(root)) == 1


@pytest.mark.parametrize(
    "case", ["hourly_limit", "backoff", "clock_reversed", "reboot", "pid_reused"]
)
def test_recovery_preserves_limits_and_boot_process_identity(tmp_path, case):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    if case == "hourly_limit":
        value["attempts"] = [
            {
                "id": str(uuid4()),
                "at": value["checked_at"],
                "boot": value["boot"],
                "generation": value["generation"],
            }
            for _ in range(3)
        ]
    elif case == "backoff":
        value["next_start_at"] = value["checked_at"] + 900
    elif case == "clock_reversed":
        value["checked_at"] += 3600
    elif case == "reboot":
        value["boot"] = str(uuid4())
    elif case == "pid_reused":
        value["instance"] = {"pid": os.getpid(), "started": "old-process-start"}
    target.write_text(json.dumps(value))
    _register(root, state="inactive", exit_code=1 if case == "reboot" else 0)
    options(root, kickstart_state="inactive", kickstart_exit=1)
    _invoke(root, env, "recover")
    assert len(starts(root)) == (1 if case == "reboot" else 0)
    after = json.loads(target.read_text())
    assert after["attempts"][: len(value["attempts"])] == value["attempts"]
    if case in {"backoff", "hourly_limit"}:
        assert after["reason"] == "RATE_LIMITED"


def test_failed_inspection_replaces_stale_healthy_diagnostic(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=0)
    (root / ".venv/bin/nexus-engine-agent-paper-loop").write_text("print('bad receipt')\n")
    assert _invoke(root, env, "recover").returncode == 0
    result = _invoke(root, env, "status")
    assert json.loads(result.stdout)["recovery"]["reason"] == "INSPECTION_UNAVAILABLE"
    assert starts(root) == []


def add_activation(root, *, pending=False):
    from uuid import uuid4

    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    attempt = {
        "id": str(uuid4()),
        "at": value["checked_at"] - 3601,
        "generation": value["generation"],
        "boot": value["boot"],
        "revision": value["revision"],
        "old_instance": None,
        "old_exit": 0,
    }
    value["attempts"].append(attempt)
    value["attempt"] = attempt if pending else None
    target.write_text(json.dumps(value))
    receipt = target.with_name("recovery-activation-" + attempt["id"] + ".json")
    receipt.write_text(
        json.dumps(
            {
                "version": 1,
                "attempt_id": attempt["id"],
                "generation": attempt["generation"],
                "revision": value["revision"],
                "boot": value["boot"],
                "pid": value["instance"]["pid"],
                "process_started": value["instance"]["started"],
                "phase": "exited",
                "reason": "database_unavailable",
            }
        )
    )
    receipt.chmod(0o600)
    return target, receipt


@pytest.mark.parametrize("pending", [False, True])
def test_verified_managed_stop_and_upgrade_preserve_history_and_allow_one_new_start(
    tmp_path, pending
):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        target, receipt = add_activation(root, pending=pending)
    finally:
        child.terminate()
        child.wait(timeout=10)
    before = receipt.read_bytes()
    assert _invoke(root, env, "stop").returncode == 0
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
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    _invoke(root, env, "start")
    assert len(starts(root)) == 1
    assert receipt.read_bytes() == before
    assert json.loads(target.read_text())["attempt"] is not None


def test_historical_classified_exit_never_authorizes_new_instance_unknown_exit(tmp_path):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        target, _ = add_activation(root)
    finally:
        child.terminate()
        child.wait(timeout=10)
    value = json.loads(target.read_text())
    value["generation"] = str(uuid4())
    target.write_text(json.dumps(value))
    _register(root, state="inactive", exit_code=1)
    _invoke(root, env, "recover")
    assert starts(root) == []
    assert json.loads(target.read_text())["reason"] == "EXIT_UNCLASSIFIED"


def test_observed_unconfirmed_exit_remains_blocked_after_new_boot(tmp_path):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=75)
    _invoke(root, env, "recover")
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    value = json.loads(target.read_text())
    value["boot"] = str(uuid4())
    target.write_text(json.dumps(value))
    (root.parent / "state").write_text("{}")
    _invoke(root, env, "recover")
    assert starts(root) == []


@pytest.mark.parametrize("fault", ["exit75", "owner", "corrupt"])
def test_status_keeps_management_diagnostics_on_errors(tmp_path, fault):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    if fault == "exit75":
        _register(root, state="inactive", exit_code=75)
    elif fault == "owner":
        options(root, duplicate_owner=True)
    else:
        (root / ".secrets/nexus-agent-paper/recovery.json").write_text("private-invalid-canary")
    result = _invoke(root, env, "status")
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["recovery"]["desired"] == ("unknown" if fault == "corrupt" else "enabled")
    assert "private-invalid-canary" not in result.stdout + result.stderr


def test_deployment_can_enroll_existing_host_in_maintenance_before_one_managed_cutover(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "maintenance").returncode == 0
        target = root / ".secrets/nexus-agent-paper/recovery.json"
        assert json.loads(target.read_text())["desired"] == "maintenance"
        assert not (root.parent / "recovery-registration").exists()
    finally:
        child.terminate()
        child.wait(timeout=10)
    assert _invoke(root, env, "stop").returncode == 0
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    _invoke(root, env, "start")
    assert len(starts(root)) == 1
    assert json.loads(target.read_text())["attempt"] is not None


def test_missing_registration_does_not_become_a_clean_exit_through_stop(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    (root.parent / "state").write_text("{}")
    result = _invoke(root, env, "stop")
    assert result.returncode == 1
    value = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
    assert value.get("stopped_confirmed") is not True
    _invoke(root, env, "start")
    assert starts(root) == []


def test_new_running_process_without_activation_receipt_is_still_pending(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        child.terminate()
        child.wait(timeout=10)
    assert _invoke(root, env, "stop").returncode == 0
    options(root, kickstart_state="running", kickstart_exit=0, exit=0)
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_START_FAILED"
    value = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
    assert value["attempt"] is not None and len(starts(root)) == 1


def test_reenrolling_same_healthy_instance_keeps_its_activation_exit_authority(tmp_path):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        target, receipt = add_activation(root)
        value = json.loads(target.read_text())
        value["active_attempt"] = value["attempts"][-1]
        target.write_text(json.dumps(value))
        original = receipt.read_bytes()
        assert _invoke(root, env, "disable-recovery").returncode == 0
        assert _invoke(root, env, "enable-recovery").returncode == 0
        assert starts(root) == []
    finally:
        child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=1)
    _invoke(root, env, "recover")
    assert len(starts(root)) == 1
    assert receipt.read_bytes() == original


@pytest.mark.parametrize("eligible", [False, True])
def test_old_boot_pending_keeps_history_and_requires_current_business_eligibility(
    tmp_path, eligible
):
    from uuid import uuid4

    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        target, receipt = add_activation(root, pending=True)
        value = json.loads(target.read_text())
        original_attempt = value["attempt"]
        content = json.loads(receipt.read_text())
        content["phase"] = "activated"
        receipt.write_text(json.dumps(content))
        original_receipt = receipt.read_bytes()
    finally:
        child.terminate()
        child.wait(timeout=10)
    _executable(tmp_path / "bin/sysctl", "print(" + repr(str(uuid4())) + ")\n")
    (root.parent / "state").write_text("{}")
    options(root, recovery_status="ready" if eligible else "blocked")
    _invoke(root, env, "recover")
    assert len(starts(root)) == int(eligible)
    assert receipt.read_bytes() == original_receipt
    current = json.loads(target.read_text())
    assert current["attempts"][0] == original_attempt
    assert (current["attempt"]["id"] != original_attempt["id"]) is eligible


@pytest.mark.parametrize("phase", ["missing", "checking", "rejected", "wrong_health", "activated"])
def test_pending_confirmation_requires_exact_activation_and_fresh_health(tmp_path, phase):
    root, env = recovery_root(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        target, receipt = add_activation(root, pending=True)
        pending = json.loads(target.read_text())["attempt"]
        if phase == "missing":
            receipt.unlink()
        else:
            value = json.loads(receipt.read_text())
            value["phase"] = "activated" if phase == "wrong_health" else phase
            receipt.write_text(json.dumps(value))
        if phase == "wrong_health":
            options(root, health_revision="b" * 40)
        _invoke(root, env, "recover")
        state = json.loads(target.read_text())
        if phase == "activated":
            assert state["attempt"] is None
            assert state["active_attempt"] == pending
            result = _invoke(root, env, "start")
            assert result.returncode == 0 and json.loads(result.stdout)["status"] == "running"
        else:
            assert state["attempt"] == pending
        assert starts(root) == []
    finally:
        child.terminate()
        child.wait(timeout=10)


@pytest.mark.parametrize(
    "case",
    [
        "ready",
        "unknown",
        "missing",
        "checking",
        "wrong_attempt",
        "wrong_boot",
        "wrong_generation",
        "wrong_revision",
        "live",
        "exit75",
    ],
)
def test_revoked_pending_guard_exit_settles_disabled_then_reenables_once(tmp_path, case):
    """Real child guard, manager subprocesses and process exit; only launchd/DB are isolated."""
    root, env = recovery_root(tmp_path)
    clock = tmp_path / "clock"
    clock.write_text("2000000000")
    helper = root / "scripts/macos/agent-paper-recovery.py"
    helper.write_text(
        helper.read_text().replace(
            'LABEL = "com.nexus.agent-paper-recovery"',
            f"time.time = lambda: float(Path({str(clock)!r}).read_text())\n"
            'LABEL = "com.nexus.agent-paper-recovery"',
        )
    )
    previous = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        register_pid(root, previous.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
    finally:
        previous.terminate()
        previous.wait(timeout=10)
    _register(root, state="inactive", exit_code=0)
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "recover").returncode == 0
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    before = json.loads(target.read_text())
    original = before["attempt"]
    assert original is not None and len(starts(root)) == 1
    # Pause immediately before the actual guard's final locked validation.
    source = """
import asyncio, json, sys
from pathlib import Path
from nexus.ops import agent_paper_recovery as readiness
from nexus.ops.agent_paper_recovery_activation import make_recovery_guard
from nexus.settings import EngineSettings
root = Path(sys.argv[1])
state = json.loads((root / ".secrets/nexus-agent-paper/recovery.json").read_text())
settings = EngineSettings(release_revision=state["revision"])
async def ready(_): return {"status": "ready"}
checks = []
def quiescent():
    checks.append(True)
    if len(checks) == 2:
        print("at-final-guard", flush=True)
        assert sys.stdin.readline().strip() == "continue"
readiness.inspect_recovery_readiness = ready
readiness.require_recovery_process_quiescence = quiescent
guard = make_recovery_guard(settings, repo_root=root,
    env_file=root / "infra/.env.test", generation=state["generation"],
    attempt_id=state["attempt"]["id"])
try:
    asyncio.run(guard(settings))
except ValueError:
    print("rejected", flush=True)
    assert sys.stdin.readline().strip() == "exit"
    sys.exit(1)
raise AssertionError("revoked child activated")
"""
    child_env = {**env, "PYTHONPATH": str(ROOT / "engine/src")}
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", source, str(root)],
        env=child_env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "at-final-guard"
        assert _invoke(root, env, "disable-recovery").returncode == 0
        disabled = json.loads(target.read_text())
        assert disabled["generation"] != original["generation"]
        assert disabled["attempt"] == original
        child.stdin.write("continue\n")
        child.stdin.flush()
        assert child.stdout.readline().strip() == "rejected"
        receipt = target.with_name(f"recovery-activation-{original['id']}.json")
        saved = receipt.read_bytes()
        evidence = json.loads(saved)
        assert evidence["phase"] == "rejected"
        assert evidence["reason"] == "PRE_ACTIVATION_REJECTED"
        if case != "live":
            child.stdin.write("exit\n")
            child.stdin.flush()
            assert child.wait(timeout=10) == 1
        if case == "missing":
            receipt.unlink()
        elif case in {
            "checking",
            "wrong_attempt",
            "wrong_boot",
            "wrong_generation",
            "wrong_revision",
        }:
            field, value = {
                "checking": ("phase", "checking"),
                "wrong_attempt": ("attempt_id", "22222222-2222-2222-2222-222222222222"),
                "wrong_boot": ("boot", "22222222-2222-2222-2222-222222222222"),
                "wrong_generation": ("generation", "22222222-2222-2222-2222-222222222222"),
                "wrong_revision": ("revision", "b" * 40),
            }[case]
            evidence[field] = value
            receipt.write_text(json.dumps(evidence))
        _register(root, state="inactive", exit_code=75 if case == "exit75" else 1)
        preserved = receipt.read_bytes() if receipt.exists() else None
        _invoke(root, env, "recover")
        settled = json.loads(target.read_text())
        assert settled["desired"] == "disabled"
        assert settled["attempts"] == before["attempts"]
        assert len(starts(root)) == 1
        assert (receipt.read_bytes() if receipt.exists() else None) == preserved
        if case in {"ready", "unknown"}:
            assert settled["attempt"] is None
            assert settled["last_attempt_resolution"] == "REVOKED_BEFORE_ACTIVATION"
            assert settled["attempt_resolutions"][-1]["exit"] == 1
            assert settled["rejected_start"] == original
        else:
            assert settled["attempt"] == original
        if case == "unknown":
            options(
                root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
            )
        clock.write_text("2000000061")
        result = _invoke(root, env, "enable-recovery")
        after = json.loads(target.read_text())
        if case == "ready":
            assert result.returncode == 0, result.stdout + result.stderr
            assert len(starts(root)) == 2
            assert after["attempt"]["id"] != original["id"]
            assert after["attempts"][:-1] == before["attempts"]
            assert after["desired"] == "enabled"
            assert any(c[0] == "engine" and "--service-recovery-check" in c for c in _calls(root))
            _invoke(root, env, "recover")
            assert len(starts(root)) == 2
        else:
            assert result.returncode == 1
            assert after["desired"] == "disabled"
            assert len(starts(root)) == 1
        assert (receipt.read_bytes() if receipt.exists() else None) == preserved
    finally:
        if child.poll() is None:
            child.terminate()
        child.communicate(timeout=10)


def test_healthy_reenrollment_keeps_local_resolution_audit(tmp_path):
    root, env = recovery_root(tmp_path)
    register_pid(root, os.getpid())
    assert _invoke(root, env, "enable-recovery").returncode == 0
    target = root / ".secrets/nexus-agent-paper/recovery.json"
    before = json.loads(target.read_text())
    audit = [{"reason": "REVOKED_BEFORE_ACTIVATION", "exit": 1, "at": before["checked_at"]}]
    before["attempt_resolutions"] = audit
    target.write_text(json.dumps(before))
    assert _invoke(root, env, "disable-recovery").returncode == 0
    assert _invoke(root, env, "enable-recovery").returncode == 0
    assert json.loads(target.read_text())["attempt_resolutions"] == audit
    assert starts(root) == []
