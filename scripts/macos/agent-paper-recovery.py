"""Bounded local supervision; never owns or retries business requests."""

from __future__ import annotations

import hashlib
import json
import math
import os
import plistlib
import re
import stat
import subprocess
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4

LABEL = "com.nexus.agent-paper-recovery"


def path(root):
    return root / ".secrets/nexus-agent-paper/recovery.json"


def read(root):
    target = path(root)
    if not target.exists() and not target.is_symlink():
        return None
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "r") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > 2_000_000
        ):
            raise ValueError("AGENT_PAPER_RECOVERY_STATE_UNSAFE")
        result = json.load(stream)
    if (
        not isinstance(result, dict)
        or result.get("version") != 1
        or result.get("desired") not in {"enabled", "disabled", "maintenance"}
        or not isinstance(result.get("attempts"), list)
        or not isinstance(result.get("generation"), str)
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    UUID(result["generation"])

    def finite(value):
        return type(value) in {int, float} and math.isfinite(value) and value >= 0

    if (
        not finite(result.get("checked_at"))
        or not finite(result.get("next_start_at", 0))
        or (result.get("healthy_since") is not None and not finite(result["healthy_since"]))
        or type(result.get("failure_streak")) is not int
        or not 0 <= result["failure_streak"] <= 3
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    identifiers = []
    for attempt in result["attempts"]:
        if not isinstance(attempt, dict) or not finite(attempt.get("at")):
            raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
        identifiers.append(str(UUID(attempt["id"])))
        UUID(attempt["generation"])
        UUID(attempt["boot"])
    if len(set(identifiers)) != len(identifiers) or (
        result.get("attempt") is not None and result["attempt"] not in result["attempts"]
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    if (
        result.get("active_attempt") is not None
        and result["active_attempt"] not in result["attempts"]
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    if (
        result.get("rejected_start") is not None
        and result["rejected_start"] not in result["attempts"]
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    UUID(result["boot"])
    instance = result.get("instance")
    if instance is not None and (
        not isinstance(instance, dict)
        or type(instance.get("pid")) is not int
        or instance["pid"] <= 0
        or not isinstance(instance.get("started"), str)
        or not instance["started"]
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    return result


def write(root, state):
    # Caller holds the global service-control lock. Commit before launch or stop.
    target = path(root)
    with NamedTemporaryFile(dir=target.parent, prefix="recovery.", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            os.fchmod(stream.fileno(), 0o600)
            stream.write((json.dumps(state, sort_keys=True) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, target)
        descriptor = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def binding(service, root, env_file, project):
    revision = service._checkout_revision(root)
    if revision is None:
        raise ValueError("AGENT_PAPER_RECOVERY_VERSION_INVALID")
    return {
        "root": str(root),
        "env_file": str(env_file),
        "project": project,
        "revision": revision,
        "config_hash": hashlib.sha256(env_file.read_bytes()).hexdigest(),
    }


def bound(service, root, env_file, project, state):
    if any(
        state.get(key) != value for key, value in binding(service, root, env_file, project).items()
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_BINDING_CHANGED")


def boot_id():
    value = subprocess.run(
        ["sysctl", "-n", "kern.bootsessionuuid"],
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    ).stdout.strip()
    return str(UUID(value))


def process(pid):
    if type(pid) is not int or pid <= 0:
        raise ValueError("AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED")
    result = subprocess.run(
        ["/bin/ps", "-p", str(pid), "-o", "uid=,lstart="],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode == 1 and not result.stdout.strip():
        return None
    pieces = result.stdout.strip().split(None, 1)
    if result.returncode or len(pieces) != 2 or pieces[0] != str(os.getuid()):
        raise ValueError("AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED")
    return {"pid": pid, "started": pieces[1]}


def identity(service, root, env_file, project):
    state, _ = service._launchctl_inspection("launchctl", root, env_file, project)
    if state != "running":
        raise ValueError("AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED")
    result = subprocess.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{service.LABEL}"],
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )
    pids = re.findall(r"^\s*pid = ([1-9][0-9]*)\s*$", result.stdout, re.MULTILINE)
    if len(pids) != 1:
        raise ValueError("AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED")
    value = process(int(pids[0]))
    if value is None:
        raise ValueError("AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED")
    return value


def engine_check(service, root, env_file, argument):
    result = subprocess.run(
        [
            service._runtime_executable(),
            str(root / ".venv/bin/nexus-engine-agent-paper-loop"),
            argument,
            *service._engine_arguments(root, env_file),
        ],
        cwd=root,
        env=service._engine_environment(root),
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise ValueError("AGENT_PAPER_RECOVERY_INSPECTION_FAILED")
    return value


def healthy(service, root, env_file):
    value = engine_check(service, root, env_file, "--service-status")
    verified = (
        value.get("heartbeat", {}).get("status") == "fresh"
        and value.get("heartbeat", {}).get("release_revision") == service._checkout_revision(root)
        and isinstance(value.get("heartbeat", {}).get("migration_head"), str)
        and value.get("preflight", {}).get("status") == "ready"
    )
    return value["heartbeat"] if verified else None


def install(service, root, env_file, project):
    directory = Path.home() / "Library/LaunchAgents"
    directory.mkdir(parents=True, exist_ok=True)
    if directory.is_symlink() or directory.stat().st_uid != os.getuid():
        raise ValueError("AGENT_PAPER_RECOVERY_DIRECTORY_UNSAFE")
    target = directory / f"{LABEL}.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": [
            service._runtime_executable(),
            str(root / "scripts/macos/agent-paper-loop-service.py"),
            "recover",
            "--repo-root",
            str(root),
            "--env-file",
            str(env_file),
            "--project-name",
            project,
        ],
        "WorkingDirectory": service._service_working_directory(),
        "RunAtLoad": True,
        "StartInterval": 60,
        "KeepAlive": False,
    }
    if target.exists() or target.is_symlink():
        if target.is_symlink() or plistlib.loads(target.read_bytes()) != payload:
            raise ValueError("AGENT_PAPER_RECOVERY_OWNER_MISMATCH")
    else:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            plistlib.dump(payload, stream)
            stream.flush()
            os.fsync(stream.fileno())
    label = f"gui/{os.getuid()}/{LABEL}"
    inspected = subprocess.run(["launchctl", "print", label], capture_output=True, timeout=5)
    if inspected.returncode == 113:
        subprocess.run(
            ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(target)],
            capture_output=True,
            timeout=10,
            check=True,
        )
    elif inspected.returncode != 0:
        raise ValueError("AGENT_PAPER_RECOVERY_REGISTRATION_UNCONFIRMED")
    service._launchctl_inspection(
        "launchctl",
        root,
        env_file,
        project,
        label=LABEL,
        expected_arguments=payload["ProgramArguments"],
    )


def enable(service, root, env_file, project, *, report=True):
    service._private_directory(root)
    previous = read(root)
    if previous is not None and previous.get("attempt") is not None:
        raise ValueError("AGENT_PAPER_RECOVERY_START_UNCONFIRMED")
    status, last_exit = service._launchctl_inspection("launchctl", root, env_file, project)
    if status != "running":
        current_boot = boot_id()
        prior_boot_unused = (
            previous is not None
            and previous.get("boot") != current_boot
            and previous.get("unconfirmed_exit") is None
            and last_exit != 75
            and isinstance(previous.get("exit_reconciliation"), dict)
            and previous["exit_reconciliation"].get("consumed_by") is None
        )
        rejected = previous is not None and rejected_start_proven(
            root, previous, previous.get("rejected_start"), boot_id(), status, last_exit
        )
        reconciled = (
            previous is not None
            and not prior_boot_unused
            and previous.get("exit_reconciliation") is not None
            and service._reconciliation_module().qualified_exit(
                service,
                service._recovery_module(),
                root,
                env_file,
                project,
                previous,
                status,
                last_exit,
            )
        )
        if previous is None or not (
            previous.get("stopped_confirmed") or rejected or reconciled or prior_boot_unused
        ):
            raise ValueError("AGENT_PAPER_RECOVERY_EXIT_UNCONFIRMED")
        if any(
            previous.get(key) != value
            for key, value in {
                "root": str(root),
                "env_file": str(env_file),
                "project": project,
            }.items()
        ):
            raise ValueError("AGENT_PAPER_RECOVERY_BINDING_CHANGED")
        checked = engine_check(service, root, env_file, "--service-recovery-check")
        if checked.get("status") != "ready":
            raise ValueError("AGENT_PAPER_RECOVERY_DEPENDENCY_BLOCKED")
        if not isinstance(checked.get("migration_head"), str):
            raise ValueError("AGENT_PAPER_RECOVERY_DEPENDENCY_BLOCKED")
        if prior_boot_unused:
            current_binding = binding(service, root, env_file, project)
            if (
                previous.get("config_hash") != current_binding["config_hash"]
                or checked["migration_head"] != previous["schema"]
            ):
                raise ValueError("AGENT_PAPER_RECOVERY_BINDING_CHANGED")
            service._reconciliation_module().archive_unused_prior_boot(
                root, previous, current_boot, observed_at=time.time()
            )
        previous.update(binding(service, root, env_file, project))
        previous.update(
            desired="disabled", generation=str(uuid4()), schema=checked["migration_head"]
        )
        if reconciled:
            previous["exit_reconciliation"]["enable_generation"] = previous["generation"]
        write(root, previous)
        install(service, root, env_file, project)
        previous["desired"] = "enabled"
        write(root, previous)
        return recover(service, root, env_file, project, report=report)
    owner = identity(service, root, env_file, project)
    health = healthy(service, root, env_file)
    if health is None:
        raise ValueError("AGENT_PAPER_RECOVERY_HEALTH_UNCONFIRMED")
    if previous is not None and previous["desired"] == "enabled":
        bound(service, root, env_file, project, previous)
        install(service, root, env_file, project)
        if report:
            service._emit({"status": "enabled", "label": LABEL})
        return 0
    state = enrolled_state(service, root, env_file, project, owner, health, previous)
    # Install in disabled state: bootstrap's immediate tick cannot start work.
    write(root, {**state, "desired": "disabled"})
    install(service, root, env_file, project)
    write(root, state)
    if report:
        service._emit({"status": "enabled", "label": LABEL})
    return 0


def enrolled_state(service, root, env_file, project, owner, health, previous):
    now = time.time()
    state = {
        **binding(service, root, env_file, project),
        "version": 1,
        "schema": health["migration_head"],
        "generation": str(uuid4()),
        "desired": "enabled",
        "boot": boot_id(),
        "instance": owner,
        "attempts": [] if previous is None else previous["attempts"],
        "attempt_resolutions": [] if previous is None else previous.get("attempt_resolutions", []),
        "checked_at": now,
        "healthy_since": now,
        "failure_streak": 0 if previous is None else previous["failure_streak"],
        "next_start_at": 0 if previous is None else previous.get("next_start_at", 0),
        "reason": "HEALTHY",
        "attempt": None,
    }
    if previous is not None and all(
        previous.get(key) == state.get(key)
        for key in ("instance", "boot", "revision", "config_hash", "schema")
    ):
        state["active_attempt"] = previous.get("active_attempt")
    if previous is not None:
        for key in ("exit_reconciliation", "exit_reconciliation_history"):
            if key in previous:
                state[key] = previous[key]
    if previous is not None and previous.get("unconfirmed_exit") is not None:
        state["unconfirmed_exit"] = previous["unconfirmed_exit"]
    return state


def maintenance(service, root, env_file, project):
    service._private_directory(root)
    previous = read(root)
    if previous is not None:
        disable(root, maintenance=True)
        return
    owner = identity(service, root, env_file, project)
    health = healthy(service, root, env_file)
    if health is None:
        raise ValueError("AGENT_PAPER_RECOVERY_HEALTH_UNCONFIRMED")
    state = enrolled_state(service, root, env_file, project, owner, health, None)
    state["desired"] = "maintenance"
    write(root, state)


def disable(root, *, maintenance=False):
    state = read(root)
    if state is not None:
        state.update(desired="maintenance" if maintenance else "disabled", generation=str(uuid4()))
        write(root, state)


def record_stop(service, root, *, observed_clean_exit):
    state = read(root)
    if state is None:
        return
    current_boot = boot_id()
    if not observed_clean_exit and not (
        state.get("stopped_confirmed") is True
        and state.get("stop_proof", {}).get("boot") == current_boot
        and state.get("stop_proof", {}).get("instance") == state.get("instance")
        and state.get("stop_proof", {}).get("exit") == 0
    ):
        raise ValueError("AGENT_PAPER_SERVICE_STOP_UNCONFIRMED")
    pending = state.get("attempt")
    if pending is not None:
        receipt = activation_receipt(root, state, pending)
        if (
            receipt is not None
            and pending.get("boot") == current_boot
            and receipt.get("phase") in {"activated", "exited", "rejected"}
            and type(receipt.get("pid")) is int
            and isinstance(receipt.get("process_started"), str)
            and receipt["process_started"]
            and process(receipt["pid"]) is None
        ):
            if state.get("exit_reconciliation") is not None:
                service._reconciliation_module().archive_consumed(
                    root,
                    state,
                    pending,
                    reason="SUCCESSOR_STOPPED",
                    observed_at=time.time(),
                    confirmed_instance={
                        "pid": receipt["pid"],
                        "started": receipt["process_started"],
                    },
                )
            state["instance"] = {"pid": receipt["pid"], "started": receipt["process_started"]}
            state["last_resolved_attempt"] = pending
            state["attempt"] = None
    owner = state.get("instance")
    if owner is None or process(owner["pid"]) is not None:
        raise ValueError("AGENT_PAPER_SERVICE_STOP_UNCONFIRMED")
    state["stopped_confirmed"] = True
    state["stop_proof"] = {
        "boot": current_boot,
        "instance": owner,
        "generation": state["generation"],
        "exit": 0,
    }
    write(root, state)


def summary(root):
    state = read(root)
    if state is None:
        return {"desired": "disabled", "reason": "NOT_CONFIGURED"}
    return {
        key: state.get(key)
        for key in ("desired", "reason", "checked_at", "next_start_at", "attempt")
    }


def activation_receipt(root, state, attempt):
    target = path(root).parent / f"recovery-activation-{attempt['id']}.json"
    if not target.exists() and not target.is_symlink():
        return None
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > 65536
        ):
            raise ValueError("AGENT_PAPER_RECOVERY_STATE_UNSAFE")
        receipt = json.load(stream)
    if (
        not isinstance(receipt, dict)
        or receipt.get("version") != 1
        or receipt.get("attempt_id") != attempt["id"]
        or receipt.get("generation") != attempt["generation"]
        or receipt.get("boot") != attempt["boot"]
        or receipt.get("revision") != attempt.get("revision", state["revision"])
    ):
        raise ValueError("AGENT_PAPER_RECOVERY_STATE_INVALID")
    return receipt


def rejected_start_proven(root, state, attempt, current_boot, status, last_exit):
    """A current revoked start can prove non-activation, never business settlement."""
    if (
        attempt is None
        or attempt not in state["attempts"]
        or attempt.get("generation") == state["generation"]
        or attempt.get("boot") != current_boot
        or status != "inactive"
        or type(last_exit) is not int
        or last_exit == 75
        or state.get("unconfirmed_exit") is not None
    ):
        return False
    receipt = activation_receipt(root, state, attempt)
    return (
        receipt is not None
        and receipt.get("phase") == "rejected"
        and receipt.get("reason") == "PRE_ACTIVATION_REJECTED"
        and type(receipt.get("pid")) is int
        and isinstance(receipt.get("process_started"), str)
        and bool(receipt["process_started"])
        and process(receipt["pid"]) is None
        and (state.get("instance") is None or process(state["instance"]["pid"]) is None)
    )


def recover(service, root, env_file, project, *, report=True):
    state = read(root)
    if state is None or state["desired"] != "enabled":
        if state is not None and state.get("attempt") is not None:
            bound(service, root, env_file, project, state)
            status, last_exit = service._launchctl_inspection("launchctl", root, env_file, project)
            now = time.time()
            if now < state["checked_at"]:
                raise ValueError("AGENT_PAPER_RECOVERY_CLOCK_REVERSED")
            if last_exit == 75:
                state["unconfirmed_exit"] = {
                    "boot": boot_id(),
                    "instance": state.get("instance"),
                    "exit": 75,
                }
                write(root, state)
            if rejected_start_proven(root, state, state["attempt"], boot_id(), status, last_exit):
                pending = state["attempt"]
                if state.get("exit_reconciliation") is not None:
                    service._reconciliation_module().archive_consumed(
                        root, state, pending, reason="REVOKED_BEFORE_ACTIVATION", observed_at=now
                    )
                state.setdefault("attempt_resolutions", []).append(
                    {
                        "attempt_id": pending["id"],
                        "generation": state["generation"],
                        "at": now,
                        "reason": "REVOKED_BEFORE_ACTIVATION",
                        "exit": last_exit,
                    }
                )
                state.update(
                    attempt=None,
                    rejected_start=pending,
                    last_resolved_attempt=pending,
                    last_attempt_resolution="REVOKED_BEFORE_ACTIVATION",
                    reason="REVOKED_BEFORE_ACTIVATION",
                    checked_at=now,
                )
                write(root, state)
        if report:
            service._emit({"status": "disabled", "label": LABEL})
        return 0
    bound(service, root, env_file, project, state)
    now = time.time()
    if now < state["checked_at"]:
        raise ValueError("AGENT_PAPER_RECOVERY_CLOCK_REVERSED")
    current_boot = boot_id()
    status, last_exit = service._launchctl_inspection("launchctl", root, env_file, project)

    def finish(reason):
        state.update(reason=reason, checked_at=now)
        write(root, state)
        if report:
            service._emit({"status": "observed", "reason": reason, "label": LABEL})
        return 0

    if status == "running":
        owner = identity(service, root, env_file, project)
        if state.get("attempt") is not None:
            receipt = activation_receipt(root, state, state["attempt"])
            if (
                receipt is None
                or receipt.get("phase") != "activated"
                or receipt.get("pid") != owner["pid"]
                or receipt.get("process_started") != owner["started"]
                or owner == state["attempt"].get("old_instance")
            ):
                return finish("START_RESULT_UNKNOWN")
        try:
            verified = healthy(service, root, env_file)
        except (OSError, ValueError, subprocess.SubprocessError):
            state["healthy_since"] = None
            return finish("HEALTH_UNVERIFIED")
        if not verified:
            state["healthy_since"] = None
            return finish("RUNNING_UNVERIFIED")
        if state.get("attempt") is not None and state.get("exit_reconciliation") is not None:
            service._reconciliation_module().archive_consumed(
                root,
                state,
                state["attempt"],
                reason="SUCCESSOR_ACTIVATED",
                observed_at=now,
                confirmed_instance=owner,
            )
        if state.get("instance") != owner or state["boot"] != current_boot:
            state["healthy_since"] = now
            state["active_attempt"] = None
        if state.get("attempt") is not None:
            state["active_attempt"] = state["attempt"]
        state.update(instance=owner, boot=current_boot, attempt=None)
        since = state.get("healthy_since")
        if since is None:
            state["healthy_since"] = now
        elif now - since >= 600:
            state["failure_streak"] = 0
        return finish("HEALTHY")
    if last_exit == 75:
        state["unconfirmed_exit"] = {
            "boot": current_boot,
            "instance": state.get("instance"),
            "exit": 75,
        }
    if state.get("unconfirmed_exit") is not None:
        return finish("EXIT_UNCONFIRMED")
    pending = state.get("attempt")
    pending_prior_boot = pending is not None and pending["boot"] != current_boot
    attempt_to_inspect = pending or state.get("active_attempt")
    # A current instance retains its activation identity across management toggles.
    # Arbitrary historical attempts never classify another instance's exit.
    exit_receipt = (
        activation_receipt(root, state, attempt_to_inspect)
        if attempt_to_inspect is not None
        and (
            (pending is not None and attempt_to_inspect.get("generation") == state["generation"])
            or (pending is None and attempt_to_inspect == state.get("active_attempt"))
        )
        and attempt_to_inspect.get("boot") == current_boot
        else None
    )
    qualified_exit = (
        exit_receipt is not None
        and exit_receipt.get("phase") in {"exited", "rejected"}
        and exit_receipt.get("reason")
        in {
            "stop_requested",
            "stop_calendar_unavailable",
            "database_unavailable",
            "PRE_ACTIVATION_REJECTED",
        }
        and type(exit_receipt.get("pid")) is int
        and isinstance(exit_receipt.get("process_started"), str)
        and bool(exit_receipt["process_started"])
        and (
            state.get("attempt") is not None
            or state.get("instance")
            == {
                "pid": exit_receipt["pid"],
                "started": exit_receipt["process_started"],
            }
        )
        and process(exit_receipt["pid"]) is None
    )
    if pending is None and state.get("rejected_start") is not None:
        qualified_exit = rejected_start_proven(
            root, state, state["rejected_start"], current_boot, status, last_exit
        )
    reconciled = (
        pending is None
        and current_boot == state["boot"]
        and state.get("exit_reconciliation") is not None
        and service._reconciliation_module().qualified_exit(
            service,
            service._recovery_module(),
            root,
            env_file,
            project,
            state,
            status,
            last_exit,
            require_enable=True,
        )
    )
    qualified_exit = qualified_exit or reconciled
    if pending is not None and not qualified_exit and not pending_prior_boot:
        return finish("START_RESULT_UNKNOWN")
    if current_boot == state["boot"]:
        old = state.get("instance")
        if old is None or process(old["pid"]) is not None:
            return finish("EXIT_UNCONFIRMED")
        stopped = (
            status == "stopped"
            and state.get("stopped_confirmed") is True
            and state.get("stop_proof", {}).get("boot") == current_boot
            and state.get("stop_proof", {}).get("instance") == old
        )
        if not stopped and not qualified_exit and (status != "inactive" or last_exit != 0):
            return finish("EXIT_UNCLASSIFIED")
    recent = [item for item in state["attempts"] if item["at"] > now - 3600]
    if len(recent) >= 3 or now < state.get("next_start_at", 0):
        return finish("RATE_LIMITED")
    try:
        check = engine_check(service, root, env_file, "--service-recovery-check")
    except (OSError, ValueError, subprocess.SubprocessError):
        return finish("INSPECTION_UNAVAILABLE")
    if check.get("status") != "ready" or check.get("migration_head") != state["schema"]:
        return finish("BUSINESS_OR_DEPENDENCY_BLOCKED")
    # A changed boot ends a local process lifetime, never its unknown business effects.
    # Only the successful full read-only admission above permits a new attempt.
    if pending is not None:
        if state.get("exit_reconciliation") is not None:
            service._reconciliation_module().archive_consumed(
                root,
                state,
                pending,
                reason="PRIOR_BOOT_ENDED" if pending_prior_boot else "EXIT_CONFIRMED",
                observed_at=now,
            )
        state["last_resolved_attempt"] = pending
        state["last_attempt_resolution"] = (
            "PRIOR_BOOT_ENDED" if pending_prior_boot else "EXIT_CONFIRMED"
        )
        state["attempt"] = None
    if current_boot != state["boot"] and state.get("exit_reconciliation") is not None:
        service._reconciliation_module().archive_unused_prior_boot(
            root, state, current_boot, observed_at=now
        )
    # No engine lock is held across launchctl: child obtains it and rechecks.
    attempt = {
        "id": str(uuid4()),
        "at": now,
        "boot": current_boot,
        "old_instance": state.get("instance"),
        "old_exit": last_exit,
        "generation": state["generation"],
        "revision": state["revision"],
    }
    if reconciled:
        pointer = state["exit_reconciliation"]
        attempt["exit_reconciliation_sha256"] = pointer["evidence_sha256"]
        pointer["consumed_by"] = attempt["id"]
    state["attempts"].append(attempt)
    state["attempt"] = attempt
    state.pop("rejected_start", None)
    streak = min(state["failure_streak"], 2)
    state.update(
        failure_streak=streak + 1,
        next_start_at=now + (60, 300, 900)[streak],
        reason="START_PENDING",
        checked_at=now,
        healthy_since=None,
        stopped_confirmed=False,
    )
    write(root, state)
    if status == "stopped":
        directory = service._private_directory(root)
        service_path = service._write_service_plist(
            root, directory, service._private_log_directory(), env_file, project
        )
        subprocess.run(
            ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(service_path)],
            capture_output=True,
            timeout=10,
            check=True,
        )
    subprocess.run(
        ["launchctl", "kickstart", f"gui/{os.getuid()}/{service.LABEL}"],
        capture_output=True,
        timeout=10,
        check=False,
    )
    return finish("START_PENDING")


def activation_arguments(service, root, env_file):
    state = read(root)
    if state is None:
        return []
    if state["desired"] != "enabled":
        raise ValueError("AGENT_PAPER_RECOVERY_DISABLED")
    bound(service, root, env_file, state["project"], state)
    attempt = state.get("attempt")
    if not isinstance(attempt, dict) or attempt.get("generation") != state["generation"]:
        raise ValueError("AGENT_PAPER_RECOVERY_START_UNCONFIRMED")
    return ["--recovery-generation", state["generation"], "--recovery-attempt", attempt["id"]]
