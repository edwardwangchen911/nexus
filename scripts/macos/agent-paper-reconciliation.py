"""Explicit local lifecycle evidence; never resolves historical business effects."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import UUID

_PREFIX = "AGENT_PAPER_EXIT_RECONCILIATION_"
SAFE_CODES = frozenset(
    _PREFIX + suffix
    for suffix in (
        "BINDING_CHANGED",
        "STATE_INVALID",
        "PROCESS_PRESENT",
        "MAINTENANCE_REQUIRED",
        "PROOF_INVALID",
        "BUSINESS_BLOCKED",
        "SCHEMA_MISMATCH",
        "UNAVAILABLE",
    )
)


def _reject(suffix):
    raise ValueError(_PREFIX + suffix)


def _read(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > 2_000_000
        ):
            _reject("PROOF_INVALID")
        raw = stream.read()
    value = json.loads(raw)
    if not isinstance(value, dict):
        _reject("PROOF_INVALID")
    return value, hashlib.sha256(raw).hexdigest()


def _producer(service, recovery, root, env_file, project, state):
    if (
        state is None
        or state.get("attempt") is not None
        or state.get("unconfirmed_exit") is not None
    ):
        _reject("STATE_INVALID")
    current = recovery.binding(service, root, env_file, project)
    if any(state.get(k) != current[k] for k in ("root", "env_file", "project", "config_hash")):
        _reject("BINDING_CHANGED")
    attempt = state.get("active_attempt")
    owner = state.get("instance")
    if (
        not isinstance(attempt, dict)
        or not isinstance(owner, dict)
        or state.get("boot") != recovery.boot_id()
        or attempt.get("boot") != state["boot"]
    ):
        _reject("STATE_INVALID")
    receipt_path = root / ".secrets/nexus-agent-paper" / f"recovery-activation-{attempt['id']}.json"
    receipt, receipt_hash = _read(receipt_path)
    if (
        recovery.activation_receipt(root, state, attempt) != receipt
        or receipt.get("phase") not in {"activated", "exited"}
        or (receipt.get("phase"), receipt.get("reason"))
        not in {("activated", "READY"), ("exited", "unclassified")}
        or receipt.get("pid") != owner.get("pid")
        or receipt.get("process_started") != owner.get("started")
        or receipt.get("revision") != attempt.get("revision")
    ):
        _reject("BINDING_CHANGED")
    if recovery.process(owner["pid"]) is not None:
        _reject("PROCESS_PRESENT")
    return {
        "root": str(root),
        "env_file": str(env_file),
        "project": project,
        "config_hash": state["config_hash"],
        "schema": state["schema"],
        "boot": state["boot"],
        "instance": owner,
        "active_attempt": attempt,
        "receipt_sha256": receipt_hash,
    }


def capture_exit(service, recovery, root, env_file, project):
    service._private_directory(root)
    state = recovery.read(root)
    if state is None or state.get("desired") not in {"disabled", "maintenance"}:
        _reject("MAINTENANCE_REQUIRED")
    producer = _producer(service, recovery, root, env_file, project, state)
    status, exit_code = service._launchctl_inspection("launchctl", root, env_file, project)
    if status != "inactive" or type(exit_code) is not int or exit_code in {0, 75}:
        _reject("STATE_INVALID")
    return {
        "producer": producer,
        "exit_code": exit_code,
        "reconciliation_revision": service._checkout_revision(root),
    }


def _proof_path(root, attempt_id):
    if str(UUID(attempt_id)) != attempt_id:
        _reject("PROOF_INVALID")
    return root / ".secrets/nexus-agent-paper" / f"recovery-reconciliation-{attempt_id}.json"


def _sync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish(path, value):
    raw = (json.dumps(value, sort_keys=True) + "\n").encode()
    with NamedTemporaryFile(dir=path.parent, prefix="reconciliation.", delete=False) as stream:
        temporary = Path(stream.name)
        os.fchmod(stream.fileno(), 0o600)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        # Exclusive link publishes the complete file and cannot replace old evidence.
        os.link(temporary, path, follow_symlinks=False)
    finally:
        temporary.unlink(missing_ok=True)
    _sync(path.parent)


def _check_published_evidence(evidence, capture):
    historical = evidence.get("capture", {})
    if (
        historical.get("producer") != capture["producer"]
        or historical.get("exit_code") != capture["exit_code"]
        or evidence.get("version") != 1
        or evidence.get("historical_effects") != "not_determined"
        or evidence.get("business_disposition") != "unchanged"
        or evidence.get("execution_locked") is not True
        or evidence.get("replay_authorized") is not False
    ):
        _reject("PROOF_INVALID")


def _read_publication(target, capture, *, repair):
    """Only complete this publisher's exact interrupted link; general reads stay strict."""
    info = target.lstat()
    if info.st_nlink == 1:
        evidence, digest = _read(target)
        _check_published_evidence(evidence, capture)
        return evidence, digest
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 2
        or info.st_size > 2_000_000
    ):
        _reject("PROOF_INVALID")
    identity = (info.st_dev, info.st_ino)
    matching = [
        candidate
        for candidate in target.parent.glob("reconciliation.*")
        if (candidate.lstat().st_dev, candidate.lstat().st_ino) == identity
    ]
    if len(matching) != 1:
        _reject("PROOF_INVALID")
    temporary = matching[0]
    with os.fdopen(os.open(target, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        opened = os.fstat(stream.fileno())
        if opened != info:
            _reject("PROOF_INVALID")
        raw = stream.read()
        evidence = json.loads(raw)
        if not isinstance(evidence, dict):
            _reject("PROOF_INVALID")
        _check_published_evidence(evidence, capture)
        # No other hardlink is accepted, nor an unrelated temporary filename/inode.
        current = temporary.lstat()
        if (
            (current.st_dev, current.st_ino) != identity
            or not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 2
            or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o600
            or target.lstat() != opened
        ):
            _reject("PROOF_INVALID")
        if repair:
            temporary.unlink()
            os.fsync(stream.fileno())
            _sync(target.parent)
    if repair:
        return _read(target)
    return evidence, hashlib.sha256(raw).hexdigest()


def publish_exit(service, recovery, root, env_file, project, *, capture, readiness, check_only):
    if (
        readiness.get("status") != "ready"
        or readiness.get("blocking_codes") != []
        or readiness.get("execution_locked") is not True
        or readiness.get("external_requests") != 0
    ):
        _reject("BUSINESS_BLOCKED")
    if readiness.get("migration_head") != capture["producer"]["schema"]:
        _reject("SCHEMA_MISMATCH")
    if capture_exit(service, recovery, root, env_file, project) != capture:
        _reject("BINDING_CHANGED")
    attempt_id = capture["producer"]["active_attempt"]["id"]
    target = _proof_path(root, attempt_id)
    result = {
        "status": "eligible_at_check",
        "check_only": check_only,
        "current_eligible": True,
        "execution_locked": True,
        "external_requests": 0,
        "blocking_codes": [],
    }
    existing = target.exists() or target.is_symlink()
    state = recovery.read(root)
    if state.get("exit_reconciliation") is not None and not existing:
        _reject("PROOF_INVALID")
    if existing:
        evidence, digest = _read_publication(target, capture, repair=not check_only)
    if check_only:
        return result
    if existing:
        # A previous interrupted publication is not trusted merely because it exists.
        _sync(target)
        _sync(target.parent)
    else:
        evidence = {
            "version": 1,
            "capture": capture,
            "observed_readiness": readiness,
            "historical_effects": "not_determined",
            "business_disposition": "unchanged",
            "execution_locked": True,
            "replay_authorized": False,
        }
        _publish(target, evidence)
        _, digest = _read(target)
    if capture_exit(service, recovery, root, env_file, project) != capture:
        _reject("BINDING_CHANGED")
    state = recovery.read(root)
    pointer = {"attempt_id": attempt_id, "evidence_sha256": digest, "consumed_by": None}
    previous = state.get("exit_reconciliation")
    if previous is not None:
        if not isinstance(previous, dict) or any(
            previous.get(key) != value for key, value in pointer.items()
        ):
            _reject("PROOF_INVALID")
        # Management generations may change; the immutable old producer does not.
        pointer = previous
    state["exit_reconciliation"] = pointer
    recovery.write(root, state)
    _sync(recovery.path(root).parent)
    return {
        **result,
        "status": "already_reconciled" if existing else "reconciled",
        "proof_id": attempt_id,
        "proof_hash": digest,
    }


def qualified_exit(
    service,
    recovery,
    root,
    env_file,
    project,
    state,
    status,
    exit_code,
    *,
    require_enable=False,
    allow_retirement_pending=False,
):
    pointer = state.get("exit_reconciliation")
    if pointer is None:
        return False
    if not isinstance(pointer, dict):
        _reject("PROOF_INVALID")
    if pointer.get("consumed_by") is not None:
        return False
    evidence, digest = _read(_proof_path(root, pointer["attempt_id"]))
    if digest != pointer.get("evidence_sha256") or evidence.get("version") != 1:
        _reject("PROOF_INVALID")
    capture = evidence.get("capture", {})
    if (
        capture.get("producer") != _producer(service, recovery, root, env_file, project, state)
        or pointer["attempt_id"] != state["active_attempt"]["id"]
        or evidence.get("historical_effects") != "not_determined"
        or evidence.get("business_disposition") != "unchanged"
        or evidence.get("execution_locked") is not True
        or evidence.get("replay_authorized") is not False
    ):
        _reject("PROOF_INVALID")
    if require_enable and pointer.get("enable_generation") != state["generation"]:
        return False
    if status == "stopped":
        valid = pointer.get("registration_retired") is True or (
            allow_retirement_pending and pointer.get("retirement_requested") is True
        )
    else:
        valid = status == "inactive" and exit_code == capture.get("exit_code")
    if not valid or capture.get("exit_code") in {None, 0, 75}:
        _reject("STATE_INVALID")
    return True


def authorize_retirement(service, recovery, root, env_file, project, status, exit_code):
    state = recovery.read(root)
    if not qualified_exit(
        service,
        recovery,
        root,
        env_file,
        project,
        state,
        status,
        exit_code,
        allow_retirement_pending=True,
    ):
        return False
    checked = recovery.engine_check(service, root, env_file, "--service-recovery-check")
    if checked.get("status") != "ready" or checked.get("migration_head") != state["schema"]:
        _reject("BUSINESS_BLOCKED")
    again, observed = service._launchctl_inspection("launchctl", root, env_file, project)
    if (again, observed) != (status, exit_code):
        _reject("BINDING_CHANGED")
    if recovery.read(root) != state:
        _reject("BINDING_CHANGED")
    state["exit_reconciliation"]["retirement_requested"] = True
    recovery.write(root, state)
    _sync(recovery.path(root).parent)
    return True


def confirm_retirement(service, recovery, root, env_file, project):
    state = recovery.read(root)
    status, exit_code = service._launchctl_inspection("launchctl", root, env_file, project)
    if status != "stopped" or not qualified_exit(
        service,
        recovery,
        root,
        env_file,
        project,
        state,
        status,
        exit_code,
        allow_retirement_pending=True,
    ):
        _reject("STATE_INVALID")
    state["exit_reconciliation"]["registration_retired"] = True
    recovery.write(root, state)
    _sync(recovery.path(root).parent)


def validate_pending_activation(root, state, attempt):
    """Recheck original evidence after the child owns the lifetime lock."""
    expected = attempt.get("exit_reconciliation_sha256")
    if expected is None:
        return
    pointer = state.get("exit_reconciliation")
    if (
        not isinstance(pointer, dict)
        or pointer.get("evidence_sha256") != expected
        or pointer.get("consumed_by") != attempt.get("id")
        or pointer.get("enable_generation") != attempt.get("generation")
    ):
        _reject("PROOF_INVALID")
    if attempt.get("old_instance") != state.get("instance"):
        _reject("BINDING_CHANGED")
    _validate_original(root, state, pointer)


def _validate_original(root, state, pointer):
    evidence, digest = _read(_proof_path(root, pointer["attempt_id"]))
    producer = evidence.get("capture", {}).get("producer", {})
    if (
        digest != pointer.get("evidence_sha256")
        or producer.get("active_attempt") != state.get("active_attempt")
        or producer.get("instance") != state.get("instance")
        or any(
            producer.get(k) != state.get(k)
            for k in ("root", "env_file", "project", "config_hash", "schema", "boot")
        )
    ):
        _reject("BINDING_CHANGED")
    receipt_path = (
        root
        / ".secrets/nexus-agent-paper"
        / ("recovery-activation-" + pointer["attempt_id"] + ".json")
    )
    _, receipt_hash = _read(receipt_path)
    if receipt_hash != producer.get("receipt_sha256"):
        _reject("BINDING_CHANGED")


def archive_consumed(root, state, attempt, *, reason, observed_at, confirmed_instance=None):
    """Retire authority with its confirmed consumer; keep the immutable old proof."""
    pointer = state.get("exit_reconciliation")
    if pointer is None or pointer.get("consumed_by") != attempt.get("id"):
        return
    validate_pending_activation(root, state, attempt)
    _archive_pointer(
        state,
        pointer,
        reason,
        observed_at,
        consumer_attempt=attempt,
        confirmed_instance=confirmed_instance,
    )


def _archive_pointer(state, pointer, reason, observed_at, **facts):
    history = state.setdefault("exit_reconciliation_history", [])
    if not isinstance(history, list) or any(
        not isinstance(item, dict) or item.get("attempt_id") == pointer["attempt_id"]
        for item in history
    ):
        _reject("PROOF_INVALID")
    history.append({**pointer, "archived_at": observed_at, "archive_reason": reason, **facts})
    del state["exit_reconciliation"]


def archive_unused_prior_boot(root, state, current_boot, *, observed_at):
    """Old boot retirement is audit only and never consumes a restart permission."""
    pointer = state.get("exit_reconciliation")
    if pointer is None or pointer.get("consumed_by") is not None:
        return
    if current_boot == state["boot"] or state.get("attempt") is not None:
        _reject("STATE_INVALID")
    _validate_original(root, state, pointer)
    _archive_pointer(
        state,
        pointer,
        "UNUSED_PRIOR_BOOT_ENDED",
        observed_at,
        old_boot=state["boot"],
        observed_boot=current_boot,
    )
