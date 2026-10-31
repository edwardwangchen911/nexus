"""Explicit local lifecycle evidence; never resolves historical business effects."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile
from uuid import UUID

_PREFIX = "AGENT_PAPER_EXIT_RECONCILIATION_"
# A reviewed compatibility edge, not a numeric/ancestry-based permission.
_CROSS_SCHEMA_EDGE = ("0099_research_stages", "0105_comparison_preclaim")
_CROSS_SCHEMA_POLICY = "research_stages_0099_to_0105_v1"
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


def _target(evidence):
    if (
        type(evidence.get("version")) is not int
        or evidence["version"] not in {1, 2}
        or evidence.get("historical_effects") != "not_determined"
        or evidence.get("business_disposition") != "unchanged"
        or evidence.get("execution_locked") is not True
        or evidence.get("replay_authorized") is not False
    ):
        _reject("PROOF_INVALID")
    if evidence["version"] == 1:
        if "recovery_target" in evidence:
            _reject("PROOF_INVALID")
        return None
    capture = evidence.get("capture", {})
    target = evidence.get("recovery_target")
    observed = evidence.get("observed_readiness", {})
    if (
        not isinstance(capture, dict)
        or not isinstance(capture.get("producer"), dict)
        or not isinstance(observed, dict)
    ):
        _reject("PROOF_INVALID")
    revision = capture.get("reconciliation_revision")
    if (
        capture.get("producer", {}).get("schema") != _CROSS_SCHEMA_EDGE[0]
        or not isinstance(revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        or target
        != {"schema": _CROSS_SCHEMA_EDGE[1], "revision": revision, "policy": _CROSS_SCHEMA_POLICY}
        or observed.get("migration_head") != _CROSS_SCHEMA_EDGE[1]
        or observed.get("status") != "ready"
        or observed.get("blocking_codes") != []
        or observed.get("execution_locked") is not True
        or observed.get("external_requests") != 0
    ):
        _reject("PROOF_INVALID")
    return target


def _bound_proof(root, state):
    pointer = state.get("exit_reconciliation")
    if not isinstance(pointer, dict):
        _reject("PROOF_INVALID")
    evidence, digest = _read(_proof_path(root, pointer["attempt_id"]))
    if digest != pointer.get("evidence_sha256"):
        _reject("PROOF_INVALID")
    _target(evidence)
    return evidence


def _check_schema_binding(evidence, state, pointer):
    target = _target(evidence)
    binding = pointer.get("target_binding")
    if target is None or binding is None:
        if binding is not None or state.get("schema") != evidence["capture"]["producer"]["schema"]:
            _reject("BINDING_CHANGED")
        return
    # Only explicit enable records this transition, after current readiness.
    # A later maintenance toggle revokes execution, not this historical witness.
    generation = pointer.get("enable_generation")
    try:
        canonical_generation = isinstance(generation, str) and str(UUID(generation)) == generation
    except ValueError:
        canonical_generation = False
    if (
        not canonical_generation
        or binding
        != {"schema": target["schema"], "revision": target["revision"], "generation": generation}
        or state.get("schema") != target["schema"]
        or state.get("revision") != target["revision"]
    ):
        _reject("BINDING_CHANGED")


def validate_target_readiness(service, root, state, checked):
    target = _target(_bound_proof(root, state))
    expected = state["schema"] if target is None else target["schema"]
    if checked.get("status") != "ready" or checked.get("migration_head") != expected:
        _reject("BUSINESS_BLOCKED")
    if target is not None and service._checkout_revision(root) != target["revision"]:
        _reject("BINDING_CHANGED")


def bind_enabled_target(root, state):
    target = _target(_bound_proof(root, state))
    if target is None:
        return
    pointer = state["exit_reconciliation"]
    if (
        state.get("schema") != target["schema"]
        or state.get("revision") != target["revision"]
        or pointer.get("enable_generation") != state["generation"]
    ):
        _reject("BINDING_CHANGED")
    pointer["target_binding"] = {
        "schema": target["schema"],
        "revision": target["revision"],
        "generation": state["generation"],
    }


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
    if state.get("exit_reconciliation") is not None:
        evidence = _bound_proof(root, state)
        target = _target(evidence)
        if target is not None:
            _validate_original(root, state, state["exit_reconciliation"])
            if service._checkout_revision(root) != target["revision"]:
                _reject("BINDING_CHANGED")
            producer["schema"] = evidence["capture"]["producer"]["schema"]
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
    target = _target(evidence)
    if (
        historical.get("producer") != capture["producer"]
        or historical.get("exit_code") != capture["exit_code"]
        or target is not None
        and target["revision"] != capture["reconciliation_revision"]
        or evidence.get("historical_effects") != "not_determined"
        or evidence.get("business_disposition") != "unchanged"
        or evidence.get("execution_locked") is not True
        or evidence.get("replay_authorized") is not False
    ):
        _reject("PROOF_INVALID")


def _stable_stat(info):
    return tuple(
        getattr(info, key)
        for key in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    )


def _matching_pointer(state, attempt_id, digest):
    expected = {"attempt_id": attempt_id, "evidence_sha256": digest, "consumed_by": None}
    previous = state.get("exit_reconciliation")
    if previous is not None and (
        not isinstance(previous, dict)
        or any(previous.get(key) != value for key, value in expected.items())
    ):
        _reject("PROOF_INVALID")
    return expected if previous is None else previous


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
        if _stable_stat(opened) != _stable_stat(info):
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
            or _stable_stat(target.lstat()) != _stable_stat(opened)
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
    source_schema, current_schema = capture["producer"]["schema"], readiness.get("migration_head")
    recovery_target = None
    if source_schema != current_schema:
        if (source_schema, current_schema) != _CROSS_SCHEMA_EDGE:
            _reject("SCHEMA_MISMATCH")
        recovery_target = {
            "schema": current_schema,
            "revision": capture["reconciliation_revision"],
            "policy": _CROSS_SCHEMA_POLICY,
        }
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
        evidence, digest = _read_publication(target, capture, repair=False)
        if _target(evidence) != recovery_target:
            _reject("SCHEMA_MISMATCH")
        _matching_pointer(state, attempt_id, digest)
    if check_only:
        return result
    if existing:
        _read_publication(target, capture, repair=True)
        # A previous interrupted publication is not trusted merely because it exists.
        _sync(target)
        _sync(target.parent)
    else:
        evidence = {
            "version": 1 if recovery_target is None else 2,
            "capture": capture,
            "observed_readiness": readiness,
            "historical_effects": "not_determined",
            "business_disposition": "unchanged",
            "execution_locked": True,
            "replay_authorized": False,
        }
        if recovery_target is not None:
            evidence["recovery_target"] = recovery_target
        _target(evidence)
        _publish(target, evidence)
        _, digest = _read(target)
    if capture_exit(service, recovery, root, env_file, project) != capture:
        _reject("BINDING_CHANGED")
    state = recovery.read(root)
    pointer = _matching_pointer(state, attempt_id, digest)
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
    evidence, digest = _read(_proof_path(root, pointer["attempt_id"]))
    if digest != pointer.get("evidence_sha256"):
        _reject("PROOF_INVALID")
    target = _target(evidence)
    if pointer.get("consumed_by") is not None:
        return False
    _validate_original(root, state, pointer)
    if target is not None and service._checkout_revision(root) != target["revision"]:
        _reject("BINDING_CHANGED")
    capture = evidence.get("capture", {})
    producer = _producer(service, recovery, root, env_file, project, state)
    if target is not None:
        # _validate_original requires the explicit management-transition witness;
        # retain the old schema solely for comparison with the frozen source.
        producer["schema"] = capture["producer"]["schema"]
    if (
        capture.get("producer") != producer
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
    validate_target_readiness(service, root, state, checked)
    again, observed = service._launchctl_inspection("launchctl", root, env_file, project)
    if (again, observed) != (status, exit_code):
        _reject("BINDING_CHANGED")
    if recovery.read(root) != state:
        _reject("BINDING_CHANGED")
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
    pointer = state.get("exit_reconciliation")
    if expected is None:
        if pointer is not None and (
            not isinstance(pointer, dict) or pointer.get("consumed_by") is not None
        ):
            _reject("PROOF_INVALID")
        return
    if (
        not isinstance(pointer, dict)
        or pointer.get("evidence_sha256") != expected
        or pointer.get("consumed_by") != attempt.get("id")
        or pointer.get("enable_generation") != attempt.get("generation")
    ):
        _reject("PROOF_INVALID")
    if attempt.get("old_instance") != state.get("instance"):
        _reject("BINDING_CHANGED")
    evidence = _validate_original(root, state, pointer)
    target = _target(evidence)
    if target is not None and (
        attempt.get("revision") != target["revision"] or pointer.get("target_binding") is None
    ):
        _reject("BINDING_CHANGED")
    if target is not None:
        # The child's environment can still contain the old release revision
        # after a checkout changes. Recheck actual code after its lifetime lock.
        try:
            current = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            _reject("BINDING_CHANGED")
        if current.returncode != 0 or current.stdout.strip() != target["revision"]:
            _reject("BINDING_CHANGED")


def _validate_original(root, state, pointer):
    evidence, digest = _read(_proof_path(root, pointer["attempt_id"]))
    _check_schema_binding(evidence, state, pointer)
    producer = evidence.get("capture", {}).get("producer", {})
    if (
        digest != pointer.get("evidence_sha256")
        or producer.get("active_attempt") != state.get("active_attempt")
        or producer.get("instance") != state.get("instance")
        or any(
            producer.get(k) != state.get(k)
            for k in ("root", "env_file", "project", "config_hash", "boot")
        )
    ):
        _reject("BINDING_CHANGED")
    receipt_path = (
        root
        / ".secrets/nexus-agent-paper"
        / ("recovery-activation-" + pointer["attempt_id"] + ".json")
    )
    pending = receipt_path.with_suffix(".write-pending")
    if pending.exists() or pending.is_symlink():
        _reject("BINDING_CHANGED")
    _, receipt_hash = _read(receipt_path)
    if pending.exists() or pending.is_symlink() or receipt_hash != producer.get("receipt_sha256"):
        _reject("BINDING_CHANGED")
    return evidence


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


def validate_unused_prior_boot(root, state, current_boot):
    """Read-only verification leaves authority intact until enable commits."""
    pointer = state.get("exit_reconciliation")
    if pointer is None or pointer.get("consumed_by") is not None:
        return
    if current_boot == state["boot"] or state.get("attempt") is not None:
        _reject("STATE_INVALID")
    _validate_original(root, state, pointer)
    return pointer


def archive_unused_prior_boot(root, state, current_boot, *, observed_at):
    """Old boot retirement is audit only and never consumes a restart permission."""
    pointer = validate_unused_prior_boot(root, state, current_boot)
    if pointer is None:
        return
    _archive_pointer(
        state,
        pointer,
        "UNUSED_PRIOR_BOOT_ENDED",
        observed_at,
        old_boot=state["boot"],
        observed_boot=current_boot,
    )
