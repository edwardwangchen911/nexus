"""Security and lifecycle tests for Agent paper runtime facts."""

import json
import multiprocessing
import os
import stat
import subprocess
import sys
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path
from uuid import UUID

import nexus.ops.agent_paper_runtime as runtime
import pytest
from nexus.ops.agent_paper_runtime import (
    AgentPaperRuntimeDescriptor,
    ProcessIdentity,
    RuntimeEntrypoint,
    RuntimeExitReason,
    RuntimePaths,
    RuntimeRole,
    RuntimeSafetyError,
    RuntimeState,
    acquire_runtime_lock,
    classify_runtime,
    normalize_runtime_entrypoint,
    prepare_runtime_paths,
    process_identity,
    read_runtime_descriptor,
    write_runtime_descriptor,
)

ROOT = Path(__file__).parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _descriptor(
    *,
    role: RuntimeRole = RuntimeRole.WORKER,
    pid: int = 321,
    process_started_at: str = "Wed Sep 16 12:34:56 2026",
    entrypoint: RuntimeEntrypoint = RuntimeEntrypoint.WORKER,
    process_version: str = "agent-paper-loop-v1",
    state: RuntimeState = RuntimeState.RUNNING,
    exit_reason: RuntimeExitReason | None = None,
) -> AgentPaperRuntimeDescriptor:
    return AgentPaperRuntimeDescriptor(
        schema="nexus_agent_paper_runtime_v1",
        role=role,
        pid=pid,
        process_started_at=process_started_at,
        entrypoint=entrypoint,
        process_version=process_version,
        state=state,
        calendar_authority_id=UUID(int=1),
        exit_reason=exit_reason,
        updated_at=datetime(2026, 9, 16, tzinfo=UTC),
    )


@pytest.fixture
def runtime_paths(tmp_path: Path) -> RuntimePaths:
    return prepare_runtime_paths(tmp_path)


def _write_raw_descriptor(path: Path, payload: dict[str, object] | bytes) -> None:
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    path.write_bytes(raw)
    path.chmod(0o600)


def _swap_private_directory_during_open(
    monkeypatch: pytest.MonkeyPatch,
    runtime_paths: RuntimePaths,
    *,
    watched_prefix: str,
) -> tuple[Path, Path]:
    original_open = runtime.os.open
    moved = runtime_paths.private_directory.with_name("nexus-agent-paper-original")
    outside = runtime_paths.private_directory.with_name("outside-runtime")
    outside.mkdir(mode=0o700)
    swapped = False

    def swapping_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if not swapped and Path(path).name.startswith(watched_prefix):
            swapped = True
            runtime_paths.private_directory.rename(moved)
            runtime_paths.private_directory.symlink_to(outside, target_is_directory=True)
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(runtime.os, "open", swapping_open)
    return moved, outside


def test_prepare_runtime_paths_creates_private_directories(tmp_path: Path) -> None:
    paths = prepare_runtime_paths(tmp_path)

    assert paths.repo_root == tmp_path
    assert paths.private_directory == tmp_path / ".secrets" / "nexus-agent-paper"
    for directory in (tmp_path / ".secrets", paths.private_directory):
        info = directory.stat()
        assert stat.S_ISDIR(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o700
        assert info.st_uid == os.geteuid()


def test_prepare_runtime_paths_rejects_symlink_before_creation(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    (tmp_path / ".secrets").symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        prepare_runtime_paths(tmp_path)

    assert not (outside / "nexus-agent-paper").exists()


def test_prepare_runtime_paths_rejects_unsafe_existing_directory(tmp_path: Path) -> None:
    secrets = tmp_path / ".secrets"
    secrets.mkdir(mode=0o755)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        prepare_runtime_paths(tmp_path)


def test_descriptor_schema_has_only_safe_lifecycle_fields() -> None:
    assert set(AgentPaperRuntimeDescriptor.model_fields) == {
        "schema",
        "role",
        "pid",
        "process_started_at",
        "entrypoint",
        "process_version",
        "state",
        "calendar_authority_id",
        "exit_reason",
        "updated_at",
    }


def test_descriptor_round_trip_uses_private_regular_single_link_file(
    runtime_paths: RuntimePaths,
) -> None:
    expected = _descriptor()

    write_runtime_descriptor(runtime_paths.worker_descriptor, expected)

    info = runtime_paths.worker_descriptor.stat()
    assert stat.S_ISREG(info.st_mode)
    assert stat.S_IMODE(info.st_mode) == 0o600
    assert info.st_nlink == 1
    assert info.st_uid == os.geteuid()
    assert read_runtime_descriptor(runtime_paths.worker_descriptor) == expected


def test_descriptor_read_handles_short_regular_file_reads(
    runtime_paths: RuntimePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _descriptor()
    write_runtime_descriptor(runtime_paths.worker_descriptor, expected)
    unpatched_read = os.read

    def short_read(descriptor: int, size: int) -> bytes:
        return unpatched_read(descriptor, min(size, 7))

    monkeypatch.setattr(runtime.os, "read", short_read)

    assert read_runtime_descriptor(runtime_paths.worker_descriptor) == expected


def test_descriptor_write_atomically_replaces_previous_file(
    runtime_paths: RuntimePaths,
) -> None:
    original = _descriptor(state=RuntimeState.STARTING)
    replacement = _descriptor(state=RuntimeState.RUNNING)
    write_runtime_descriptor(runtime_paths.worker_descriptor, original)
    original_inode = runtime_paths.worker_descriptor.stat().st_ino
    old_handle = runtime_paths.worker_descriptor.open("rb")

    try:
        write_runtime_descriptor(runtime_paths.worker_descriptor, replacement)
        assert runtime_paths.worker_descriptor.stat().st_ino != original_inode
        assert AgentPaperRuntimeDescriptor.model_validate_json(old_handle.read()) == original
    finally:
        old_handle.close()
    assert read_runtime_descriptor(runtime_paths.worker_descriptor) == replacement
    assert not list(runtime_paths.private_directory.glob(".*.tmp"))


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink"])
def test_descriptor_write_rejects_unsafe_existing_target(
    runtime_paths: RuntimePaths,
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    outside.chmod(0o600)
    if unsafe_kind == "symlink":
        runtime_paths.worker_descriptor.symlink_to(outside)
    else:
        os.link(outside, runtime_paths.worker_descriptor)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        write_runtime_descriptor(runtime_paths.worker_descriptor, _descriptor())

    assert outside.read_text(encoding="utf-8") == "outside"


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink"])
def test_descriptor_read_rejects_unsafe_target(
    runtime_paths: RuntimePaths,
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    outside = tmp_path / "outside.json"
    outside.write_bytes(_descriptor().model_dump_json().encode())
    outside.chmod(0o600)
    if unsafe_kind == "symlink":
        runtime_paths.worker_descriptor.symlink_to(outside)
    else:
        os.link(outside, runtime_paths.worker_descriptor)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        read_runtime_descriptor(runtime_paths.worker_descriptor)


def test_descriptor_read_rejects_wrong_mode(runtime_paths: RuntimePaths) -> None:
    _write_raw_descriptor(
        runtime_paths.worker_descriptor,
        _descriptor().model_dump(mode="json"),
    )
    runtime_paths.worker_descriptor.chmod(0o644)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        read_runtime_descriptor(runtime_paths.worker_descriptor)


def test_descriptor_read_rejects_oversize_file(runtime_paths: RuntimePaths) -> None:
    _write_raw_descriptor(runtime_paths.worker_descriptor, b"{" + b"x" * 16_384 + b"}")

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        read_runtime_descriptor(runtime_paths.worker_descriptor)


def test_descriptor_read_rejects_wrong_schema(runtime_paths: RuntimePaths) -> None:
    payload = _descriptor().model_dump(mode="json")
    payload["schema"] = "nexus_agent_paper_runtime_v2"
    _write_raw_descriptor(runtime_paths.worker_descriptor, payload)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        read_runtime_descriptor(runtime_paths.worker_descriptor)


def test_descriptor_read_rejects_secret_field_without_echoing_value(
    runtime_paths: RuntimePaths,
) -> None:
    secret = "super-secret-runtime-value"
    payload = _descriptor().model_dump(mode="json")
    payload["access_token"] = secret
    _write_raw_descriptor(runtime_paths.worker_descriptor, payload)

    with pytest.raises(RuntimeSafetyError) as captured:
        read_runtime_descriptor(runtime_paths.worker_descriptor)

    assert str(captured.value) == "AGENT_PAPER_RUNTIME_UNSAFE"
    assert secret not in str(captured.value)


def test_descriptor_read_rejects_parent_replacement(
    runtime_paths: RuntimePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_runtime_descriptor(runtime_paths.worker_descriptor, _descriptor())
    _, outside = _swap_private_directory_during_open(
        monkeypatch,
        runtime_paths,
        watched_prefix=runtime_paths.worker_descriptor.name,
    )
    _write_raw_descriptor(
        outside / runtime_paths.worker_descriptor.name,
        _descriptor().model_dump(mode="json"),
    )

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        read_runtime_descriptor(runtime_paths.worker_descriptor)


def test_descriptor_write_does_not_follow_replaced_parent(
    runtime_paths: RuntimePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    moved, outside = _swap_private_directory_during_open(
        monkeypatch,
        runtime_paths,
        watched_prefix=f".{runtime_paths.worker_descriptor.name}.",
    )

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        write_runtime_descriptor(runtime_paths.worker_descriptor, _descriptor())

    assert not (outside / runtime_paths.worker_descriptor.name).exists()
    assert not list(outside.glob("*.tmp"))
    assert not (moved / runtime_paths.worker_descriptor.name).exists()
    assert not list(moved.glob("*.tmp"))


def test_read_missing_descriptor_returns_none(runtime_paths: RuntimePaths) -> None:
    assert read_runtime_descriptor(runtime_paths.worker_descriptor) is None


def test_lock_is_private_regular_single_link_file(runtime_paths: RuntimePaths) -> None:
    with acquire_runtime_lock(runtime_paths.worker_lock) as lock:
        info = runtime_paths.worker_lock.stat()
        assert lock.held is True
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert info.st_nlink == 1
        assert info.st_uid == os.geteuid()
    assert lock.held is False


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink"])
def test_lock_rejects_unsafe_existing_target(
    runtime_paths: RuntimePaths,
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    outside = tmp_path / "outside.lock"
    outside.write_text("", encoding="utf-8")
    outside.chmod(0o600)
    if unsafe_kind == "symlink":
        runtime_paths.worker_lock.symlink_to(outside)
    else:
        os.link(outside, runtime_paths.worker_lock)

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        acquire_runtime_lock(runtime_paths.worker_lock)


def test_lock_rejects_parent_replacement_without_locking_outside(
    runtime_paths: RuntimePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, outside = _swap_private_directory_during_open(
        monkeypatch,
        runtime_paths,
        watched_prefix=runtime_paths.worker_lock.name,
    )

    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        acquire_runtime_lock(runtime_paths.worker_lock)

    assert not (outside / runtime_paths.worker_lock.name).exists()


def test_held_lock_detects_later_parent_replacement(runtime_paths: RuntimePaths) -> None:
    lock = acquire_runtime_lock(runtime_paths.worker_lock)
    moved = runtime_paths.private_directory.with_name("nexus-agent-paper-original")
    outside = runtime_paths.private_directory.with_name("outside-runtime")
    outside.mkdir(mode=0o700)
    runtime_paths.private_directory.rename(moved)
    runtime_paths.private_directory.symlink_to(outside, target_is_directory=True)

    try:
        with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
            _ = lock.held
    finally:
        lock.close()

    assert not (outside / runtime_paths.worker_lock.name).exists()


def probe_lock_in_child(path: Path, sender: Connection) -> None:
    try:
        with acquire_runtime_lock(path):
            payload = "acquired"
    except RuntimeSafetyError as error:
        payload = str(error)
    sender.send(payload)
    sender.close()


def test_worker_lock_blocks_a_second_process(runtime_paths: RuntimePaths) -> None:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    with acquire_runtime_lock(runtime_paths.worker_lock):
        child = context.Process(
            target=probe_lock_in_child,
            args=(runtime_paths.worker_lock, sender),
        )
        child.start()
        sender.close()
        if not receiver.poll(5):
            child.terminate()
            child.join(timeout=5)
            pytest.fail("child lock probe timed out")
        result = receiver.recv()
        child.join(timeout=5)
        if child.is_alive():
            child.terminate()
            child.join(timeout=5)
        assert child.exitcode == 0
    receiver.close()
    assert result == "AGENT_PAPER_RUNTIME_LOCKED"


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        ("/repo/.venv/bin/nexus-local-stack serve", RuntimeEntrypoint.SUPERVISOR),
        (
            "uv run --project engine nexus-local-stack serve",
            RuntimeEntrypoint.SUPERVISOR,
        ),
        (
            "/repo/.venv/bin/nexus-engine-agent-paper-loop --parent-fd 7",
            RuntimeEntrypoint.WORKER,
        ),
        (
            "/repo/.venv/bin/python /repo/.venv/bin/nexus-local-stack serve",
            RuntimeEntrypoint.SUPERVISOR,
        ),
        (
            "/repo/.venv/bin/python /repo/.venv/bin/nexus-engine-agent-paper-loop "
            "--parent-fd 7",
            RuntimeEntrypoint.WORKER,
        ),
        ("python harmless.py --label 'nexus-local-stack serve'", None),
        ("python -c 'print(1)' nexus-engine-agent-paper-loop", None),
        ("python /tmp/nexus-engine-agent-paper-loop-evil", None),
        ("/tmp/nexus-engine-agent-paper-loop-evil", None),
        ("/repo/.venv/bin/nexus-local-stack status", None),
        ("python /repo/.venv/bin/nexus-local-stack status", None),
        ("", None),
    ],
)
def test_runtime_entrypoint_normalization_is_exact(
    command: str,
    expected: RuntimeEntrypoint | None,
) -> None:
    assert normalize_runtime_entrypoint(command) == expected


def test_process_identity_returns_only_normalized_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=(),
        returncode=0,
        stdout=(
            "Wed Sep 16 12:34:56 2026 "
            "/repo/.venv/bin/nexus-engine-agent-paper-loop --parent-fd 7\n"
        ),
        stderr="",
    )
    monkeypatch.setattr(runtime.subprocess, "run", lambda *args, **kwargs: completed)

    assert process_identity(321) == ProcessIdentity(
        pid=321,
        started_at="Wed Sep 16 12:34:56 2026",
        entrypoint=RuntimeEntrypoint.WORKER,
    )


@pytest.mark.parametrize(
    ("command", "entrypoint"),
    [
        (
            "/repo/.venv/bin/python /repo/.venv/bin/nexus-local-stack serve",
            RuntimeEntrypoint.SUPERVISOR,
        ),
        (
            "/repo/.venv/bin/python /repo/.venv/bin/nexus-engine-agent-paper-loop "
            "--parent-fd 7",
            RuntimeEntrypoint.WORKER,
        ),
    ],
)
def test_process_identity_accepts_python_console_script_shape(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    entrypoint: RuntimeEntrypoint,
) -> None:
    completed = subprocess.CompletedProcess(
        args=(),
        returncode=0,
        stdout=f"Wed Sep 16 12:34:56 2026 {command}\n",
        stderr="",
    )
    monkeypatch.setattr(runtime.subprocess, "run", lambda *args, **kwargs: completed)

    assert process_identity(321) == ProcessIdentity(
        pid=321,
        started_at="Wed Sep 16 12:34:56 2026",
        entrypoint=entrypoint,
    )


@pytest.mark.parametrize(
    "completed",
    [
        subprocess.CompletedProcess(
            args=(), returncode=7, stdout="ignored", stderr="token=very-secret"
        ),
        subprocess.CompletedProcess(
            args=(), returncode=0, stdout="", stderr="token=very-secret"
        ),
        subprocess.CompletedProcess(
            args=(), returncode=0, stdout="malformed", stderr="token=very-secret"
        ),
    ],
)
def test_process_identity_fails_closed_without_emitting_process_output(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    completed: subprocess.CompletedProcess[str],
) -> None:
    monkeypatch.setattr(runtime.subprocess, "run", lambda *args, **kwargs: completed)
    monkeypatch.setattr(runtime.os, "kill", lambda pid, signal_number: None)

    assert process_identity(321) is runtime.ProcessProbeState.UNKNOWN
    captured = capsys.readouterr()
    assert "very-secret" not in captured.out
    assert "very-secret" not in captured.err
    assert "very-secret" not in caplog.text


def test_process_identity_timeout_does_not_emit_process_output(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def time_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(
            cmd="token=very-secret",
            timeout=2,
            output="token=very-secret",
            stderr="token=very-secret",
        )

    monkeypatch.setattr(runtime.subprocess, "run", time_out)

    assert process_identity(321) is runtime.ProcessProbeState.UNKNOWN
    captured = capsys.readouterr()
    assert "very-secret" not in captured.out
    assert "very-secret" not in captured.err
    assert "very-secret" not in caplog.text


def test_running_worker_requires_matching_lock_identity_and_heartbeat() -> None:
    descriptor = _descriptor()

    status = classify_runtime(
        descriptor,
        lock_held=True,
        heartbeat_fresh=True,
        process_identity=ProcessIdentity(
            pid=descriptor.pid,
            started_at=descriptor.process_started_at,
            entrypoint=RuntimeEntrypoint.WORKER,
        ),
    )

    assert status.code == "running"


def test_running_supervisor_does_not_require_worker_heartbeat() -> None:
    descriptor = _descriptor(
        role=RuntimeRole.SUPERVISOR,
        entrypoint=RuntimeEntrypoint.SUPERVISOR,
        process_version="local-stack-v1",
    )

    status = classify_runtime(
        descriptor,
        lock_held=True,
        heartbeat_fresh=False,
        process_identity=ProcessIdentity(
            pid=descriptor.pid,
            started_at=descriptor.process_started_at,
            entrypoint=RuntimeEntrypoint.SUPERVISOR,
        ),
    )

    assert status.code == "running"


def test_pid_reuse_is_unconfirmed(runtime_paths: RuntimePaths) -> None:
    descriptor = _descriptor(process_started_at="token-a")
    write_runtime_descriptor(runtime_paths.worker_descriptor, descriptor)

    status = classify_runtime(
        descriptor,
        lock_held=True,
        heartbeat_fresh=True,
        process_identity=ProcessIdentity(
            pid=321,
            started_at="token-b",
            entrypoint=RuntimeEntrypoint.WORKER,
        ),
    )

    assert status.code == "unconfirmed"


def test_process_probe_confirms_absence_separately_from_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=(),
        returncode=1,
        stdout="",
        stderr="",
    )

    def absent(pid: int, signal_number: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(runtime.subprocess, "run", lambda *args, **kwargs: completed)
    monkeypatch.setattr(runtime.os, "kill", absent)

    assert process_identity(321) is runtime.ProcessProbeState.ABSENT


@pytest.mark.parametrize(
    "scenario",
    ["timeout", "nonzero", "permission", "malformed", "entrypoint_mismatch"],
)
def test_stopped_classification_rejects_unknown_process_probe(
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
) -> None:
    if scenario == "timeout":

        def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd=("ps",), timeout=2)

    elif scenario in {"nonzero", "permission"}:
        completed = subprocess.CompletedProcess(
            args=(), returncode=1, stdout="", stderr="private"
        )

        def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            return completed

        if scenario == "permission":

            def probe(pid: int, signal_number: int) -> None:
                raise PermissionError

            monkeypatch.setattr(runtime.os, "kill", probe)
        else:
            monkeypatch.setattr(runtime.os, "kill", lambda pid, signal_number: None)
    elif scenario == "malformed":
        completed = subprocess.CompletedProcess(
            args=(), returncode=0, stdout="malformed", stderr="private"
        )

        def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            return completed

    else:
        completed = subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout="Wed Sep 16 12:34:56 2026 python harmless.py\n",
            stderr="private",
        )

        def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            return completed

    monkeypatch.setattr(runtime.subprocess, "run", run)
    observation = process_identity(321)

    status = classify_runtime(
        _descriptor(
            state=RuntimeState.STOPPED,
            exit_reason=RuntimeExitReason.STOP_REQUESTED,
        ),
        lock_held=False,
        heartbeat_fresh=False,
        process_identity=observation,
    )

    assert observation is runtime.ProcessProbeState.UNKNOWN
    assert status.code == "unconfirmed"


@pytest.mark.parametrize(
    ("descriptor", "identity"),
    [
        (
            _descriptor(
                role=RuntimeRole.SUPERVISOR,
                entrypoint=RuntimeEntrypoint.SUPERVISOR,
            ),
            ProcessIdentity(
                pid=321,
                started_at="Wed Sep 16 12:34:56 2026",
                entrypoint=RuntimeEntrypoint.WORKER,
            ),
        ),
        (
            _descriptor(),
            ProcessIdentity(
                pid=321,
                started_at="Wed Sep 16 12:34:56 2026",
                entrypoint=RuntimeEntrypoint.SUPERVISOR,
            ),
        ),
    ],
)
def test_role_swap_is_unconfirmed(
    descriptor: AgentPaperRuntimeDescriptor,
    identity: ProcessIdentity,
) -> None:
    status = classify_runtime(
        descriptor,
        lock_held=True,
        heartbeat_fresh=True,
        process_identity=identity,
    )

    assert status.code == "unconfirmed"


@pytest.mark.parametrize(
    ("lock_held", "heartbeat_fresh", "identity"),
    [
        (False, True, ProcessIdentity(321, "token-a", RuntimeEntrypoint.WORKER)),
        (True, False, ProcessIdentity(321, "token-a", RuntimeEntrypoint.WORKER)),
        (True, True, None),
    ],
)
def test_running_worker_with_inconsistent_fact_is_unconfirmed(
    lock_held: bool,
    heartbeat_fresh: bool,
    identity: ProcessIdentity | None,
) -> None:
    descriptor = _descriptor(process_started_at="token-a")

    status = classify_runtime(
        descriptor,
        lock_held=lock_held,
        heartbeat_fresh=heartbeat_fresh,
        process_identity=identity,
    )

    assert status.code == "unconfirmed"


def test_stopped_runtime_requires_all_observed_facts_to_be_stopped() -> None:
    descriptor = _descriptor(
        state=RuntimeState.STOPPED,
        exit_reason=RuntimeExitReason.STOP_REQUESTED,
    )

    confirmed = classify_runtime(
        descriptor,
        lock_held=False,
        heartbeat_fresh=False,
        process_identity=runtime.ProcessProbeState.ABSENT,
    )
    stale_identity = classify_runtime(
        descriptor,
        lock_held=False,
        heartbeat_fresh=False,
        process_identity=ProcessIdentity(
            descriptor.pid,
            descriptor.process_started_at,
            descriptor.entrypoint,
        ),
    )

    assert confirmed.code == "stopped"
    assert stale_identity.code == "unconfirmed"


@pytest.mark.parametrize(
    "exit_reason",
    [
        None,
        RuntimeExitReason.STOP_UNCONFIRMED,
        RuntimeExitReason.WORKER_EXIT_REASON_UNKNOWN,
        RuntimeExitReason.RUNTIME_ABSENT_UNCONFIRMED,
    ],
)
def test_stopped_runtime_rejects_uncertain_exit_reason(
    exit_reason: RuntimeExitReason | None,
) -> None:
    status = classify_runtime(
        _descriptor(state=RuntimeState.STOPPED, exit_reason=exit_reason),
        lock_held=False,
        heartbeat_fresh=False,
        process_identity=runtime.ProcessProbeState.ABSENT,
    )

    assert status.code == "unconfirmed"


def test_missing_descriptor_is_unconfirmed() -> None:
    status = classify_runtime(
        None,
        lock_held=False,
        heartbeat_fresh=False,
        process_identity=None,
    )

    assert status.code == "unconfirmed"
