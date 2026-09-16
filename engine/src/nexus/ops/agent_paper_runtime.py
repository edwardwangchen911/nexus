"""Fail-closed local runtime facts for the Agent paper service."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import secrets
import shlex
import stat
import subprocess
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

_MAX_DESCRIPTOR_BYTES = 16 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_PYTHON_INTERPRETER_NAMES = frozenset({"python", "python3", "python3.12"})


class RuntimeSafetyError(RuntimeError):
    """A fixed-code failure for runtime facts that cannot be trusted."""


class RuntimeRole(StrEnum):
    SUPERVISOR = "supervisor"
    WORKER = "worker"


class RuntimeEntrypoint(StrEnum):
    SUPERVISOR = "nexus-local-stack serve"
    WORKER = "nexus-engine-agent-paper-loop"


class RuntimeState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    UNCONFIRMED = "unconfirmed"


class ProcessProbeState(StrEnum):
    ABSENT = "absent"
    UNKNOWN = "unknown"


class RuntimeExitReason(StrEnum):
    STOP_REQUESTED = "STOP_REQUESTED"
    STOP_UNCONFIRMED = "STOP_UNCONFIRMED"
    WORKER_EXITED_UNCLEANLY = "WORKER_EXITED_UNCLEANLY"
    WORKER_EXIT_REASON_UNKNOWN = "WORKER_EXIT_REASON_UNKNOWN"
    RUNTIME_ABSENT_UNCONFIRMED = "RUNTIME_ABSENT_UNCONFIRMED"


@dataclass(frozen=True)
class RuntimePaths:
    repo_root: Path
    private_directory: Path
    supervisor_lock: Path
    supervisor_descriptor: Path
    worker_lock: Path
    worker_descriptor: Path


class AgentPaperRuntimeDescriptor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema: Literal["nexus_agent_paper_runtime_v1"]  # type: ignore[assignment]
    role: RuntimeRole
    pid: int = Field(ge=1)
    process_started_at: str = Field(min_length=1, max_length=128)
    entrypoint: RuntimeEntrypoint
    process_version: str = Field(min_length=1, max_length=100)
    state: RuntimeState
    calendar_authority_id: UUID | None = None
    exit_reason: RuntimeExitReason | None = None
    updated_at: datetime


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    started_at: str
    entrypoint: RuntimeEntrypoint


@dataclass(frozen=True)
class RuntimeClassification:
    code: Literal["running", "stopped", "unconfirmed"]


@dataclass
class _RuntimeDirectory:
    repo_root: Path
    repo_descriptor: int
    secrets_descriptor: int
    private_descriptor: int
    repo_identity: tuple[int, int]
    secrets_identity: tuple[int, int]
    private_identity: tuple[int, int]

    def close(self) -> None:
        for descriptor_name in (
            "private_descriptor",
            "secrets_descriptor",
            "repo_descriptor",
        ):
            descriptor = getattr(self, descriptor_name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, descriptor_name, -1)


def _unsafe() -> RuntimeSafetyError:
    return RuntimeSafetyError("AGENT_PAPER_RUNTIME_UNSAFE")


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _validate_private_directory_info(info: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != _PRIVATE_DIRECTORY_MODE
        or info.st_uid != os.geteuid()
    ):
        raise _unsafe()


def _open_directory(path: Path | str, *, dir_fd: int | None = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    if dir_fd is None:
        return os.open(path, flags)
    return os.open(path, flags, dir_fd=dir_fd)


def _same_identity(info: os.stat_result, expected: tuple[int, int]) -> bool:
    return _identity(info) == expected


def _assert_runtime_directory_current(directory: _RuntimeDirectory) -> None:
    repo_descriptor: int | None = None
    secrets_descriptor: int | None = None
    private_descriptor: int | None = None
    try:
        repo_descriptor = _open_directory(directory.repo_root)
        repo_info = os.fstat(repo_descriptor)
        if not stat.S_ISDIR(repo_info.st_mode) or not _same_identity(
            repo_info, directory.repo_identity
        ):
            raise _unsafe()
        secrets_descriptor = _open_directory(".secrets", dir_fd=repo_descriptor)
        secrets_info = os.fstat(secrets_descriptor)
        _validate_private_directory_info(secrets_info)
        if not _same_identity(secrets_info, directory.secrets_identity):
            raise _unsafe()
        private_descriptor = _open_directory(
            "nexus-agent-paper", dir_fd=secrets_descriptor
        )
        private_info = os.fstat(private_descriptor)
        _validate_private_directory_info(private_info)
        if not _same_identity(private_info, directory.private_identity):
            raise _unsafe()
    except OSError:
        raise _unsafe() from None
    finally:
        for descriptor in (private_descriptor, secrets_descriptor, repo_descriptor):
            if descriptor is not None:
                os.close(descriptor)


def _open_runtime_directory(path: Path) -> _RuntimeDirectory:
    private_directory = path.parent
    secrets_directory = private_directory.parent
    if private_directory.name != "nexus-agent-paper" or secrets_directory.name != ".secrets":
        raise _unsafe()
    repo_root = secrets_directory.parent
    repo_descriptor: int | None = None
    secrets_descriptor: int | None = None
    private_descriptor: int | None = None
    directory: _RuntimeDirectory | None = None
    try:
        repo_descriptor = _open_directory(repo_root)
        repo_info = os.fstat(repo_descriptor)
        if not stat.S_ISDIR(repo_info.st_mode):
            raise _unsafe()
        secrets_descriptor = _open_directory(".secrets", dir_fd=repo_descriptor)
        secrets_info = os.fstat(secrets_descriptor)
        _validate_private_directory_info(secrets_info)
        private_descriptor = _open_directory(
            "nexus-agent-paper", dir_fd=secrets_descriptor
        )
        private_info = os.fstat(private_descriptor)
        _validate_private_directory_info(private_info)
        directory = _RuntimeDirectory(
            repo_root=repo_root,
            repo_descriptor=repo_descriptor,
            secrets_descriptor=secrets_descriptor,
            private_descriptor=private_descriptor,
            repo_identity=_identity(repo_info),
            secrets_identity=_identity(secrets_info),
            private_identity=_identity(private_info),
        )
        repo_descriptor = None
        secrets_descriptor = None
        private_descriptor = None
        _assert_runtime_directory_current(directory)
        return directory
    except OSError:
        raise _unsafe() from None
    except RuntimeSafetyError:
        if directory is not None:
            directory.close()
            directory = None
        raise
    finally:
        for descriptor in (private_descriptor, secrets_descriptor, repo_descriptor):
            if descriptor is not None:
                os.close(descriptor)


def _validate_private_file(
    info: os.stat_result,
    *,
    enforce_size: bool = False,
) -> None:
    if (
        not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) != _PRIVATE_FILE_MODE
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or (enforce_size and info.st_size > _MAX_DESCRIPTOR_BYTES)
    ):
        raise _unsafe()


def _validate_existing_descriptor(directory: _RuntimeDirectory, name: str) -> None:
    try:
        info = os.stat(
            name,
            dir_fd=directory.private_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    except OSError:
        raise _unsafe() from None
    _validate_private_file(info, enforce_size=True)


def prepare_runtime_paths(repo_root: Path) -> RuntimePaths:
    """Create and validate the private directories used by runtime facts."""
    root = Path(repo_root)
    repo_descriptor: int | None = None
    secrets_descriptor: int | None = None
    private_descriptor: int | None = None
    directory: _RuntimeDirectory | None = None
    try:
        repo_descriptor = _open_directory(root)
        repo_info = os.fstat(repo_descriptor)
        if not stat.S_ISDIR(repo_info.st_mode):
            raise _unsafe()
        try:
            os.mkdir(
                ".secrets",
                mode=_PRIVATE_DIRECTORY_MODE,
                dir_fd=repo_descriptor,
            )
        except FileExistsError:
            pass
        secrets_descriptor = _open_directory(".secrets", dir_fd=repo_descriptor)
        secrets_info = os.fstat(secrets_descriptor)
        _validate_private_directory_info(secrets_info)
        try:
            os.mkdir(
                "nexus-agent-paper",
                mode=_PRIVATE_DIRECTORY_MODE,
                dir_fd=secrets_descriptor,
            )
        except FileExistsError:
            pass
        private_descriptor = _open_directory(
            "nexus-agent-paper", dir_fd=secrets_descriptor
        )
        private_info = os.fstat(private_descriptor)
        _validate_private_directory_info(private_info)
        directory = _RuntimeDirectory(
            repo_root=root,
            repo_descriptor=repo_descriptor,
            secrets_descriptor=secrets_descriptor,
            private_descriptor=private_descriptor,
            repo_identity=_identity(repo_info),
            secrets_identity=_identity(secrets_info),
            private_identity=_identity(private_info),
        )
        repo_descriptor = None
        secrets_descriptor = None
        private_descriptor = None
        _assert_runtime_directory_current(directory)
    except OSError:
        raise _unsafe() from None
    finally:
        if directory is not None:
            directory.close()
        for descriptor in (private_descriptor, secrets_descriptor, repo_descriptor):
            if descriptor is not None:
                os.close(descriptor)

    secrets_directory = root / ".secrets"
    private_directory = secrets_directory / "nexus-agent-paper"
    return RuntimePaths(
        repo_root=root,
        private_directory=private_directory,
        supervisor_lock=private_directory / "serve.lock",
        supervisor_descriptor=private_directory / "serve.runtime.json",
        worker_lock=private_directory / "worker.lock",
        worker_descriptor=private_directory / "worker.runtime.json",
    )


class RuntimeLock:
    """An acquired advisory lock whose file descriptor defines ownership."""

    def __init__(
        self,
        path: Path,
        descriptor: int,
        runtime_directory: _RuntimeDirectory,
    ) -> None:
        self.path = path
        self._descriptor: int | None = descriptor
        self._runtime_directory: _RuntimeDirectory | None = runtime_directory

    @property
    def held(self) -> bool:
        if self._descriptor is None or self._runtime_directory is None:
            return False
        _assert_runtime_directory_current(self._runtime_directory)
        return True

    def __enter__(self) -> Self:
        if not self.held:
            raise RuntimeSafetyError("AGENT_PAPER_RUNTIME_LOCK_RELEASED")
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        descriptor = self._descriptor
        runtime_directory = self._runtime_directory
        self._descriptor = None
        self._runtime_directory = None
        try:
            if descriptor is not None:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if runtime_directory is not None:
                runtime_directory.close()


def acquire_runtime_lock(path: Path) -> RuntimeLock:
    """Acquire one non-blocking cross-process lock without following links."""
    path = Path(path)
    runtime_directory = _open_runtime_directory(path)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path.name,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            _PRIVATE_FILE_MODE,
            dir_fd=runtime_directory.private_descriptor,
        )
        _validate_private_file(os.fstat(descriptor))
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _assert_runtime_directory_current(runtime_directory)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        runtime_directory.close()
        if error.errno in {errno.EACCES, errno.EAGAIN}:
            raise RuntimeSafetyError("AGENT_PAPER_RUNTIME_LOCKED") from None
        raise _unsafe() from None
    except RuntimeSafetyError:
        if descriptor is not None:
            os.close(descriptor)
        runtime_directory.close()
        raise
    return RuntimeLock(path, descriptor, runtime_directory)


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("short runtime descriptor write")
        remaining = remaining[written:]


def _read_up_to(descriptor: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total < limit:
        chunk = os.read(descriptor, limit - total)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


def _fsync_private_directory(directory: _RuntimeDirectory) -> None:
    try:
        info = os.fstat(directory.private_descriptor)
        _validate_private_directory_info(info)
        if not _same_identity(info, directory.private_identity):
            raise _unsafe()
        os.fsync(directory.private_descriptor)
    except OSError:
        raise _unsafe() from None


def write_runtime_descriptor(
    path: Path,
    descriptor: AgentPaperRuntimeDescriptor,
) -> None:
    """Atomically replace one validated, secret-free runtime descriptor."""
    path = Path(path)
    runtime_directory = _open_runtime_directory(path)
    temporary_name: str | None = None
    temporary_descriptor: int | None = None
    try:
        _validate_existing_descriptor(runtime_directory, path.name)
        payload = json.dumps(
            descriptor.model_dump(mode="json"),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(payload) > _MAX_DESCRIPTOR_BYTES:
            raise _unsafe()
        temporary_name = f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        temporary_descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            _PRIVATE_FILE_MODE,
            dir_fd=runtime_directory.private_descriptor,
        )
        _validate_private_file(os.fstat(temporary_descriptor))
        _write_all(temporary_descriptor, payload)
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        _assert_runtime_directory_current(runtime_directory)
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=runtime_directory.private_descriptor,
            dst_dir_fd=runtime_directory.private_descriptor,
        )
        _assert_runtime_directory_current(runtime_directory)
        _fsync_private_directory(runtime_directory)
    except OSError:
        raise _unsafe() from None
    finally:
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        try:
            if temporary_name is not None:
                os.unlink(temporary_name, dir_fd=runtime_directory.private_descriptor)
        except FileNotFoundError:
            pass
        except OSError:
            raise _unsafe() from None
        finally:
            runtime_directory.close()


def read_runtime_descriptor(path: Path) -> AgentPaperRuntimeDescriptor | None:
    """Read one validated runtime descriptor, or return None when it is absent."""
    path = Path(path)
    runtime_directory = _open_runtime_directory(path)
    file_descriptor: int | None = None
    try:
        try:
            file_descriptor = os.open(
                path.name,
                os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,
                dir_fd=runtime_directory.private_descriptor,
            )
        except FileNotFoundError:
            _assert_runtime_directory_current(runtime_directory)
            return None
        _assert_runtime_directory_current(runtime_directory)
        info = os.fstat(file_descriptor)
        _validate_private_file(info, enforce_size=True)
        payload = _read_up_to(file_descriptor, _MAX_DESCRIPTOR_BYTES + 1)
        if len(payload) > _MAX_DESCRIPTOR_BYTES:
            raise _unsafe()
        return AgentPaperRuntimeDescriptor.model_validate_json(payload)
    except (OSError, ValidationError, ValueError):
        raise _unsafe() from None
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        runtime_directory.close()


def _command_start(tokens: list[str]) -> int | None:
    if not tokens:
        return None
    if Path(tokens[0]).name != "uv" or len(tokens) < 2 or tokens[1] != "run":
        return 0
    index = 2
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            return index + 1 if index + 1 < len(tokens) else None
        if token == "--project":
            index += 2
            continue
        if token.startswith("--project="):
            index += 1
            continue
        if token.startswith("-"):
            return None
        return index
    return None


def normalize_runtime_entrypoint(command: str) -> RuntimeEntrypoint | None:
    """Reduce an allowlisted command shape to a safe lifecycle entrypoint."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    start = _command_start(tokens)
    if start is None or start >= len(tokens):
        return None
    if Path(tokens[start]).name in _PYTHON_INTERPRETER_NAMES:
        start += 1
        if start >= len(tokens) or tokens[start].startswith("-"):
            return None
    executable = Path(tokens[start]).name
    if (
        executable == "nexus-local-stack"
        and start + 1 < len(tokens)
        and tokens[start + 1] == "serve"
    ):
        return RuntimeEntrypoint.SUPERVISOR
    if executable == "nexus-engine-agent-paper-loop":
        return RuntimeEntrypoint.WORKER
    return None


def split_ps_identity(raw: str) -> tuple[str, str]:
    """Split the fixed-width ps lstart field from its command without exposing it."""
    if len(raw) <= 25:
        raise ValueError
    started_at = raw[:24]
    command = raw[24:].strip()
    if (
        len(started_at) != 24
        or started_at[3] != " "
        or started_at[7] != " "
        or started_at[10] != " "
        or started_at[19] != " "
        or not command
    ):
        raise ValueError
    return started_at, command


def _probe_absence_after_ps_failure(pid: int) -> ProcessProbeState:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return ProcessProbeState.ABSENT
    except OSError:
        return ProcessProbeState.UNKNOWN
    return ProcessProbeState.UNKNOWN


def process_identity(pid: int) -> ProcessIdentity | ProcessProbeState:
    """Probe and normalize a process identity without emitting command output."""
    if pid < 1:
        return ProcessProbeState.UNKNOWN
    try:
        completed = subprocess.run(
            ("ps", "-ww", "-p", str(pid), "-o", "lstart=", "-o", "command="),
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ProcessProbeState.UNKNOWN
    raw = completed.stdout.strip()
    if completed.returncode != 0:
        return _probe_absence_after_ps_failure(pid)
    if not raw:
        return ProcessProbeState.UNKNOWN
    try:
        started_at, command = split_ps_identity(raw)
    except ValueError:
        return ProcessProbeState.UNKNOWN
    entrypoint = normalize_runtime_entrypoint(command)
    if entrypoint is None:
        return ProcessProbeState.UNKNOWN
    return ProcessIdentity(pid=pid, started_at=started_at, entrypoint=entrypoint)


def _expected_entrypoint(role: RuntimeRole) -> RuntimeEntrypoint:
    if role is RuntimeRole.SUPERVISOR:
        return RuntimeEntrypoint.SUPERVISOR
    return RuntimeEntrypoint.WORKER


def classify_runtime(
    descriptor: AgentPaperRuntimeDescriptor | None,
    *,
    lock_held: bool,
    heartbeat_fresh: bool,
    process_identity: ProcessIdentity | ProcessProbeState | None,
) -> RuntimeClassification:
    """Classify only fact combinations that agree; ambiguity stays unconfirmed."""
    unconfirmed = RuntimeClassification(code="unconfirmed")
    if descriptor is None:
        return unconfirmed
    expected_entrypoint = _expected_entrypoint(descriptor.role)
    if descriptor.entrypoint is not expected_entrypoint:
        return unconfirmed

    if descriptor.state is RuntimeState.RUNNING:
        if (
            not lock_held
            or descriptor.exit_reason is not None
            or not isinstance(process_identity, ProcessIdentity)
        ):
            return unconfirmed
        if (
            process_identity.pid != descriptor.pid
            or process_identity.started_at != descriptor.process_started_at
            or process_identity.entrypoint is not descriptor.entrypoint
        ):
            return unconfirmed
        if descriptor.role is RuntimeRole.WORKER and not heartbeat_fresh:
            return unconfirmed
        return RuntimeClassification(code="running")

    if descriptor.state is RuntimeState.STOPPED:
        if lock_held or process_identity is not ProcessProbeState.ABSENT:
            return unconfirmed
        if descriptor.role is RuntimeRole.WORKER and heartbeat_fresh:
            return unconfirmed
        confirmed_exit_reasons = {RuntimeExitReason.STOP_REQUESTED}
        if descriptor.role is RuntimeRole.WORKER:
            confirmed_exit_reasons.add(RuntimeExitReason.WORKER_EXITED_UNCLEANLY)
        if descriptor.exit_reason not in confirmed_exit_reasons:
            return unconfirmed
        return RuntimeClassification(code="stopped")

    return unconfirmed
