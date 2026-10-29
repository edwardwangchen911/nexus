"""Single-use, generation-bound activation after the host process lock."""

import asyncio
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
from contextlib import AbstractContextManager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, cast
from uuid import UUID

from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from nexus.paper.manual_entry import manual_host_scope
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.settings import EngineSettings

_BLOCKED = "AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED"


def _read(path: Path) -> dict[str, Any]:
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "r") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > 2_000_000
        ):
            raise ValueError(_BLOCKED)
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(_BLOCKED)
    return value


def _service_control_lock(root: Path) -> AbstractContextManager[None]:
    """Share the manager's nonblocking lock only for the activation commit."""
    source = root / "scripts/macos/agent-paper-loop-service.py"
    spec = importlib.util.spec_from_file_location("nexus_activation_service_control", source)
    if spec is None or spec.loader is None:
        raise ValueError(_BLOCKED)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(AbstractContextManager[None], module._service_control_lock())


def _boot_id() -> str:
    return str(
        UUID(
            subprocess.check_output(
                ["sysctl", "-n", "kern.bootsessionuuid"], text=True, timeout=5
            ).strip()
        )
    )


class RecoveryActivation:
    def __init__(
        self,
        settings: EngineSettings,
        *,
        repo_root: Path,
        env_file: Path,
        generation: str,
        attempt_id: str,
    ) -> None:
        if str(UUID(generation)) != generation or str(UUID(attempt_id)) != attempt_id:
            raise ValueError(_BLOCKED)
        self.settings = settings
        self.root = repo_root
        self.env_file = env_file
        self.generation = generation
        self.attempt_id = attempt_id
        self.directory = repo_root / ".secrets/nexus-agent-paper"
        self.receipt_path = self.directory / f"recovery-activation-{attempt_id}.json"
        self.receipt: dict[str, object] | None = None

    def _validate(self, effective: EngineSettings) -> None:
        value = _read(self.directory / "recovery.json")
        attempt = value.get("attempt")
        if (
            self.directory.is_symlink()
            or self.env_file.is_symlink()
            or value.get("version") != 1
            or value.get("desired") != "enabled"
            or value.get("root") != str(self.root)
            or value.get("env_file") != str(self.env_file)
            or value.get("generation") != self.generation
            or value.get("schema") != CURRENT_MIGRATION_HEAD
            or value.get("revision") != effective.release_revision
            or self.settings.release_revision != effective.release_revision
            or value.get("config_hash") != hashlib.sha256(self.env_file.read_bytes()).hexdigest()
            or not isinstance(attempt, dict)
            or attempt.get("generation") != self.generation
            or attempt.get("id") != self.attempt_id
            or attempt.get("boot") != _boot_id()
            or self.settings.database_url != effective.database_url
            or manual_host_scope(self.settings) != manual_host_scope(effective)
        ):
            raise ValueError(_BLOCKED)
        if attempt.get("exit_reconciliation_sha256") is not None:
            source = self.root / "scripts/macos/agent-paper-reconciliation.py"
            spec = importlib.util.spec_from_file_location("nexus_activation_reconciliation", source)
            if spec is None or spec.loader is None:
                raise ValueError(_BLOCKED)
            helper = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(helper)
            helper.validate_pending_activation(self.root, value, attempt)

    def _save(self, phase: str, reason: str, *, exit_code: str | None = None) -> None:
        if self.receipt is None:
            raise ValueError(_BLOCKED)
        receipt = {**self.receipt, "phase": phase, "reason": reason}
        if exit_code is not None:
            receipt.update(exit_stage="host_loop", exit_code=exit_code)
        # Keep a durable negative witness until every receipt write step has
        # completed. A replace followed by failed directory fsync is unknown,
        # even if a reader can already see the new qualified exit contents.
        pending = self.receipt_path.with_suffix(".write-pending")
        descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(b"AGENT_PAPER_RECOVERY_EXIT_WRITE_UNCONFIRMED\n")
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        temporary: Path | None = None
        try:
            with NamedTemporaryFile(
                dir=self.directory, prefix="activation.", delete=False
            ) as stream:
                temporary = Path(stream.name)
                os.fchmod(stream.fileno(), 0o600)
                stream.write((json.dumps(receipt, sort_keys=True) + "\n").encode())
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.receipt_path)
            descriptor = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        # Last fallible operation: no fsync after removing the negative witness.
        # A crash that resurrects it can only block recovery, never authorize it.
        pending.unlink()
        self.receipt = receipt

    async def __call__(self, effective: EngineSettings) -> None:
        from nexus.ops.agent_paper_recovery import (
            inspect_recovery_readiness,
            require_recovery_process_quiescence,
        )

        try:
            self._validate(effective)
            descriptor = os.open(
                self.receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
            )
            self.receipt = {
                "version": 1,
                "attempt_id": self.attempt_id,
                "generation": self.generation,
                "revision": effective.release_revision,
                "boot": _boot_id(),
                "pid": os.getpid(),
                "process_started": subprocess.check_output(
                    ["/bin/ps", "-p", str(os.getpid()), "-o", "lstart="], text=True, timeout=5
                ).strip(),
                "phase": "checking",
            }
            with os.fdopen(descriptor, "w") as stream:
                json.dump(self.receipt, stream)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                require_recovery_process_quiescence()
            except (OSError, RuntimeError, ValueError):
                self._save("rejected", "PRE_ACTIVATION_REJECTED")
                raise ValueError(_BLOCKED) from None
            result = await inspect_recovery_readiness(effective)
            self._validate(effective)
            try:
                require_recovery_process_quiescence()
            except (OSError, RuntimeError, ValueError):
                self._save("rejected", "PRE_ACTIVATION_REJECTED")
                raise ValueError(_BLOCKED) from None
            if result.get("status") != "ready":
                self._save("rejected", "PRE_ACTIVATION_REJECTED")
                raise ValueError(_BLOCKED)
            try:
                # The engine lifetime lock is already held. Never wait for the
                # manager, which can hold its lock while dispatching this child.
                with _service_control_lock(self.root):
                    self._validate(effective)
                    self._save("activated", "READY")
            except (OSError, RuntimeError, ValueError):
                self._save("rejected", "PRE_ACTIVATION_REJECTED")
                raise ValueError(_BLOCKED) from None
        except (OSError, RuntimeError, ValueError, TypeError, KeyError):
            raise ValueError(_BLOCKED) from None

    def finish_exception(self, error: BaseException) -> None:
        # A rejected/checking activation is not a running host. Preserve its
        # original evidence, and never infer a business result from this exit.
        if self.receipt is None or self.receipt.get("phase") != "activated":
            return
        code = (
            "AGENT_PAPER_LOOP_INTERRUPTED"
            if isinstance(error, KeyboardInterrupt)
            else "AGENT_PAPER_LOOP_CANCELLED"
            if isinstance(error, asyncio.CancelledError)
            else "AGENT_PAPER_LOOP_RESULT_UNKNOWN"
        )
        self._save("exited", "unclassified", exit_code=code)

    def finish_database_error(self, error: SQLAlchemyError) -> None:
        self.finish(
            "database_unavailable"
            if isinstance(error, DBAPIError) and error.connection_invalidated
            else "unclassified"
        )

    def finish(self, reason: str) -> None:
        # Only explicit infrastructure/clean exits are eligible for reinspection.
        allowed = {"stop_requested", "stop_calendar_unavailable", "database_unavailable"}
        self._save("exited", reason if reason in allowed else "unclassified")


def make_recovery_guard(
    settings: EngineSettings,
    *,
    repo_root: Path,
    env_file: Path,
    generation: str,
    attempt_id: str,
) -> RecoveryActivation:
    return RecoveryActivation(
        settings,
        repo_root=repo_root,
        env_file=env_file,
        generation=generation,
        attempt_id=attempt_id,
    )
