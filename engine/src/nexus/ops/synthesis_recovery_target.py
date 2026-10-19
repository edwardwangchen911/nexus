"""Bind an isolated maintenance database to the original host inspection identity."""

import argparse
import hashlib
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.ops.agent_paper_loop import load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _validate_local_env
from nexus.paper.synthesis_usage_acceptance import SynthesisUsageDiagnosticError
from nexus.settings import EngineSettings


def _reject(code: str) -> SynthesisUsageDiagnosticError:
    return SynthesisUsageDiagnosticError("synthesis_recovery_target", code)


def _source(root: Path, path: Path) -> tuple[str, str]:
    path = _validate_local_env(root, path)
    info = path.stat()
    if (
        info.st_uid != os.getuid()
        or not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) & 0o077
        or info.st_nlink != 1
    ):
        raise _reject("SOURCE_CONFIGURATION_UNSAFE")
    before = path.read_bytes()
    environment = load_agent_paper_service_environment(root, {}, env_file=path)
    url = environment.get("DATABASE_URL")
    _validate_local_database(url)
    name = make_url(url or "").database
    if not name or path.read_bytes() != before:
        raise _reject("SOURCE_CONFIGURATION_UNSAFE")
    return name, hashlib.sha256(before).hexdigest()


@dataclass(frozen=True)
class RecoveryProducer:
    """No source DB connection: only inspect its normal protected configuration."""

    repo_root: Path
    env_file: Path
    source_database: str
    source_digest: str
    target_database: str

    async def verify(self, session: AsyncSession) -> None:
        try:
            source, digest = _source(self.repo_root, self.env_file)
        except (OSError, ValueError):
            raise _reject("SOURCE_CONFIGURATION_UNSAFE") from None
        if source != self.source_database or digest != self.source_digest:
            raise _reject("SOURCE_CONFIGURATION_CHANGED")
        if source == self.target_database:
            raise _reject("SOURCE_TARGET_DATABASE_IDENTICAL")
        if await session.scalar(text("SELECT current_database()")) != self.target_database:
            raise _reject("ACTUAL_DATABASE_MISMATCH")


def load_recovery_producer(
    settings: EngineSettings, args: argparse.Namespace
) -> RecoveryProducer | None:
    root = getattr(args, "recovery_producer_repo_root", None)
    path = getattr(args, "recovery_producer_env_file", None)
    if root is None and path is None:
        return None
    if root is None or path is None:
        raise _reject("PRODUCER_ARGUMENT_PAIR_REQUIRED")
    root = Path(root).resolve()
    path = Path(path)
    path = path if path.is_absolute() else root / path
    _validate_local_database(settings.database_url)
    target = make_url(settings.database_url or "").database
    if target is None or re.fullmatch(r"nexus_recovery_[a-z0-9_]{1,48}", target) is None:
        raise _reject("RECOVERY_DATABASE_REQUIRED")
    try:
        source, digest = _source(root, path)
    except (OSError, ValueError):
        raise _reject("SOURCE_CONFIGURATION_UNSAFE") from None
    if source == target:
        raise _reject("SOURCE_TARGET_DATABASE_IDENTICAL")
    return RecoveryProducer(root, path, source, digest, target)
