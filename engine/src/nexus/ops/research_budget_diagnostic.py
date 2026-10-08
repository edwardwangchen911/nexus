"""Execute one fresh research Job in a dedicated synthetic diagnostic database."""

import argparse
import asyncio
import json
import os
import stat
import time
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
from nexus.persistence.base import Base
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.portfolio import Account
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings

_SAFE_ERRORS = frozenset(
    {
        "DIAGNOSTIC_DATABASE_REQUIRED",
        "DIAGNOSTIC_FRESH_RUN_REQUIRED",
        "DIAGNOSTIC_FRESH_JOB_REQUIRED",
        "DIAGNOSTIC_BUSINESS_STATE_FORBIDDEN",
        "DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED",
        "DIAGNOSTIC_SINGLE_CASE_REQUIRED",
        "DIAGNOSTIC_OUTPUT_ALREADY_EXISTS",
        "DIAGNOSTIC_DIRECTORY_UNSAFE",
        "DIAGNOSTIC_CONFIG_UNSAFE",
        "DIAGNOSTIC_SDK_REQUIRED",
        "DIAGNOSTIC_JOB_CHANGED",
    }
)


def require_diagnostic_url(url: str) -> None:
    parsed = make_url(url)
    if (
        parsed.drivername != "postgresql+asyncpg"
        or parsed.host not in {"localhost", "127.0.0.1", "::1"}
        or not (parsed.database or "").startswith("nexus_diag_")
    ):
        raise ValueError("DIAGNOSTIC_DATABASE_REQUIRED")


def require_fresh_identity(run: AgentResearchRunRecord, job: Job, run_id: UUID) -> None:
    if (
        run.status != "prepared"
        or run.pricing_version != DEADLINE_BUDGET_CONFIG_VERSION
        or run.actor_id != "local-research-cli"
        or not run.idempotency_key.startswith("budget-diagnostic:")
    ):
        raise ValueError("DIAGNOSTIC_FRESH_RUN_REQUIRED")
    if (
        job.job_type != "agent.research"
        or job.queue != "general"
        or job.status != "queued"
        or job.attempt_count != 0
        or job.manual_retry_count != 0
        or job.max_attempts != 1
        or job.retry_policy != "automatic_safe"
        or job.payload.get("run_id") != str(run_id)
    ):
        raise ValueError("DIAGNOSTIC_FRESH_JOB_REQUIRED")


async def require_isolated_case(session: AsyncSession, run_id: UUID) -> UUID:
    database = await session.scalar(text("SELECT current_database()"))
    if not isinstance(database, str) or not database.startswith("nexus_diag_"):
        raise ValueError("DIAGNOSTIC_DATABASE_REQUIRED")
    for name in (
        "account_external_id_envelopes",
        "broker_account_bindings",
        "paper_accounts",
        "agent_paper_cycles",
        "paper_orders",
    ):
        if await session.scalar(select(func.count()).select_from(Base.metadata.tables[name])):
            raise ValueError("DIAGNOSTIC_BUSINESS_STATE_FORBIDDEN")
    if await session.scalar(
        select(func.count()).select_from(Account).where(Account.provider != "diagnostic_synthetic")
    ):
        raise ValueError("DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED")
    runs = list(await session.scalars(select(AgentResearchRunRecord)))
    jobs = list(await session.scalars(select(Job)))
    if len(runs) != 1 or runs[0].id != run_id or len(jobs) != 1:
        raise ValueError("DIAGNOSTIC_SINGLE_CASE_REQUIRED")
    require_fresh_identity(runs[0], jobs[0], run_id)
    if await session.scalar(select(func.count()).select_from(AgentResearchOutputRecord)):
        raise ValueError("DIAGNOSTIC_OUTPUT_ALREADY_EXISTS")
    return jobs[0].id


def _private_directory(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("DIAGNOSTIC_DIRECTORY_UNSAFE")


def read_database_url(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("DIAGNOSTIC_CONFIG_UNSAFE")
        url = stream.read(4097).strip()
    if len(url) > 4096:
        raise ValueError("DIAGNOSTIC_CONFIG_UNSAFE")
    require_diagnostic_url(url)
    return url


def _receipt(path: Path, result: dict[str, object]) -> None:
    with path.open("x") as stream:
        os.chmod(path, 0o600)
        json.dump(result, stream, sort_keys=True)


def usage_is_known(run: AgentResearchRunRecord) -> bool:
    """Only settled complete counters prove known usage; held counters can be lower bounds."""
    return (
        run.settled_cost_usd is not None
        and run.input_tokens is not None
        and run.output_tokens is not None
    )


async def execute_diagnostic(
    settings: EngineSettings, *, run_id: UUID, output: Path
) -> dict[str, object]:
    require_diagnostic_url(settings.database_url or "")
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("DIAGNOSTIC_SDK_REQUIRED")
    _private_directory(output.parent)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            job_id = await require_isolated_case(session, run_id)
        # A persistent attempt marker makes observation timeout/unknown outcomes non-replayable.
        output.mkdir(mode=0o700, exist_ok=False)
        _receipt(output / "started.json", {"status": "started", "run_id": str(run_id)})

        async def preclaim_guard(session: AsyncSession) -> None:
            if await require_isolated_case(session, run_id) != job_id:
                raise ValueError("DIAGNOSTIC_JOB_CHANGED")

        from nexus.worker import run_agent_worker_once

        started = time.monotonic()
        claimed = await run_agent_worker_once(
            settings, job_ids=(job_id,), preclaim_guard=preclaim_guard
        )
        elapsed = time.monotonic() - started
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            run = await session.get(AgentResearchRunRecord, run_id)
            job = await session.get(Job, job_id)
            assert run is not None and job is not None
            result: dict[str, object] = {
                "status": "observed" if claimed else "not_claimed",
                "run_id": str(run_id),
                "run_status": run.status,
                "job_status": job.status,
                "attempt_count": job.attempt_count,
                "worker_elapsed_seconds": round(elapsed, 3),
                "output_count": await session.scalar(
                    select(func.count())
                    .select_from(AgentResearchOutputRecord)
                    .where(AgentResearchOutputRecord.run_id == run_id)
                ),
                "usage_known": usage_is_known(run),
                "execution_locked": True,
                "formal_acceptance": False,
            }
        _receipt(output / "result.json", result)
        return result
    finally:
        await factory.kw["bind"].dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        from nexus.ops.openai_local import local_settings

        settings = local_settings(args.repo_root, backend="codex_sdk").model_copy(
            update={"database_url": read_database_url(args.database_url_file)}
        )
        result = asyncio.run(
            execute_diagnostic(settings, run_id=args.run_id, output=args.output_dir)
        )
    except Exception as error:
        # Never expose SQL URLs, source contents, account data or raw SDK exceptions.
        code = str(error) if isinstance(error, ValueError) else "UNKNOWN"
        if code not in _SAFE_ERRORS:
            code = "UNKNOWN"
        print(
            json.dumps(
                {"status": "DIAGNOSTIC_STOPPED", "code": code, "outcome": "UNKNOWN_NO_REPLAY"}
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
