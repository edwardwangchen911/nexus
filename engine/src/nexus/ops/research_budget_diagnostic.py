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

from nexus.agents.codex_versions import (
    CANDIDATE_BUDGET_CONFIG_VERSION,
    DEADLINE_BUDGET_CONFIG_VERSION,
)
from nexus.agents.committee import StageFailureDiagnostic, StageInvocationTiming
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
        "DIAGNOSTIC_SYNTHETIC_LOAD_INPUT_INVALID",
        "DIAGNOSTIC_TIMING_INVALID",
        "DIAGNOSTIC_CANDIDATE_ARGUMENT_INVALID",
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


def require_fresh_identity(
    run: AgentResearchRunRecord, job: Job, run_id: UUID, *, candidate_stage_capacity: bool = False
) -> None:
    if type(candidate_stage_capacity) is not bool:
        raise ValueError("DIAGNOSTIC_CANDIDATE_ARGUMENT_INVALID")
    if (
        run.status != "prepared"
        or run.pricing_version
        != (
            CANDIDATE_BUDGET_CONFIG_VERSION
            if candidate_stage_capacity
            else DEADLINE_BUDGET_CONFIG_VERSION
        )
        or run.actor_id != "local-research-cli"
        or not run.idempotency_key.startswith("budget-diagnostic:")
    ):
        raise ValueError("DIAGNOSTIC_FRESH_RUN_REQUIRED")
    if (
        job.job_type != ("agent.paper.research" if candidate_stage_capacity else "agent.research")
        or job.queue != "general"
        or job.status != "queued"
        or job.attempt_count != 0
        or job.manual_retry_count != 0
        or job.max_attempts != 1
        or job.retry_policy != "automatic_safe"
        or job.payload.get("run_id") != str(run_id)
    ):
        raise ValueError("DIAGNOSTIC_FRESH_JOB_REQUIRED")


async def require_synthetic_database(
    session: AsyncSession, *, configured_database: str | None = None
) -> None:
    """Check the actual target and absence of private/business state before writes."""
    database = await session.scalar(text("SELECT current_database()"))
    if (
        not isinstance(database, str)
        or not database.startswith("nexus_diag_")
        or configured_database is not None
        and configured_database != database
    ):
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


async def require_isolated_case(
    session: AsyncSession,
    run_id: UUID,
    *,
    candidate_stage_capacity: bool = False,
    configured_database: str | None = None,
) -> UUID:
    await require_synthetic_database(session, configured_database=configured_database)
    runs = list(await session.scalars(select(AgentResearchRunRecord)))
    jobs = list(await session.scalars(select(Job)))
    if len(runs) != 1 or runs[0].id != run_id or len(jobs) != 1:
        raise ValueError("DIAGNOSTIC_SINGLE_CASE_REQUIRED")
    require_fresh_identity(
        runs[0], jobs[0], run_id, candidate_stage_capacity=candidate_stage_capacity
    )
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


def timing_metadata(records: list[AgentResearchOutputRecord]) -> dict[str, object]:
    """Return schema-checked timings only, never model content or raw exception text."""
    timings: dict[str, object] = {}
    diagnostic: dict[str, object] | None = None
    try:
        for record in records:
            stages = record.payload.get("stage_timings", {})
            if not isinstance(stages, dict) or set(stages) - {"research", "critique", "synthesis"}:
                raise ValueError("invalid stages")
            for stage, value in stages.items():
                timings[stage] = StageInvocationTiming.model_validate(value).model_dump(mode="json")
            if record.payload.get("diagnostic") is not None:
                diagnostic = StageFailureDiagnostic.model_validate(
                    record.payload["diagnostic"]
                ).model_dump(mode="json", exclude={"response_hash"})
    except ValueError:
        raise ValueError("DIAGNOSTIC_TIMING_INVALID") from None
    return {"stage_timings": timings, "failure_diagnostic": diagnostic}


def usage_is_known(run: AgentResearchRunRecord) -> bool:
    """Only settled complete counters prove known usage; held counters can be lower bounds."""
    return (
        run.settled_cost_usd is not None
        and run.input_tokens is not None
        and run.output_tokens is not None
    )


async def execute_diagnostic(
    settings: EngineSettings,
    *,
    run_id: UUID,
    output: Path,
    synthetic_paper_load: bool = False,
    candidate_stage_capacity: bool = False,
) -> dict[str, object]:
    if type(candidate_stage_capacity) is not bool:
        raise ValueError("DIAGNOSTIC_CANDIDATE_ARGUMENT_INVALID")
    require_diagnostic_url(settings.database_url or "")
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("DIAGNOSTIC_SDK_REQUIRED")
    _private_directory(output.parent)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            job_id = await require_isolated_case(
                session,
                run_id,
                candidate_stage_capacity=candidate_stage_capacity,
                configured_database=make_url(settings.database_url or "").database,
            )
        # A persistent attempt marker makes observation timeout/unknown outcomes non-replayable.
        output.mkdir(mode=0o700, exist_ok=False)
        _receipt(output / "started.json", {"status": "started", "run_id": str(run_id)})

        async def preclaim_guard(session: AsyncSession) -> None:
            if (
                await require_isolated_case(
                    session,
                    run_id,
                    candidate_stage_capacity=candidate_stage_capacity,
                    configured_database=make_url(settings.database_url or "").database,
                )
                != job_id
            ):
                raise ValueError("DIAGNOSTIC_JOB_CHANGED")

        from nexus.worker import run_agent_worker_once

        started = time.monotonic()
        if synthetic_paper_load:
            from nexus.agents.database_tools import database_read_only_tools
            from nexus.ops.synthetic_paper_load import SyntheticPaperLoadTools

            async with factory() as source, source.begin():
                await source.execute(text("SET TRANSACTION READ ONLY"))
                source_run = await source.get(AgentResearchRunRecord, run_id)
                assert source_run is not None
                base = await database_read_only_tools(
                    source,
                    source_run,
                    allow_fixture_evidence=False,
                    allow_fixture_price_evidence=False,
                )
                tools = SyntheticPaperLoadTools(
                    base,
                    instrument_id=source_run.instrument_id,
                    decision_time=source_run.decision_time,
                )
                claimed = await run_agent_worker_once(
                    settings,
                    tools=tools,
                    job_ids=(job_id,),
                    preclaim_guard=preclaim_guard,
                    candidate_stage_capacity=candidate_stage_capacity,
                )
        else:
            claimed = await run_agent_worker_once(
                settings,
                job_ids=(job_id,),
                preclaim_guard=preclaim_guard,
                candidate_stage_capacity=candidate_stage_capacity,
            )
        elapsed = time.monotonic() - started
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            run = await session.get(AgentResearchRunRecord, run_id)
            job = await session.get(Job, job_id)
            assert run is not None and job is not None
            records = list(
                await session.scalars(
                    select(AgentResearchOutputRecord)
                    .where(AgentResearchOutputRecord.run_id == run_id)
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            result: dict[str, object] = {
                "status": "observed" if claimed else "not_claimed",
                "run_id": str(run_id),
                "run_status": run.status,
                "job_status": job.status,
                "attempt_count": job.attempt_count,
                "worker_elapsed_seconds": round(elapsed, 3),
                "output_count": len(records),
                **timing_metadata(records),
                "usage_known": usage_is_known(run),
                "execution_locked": True,
                "formal_acceptance": False,
                "candidate_stage_capacity": candidate_stage_capacity,
                "paper_context_kind": "synthetic_load_only" if synthetic_paper_load else "absent",
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
    parser.add_argument(
        "--candidate-stage-capacity",
        action="store_true",
        help="显式验证隔离 v7 候选 Paper 研究；不注册或激活正式配置",
    )
    parser.add_argument(
        "--synthetic-paper-load",
        action="store_true",
        help="诊断合成Paper输入负载；不创建Paper周期或权威，不代表正式验收",
    )
    args = parser.parse_args()
    try:
        from nexus.ops.openai_local import local_settings

        settings = local_settings(args.repo_root, backend="codex_sdk").model_copy(
            update={"database_url": read_database_url(args.database_url_file)}
        )
        result = asyncio.run(
            execute_diagnostic(
                settings,
                run_id=args.run_id,
                output=args.output_dir,
                synthetic_paper_load=args.synthetic_paper_load,
                candidate_stage_capacity=args.candidate_stage_capacity,
            )
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
