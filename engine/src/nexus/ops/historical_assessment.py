"""Consume one existing archive into an explicitly isolated business database."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.paper import agent_cycle
from nexus.paper.historical_assessment import (
    SAFE_ARCHIVE_CODES,
    admit_archive,
    archive_registry,
    require_isolated,
    source_for_cycle,
    validate_research,
)
from nexus.paper.historical_assessment_source import read_archive
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt
from nexus.settings import EngineSettings


async def execute(
    database_url: str,
    archive_path: Path,
    *,
    settings: EngineSettings | None = None,
    synthesis_observation: bool = False,
) -> dict[str, object]:
    if synthesis_observation:
        from nexus.ops.synthesis_observation import read_observation

        archive = read_observation(archive_path)
    else:
        archive = read_archive(archive_path)
    engine = create_async_engine(database_url)
    runtime = (settings or EngineSettings()).model_copy(update={"database_url": database_url})
    sessions = async_sessionmaker(engine, expire_on_commit=False, info={"engine_settings": runtime})
    try:
        async with sessions.begin() as session:
            await require_isolated(session)
            cycle_id, jobs = await admit_archive(session, archive)
        started = datetime.now(UTC)
        for job_id in jobs:
            consumed = await run_job_worker_once(
                sessions,
                registry=archive_registry(),
                queue=QueueName.GENERAL,
                worker_id=f"archive-{cycle_id}",
                worker_started_at=started,
                process_version="historical-archive-consumer-v1",
                migration_head=CURRENT_MIGRATION_HEAD,
                clock=lambda: datetime.now(UTC),
                claim_registered_only=True,
                job_ids=(job_id,),
            )
            async with sessions.begin() as session:
                job = await session.get(Job, job_id)
                if not consumed or job is None or job.status != "succeeded":
                    code = job.last_error_code if job is not None else None
                    raise ValueError(code if code in SAFE_ARCHIVE_CODES else "ARCHIVE_JOB_FAILED")
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            now = await session.scalar(select(func.clock_timestamp()))
            if cycle is None:
                raise ValueError("ARCHIVE_CYCLE_MISSING")
            source = await source_for_cycle(session, cycle_id)
            if source is None or not isinstance(now, datetime):
                raise ValueError("ARCHIVE_SOURCE_MISSING")
            projection = await validate_research(session, source, at=now)
            result = await agent_cycle._resume_portfolio_synthesis(
                session,
                cycle=cycle,
                run_ids=tuple(UUID(row["run_id"]) for row in projection["research"]),
                events=await agent_cycle._events(session, cycle_id),
                now=now,
                daily_limit_usd=None,
            )
            if result.result_code != "PORTFOLIO_ASSESSMENT_DEFERRED":
                raise ValueError("ARCHIVE_CLOSEOUT_INCOMPLETE")
            attempts = await session.scalar(
                select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id.in_(jobs))
            )
            if attempts != 4:
                raise ValueError("ARCHIVE_ATTEMPT_BINDING_INVALID")
        return dict(
            status="completed",
            cycle_id=str(cycle_id),
            job_count=4,
            model_calls=0,
            result_code="PORTFOLIO_ASSESSMENT_DEFERRED",
            execution_allowed=False,
            archive_hash=archive["archive_hash"],
        )
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--case-dir", type=Path)
    sources.add_argument("--observation-dir", type=Path)
    parser.add_argument("--source-repo", type=Path, required=True)
    args = parser.parse_args()
    safe_codes = SAFE_ARCHIVE_CODES
    if args.observation_dir is not None:
        from nexus.ops.synthesis_observation import SAFE_CODES as OBSERVATION_CODES

        safe_codes = safe_codes | OBSERVATION_CODES
    try:
        from nexus.ops.openai_local import local_settings

        runtime = local_settings(args.source_repo, load_model_key=False)
        result = asyncio.run(
            execute(
                EngineSettings().database_url or "",
                args.observation_dir if args.observation_dir else args.case_dir / "case.sqlite3",
                synthesis_observation=args.observation_dir is not None,
                settings=runtime,
            )
        )
    except Exception as error:
        code = error.args[0] if len(error.args) == 1 else None
        safe = code if isinstance(code, str) and code in safe_codes else "UNKNOWN"
        print(json.dumps({"status": "failed", "code": safe}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
