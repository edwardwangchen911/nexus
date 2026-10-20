"""Close one exactly bound formal synthesis orphan without retrying its provider call."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
from nexus.agents.portfolio_synthesis_worker import (
    formal_synthesis_job_qualified,
    read_synthesis_job,
)
from nexus.domain.time import require_utc
from nexus.jobs.models import JobKeyV1, JobType, canonical_job_key
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


async def recover_formal_synthesis_orphan(
    session: AsyncSession, *, cycle_id: UUID, now: datetime
) -> bool:
    """Caller holds account/cycle locks; only expired one-shot running claims qualify."""
    require_utc(now)
    found = await read_synthesis_job(session, cycle_id=cycle_id)
    if found is None:
        return False
    row = (
        await session.execute(
            select(Job, JobLease)
            .join(JobLease, JobLease.job_id == Job.id)
            .where(Job.id == found.id, Job.status == "running", JobLease.expires_at <= now)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        return False
    job, lease = row
    if not await formal_synthesis_job_qualified(
        session, job, now=now, claimed=True, expired_recovery=True
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_ORPHAN_BINDING_INVALID")
    run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(job.payload["run_id"]))
    assert run is not None
    attempts = list(
        await session.scalars(
            select(JobAttempt).where(JobAttempt.job_id == job.id).with_for_update()
        )
    )
    if (
        run.input_tokens is not None
        or run.output_tokens is not None
        or run.settled_cost_usd is not None
        or run.completed_at is not None
        or len(attempts) != 1
        or attempts[0].attempt_no != 1
        or attempts[0].outcome != "running"
        or attempts[0].lease_token != lease.lease_token
        or attempts[0].fence_token != lease.fence_token
        or attempts[0].worker_id != lease.worker_id
        or attempts[0].started_at != job.started_at
        or attempts[0].finished_at is not None
        or attempts[0].error_code is not None
        or job.started_at is None
        or job.started_at > lease.heartbeat_at
        or lease.heartbeat_at >= lease.expires_at
        or job.finished_at is not None
        or job.job_key
        != canonical_job_key(
            JobKeyV1(
                job_type=JobType.AGENT_PORTFOLIO_SYNTHESIS,
                scope_kind="portfolio",
                scope_id=job.scope_id,
                effective_at=job.effective_at,
                input_hash=run.input_hash,
            )
        )
        or await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord.id).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
        is not None
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_ORPHAN_BINDING_INVALID")
    attempt = attempts[0]
    attempt.outcome, attempt.finished_at, attempt.error_code = "lost", now, "LEASE_EXPIRED"
    job.status, job.finished_at, job.last_error_code = "dead_letter", now, "LEASE_EXPIRED"
    job.result_code = None
    job.version += 1
    await session.flush()
    await persist_portfolio_synthesis_terminal(
        session,
        run_id=run.id,
        input_hash=run.input_hash,
        status="failed",
        intent=None,
        input_tokens=None,
        output_tokens=None,
        now=now,
    )
    return True
