"""Proof of native local loss; never evidence of SDK completion or zero usage."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution import _hash
from nexus.jobs.models import JOB_PRIORITIES, JobKeyV1, JobType, canonical_job_key
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


async def _legacy_require_expired_research_proof(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    reservation: AgentDailyBudgetReservationRecord,
    job: Job,
    attempt: JobAttempt,
    lease: JobLease,
    outputs: tuple[AgentResearchOutputRecord, ...],
    events: tuple[AgentResearchEventRecord, ...],
    now: datetime,
    _verified_stage_usage: tuple[int | None, int | None] | None = None,
) -> None:
    """Called only after the common frozen cycle/model/budget/claim checks."""
    bound_jobs = tuple(
        await session.scalars(
            select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
        )
    )
    if (
        run.status != "failed"
        or (run.input_tokens, run.output_tokens) != (_verified_stage_usage or (None, None))
        or run.settled_cost_usd is not None
        or reservation.state != "reserved"
        or reservation.run_id != run.id
        or bound_jobs != (job.id,)
        or attempt.job_id != job.id
        or lease.job_id != job.id
        or attempt.lease_token != lease.lease_token
        or attempt.fence_token != lease.fence_token
        or job.status != "dead_letter"
        or job.queue != "general"
        or job.schema_version != "agent_research_job_v1"
        or job.scope_kind != "instrument"
        or job.priority != JOB_PRIORITIES[JobType.AGENT_PAPER_RESEARCH]
        or job.last_error_code != "LEASE_EXPIRED"
        or job.result_code is not None
        or job.started_at is None
        or job.started_at != lease.acquired_at
        or not lease.acquired_at <= lease.heartbeat_at < lease.expires_at <= now
        or attempt.outcome != "lost"
        or attempt.error_code != "LEASE_EXPIRED"
        or attempt.started_at != job.started_at
        or attempt.finished_at != job.finished_at
        or attempt.finished_at is None
        or attempt.finished_at < lease.expires_at
        or attempt.worker_id != lease.worker_id
        or job.job_key
        != canonical_job_key(
            JobKeyV1(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                scope_kind="instrument",
                scope_id=job.scope_id,
                effective_at=job.effective_at,
                input_hash=job.input_hash,
            )
        )
        or len(outputs) != 1
        or len(events) != 1
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    output, event = outputs[0], events[0]
    reason = {"reason": "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"}
    if (
        output.run_id != run.id
        or event.run_id != run.id
        or output.sequence != 1
        or output.role is not None
        or output.prompt_hash is not None
        or output.status != "failed"
        or output.intent is not None
        or output.payload != reason
        or output.content_hash
        != _hash({"status": "failed", "role": None, "payload": reason, "intent": None})
        or event.sequence != 1
        or event.state != "failed"
        or event.details != reason
        or event.event_hash != _hash({"state": "failed", "details": reason})
        or output.created_at > now
        or event.created_at > now
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def require_expired_research_proof(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    reservation: AgentDailyBudgetReservationRecord,
    job: Job,
    attempt: JobAttempt,
    lease: JobLease,
    outputs: tuple[AgentResearchOutputRecord, ...],
    events: tuple[AgentResearchEventRecord, ...],
    now: datetime,
) -> None:
    from nexus.agents.research_stages.public import _expired_proof

    await _expired_proof(
        session,
        run=run,
        reservation=reservation,
        job=job,
        attempt=attempt,
        lease=lease,
        outputs=outputs,
        events=events,
        now=now,
    )
