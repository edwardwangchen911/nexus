"""Fence a precisely bound expired formal research attempt, without replay or settlement."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.execution import _persist_terminal
from nexus.agents.persistence import require_reserved_daily_budget
from nexus.domain.time import require_utc
from nexus.jobs.models import JOB_PRIORITIES, JobKeyV1, JobType, canonical_job_key
from nexus.persistence.models.agents import (
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


async def recover_formal_research_orphans(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord | None,
    run_ids: tuple[UUID, ...],
    now: datetime,
    research_request: ManualPaperRequest | None = None,
) -> int:
    """Caller holds the cycle/account lock; the expired fence never grants execution."""
    require_utc(now)
    if research_request is not None:
        from nexus.paper.research_request import read_research_manifest

        manifest = await read_research_manifest(session, research_request)
        if (
            cycle is not None
            or manifest is None
            or set(run_ids) != {m.run_id for m in manifest.members}
        ):
            raise ValueError("AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID")
        owner_id, cutoff = research_request.id, manifest.research_as_of
        actor, prefix = "local-research-only-v2", "research-only"
    elif cycle is not None:
        owner_id, cutoff = cycle.id, cycle.decision_time
        actor, prefix = "local-agent-paper-cycle", "agent-paper"
    else:
        raise ValueError("AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID")
    rows = (
        await session.execute(
            select(Job, JobLease)
            .join(JobLease, JobLease.job_id == Job.id)
            .where(
                Job.job_type == JobType.AGENT_PAPER_RESEARCH.value,
                Job.payload["run_id"].as_string().in_(tuple(str(value) for value in run_ids)),
                Job.status == "running",
                JobLease.expires_at <= now,
            )
            .order_by(Job.id)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).all()
    for job, lease in rows:
        if not await formal_stage_job_qualified(session, job, claimed=True):
            raise ValueError("AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID")
        run = await session.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
        assert run is not None
        attempts = list(
            await session.scalars(
                select(JobAttempt).where(JobAttempt.job_id == job.id).with_for_update()
            )
        )
        events = list(
            await session.scalars(
                select(AgentResearchEventRecord)
                .where(AgentResearchEventRecord.run_id == run.id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
        bound_jobs = list(
            await session.scalars(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
        )
        if (
            run.id not in run_ids
            or run.actor_id != actor
            or run.idempotency_key != f"{prefix}:{owner_id}:{run.instrument_id}"
            or run.decision_time != cutoff
            or any(
                value is not None
                for value in (run.input_tokens, run.output_tokens, run.settled_cost_usd)
            )
            or bound_jobs != [job.id]
            or job.priority != JOB_PRIORITIES[JobType.AGENT_PAPER_RESEARCH]
            or job.started_at is None
            or job.finished_at is not None
            or job.started_at != lease.acquired_at
            or not lease.acquired_at <= lease.heartbeat_at < lease.expires_at
            or len(attempts) != 1
            or attempts[0].attempt_no != 1
            or attempts[0].outcome != "running"
            or attempts[0].lease_token != lease.lease_token
            or attempts[0].fence_token != lease.fence_token
            or attempts[0].worker_id != lease.worker_id
            or attempts[0].started_at != job.started_at
            or attempts[0].finished_at is not None
            or attempts[0].error_code is not None
            or bool(events)
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
            or await session.scalar(
                select(AgentResearchOutputRecord.id).where(
                    AgentResearchOutputRecord.run_id == run.id
                )
            )
            is not None
        ):
            raise ValueError("AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID")
        await require_reserved_daily_budget(session, run_id=run.id)
        attempt = attempts[0]
        attempt.outcome, attempt.finished_at, attempt.error_code = "lost", now, "LEASE_EXPIRED"
        job.status, job.finished_at, job.last_error_code = "dead_letter", now, "LEASE_EXPIRED"
        job.result_code = None
        job.version += 1
        await session.flush()
        # This is a local lost-attempt observation. No upstream error, zero usage,
        # frozen SDK input, or user acceptance may be inferred from an absent result.
        await _persist_terminal(
            session,
            run,
            output_sequence=1,
            event_sequence=1,
            status="failed",
            reason="RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN",
        )
    return len(rows)
