"""Normal single-security monitor admission; no model calls or execution authority."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.models import job_input_hash
from nexus.monitoring.context import read_monitor_context
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


def increment_manifest_hash(member: MonitorIncrement, run: AgentResearchRunRecord, job: Job) -> str:
    return job_input_hash(
        {
            "purpose": "price_monitor_increment_v1",
            "account_id": str(member.account_id),
            "instrument_id": str(member.instrument_id),
            "trigger_id": str(member.trigger_id),
            "plan_id": str(member.plan_id),
            "run_id": str(run.id),
            "job_id": str(job.id),
            "snapshot_id": str(member.snapshot_id),
            "research_as_of": member.research_as_of.isoformat(),
            "session_date": member.session_date.isoformat(),
            "reserved_sdk_calls": member.reserved_sdk_calls,
            "daily_sdk_call_limit": member.daily_sdk_call_limit,
            "context_hash": member.context_hash,
            "job_input_hash": job.input_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "input_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "tool_hash": run.tool_hash,
        }
    )


async def require_increment_binding(
    session: AsyncSession, run: AgentResearchRunRecord
) -> MonitorIncrement:
    from nexus.agents.codex_formal_stage import _formal_stage_job_hash

    invalid = "MONITOR_INCREMENT_BINDING_INVALID"
    member = await session.scalar(select(MonitorIncrement).where(MonitorIncrement.run_id == run.id))
    if member is None:
        raise ValueError(invalid)
    job = await session.get(Job, member.job_id)
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    bound = await read_monitor_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if (
        job is None
        or trigger is None
        or bound is None
        or member.snapshot_id != run.data_snapshot_id
        or member.instrument_id != run.instrument_id
        or member.research_as_of != run.decision_time
        or member.account_id != bound[0].account_id
        or member.plan_id != bound[0].plan_id
        or member.context_hash != bound[1].content_hash
        or member.trigger_id != bound[0].trigger_checks[0].trigger_id
        or member.reserved_sdk_calls != 3
        or not 3 <= member.daily_sdk_call_limit <= 90
        or trigger.research_run_id != run.id
        or trigger.status not in {"admitted", "completed", "failed", "unknown"}
        or job.payload != {"run_id": str(run.id)}
        or job.job_type != "agent.paper.research"
        or job.schema_version != "agent_research_job_v1"
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.scope_id != str(run.instrument_id)
        or job.scope_kind != "instrument"
        or job.effective_at != run.decision_time
        or job.input_hash != _formal_stage_job_hash(run)
        or member.manifest_hash != increment_manifest_hash(member, run, job)
    ):
        raise ValueError(invalid)
    return member


async def require_monitor_account_available(
    session: AsyncSession, *, account_id: UUID, now: datetime
) -> None:
    """Shared by manual, scheduled, and monitor admission; unknown remains occupied."""
    from nexus.paper.target_service import lock_portfolio_account

    await lock_portfolio_account(session, account_id)
    members = tuple(
        await session.scalars(
            select(MonitorIncrement).where(MonitorIncrement.account_id == account_id)
        )
    )
    for member in members:
        run = await session.get(AgentResearchRunRecord, member.run_id)
        job = await session.get(Job, member.job_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == member.run_id
            )
        )
        if (
            run is None
            or job is None
            or reservation is None
            or run.status not in {"completed", "failed", "abstained"}
            or job.status not in {"succeeded", "failed"}
            or job.finished_at is None
            or job.finished_at > now
            or run.settled_cost_usd is None
            or run.input_tokens is None
            or run.output_tokens is None
            or reservation.state != "settled"
            or reservation.settled_usd != run.settled_cost_usd
        ):
            raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
        leases = tuple(
            await session.scalars(
                select(JobLease).where(JobLease.job_id == job.id, JobLease.expires_at > now)
            )
        )
        for lease in leases:
            consumed = await session.scalar(
                select(JobAttempt.id)
                .where(
                    JobAttempt.job_id == job.id,
                    JobAttempt.lease_token == lease.lease_token,
                    JobAttempt.fence_token == lease.fence_token,
                    JobAttempt.attempt_no == job.attempt_count,
                    JobAttempt.outcome == job.status,
                    JobAttempt.finished_at == job.finished_at,
                    JobAttempt.finished_at <= now,
                )
                .limit(1)
            )
            if consumed is None or job.attempt_count != 1 or job.manual_retry_count != 0:
                raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
