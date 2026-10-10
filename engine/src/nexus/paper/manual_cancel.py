"""Cancel untouched research via native fenced Job finalization, with no provider call."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.agents.execution import _terminal_without_usage
from nexus.agents.persistence import require_reserved_daily_budget
from nexus.jobs.models import QueueName, job_input_hash
from nexus.jobs.repository import claim_next_job, finalize_job
from nexus.persistence.models.agents import (
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


async def cancel_unused_manual_research(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord,
    events: tuple[AgentPaperCycleEventRecord, ...],
    now: datetime,
) -> None:
    # Caller holds account/cycle locks and validated this manual terminal event chain.
    run_ids: set[UUID] = set()
    try:
        for event in events:
            raw = event.fact_ids.get("research_run_ids", [])
            if not isinstance(raw, list):
                raise ValueError
            run_ids.update(UUID(str(item)) for item in raw)
    except (ValueError, TypeError):
        raise ValueError("MANUAL_RESULT_UNKNOWN") from None
    if len(run_ids) > 5:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    jobs = tuple(
        await session.scalars(
            select(Job)
            .where(
                Job.job_type == "agent.paper.research",
                Job.payload["run_id"].as_string().in_([str(item) for item in run_ids]),
            )
            .order_by(Job.id)
            .with_for_update()
        )
    )
    if len(jobs) != len(run_ids):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    for job in jobs:
        if job.status != "queued":
            continue
        run = await session.get(
            AgentResearchRunRecord, UUID(str(job.payload["run_id"])), with_for_update=True
        )
        if run is None or (
            run.status != "prepared"
            or run.retry_count != 0
            or run.settled_cost_usd is not None
            or run.input_tokens is not None
            or run.output_tokens is not None
            or run.actor_id != "local-agent-paper-cycle"
            or run.idempotency_key != f"agent-paper:{cycle.id}:{run.instrument_id}"
            or run.decision_time != cycle.decision_time
            or job.attempt_count != 0
            or job.manual_retry_count != 0
            or job.payload != {"run_id": str(run.id)}
            or job.scope_id != str(run.instrument_id)
            or job.effective_at != cycle.decision_time
            or job.input_hash
            != job_input_hash(
                {
                    "instrument_id": str(run.instrument_id),
                    "data_snapshot_id": str(run.data_snapshot_id),
                    "decision_time": run.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "request_hash": run.input_hash,
                    "schema_hash": run.schema_hash,
                    "model_hash": run.model_hash,
                    "prompt_hash": run.prompt_hash,
                    "evidence_hash": run.evidence_hash,
                    "tool_hash": run.tool_hash,
                    "pricing_version": run.pricing_version,
                }
            )
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        if (
            await session.scalar(select(JobAttempt.id).where(JobAttempt.job_id == job.id).limit(1))
            or await session.scalar(select(JobLease.id).where(JobLease.job_id == job.id).limit(1))
            or await session.scalar(
                select(AgentResearchOutputRecord.id)
                .where(AgentResearchOutputRecord.run_id == run.id)
                .limit(1)
            )
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        research_events = tuple(
            await session.scalars(
                select(AgentResearchEventRecord)
                .where(AgentResearchEventRecord.run_id == run.id)
                .order_by(AgentResearchEventRecord.sequence)
            )
        )
        if any(event.state != "prepared" for event in research_events):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        await require_reserved_daily_budget(session, run_id=run.id)

        async def guard(_session: AsyncSession, selected: Job) -> bool:
            return selected.id == job.id and selected.attempt_count == 0

        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=f"manual-cancel:{cycle.id}",
            now=now,
            lease_seconds=210,
            job_ids=(job.id,),
            job_types=("agent.paper.research",),
            locked_job_guard=guard,
            research_budget_profile=FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        )
        if claim is None:
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        run.settled_cost_usd = Decimal("0")
        run.input_tokens = run.output_tokens = 0
        await session.flush()
        await _terminal_without_usage(
            session,
            run,
            output_sequence=1,
            event_sequence=len(research_events) + 1,
            status="abstained",
            reason="manual_unused_research_cancelled",
        )
        await finalize_job(
            session,
            claim=claim,
            now=now,
            outcome="skipped",
            result_code="MANUAL_UNUSED_RESEARCH_CANCELLED",
        )
