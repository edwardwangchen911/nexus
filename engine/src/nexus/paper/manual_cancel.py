"""Cancel untouched research via native fenced Job finalization, with no provider call."""

from datetime import datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import (
    ACTIONABLE_CONFIG_VERSIONS,
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
)
from nexus.agents.execution import _terminal_without_usage
from nexus.agents.persistence import require_reserved_daily_budget, try_lock_budget_day
from nexus.jobs.models import QueueName, job_input_hash
from nexus.jobs.repository import claim_next_job, finalize_job
from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    lock_portfolio_account,
    require_activation_projection_unoccupied,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


class TerminalCycleCancelReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: str = "unused_research_cancelled"
    cancelled_research: int
    execution_locked: bool = True


async def cancel_unused_manual_research(
    session: AsyncSession,
    *,
    cycle: AgentPaperCycleRecord | None,
    events: tuple[AgentPaperCycleEventRecord, ...],
    now: datetime,
    allow_legacy: bool = False,
    nonblocking: bool = False,
    research_request: ManualPaperRequest | None = None,
) -> int:
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
    if research_request is not None:
        from nexus.paper.research_request import read_research_manifest

        manifest = await read_research_manifest(session, research_request)
        if cycle is not None or events or manifest is None:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        run_ids = {member.run_id for member in manifest.members}
        owner_id, cutoff = research_request.id, manifest.research_as_of
        actor, prefix = (
            ("local-independent-monitor-research-v1", "independent-monitor")
            if research_request.contract_version == 3
            else ("local-research-only-v2", "research-only")
        )
    elif cycle is not None:
        owner_id, cutoff = cycle.id, cycle.decision_time
        actor, prefix = "local-agent-paper-cycle", "agent-paper"
    else:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if len(run_ids) > 5:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    query = (
        select(Job)
        .where(
            Job.job_type == "agent.paper.research",
            Job.payload["run_id"].as_string().in_([str(item) for item in run_ids]),
        )
        .order_by(Job.id)
    )
    jobs = tuple(await session.scalars(query if nonblocking else query.with_for_update()))
    if len(jobs) != len(run_ids):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if nonblocking:
        queued_ids = [UUID(str(job.payload["run_id"])) for job in jobs if job.status == "queued"]
        if not queued_ids:
            return 0
        days = tuple(
            await session.scalars(
                select(AgentDailyBudgetReservationRecord.business_date)
                .where(AgentDailyBudgetReservationRecord.run_id.in_(queued_ids))
                .distinct()
                .order_by(AgentDailyBudgetReservationRecord.business_date)
            )
        )
        for day in days:
            if not await try_lock_budget_day(session, day.isoformat()):
                return 0
        # Worker ownership wins: never wait for its Job lock or cancel its claim.
        jobs = tuple(
            await session.scalars(
                query.with_for_update(skip_locked=True).execution_options(populate_existing=True)
            )
        )
    cancelled = 0
    for job in jobs:
        if job.status != "queued":
            continue
        run = await session.get(
            AgentResearchRunRecord,
            UUID(str(job.payload["run_id"])),
            with_for_update=True,
            populate_existing=True,
        )
        if run is None or (
            run.status != "prepared"
            or run.retry_count != 0
            or run.settled_cost_usd is not None
            or run.input_tokens is not None
            or run.output_tokens is not None
            or run.actor_id != actor
            or run.idempotency_key != f"{prefix}:{owner_id}:{run.instrument_id}"
            or run.decision_time != cutoff
            or job.attempt_count != 0
            or job.manual_retry_count != 0
            or job.payload != {"run_id": str(run.id)}
            or job.scope_id != str(run.instrument_id)
            or job.effective_at != cutoff
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
        model = await session.get(AgentModelConfigRecord, run.model_config_id)
        if (
            model is None
            or model.config_hash != run.model_hash
            or model.version != run.pricing_version
        ):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        legacy = allow_legacy and run.pricing_version in ACTIONABLE_CONFIG_VERSIONS
        if not legacy and run.pricing_version not in {
            FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            FULL_INPUT_CAPACITY_CONFIG_VERSION,
            BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
            FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        }:
            raise ValueError("MANUAL_RESULT_UNKNOWN")

        async def guard(_session: AsyncSession, selected: Job) -> bool:
            return selected.id == job.id and selected.attempt_count == 0

        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=f"manual-cancel:{owner_id}",
            now=now,
            lease_seconds=30
            if legacy
            else 330
            if run.pricing_version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
            else 210,
            job_ids=(job.id,),
            job_types=("agent.paper.research",),
            locked_job_guard=guard,
            research_budget_profile=None if legacy else run.pricing_version,
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
        cancelled += 1
    return cancelled


async def close_terminal_cycle_unused_research(
    session: AsyncSession, *, account_id: UUID, cycle_id: UUID, now: datetime
) -> TerminalCycleCancelReceipt:
    """Cancel only untouched jobs of an original scheduled terminal; no replay."""
    await lock_portfolio_account(session, account_id)
    cycle = await session.scalar(
        select(AgentPaperCycleRecord)
        .where(AgentPaperCycleRecord.id == cycle_id, AgentPaperCycleRecord.account_id == account_id)
        .with_for_update()
    )
    if cycle is None or not cycle.execution_locked:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if await session.scalar(
        select(ManualPaperRequestEvent.request_id).where(
            ManualPaperRequestEvent.cycle_id == cycle_id
        )
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )
    prior = None
    for sequence, event in enumerate(events, start=1):
        if (
            event.sequence != sequence
            or event.from_status != prior
            or event.occurred_at > now
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=cycle_id,
                sequence=sequence,
                from_status=cast(CycleStatus | None, event.from_status),
                to_status=cast(CycleStatus, event.to_status),
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        prior = event.to_status
    if prior not in {"failed", "unavailable"}:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )
    cancelled = await cancel_unused_manual_research(
        session, cycle=cycle, events=events, now=now, allow_legacy=True
    )

    return TerminalCycleCancelReceipt(cancelled_research=cancelled)
