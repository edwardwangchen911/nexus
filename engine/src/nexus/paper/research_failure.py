"""Safe preclaim facts and proof of an entirely unused, natively closed request."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.jobs.repository import job_lease_consumed
from nexus.paper.manual_request import ResearchMemberView, read_manual_request
from nexus.paper.research_request import database_now, require_research_jobs_settled
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

PRECLAIM_CODES = frozenset(
    {"CODEX_CAPACITY_POOL_WIRE_EXCEEDED", "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"}
)


async def require_claimed_uncalled_research_closed(
    session: AsyncSession, *, run_id: UUID, job_id: UUID, now: datetime
) -> None:
    """Prove the native pure-preparation stop, never infer zero calls from absence.

    stop_uncalled_research is the only native path with this exact two-event
    stopped shape before a running event. The Job must finalize in that same
    transaction; an expired or terminal-looking lease alone is insufficient.
    """
    from nexus.agents.execution import _hash

    invalid = "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
    run = await session.get(AgentResearchRunRecord, run_id)
    job = await session.get(Job, job_id)
    budget = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run_id
        )
    )
    attempts = tuple(await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id)))
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == job_id)))
    events = tuple(
        await session.scalars(
            select(AgentResearchEventRecord)
            .where(AgentResearchEventRecord.run_id == run_id)
            .order_by(AgentResearchEventRecord.sequence)
        )
    )
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
    )
    if (
        run is None
        or job is None
        or budget is None
        or run.status != "abstained"
        or run.retry_count != 0
        or (run.input_tokens, run.output_tokens, run.settled_cost_usd) != (0, 0, 0)
        or budget.state != "released"
        or budget.settled_usd is not None
        or budget.reserved_usd != run.reserved_cost_usd
        or job.job_type != "agent.paper.research"
        or job.payload != {"run_id": str(run_id)}
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.status != "skipped"
        or job.result_code != "AGENT_ABSTAINED"
        or job.attempt_count != 1
        or job.manual_retry_count != 0
        or len(attempts) != 1
        or len(leases) != 1
        or len(events) != 2
        or len(outputs) != 1
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
    ):
        raise ValueError(invalid)
    reason = {"reason": "model_research_stopped"}
    for event, sequence, state, details in (
        (events[0], 1, "prepared", {}),
        (events[1], 2, "abstained", reason),
    ):
        if (
            event.sequence != sequence
            or event.state != state
            or event.details != details
            or event.event_hash != _hash({"state": state, "details": details})
            or event.created_at > now
        ):
            raise ValueError(invalid)
    output = outputs[0]
    if (
        output.sequence != 1
        or output.status != "abstained"
        or output.role is not None
        or output.prompt_hash is not None
        or output.intent is not None
        or output.payload != reason
        or output.created_at > now
        or output.content_hash
        != _hash({"status": "abstained", "role": None, "payload": reason, "intent": None})
    ):
        raise ValueError(invalid)


class CapacityPreclaimRejected(ValueError):
    """Constructed only at the input-only Worker preclaim boundary."""

    def __init__(self, code: str):
        if code not in PRECLAIM_CODES:
            raise ValueError("RESEARCH_FAILURE_UNCLASSIFIED")
        self.code = code
        super().__init__(code)


async def record_research_failure(
    session: AsyncSession,
    *,
    request_id: UUID,
    error: BaseException,
    preparation: bool = False,
    cleanup: bool = False,
) -> None:
    row = await session.get(ManualPaperRequest, request_id)
    if row is None or row.contract_version not in {2, 3}:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    code = (
        error.code
        if not preparation
        and not cleanup
        and isinstance(error, CapacityPreclaimRejected)
        and error.code in PRECLAIM_CODES
        else None
    )
    await append_audit_event(
        session,
        actor_id="manual-research-host",
        action="research.request_failure",
        schema_version="research_request_failure_v1",
        occurred_at=await database_now(session),
        payload={
            "request_id": str(row.id),
            "request_hash": row.request_hash,
            "failure_phase": "research_cleanup"
            if cleanup
            else "research_preparation"
            if preparation
            else "research_preclaim"
            if code is not None
            else "research_dispatch",
            "failure_code": code or "RESEARCH_FAILURE_UNCLASSIFIED",
        },
    )


async def require_unused_request_closed(session: AsyncSession, *, request_id: UUID) -> None:
    row = await session.get(ManualPaperRequest, request_id)
    if row is None or row.contract_version not in {2, 3}:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    await lock_portfolio_account(session, row.account_id)
    view = await read_manual_request(session, row)
    now = await database_now(session)
    if view.status != "unavailable" or view.research is None or view.cycle_id is not None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    await require_research_jobs_settled(session, row=row, view=view.research, now=now)
    for member in view.research.members:
        await require_unused_research_member_closed(
            session, owner_id=row.id, member=member, now=now
        )


async def require_unused_research_member_closed(
    session: AsyncSession, *, owner_id: UUID, member: ResearchMemberView, now: datetime
) -> None:
    """Original exact native cancellation proof, reusable for a single manifest member."""
    run = await session.get(AgentResearchRunRecord, member.run_id)
    job = await session.get(Job, member.job_id)
    budget = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == member.run_id
        )
    )
    attempts = tuple(
        await session.scalars(select(JobAttempt).where(JobAttempt.job_id == member.job_id))
    )
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == member.job_id)))
    if (
        run is None
        or job is None
        or budget is None
        or run.status != "abstained"
        or (run.input_tokens, run.output_tokens, run.settled_cost_usd) != (0, 0, 0)
        or budget.state != "released"
        or budget.settled_usd is not None
        or job.status != "skipped"
        or job.result_code != "MANUAL_UNUSED_RESEARCH_CANCELLED"
        or run.retry_count != 0
        or job.attempt_count != 1
        or job.manual_retry_count != 0
        or len(attempts) != 1
        or len(leases) != 1
        or attempts[0].worker_id != f"manual-cancel:{owner_id}"
        or leases[0].worker_id != attempts[0].worker_id
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
