"""Safe preclaim facts and proof of an entirely unused, natively closed request."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.jobs.repository import job_lease_consumed
from nexus.paper.manual_request import read_manual_request
from nexus.paper.research_request import database_now, require_research_jobs_settled
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

PRECLAIM_CODES = frozenset(
    {"CODEX_CAPACITY_POOL_WIRE_EXCEEDED", "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"}
)


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
        leases = tuple(
            await session.scalars(select(JobLease).where(JobLease.job_id == member.job_id))
        )
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
            or attempts[0].worker_id != f"manual-cancel:{row.id}"
            or leases[0].worker_id != attempts[0].worker_id
            or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
