"""Dedicated short-transaction worker for bounded portfolio decisions."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from sqlalchemy import String, cast, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.codex_versions import PORTFOLIO_DECISION_LEASE_SECONDS
from nexus.agents.portfolio_decision_execution import (
    PortfolioDecisionExecutionAuthority,
    PortfolioDecisionExecutionError,
    load_portfolio_decision_execution_authority,
    mark_first_requested,
    mark_second_requested,
    persist_portfolio_decision_terminal,
    record_tool_result,
)
from nexus.agents.portfolio_decision_model import (
    PortfolioDecisionModelError,
    ResponsesPortfolioDecisionModel,
)
from nexus.agents.responses import ResponsesClient
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName
from nexus.jobs.repository import (
    ClaimedJob,
    JobCompletion,
    checkpoint_job,
    claim_next_job,
    dead_letter_job,
    execute_and_finalize_job,
    finalize_job,
    reclaim_expired_jobs,
    upsert_worker_heartbeat,
)
from nexus.market.ingestion import MarketObjectStore
from nexus.persistence.models.agents import PortfolioDecisionRunRecord
from nexus.persistence.models.runtime import Job

Clock = Callable[[], datetime]
_JOB_SCHEMA_VERSION = "portfolio_decision_job_v1"


async def run_portfolio_decision_worker_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    responses_client: ResponsesClient,
    worker_id: str,
    worker_started_at: datetime,
    process_version: str,
    migration_head: str,
    clock: Clock,
    scenario_evidence_store: MarketObjectStore,
    job_ids: tuple[UUID, ...] | None = None,
) -> bool:
    """Claim and complete at most one portfolio decision with no provider transaction."""
    claim_time = clock()
    async with factory() as session, session.begin():
        await upsert_worker_heartbeat(
            session,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            process_version=process_version,
            migration_head=migration_head,
            started_at=worker_started_at,
            seen_at=claim_time,
        )
        await reclaim_expired_jobs(
            session,
            queue=QueueName.GENERAL,
            now=claim_time,
            job_ids=job_ids,
        )
        await reconcile_orphaned_portfolio_decisions(session, now=claim_time)
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            now=claim_time,
            lease_seconds=PORTFOLIO_DECISION_LEASE_SECONDS,
            job_types=(JobType.AGENT_PORTFOLIO_DECISION.value,),
            job_ids=job_ids,
        )
    if claim is None:
        return False
    try:
        run_id = _claim_run_id(claim)
    except (TypeError, ValueError):
        async with factory() as session, session.begin():
            await dead_letter_job(
                session,
                claim=claim,
                now=clock(),
                error_code="HANDLER_PAYLOAD_INVALID",
            )
        return True

    async with factory() as session:
        run_exists = await session.get(PortfolioDecisionRunRecord, run_id) is not None
    if not run_exists:
        async with factory() as session, session.begin():
            await dead_letter_job(
                session,
                claim=claim,
                now=clock(),
                error_code="PORTFOLIO_DECISION_AUTHORITY_INVALID",
            )
        return True
    if not _claim_authority_valid(claim):
        await _finish_pre_provider_failure(
            factory,
            claim=claim,
            run_id=run_id,
            now=clock(),
            outcome="failed",
            result_code="PORTFOLIO_DECISION_FAILED",
        )
        return True
    if not claim.dependencies_satisfied:
        await _finish_pre_provider_failure(
            factory,
            claim=claim,
            run_id=run_id,
            now=clock(),
            outcome="skipped",
            result_code=claim.dependency_result_code or "DEPENDENCY_NOT_SATISFIED",
        )
        return True

    authority: PortfolioDecisionExecutionAuthority | None = None

    async def start(target: AsyncSession) -> None:
        nonlocal authority
        run = await target.get(PortfolioDecisionRunRecord, run_id, with_for_update=True)
        if run is None:
            raise ValueError("portfolio decision run is missing")
        authority = await load_portfolio_decision_execution_authority(
            target,
            run=run,
            scenario_evidence_store=scenario_evidence_store,
        )
        supports = getattr(responses_client, "supports_model_config", None)
        try:
            compatible = callable(supports) and supports(authority.model_config)
        except Exception as error:
            raise PortfolioDecisionExecutionError(
                "PORTFOLIO_DECISION_AUTHORITY_INVALID"
            ) from error
        if not compatible:
            raise PortfolioDecisionExecutionError("PORTFOLIO_DECISION_AUTHORITY_INVALID")
        await mark_first_requested(target, run_id=run_id)

    try:
        async with factory() as session, session.begin():
            await checkpoint_job(session, claim=claim, now=clock(), apply=start)
    except PortfolioDecisionExecutionError:
        await _finish_pre_provider_failure(
            factory,
            claim=claim,
            run_id=run_id,
            now=clock(),
            outcome="failed",
            result_code="PORTFOLIO_DECISION_FAILED",
        )
        return True
    if authority is None:
        raise RuntimeError("portfolio decision authority was not loaded")

    model = ResponsesPortfolioDecisionModel(
        client=responses_client,
        config=authority.model_config,
    )
    try:
        first = await model.request_scenario(
            context=authority.context,
            prompt=authority.prompt,
            tools=authority.tools,
        )
    except PortfolioDecisionModelError as error:
        await _finish_model_error(
            factory,
            claim=claim,
            run_id=run_id,
            error=error,
            now=clock(),
        )
        return True

    async def store_tool(target: AsyncSession) -> None:
        await record_tool_result(
            target,
            run_id=run_id,
            projection=first.projection,
            usage=model.usage,
        )

    async with factory() as session, session.begin():
        await checkpoint_job(session, claim=claim, now=clock(), apply=store_tool)

    async def start_second(target: AsyncSession) -> None:
        await mark_second_requested(target, run_id=run_id)

    async with factory() as session, session.begin():
        await checkpoint_job(session, claim=claim, now=clock(), apply=start_second)

    try:
        result = await model.request_intent(
            context=authority.context,
            prompt=authority.prompt,
            frozen=authority.frozen,
            first_turn=first,
        )
    except PortfolioDecisionModelError as error:
        await _finish_model_error(
            factory,
            claim=claim,
            run_id=run_id,
            error=error,
            now=clock(),
        )
        return True

    async def complete(target: AsyncSession) -> None:
        await persist_portfolio_decision_terminal(
            target,
            run_id=run_id,
            status="completed",
            error_code=None,
            intent=result.intent,
            usage=result.usage,
            usage_known=True,
        )

    async with factory() as session, session.begin():
        await finalize_job(
            session,
            claim=claim,
            now=clock(),
            outcome="succeeded",
            result_code="PORTFOLIO_DECISION_COMPLETED",
            apply=complete,
        )
    return True


async def _finish_model_error(
    factory: async_sessionmaker[AsyncSession],
    *,
    claim: ClaimedJob,
    run_id: UUID,
    error: PortfolioDecisionModelError,
    now: datetime,
) -> None:
    abstained = error.code in {
        "PORTFOLIO_DECISION_TOOL_REQUIRED",
        "PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH",
        "PORTFOLIO_DECISION_OUTPUT_INVALID",
    }

    async def persist(target: AsyncSession) -> None:
        await persist_portfolio_decision_terminal(
            target,
            run_id=run_id,
            status="abstained" if abstained else "failed",
            error_code=error.code,
            intent=None,
            usage=error.usage if error.usage_known else None,
            usage_known=error.usage_known,
        )

    async with factory() as session, session.begin():
        if abstained:
            await finalize_job(
                session,
                claim=claim,
                now=now,
                outcome="skipped",
                result_code="PORTFOLIO_DECISION_ABSTAINED",
                apply=persist,
            )
            return

        async def fail(target: AsyncSession) -> JobCompletion:
            await persist(target)
            return JobCompletion(
                outcome="failed",
                result_code="PORTFOLIO_DECISION_FAILED",
                finished_at=now,
                error_code=error.code,
            )

        await execute_and_finalize_job(
            session,
            claim=claim,
            checked_at=now,
            execute=fail,
        )


async def _finish_pre_provider_failure(
    factory: async_sessionmaker[AsyncSession],
    *,
    claim: ClaimedJob,
    run_id: UUID,
    now: datetime,
    outcome: str,
    result_code: str,
) -> None:
    async def persist(target: AsyncSession) -> None:
        await persist_portfolio_decision_terminal(
            target,
            run_id=run_id,
            status="failed",
            error_code="PORTFOLIO_DECISION_AUTHORITY_INVALID",
            intent=None,
            usage=UsageSnapshot(),
            usage_known=True,
        )

    async with factory() as session, session.begin():
        if outcome == "skipped":
            await finalize_job(
                session,
                claim=claim,
                now=now,
                outcome="skipped",
                result_code=result_code,
                apply=persist,
            )
            return

        async def fail(target: AsyncSession) -> JobCompletion:
            await persist(target)
            return JobCompletion(
                outcome="failed",
                result_code=result_code,
                finished_at=now,
                error_code="PORTFOLIO_DECISION_AUTHORITY_INVALID",
            )

        await execute_and_finalize_job(
            session,
            claim=claim,
            checked_at=now,
            execute=fail,
        )


async def reconcile_orphaned_portfolio_decisions(
    session: AsyncSession,
    *,
    now: datetime,
    limit: int = 100,
) -> int:
    """Fail terminal-Job orphans from durable checkpoints without provider replay."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must use UTC")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    job_ids = tuple(
        await session.scalars(
            select(Job.id)
            .join(
                PortfolioDecisionRunRecord,
                Job.payload["run_id"].as_string() == cast(PortfolioDecisionRunRecord.id, String),
            )
            .where(
                Job.job_type == JobType.AGENT_PORTFOLIO_DECISION.value,
                Job.status.in_(("failed", "dead_letter")),
                PortfolioDecisionRunRecord.status.in_(("prepared", "running")),
            )
            .order_by(Job.finished_at, Job.id)
            .limit(limit)
        )
    )
    reconciled = 0
    for job_id in job_ids:
        job = await session.get(Job, job_id, with_for_update=True)
        if job is None or job.status not in {"failed", "dead_letter"}:
            continue
        try:
            run_id = _run_id_from_payload(job)
        except (TypeError, ValueError):
            continue
        run = await session.get(PortfolioDecisionRunRecord, run_id, with_for_update=True)
        if run is None or run.status in {"completed", "abstained", "failed"}:
            continue
        usage_known = run.provider_phase not in {"first_requested", "second_requested"}
        usage = UsageSnapshot() if run.provider_phase == "none" else None
        await persist_portfolio_decision_terminal(
            session,
            run_id=run.id,
            status="failed",
            error_code="PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN",
            intent=None,
            usage=usage,
            usage_known=usage_known,
        )
        reconciled += 1
    return reconciled


def _claim_run_id(claim: ClaimedJob) -> UUID:
    raw_run_id = claim.payload.get("run_id")
    if not isinstance(raw_run_id, str):
        raise ValueError("portfolio decision Job run_id is invalid")
    return UUID(raw_run_id)


def _claim_authority_valid(claim: ClaimedJob) -> bool:
    return not (
        claim.job_type != JobType.AGENT_PORTFOLIO_DECISION.value
        or claim.schema_version != _JOB_SCHEMA_VERSION
        or claim.queue != QueueName.GENERAL.value
        or claim.priority != JOB_PRIORITIES[JobType.AGENT_PORTFOLIO_DECISION]
        or claim.retry_policy != "never"
        or claim.max_attempts != 1
        or claim.attempt_no != 1
        or set(claim.payload) != {"run_id"}
        or claim.dependencies_satisfied
        and claim.dependency_result_code is not None
        or not claim.dependencies_satisfied
        and claim.dependency_result_code is None
    )


def _run_id_from_payload(job: Job) -> UUID:
    raw_run_id = job.payload.get("run_id")
    if (
        job.schema_version != _JOB_SCHEMA_VERSION
        or job.queue != QueueName.GENERAL.value
        or job.retry_policy != "never"
        or job.max_attempts != 1
        or set(job.payload) != {"run_id"}
        or not isinstance(raw_run_id, str)
    ):
        raise ValueError("portfolio decision Job authority is invalid")
    return UUID(raw_run_id)
