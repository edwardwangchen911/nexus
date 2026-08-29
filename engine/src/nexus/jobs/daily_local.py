"""Local-only paper, Shadow, and performance adapters for the daily DAG."""

from __future__ import annotations

from datetime import date, timedelta
from uuid import UUID

from sqlalchemy import and_, any_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.manifest import AuditManifestObjectStore, publish_audit_chain_manifest
from nexus.evolution.jobs import resolve_due_strategy_rnd_jobs
from nexus.jobs.daily_payloads import (
    STAGE_SCHEMA,
    DailyPipelinePayload,
    DailyStagePayload,
    format_utc,
    parse_utc,
)
from nexus.jobs.daily_pipeline import (
    DailyTargetSet,
    build_daily_pipeline_registry,
)
from nexus.jobs.daily_profiles import PRIMARY_DAILY_PROFILE
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.jobs.repository import JobDependencyInput, enqueue_job
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.shadow.service import advance_shadow_session

LOCAL_SAFE_DAILY_PROFILE = PRIMARY_DAILY_PROFILE
_LOCAL_SAFE_STAGES = frozenset(
    {
        JobType.PAPER_EXECUTE_DUE,
        JobType.SHADOW_ADVANCE_DUE,
        JobType.PAPER_PERFORMANCE,
    }
)


def build_local_daily_pipeline_registry(
    *,
    audit_object_store: AuditManifestObjectStore | None = None,
    stage_specs: tuple[JobHandlerSpec, ...] = (),
    include_provider_stages: bool = False,
) -> JobRegistry:
    """Compose local paper adapters and optional audit storage, never broker or Agent."""
    enabled = (
        _LOCAL_SAFE_STAGES
        if audit_object_store is None
        else _LOCAL_SAFE_STAGES | {JobType.AUDIT_MANIFEST}
    )
    enabled = frozenset((*enabled, *(spec.job_type for spec in stage_specs)))
    if include_provider_stages:
        enabled |= frozenset(
            {JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO}
        )

    async def execute(
        job_type: JobType,
        context: JobHandlerContext,
        payload: DailyStagePayload,
    ) -> JobHandlerResult:
        if job_type is JobType.AUDIT_MANIFEST:
            if audit_object_store is None:
                raise ValueError("audit manifest object store is unavailable")
            await publish_audit_chain_manifest(
                context.session,
                session_date=date.fromisoformat(payload.session),
                object_store=audit_object_store,
            )
            return JobHandlerResult(
                outcome="succeeded",
                result_code="AUDIT_MANIFEST_PUBLISHED",
            )
        return await _execute_local_daily_stage(job_type, context, payload)

    return build_daily_pipeline_registry(
        profile_id=LOCAL_SAFE_DAILY_PROFILE,
        stage_executor=execute,
        enabled_stage_types=frozenset(enabled),
        target_resolver=_resolve_local_targets,
        stage_specs=stage_specs,
    )


async def _resolve_local_targets(
    session: AsyncSession,
    root: DailyPipelinePayload,
) -> DailyTargetSet:
    return DailyTargetSet(
        paper_execution=await _paper_execution_target_ids(session, root=root),
        shadow=await _shadow_target_ids(session, root=root),
        performance=await _performance_target_ids(session),
        strategy_rnd=await resolve_due_strategy_rnd_jobs(
            session,
            session_date=date.fromisoformat(root.session),
            now=parse_utc(root.planned_at),
        ),
    )


async def _paper_execution_target_ids(
    session: AsyncSession,
    *,
    root: DailyPipelinePayload,
) -> tuple[UUID, ...]:
    if root.next_session is None:
        return ()
    latest_events = (
        select(
            PaperOrderEventRecord.order_id.label("order_id"),
            func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
        )
        .group_by(PaperOrderEventRecord.order_id)
        .subquery()
    )
    return tuple(
        (
            await session.scalars(
                select(PaperOrderRecord.id)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id
                    == PaperOrderRecord.strategy_decision_id,
                )
                .join(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
                .join(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == latest_events.c.order_id,
                        PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                    ),
                )
                .where(
                    StrategyDecisionRecord.execution_session
                    == date.fromisoformat(root.next_session),
                    PaperOrderEventRecord.to_status.in_(
                        ("submitted", "partially_filled")
                    ),
                )
                .order_by(PaperOrderRecord.id)
            )
        ).all()
    )


async def _shadow_target_ids(
    session: AsyncSession,
    *,
    root: DailyPipelinePayload,
) -> tuple[UUID, ...]:
    session_date = date.fromisoformat(root.session)
    return tuple(
        (
            await session.scalars(
                select(OptimizationProposalGroupRecord.id)
                .join(
                    ShadowGroupRecord,
                    ShadowGroupRecord.proposal_group_id
                    == OptimizationProposalGroupRecord.id,
                )
                .where(
                    OptimizationProposalGroupRecord.status == "shadowing",
                    any_(ShadowGroupRecord.evaluation_sessions) == session_date,
                )
                .order_by(OptimizationProposalGroupRecord.id)
            )
        ).all()
    )


async def _performance_target_ids(
    session: AsyncSession,
) -> tuple[UUID, ...]:
    return tuple(
        (
            await session.scalars(
                select(PaperAccountRecord.id)
                .where(
                    PaperAccountRecord.capability == "simulation_write_internal"
                )
                .order_by(PaperAccountRecord.id)
            )
        ).all()
    )


async def _execute_local_daily_stage(
    job_type: JobType,
    context: JobHandlerContext,
    payload: DailyStagePayload,
) -> JobHandlerResult:
    if payload.target_id is None:
        raise ValueError("local daily stage requires a frozen target")
    target_id = UUID(payload.target_id)
    if job_type is JobType.PAPER_EXECUTE_DUE:
        paper_result = await advance_internal_paper_order(
            context.session,
            order_id=target_id,
            evaluated_at=parse_utc(payload.scheduled_for),
            idempotency_key=f"daily-job-{context.claim.job_id}",
        )
        if paper_result.status in {
            "submitted",
            "partially_filled",
            "reconciling",
        }:
            await _enqueue_next_paper_execution(
                context,
                payload=payload,
                order_id=target_id,
            )
        return JobHandlerResult(
            outcome="succeeded",
            result_code=f"PAPER_{paper_result.action.upper()}",
        )
    if job_type is JobType.SHADOW_ADVANCE_DUE:
        shadow_result = await advance_shadow_session(
            context.session,
            group_id=target_id,
            session_date=date.fromisoformat(payload.session),
            observed_at=context.finished_at(),
        )
        skipped = shadow_result.result in {
            "data_partial",
            "data_stale",
            "source_conflict",
            "session_gap",
        }
        return JobHandlerResult(
            outcome="skipped" if skipped else "succeeded",
            result_code=f"SHADOW_{shadow_result.result.upper()}",
        )
    if job_type is JobType.PAPER_PERFORMANCE:
        performance_result = await publish_paper_performance_snapshot(
            context.session,
            account_id=target_id,
            as_of=parse_utc(payload.market_close_at),
        )
        return JobHandlerResult(
            outcome="succeeded",
            result_code=(
                "PERFORMANCE_REPLAYED"
                if performance_result.replayed
                else "PERFORMANCE_PUBLISHED"
            ),
        )
    raise ValueError(f"local daily stage is not enabled: {job_type.value}")


async def _enqueue_next_paper_execution(
    context: JobHandlerContext,
    *,
    payload: DailyStagePayload,
    order_id: UUID,
) -> Job | None:
    order = await context.session.get(PaperOrderRecord, order_id)
    if order is None:
        raise ValueError("paper execution target disappeared")
    decision = await context.session.get(
        StrategyDecisionRecord,
        order.strategy_decision_id,
    )
    if decision is None:
        raise ValueError("paper execution decision disappeared")
    calendar = await context.session.get(
        ExchangeCalendarSessionRecord,
        (decision.calendar_authority_id, decision.execution_session),
    )
    if calendar is None:
        raise ValueError("paper execution calendar disappeared")
    next_evaluation = parse_utc(payload.scheduled_for) + timedelta(minutes=5)
    if next_evaluation > calendar.market_close_at:
        return None
    next_payload = payload.model_copy(
        update={"scheduled_for": format_utc(next_evaluation)}
    ).model_dump(mode="json")
    dependencies = (JobDependencyInput(context.claim.job_id),)
    authority_hash = job_input_hash(
        {
            "payload": next_payload,
            "dependencies": [(str(context.claim.job_id), "succeeded")],
        }
    )
    return await enqueue_job(
        context.session,
        key=JobKeyV1(
            job_type=JobType.PAPER_EXECUTE_DUE,
            scope_kind="portfolio",
            scope_id=str(order_id),
            effective_at=next_evaluation,
            input_hash=authority_hash,
        ),
        schema_version=STAGE_SCHEMA,
        payload=next_payload,
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        available_at=next_evaluation,
        dependencies=dependencies,
    )
