"""Deterministic XNYS daily-pipeline scheduling and DAG materialization."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.evolution.jobs import StrategyRNDJobPayloadV1, enqueue_strategy_rnd_job
from nexus.jobs.daily_payloads import (
    PROFILE_PATTERN,
    ROOT_SCHEMA,
    STAGE_SCHEMA,
    DailyPipelinePayload,
    DailyStagePayload,
    format_utc,
    parse_utc,
)
from nexus.jobs.daily_schedule import build_daily_session_schedule
from nexus.jobs.models import (
    BROKER_JOB_TYPES,
    JOB_PRIORITIES,
    PROVIDER_JOB_TYPES,
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    canonical_job_key,
    job_input_hash,
)
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.jobs.repository import JobDependencyInput, enqueue_job
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import UniverseSnapshot
from nexus.persistence.models.runtime import Job, JobDependency

DailyStageExecutor = Callable[
    [JobType, JobHandlerContext, DailyStagePayload],
    Awaitable[JobHandlerResult],
]


@dataclass(frozen=True, slots=True)
class DailyTargetSet:
    """Frozen local targets selected before child Job materialization."""

    paper_execution: tuple[UUID | None, ...]
    shadow: tuple[UUID | None, ...]
    performance: tuple[UUID | None, ...]
    strategy_rnd: tuple[StrategyRNDJobPayloadV1, ...] = ()


DailyTargetResolver = Callable[
    [AsyncSession, DailyPipelinePayload],
    Awaitable[DailyTargetSet],
]


async def schedule_due_daily_pipeline(
    session: AsyncSession,
    *,
    now: datetime,
    profile_id: str,
) -> Job | None:
    """Idempotently enqueue the latest due XNYS session from frozen authority."""
    now = parse_utc(format_utc(now))
    if PROFILE_PATTERN.fullmatch(profile_id) is None:
        raise ValueError("daily pipeline profile_id is invalid")
    calendar = await session.scalar(
        select(ExchangeCalendarSessionRecord)
        .join(
            ExchangeCalendarAuthorityRecord,
            ExchangeCalendarAuthorityRecord.id
            == ExchangeCalendarSessionRecord.calendar_authority_id,
        )
        .where(
            ExchangeCalendarAuthorityRecord.exchange == "XNYS",
            ExchangeCalendarAuthorityRecord.completeness_status == "complete",
            ExchangeCalendarAuthorityRecord.published_at <= now,
            ExchangeCalendarSessionRecord.market_close_at
            <= now - timedelta(minutes=30),
        )
        .order_by(
            ExchangeCalendarSessionRecord.market_close_at.desc(),
            ExchangeCalendarAuthorityRecord.published_at.desc(),
            ExchangeCalendarAuthorityRecord.id.desc(),
        )
        .limit(1)
    )
    if calendar is None:
        return None
    next_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id
            == calendar.calendar_authority_id,
            ExchangeCalendarSessionRecord.session > calendar.session,
        )
        .order_by(ExchangeCalendarSessionRecord.session)
        .limit(1)
    )
    if next_session is None:
        return None
    schedule = build_daily_session_schedule(
        session=calendar.session,
        market_close_at=calendar.market_close_at,
        next_session=next_session,
    )
    if schedule is None:
        return None
    universe = await session.scalar(
        select(UniverseSnapshot)
        .where(
            UniverseSnapshot.effective_from <= schedule.session,
            UniverseSnapshot.published_at.is_not(None),
            UniverseSnapshot.published_at <= schedule.planned_at,
        )
        .order_by(
            UniverseSnapshot.published_at.desc(),
            UniverseSnapshot.effective_from.desc(),
            UniverseSnapshot.id.desc(),
        )
        .limit(1)
    )
    payload = DailyPipelinePayload(
        calendar_authority_id=str(calendar.calendar_authority_id),
        session=schedule.session.isoformat(),
        market_close_at=format_utc(calendar.market_close_at),
        planned_at=format_utc(schedule.planned_at),
        quality_cutoff_at=format_utc(schedule.quality_cutoff_at),
        next_session=None if next_session is None else next_session.isoformat(),
        next_paper_execution_at=(
            None
            if schedule.next_paper_execution_at is None
            else format_utc(schedule.next_paper_execution_at)
        ),
        last_session_of_week=schedule.last_session_of_week,
        profile_id=profile_id,
        universe_snapshot_id=None if universe is None else str(universe.id),
        universe_content_hash=None if universe is None else universe.content_hash,
    )
    document = payload.model_dump(mode="json")
    return await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.DAILY_PIPELINE,
            scope_kind="global",
            scope_id=f"XNYS.{schedule.session.isoformat()}",
            effective_at=schedule.planned_at,
            input_hash=job_input_hash(document),
        ),
        schema_version=ROOT_SCHEMA,
        payload=document,
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        available_at=schedule.planned_at,
    )


def build_daily_pipeline_registry(
    *,
    profile_id: str,
    stage_executor: DailyStageExecutor,
    enabled_stage_types: frozenset[JobType] | None = None,
    target_resolver: DailyTargetResolver | None = None,
    stage_specs: Sequence[JobHandlerSpec] = (),
) -> JobRegistry:
    """Build an explicit local registry; constructing it never loads broker code."""
    if PROFILE_PATTERN.fullmatch(profile_id) is None:
        raise ValueError("daily pipeline profile_id is invalid")

    async def materialize(context: JobHandlerContext) -> JobHandlerResult:
        payload = cast(DailyPipelinePayload, context.payload)
        if payload.profile_id != profile_id:
            raise ValueError("daily pipeline capability profile is inconsistent")
        await _materialize_daily_dag(
            context,
            payload,
            enabled_stage_types=(
                _GENERAL_STAGE_TYPES
                if enabled_stage_types is None
                else enabled_stage_types
            ),
            target_resolver=target_resolver,
        )
        return JobHandlerResult(outcome="succeeded", result_code="PIPELINE_MATERIALIZED")

    specs = [
        JobHandlerSpec(
            job_type=JobType.DAILY_PIPELINE,
            schema_version=ROOT_SCHEMA,
            payload_model=DailyPipelinePayload,
            queue=QueueName.GENERAL,
            timeout_seconds=10,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=3,
            idempotency_namespace="daily.pipeline",
            handler=materialize,
        )
    ]
    registered_stages = (
        _GENERAL_STAGE_TYPES if enabled_stage_types is None else enabled_stage_types
    )
    provided_specs = {spec.job_type: spec for spec in stage_specs}
    if len(provided_specs) != len(stage_specs):
        raise ValueError("duplicate daily stage handler")
    if not set(provided_specs).issubset(registered_stages):
        raise ValueError("daily stage handler is not enabled")
    for spec in provided_specs.values():
        if (
            spec.job_type not in _GENERAL_STAGE_TYPES
            or spec.schema_version != STAGE_SCHEMA
            or spec.payload_model is not DailyStagePayload
            or spec.queue
            is not (
                QueueName.PROVIDER
                if spec.job_type in PROVIDER_JOB_TYPES
                else QueueName.GENERAL
            )
        ):
            raise ValueError("daily stage handler authority is invalid")

    for job_type in sorted(registered_stages, key=lambda item: item.value):
        if job_type not in _GENERAL_STAGE_TYPES:
            raise ValueError(f"unsupported daily stage type: {job_type.value}")

        provided = provided_specs.get(job_type)
        if provided is not None:
            specs.append(provided)
            continue
        if job_type in PROVIDER_JOB_TYPES:
            continue

        async def handle(
            context: JobHandlerContext,
            *,
            current_type: JobType = job_type,
        ) -> JobHandlerResult:
            payload = cast(DailyStagePayload, context.payload)
            if payload.profile_id != profile_id:
                raise ValueError("daily stage capability profile is inconsistent")
            return await stage_executor(current_type, context, payload)

        specs.append(
            JobHandlerSpec(
                job_type=job_type,
                schema_version=STAGE_SCHEMA,
                payload_model=DailyStagePayload,
                queue=(
                    QueueName.PROVIDER
                    if job_type in PROVIDER_JOB_TYPES
                    else QueueName.GENERAL
                ),
                timeout_seconds=45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace=job_type.value,
                handler=handle,
            )
        )
    return JobRegistry(specs)


_GENERAL_STAGE_TYPES = frozenset(
    {
        JobType.COLLECT_MARKET,
        JobType.COLLECT_SEC_MACRO,
        JobType.QUALITY_FREEZE,
        JobType.DIAGNOSTICS_RUN,
        JobType.PAPER_EXECUTE_DUE,
        JobType.SHADOW_ADVANCE_DUE,
        JobType.AGENT_RESEARCH,
        JobType.OPTIMIZATION_PROPOSE,
        JobType.PAPER_ADMIT_ORDERS,
        JobType.PAPER_PERFORMANCE,
        JobType.DAILY_BRIEF,
        JobType.AUDIT_MANIFEST,
    }
)

async def _materialize_daily_dag(
    context: JobHandlerContext,
    root: DailyPipelinePayload,
    *,
    enabled_stage_types: frozenset[JobType],
    target_resolver: DailyTargetResolver | None,
) -> None:
    targets = (
        DailyTargetSet(
            paper_execution=(None,),
            shadow=(None,),
            performance=(None,),
            strategy_rnd=(),
        )
        if target_resolver is None
        else await target_resolver(context.session, root)
    )
    root_dependency = (JobDependencyInput(context.claim.job_id),)
    broker = await _record_terminal_skip(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.BROKER_SYNC,
        dependencies=root_dependency,
        finished_at=context.finished_at(),
        result_code="SKIPPED_CAPABILITY",
    )
    market = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.COLLECT_MARKET,
        dependencies=root_dependency,
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    sec_macro = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.COLLECT_SEC_MACRO,
        dependencies=root_dependency,
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    shadow_nodes = await _target_stages(
        context,
        root=root,
        job_type=JobType.SHADOW_ADVANCE_DUE,
        target_ids=targets.shadow,
        dependencies=root_dependency,
        enabled_stage_types=enabled_stage_types,
    )
    if root.next_paper_execution_at is not None:
        await _target_stages(
            context,
            root=root,
            job_type=JobType.PAPER_EXECUTE_DUE,
            target_ids=targets.paper_execution,
            scheduled_for=parse_utc(root.next_paper_execution_at),
            dependencies=root_dependency,
            enabled_stage_types=enabled_stage_types,
        )
    quality = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.QUALITY_FREEZE,
        dependencies=tuple(
            JobDependencyInput(job.id, "succeeded_or_skipped")
            for job in (broker, market, sec_macro, *shadow_nodes)
        ),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    diagnostics = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.DIAGNOSTICS_RUN,
        dependencies=(JobDependencyInput(quality.id),),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    research = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.AGENT_RESEARCH,
        dependencies=(JobDependencyInput(diagnostics.id),),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    optimization = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.OPTIMIZATION_PROPOSE,
        dependencies=(JobDependencyInput(research.id),),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    admission = None
    if root.last_session_of_week:
        admission = await _enqueue_or_skip_stage(
            context.session,
            root=root,
            root_job_id=context.claim.job_id,
            job_type=JobType.PAPER_ADMIT_ORDERS,
            dependencies=(JobDependencyInput(optimization.id),),
            enabled_stage_types=enabled_stage_types,
            finished_at=context.finished_at(),
        )
    performance_nodes = await _target_stages(
        context,
        root=root,
        job_type=JobType.PAPER_PERFORMANCE,
        target_ids=targets.performance,
        dependencies=(
            JobDependencyInput(quality.id, "succeeded_or_skipped"),
            *(
                JobDependencyInput(job.id, "succeeded_or_skipped")
                for job in shadow_nodes
            ),
            JobDependencyInput(
                optimization.id if admission is None else admission.id,
                "succeeded_or_skipped",
            ),
        ),
        enabled_stage_types=enabled_stage_types,
    )
    brief = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.DAILY_BRIEF,
        dependencies=(
            JobDependencyInput(quality.id, "succeeded_or_skipped"),
            JobDependencyInput(research.id, "succeeded_or_skipped"),
            *(
                JobDependencyInput(job.id, "succeeded_or_skipped")
                for job in performance_nodes
            ),
        ),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    audit_parents = [
        broker,
        market,
        sec_macro,
        *shadow_nodes,
        quality,
        diagnostics,
        research,
        optimization,
        *performance_nodes,
        brief,
    ]
    if admission is not None:
        audit_parents.append(admission)
    audit = await _enqueue_or_skip_stage(
        context.session,
        root=root,
        root_job_id=context.claim.job_id,
        job_type=JobType.AUDIT_MANIFEST,
        dependencies=tuple(
            JobDependencyInput(job.id, "succeeded_or_skipped")
            for job in audit_parents
        ),
        enabled_stage_types=enabled_stage_types,
        finished_at=context.finished_at(),
    )
    for payload in targets.strategy_rnd:
        await enqueue_strategy_rnd_job(
            context.session,
            payload=payload,
            effective_at=parse_utc(root.planned_at),
            dependencies=(JobDependencyInput(audit.id, "succeeded_or_skipped"),),
        )


async def _enqueue_or_skip_stage(
    session: AsyncSession,
    *,
    root: DailyPipelinePayload,
    root_job_id: UUID,
    job_type: JobType,
    dependencies: Sequence[JobDependencyInput],
    enabled_stage_types: frozenset[JobType],
    finished_at: datetime,
    scheduled_for: datetime | None = None,
    target_id: UUID | None = None,
) -> Job:
    if (
        job_type in {JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO}
        and root.universe_snapshot_id is None
    ):
        return await _record_terminal_skip(
            session,
            root=root,
            root_job_id=root_job_id,
            job_type=job_type,
            dependencies=dependencies,
            finished_at=finished_at,
            result_code="NO_FROZEN_UNIVERSE",
            scheduled_for=scheduled_for,
            target_id=target_id,
        )
    if job_type in enabled_stage_types:
        return await _enqueue_stage(
            session,
            root=root,
            root_job_id=root_job_id,
            job_type=job_type,
            dependencies=dependencies,
            scheduled_for=scheduled_for,
            target_id=target_id,
        )
    return await _record_terminal_skip(
        session,
        root=root,
        root_job_id=root_job_id,
        job_type=job_type,
        dependencies=dependencies,
        finished_at=finished_at,
        result_code="SKIPPED_CAPABILITY",
        scheduled_for=scheduled_for,
        target_id=target_id,
    )


async def _target_stages(
    context: JobHandlerContext,
    *,
    root: DailyPipelinePayload,
    job_type: JobType,
    target_ids: Sequence[UUID | None],
    dependencies: Sequence[JobDependencyInput],
    enabled_stage_types: frozenset[JobType],
    scheduled_for: datetime | None = None,
) -> tuple[Job, ...]:
    if not target_ids:
        return (
            await _record_terminal_skip(
                context.session,
                root=root,
                root_job_id=context.claim.job_id,
                job_type=job_type,
                dependencies=dependencies,
                finished_at=context.finished_at(),
                result_code="SKIPPED_NO_DUE_TARGETS",
                scheduled_for=scheduled_for,
            ),
        )
    return tuple(
        [
            await _enqueue_or_skip_stage(
                context.session,
                root=root,
                root_job_id=context.claim.job_id,
                job_type=job_type,
                dependencies=dependencies,
                enabled_stage_types=enabled_stage_types,
                finished_at=context.finished_at(),
                scheduled_for=scheduled_for,
                target_id=target_id,
            )
            for target_id in target_ids
        ]
    )


def _stage_payload(
    *,
    root: DailyPipelinePayload,
    root_job_id: UUID,
    scheduled_for: datetime,
    target_id: UUID | None = None,
) -> DailyStagePayload:
    return DailyStagePayload(
        root_job_id=str(root_job_id),
        calendar_authority_id=root.calendar_authority_id,
        session=root.session,
        market_close_at=root.market_close_at,
        scheduled_for=format_utc(scheduled_for),
        quality_cutoff_at=root.quality_cutoff_at,
        profile_id=root.profile_id,
        target_id=None if target_id is None else str(target_id),
        universe_snapshot_id=root.universe_snapshot_id,
        universe_content_hash=root.universe_content_hash,
    )


async def _enqueue_stage(
    session: AsyncSession,
    *,
    root: DailyPipelinePayload,
    root_job_id: UUID,
    job_type: JobType,
    dependencies: Sequence[JobDependencyInput],
    scheduled_for: datetime | None = None,
    target_id: UUID | None = None,
) -> Job:
    effective_at = (
        parse_utc(root.planned_at) if scheduled_for is None else scheduled_for
    )
    payload = _stage_payload(
        root=root,
        root_job_id=root_job_id,
        scheduled_for=effective_at,
        target_id=target_id,
    ).model_dump(mode="json")
    dependency_authority = sorted(
        (str(item.depends_on_job_id), item.required_outcome) for item in dependencies
    )
    authority_hash = job_input_hash(
        {"payload": payload, "dependencies": dependency_authority}
    )
    queue = (
        QueueName.STRATEGY_RND
        if job_type is JobType.STRATEGY_RND
        else QueueName.PROVIDER
        if job_type in PROVIDER_JOB_TYPES
        else QueueName.GENERAL
    )
    return await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=job_type,
            scope_kind="global" if target_id is None else "portfolio",
            scope_id=(
                f"XNYS.{root.session}" if target_id is None else str(target_id)
            ),
            effective_at=effective_at,
            input_hash=authority_hash,
        ),
        schema_version=STAGE_SCHEMA,
        payload=payload,
        queue=queue,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        available_at=effective_at,
        dependencies=dependencies,
    )


async def _record_terminal_skip(
    session: AsyncSession,
    *,
    root: DailyPipelinePayload,
    root_job_id: UUID,
    job_type: JobType,
    dependencies: Sequence[JobDependencyInput],
    finished_at: datetime,
    result_code: str,
    scheduled_for: datetime | None = None,
    target_id: UUID | None = None,
) -> Job:
    """Record a scheduler-known skip without claiming a disabled queue."""
    effective_at = (
        parse_utc(root.planned_at) if scheduled_for is None else scheduled_for
    )
    finished_at = parse_utc(format_utc(finished_at))
    payload = _stage_payload(
        root=root,
        root_job_id=root_job_id,
        scheduled_for=effective_at,
        target_id=target_id,
    ).model_dump(mode="json")
    authority_hash = job_input_hash(
        {
            "payload": payload,
            "dependencies": sorted(
                (str(item.depends_on_job_id), item.required_outcome)
                for item in dependencies
            ),
            "result_code": result_code,
        }
    )
    key = JobKeyV1(
        job_type=job_type,
        scope_kind=(
            "provider"
            if job_type in BROKER_JOB_TYPES
            else "global"
            if target_id is None
            else "portfolio"
        ),
        scope_id=(
            "broker.disabled"
            if job_type in BROKER_JOB_TYPES
            else f"XNYS.{root.session}"
            if target_id is None
            else str(target_id)
        ),
        effective_at=effective_at,
        input_hash=authority_hash,
    )
    job = Job(
        id=new_uuid7(),
        job_key=canonical_job_key(key),
        input_hash=authority_hash,
        status="skipped",
        schema_version=STAGE_SCHEMA,
        payload=payload,
        job_type=job_type.value,
        queue=(
            QueueName.BROKER.value
            if job_type in BROKER_JOB_TYPES
            else QueueName.PROVIDER.value
            if job_type in PROVIDER_JOB_TYPES
            else QueueName.STRATEGY_RND.value
            if job_type is JobType.STRATEGY_RND
            else QueueName.GENERAL.value
        ),
        scope_kind=key.scope_kind,
        scope_id=key.scope_id,
        effective_at=effective_at,
        priority=JOB_PRIORITIES[job_type],
        retry_policy=RetryPolicy.NEVER.value,
        max_attempts=1,
        attempt_count=0,
        manual_retry_count=0,
        available_at=effective_at,
        started_at=finished_at,
        finished_at=finished_at,
        last_error_code=None,
        result_code=result_code,
        version=1,
    )
    session.add(job)
    await session.flush()
    for dependency in dependencies:
        session.add(
            JobDependency(
                id=new_uuid7(),
                job_id=job.id,
                depends_on_job_id=dependency.depends_on_job_id,
                required_outcome=dependency.required_outcome,
            )
        )
    await session.flush()
    return job
