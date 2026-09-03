"""Explicit frozen snapshot -> reconciliation validation -> local diagnosis."""

from datetime import UTC, date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.service import diagnose_and_persist
from nexus.jobs.daily_payloads import (
    STAGE_SCHEMA,
    DailyPipelinePayload,
    DailyStagePayload,
    format_utc,
)
from nexus.jobs.daily_pipeline import (
    _enqueue_stage,
    _stage_payload,
    assert_daily_pipeline_schema_ready,
)
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import JobDependencyInput, enqueue_job
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.models.runtime import Job, JobDependency

MANUAL_ROOT_SCHEMA = "daily_manual_root_v1"
_PROFILE = "local_manual_diagnostic_v1"
_STAGES = (JobType.QUALITY_FREEZE, JobType.DIAGNOSTICS_RUN)


class ManualDailyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    snapshot_id: UUID
    calendar_authority_id: UUID
    session: date
    cutoff_at: datetime

    @field_validator("cutoff_at", "snapshot_as_of", "market_close_at", check_fields=False)
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("MANUAL_DAILY_TIME_INVALID")
        return value


class ManualDailyPayload(ManualDailyRequest):
    portfolio_id: UUID
    snapshot_as_of: datetime
    market_close_at: datetime


async def _freeze(
    session: AsyncSession,
    request: ManualDailyRequest,
    now: datetime,
) -> ManualDailyPayload:
    ManualDailyRequest._utc(now)
    if request.cutoff_at > now:
        raise ValueError("MANUAL_DAILY_TIME_IN_FUTURE")
    snapshot = await session.get(PortfolioSnapshotRecord, request.snapshot_id)
    calendar = await session.get(ExchangeCalendarAuthorityRecord, request.calendar_authority_id)
    day = await session.get(
        ExchangeCalendarSessionRecord,
        (request.calendar_authority_id, request.session),
    )
    if (
        snapshot is None
        or snapshot.as_of is None
        or snapshot.as_of > request.cutoff_at
        or snapshot.created_at > request.cutoff_at
    ):
        raise ValueError("MANUAL_DAILY_SNAPSHOT_UNAVAILABLE")
    if (
        calendar is None
        or day is None
        or calendar.exchange != "XNYS"
        or calendar.completeness_status != "complete"
        or not calendar.range_start <= request.session <= calendar.range_end
        or calendar.published_at > request.cutoff_at
        or calendar.created_at > request.cutoff_at
        or day.created_at > request.cutoff_at
        or day.market_close_at > snapshot.as_of
    ):
        raise ValueError("MANUAL_DAILY_CALENDAR_UNAVAILABLE")
    return ManualDailyPayload(
        snapshot_id=request.snapshot_id,
        calendar_authority_id=request.calendar_authority_id,
        session=request.session,
        cutoff_at=request.cutoff_at,
        portfolio_id=snapshot.portfolio_id,
        snapshot_as_of=snapshot.as_of,
        market_close_at=day.market_close_at,
    )


async def enqueue_manual_daily_root(
    session: AsyncSession,
    *,
    request: ManualDailyRequest,
    now: datetime,
) -> Job:
    """Freeze explicit stored identities, without collecting or running anything."""
    await assert_daily_pipeline_schema_ready(session)
    payload = (await _freeze(session, request, now)).model_dump(mode="json")
    return await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.DAILY_MANUAL_PIPELINE,
            scope_kind="portfolio",
            scope_id=str(request.snapshot_id),
            effective_at=request.cutoff_at,
            input_hash=job_input_hash(payload),
        ),
        schema_version=MANUAL_ROOT_SCHEMA,
        payload=payload,
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
    )


def _daily(payload: ManualDailyPayload) -> DailyPipelinePayload:
    # Only reuse the stage serialization. This does not schedule an old daily
    # pipeline or pretend manual receipt occurred at the old 18:00 cutoff.
    return DailyPipelinePayload(
        calendar_authority_id=str(payload.calendar_authority_id),
        session=payload.session.isoformat(),
        market_close_at=format_utc(payload.market_close_at),
        planned_at=format_utc(payload.cutoff_at),
        quality_cutoff_at=format_utc(payload.cutoff_at),
        last_session_of_week=False,
        profile_id=_PROFILE,
    )


async def _root(session: AsyncSession, root_id: UUID) -> tuple[Job, ManualDailyPayload]:
    root = await session.get(Job, root_id)
    if root is None or root.job_type != JobType.DAILY_MANUAL_PIPELINE.value:
        raise ValueError("MANUAL_DAILY_ROOT_UNAVAILABLE")
    if root.schema_version != MANUAL_ROOT_SCHEMA:
        raise ValueError("MANUAL_DAILY_ROOT_INVALID")
    payload = ManualDailyPayload.model_validate(root.payload)
    if (
        root.input_hash != job_input_hash(payload.model_dump(mode="json"))
        or root.scope_kind != "portfolio"
        or root.scope_id != str(payload.snapshot_id)
        or root.effective_at != payload.cutoff_at
        or root.queue != QueueName.GENERAL.value
    ):
        raise ValueError("MANUAL_DAILY_ROOT_INVALID")
    return root, payload


async def manual_daily_job_ids(session: AsyncSession, *, root_id: UUID) -> tuple[UUID, ...]:
    """Return only the root and its exact two frozen, dependency-bound stages."""
    root, payload = await _root(session, root_id)
    children = tuple(
        await session.scalars(
            select(Job).where(Job.payload["root_job_id"].as_string() == str(root_id)).limit(3)
        )
    )
    by_type = {job.job_type: job for job in children}
    if len(children) != len(by_type) or set(by_type) - {item.value for item in _STAGES}:
        raise ValueError("MANUAL_DAILY_CHILD_INVALID")
    if children and set(by_type) != {item.value for item in _STAGES}:
        raise ValueError("MANUAL_DAILY_CHILD_INVALID")
    if not children:
        if root.status == "succeeded":
            raise ValueError("MANUAL_DAILY_CHILD_MISSING")
        return (root_id,)
    expected = _stage_payload(
        root=_daily(payload),
        root_job_id=root_id,
        scheduled_for=payload.cutoff_at,
        target_id=payload.snapshot_id,
    ).model_dump(mode="json")
    parent = root_id
    ordered = [root_id]
    for job_type in _STAGES:
        job = by_type[job_type.value]
        dependencies = tuple(
            (
                await session.execute(
                    select(JobDependency.depends_on_job_id, JobDependency.required_outcome).where(
                        JobDependency.job_id == job.id
                    )
                )
            ).all()
        )
        if (
            job.schema_version != STAGE_SCHEMA
            or job.payload != expected
            or job.queue != QueueName.GENERAL.value
            or dependencies != ((parent, "succeeded"),)
            or job.input_hash
            != job_input_hash(
                {
                    "payload": expected,
                    "dependencies": [(str(parent), "succeeded")],
                }
            )
        ):
            raise ValueError("MANUAL_DAILY_CHILD_INVALID")
        ordered.append(job.id)
        parent = job.id
    return tuple(ordered)


def build_manual_daily_registry(*, artifact_reader: ArtifactByteReader) -> JobRegistry:
    """No native research, optimizer, provider, paper, or scheduler handlers."""

    async def root_handler(context: JobHandlerContext) -> JobHandlerResult:
        _, payload = await _root(context.session, context.claim.job_id)
        if await _freeze(context.session, payload, context.finished_at()) != payload:
            raise ValueError("MANUAL_DAILY_CONTEXT_CHANGED")
        parent = context.claim.job_id
        for job_type in _STAGES:
            job = await _enqueue_stage(
                context.session,
                root=_daily(payload),
                root_job_id=context.claim.job_id,
                job_type=job_type,
                dependencies=(JobDependencyInput(parent),),
                target_id=payload.snapshot_id,
            )
            parent = job.id
        return JobHandlerResult(outcome="succeeded", result_code="MANUAL_DAILY_MATERIALIZED")

    async def stage_handler(context: JobHandlerContext) -> JobHandlerResult:
        stage = DailyStagePayload.model_validate(context.payload.model_dump())
        root_id = UUID(stage.root_job_id)
        if context.claim.job_id not in await manual_daily_job_ids(context.session, root_id=root_id):
            raise ValueError("MANUAL_DAILY_CHILD_INVALID")
        _, payload = await _root(context.session, root_id)
        if await _freeze(context.session, payload, context.finished_at()) != payload:
            raise ValueError("MANUAL_DAILY_CONTEXT_CHANGED")
        if context.claim.job_type == JobType.QUALITY_FREEZE.value:
            row = (
                await context.session.execute(
                    select(PortfolioSnapshotRecord.status, BrokerSyncRun.cash_flow_quality)
                    .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
                    .where(PortfolioSnapshotRecord.id == payload.snapshot_id)
                )
            ).one()
            if row != ("READY", "PASS"):
                return JobHandlerResult(
                    outcome="skipped", result_code="MANUAL_DAILY_NOT_RECONCILED"
                )
            return JobHandlerResult(outcome="succeeded", result_code="MANUAL_DAILY_SNAPSHOT_VALID")
        report = await diagnose_and_persist(
            context.session,
            portfolio_id=payload.portfolio_id,
            snapshot_id=payload.snapshot_id,
            artifact_reader=artifact_reader,
        )
        return JobHandlerResult(
            outcome="skipped" if report.optimization_need == "BLOCKED" else "succeeded",
            result_code=f"DIAGNOSTIC_{report.optimization_need}",
        )

    return JobRegistry(
        tuple(
            JobHandlerSpec(
                job_type=job_type,
                schema_version=MANUAL_ROOT_SCHEMA
                if job_type == JobType.DAILY_MANUAL_PIPELINE
                else STAGE_SCHEMA,
                payload_model=ManualDailyPayload
                if job_type == JobType.DAILY_MANUAL_PIPELINE
                else DailyStagePayload,
                queue=QueueName.GENERAL,
                timeout_seconds=30,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="manual-daily",
                handler=root_handler
                if job_type == JobType.DAILY_MANUAL_PIPELINE
                else stage_handler,
            )
            for job_type in (JobType.DAILY_MANUAL_PIPELINE, *_STAGES)
        )
    )
