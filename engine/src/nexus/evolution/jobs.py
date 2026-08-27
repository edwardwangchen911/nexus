"""Bounded, content-addressed Jobs for the isolated strategy-evolution queue."""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.audit.service import append_audit_event
from nexus.evolution.repository import experiment_projection_is_valid
from nexus.jobs.models import (
    JOB_PRIORITIES,
    Hash64,
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    job_input_hash,
)
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import JobDependencyInput, enqueue_job
from nexus.outbox.models import OutboxMessage, PublisherSpec
from nexus.outbox.registry import PublisherRegistry
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.evolution import (
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
    StrategyShadowOrderRecord,
    StrategyShadowSessionAuthorityRecord,
)
from nexus.persistence.models.runtime import Job, OutboxEvent

STRATEGY_RND_JOB_SCHEMA = "strategy_rnd_job_v1"
StrategyRNDOperation = Literal[
    "generate_family",
    "evaluate_candidate",
    "advance_shadow",
    "finalize_promotion",
]
StrategyRNDOperationExecutor = Callable[
    [JobHandlerContext, "StrategyRNDJobPayloadV1"],
    Awaitable[JobHandlerResult],
]
StrategyGenerationReason = Literal[
    "GENERATION_ELIGIBLE",
    "STRATEGY_REGISTRY_INCOMPLETE",
    "EVOLUTION_PAUSED",
    "REGISTRY_TRANSITION_PENDING",
    "LINEAGE_AUTHORITY_INCOMPLETE",
    "ACTIVE_FAMILY_EXISTS",
    "EVIDENCE_INCOMPLETE",
    "EXPERIMENT_COOLDOWN_ACTIVE",
    "EXPERIMENT_ALREADY_QUEUED",
]
_NEW_YORK = ZoneInfo("America/New_York")
_TERMINAL_FAMILY_STATES = (
    "REJECTED",
    "BACKTEST_FAILED",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
)
_CANDIDATE_METRIC_STATES = frozenset(
    {
        "PROPOSED",
        "REJECTED",
        "ADMITTED",
        "BACKTEST_RUNNING",
        "BACKTEST_FAILED",
        "BACKTEST_PASSED",
        "SHADOW_RUNNING",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTION_ELIGIBLE",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    }
)
_GATE_METRIC_STATES = frozenset({"PASS", "FAIL", "UNKNOWN"})


class StrategyRNDJobPayloadV1(BaseModel):
    """One non-executable operation over immutable database identities."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["strategy_rnd_job_v1"] = "strategy_rnd_job_v1"
    operation: StrategyRNDOperation
    family_id: UUID | None
    experiment_id: UUID | None
    expected_champion_generation: int = Field(ge=1)
    input_hash: Hash64

    @field_validator("family_id", "experiment_id", mode="before")
    @classmethod
    def _parse_canonical_json_uuid(cls, value: object) -> object:
        """Strict workers receive JSON strings; admit only canonical UUID text."""
        if isinstance(value, str):
            parsed = UUID(value)
            if str(parsed) != value:
                raise ValueError("strategy R&D identifiers must be canonical UUIDs")
            return parsed
        return value

    @model_validator(mode="after")
    def _exact_identity_and_hash(self) -> StrategyRNDJobPayloadV1:
        identifiers_absent = self.family_id is None and self.experiment_id is None
        identifiers_present = self.family_id is not None and self.experiment_id is not None
        if self.operation == "generate_family" and not identifiers_absent:
            raise ValueError("generate_family must not accept family or experiment IDs")
        if self.operation != "generate_family" and not identifiers_present:
            raise ValueError("candidate operations require family and experiment IDs")
        expected = strategy_rnd_payload_hash(
            operation=self.operation,
            family_id=self.family_id,
            experiment_id=self.experiment_id,
            expected_champion_generation=self.expected_champion_generation,
        )
        if self.input_hash != expected:
            raise ValueError("strategy R&D input_hash does not match its authority")
        return self


class StrategyGenerationEligibilityV1(BaseModel):
    """A safe reasoned preflight shared by the scheduler and command API."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    eligible: bool
    reason_code: StrategyGenerationReason
    champion_generation: int | None = Field(default=None, ge=1)
    registry_version: int | None = Field(default=None, ge=1)
    cooldown_until: datetime | None = None


def strategy_rnd_payload_hash(
    *,
    operation: StrategyRNDOperation,
    family_id: UUID | None,
    experiment_id: UUID | None,
    expected_champion_generation: int,
) -> str:
    """Hash the complete business input while excluding only the hash itself."""
    return job_input_hash(
        {
            "schema_version": STRATEGY_RND_JOB_SCHEMA,
            "operation": operation,
            "family_id": None if family_id is None else str(family_id),
            "experiment_id": None if experiment_id is None else str(experiment_id),
            "expected_champion_generation": expected_champion_generation,
        }
    )


def build_strategy_rnd_job_payload(
    *,
    operation: str,
    family_id: UUID | None,
    experiment_id: UUID | None,
    expected_champion_generation: int,
) -> StrategyRNDJobPayloadV1:
    """Build the only payload shape accepted by the Strategy R&D worker."""
    typed_operation = cast(StrategyRNDOperation, operation)
    return StrategyRNDJobPayloadV1(
        operation=typed_operation,
        family_id=family_id,
        experiment_id=experiment_id,
        expected_champion_generation=expected_champion_generation,
        input_hash=strategy_rnd_payload_hash(
            operation=typed_operation,
            family_id=family_id,
            experiment_id=experiment_id,
            expected_champion_generation=expected_champion_generation,
        ),
    )


async def enqueue_strategy_rnd_job(
    session: AsyncSession,
    *,
    payload: StrategyRNDJobPayloadV1,
    effective_at: datetime,
    dependencies: Sequence[JobDependencyInput] = (),
) -> Job:
    """Enqueue one low-priority intent or replay its exact durable identity."""
    effective_at = _utc(effective_at)
    document = payload.model_dump(mode="json")
    scope_id = (
        f"generation.{payload.expected_champion_generation}"
        if payload.experiment_id is None
        else str(payload.experiment_id)
    )
    return await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.STRATEGY_RND,
            scope_kind="global",
            scope_id=scope_id,
            effective_at=effective_at,
            input_hash=payload.input_hash,
        ),
        schema_version=STRATEGY_RND_JOB_SCHEMA,
        payload=document,
        queue=QueueName.STRATEGY_RND,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=3,
        available_at=effective_at,
        dependencies=dependencies,
    )


async def resolve_due_strategy_rnd_jobs(
    session: AsyncSession,
    *,
    session_date: date,
    now: datetime,
) -> tuple[StrategyRNDJobPayloadV1, ...]:
    """Freeze due operations from database facts; handlers revalidate before work."""
    now = _utc(now)
    head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
    if head is None:
        return ()
    targets: list[StrategyRNDJobPayloadV1] = []
    targets.extend(await _due_candidate_jobs(session, generation=head.active_generation))
    targets.extend(
        await _due_shadow_jobs(
            session,
            session_date=session_date,
            generation=head.active_generation,
        )
    )
    if await _generation_is_due(session, head=head, now=now):
        targets.append(
            build_strategy_rnd_job_payload(
                operation="generate_family",
                family_id=None,
                experiment_id=None,
                expected_champion_generation=head.active_generation,
            )
        )
    return tuple(
        sorted(
            targets,
            key=lambda item: (
                item.operation,
                "" if item.family_id is None else str(item.family_id),
                "" if item.experiment_id is None else str(item.experiment_id),
            ),
        )
    )


async def materialize_strategy_rnd_outbox_jobs(
    session: AsyncSession,
    *,
    message: OutboxMessage,
) -> tuple[Job, ...]:
    """Translate a registered evolution outbox fact into exact idempotent Jobs."""
    source = await session.get(OutboxEvent, message.event_id)
    if (
        source is None
        or source.event_type != message.event_type
        or source.aggregate_id != message.aggregate_id
        or source.schema_version != message.schema_version
        or source.payload != dict(message.payload)
    ):
        raise ValueError("strategy evolution outbox authority is inconsistent")
    if message.event_type == "strategy.experiment.admitted":
        family = await session.get(StrategyExperimentFamilyRecord, message.aggregate_id)
        if family is None or family.admission_status != "ADMITTED":
            raise ValueError("admitted family authority is unavailable")
        experiments = tuple(
            await session.scalars(
                select(StrategyExperimentRecord)
                .where(StrategyExperimentRecord.family_id == family.id)
                .order_by(StrategyExperimentRecord.candidate_ordinal)
            )
        )
        if len(experiments) != family.candidate_count:
            raise ValueError("admitted candidate authority is incomplete")
        jobs: list[Job] = []
        for candidate in experiments:
            payload = build_strategy_rnd_job_payload(
                operation="evaluate_candidate",
                family_id=family.id,
                experiment_id=candidate.id,
                expected_champion_generation=family.champion_generation,
            )
            existing = await _existing_exact_operation_job(session, payload=payload)
            jobs.append(
                existing
                if existing is not None
                else await enqueue_strategy_rnd_job(
                    session,
                    payload=payload,
                    effective_at=source.created_at,
                )
            )
        return tuple(jobs)
    if message.event_type == "strategy.shadow.finalized":
        group = await session.get(StrategyShadowGroupRecord, message.aggregate_id)
        shadow_experiment = (
            None
            if group is None
            else await session.get(StrategyExperimentRecord, group.experiment_id)
        )
        family = (
            None
            if shadow_experiment is None
            else await session.get(
                StrategyExperimentFamilyRecord,
                shadow_experiment.family_id,
            )
        )
        if group is None or shadow_experiment is None or family is None:
            raise ValueError("finalized shadow authority is unavailable")
        payload = build_strategy_rnd_job_payload(
            operation="finalize_promotion",
            family_id=family.id,
            experiment_id=shadow_experiment.id,
            expected_champion_generation=group.champion_generation,
        )
        existing = await _existing_exact_operation_job(session, payload=payload)
        return (
            existing
            if existing is not None
            else await enqueue_strategy_rnd_job(
                session,
                payload=payload,
                effective_at=source.created_at,
            ),
        )
    raise ValueError("unregistered strategy evolution outbox event")


async def _existing_exact_operation_job(
    session: AsyncSession,
    *,
    payload: StrategyRNDJobPayloadV1,
) -> Job | None:
    existing = await session.scalar(
        select(Job)
        .where(
            Job.job_type == JobType.STRATEGY_RND.value,
            Job.input_hash == payload.input_hash,
        )
        .order_by(Job.created_at, Job.id)
        .limit(1)
    )
    if existing is None:
        return None
    if (
        existing.schema_version != STRATEGY_RND_JOB_SCHEMA
        or existing.payload != payload.model_dump(mode="json")
        or existing.queue != QueueName.STRATEGY_RND.value
        or existing.priority != JOB_PRIORITIES[JobType.STRATEGY_RND]
    ):
        raise ValueError("existing strategy evolution Job authority conflicts")
    return existing


def build_strategy_rnd_outbox_registry(
    *,
    session_factory: async_sessionmaker[AsyncSession],
) -> PublisherRegistry:
    """Compose exact at-least-once publishers without any external side effect."""

    async def publish(message: OutboxMessage) -> None:
        factory_session = session_factory()
        async with factory_session as session, session.begin():
            await materialize_strategy_rnd_outbox_jobs(session, message=message)

    return PublisherRegistry(
        (
            PublisherSpec(
                event_type="strategy.experiment.admitted",
                schema_version="strategy_evolution_event_v1",
                consumer_idempotent=True,
                publisher=publish,
            ),
            PublisherSpec(
                event_type="strategy.shadow.finalized",
                schema_version="1",
                consumer_idempotent=True,
                publisher=publish,
            ),
        )
    )


async def _generation_is_due(
    session: AsyncSession,
    *,
    head: StrategyRegistryHeadRecord,
    now: datetime,
    check_existing_job: bool = True,
) -> bool:
    eligibility = await strategy_generation_eligibility(
        session,
        now=now,
        check_existing_job=check_existing_job,
        known_head=head,
    )
    return eligibility.eligible


async def strategy_generation_eligibility(
    session: AsyncSession,
    *,
    now: datetime,
    check_existing_job: bool = True,
    known_head: StrategyRegistryHeadRecord | None = None,
) -> StrategyGenerationEligibilityV1:
    """Explain why one server-chosen family generation can or cannot be queued."""
    now = _utc(now)
    head = known_head or await session.get(StrategyRegistryHeadRecord, "internal_paper")
    if head is None:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="STRATEGY_REGISTRY_INCOMPLETE",
        )
    if head.auto_promotion_paused:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="EVOLUTION_PAUSED",
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    if head.pending_event_id is not None:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="REGISTRY_TRANSITION_PENDING",
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    active_lineages = tuple(
        await session.scalars(
            select(StrategyResearchLineageRecord)
            .where(StrategyResearchLineageRecord.status == "active")
            .order_by(StrategyResearchLineageRecord.id)
        )
    )
    if len(active_lineages) != 1:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="LINEAGE_AUTHORITY_INCOMPLETE",
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    lineage = active_lineages[0]
    active_family_count = int(
        await session.scalar(
            select(func.count())
            .select_from(StrategyExperimentStateProjectionRecord)
            .where(
                StrategyExperimentStateProjectionRecord.research_lineage_id == lineage.id,
                StrategyExperimentStateProjectionRecord.current_state.not_in(
                    _TERMINAL_FAMILY_STATES
                ),
            )
        )
        or 0
    )
    if active_family_count:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="ACTIVE_FAMILY_EXISTS",
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    latest_quality = await session.scalar(
        select(QualityReport)
        .join(DataSnapshot, DataSnapshot.id == QualityReport.data_snapshot_id)
        .where(
            DataSnapshot.available_at <= now,
            DataSnapshot.received_at <= now,
            QualityReport.created_at <= now,
        )
        .order_by(
            DataSnapshot.available_at.desc(),
            QualityReport.created_at.desc(),
            QualityReport.id.desc(),
        )
        .limit(1)
    )
    if latest_quality is None or latest_quality.status != "PASS":
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="EVIDENCE_INCOMPLETE",
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    latest_family_at = await session.scalar(
        select(func.max(StrategyExperimentFamilyRecord.created_at)).where(
            StrategyExperimentFamilyRecord.research_lineage_id == lineage.id
        )
    )
    cooldown_until = None if latest_family_at is None else _calendar_quarter_after(latest_family_at)
    if cooldown_until is not None and now < cooldown_until:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="EXPERIMENT_COOLDOWN_ACTIVE",
            cooldown_until=cooldown_until,
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    if not check_existing_job:
        return StrategyGenerationEligibilityV1(
            eligible=True,
            reason_code="GENERATION_ELIGIBLE",
            cooldown_until=cooldown_until,
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    quarter_start = datetime(
        now.year,
        ((now.month - 1) // 3) * 3 + 1,
        1,
        tzinfo=UTC,
    )
    already_scheduled = await session.scalar(
        select(Job.id)
        .where(
            Job.job_type == JobType.STRATEGY_RND.value,
            Job.payload["operation"].as_string() == "generate_family",
            Job.payload["expected_champion_generation"].as_integer() == head.active_generation,
            Job.effective_at >= quarter_start,
        )
        .limit(1)
    )
    if already_scheduled is not None:
        return StrategyGenerationEligibilityV1(
            eligible=False,
            reason_code="EXPERIMENT_ALREADY_QUEUED",
            cooldown_until=cooldown_until,
            champion_generation=head.active_generation,
            registry_version=head.version,
        )
    return StrategyGenerationEligibilityV1(
        eligible=True,
        reason_code="GENERATION_ELIGIBLE",
        cooldown_until=cooldown_until,
        champion_generation=head.active_generation,
        registry_version=head.version,
    )


async def _due_candidate_jobs(
    session: AsyncSession,
    *,
    generation: int,
) -> list[StrategyRNDJobPayloadV1]:
    rows = tuple(
        (
            await session.execute(
                select(StrategyExperimentRecord, StrategyExperimentFamilyRecord)
                .join(
                    StrategyExperimentFamilyRecord,
                    StrategyExperimentFamilyRecord.id == StrategyExperimentRecord.family_id,
                )
                .join(
                    StrategyExperimentStateProjectionRecord,
                    StrategyExperimentStateProjectionRecord.family_id
                    == StrategyExperimentFamilyRecord.id,
                )
                .where(
                    StrategyExperimentFamilyRecord.admission_status == "ADMITTED",
                    StrategyExperimentFamilyRecord.champion_generation == generation,
                    StrategyExperimentRecord.status == "ADMITTED",
                    StrategyExperimentStateProjectionRecord.current_state.in_(
                        ("ADMITTED", "BACKTEST_RUNNING")
                    ),
                )
                .order_by(
                    StrategyExperimentFamilyRecord.id,
                    StrategyExperimentRecord.candidate_ordinal,
                )
            )
        ).tuples()
    )
    targets: list[StrategyRNDJobPayloadV1] = []
    for experiment, family in rows:
        existing = await session.scalar(
            select(Job.id)
            .where(
                Job.job_type == JobType.STRATEGY_RND.value,
                Job.payload["operation"].as_string() == "evaluate_candidate",
                Job.payload["experiment_id"].as_string() == str(experiment.id),
            )
            .limit(1)
        )
        if existing is None:
            targets.append(
                build_strategy_rnd_job_payload(
                    operation="evaluate_candidate",
                    family_id=family.id,
                    experiment_id=experiment.id,
                    expected_champion_generation=generation,
                )
            )
    return targets


async def _due_shadow_jobs(
    session: AsyncSession,
    *,
    session_date: date,
    generation: int,
) -> list[StrategyRNDJobPayloadV1]:
    rows = tuple(
        (
            await session.execute(
                select(
                    StrategyShadowGroupRecord,
                    StrategyExperimentRecord,
                    StrategyExperimentFamilyRecord,
                )
                .join(
                    StrategyExperimentRecord,
                    StrategyExperimentRecord.id == StrategyShadowGroupRecord.experiment_id,
                )
                .join(
                    StrategyExperimentFamilyRecord,
                    StrategyExperimentFamilyRecord.id == StrategyExperimentRecord.family_id,
                )
                .where(
                    StrategyShadowGroupRecord.status == "running",
                    StrategyShadowGroupRecord.champion_generation == generation,
                )
                .order_by(StrategyShadowGroupRecord.id)
            )
        ).tuples()
    )
    targets: list[StrategyRNDJobPayloadV1] = []
    for group, experiment, family in rows:
        finalized = await session.scalar(
            select(StrategyShadowFinalizationRecord.id).where(
                StrategyShadowFinalizationRecord.group_id == group.id
            )
        )
        if finalized is not None:
            continue
        completed_sessions = int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyShadowSessionAuthorityRecord)
                .where(StrategyShadowSessionAuthorityRecord.group_id == group.id)
            )
            or 0
        )
        if completed_sessions >= group.evaluation_sessions:
            operation = "finalize_promotion"
        else:
            expected_session = await session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == group.calendar_authority_id,
                    ExchangeCalendarSessionRecord.session
                    > group.created_at.astimezone(_NEW_YORK).date(),
                )
                .order_by(ExchangeCalendarSessionRecord.session)
                .offset(completed_sessions)
                .limit(1)
            )
            if expected_session is None or expected_session > session_date:
                continue
            operation = "advance_shadow"
        existing = await session.scalar(
            select(Job.id)
            .where(
                Job.job_type == JobType.STRATEGY_RND.value,
                Job.payload["operation"].as_string() == operation,
                Job.payload["experiment_id"].as_string() == str(experiment.id),
                Job.status.in_(("queued", "running")),
            )
            .limit(1)
        )
        if existing is None:
            targets.append(
                build_strategy_rnd_job_payload(
                    operation=operation,
                    family_id=family.id,
                    experiment_id=experiment.id,
                    expected_champion_generation=generation,
                )
            )
    return targets


def _calendar_quarter_after(value: datetime) -> datetime:
    month_index = value.month - 1 + 3
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


async def strategy_rnd_handler(
    context: JobHandlerContext,
    *,
    executor: StrategyRNDOperationExecutor,
) -> JobHandlerResult:
    """Revalidate job identity/generation before invoking a bounded domain adapter."""
    payload = cast(StrategyRNDJobPayloadV1, context.payload)
    job = await context.session.get(Job, context.claim.job_id)
    if job is None or job.input_hash != payload.input_hash:
        raise ValueError("strategy R&D claim authority is inconsistent")
    terminal = await _preflight_strategy_rnd_job(
        context.session,
        payload=payload,
        now=context.finished_at(),
    )
    if terminal is not None:
        if payload.operation in {"generate_family", "evaluate_candidate"}:
            await _append_strategy_operation_audit(
                context,
                payload=payload,
                result=terminal,
            )
        return terminal
    result = await executor(context, payload)
    if payload.operation in {"generate_family", "evaluate_candidate"}:
        await _append_strategy_operation_audit(context, payload=payload, result=result)
    return result


async def _append_strategy_operation_audit(
    context: JobHandlerContext,
    *,
    payload: StrategyRNDJobPayloadV1,
    result: JobHandlerResult,
) -> None:
    completed = result.outcome == "succeeded"
    await append_audit_event(
        context.session,
        actor_id="service:strategy_rnd",
        action=(
            "strategy.proposal.created"
            if payload.operation == "generate_family" and completed
            else "strategy.proposal.rejected"
            if payload.operation == "generate_family"
            else "strategy.backtest.completed"
            if completed
            else "strategy.backtest.failed"
        ),
        schema_version="strategy_evolution_event_v1",
        payload={
            "schema_version": "strategy_evolution_event_v1",
            "event_type": payload.operation.upper(),
            "status": result.outcome.upper(),
            "reason_code": result.error_code or result.result_code or "NO_RESULT_CODE",
        },
        occurred_at=context.finished_at(),
    )


def build_strategy_rnd_registry(*, executor: StrategyRNDOperationExecutor) -> JobRegistry:
    """Compose only the isolated strategy queue; no broker capability is imported."""

    async def handle(context: JobHandlerContext) -> JobHandlerResult:
        return await strategy_rnd_handler(context, executor=executor)

    return JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.STRATEGY_RND,
                schema_version=STRATEGY_RND_JOB_SCHEMA,
                payload_model=StrategyRNDJobPayloadV1,
                queue=QueueName.STRATEGY_RND,
                timeout_seconds=45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="strategy.rnd",
                handler=handle,
            ),
        )
    )


async def _preflight_strategy_rnd_job(
    session: AsyncSession,
    *,
    payload: StrategyRNDJobPayloadV1,
    now: datetime,
) -> JobHandlerResult | None:
    head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
    if head is None:
        raise ValueError("strategy registry authority is unavailable")
    if head.active_generation != payload.expected_champion_generation:
        return JobHandlerResult(outcome="skipped", result_code="STALE_CHAMPION_GENERATION")
    if payload.operation == "generate_family":
        if not await _generation_is_due(
            session,
            head=head,
            now=_utc(now),
            check_existing_job=False,
        ):
            return JobHandlerResult(outcome="skipped", result_code="GENERATION_NOT_DUE")
        return None

    family = await session.get(StrategyExperimentFamilyRecord, payload.family_id)
    experiment = await session.get(StrategyExperimentRecord, payload.experiment_id)
    projection = (
        None
        if family is None
        else await session.get(StrategyExperimentStateProjectionRecord, family.id)
    )
    if (
        family is None
        or experiment is None
        or projection is None
        or experiment.family_id != family.id
        or family.champion_generation != payload.expected_champion_generation
    ):
        raise ValueError("strategy experiment authority is inconsistent")
    if not await experiment_projection_is_valid(session, projection):
        raise ValueError("strategy experiment authority is inconsistent")
    if projection.current_state in _TERMINAL_FAMILY_STATES:
        return JobHandlerResult(outcome="skipped", result_code="FAMILY_TERMINAL")
    if payload.operation == "evaluate_candidate" and experiment.status in {
        "BACKTEST_FAILED",
        "BACKTEST_PASSED",
        "SHADOW_RUNNING",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTION_ELIGIBLE",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    }:
        return JobHandlerResult(outcome="skipped", result_code="CANDIDATE_ALREADY_EVALUATED")
    if payload.operation == "evaluate_candidate" and projection.current_state not in {
        "ADMITTED",
        "BACKTEST_RUNNING",
    }:
        return JobHandlerResult(outcome="skipped", result_code="CANDIDATE_ALREADY_EVALUATED")
    if payload.operation in {"advance_shadow", "finalize_promotion"}:
        group = await session.scalar(
            select(StrategyShadowGroupRecord).where(
                StrategyShadowGroupRecord.experiment_id == experiment.id
            )
        )
        if group is None:
            raise ValueError("strategy shadow authority is unavailable")
        finalization = await session.scalar(
            select(StrategyShadowFinalizationRecord).where(
                StrategyShadowFinalizationRecord.group_id == group.id
            )
        )
        if payload.operation == "advance_shadow" and finalization is not None:
            return JobHandlerResult(outcome="skipped", result_code="SHADOW_ALREADY_FINALIZED")
        if (
            payload.operation == "finalize_promotion"
            and finalization is not None
            and finalization.status != "PASS"
        ):
            return JobHandlerResult(outcome="skipped", result_code="PROMOTION_GATE_NOT_PASSED")
        if group.status != "running":
            return JobHandlerResult(outcome="skipped", result_code="SHADOW_NOT_RUNNING")
    return None


class StrategyEvolutionMetricsV1(BaseModel):
    """Non-identifying aggregate process metrics for evolution observability."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    active_family_count: int = Field(ge=0)
    candidate_status_counts: dict[str, int]
    strategy_queue_oldest_age_seconds: int = Field(ge=0)
    shadow_running_count: int = Field(ge=0)
    shadow_session_count: int = Field(ge=0)
    shadow_order_count: int = Field(ge=0)
    promotion_gate_counts: dict[str, int]
    registry_generation: int | None = Field(default=None, ge=1)
    registry_pending_transition: bool

    @field_validator("candidate_status_counts")
    @classmethod
    def _candidate_labels_are_closed(cls, value: dict[str, int]) -> dict[str, int]:
        if not set(value) <= _CANDIDATE_METRIC_STATES or any(count < 0 for count in value.values()):
            raise ValueError("candidate metric labels or counts are invalid")
        return value

    @field_validator("promotion_gate_counts")
    @classmethod
    def _gate_labels_are_closed(cls, value: dict[str, int]) -> dict[str, int]:
        if not set(value) <= _GATE_METRIC_STATES or any(count < 0 for count in value.values()):
            raise ValueError("promotion gate metric labels or counts are invalid")
        return value


async def load_strategy_evolution_metrics(
    session: AsyncSession,
    *,
    now: datetime,
) -> StrategyEvolutionMetricsV1:
    """Load aggregate-only metrics with a fixed, non-investment label space."""
    now = _utc(now)
    terminal_states = (
        "REJECTED",
        "BACKTEST_FAILED",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    )
    active_family_count = int(
        await session.scalar(
            select(func.count())
            .select_from(StrategyExperimentStateProjectionRecord)
            .where(StrategyExperimentStateProjectionRecord.current_state.not_in(terminal_states))
        )
        or 0
    )
    candidate_rows = tuple(
        (
            await session.execute(
                select(
                    StrategyExperimentStateProjectionRecord.current_state,
                    func.sum(StrategyExperimentFamilyRecord.candidate_count),
                )
                .join(
                    StrategyExperimentFamilyRecord,
                    StrategyExperimentFamilyRecord.id
                    == StrategyExperimentStateProjectionRecord.family_id,
                )
                .group_by(StrategyExperimentStateProjectionRecord.current_state)
                .order_by(StrategyExperimentStateProjectionRecord.current_state)
            )
        ).tuples()
    )
    gate_rows = tuple(
        (
            await session.execute(
                select(StrategyPromotionDecisionRecord.status, func.count())
                .group_by(StrategyPromotionDecisionRecord.status)
                .order_by(StrategyPromotionDecisionRecord.status)
            )
        ).tuples()
    )
    oldest = await session.scalar(
        select(func.min(Job.created_at)).where(
            Job.queue == QueueName.STRATEGY_RND.value,
            Job.status == "queued",
        )
    )
    head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
    return StrategyEvolutionMetricsV1(
        active_family_count=active_family_count,
        candidate_status_counts={str(status): int(count) for status, count in candidate_rows},
        strategy_queue_oldest_age_seconds=(
            0 if oldest is None else max(0, int((now - oldest).total_seconds()))
        ),
        shadow_running_count=int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyShadowGroupRecord)
                .where(StrategyShadowGroupRecord.status == "running")
            )
            or 0
        ),
        shadow_session_count=int(
            await session.scalar(
                select(func.count()).select_from(StrategyShadowSessionAuthorityRecord)
            )
            or 0
        ),
        shadow_order_count=int(
            await session.scalar(select(func.count()).select_from(StrategyShadowOrderRecord)) or 0
        ),
        promotion_gate_counts={str(status): int(count) for status, count in gate_rows},
        registry_generation=None if head is None else head.active_generation,
        registry_pending_transition=head is not None and head.pending_event_id is not None,
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("strategy R&D time must use UTC")
    return value.astimezone(UTC)
