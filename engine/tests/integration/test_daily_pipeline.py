"""PostgreSQL contracts for the deterministic daily automation DAG."""

import gzip
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest
from helpers.paper_execution import FIRST_EVALUATION, seed_paper_execution
from nexus.audit.service import append_audit_event
from nexus.jobs.daily_local import (
    LOCAL_SAFE_DAILY_PROFILE,
    build_local_daily_pipeline_registry,
)
from nexus.jobs.daily_payloads import DailyStagePayload
from nexus.jobs.daily_pipeline import (
    build_daily_pipeline_registry,
    schedule_due_daily_pipeline,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import UniverseSnapshot
from nexus.persistence.models.runtime import Job, JobDependency, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

_CALENDAR_ID = UUID("0198d500-1000-7000-8000-000000002201")
_UNIVERSE_ID = UUID("0198d500-1000-7000-8000-000000002211")
_PROFILE = "fixture_daily_v1"
_THURSDAY = date(2026, 8, 27)
_FRIDAY = date(2026, 8, 28)
_MONDAY = date(2026, 8, 31)
_THURSDAY_CLOSE = datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
_FRIDAY_CLOSE = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)

StageExecutor = Callable[
    [JobType, JobHandlerContext, DailyStagePayload],
    Awaitable[JobHandlerResult],
]


class _AuditStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        self.objects.setdefault(key, content)
        return f"memory://daily-audit/{key}"


async def _seed_calendar(
    factory: async_sessionmaker[AsyncSession], *, include_universe: bool = True
) -> None:
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=_CALENDAR_ID,
                exchange="XNYS",
                version="daily-pipeline-fixture-v1",
                source="fixture",
                range_start=_THURSDAY,
                range_end=_MONDAY,
                query_parameters={},
                completeness_status="complete",
                content_hash="d" * 64,
                published_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        session.add_all(
            (
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=_CALENDAR_ID,
                    session=_THURSDAY,
                    market_open_at=datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
                    market_close_at=_THURSDAY_CLOSE,
                    decision_at=datetime(2026, 8, 27, 21, 0, tzinfo=UTC),
                ),
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=_CALENDAR_ID,
                    session=_FRIDAY,
                    market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                    market_close_at=_FRIDAY_CLOSE,
                    decision_at=datetime(2026, 8, 28, 21, 0, tzinfo=UTC),
                ),
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=_CALENDAR_ID,
                    session=_MONDAY,
                    market_open_at=datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
                    market_close_at=datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
                    decision_at=datetime(2026, 8, 31, 21, 0, tzinfo=UTC),
                ),
            )
        )
        if include_universe:
            session.add(
                UniverseSnapshot(
                    id=_UNIVERSE_ID,
                    key="us_liquid",
                    version="daily-pipeline-fixture-v1",
                    effective_from=_THURSDAY,
                    source="fixture",
                    content_hash="a" * 64,
                    published_at=datetime(2026, 8, 1, tzinfo=UTC),
                )
            )


@pytest.mark.asyncio
async def test_scheduler_fails_closed_without_next_authoritative_session(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    authority_id = UUID("0198d500-1000-7000-8000-000000002202")
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=authority_id,
                exchange="XNYS",
                version="incomplete-future-fixture-v1",
                source="fixture",
                range_start=_FRIDAY,
                range_end=_FRIDAY,
                query_parameters={},
                completeness_status="complete",
                content_hash="e" * 64,
                published_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=authority_id,
                session=_FRIDAY,
                market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                market_close_at=_FRIDAY_CLOSE,
                decision_at=datetime(2026, 8, 28, 21, 0, tzinfo=UTC),
            )
        )
    async with factory() as session, session.begin():
        assert (
            await schedule_due_daily_pipeline(
                session,
                now=_FRIDAY_CLOSE + timedelta(minutes=31),
                profile_id=_PROFILE,
            )
            is None
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


async def _drain_daily_queues(
    factory: async_sessionmaker[AsyncSession],
    *,
    now: datetime,
    executor: StageExecutor,
) -> int:
    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=executor,
        stage_specs=_provider_stage_specs(executor),
    )
    worked = 0
    for _ in range(64):
        tick_worked = False
        for queue in (QueueName.GENERAL, QueueName.PROVIDER):
            queue_worked = await run_job_worker_once(
                factory,
                registry=registry,
                queue=queue,
                worker_id=f"daily-fixture-{queue.value}-worker",
                worker_started_at=now,
                process_version="test-v1",
                migration_head="0027_fosun_market_authority",
                clock=lambda: now,
                claim_registered_only=True,
            )
            tick_worked = tick_worked or queue_worked
            worked += int(queue_worked)
        if not tick_worked:
            return worked
    raise AssertionError("daily fixture did not drain within the bounded tick count")


def _provider_stage_specs(executor: StageExecutor) -> tuple[JobHandlerSpec, ...]:
    specs: list[JobHandlerSpec] = []
    for job_type in (JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO):

        async def handle(
            context: JobHandlerContext,
            *,
            current_type: JobType = job_type,
        ) -> JobHandlerResult:
            return await executor(
                current_type,
                context,
                cast(DailyStagePayload, context.payload),
            )

        specs.append(
            JobHandlerSpec(
                job_type=job_type,
                schema_version="daily_pipeline_stage_v1",
                payload_model=DailyStagePayload,
                queue=QueueName.PROVIDER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace=job_type.value,
                handler=handle,
            )
        )
    return tuple(specs)


def _successful_executor(executed: list[JobType]) -> StageExecutor:
    async def execute(
        job_type: JobType,
        context: JobHandlerContext,
        payload: DailyStagePayload,
    ) -> JobHandlerResult:
        del context, payload
        executed.append(job_type)
        return JobHandlerResult(
            outcome="succeeded",
            result_code=f"{job_type.name}_OK",
        )

    return execute


@pytest.mark.asyncio
async def test_scheduler_is_idempotent_and_materializes_strict_weekly_dag(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        first = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    async with factory() as session, session.begin():
        replay = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    assert first is not None and replay is not None and first.id == replay.id
    assert first.payload["last_session_of_week"] is True
    assert first.payload["universe_snapshot_id"] == str(_UNIVERSE_ID)
    assert first.payload["universe_content_hash"] == "a" * 64

    executed: list[JobType] = []
    assert await _drain_daily_queues(
        factory,
        now=now,
        executor=_successful_executor(executed),
    ) == 12

    parent = aliased(Job)
    child = aliased(Job)
    async with factory() as session:
        jobs = tuple((await session.scalars(select(Job).order_by(Job.created_at, Job.id))).all())
        edge_rows = tuple(
            (
                await session.execute(
                    select(child.job_type, parent.job_type)
                    .select_from(JobDependency)
                    .join(child, child.id == JobDependency.job_id)
                    .join(parent, parent.id == JobDependency.depends_on_job_id)
                )
            ).tuples()
        )
        broker_leases = await session.scalar(
            select(func.count())
            .select_from(JobLease)
            .join(Job, Job.id == JobLease.job_id)
            .where(Job.queue == QueueName.BROKER.value)
        )
    by_type = {job.job_type: job for job in jobs}
    assert len([job for job in jobs if job.job_type == JobType.DAILY_PIPELINE.value]) == 1
    root = next(job for job in jobs if job.job_type == JobType.DAILY_PIPELINE.value)
    assert root.queue == QueueName.GENERAL.value
    assert by_type[JobType.BROKER_SYNC.value].status == "skipped"
    assert by_type[JobType.BROKER_SYNC.value].result_code == "SKIPPED_CAPABILITY"
    assert broker_leases == 0
    assert by_type[JobType.PAPER_EXECUTE_DUE.value].status == "queued"
    assert by_type[JobType.PAPER_EXECUTE_DUE.value].effective_at == datetime(
        2026, 8, 31, 13, 55, tzinfo=UTC
    )
    assert by_type[JobType.PAPER_ADMIT_ORDERS.value].status == "succeeded"
    assert JobType.STRATEGY_RND.value not in by_type
    edges = set(edge_rows)
    assert (JobType.QUALITY_FREEZE.value, JobType.BROKER_SYNC.value) in edges
    assert (JobType.DIAGNOSTICS_RUN.value, JobType.QUALITY_FREEZE.value) in edges
    assert (JobType.AGENT_RESEARCH.value, JobType.DIAGNOSTICS_RUN.value) in edges
    assert (JobType.OPTIMIZATION_PROPOSE.value, JobType.AGENT_RESEARCH.value) in edges
    assert (JobType.PAPER_ADMIT_ORDERS.value, JobType.OPTIMIZATION_PROPOSE.value) in edges
    assert (JobType.DAILY_BRIEF.value, JobType.PAPER_PERFORMANCE.value) in edges
    assert (JobType.AUDIT_MANIFEST.value, JobType.DAILY_BRIEF.value) in edges
    assert executed.index(JobType.QUALITY_FREEZE) > executed.index(
        JobType.SHADOW_ADVANCE_DUE
    )
    assert executed.index(JobType.DIAGNOSTICS_RUN) > executed.index(
        JobType.QUALITY_FREEZE
    )
    assert executed.index(JobType.OPTIMIZATION_PROPOSE) > executed.index(
        JobType.AGENT_RESEARCH
    )


@pytest.mark.asyncio
async def test_collection_stages_skip_when_scheduler_cannot_freeze_a_universe(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(session, now=now, profile_id=_PROFILE)
    assert root is not None
    assert root.payload["universe_snapshot_id"] is None
    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-no-universe-worker",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: now,
        claim_registered_only=True,
    )
    async with factory() as session:
        collection_rows = tuple(
            (
                await session.execute(
                    select(Job.job_type, Job.status, Job.result_code).where(
                        Job.job_type.in_(
                            (
                                JobType.COLLECT_MARKET.value,
                                JobType.COLLECT_SEC_MACRO.value,
                            )
                        )
                    )
                )
            ).tuples()
        )
    assert set(collection_rows) == {
        (JobType.COLLECT_MARKET.value, "skipped", "NO_FROZEN_UNIVERSE"),
        (JobType.COLLECT_SEC_MACRO.value, "skipped", "NO_FROZEN_UNIVERSE"),
    }


@pytest.mark.asyncio
async def test_quality_cutoff_skips_decision_chain_but_completes_audit_tail(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = datetime(2026, 8, 27, 22, 1, tzinfo=UTC)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    assert root is not None and root.payload["session"] == "2026-08-27"
    executed: list[JobType] = []

    async def cutoff_executor(
        job_type: JobType,
        context: JobHandlerContext,
        payload: DailyStagePayload,
    ) -> JobHandlerResult:
        del context
        executed.append(job_type)
        if job_type is JobType.QUALITY_FREEZE:
            assert datetime.fromisoformat(payload.quality_cutoff_at) < now
            return JobHandlerResult(
                outcome="skipped",
                result_code="SKIPPED_DATA_QUALITY",
            )
        return JobHandlerResult(
            outcome="succeeded",
            result_code=f"{job_type.name}_OK",
        )

    assert await _drain_daily_queues(factory, now=now, executor=cutoff_executor) == 11
    async with factory() as session:
        rows = tuple(
            (
                await session.execute(
                    select(Job.job_type, Job.status, Job.result_code).where(
                        Job.job_type.in_(
                            (
                                JobType.QUALITY_FREEZE.value,
                                JobType.DIAGNOSTICS_RUN.value,
                                JobType.AGENT_RESEARCH.value,
                                JobType.OPTIMIZATION_PROPOSE.value,
                                JobType.PAPER_ADMIT_ORDERS.value,
                                JobType.PAPER_PERFORMANCE.value,
                                JobType.DAILY_BRIEF.value,
                                JobType.AUDIT_MANIFEST.value,
                            )
                        )
                    )
                )
            ).tuples()
        )
    outcomes = {job_type: (status, code) for job_type, status, code in rows}
    assert outcomes[JobType.QUALITY_FREEZE.value] == (
        "skipped",
        "SKIPPED_DATA_QUALITY",
    )
    for job_type in (
        JobType.DIAGNOSTICS_RUN,
        JobType.AGENT_RESEARCH,
        JobType.OPTIMIZATION_PROPOSE,
    ):
        assert outcomes[job_type.value] == ("skipped", "DEPENDENCY_DATA_QUALITY")
        assert job_type not in executed
    assert JobType.PAPER_ADMIT_ORDERS.value not in outcomes
    assert outcomes[JobType.PAPER_PERFORMANCE.value][0] == "succeeded"
    assert outcomes[JobType.DAILY_BRIEF.value][0] == "succeeded"
    assert outcomes[JobType.AUDIT_MANIFEST.value][0] == "succeeded"


@pytest.mark.asyncio
async def test_local_safe_profile_never_claims_broker_or_placeholder_agent_jobs(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    assert root is not None
    registry = build_local_daily_pipeline_registry()
    assert JobType.AGENT_RESEARCH.value not in registry.job_types
    assert JobType.BROKER_SYNC.value not in registry.job_types
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-local-safe-worker",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: now,
        claim_registered_only=True,
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-local-safe-worker",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: now,
        claim_registered_only=True,
    )
    assert not await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-local-safe-worker",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: now,
        claim_registered_only=True,
    )
    async with factory() as session:
        jobs = tuple((await session.scalars(select(Job))).all())
        broker_leases = await session.scalar(
            select(func.count())
            .select_from(JobLease)
            .join(Job, Job.id == JobLease.job_id)
            .where(Job.queue == QueueName.BROKER.value)
        )
    assert broker_leases == 0
    assert {job.status for job in jobs} <= {"succeeded", "skipped"}
    results = {job.job_type: job.result_code for job in jobs}
    assert results[JobType.BROKER_SYNC.value] == "SKIPPED_CAPABILITY"
    assert results[JobType.AGENT_RESEARCH.value] == "SKIPPED_CAPABILITY"
    assert results[JobType.PAPER_EXECUTE_DUE.value] == "SKIPPED_NO_DUE_TARGETS"
    assert results[JobType.PAPER_PERFORMANCE.value] == "PERFORMANCE_PUBLISHED"


@pytest.mark.asyncio
async def test_general_root_materializes_provider_children_without_registering_handlers(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    assert root is not None
    registry = build_local_daily_pipeline_registry(include_provider_stages=True)
    assert JobType.COLLECT_MARKET.value not in registry.job_types
    assert JobType.COLLECT_SEC_MACRO.value not in registry.job_types

    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-provider-materializer",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0027_fosun_market_authority",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        children = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(Job.job_type.in_((
                        JobType.COLLECT_MARKET.value,
                        JobType.COLLECT_SEC_MACRO.value,
                    )))
                    .order_by(Job.job_type)
                )
            ).all()
        )
    assert [child.job_type for child in children] == [
        JobType.COLLECT_MARKET.value,
        JobType.COLLECT_SEC_MACRO.value,
    ]
    assert all(child.queue == QueueName.PROVIDER.value for child in children)
    assert all(child.status == "queued" for child in children)


@pytest.mark.asyncio
async def test_local_audit_manifest_runs_only_with_injected_object_store(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id="system:daily-test",
            action="audit.daily.test",
            schema_version="audit_test_v1",
            payload={"status": "ok", "account_id": "must-not-export"},
            occurred_at=now,
        )
        assert await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    store = _AuditStore()
    registry = build_local_daily_pipeline_registry(audit_object_store=store)
    for _ in range(4):
        await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id="daily-audit-worker",
            worker_started_at=now,
            process_version="test-v1",
            migration_head="0021_manual_job_retry_allowance",
            clock=lambda: now,
            claim_registered_only=True,
        )
    async with factory() as session:
        manifest_job = await session.scalar(
            select(Job).where(Job.job_type == JobType.AUDIT_MANIFEST.value)
        )
    assert manifest_job is not None
    assert (manifest_job.status, manifest_job.result_code) == (
        "succeeded",
        "AUDIT_MANIFEST_PUBLISHED",
    )
    assert len(store.objects) == 1
    content = gzip.decompress(next(iter(store.objects.values())))
    assert b"must-not-export" not in content


@pytest.mark.asyncio
async def test_local_safe_profile_advances_due_internal_paper_order(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    planned_tick = datetime(2026, 8, 26, 20, 31, tzinfo=UTC)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=planned_tick,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    assert root is not None and root.payload["session"] == "2026-08-26"
    registry = build_local_daily_pipeline_registry()
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-paper-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: planned_tick,
        claim_registered_only=True,
    )
    async with factory() as session:
        execution_job = await session.scalar(
            select(Job).where(
                Job.job_type == JobType.PAPER_EXECUTE_DUE.value,
                Job.payload["target_id"].as_string() == str(seed.order_id),
            )
        )
    assert execution_job is not None
    assert execution_job.effective_at == FIRST_EVALUATION
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-paper-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: FIRST_EVALUATION,
        claim_registered_only=True,
    )
    async with factory() as session:
        completed = await session.get(Job, execution_job.id)
    assert completed is not None
    assert completed.status == "succeeded"
    assert completed.result_code is not None
    assert completed.result_code.startswith("PAPER_")


@pytest.mark.asyncio
async def test_nonterminal_paper_execution_enqueues_next_five_minute_window(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("10"), Decimal("10")),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    planned_tick = datetime(2026, 8, 26, 20, 31, tzinfo=UTC)
    async with factory() as session, session.begin():
        assert await schedule_due_daily_pipeline(
            session,
            now=planned_tick,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    registry = build_local_daily_pipeline_registry()
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-partial-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: planned_tick,
        claim_registered_only=True,
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-partial-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: FIRST_EVALUATION,
        claim_registered_only=True,
    )
    async with factory() as session:
        jobs = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(
                        Job.job_type == JobType.PAPER_EXECUTE_DUE.value,
                        Job.payload["target_id"].as_string() == str(seed.order_id),
                    )
                    .order_by(Job.effective_at)
                )
            ).all()
        )
    assert len(jobs) == 2
    assert jobs[0].result_code == "PAPER_NO_CAPACITY"
    assert jobs[1].status == "queued"
    assert jobs[1].effective_at == FIRST_EVALUATION + timedelta(minutes=5)
