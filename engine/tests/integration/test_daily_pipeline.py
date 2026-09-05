"""PostgreSQL contracts for the deterministic daily automation DAG."""

import gzip
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import nexus.jobs.daily_local as daily_local_module
import pytest
from helpers.paper_execution import FIRST_EVALUATION, seed_paper_execution
from helpers.paper_performance import seed_paper_performance, seed_second_paper_close
from helpers.shadow import seed_complete_shadow_session_inputs, seed_started_shadow
from nexus.audit.service import append_audit_event
from nexus.jobs.daily_local import (
    LOCAL_SAFE_DAILY_PROFILE,
    build_local_daily_pipeline_registry,
)
from nexus.jobs.daily_payloads import DailyPipelinePayload, DailyStagePayload
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
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionAdjustmentEventRecord,
    PaperPositionSnapshotRecord,
    PaperRequestIdempotencyRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
)
from nexus.persistence.models.runtime import (
    AuditEvent,
    IntegrationStatus,
    Job,
    JobDependency,
    JobLease,
    OutboxEvent,
)
from nexus.persistence.models.shadow import (
    ShadowAdvanceAttemptRecord,
    ShadowCheckpointRecord,
    ShadowPortfolioStateRecord,
    ShadowSealRecord,
    ShadowSessionAuthorityRecord,
    ShadowSimulatedFillRecord,
    ShadowSimulatedOutcomeRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.shadow.service import advance_shadow_session
from sqlalchemy import func, select, text
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


async def _paper_execution_domain_counts(
    session: AsyncSession,
    *,
    account_id: UUID,
    order_id: UUID,
) -> tuple[int, ...]:
    counts: list[int] = []
    for model in (
        PaperOrderEventRecord,
        PaperExecutionWindowAuthorityRecord,
        PaperFillRecord,
        PaperRequestIdempotencyRecord,
    ):
        counts.append(
            int(
                await session.scalar(
                    select(func.count()).select_from(model).where(model.order_id == order_id)
                )
                or 0
            )
        )
    reservation_ids = select(PaperReservationRecord.id).where(
        PaperReservationRecord.order_id == order_id
    )
    counts.extend(
        (
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperReservationRecord)
                    .where(PaperReservationRecord.order_id == order_id)
                )
                or 0
            ),
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id.in_(reservation_ids))
                )
                or 0
            ),
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperLedgerTransactionRecord)
                    .where(PaperLedgerTransactionRecord.order_id == order_id)
                )
                or 0
            ),
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperLedgerEntryRecord)
                    .join(
                        PaperLedgerTransactionRecord,
                        PaperLedgerTransactionRecord.id == PaperLedgerEntryRecord.transaction_id,
                    )
                    .where(PaperLedgerTransactionRecord.order_id == order_id)
                )
                or 0
            ),
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(
                        AuditEvent.schema_version == "paper_execution_v1",
                        AuditEvent.payload["order_id"].as_string() == str(order_id),
                    )
                )
                or 0
            ),
            int(
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(
                        OutboxEvent.schema_version == "paper_execution_v1",
                        OutboxEvent.aggregate_id == order_id,
                    )
                )
                or 0
            ),
        )
    )
    counts.append(
        int(
            await session.scalar(
                select(func.count())
                .select_from(PaperPositionAdjustmentEventRecord)
                .where(PaperPositionAdjustmentEventRecord.account_id == account_id)
            )
            or 0
        )
    )
    return tuple(counts)


async def _paper_performance_fact_counts(session: AsyncSession) -> tuple[int, int, int]:
    counts: list[int] = []
    for model in (
        PaperPerformanceSnapshotRecord,
        PaperPositionSnapshotRecord,
        PaperPerformanceAttributionRecord,
    ):
        counts.append(int(await session.scalar(select(func.count()).select_from(model)) or 0))
    return cast(tuple[int, int, int], tuple(counts))


async def _shadow_session_fact_counts(
    session: AsyncSession,
    *,
    shadow_group_id: UUID,
) -> tuple[int, ...]:
    counts = [
        int(
            await session.scalar(
                select(func.count())
                .select_from(model)
                .where(model.shadow_group_id == shadow_group_id)
            )
            or 0
        )
        for model in (
            ShadowSessionAuthorityRecord,
            ShadowPortfolioStateRecord,
            ShadowSimulatedOutcomeRecord,
            ShadowSimulatedFillRecord,
            ShadowAdvanceAttemptRecord,
            ShadowCheckpointRecord,
            ShadowSealRecord,
        )
    ]
    counts.append(
        int(
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.payload["shadow_group_id"].as_string() == str(shadow_group_id),
                    AuditEvent.action.in_(
                        ("shadow.session_applied", "shadow.session_failed", "shadow.sealed")
                    ),
                )
            )
            or 0
        )
    )
    counts.append(
        int(
            await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.aggregate_id == shadow_group_id,
                    OutboxEvent.event_type.in_(
                        ("shadow.session_applied", "shadow.session_failed", "shadow.sealed")
                    ),
                )
            )
            or 0
        )
    )
    return tuple(counts)


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
                    version="us_liquid_v1",
                    effective_from=_THURSDAY,
                    source="fixture",
                    content_hash="a" * 64,
                    published_at=datetime(2026, 8, 1, tzinfo=UTC),
                )
            )


async def _seed_us_universe_members(
    factory: async_sessionmaker[AsyncSession], *, count: int
) -> None:
    async with factory() as session, session.begin():
        snapshot = UniverseSnapshot(
            id=_UNIVERSE_ID,
            key="us_liquid",
            version="us_liquid_v1",
            effective_from=_THURSDAY,
            source="fixture",
            content_hash="a" * 64,
            published_at=None,
        )
        instruments = tuple(
            Instrument(
                id=UUID(int=10_000 + index),
                asset_type="equity",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="equity",
            )
            for index in range(count)
        )
        session.add(snapshot)
        session.add_all(instruments)
        await session.flush()
        session.add_all(
            InstrumentAlias(
                instrument_id=instrument.id,
                symbol=f"TST{index}",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
            for index, instrument in enumerate(instruments)
        )
        session.add_all(
            UniverseMember(
                universe_snapshot_id=_UNIVERSE_ID,
                instrument_id=instrument.id,
                inclusion_reason="daily-shard-test",
            )
            for instrument in instruments
        )
        await session.flush()
        snapshot.published_at = datetime(2026, 8, 1, tzinfo=UTC)


async def _seed_fosun_daily_authority(
    factory: async_sessionmaker[AsyncSession],
    *,
    now: datetime,
) -> None:
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=UUID("0198d500-1000-7000-8000-000000002231"),
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="b" * 64,
                expires_at=now + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=UUID("0198d500-1000-7000-8000-000000002232"),
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="b" * 64,
                created_at=now - timedelta(hours=2),
            )
        )


async def _seed_us_universe_with_duplicate_ticker_symbol(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_instrument_period"
            )
        )
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT IF EXISTS "
                "ex_instrument_aliases_ticker_symbol_period"
            )
        )
        snapshot = UniverseSnapshot(
            id=_UNIVERSE_ID,
            key="us_liquid",
            version="us_liquid_v1",
            effective_from=_THURSDAY,
            source="fixture",
            content_hash="a" * 64,
            published_at=None,
        )
        instruments = (
            Instrument(
                id=UUID(int=19_001),
                asset_type="equity",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="equity",
            ),
            Instrument(
                id=UUID(int=19_002),
                asset_type="equity",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="equity",
            ),
        )
        session.add(snapshot)
        session.add_all(instruments)
        await session.flush()
        session.add_all(
            (
                InstrumentAlias(
                    instrument_id=instruments[0].id,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                ),
                InstrumentAlias(
                    instrument_id=instruments[1].id,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=date(2021, 1, 1),
                    valid_to=None,
                ),
            )
        )
        session.add_all(
            UniverseMember(
                universe_snapshot_id=_UNIVERSE_ID,
                instrument_id=instrument.id,
                inclusion_reason="duplicate-ticker-authority-test",
            )
            for instrument in instruments
        )
        await session.flush()
        snapshot.published_at = datetime(2026, 8, 1, tzinfo=UTC)


async def _seed_us_universe_with_noncanonical_ticker_symbol(
    factory: async_sessionmaker[AsyncSession],
    *,
    symbol: str,
) -> None:
    async with factory() as session, session.begin():
        snapshot = UniverseSnapshot(
            id=_UNIVERSE_ID,
            key="us_liquid",
            version="us_liquid_v1",
            effective_from=_THURSDAY,
            source="fixture",
            content_hash="a" * 64,
            published_at=None,
        )
        instrument = Instrument(
            id=UUID(int=19_101),
            asset_type="equity",
            exchange="XNYS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="equity",
        )
        session.add_all((snapshot, instrument))
        await session.flush()
        session.add_all(
            (
                InstrumentAlias(
                    instrument_id=instrument.id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                ),
                UniverseMember(
                    universe_snapshot_id=_UNIVERSE_ID,
                    instrument_id=instrument.id,
                    inclusion_reason="noncanonical-ticker-authority-test",
                ),
            )
        )
        await session.flush()
        snapshot.published_at = datetime(2026, 8, 1, tzinfo=UTC)


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
                migration_head="0029_fosun_bar_lineage",
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
    assert (
        await _drain_daily_queues(
            factory,
            now=now,
            executor=_successful_executor(executed),
        )
        == 11
    )

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
    assert by_type[JobType.COLLECT_MARKET.value].status == "skipped"
    assert by_type[JobType.COLLECT_MARKET.value].result_code == "FOSUN_MARKET_AUTHORITY_UNAVAILABLE"
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
    assert executed.index(JobType.QUALITY_FREEZE) > executed.index(JobType.SHADOW_ADVANCE_DUE)
    assert executed.index(JobType.DIAGNOSTICS_RUN) > executed.index(JobType.QUALITY_FREEZE)
    assert executed.index(JobType.OPTIMIZATION_PROPOSE) > executed.index(JobType.AGENT_RESEARCH)


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_mode", ("real_time", "delayed"))
async def test_scheduler_freezes_actual_fosun_delivery_authority_at_schedule_time(
    migrated_database_url: str,
    delivery_mode: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    capability_id = UUID("0198d500-1000-7000-8000-000000002221")
    status_id = UUID("0198d500-1000-7000-8000-000000002222")
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=capability_id,
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="b" * 64,
                expires_at=now + timedelta(hours=1),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": delivery_mode,
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=status_id,
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="b" * 64,
                created_at=now - timedelta(hours=2),
            )
        )
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )

    assert root is not None
    payload = DailyPipelinePayload.model_validate(root.payload)
    assert payload.market_source == "fosun"
    assert payload.market_authority is not None
    assert payload.market_authority.capability_id == capability_id
    assert payload.market_authority.status_id == status_id
    assert payload.market_authority.delivery_mode == delivery_mode


@pytest.mark.asyncio
async def test_scheduler_reuses_session_root_after_fosun_authority_expires(
    migrated_database_url: str,
) -> None:
    """Authority drift must not generate a second full DAG for one session/profile."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    capability_id = UUID("0198d500-1000-7000-8000-000000002241")
    status_id = UUID("0198d500-1000-7000-8000-000000002242")
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=capability_id,
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="d" * 64,
                expires_at=now + timedelta(hours=1),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=status_id,
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="d" * 64,
                created_at=now - timedelta(hours=2),
            )
        )

    async with factory() as session, session.begin():
        first = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    async with factory() as session, session.begin():
        after_expiry = await schedule_due_daily_pipeline(
            session,
            now=now + timedelta(hours=2),
            profile_id=_PROFILE,
        )

    assert first is not None and after_expiry is not None
    assert first.id == after_expiry.id
    assert first.payload["market_authority"] is not None
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Job)
                .where(Job.job_type == JobType.DAILY_PIPELINE.value)
            )
            == 1
        )


@pytest.mark.asyncio
async def test_fosun_daily_materializes_one_rate_limited_job_per_instrument(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    await _seed_us_universe_members(factory, count=49)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    capability_id = UUID("0198d500-1000-7000-8000-000000002231")
    status_id = UUID("0198d500-1000-7000-8000-000000002232")
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=capability_id,
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="b" * 64,
                expires_at=now + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=status_id,
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="b" * 64,
                created_at=now - timedelta(hours=2),
            )
        )
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    assert root is not None

    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-fosun-sharding-materializer",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        children = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(Job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
                    .order_by(Job.available_at, Job.id)
                )
            ).all()
        )
        publish = await session.scalar(
            select(Job).where(Job.job_type == JobType.PUBLISH_FOSUN_DAILY.value)
        )
        assert publish is not None
        dependencies = tuple(
            (
                await session.execute(
                    select(
                        JobDependency.depends_on_job_id,
                        JobDependency.required_outcome,
                    ).where(JobDependency.job_id == publish.id)
                )
            ).tuples()
        )
        quality_job = aliased(Job)
        quality_parent = aliased(Job)
        quality_parent_types = set(
            (
                await session.scalars(
                    select(quality_parent.job_type)
                    .select_from(JobDependency)
                    .join(quality_job, quality_job.id == JobDependency.job_id)
                    .join(
                        quality_parent,
                        quality_parent.id == JobDependency.depends_on_job_id,
                    )
                    .where(quality_job.job_type == JobType.QUALITY_FREEZE.value)
                )
            ).all()
        )

    assert len(children) == 49
    assert all(child.max_attempts == 1 for child in children)
    assert all(child.queue == QueueName.PROVIDER.value for child in children)
    assert all(
        right.available_at - left.available_at >= timedelta(seconds=7)
        for left, right in zip(children, children[1:], strict=False)
    )
    for index, child in enumerate(children):
        window_end = child.available_at + timedelta(seconds=60)
        assert sum(candidate.available_at < window_end for candidate in children[index:]) <= 9
    assert set(dependencies) == {(child.id, "succeeded") for child in children}
    assert JobType.PUBLISH_FOSUN_DAILY.value in quality_parent_types
    assert JobType.COLLECT_MARKET.value not in quality_parent_types


@pytest.mark.asyncio
async def test_fosun_daily_rejects_unverified_fiftieth_instrument(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    await _seed_us_universe_members(factory, count=50)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    await _seed_fosun_daily_authority(factory, now=now)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(session, now=now, profile_id=_PROFILE)
    assert root is not None

    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-fosun-50-fail-closed",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        market_stage = await session.scalar(
            select(Job).where(Job.job_type == JobType.COLLECT_MARKET.value)
        )
        child_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
        )
        publish_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.job_type == JobType.PUBLISH_FOSUN_DAILY.value)
        )

    assert market_stage is not None
    assert (market_stage.status, market_stage.result_code) == (
        "skipped",
        "FROZEN_UNIVERSE_TOO_LARGE",
    )
    assert child_count == 0
    assert publish_count == 0


@pytest.mark.asyncio
async def test_fosun_daily_materializer_rejects_two_instruments_with_one_ticker(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    await _seed_us_universe_with_duplicate_ticker_symbol(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=UUID("0198d500-1000-7000-8000-000000002241"),
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="b" * 64,
                expires_at=now + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=UUID("0198d500-1000-7000-8000-000000002242"),
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="b" * 64,
                created_at=now - timedelta(hours=2),
            )
        )
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(session, now=now, profile_id=_PROFILE)
    assert root is not None

    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-duplicate-ticker-materializer",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        refreshed = await session.get(Job, root.id)
        market_stage = await session.scalar(
            select(Job).where(Job.job_type == JobType.COLLECT_MARKET.value)
        )
        child_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
        )
    assert refreshed is not None
    assert (refreshed.status, refreshed.result_code) == (
        "succeeded",
        "PIPELINE_MATERIALIZED",
    )
    assert market_stage is not None
    assert (market_stage.status, market_stage.result_code) == (
        "skipped",
        "FROZEN_INSTRUMENT_ALIAS_INVALID",
    )
    assert child_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", ("aapl", "AA/PL"))
async def test_fosun_daily_materializer_terminally_skips_noncanonical_ticker(
    migrated_database_url: str,
    symbol: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    await _seed_us_universe_with_noncanonical_ticker_symbol(factory, symbol=symbol)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        session.add(
            ProviderCapability(
                id=UUID("0198d500-1000-7000-8000-000000002251"),
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="b" * 64,
                expires_at=now + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=now - timedelta(hours=1),
            )
        )
        session.add(
            IntegrationStatus(
                id=UUID("0198d500-1000-7000-8000-000000002252"),
                provider="fosun",
                status="read_only",
                last_success_at=now,
                version=1,
                mode_changed_at=now - timedelta(hours=2),
                capability_invalidated_at=None,
                admitted_contract_hash="b" * 64,
                created_at=now - timedelta(hours=2),
            )
        )
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(session, now=now, profile_id=_PROFILE)
    assert root is not None

    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id=f"daily-noncanonical-ticker-{symbol}",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0032_ticker_alias_authority",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        market_stage = await session.scalar(
            select(Job).where(Job.job_type == JobType.COLLECT_MARKET.value)
        )
        child_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.job_type == JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value)
        )
    assert market_stage is not None
    assert (market_stage.status, market_stage.result_code) == (
        "skipped",
        "FROZEN_INSTRUMENT_ALIAS_INVALID",
    )
    assert child_count == 0


@pytest.mark.asyncio
async def test_scheduler_preserves_explicit_alpaca_market_mode(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)

    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
            market_source="alpaca",
        )

    assert root is not None
    payload = DailyPipelinePayload.model_validate(root.payload)
    assert payload.market_source == "alpaca"
    assert payload.market_authority is None


@pytest.mark.asyncio
async def test_missing_fosun_authority_terminal_skips_only_market_collection(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=now,
            profile_id=_PROFILE,
        )
    assert root is not None
    assert root.payload["market_authority"] is None

    registry = build_daily_pipeline_registry(
        profile_id=_PROFILE,
        stage_executor=_successful_executor([]),
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-no-market-authority-worker",
        worker_started_at=now,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
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
        (
            JobType.COLLECT_MARKET.value,
            "skipped",
            "FOSUN_MARKET_AUTHORITY_UNAVAILABLE",
        ),
        (JobType.COLLECT_SEC_MACRO.value, "queued", None),
    }


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

    assert await _drain_daily_queues(factory, now=now, executor=cutoff_executor) == 10
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
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: now,
        claim_registered_only=True,
    )

    async with factory() as session:
        children = tuple(
            (
                await session.scalars(
                    select(Job)
                    .where(
                        Job.job_type.in_(
                            (
                                JobType.COLLECT_MARKET.value,
                                JobType.COLLECT_SEC_MACRO.value,
                            )
                        )
                    )
                    .order_by(Job.job_type)
                )
            ).all()
        )
    assert [child.job_type for child in children] == [
        JobType.COLLECT_MARKET.value,
        JobType.COLLECT_SEC_MACRO.value,
    ]
    assert all(child.queue == QueueName.PROVIDER.value for child in children)
    assert {(child.job_type, child.status, child.result_code) for child in children} == {
        (
            JobType.COLLECT_MARKET.value,
            "skipped",
            "FOSUN_MARKET_AUTHORITY_UNAVAILABLE",
        ),
        (JobType.COLLECT_SEC_MACRO.value, "queued", None),
    }


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


@pytest.mark.parametrize("legacy_market_policy", (False, True))
@pytest.mark.asyncio
async def test_preexisting_unpriced_paper_order_never_fills_and_is_released_at_cutoff(
    migrated_database_url: str,
    legacy_market_policy: bool,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        legacy_market_policy=legacy_market_policy,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    planned_tick = datetime(2026, 8, 26, 20, 31, tzinfo=UTC)
    async with factory() as session:
        before_counts = await _paper_execution_domain_counts(
            session,
            account_id=seed.account_id,
            order_id=seed.order_id,
        )
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
        observed_jobs = tuple(
            (
                row.job_type,
                row.status,
                row.result_code,
                row.last_error_code,
                row.payload.get("target_id"),
            )
            for row in (await session.scalars(select(Job).order_by(Job.created_at))).all()
        )
    assert execution_job is not None, observed_jobs
    assert execution_job.effective_at == FIRST_EVALUATION
    cutoff = FIRST_EVALUATION + timedelta(minutes=20)
    for _ in range(64):
        async with factory() as session:
            completed = await session.scalar(
                select(Job)
                .where(
                    Job.job_type == JobType.PAPER_EXECUTE_DUE.value,
                    Job.payload["target_id"].as_string() == str(seed.order_id),
                    Job.result_code == "PAPER_CANCELLED",
                )
                .limit(1)
            )
        if completed is not None:
            break
        worked = await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id="daily-paper-worker",
            worker_started_at=planned_tick,
            process_version="test-v1",
            migration_head="0021_manual_job_retry_allowance",
            clock=lambda: cutoff,
            claim_registered_only=True,
        )
        if not worked:
            break
    async with factory() as session:
        after_counts = await _paper_execution_domain_counts(
            session,
            account_id=seed.account_id,
            order_id=seed.order_id,
        )
        execution_jobs = tuple(
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
        stage_jobs = tuple(
            (
                row.job_type,
                row.status,
                row.result_code,
                row.last_error_code,
                row.effective_at,
            )
            for row in (await session.scalars(select(Job).order_by(Job.created_at))).all()
        )
        reservation = await session.scalar(
            select(PaperReservationRecord).where(PaperReservationRecord.order_id == seed.order_id)
        )
        assert reservation is not None
        reservation_events = tuple(
            (
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            ).all()
        )
        order_events = tuple(
            (
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == seed.order_id)
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            ).all()
        )
    assert completed is not None, stage_jobs
    assert (completed.status, completed.result_code) == (
        "succeeded",
        "PAPER_CANCELLED",
    )
    assert [job.result_code for job in execution_jobs] == [
        "PAPER_NO_DATA",
        "PAPER_NO_DATA",
        "PAPER_NO_DATA",
        "PAPER_NO_DATA",
        "PAPER_CANCELLED",
    ]
    assert [job.effective_at for job in execution_jobs] == [
        FIRST_EVALUATION + timedelta(minutes=offset) for offset in (0, 5, 10, 15, 20)
    ]
    assert after_counts[0] == before_counts[0] + 1
    assert after_counts[1] == before_counts[1] == 0
    assert after_counts[2] == before_counts[2] == 0
    assert after_counts[3] == before_counts[3] + 5
    assert after_counts[4] == before_counts[4]
    assert after_counts[5] == before_counts[5] + 1
    assert after_counts[6] == before_counts[6] + 1
    assert after_counts[7] == before_counts[7] + 2
    assert after_counts[10] == before_counts[10] == 0
    assert [event.event_type for event in reservation_events] == [
        "reserved",
        "released",
    ]
    assert order_events[-1].to_status == "cancelled"
    assert order_events[-1].reason_code == "paper_execution_price_authority_unavailable"
    assert order_events[-1].actor_kind == "task19"


@pytest.mark.parametrize("existing_session", (False, True))
@pytest.mark.asyncio
async def test_daily_shadow_preserves_replay_but_skips_new_unpriced_session(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    existing_session: bool,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url,
        started=started,
        ordinal=1,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        calendar = await session.get(
            ExchangeCalendarSessionRecord,
            (
                started.eligible.calendar_id,
                started.start.start_session,
            ),
        )
    assert calendar is not None
    planned_tick = calendar.market_close_at + timedelta(minutes=31)
    if existing_session:
        with monkeypatch.context() as legacy_authority:
            legacy_authority.setattr(
                "nexus.shadow.service.current_proposal_shadow_price_authority_available",
                lambda _source_policy_id: True,
            )
            async with factory() as session, session.begin():
                applied = await advance_shadow_session(
                    session,
                    group_id=started.eligible.group_id,
                    session_date=started.start.start_session,
                    observed_at=observed_at,
                )
        assert applied.result == "applied"
    async with factory() as session:
        before_counts = await _shadow_session_fact_counts(
            session,
            shadow_group_id=started.start.shadow_group_id,
        )
    monkeypatch.setattr(
        daily_local_module,
        "current_proposal_shadow_price_authority_available",
        lambda _source_policy_id: False,
        raising=False,
    )

    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=planned_tick,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    assert root is not None
    assert root.payload["session"] == started.start.start_session.isoformat()
    registry = build_local_daily_pipeline_registry()
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-shadow-root-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: planned_tick,
        claim_registered_only=True,
    )
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-shadow-stage-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: planned_tick,
        claim_registered_only=True,
    )

    async with factory() as session:
        shadow_job = await session.scalar(
            select(Job).where(
                Job.job_type == JobType.SHADOW_ADVANCE_DUE.value,
                Job.payload["target_id"].as_string() == str(started.eligible.group_id),
            )
        )
        after_counts = await _shadow_session_fact_counts(
            session,
            shadow_group_id=started.start.shadow_group_id,
        )

    assert shadow_job is not None
    assert (shadow_job.status, shadow_job.result_code) == (
        ("succeeded", "SHADOW_REPLAYED")
        if existing_session
        else ("skipped", "SHADOW_PRICE_AUTHORITY_UNAVAILABLE")
    )
    assert after_counts == before_counts
    if existing_session:
        assert before_counts[0] == 1
    else:
        assert before_counts == (0, 0, 0, 0, 0, 0, 0, 0, 0)


@pytest.mark.asyncio
async def test_daily_performance_skips_open_position_without_current_price_authority(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    await seed_second_paper_close(
        migrated_database_url,
        execution=seed.execution,
        close=Decimal("111"),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    planned_tick = datetime(2026, 8, 27, 20, 31, tzinfo=UTC)
    async with factory() as session:
        before_counts = await _paper_performance_fact_counts(session)
    async with factory() as session, session.begin():
        root = await schedule_due_daily_pipeline(
            session,
            now=planned_tick,
            profile_id=LOCAL_SAFE_DAILY_PROFILE,
        )
    assert root is not None and root.payload["session"] == "2026-08-27"
    registry = build_local_daily_pipeline_registry()
    assert await run_job_worker_once(
        factory,
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="daily-performance-worker",
        worker_started_at=planned_tick,
        process_version="test-v1",
        migration_head="0021_manual_job_retry_allowance",
        clock=lambda: planned_tick,
        claim_registered_only=True,
    )
    async with factory() as session:
        performance_job = await session.scalar(
            select(Job).where(
                Job.job_type == JobType.PAPER_PERFORMANCE.value,
                Job.payload["target_id"].as_string() == str(seed.execution.account_id),
            )
        )
    assert performance_job is not None
    for _ in range(8):
        async with factory() as session:
            current = await session.get(Job, performance_job.id)
        assert current is not None
        if current.status != "queued":
            break
        worked = await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id="daily-performance-worker",
            worker_started_at=planned_tick,
            process_version="test-v1",
            migration_head="0021_manual_job_retry_allowance",
            clock=lambda: planned_tick,
            claim_registered_only=True,
        )
        if not worked:
            break
    async with factory() as session:
        completed = await session.get(Job, performance_job.id)
        after_counts = await _paper_performance_fact_counts(session)

    assert completed is not None
    assert (completed.status, completed.result_code) == (
        "skipped",
        "PERFORMANCE_PRICE_AUTHORITY_UNAVAILABLE",
    )
    assert after_counts == before_counts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "other_key",
        "other_version",
        "wrong_local_source",
        "future_effective",
        "future_published",
        "valid_local",
        "fallback_only",
        "no_eligible",
    ],
)
async def test_daily_universe_selection_cannot_be_taken_over_by_unrelated_pool(
    migrated_database_url, case
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    fallback_id, other_id = UUID(int=820001), UUID(int=820002)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    try:
        async with factory() as s, s.begin():
            if case != "no_eligible":
                s.add(
                    UniverseSnapshot(
                        id=fallback_id,
                        key="us_liquid",
                        version="us_liquid_v1",
                        effective_from=_THURSDAY,
                        source="fixture",
                        content_hash="a" * 64,
                        published_at=_FRIDAY_CLOSE
                        if case == "valid_local"
                        else datetime(2026, 8, 1, tzinfo=UTC),
                    )
                )
            if case != "fallback_only":
                s.add(
                    UniverseSnapshot(
                        id=other_id,
                        key="internal_backtest"
                        if case in {"other_key", "no_eligible"}
                        else ("us_liquid" if case == "other_version" else "fosun_local_holdings"),
                        version="newer-pool",
                        effective_from=_MONDAY if case == "future_effective" else _THURSDAY,
                        source="unrelated_source"
                        if case == "wrong_local_source"
                        else "fosun_local_read_only",
                        content_hash="b" * 64,
                        published_at=now + timedelta(hours=1)
                        if case == "future_published"
                        else _FRIDAY_CLOSE - timedelta(hours=1),
                    )
                )
        async with factory() as s, s.begin():
            root = await schedule_due_daily_pipeline(s, now=now, profile_id=_PROFILE)
            expected = (
                None
                if case == "no_eligible"
                else (other_id if case == "valid_local" else fallback_id)
            )
            assert root.payload["universe_snapshot_id"] == (str(expected) if expected else None)
            assert root.payload["universe_content_hash"] == (
                None if expected is None else ("b" if case == "valid_local" else "a") * 64
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_existing_daily_root_keeps_its_frozen_pool_after_new_local_pool(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_calendar(factory, include_universe=False)
    now = _FRIDAY_CLOSE + timedelta(minutes=31)
    try:
        async with factory() as s, s.begin():
            s.add(
                UniverseSnapshot(
                    id=UUID(int=821001),
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=_THURSDAY,
                    source="fixture",
                    content_hash="a" * 64,
                    published_at=datetime(2026, 8, 1, tzinfo=UTC),
                )
            )
        async with factory() as s, s.begin():
            first = await schedule_due_daily_pipeline(s, now=now, profile_id=_PROFILE)
            frozen = dict(first.payload)
        async with factory() as s, s.begin():
            s.add(
                UniverseSnapshot(
                    id=UUID(int=821002),
                    key="fosun_local_holdings",
                    version="new-local",
                    effective_from=_THURSDAY,
                    source="fosun_local_read_only",
                    content_hash="b" * 64,
                    published_at=_FRIDAY_CLOSE - timedelta(hours=1),
                )
            )
        async with factory() as s, s.begin():
            replay = await schedule_due_daily_pipeline(s, now=now, profile_id=_PROFILE)
            assert replay.id == first.id and replay.payload == frozen
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_market_selector_keeps_session_separate_from_publication_cutoff(
    migrated_database_url,
):
    from nexus.market.service import MarketService
    from nexus.market.universe_selection import select_market_universe

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    cutoff = datetime(2026, 8, 31, 21, 0, tzinfo=UTC)
    try:
        async with factory() as s, s.begin():
            s.add_all(
                [
                    UniverseSnapshot(
                        id=UUID(int=822001),
                        key="us_liquid",
                        version="us_liquid_v1",
                        effective_from=_THURSDAY,
                        source="fixture",
                        content_hash="a" * 64,
                        published_at=datetime(2026, 8, 1, tzinfo=UTC),
                    ),
                    UniverseSnapshot(
                        id=UUID(int=822002),
                        key="fosun_local_holdings",
                        version="monday",
                        effective_from=_MONDAY,
                        source="fosun_local_read_only",
                        content_hash="b" * 64,
                        published_at=_FRIDAY_CLOSE,
                    ),
                ]
            )
        async with factory() as s:
            daily_scope = await select_market_universe(
                s, effective_session=_FRIDAY, published_by=cutoff
            )
            assert daily_scope.id == UUID(int=822001)
            market = MarketService(s, cutoff, cursor_signing_secrets=["synthetic"])
            assert await market._active_market_universe_id(cutoff) == UUID(int=822002)
    finally:
        await factory.kw["bind"].dispose()
