"""Manual daily composition uses frozen data and the real diagnostic service."""

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.portfolio_api import CLOCK, seed_portfolio_list, settings
from nexus.jobs.daily_manual import (
    ManualDailyRequest,
    build_manual_daily_registry,
    enqueue_manual_daily_root,
    manual_daily_job_ids,
)
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import enqueue_job
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

_CALENDAR = UUID("0198d500-1000-7000-8000-000000009101")
_SNAPSHOT = UUID("0198d500-1000-7000-8000-000000000041")
_SESSION = date(2026, 8, 21)
_CLOSE = datetime(2026, 8, 21, 20, tzinfo=UTC)


async def _seed(url, *, cash_quality="PASS", published_at=None, exchange="XNYS", close=_CLOSE):
    await seed_portfolio_list(url, ready_cash_flow_quality=cash_quality)
    factory = session_factory(settings(url))
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=_CALENDAR,
                exchange=exchange,
                version="manual-daily-test",
                source="fixture",
                range_start=_SESSION,
                range_end=_SESSION,
                query_parameters={},
                completeness_status="complete",
                content_hash="d" * 64,
                published_at=published_at or _CLOSE - timedelta(days=1),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=_CALENDAR,
                session=_SESSION,
                market_open_at=close - timedelta(hours=6, minutes=30),
                market_close_at=close,
                decision_at=close + timedelta(hours=1),
            )
        )
    return factory


def _request(cutoff):
    return ManualDailyRequest(
        snapshot_id=_SNAPSHOT,
        calendar_authority_id=_CALENDAR,
        session=_SESSION,
        cutoff_at=cutoff,
    )


async def _tick(factory, root_id):
    now = datetime.now(UTC)
    async with factory() as session:
        job_ids = await manual_daily_job_ids(session, root_id=root_id)
    return await run_job_worker_once(
        factory,
        registry=build_manual_daily_registry(artifact_reader=FixtureArtifactReader({})),
        queue=QueueName.GENERAL,
        worker_id="manual-test",
        worker_started_at=_CLOSE,
        process_version="test",
        migration_head="0036",
        clock=lambda: now,
        job_ids=job_ids,
        claim_registered_only=True,
    )


@pytest.mark.asyncio
async def test_manual_root_runs_real_diagnosis_once_at_original_snapshot_cutoff(
    migrated_database_url,
):
    factory = await _seed(migrated_database_url)
    request = _request(datetime.now(UTC))
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(session, request=request, now=datetime.now(UTC))
        duplicate = await enqueue_manual_daily_root(session, request=request, now=datetime.now(UTC))
        assert root.id == duplicate.id
        assert root.payload["cutoff_at"] != root.payload["snapshot_as_of"]
    for _ in range(3):
        assert await _tick(factory, root.id)
    assert not await _tick(factory, root.id)
    async with factory() as session:
        jobs = tuple(await session.scalars(select(Job)))
        assert {job.job_type for job in jobs} == {
            JobType.DAILY_MANUAL_PIPELINE.value,
            JobType.QUALITY_FREEZE.value,
            JobType.DIAGNOSTICS_RUN.value,
        }
        assert all(job.status == "succeeded" for job in jobs), [
            (job.job_type, job.status, job.result_code, job.last_error_code) for job in jobs
        ]
        report = await session.scalar(select(PortfolioDiagnosticReportRecord))
        assert report is not None
        assert report.portfolio_snapshot_id == _SNAPSHOT
        assert report.snapshot_as_of == CLOCK - timedelta(minutes=1)
        assert report.optimization_need == "RESEARCH_REQUIRED"
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
            == 1
        )


@pytest.mark.asyncio
async def test_unreconciled_snapshot_skips_diagnostic_without_inventing_pass(
    migrated_database_url,
):
    factory = await _seed(migrated_database_url, cash_quality="NOT_RUN")
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )
    for _ in range(3):
        assert await _tick(factory, root.id)
    async with factory() as session:
        jobs = {job.job_type: job for job in await session.scalars(select(Job))}
        assert jobs[JobType.QUALITY_FREEZE.value].status == "skipped"
        assert jobs[JobType.DIAGNOSTICS_RUN.value].status == "skipped"
        assert jobs[JobType.DIAGNOSTICS_RUN.value].result_code == "DEPENDENCY_NOT_SATISFIED"
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    [
        "future_cutoff",
        "late_snapshot",
        "late_calendar",
        "wrong_exchange",
        "unclosed_session",
    ],
)
async def test_manual_root_rejects_fact_or_request_after_cutoff(migrated_database_url, reason):
    now = datetime.now(UTC)
    factory = await _seed(
        migrated_database_url,
        published_at=now + timedelta(days=1) if reason == "late_calendar" else None,
        exchange="XHKG" if reason == "wrong_exchange" else "XNYS",
        close=CLOCK if reason == "unclosed_session" else _CLOSE,
    )
    cutoff = (
        now + timedelta(days=1)
        if reason == "future_cutoff"
        else CLOCK - timedelta(minutes=2)
        if reason == "late_snapshot"
        else datetime.now(UTC)
    )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MANUAL_DAILY_"):
            await enqueue_manual_daily_root(
                session,
                request=_request(cutoff),
                now=datetime.now(UTC),
            )
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_two_manual_roots_are_independent_and_reuse_frozen_diagnostic(migrated_database_url):
    factory = await _seed(migrated_database_url)
    async with factory() as session, session.begin():
        first = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )
        second = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )
        assert first.id != second.id
    for _ in range(3):
        assert await _tick(factory, second.id)
    async with factory() as session:
        untouched = await session.get(Job, first.id)
        assert (
            untouched is not None and untouched.status == "queued" and untouched.attempt_count == 0
        )
        assert await manual_daily_job_ids(session, root_id=first.id) == (first.id,)
    for _ in range(3):
        assert await _tick(factory, first.id)
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
            == 1
        )


@pytest.mark.asyncio
async def test_foreign_stage_cannot_expand_manual_scope(migrated_database_url):
    factory = await _seed(migrated_database_url)
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )
    assert await _tick(factory, root.id)
    async with factory() as session, session.begin():
        payload = {"root_job_id": str(root.id)}
        extra = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.DAILY_BRIEF,
                scope_kind="global",
                scope_id="foreign-stage",
                effective_at=_CLOSE,
                input_hash=job_input_hash(payload),
            ),
            schema_version="foreign_test_v1",
            payload=payload,
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=3,
        )
    async with factory() as session:
        with pytest.raises(ValueError, match="MANUAL_DAILY_CHILD_INVALID"):
            await manual_daily_job_ids(session, root_id=root.id)
        untouched = await session.get(Job, extra.id)
        assert (
            untouched is not None and untouched.status == "queued" and untouched.attempt_count == 0
        )


@pytest.mark.asyncio
async def test_blocked_diagnostic_cannot_satisfy_success_dependency(
    migrated_database_url, monkeypatch
):
    factory = await _seed(migrated_database_url)
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )

    async def blocked_report(*args, **kwargs):
        return SimpleNamespace(optimization_need="BLOCKED")

    # Only this outcome adapter test stubs the service; the normal path above
    # exercises actual assembly and persistence with an injected artifact reader.
    monkeypatch.setattr("nexus.jobs.daily_manual.diagnose_and_persist", blocked_report)
    for _ in range(3):
        assert await _tick(factory, root.id)
    async with factory() as session:
        job = await session.scalar(select(Job).where(Job.job_type == JobType.DIAGNOSTICS_RUN.value))
        assert (
            job is not None and job.status == "skipped" and job.result_code == "DIAGNOSTIC_BLOCKED"
        )
