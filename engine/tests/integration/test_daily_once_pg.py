"""Public manual command drives actual jobs without touching other workflows."""

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.portfolio_api import seed_portfolio_list
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, fail_job
from nexus.ops.daily_once import DailyAdmitRequest, DailyAdvanceRequest, DailyOnceSettings, run_once
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

_CALENDAR = UUID("0198d500-1000-7000-8000-000000009201")
_SNAPSHOT = UUID("0198d500-1000-7000-8000-000000000041")
_DAY = date(2026, 8, 21)
_CLOSE = datetime(2026, 8, 21, 20, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_for_availability", [False, True])
async def test_cli_admit_advance_preserves_other_queued_and_expired_jobs(
    migrated_database_url,
    monkeypatch,
    wait_for_availability,
):
    await seed_portfolio_list(migrated_database_url)
    config = DailyOnceSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:19000",
        object_store_region="test-region",
        object_store_bucket="synthetic-manual-test",
        object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )
    factory = session_factory(config)
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=_CALENDAR,
                exchange="XNYS",
                version="daily-cli-fixture",
                source="fixture",
                range_start=_DAY,
                range_end=_DAY,
                query_parameters={},
                completeness_status="complete",
                content_hash="e" * 64,
                published_at=_CLOSE - timedelta(days=1),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=_CALENDAR,
                session=_DAY,
                market_open_at=_CLOSE - timedelta(hours=6, minutes=30),
                market_close_at=_CLOSE,
                decision_at=_CLOSE,
            )
        )
        others = []
        for label in ("expired", "queued"):
            payload = {"value": label}
            other = await enqueue_job(
                session,
                key=JobKeyV1(
                    job_type=JobType.QUALITY_FREEZE,
                    scope_kind="global",
                    scope_id=label,
                    effective_at=_CLOSE,
                    input_hash=job_input_hash(payload),
                ),
                schema_version="unrelated_test_v1",
                payload=payload,
                queue=QueueName.GENERAL,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
            )
            others.append(other.id)
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="other-owner",
            now=_CLOSE,
            job_ids=(others[0],),
        )
        assert claim is not None
    monkeypatch.setattr(
        "nexus.ops.daily_once.S3ProviderEvidenceStore",
        lambda _: SimpleNamespace(read_normalized=FixtureArtifactReader({})),
    )
    admission = await run_once(
        config,
        DailyAdmitRequest(
            action="admit",
            snapshot_id=_SNAPSHOT,
            calendar_authority_id=_CALENDAR,
            session=_DAY,
            cutoff_at=datetime.now(UTC),
        ),
    )
    root_id = UUID(str(admission["root_id"]))
    if wait_for_availability:
        async with factory() as session, session.begin():
            root_claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="manual-previous",
                now=datetime.now(UTC),
                job_ids=(root_id,),
            )
            assert root_claim is not None
            await fail_job(
                session,
                claim=root_claim,
                now=datetime.now(UTC),
                error_code="SYNTHETIC_WAIT",
                retry_delay_seconds=60,
            )
    result = await run_once(config, DailyAdvanceRequest(action="advance", root_id=root_id))
    assert result["status"] == ("pending" if wait_for_availability else "completed")
    assert result["processed"] == (0 if wait_for_availability else 3)
    async with factory() as session:
        expired = await session.get(Job, others[0])
        queued = await session.get(Job, others[1])
        assert expired is not None and (expired.status, expired.attempt_count) == ("running", 1)
        assert queued is not None and (queued.status, queued.attempt_count) == ("queued", 0)
        assert await session.scalar(
            select(func.count()).select_from(PortfolioDiagnosticReportRecord)
        ) == (0 if wait_for_availability else 1)
    # Re-observation neither reruns completed work nor waits for a future job.
    assert (await run_once(config, DailyAdvanceRequest(action="advance", root_id=root_id)))[
        "processed"
    ] == 0
