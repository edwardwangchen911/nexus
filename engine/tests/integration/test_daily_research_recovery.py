"""Normal manual stages survive an uncertain model result without external replay."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.research_snapshots import INSTRUMENT, NOW
from nexus.agents import codex_staged
from nexus.jobs.daily_research import resolve_daily_research_job
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job
from nexus.ops.daily_once import (
    DailyAdmitRequest,
    DailyAdvanceRequest,
    DailyOnceError,
    DailyOnceSettings,
    DailyProposalBuildRequest,
    DailyResearchAdmitRequest,
    DailyResearchRunRequest,
    run_once,
)
from nexus.ops.openai_local import prepare_database
from nexus.optimizer.research_context import freeze_daily_research_context
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select

from engine.tests.integration.test_research_explicit_portfolio import _seed, _snapshot


def _settings(url):
    return DailyOnceSettings(
        database_url=url, agent_runtime_mode="codex_sdk",
        object_store_endpoint="http://127.0.0.1:19000", object_store_region="test-region",
        object_store_bucket="synthetic-recovery", object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )


@pytest.mark.asyncio
async def test_normal_daily_research_failure_survives_fresh_sessions_without_replay(
    migrated_database_url, monkeypatch,
):
    async def no_network(*args, **kwargs):
        pytest.fail("The cross-stage recovery case must stay offline")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", no_network)
    monkeypatch.setattr(
        "nexus.ops.daily_once.S3ProviderEvidenceStore",
        lambda _: SimpleNamespace(read_normalized=FixtureArtifactReader({})),
    )
    invoke = AsyncMock(side_effect=RuntimeError("synthetic-private-transport-result-unknown"))
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    configured = _settings(migrated_database_url)
    await prepare_database(configured, structured_synthesis=True)
    factory = session_factory(configured)
    close = NOW - timedelta(hours=8)
    async with factory() as session, session.begin():
        portfolio, snapshot = await _seed(session)
        calendar = ExchangeCalendarAuthorityRecord(
            exchange="XNYS", version="recovery-test", source="fixture",
            range_start=close.date(), range_end=close.date(), query_parameters={},
            completeness_status="complete", content_hash="f" * 64, published_at=close,
        )
        session.add(calendar)
        await session.flush()
        session.add(ExchangeCalendarSessionRecord(
            calendar_authority_id=calendar.id, session=close.date(),
            market_open_at=close - timedelta(hours=6, minutes=30), market_close_at=close,
            decision_at=close + timedelta(hours=1),
        ))
        model_id = await session.scalar(select(AgentModelConfigRecord.id))
        others = []
        for label in ("expired", "queued"):
            payload = {"value": label}
            other = await enqueue_job(
                session, key=JobKeyV1(
                    job_type=JobType.QUALITY_FREEZE, scope_kind="global", scope_id=label,
                    effective_at=NOW, input_hash=job_input_hash(payload),
                ), schema_version="unrelated_recovery_test_v1", payload=payload,
                queue=QueueName.GENERAL, retry_policy=RetryPolicy.AUTOMATIC_SAFE, max_attempts=3,
            )
            others.append(other.id)
        assert await claim_next_job(
            session, queue=QueueName.GENERAL, worker_id="other-owner", now=NOW,
            job_ids=(others[0],),
        ) is not None
    admit = DailyAdmitRequest(
        action="admit", snapshot_id=snapshot.id, calendar_authority_id=calendar.id,
        session=close.date(), cutoff_at=datetime.now(UTC),
    )
    root_id = UUID(str((await run_once(configured, admit))["root_id"]))
    advance = DailyAdvanceRequest(action="advance", root_id=root_id)
    daily = await run_once(configured, advance)
    assert daily["status"] == "completed" and daily["processed"] == 3
    # A newly stored portfolio must not replace the root's explicitly frozen portfolio.
    async with factory() as session, session.begin():
        await _snapshot(session, portfolio, offset=2)
    research_request = DailyResearchAdmitRequest(
        action="research-admit", root_id=root_id, instrument_id=INSTRUMENT,
        research_as_of=datetime.now(UTC), model_config_id=model_id,
    )
    research = await run_once(configured, research_request)
    run_id = UUID(str(research["run_id"]))
    run_request = DailyResearchRunRequest(action="research-run", root_id=root_id, run_id=run_id)
    failed = await run_once(configured, run_request)
    assert failed["processed"] == 1 and failed["run_status"] == "failed"
    assert failed["status"] == "blocked" and invoke.await_count == 1

    async def durable_state():
        async with session_factory(_settings(migrated_database_url))() as session:
            run = await session.get(AgentResearchRunRecord, run_id)
            job = await resolve_daily_research_job(session, root_id=root_id, run_id=run_id)
            assert run.status == "failed" and run.retry_count == 0
            assert job.attempt_count == job.max_attempts == 1
            data = await session.get(DataSnapshot, run.data_snapshot_id)
            assert data.payload["portfolio_context"]["portfolio_snapshot_id"] == str(snapshot.id)
            outputs = list(await session.scalars(select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run_id,
            )))
            assert len(outputs) == 1 and outputs[0].role is None and outputs[0].intent is None
            assert "synthetic-private-transport" not in json.dumps(outputs[0].payload)
            reservation = await session.scalar(select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id,
            ))
            assert reservation is not None
            expired, queued = [await session.get(Job, item) for item in others]
            assert (expired.status, expired.attempt_count) == ("running", 1)
            assert (queued.status, queued.attempt_count) == ("queued", 0)
            counts = [await session.scalar(select(func.count()).select_from(model)) for model in (
                Job, AgentResearchRunRecord, AgentResearchOutputRecord, DataSnapshot,
                AgentDailyBudgetReservationRecord, OptimizationProposalGroupRecord,
            )]
            return (
                counts, data.id, data.revision, json.dumps(data.payload, sort_keys=True),
                outputs[0].id, json.dumps(outputs[0].payload, sort_keys=True),
                reservation.id, reservation.state,
                reservation.reserved_usd, reservation.settled_usd,
            )

    original = await durable_state()
    restarted = _settings(migrated_database_url)
    assert (await run_once(restarted, admit))["root_id"] == str(root_id)
    assert (await run_once(restarted, advance))["processed"] == 0
    replay = await run_once(restarted, research_request)
    assert replay["replayed"] and replay["run_id"] == str(run_id)
    assert (await run_once(restarted, run_request))["processed"] == 0
    context_at = datetime.now(UTC)
    async with session_factory(restarted)() as session:
        context = await freeze_daily_research_context(
            session, root_id=root_id, run_id=run_id, context_as_of=context_at,
        )
        assert context.unavailable_reason == "RESEARCH_FAILED" and context.intent is None
        assert context.quality_status == "partial" and context.execution_locked
    with pytest.raises(DailyOnceError, match="^DAILY_ONCE_DRAFT_INPUT_UNAVAILABLE$"):
        await run_once(restarted, DailyProposalBuildRequest(
            action="proposal-build", root_id=root_id, run_id=run_id,
            context_as_of=context_at, idempotency_key="recovery-after-unknown",
        ))
    assert await durable_state() == original
    assert invoke.await_count == 1
