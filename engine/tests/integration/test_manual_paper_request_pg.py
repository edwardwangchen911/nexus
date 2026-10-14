"""Actual isolated PG request identities and replay, without provider/model calls."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle, manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import admit_manual_request, read_manual_request
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_formal_stage_registration import OPTIONS

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)


@pytest.mark.parametrize("candidate_count", [3, 5])
@pytest.mark.asyncio
async def test_manual_request_freezes_identity_replays_and_keeps_history(
    migrated_database_url, monkeypatch, candidate_count
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    candidates = (
        INSTRUMENTS
        if candidate_count == 3
        else INSTRUMENTS
        + (
            (UUID(int=101), Decimal("70"), Decimal("0"), "industrials"),
            (UUID(int=102), Decimal("80"), Decimal("0"), "consumer_staples"),
        )
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=candidates, decision_time=AT
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    try:
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="manual-batch",
                created_at=AT,
            )
            assert len(batch.instrument_ids) == candidate_count

        async def concurrent_replay():
            async with sessions.begin() as session:
                return await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_only",
                    idempotency_key="one-click",
                )

        first, second = await asyncio.gather(concurrent_replay(), concurrent_replay())
        request = first
        assert second == request
        assert request.status == "queued" and request.cycle_id is None
        assert request.requested_at == AT and request.execution_locked is True
        async with sessions.begin() as session:
            replay = await admit_manual_request(
                session,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_only",
                idempotency_key="one-click",
            )
            assert replay == request
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(ManualPaperRequestEvent)) == 1
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_and_paper",
                    idempotency_key="one-click",
                )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_only",
                    idempotency_key="another-click",
                )
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request.request_id)
            assert await read_manual_request(session, row) == request
        async with sessions.begin() as session:
            with pytest.raises(Exception, match="MANUAL_PAPER_HISTORY_IMMUTABLE"):
                async with session.begin_nested():
                    await session.execute(
                        text("UPDATE manual_paper_requests SET mode='research_and_paper'")
                    )
        async with sessions.begin() as session:
            with pytest.raises(Exception, match="MANUAL_PAPER_HISTORY_IMMUTABLE"):
                async with session.begin_nested():
                    await session.execute(text("DELETE FROM manual_paper_request_events"))
        async with sessions.begin() as session:
            claimed, should_prepare = await manual_request.claim_manual_request(
                session, request_id=request.request_id
            )
            assert should_prepare and claimed.status == "preparing"
        async with sessions.begin() as session:
            again, should_prepare = await manual_request.claim_manual_request(
                session, request_id=request.request_id
            )
            assert again == claimed and not should_prepare
            row = await session.get(ManualPaperRequest, request.request_id)
            with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
                await manual_request.record_manual_request_event(
                    session, row, status="unknown", code="MANUAL_PAPER_COMPLETED"
                )
            unknown = await manual_request.record_manual_request_event(
                session, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
            )
            assert unknown.status == "unknown"
        async with sessions.begin() as session:
            unknown_read, should_prepare = await manual_request.claim_manual_request(
                session, request_id=request.request_id
            )
            assert unknown_read == unknown and not should_prepare
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_only",
                    idempotency_key="after-unknown",
                )
        async with sessions.begin() as session:
            head_before_downgrade = await session.scalar(
                text("SELECT version_num FROM alembic_version")
            )
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="MANUAL_PAPER_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0074_economic_plan_windows")
        async with sessions.begin() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == head_before_downgrade
            )
            row = await session.get(ManualPaperRequest, request.request_id)
            assert await read_manual_request(session, row) == unknown
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_request_rejects_actual_old_order_reservations(
    migrated_database_url, monkeypatch
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, occupied=True, decision_time=AT
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    from nexus.paper.target_service import TargetActivationBlockedError

    try:
        async with sessions.begin() as session:
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="occupied-manual",
                created_at=AT,
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            with pytest.raises(TargetActivationBlockedError):
                await admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_and_paper",
                    idempotency_key="occupied-start",
                )
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_cycle_does_not_hide_queued_native_jobs(migrated_database_url, monkeypatch):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT
    )
    try:
        async with sessions.begin() as session:
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="terminal-with-native-jobs",
                created_at=AT,
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            result = await agent_cycle._start_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                daily_limit_usd=Decimal("3"),
                candidate_batch_id=batch.batch_id,
                requested_instrument_ids=batch.instrument_ids,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            assert result.status == "researching" and len(result.research_run_ids) == 2
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            # A terminal controller fact is deliberately present alongside real queued Jobs.
            # Admission must inspect the native authority rather than trust only that fact.
            await agent_cycle._append_event(
                session, cycle, "researching", "unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE", {}
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(Job)
                    .where(
                        Job.payload["run_id"]
                        .as_string()
                        .in_([str(value) for value in result.research_run_ids]),
                        Job.status == "queued",
                    )
                )
                == 2
            )
            request_row = ManualPaperRequest(
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                key_hash="a" * 64,
                request_hash="b" * 64,
                batch_hash=batch.batch_hash,
                model_hash=model.config_hash,
                requested_at=AT,
            )
            with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
                await manual_request._require_manual_cycle_binding(session, request_row, cycle.id)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.require_manual_account_available(
                    session, account_id=authority.account_id, now=AT
                )
            # Known usage alone cannot conceal still-queued native Jobs.
            runs = (
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.in_(result.research_run_ids)
                    )
                )
            ).all()
            for run in runs:
                run.status = "failed"
                run.settled_cost_usd = Decimal("0")
            await session.flush()
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.require_manual_account_available(
                    session, account_id=authority.account_id, now=AT
                )
            jobs = (
                await session.scalars(
                    select(Job).where(
                        Job.payload["run_id"]
                        .as_string()
                        .in_([str(value) for value in result.research_run_ids])
                    )
                )
            ).all()
            for job in jobs:
                job.status = "running"
                job.started_at = AT
                job.attempt_count = 1
                job.version += 1
            await session.flush()
            for job in jobs:
                job.status = "failed"
                job.finished_at = AT
                job.version += 1
            await session.flush()
            await manual_request.require_manual_account_available(
                session, account_id=authority.account_id, now=AT
            )
            lease = JobLease(
                job_id=jobs[0].id,
                worker_id="isolated-manual-test",
                expires_at=AT + timedelta(minutes=5),
                lease_token=uuid4(),
                fence_token=1,
                acquired_at=AT,
                heartbeat_at=AT,
            )
            session.add(lease)
            await session.flush()
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.require_manual_account_available(
                    session, account_id=authority.account_id, now=AT
                )
            # Retained lease history is not an active lease after expiry. No deletion.
            await manual_request.require_manual_account_available(
                session, account_id=authority.account_id, now=AT + timedelta(minutes=6)
            )
    finally:
        await engine.dispose()


def test_empty_manual_migration_roundtrip(temporary_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0075_manual_paper_requests")
    command.downgrade(config, "0074_economic_plan_windows")
    from sqlalchemy.ext.asyncio import create_async_engine

    async def verify():
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(text("SELECT to_regclass('manual_paper_requests')"))
                    is None
                )
                assert (
                    await connection.scalar(
                        text("SELECT to_regclass('manual_paper_request_events')")
                    )
                    is None
                )
                assert (
                    await connection.scalar(text("SELECT to_regclass('agent_paper_cycles')"))
                    is not None
                )
                assert (
                    await connection.scalar(text("SELECT version_num FROM alembic_version"))
                    == "0074_economic_plan_windows"
                )
        finally:
            await engine.dispose()

    asyncio.run(verify())
    command.upgrade(config, "0075_manual_paper_requests")
