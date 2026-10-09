"""Pending formal execution plumbing is qualified using synthetic initial records only."""

import pytest
from integration.test_candidate_research_authority import NOW, admit, prepare
from nexus.agents.codex_formal_stage import _formal_stage_job_hash, formal_stage_job_qualified
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.domain.ids import new_uuid7
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import claim_next_job
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.ops.test_formal_stage_registration import OPTIONS


async def frozen_formal_test_job(settings, *, wrong_hash=False):
    # v8 admission remains closed. Seed new synthetic immutable rows, rather than
    # relabeling an old run or loosening any production admission guard.
    factory, candidate_id = await prepare(settings)
    await admit(factory, candidate_id)
    await prepare_database(settings, **OPTIONS)
    async with factory.begin() as session:
        source_run = await session.scalar(select(AgentResearchRunRecord))
        source_job = await session.scalar(select(Job))
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            )
        )
        values = {
            c.name: getattr(source_run, c.name)
            for c in source_run.__table__.columns
            if c.name not in {"id", "created_at"}
        }
        values.update(
            idempotency_key="formal-synthetic-worker",
            model_config_id=model.id,
            model_hash=model.config_hash,
            pricing_version=model.version,
            input_hash="b" * 64,
        )
        run = AgentResearchRunRecord(id=new_uuid7(), **values)
        session.add(run)
        await session.flush()
        job_values = {
            c.name: getattr(source_job, c.name)
            for c in source_job.__table__.columns
            if c.name not in {"id", "created_at"}
        }
        job_values.update(
            job_key=source_job.job_key + ":formal-test",
            payload={"run_id": str(run.id)},
            input_hash="c" * 64 if wrong_hash else _formal_stage_job_hash(run),
        )
        job = Job(id=new_uuid7(), **job_values)
        session.add(job)
        await session.flush()
        return factory, job.id


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_hash", [False, True])
async def test_formal_exact_claim_checks_frozen_hash_before_attempt(
    migrated_database_url, wrong_hash
):
    factory, job_id = await frozen_formal_test_job(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        wrong_hash=wrong_hash,
    )
    async with factory.begin() as session:
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="formal-test",
            now=NOW,
            lease_seconds=210,
            job_types=(JobType.AGENT_PAPER_RESEARCH.value,),
            job_ids=(job_id,),
            research_budget_profile=FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            locked_job_guard=formal_stage_job_qualified,
        )
        assert (claim is None) == wrong_hash
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == (
            0 if wrong_hash else 1
        )
        assert await session.scalar(select(func.count()).select_from(JobLease)) == (
            0 if wrong_hash else 1
        )
        job = await session.get(Job, job_id, populate_existing=True)
        assert job.attempt_count == (0 if wrong_hash else 1)
        if not wrong_hash:
            assert await formal_stage_job_qualified(session, job, claimed=True)
            lease = await session.scalar(select(JobLease))
            assert (lease.expires_at - NOW).total_seconds() == 210


@pytest.mark.asyncio
async def test_generic_claim_does_not_consume_pending_formal_even_with_exact_id(
    migrated_database_url,
):
    factory, job_id = await frozen_formal_test_job(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    )
    async with factory.begin() as session:
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="generic-test",
            now=NOW,
            lease_seconds=150,
            job_ids=(job_id,),
        )
        assert claim is None
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


@pytest.mark.asyncio
async def test_normal_formal_worker_refuses_missing_evidence_and_never_replays(
    migrated_database_url,
    monkeypatch,
):
    from unittest.mock import AsyncMock

    from nexus.agents import codex_staged
    from nexus.worker import run_agent_worker_once

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, job_id = await frozen_formal_test_job(settings)
    sdk = AsyncMock(side_effect=AssertionError("synthetic run has no frozen input authority"))
    monkeypatch.setattr(codex_staged, "invoke_codex", sdk)
    assert await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
    async with factory() as session:
        job = await session.get(Job, job_id)
        assert job.status == "dead_letter" and job.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 1
    assert not await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
    sdk.assert_not_awaited()
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 1


@pytest.mark.asyncio
async def test_formal_worker_actual_database_mismatch_refuses_before_attempt(
    migrated_database_url,
    monkeypatch,
):
    from nexus import worker
    from sqlalchemy.engine import make_url

    factory, job_id = await frozen_formal_test_job(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    )
    claimed = make_url(migrated_database_url).set(database="nexus_diag_wrong_formal_worker")
    monkeypatch.setattr(worker, "session_factory", lambda _: factory)
    with pytest.raises(ValueError, match="CODEX_FORMAL_WORKER_DATABASE_INVALID"):
        await worker.run_agent_worker_once(
            EngineSettings(
                database_url=claimed.render_as_string(hide_password=False),
                agent_runtime_mode="codex_sdk",
            ),
            job_ids=(job_id,),
            formal_stage_capacity=True,
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


@pytest.mark.asyncio
async def test_generic_reclaimer_never_recovers_expired_formal_attempt(migrated_database_url):
    from datetime import timedelta

    from nexus.jobs.repository import reclaim_expired_jobs

    factory, job_id = await frozen_formal_test_job(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    )
    async with factory.begin() as session:
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="formal-test",
            now=NOW,
            lease_seconds=210,
            job_ids=(job_id,),
            research_budget_profile=FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            locked_job_guard=formal_stage_job_qualified,
        )
        assert claim is not None
    async with factory.begin() as session:
        assert (
            await reclaim_expired_jobs(
                session,
                queue=QueueName.GENERAL,
                now=NOW + timedelta(seconds=211),
                job_ids=(job_id,),
            )
            == 0
        )
        job = await session.get(Job, job_id)
        assert job.status == "running" and job.attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 1
