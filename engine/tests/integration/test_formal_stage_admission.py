"""Explicit native Paper admission binds the independent formal profile atomically."""

from decimal import Decimal
from uuid import UUID

import pytest
from integration.test_agent_execution_pg import _INSTRUMENT_ID, _NOW, _SNAPSHOT_ID, _seed_authority
from integration.test_candidate_research_authority import NOW
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.jobs.daily_research import admit_agent_paper_research
from nexus.jobs.models import JobType
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.ops.test_formal_stage_registration import OPTIONS


async def prepare_formal(url):
    settings = EngineSettings(database_url=url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    await prepare_database(settings, **OPTIONS)
    async with factory() as session:
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            )
        )
        return factory, model.id


async def explicit_admit(session, model_id, **change):
    options = dict(
        actor_id="formal-paper-test",
        route="/internal/formal-paper-test",
        idempotency_key="formal-first",
        request_hash="a" * 64,
        request=ResearchRequest(
            instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
        ),
        prompt_version_id=None,
        model_config_id=model_id,
        daily_limit_usd=Decimal("3"),
        now=NOW,
        job_type=JobType.AGENT_PAPER_RESEARCH,
        formal_stage_capacity=True,
    )
    return await admit_research_run(session, **(options | change))


@pytest.mark.asyncio
async def test_explicit_formal_admission_is_frozen_native_and_idempotent(migrated_database_url):
    factory, model_id = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        first = await explicit_admit(session, model_id)
    async with factory.begin() as session:
        second = await explicit_admit(session, model_id)
        assert first.run_id == second.run_id and second.replayed
        job = await session.scalar(select(Job))
        assert job.job_type == JobType.AGENT_PAPER_RESEARCH.value and job.max_attempts == 1
        assert await formal_stage_job_qualified(session, job)
        run = await session.get(AgentResearchRunRecord, first.run_id)
        assert run.model_config_id == model_id
        assert run.pricing_version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_stage_capacity": 1},
        {"job_type": JobType.AGENT_RESEARCH},
        {"model_config_id": None},
        {"reuse_latest_actor_agent_version": True},
    ],
)
async def test_formal_refusal_precedes_claim_even_when_caught(migrated_database_url, change):
    factory, model_id = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
        with pytest.raises(ValueError, match="CODEX_FORMAL_ADMISSION_ARGUMENT_INVALID"):
            await explicit_admit(session, model_id, **change)
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_normal_paper_entry_explicitly_binds_formal_profile(migrated_database_url):
    factory, model_id = await prepare_formal(migrated_database_url)
    options = dict(
        cycle_id=UUID(int=17),
        instrument_id=_INSTRUMENT_ID,
        research_snapshot_id=_SNAPSHOT_ID,
        research_as_of=_NOW,
        daily_limit_usd=Decimal("3"),
        now=NOW,
        model_config_id=model_id,
        formal_stage_capacity=True,
    )
    async with factory.begin() as session:
        first = await admit_agent_paper_research(session, **options)
    async with factory.begin() as session:
        second = await admit_agent_paper_research(session, **options)
        assert first.run_id == second.run_id and second.replayed
        job = await session.scalar(select(Job))
        assert await formal_stage_job_qualified(session, job)


@pytest.mark.asyncio
async def test_formal_downstream_refusal_rolls_back_claim_when_caught(migrated_database_url):
    factory, model_id = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
        with pytest.raises(ValueError, match="point-in-time data authority is invalid"):
            await explicit_admit(
                session,
                model_id,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID, data_snapshot_id=UUID(int=999), decision_time=_NOW
                ),
            )
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_noncanonical_model_refuses_before_claim(migrated_database_url):
    factory, _ = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        model_id = await session.scalar(
            select(AgentModelConfigRecord.id).where(
                AgentModelConfigRecord.version != FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            )
        )
        before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
        with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
            await explicit_admit(session, model_id)
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
        assert await session.scalar(select(func.count()).select_from(Job)) == 0


@pytest.mark.asyncio
async def test_default_mode_cannot_replay_formal_claim(migrated_database_url):
    factory, model_id = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        await explicit_admit(session, model_id)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="CODEX_FORMAL_STAGE_ACTIVATION_REQUIRED"):
            await explicit_admit(session, None, formal_stage_capacity=False)
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
async def test_formal_mode_cannot_replay_old_model_claim(migrated_database_url):
    from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION

    factory, formal_id = await prepare_formal(migrated_database_url)
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(OPTIONS | {"formal_stage_capacity": False}),
    )
    async with factory.begin() as session:
        old_id = await session.scalar(
            select(AgentModelConfigRecord.id).where(
                AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION
            )
        )
        await explicit_admit(session, old_id, formal_stage_capacity=False)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
            await explicit_admit(session, formal_id)
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


@pytest.mark.asyncio
async def test_formal_durable_job_reuse_keeps_exact_frozen_model(migrated_database_url):
    factory, model_id = await prepare_formal(migrated_database_url)
    async with factory.begin() as session:
        first = await explicit_admit(session, model_id)
    async with factory.begin() as session:
        second = await explicit_admit(session, model_id, idempotency_key="formal-second")
        assert first.run_id == second.run_id and second.replayed
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
