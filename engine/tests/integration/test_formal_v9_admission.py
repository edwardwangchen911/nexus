"""Formal preparation of the independent v9 identity, without runtime activation."""

from types import SimpleNamespace

import pytest
from integration.test_full_input_capacity_admission import admit, prepare
from nexus.agents.codex_capacity import capacity_config_hash
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_explicit_formal_v9_admission_preserves_new_identity_and_replay(
    migrated_database_url,
):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        first = await admit(session, model_id, formal_stage_capacity=True)
        second = await admit(session, model_id, formal_stage_capacity=True)
        assert second.replayed and second.run_id == first.run_id
        run = await session.get(AgentResearchRunRecord, first.run_id)
        assert run.pricing_version == "codex-sdk-chatgpt-v9-full-input-capacity"
        assert run.model_hash == capacity_config_hash()
        job = await session.scalar(select(Job))
        assert job.max_attempts == 1 and job.attempt_count == 0 and job.status == "queued"
        with pytest.raises(ValueError, match="CODEX_CAPACITY_ISOLATED_ADMISSION_REQUIRED"):
            await admit(session, model_id)


@pytest.mark.asyncio
async def test_formal_v9_job_requires_whole_cycle_before_worker_claim(migrated_database_url):
    from integration.test_candidate_research_authority import NOW
    from nexus.agents.codex_formal_stage import (
        formal_stage_job_qualified,
        require_formal_stage_model_profile,
    )

    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        await admit(session, model_id, formal_stage_capacity=True)
        job = await session.scalar(select(Job))
        assert await formal_stage_job_qualified(session, job)
        assert (
            await require_formal_stage_model_profile(session, model_config_id=model_id, now=NOW)
        ).id == model_id
        job_id = job.id
    from nexus.persistence.models.runtime import JobAttempt
    from nexus.settings import EngineSettings
    from nexus.worker import run_agent_worker_once

    with pytest.raises(ValueError, match="CODEX_CAPACITY_POOL_BINDING_INVALID"):
        await run_agent_worker_once(
            EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
            job_ids=(job_id,),
            formal_stage_capacity=True,
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
        assert (await session.get(Job, job_id)).status == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
async def test_formal_v9_rechecks_exact_model_after_authority_wait(
    migrated_database_url, monkeypatch, replay
):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        if replay:
            await admit(session, model_id, formal_stage_capacity=True)
        get = session.get
        drift = False

        async def after_authority():
            nonlocal drift
            drift = True

        async def observed(entity, key, **kwargs):
            result = await get(entity, key, **kwargs)
            if drift and entity.__name__ == "AgentModelConfigRecord":
                fields = {
                    column.key: getattr(result, column.key) for column in result.__table__.columns
                }
                fields["worst_case_cost_usd"] = result.worst_case_cost_usd
                return SimpleNamespace(**(fields | {"max_input_tokens": 96001}))
            return result

        monkeypatch.setattr(session, "get", observed)
        with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
            await admit(
                session, model_id, formal_stage_capacity=True, after_authority=after_authority
            )
        assert await session.scalar(select(func.count()).select_from(Job)) == int(replay)
        assert await session.scalar(
            select(func.count()).select_from(AgentResearchRunRecord)
        ) == int(replay)
