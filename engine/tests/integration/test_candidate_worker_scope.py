"""Generic workers cannot consume or recover isolated candidate Jobs."""

from datetime import timedelta

import pytest
from integration.test_candidate_research_authority import NOW, admit, prepare
from integration.test_job_scope import _state
from nexus.agents.codex_candidate import candidate_job_qualified
from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job, reclaim_expired_jobs
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select


@pytest.mark.asyncio
async def test_output_qualification_refuses_nonisolated_actual_database(temporary_database_factory):
    from uuid import uuid4

    from nexus.agents.codex_candidate import candidate_output_qualified
    from nexus.persistence.session import session_factory

    # Owned test-container admin target; rejection must precede any business table read.
    factory = session_factory(
        EngineSettings(
            database_url=temporary_database_factory.admin_url.render_as_string(hide_password=False)
        )
    )
    try:
        async with factory() as session:
            with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
                await candidate_output_qualified(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_prepared_candidate_is_not_completed_output_authority(migrated_database_url):
    from nexus.agents.codex_candidate import candidate_output_qualified

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    admission = await admit(factory, model_id)
    try:
        async with factory() as session:
            assert not await candidate_output_qualified(session, admission.run_id)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_generic_scoped_worker_never_consumes_candidate(migrated_database_url):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    await admit(factory, model_id)
    async with factory() as session:
        job_id = await session.scalar(select(Job.id))
    before = await _state(factory, job_id)
    assert await run_agent_worker_once(settings, job_ids=(job_id,)) is False
    assert await _state(factory, job_id) == before


@pytest.mark.asyncio
async def test_generic_reclaimer_does_not_change_candidate_expired_attempt(migrated_database_url):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    await admit(factory, model_id)
    async with factory() as session, session.begin():
        job_id = await session.scalar(select(Job.id))
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="candidate-test",
            now=NOW,
            lease_seconds=210,
            job_ids=(job_id,),
            locked_job_guard=candidate_job_qualified,
            research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
        )
        assert claim is not None
    before = await _state(factory, job_id)
    async with factory() as session, session.begin():
        assert (
            await reclaim_expired_jobs(
                session, queue=QueueName.GENERAL, now=NOW + timedelta(seconds=211)
            )
            == 0
        )
    assert await _state(factory, job_id) == before
