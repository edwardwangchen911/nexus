"""Candidate qualification binds actual database and frozen Job/Run/Model facts."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from integration.test_agent_execution_pg import (
    _INSTRUMENT_ID,
    _NOW,
    _SNAPSHOT_ID,
    _seed_authority,
)
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import claim_next_job
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

OPTIONS = dict(
    actionable_paper=True,
    structured_synthesis=True,
    aggregate_input_tokens=96000,
    max_wire_bytes=64000,
    inline_citations=True,
    stage_time_budget=True,
    candidate_stage_capacity=True,
)
NOW = datetime(2026, 9, 2, tzinfo=UTC)


async def prepare(settings):
    from nexus.agents.codex_candidate import candidate_config_hash
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION

    factory = session_factory(settings)
    await _seed_authority(factory)
    await prepare_database(settings, **OPTIONS)
    await prepare_database(settings, **OPTIONS)
    async with factory() as session:
        models = tuple(
            await session.scalars(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == CANDIDATE_BUDGET_CONFIG_VERSION
                )
            )
        )
        assert len(models) == 1 and models[0].config_hash == candidate_config_hash()
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        return factory, models[0].id


async def admit(factory, model_id, *, job_type=JobType.AGENT_PAPER_RESEARCH, key="candidate"):
    async with factory() as session, session.begin():
        return await admit_research_run(
            session,
            actor_id="isolated-candidate",
            route="/internal/v1/research-runs",
            idempotency_key=key,
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
            ),
            prompt_version_id=None,
            model_config_id=model_id,
            daily_limit_usd=Decimal("3"),
            now=NOW,
            job_type=job_type,
        )


@pytest.mark.asyncio
async def test_registered_candidate_qualifies_only_exact_frozen_first_paper_claim(
    migrated_database_url,
):
    from nexus.agents.codex_candidate import candidate_job_qualified
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    admission = await admit(factory, model_id)
    async with factory() as session, session.begin():
        job = await session.scalar(select(Job))
        assert job.max_attempts == 1
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="isolated-candidate",
            now=NOW,
            lease_seconds=210,
            job_ids=(job.id,),
            locked_job_guard=candidate_job_qualified,
            research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
        )
        assert claim is not None and claim.payload == {"run_id": str(admission.run_id)}
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 1
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 1


@pytest.mark.asyncio
async def test_candidate_is_not_selected_by_default_or_admitted_as_generic_research(
    migrated_database_url,
):
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_AUTHORITY_INVALID"):
        await admit(factory, model_id, job_type=JobType.AGENT_RESEARCH)
    await prepare_database(settings)
    admission = await admit(factory, None, job_type=JobType.AGENT_RESEARCH, key="default")
    async with factory() as session:
        model = await session.get(AgentModelConfigRecord, admission.run.model_config_id)
        assert model.version != CANDIDATE_BUDGET_CONFIG_VERSION


@pytest.mark.asyncio
async def test_candidate_registration_rejects_actual_database_mismatch_before_writes(
    migrated_database_url,
    monkeypatch,
):
    import nexus.ops.openai_local as module
    from sqlalchemy.engine import make_url

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claimed = make_url(migrated_database_url).set(database="nexus_diag_claimed")
    settings = EngineSettings(
        database_url=claimed.render_as_string(hide_password=False), agent_runtime_mode="codex_sdk"
    )
    monkeypatch.setattr(module, "session_factory", lambda _: factory)
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
        await prepare_database(settings, **OPTIONS)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentModelConfigRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field, value",
    [
        ("config_hash", "b" * 64),
        ("model_id", "other-model"),
        ("max_input_tokens", 64000),
        ("max_output_tokens", 8000),
        ("max_tool_calls", 1),
        ("input_usd_per_million", Decimal("0.01")),
        ("pricing_available_at", datetime(2026, 8, 2, tzinfo=UTC)),
    ],
)
async def test_candidate_admission_rejects_noncanonical_model_before_job_creation(
    migrated_database_url,
    field,
    value,
):
    from nexus.agents.codex_candidate import candidate_config, candidate_config_hash

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    config = candidate_config()
    record = dict(
        model_id=config["model_id"],
        version=config["version"],
        config_hash=candidate_config_hash(),
        max_input_tokens=96000,
        max_output_tokens=8192,
        max_tool_calls=0,
        input_usd_per_million=Decimal("0"),
        output_usd_per_million=Decimal("0"),
        pricing_available_at=datetime(2026, 9, 2, tzinfo=UTC),
        pricing_expires_at=datetime(2027, 9, 2, tzinfo=UTC),
    )
    record[field] = value
    async with factory() as session, session.begin():
        model = AgentModelConfigRecord(**record)
        session.add(model)
        await session.flush()
        model_id = model.id
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_AUTHORITY_INVALID"):
        await admit(factory, model_id)
    async with factory() as session:
        for kind in (Job, AgentResearchRunRecord, JobAttempt, JobLease):
            assert await session.scalar(select(func.count()).select_from(kind)) == 0


@pytest.mark.asyncio
async def test_candidate_locked_claim_rejects_different_durable_input_without_mutation(
    migrated_database_url,
):
    from nexus.agents.codex_candidate import candidate_job_qualified
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
    from nexus.jobs.models import JobKeyV1, RetryPolicy
    from nexus.jobs.repository import enqueue_job

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    admission = await admit(factory, model_id)
    async with factory() as session, session.begin():
        wrong = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                scope_kind="instrument",
                scope_id=str(_INSTRUMENT_ID),
                effective_at=_NOW,
                input_hash="c" * 64,
            ),
            schema_version="agent_research_job_v1",
            payload={"run_id": str(admission.run_id)},
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=1,
            available_at=NOW,
        )
        wrong_id = wrong.id
    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="isolated-candidate",
                now=NOW,
                lease_seconds=210,
                job_ids=(wrong_id,),
                locked_job_guard=candidate_job_qualified,
                research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
            )
            is None
        )
        wrong = await session.get(Job, wrong_id)
        assert wrong.status == "queued" and wrong.attempt_count == 0 and wrong.version == 1
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


@pytest.mark.asyncio
async def test_candidate_guard_refreshes_cached_run_after_other_connection_terminalizes(
    migrated_database_url,
):
    from nexus.agents.codex_candidate import candidate_job_qualified
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
    from nexus.agents.committee import FixtureCommittee
    from nexus.agents.execution import execute_research_run

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    admission = await admit(factory, model_id)
    async with factory() as session, session.begin():
        cached = await session.get(AgentResearchRunRecord, admission.run_id)
        assert cached.status == "prepared"
        async with factory() as other, other.begin():
            result = await execute_research_run(
                other,
                run_id=admission.run_id,
                committee=FixtureCommittee(),
                allow_fixture_evidence=True,
                allow_fixture_price_authority=True,
            )
            assert result.status != "prepared"
        job = await session.scalar(select(Job))
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="isolated-candidate",
                now=NOW,
                lease_seconds=210,
                job_ids=(job.id,),
                locked_job_guard=candidate_job_qualified,
                research_budget_profile=CANDIDATE_BUDGET_CONFIG_VERSION,
            )
            is None
        )
        assert cached.status != "prepared"
        assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


@pytest.mark.asyncio
async def test_candidate_refusal_precedes_idempotency_write_even_if_caller_catches_error(
    migrated_database_url,
):
    from nexus.persistence.models.runtime import IdempotencyRecord

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="CODEX_CANDIDATE_AUTHORITY_INVALID"):
            await admit_research_run(
                session,
                actor_id="isolated-candidate",
                route="/internal/v1/research-runs",
                idempotency_key="wrong-kind-caught",
                request_hash="a" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
                ),
                prompt_version_id=None,
                model_config_id=model_id,
                daily_limit_usd=Decimal("3"),
                now=NOW,
                job_type=JobType.AGENT_RESEARCH,
            )
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("inheritance", ["source_run", "latest_actor"])
async def test_inherited_candidate_refusal_leaves_no_partial_idempotency_record(
    migrated_database_url,
    inheritance,
):
    from nexus.persistence.models.runtime import IdempotencyRecord

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory, model_id = await prepare(settings)
    prior = await admit(factory, model_id)
    async with factory() as session, session.begin():
        before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
        kwargs = (
            {"version_source_run_id": prior.run_id}
            if inheritance == "source_run"
            else {"reuse_latest_actor_agent_version": True}
        )
        with pytest.raises(ValueError, match="CODEX_CANDIDATE_AUTHORITY_INVALID"):
            await admit_research_run(
                session,
                actor_id="isolated-candidate",
                route="/internal/v1/research-runs",
                idempotency_key=f"inherit-{inheritance}",
                request_hash="d" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
                ),
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=NOW,
                job_type=JobType.AGENT_PAPER_RESEARCH,
                **kwargs,
            )
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
