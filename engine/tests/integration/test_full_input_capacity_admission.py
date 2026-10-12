"""Fresh synthetic v9 facts only in temporary PostgreSQL databases."""

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from integration.test_agent_execution_pg import _INSTRUMENT_ID, _NOW, _SNAPSHOT_ID, _seed_authority
from integration.test_candidate_research_authority import NOW
from nexus.agents.codex_capacity import capacity_config, capacity_config_hash
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.jobs.models import JobType
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def prepare(url, *, real_sources=False):
    factory = session_factory(EngineSettings(database_url=url, agent_runtime_mode="codex_sdk"))
    await _seed_authority(factory, include_base_evidence=not real_sources)
    if real_sources:
        from integration.test_agent_execution_pg import _seed_database_tool_authority

        await _seed_database_tool_authority(
            factory, link_price=False, bar_count=1, price_quality_flags=("missing",)
        )
    config = capacity_config()
    async with factory.begin() as session:
        model = AgentModelConfigRecord(
            model_id=config["model_id"],
            version=config["version"],
            config_hash=capacity_config_hash(),
            max_input_tokens=96000,
            max_output_tokens=8192,
            max_tool_calls=0,
            input_usd_per_million=Decimal(0),
            output_usd_per_million=Decimal(0),
            pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
            pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
            created_at=NOW - timedelta(minutes=1),
        )
        session.add(model)
        await session.flush()
        return factory, model.id


async def admit(session, model_id, **options):
    return await admit_research_run(
        session,
        actor_id="isolated-v9",
        route="/internal/v1/research-runs",
        idempotency_key=options.pop("key", "fresh-v9"),
        request_hash="a" * 64,
        request=ResearchRequest(
            instrument_id=_INSTRUMENT_ID, data_snapshot_id=_SNAPSHOT_ID, decision_time=_NOW
        ),
        prompt_version_id=None,
        model_config_id=model_id,
        daily_limit_usd=Decimal("3"),
        now=NOW,
        job_type=JobType.AGENT_PAPER_RESEARCH,
        **options,
    )


@pytest.mark.asyncio
async def test_default_v9_refusal_leaves_no_claim_run_or_job(migrated_database_url):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="CODEX_CAPACITY_ISOLATED_ADMISSION_REQUIRED"):
            await admit(session, model_id)
        for record in (
            IdempotencyRecord,
            AgentResearchRunRecord,
            Job,
            AgentDailyBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(record)) == 0


@pytest.mark.asyncio
async def test_explicit_v9_admission_binds_new_identity_once(migrated_database_url):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        first = await admit(session, model_id, full_input_capacity=True)
        second = await admit(session, model_id, full_input_capacity=True)
        assert second.replayed and first.run_id == second.run_id
        job = await session.scalar(select(Job))
        assert job.max_attempts == 1 and job.payload == {"run_id": str(first.run_id)}
        assert first.run.model_hash == capacity_config_hash()
        assert first.run.pricing_version == "codex-sdk-chatgpt-v9-full-input-capacity"
        with pytest.raises(ValueError, match="CODEX_CAPACITY_ISOLATED_ADMISSION_REQUIRED"):
            await admit(session, model_id)


@pytest.mark.asyncio
async def test_v9_is_excluded_from_automatic_model_selection(migrated_database_url):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        result = await admit(session, None)
        assert result.run.model_config_id != model_id


@pytest.mark.asyncio
async def test_inherited_v9_refusal_rolls_back_claim_inside_caller_transaction(
    migrated_database_url,
):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        first = await admit(session, model_id, full_input_capacity=True)
        with pytest.raises(ValueError, match="CODEX_CAPACITY_ISOLATED_ADMISSION_REQUIRED"):
            await admit(session, None, key="inherit-v9", reuse_latest_actor_agent_version=True)
        assert first.run.pricing_version == "codex-sdk-chatgpt-v9-full-input-capacity"
        for record in (
            IdempotencyRecord,
            AgentResearchRunRecord,
            Job,
            AgentDailyBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(record)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [{"full_input_capacity": 1}, {"full_input_capacity": True, "formal_stage_capacity": True}],
)
async def test_invalid_profile_arguments_refuse_before_any_business_fact(
    migrated_database_url, options
):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="CODEX_CAPACITY_ADMISSION_ARGUMENT_INVALID"):
            await admit(session, model_id, **options)
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 0


@pytest.mark.asyncio
async def test_replay_rechecks_model_after_idempotency_wait(migrated_database_url, monkeypatch):
    factory, model_id = await prepare(migrated_database_url)
    async with factory.begin() as session:
        await admit(session, model_id, full_input_capacity=True)
        get = session.get
        drift = False

        async def after_authority():
            nonlocal drift
            drift = True

        async def observed_model(entity, key, **kwargs):
            result = await get(entity, key, **kwargs)
            if drift and entity is AgentModelConfigRecord and result is not None:
                # A controlled observation fault; never update an immutable row.
                fields = {
                    column.key: getattr(result, column.key) for column in result.__table__.columns
                }
                return SimpleNamespace(**(fields | {"max_input_tokens": 96001}))
            return result

        monkeypatch.setattr(session, "get", observed_model)
        with pytest.raises(ValueError, match="CODEX_CAPACITY_AUTHORITY_INVALID"):
            await admit(
                session, model_id, full_input_capacity=True, after_authority=after_authority
            )
        for record in (
            IdempotencyRecord,
            AgentResearchRunRecord,
            Job,
            AgentDailyBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(record)) == 1
