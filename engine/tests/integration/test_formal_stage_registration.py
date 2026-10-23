"""Normal registration persists a distinct pending profile without activating Jobs."""

from datetime import datetime
from decimal import Decimal

import pytest
from integration.test_agent_execution_pg import _seed_authority
from integration.test_candidate_research_authority import admit
from nexus.agents.codex_formal_stage import formal_stage_config, formal_stage_config_hash
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.jobs.models import JobType
from nexus.ops import openai_local
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["v8", "v9", "v10", "formal-v10", "formal-v11"])
async def test_pending_formal_registration_is_idempotent_and_default_stays_unchanged(
    migrated_database_url,
    profile,
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    from nexus.agents.codex_bounded_budget import bounded_stage_config_hash
    from nexus.agents.codex_capacity import capacity_config_hash
    from nexus.agents.codex_flexible_budget import flexible_stage_config_hash
    from nexus.agents.codex_versions import (
        BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    )
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    options = (
        (V9_OPTIONS | {"formal_bounded_stage_budget": True})
        if profile == "formal-v11"
        else (V9_OPTIONS | {"formal_flexible_stage_budget": True})
        if profile == "formal-v10"
        else (V9_OPTIONS | {"flexible_stage_budget": True})
        if profile == "v10"
        else V9_OPTIONS
        if profile == "v9"
        else OPTIONS
    )
    version = (
        BOUNDED_STAGE_BUDGET_CONFIG_VERSION
        if profile == "formal-v11"
        else FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION
        if profile in {"v10", "formal-v10"}
        else FULL_INPUT_CAPACITY_CONFIG_VERSION
        if profile == "v9"
        else FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    )
    expected_hash = (
        bounded_stage_config_hash()
        if profile == "formal-v11"
        else flexible_stage_config_hash()
        if profile in {"v10", "formal-v10"}
        else capacity_config_hash()
        if profile == "v9"
        else formal_stage_config_hash()
    )
    await _seed_authority(factory)
    await openai_local.prepare_database(settings)
    # Make only future inserts newer; do not rewrite an existing model record.
    async with factory.begin() as session:
        await session.execute(
            text(
                "ALTER TABLE agent_model_configs ALTER COLUMN created_at "
                "SET DEFAULT '2026-09-02T00:00:00+00:00'::timestamptz"
            )
        )
    await openai_local.prepare_database(settings, **options)
    await openai_local.prepare_database(settings, **options)
    async with factory() as session:
        models = list(
            await session.scalars(
                select(AgentModelConfigRecord).where(AgentModelConfigRecord.version == version)
            )
        )
        assert len(models) == 1 and models[0].config_hash == expected_hash
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
    result = await admit(factory, None, job_type=JobType.AGENT_RESEARCH, key="formal-default")
    async with factory() as session:
        chosen = await session.get(AgentModelConfigRecord, result.run.model_config_id)
        assert chosen.version not in {
            FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            FULL_INPUT_CAPACITY_CONFIG_VERSION,
            BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
            FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("job_type", [JobType.AGENT_RESEARCH, JobType.AGENT_PAPER_RESEARCH])
async def test_pending_formal_explicit_admission_refuses_before_idempotency_write(
    migrated_database_url, job_type
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    await openai_local.prepare_database(settings, **OPTIONS)
    async with factory() as session:
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            )
        )
        before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
    with pytest.raises(ValueError, match="CODEX_FORMAL_STAGE_ACTIVATION_REQUIRED"):
        await admit(factory, model.id, job_type=job_type)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0


@pytest.mark.asyncio
async def test_formal_registration_checks_actual_database_before_writes(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claimed = make_url(migrated_database_url).set(database="nexus_diag_wrong_formal")
    settings = EngineSettings(
        database_url=claimed.render_as_string(hide_password=False), agent_runtime_mode="codex_sdk"
    )
    monkeypatch.setattr(openai_local, "session_factory", lambda _: factory)
    with pytest.raises(ValueError, match="CODEX_STAGE_TIME_BUDGET_ISOLATION_REQUIRED"):
        await openai_local.prepare_database(settings, **OPTIONS)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentModelConfigRecord)) == 0


@pytest.mark.asyncio
async def test_formal_registration_refuses_conflicting_stored_fields(migrated_database_url):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    config = formal_stage_config()
    async with factory.begin() as session:
        # Seed a conflicting initial record; append-only audit protection stays enabled.
        model = AgentModelConfigRecord(
            model_id=str(config["model_id"]),
            version=FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
            config_hash=formal_stage_config_hash(),
            max_input_tokens=1,
            max_output_tokens=8192,
            max_tool_calls=0,
            input_usd_per_million=Decimal("0"),
            output_usd_per_million=Decimal("0"),
            pricing_available_at=datetime.fromisoformat(str(config["pricing_available_at"])),
            pricing_expires_at=datetime.fromisoformat(str(config["pricing_expires_at"])),
        )
        session.add(model)
        await session.flush()
        model_id = model.id
    with pytest.raises(ValueError, match="OPENAI_MODEL_VERSION_CONFLICT"):
        await openai_local.prepare_database(settings, **OPTIONS)
    async with factory() as session:
        model = await session.get(AgentModelConfigRecord, model_id)
        assert model.max_input_tokens == 1


@pytest.mark.asyncio
async def test_bounded_setup_versions_scope_without_rewriting_frozen_prompt_authority(
    migrated_database_url,
):
    import hashlib
    import json

    from nexus.agents.execution_authority import load_frozen_prompts
    from nexus.agents.persistence import load_complete_prompt_authority
    from nexus.persistence.models.agents import AgentPromptVersionRecord
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await openai_local.prepare_database(settings, **OPTIONS)
    async with factory() as session:
        old = await load_complete_prompt_authority(session)
        assert old is not None and old["portfolio"].version == "local-portfolio-synthesis-v4"
        original = {
            row.id: {column.name: getattr(row, column.name) for column in row.__table__.columns}
            for row in old.values()
        }
        manifest = {
            role: {"id": str(row.id), "version": row.version, "content_hash": row.content_hash}
            for role, row in old.items()
        }
        frozen = AgentResearchRunRecord(
            prompt_version_id=old["orchestrator"].id,
            prompt_manifest=manifest,
            prompt_hash=hashlib.sha256(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
    # A previously registered v5 authority remains readable and byte-identical.
    from nexus.agents.committee import load_portfolio_synthesis_prompt

    v5 = load_portfolio_synthesis_prompt(security_research_scope=True)
    async with factory.begin() as session:
        previous = AgentPromptVersionRecord(
            role=v5.role, version=v5.version, content=v5.content, content_hash=v5.content_hash
        )
        session.add(previous)
        await session.flush()
        previous_id = previous.id
    options = V9_OPTIONS | {"formal_bounded_stage_budget": True}
    await openai_local.prepare_database(settings, **options)
    await openai_local.prepare_database(settings, **options)
    async with factory() as session:
        current = await load_complete_prompt_authority(session)
        assert current is not None
        assert current["portfolio"].version == "local-portfolio-synthesis-v6-claim-review"
        assert "research_scope=single_security" in current["portfolio"].content
        assert "atomic claims" in current["portfolio"].content
        preserved_v5 = await session.get(AgentPromptVersionRecord, previous_id)
        assert preserved_v5.content == v5.content and preserved_v5.content_hash == v5.content_hash
        historical = await load_frozen_prompts(session, frozen)
        assert historical is not None
        assert historical["portfolio"].version == "local-portfolio-synthesis-v4"
        assert {role: row.id for role, row in historical.items()} == {
            role: row.id for role, row in old.items()
        }
        for prompt_id, expected in original.items():
            actual = await session.get(AgentPromptVersionRecord, prompt_id)
            assert {
                column.name: getattr(actual, column.name) for column in actual.__table__.columns
            } == expected
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AgentPromptVersionRecord)
                .where(
                    AgentPromptVersionRecord.version == "local-portfolio-synthesis-v6-claim-review"
                )
            )
            == 1
        )
        for table in (AgentResearchRunRecord, Job, IdempotencyRecord):
            assert await session.scalar(select(func.count()).select_from(table)) == 0
