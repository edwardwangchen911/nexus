"""OpenAI activation registers real configuration without inventing research facts."""

import pytest
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.evolution import StrategyExperimentFamilyRecord, StrategyRNDRunRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,staged,strategy_rnd",
    [
        ("fixture", False, False),
        ("codex_sdk", False, False),
        ("codex_sdk", True, False),
        ("codex_sdk", False, True),
    ],
)
async def test_prepare_is_idempotent_and_does_not_create_research_or_market_evidence(
    migrated_database_url: str,
    backend: str,
    staged: bool,
    strategy_rnd: bool,
) -> None:
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode=backend)
    await prepare_database(settings, staged=staged, strategy_rnd=strategy_rnd)
    await prepare_database(settings, staged=staged, strategy_rnd=strategy_rnd)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(AgentPromptVersionRecord)) == (
            10 if strategy_rnd else 9
        )
        expected_models = 2 if backend == "codex_sdk" else 1
        assert (
            await session.scalar(select(func.count()).select_from(AgentModelConfigRecord))
            == expected_models
        )
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        for model_type in (Job, StrategyRNDRunRecord, StrategyExperimentFamilyRecord):
            assert await session.scalar(select(func.count()).select_from(model_type)) == 0
        from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION

        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version != PORTFOLIO_DECISION_CONFIG_VERSION
            )
        )
        assert model.worst_case_cost_usd < 1
        assert "fixture" not in model.version
        if backend == "codex_sdk":
            expected_version = (
                "codex-sdk-strategy-rnd-v1"
                if strategy_rnd
                else "codex-sdk-chatgpt-v2"
                if staged
                else "codex-sdk-chatgpt-v1"
            )
            assert model.version == expected_version
            assert model.worst_case_cost_usd == 0  # API dollars, not unlimited subscription quota.
            assert model.max_tool_calls == 0
        if strategy_rnd:
            prompt = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == "strategy_rnd"
                )
            )
            assert prompt is not None
            assert prompt.version == "strategy_rnd_v1"


@pytest.mark.asyncio
async def test_codex_setup_registers_a_separate_bounded_portfolio_model(
    migrated_database_url: str,
) -> None:
    from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings)

    async with session_factory(settings)() as session:
        models = {
            model.version: model
            for model in await session.scalars(select(AgentModelConfigRecord))
        }
    decision = models[PORTFOLIO_DECISION_CONFIG_VERSION]
    assert decision.model_id == "gpt-5.6-terra"
    assert decision.max_input_tokens == 128_000
    assert decision.max_output_tokens == 16_384
    assert decision.max_tool_calls == 1
    assert decision.worst_case_cost_usd == 0


@pytest.mark.asyncio
async def test_strategy_rnd_setup_preserves_existing_committee_model_versions(
    migrated_database_url: str,
) -> None:
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="codex_sdk",
    )
    await prepare_database(settings)
    await prepare_database(settings, staged=True)
    async with session_factory(settings)() as session:
        previous = {
            model.version: (model.id, model.config_hash)
            for model in await session.scalars(select(AgentModelConfigRecord))
        }

    await prepare_database(settings, strategy_rnd=True)
    await prepare_database(settings, strategy_rnd=True)

    async with session_factory(settings)() as session:
        current = {
            model.version: (model.id, model.config_hash)
            for model in await session.scalars(select(AgentModelConfigRecord))
        }
        assert len(current) == len(previous) + 1
        assert {version: current[version] for version in previous} == previous
        assert "codex-sdk-strategy-rnd-v1" in current
        prompt_count = await session.scalar(
            select(func.count()).select_from(AgentPromptVersionRecord)
        )
        assert prompt_count == 10
        portfolio_prompt = await session.scalar(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.role == "portfolio_decision"
            )
        )
        assert portfolio_prompt is not None
        assert portfolio_prompt.version == "portfolio-decision-v1"


@pytest.mark.asyncio
async def test_inline_setup_is_idempotent_and_preserves_old_config(migrated_database_url):
    from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    options = dict(structured_synthesis=True, aggregate_input_tokens=96000, max_wire_bytes=64000)
    await prepare_database(settings, **options)
    factory = session_factory(settings)
    async with factory() as session:
        old = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version != PORTFOLIO_DECISION_CONFIG_VERSION
            )
        )
        assert old is not None
        before = {column.name: getattr(old, column.name) for column in old.__table__.columns}
        prompts = await session.scalar(select(func.count()).select_from(AgentPromptVersionRecord))
    await prepare_database(settings, **options, inline_citations=True)
    await prepare_database(settings, **options, inline_citations=True)
    async with factory() as session:
        models = list(await session.scalars(select(AgentModelConfigRecord)))
        assert len(models) == 3
        original = next(row for row in models if row.id == before["id"])
        assert {
            column.name: getattr(original, column.name) for column in original.__table__.columns
        } == before
        new = next(
            row
            for row in models
            if row.id != before["id"]
            and row.version != PORTFOLIO_DECISION_CONFIG_VERSION
        )
        assert new.version == "codex-sdk-chatgpt-v3-input96k-wire64k-cite1"
        assert new.config_hash != before["config_hash"]
        assert new.max_input_tokens == original.max_input_tokens
        assert new.max_output_tokens == original.max_output_tokens
        assert (
            await session.scalar(select(func.count()).select_from(AgentPromptVersionRecord))
            == prompts
        )
        for model_type in (AgentResearchRunRecord, DataSnapshot, Job):
            assert await session.scalar(select(func.count()).select_from(model_type)) == 0


@pytest.mark.asyncio
async def test_actionable_setup_is_idempotent_and_creates_no_work(migrated_database_url):
    from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    options = dict(
        actionable_paper=True,
        structured_synthesis=True,
        aggregate_input_tokens=96000,
        max_wire_bytes=64000,
        inline_citations=True,
    )
    await prepare_database(settings, **options)
    await prepare_database(settings, **options)
    async with session_factory(settings)() as session:
        models = tuple(
            await session.scalars(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == ACTIONABLE_CONFIG_VERSION
                )
            )
        )
        assert len(models) == 1
        assert models[0].max_input_tokens == 96000
        assert models[0].max_output_tokens == 8192
        assert models[0].max_tool_calls == 0
        synthesis_prompt = await session.scalar(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.role == "portfolio",
                AgentPromptVersionRecord.version == "local-portfolio-synthesis-v3",
            )
        )
        assert synthesis_prompt is not None
        assert "must not by itself make a review unresolved" in synthesis_prompt.content
        for model_type in (AgentResearchRunRecord, DataSnapshot, Job):
            assert await session.scalar(select(func.count()).select_from(model_type)) == 0
