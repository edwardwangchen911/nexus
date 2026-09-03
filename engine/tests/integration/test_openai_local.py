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
            9 if strategy_rnd else 8
        )
        assert await session.scalar(select(func.count()).select_from(AgentModelConfigRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        for model_type in (Job, StrategyRNDRunRecord, StrategyExperimentFamilyRecord):
            assert await session.scalar(select(func.count()).select_from(model_type)) == 0
        model = await session.scalar(select(AgentModelConfigRecord))
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
        assert len(current) == 3
        assert {version: current[version] for version in previous} == previous
        assert "codex-sdk-strategy-rnd-v1" in current
        assert await session.scalar(select(func.count()).select_from(AgentPromptVersionRecord)) == 9
