"""OpenAI activation registers real configuration without inventing research facts."""

import pytest
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,staged", [("fixture", False), ("codex_sdk", False), ("codex_sdk", True)]
)
async def test_prepare_is_idempotent_and_does_not_create_research_or_market_evidence(
    migrated_database_url: str,
    backend: str,
    staged: bool,
) -> None:
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode=backend)
    await prepare_database(settings, staged=staged)
    await prepare_database(settings, staged=staged)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(AgentPromptVersionRecord)) == 8
        assert await session.scalar(select(func.count()).select_from(AgentModelConfigRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        model = await session.scalar(select(AgentModelConfigRecord))
        assert model.worst_case_cost_usd < 1
        assert "fixture" not in model.version
        if backend == "codex_sdk":
            assert model.version == ("codex-sdk-chatgpt-v2" if staged else "codex-sdk-chatgpt-v1")
            assert model.worst_case_cost_usd == 0  # API dollars, not unlimited subscription quota.
            assert model.max_tool_calls == 0
