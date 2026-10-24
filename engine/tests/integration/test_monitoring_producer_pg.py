"""New v8 proposals traverse normal admission, Worker and durable research outputs."""

import pytest
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as run_normal_cycle,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from sqlalchemy import select


@pytest.mark.asyncio
async def test_monitoring_proposal_survives_formal_worker_persistence(
    migrated_database_url, monkeypatch
):
    await run_normal_cycle(
        migrated_database_url, monkeypatch, 3, False, "formal_v11", monitoring=True
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        rows = (
            await session.scalars(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.status == "completed"
                )
            )
        ).all()
        assert len(rows) == 3
        for row in rows:
            intent = ResearchIntent.model_validate(row.intent)
            assert intent.monitoring_proposal is not None
            assert intent.monitoring_proposal.condition.threshold == "123.45"
            run = await session.get(AgentResearchRunRecord, row.run_id)
            assert run.status == "completed"
            assert run.settled_cost_usd is not None
            assert (run.input_tokens, run.output_tokens) == (300, 300)
