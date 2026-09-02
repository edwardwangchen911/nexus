"""Persisted research is an index, not a broker-readiness or trading authority."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import PriceBarSetAuthority
from nexus.persistence.session import session_factory
from nexus.portfolio.research import read_position_research
from nexus.settings import EngineSettings
from sqlalchemy import func, select

from engine.tests.integration.test_agent_portfolio_context import seed_context
from engine.tests.integration.test_research_snapshot_preparation import INSTRUMENT, NOW, seed_filing


@pytest.mark.asyncio
@pytest.mark.parametrize("quality", ["PASS", "NOT_RUN"])
async def test_frozen_inputs_reach_portfolio_api_without_promoting_readiness(
    migrated_database_url, quality
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        portfolio, _ = await seed_context(session, quality=quality)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
    read_at = datetime.now(UTC)
    path = f"/internal/v1/portfolios/{portfolio.portfolio_id}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=read_at)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    data = response.json()["data"]
    research = data["positions"][0]["research"]
    assert research["status"] == "not_started"
    assert research["run_id"] is research["quality_status"] is research["summary"] is None
    assert research["data_snapshot_id"] == str(frozen.id)
    assert datetime.fromisoformat(research["decision_time"]) == NOW
    assert datetime.fromisoformat(research["read_at"]) == read_at
    assert "private-" not in str(research)
    assert data["execution_locked"] is True
    assert data["decision_readiness"] == (
        "DECISION_READY" if quality == "PASS" else "RECONCILIATION_PENDING"
    )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_missing_and_late_arriving_inputs_are_not_invented(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    missing = UUID(int=42)
    async with factory() as session, session.begin():
        await seed_filing(session)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        read_at = frozen.created_at - timedelta(microseconds=1)
    async with factory() as session:
        assert await read_position_research(session, (), read_at=read_at) == {}
        index = await read_position_research(session, (INSTRUMENT, missing), read_at=read_at)
        assert set(index) == {INSTRUMENT, missing}
        assert all(item.status == "not_started" for item in index.values())
        assert all(item.data_snapshot_id is None for item in index.values())
    async with factory() as session:
        index = await read_position_research(
            session, (INSTRUMENT, missing), read_at=datetime.now(UTC)
        )
        assert index[INSTRUMENT].data_snapshot_id == frozen.id
        assert index[missing].data_snapshot_id is None
