"""Economic evidence reads must not manufacture a paired evaluation."""

from datetime import timedelta

import httpx
import pytest
from helpers.paper_api import app, headers
from integration.test_portfolio_proposal_pg import proposal_source
from nexus.paper.agent_cycle import _events, _resume_portfolio_synthesis
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.paper import AgentPaperCycleRecord
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [True, False])
async def test_economic_report_keeps_ordinary_samples_separate_and_is_read_only(
    migrated_database_url, completed
):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            if completed:
                result = await _resume_portfolio_synthesis(
                    session,
                    cycle=cycle,
                    events=await _events(session, cycle.id),
                    run_ids=(),
                    now=args["now"] + timedelta(seconds=1),
                    daily_limit_usd=None,
                )
                assert result.status == "completed"
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            reader = PaperReadService(session, args["now"] + timedelta(minutes=2))
            method = getattr(reader, "portfolio_economic_evidence", None)
            assert callable(method), "normal economic evidence read is missing"
            first = await method()
            second = await method()
            assert first == second
            assert first.protocol_status == "not_implemented"
            assert first.economic_status == "not_evaluated"
            assert first.planned_slot_count is None
            assert first.economic_pair_count is None
            assert first.execution_locked is True
            assert len(first.ordinary_samples) == (1 if completed else 0)
            if completed:
                assert first.ordinary_samples[0].cycle_id == cycle.id
                assert first.ordinary_samples[0].decision_slot_at == cycle.decision_slot_at
                assert first.ordinary_samples[0].execution_state == "no_order"
            payload = first.model_dump_json()
            for forbidden in (
                "prompt_content",
                "initial_capital",
                "account_id",
                "cash_weight",
                "candidate_facts",
            ):
                assert forbidden not in payload
        path = "/internal/v1/paper/economic-evidence"
        application = app(migrated_database_url, now=args["now"] + timedelta(minutes=2))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            for _ in range(2):
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                assert response.json()["data"] == first.model_dump(mode="json")
            rejected = await client.get(
                path + "?cycle_id=anything", headers=headers("GET", path + "?cycle_id=anything")
            )
            assert rejected.status_code == 422
            assert (await client.post(path)).status_code == 405
    finally:
        await engine.dispose()
