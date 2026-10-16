"""Normal HTTP admission must reject damaged historical Job/Run bindings."""

import json
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.main import create_engine_app
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["valid", "duplicate_missing", "wrong_family", "extra_payload"])
async def test_manual_http_requires_one_matching_job_per_historical_run(
    migrated_database_url, monkeypatch, shape
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **{**V9_OPTIONS, "formal_flexible_stage_budget": True},
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS, decision_time=AT
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return AT
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    app = None
    try:
        async with sessions.begin() as session:
            next_at = AT + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=AT - timedelta(hours=1),
                )
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="historical-binding-batch",
                created_at=AT,
            )
            result = await agent_cycle._start_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                daily_limit_usd=Decimal("3"),
                candidate_batch_id=batch.batch_id,
                requested_instrument_ids=batch.instrument_ids,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            assert len(result.research_run_ids) == 3
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            await agent_cycle._append_event(
                session, cycle, "researching", "unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE", {}
            )
            runs = list(await session.scalars(select(AgentResearchRunRecord)))
            jobs = list(await session.scalars(select(Job).order_by(Job.id)))
            assert len(jobs) == len(runs) == 3
            for run in runs:
                run.status = "failed"
                run.settled_cost_usd = Decimal("0")
            for job in jobs:
                job.status = "running"
                job.started_at = AT
                job.attempt_count = 1
                job.version += 1
            await session.flush()
            for job in jobs:
                job.status = "dead_letter"
                job.finished_at = AT
                job.version += 1
            await session.flush()
            if shape != "valid":
                # Corruption fixture only, in a freshly provisioned test database.
                # Normal producers cannot mutate immutable Job authority this way.
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                if shape == "duplicate_missing":
                    jobs[1].payload = dict(jobs[0].payload)
                elif shape == "wrong_family":
                    jobs[1].job_type = "agent.portfolio_synthesis"
                else:
                    jobs[1].payload = {**jobs[1].payload, "unexpected": True}
                await session.flush()
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
            before = [
                (run.id, run.status, run.reserved_cost_usd, run.settled_cost_usd) for run in runs
            ]
        settings = EngineSettings(
            **{
                **api_settings(migrated_database_url).model_dump(),
                "agent_runtime_mode": "codex_sdk",
                "agent_paper_account_id": authority.account_id,
                "agent_paper_candidate_batch_id": batch.batch_id,
                "agent_paper_formal_model_config_id": model.id,
                "agent_paper_cycle_enabled": True,
                "agent_paper_manual_enabled": True,
                "release_revision": "a" * 40,
            }
        )
        await heartbeat_agent_paper_loop(
            settings,
            account_id=authority.account_id,
            worker_id=f"agent-paper-loop-{UUID(int=1)}",
            started_at=AT - timedelta(minutes=1),
            seen_at=AT,
        )
        app = create_engine_app(settings)
        path = "/internal/v1/paper/manual-requests"
        body = json.dumps({"mode": "research_and_paper"}).encode()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "independent-new"},
            )
        if shape == "valid":
            assert response.status_code == 200, response.text
            assert response.json()["data"]["status"] == "queued"
        else:
            assert response.status_code == 409, response.text
            assert (
                response.json()["error"]["details"]["reason_code"]
                == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
            )
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == (
                1 if shape == "valid" else 0
            )
            after = list(await session.scalars(select(AgentResearchRunRecord)))
            assert sorted(before) == sorted(
                (run.id, run.status, run.reserved_cost_usd, run.settled_cost_usd) for run in after
            )
    finally:
        if app is not None:
            await app.state.session_factory.kw["bind"].dispose()
        await engine.dispose()
