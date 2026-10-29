"""New v8 proposals traverse normal admission, Worker and durable research outputs."""

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as run_normal_cycle,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize("rejected", [False, True], ids=["accepted-monitor", "audit-veto"])
async def test_monitoring_proposal_survives_formal_worker_persistence(
    migrated_database_url, monkeypatch, rejected
):
    await run_normal_cycle(
        migrated_database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        monitoring_rejected=rejected,
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
            if rejected:
                assert intent.monitoring_proposal is None
            else:
                assert intent.monitoring_proposal is not None
                assert intent.monitoring_proposal.condition.threshold == "123.45"
            run = await session.get(AgentResearchRunRecord, row.run_id)
            assert run.status == "completed"
            assert run.settled_cost_usd is not None
            assert (run.input_tokens, run.output_tokens) == (300, 300)
    from datetime import timedelta
    from uuid import UUID

    from integration.test_candidate_multistock_path import AT
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.monitoring.installation import install_plan_from_output
    from nexus.ops.research_monitor import parser
    from nexus.ops.research_monitor import run as run_monitor
    from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import func

    for row in rows:
        async with factory() as session:
            run = await session.get(AgentResearchRunRecord, row.run_id)
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=run.instrument_id,
                as_of=run.decision_time,
            )
            cycle = await session.get(AgentPaperCycleRecord, bound[0].cycle_id)
            account_id = cycle.account_id
        if rejected:
            with pytest.raises(ValueError, match="MONITOR_SOURCE_INVALID"):
                async with factory() as session, session.begin():
                    await install_plan_from_output(
                        session,
                        account_id=account_id,
                        output_id=row.id,
                        expected_predecessor_id=None,
                        now=AT + timedelta(seconds=3),
                    )
            continue
        with pytest.raises(ValueError, match="MONITOR_SOURCE_INVALID"):
            async with factory() as session, session.begin():
                await install_plan_from_output(
                    session,
                    account_id=UUID(int=1),
                    output_id=row.id,
                    expected_predecessor_id=None,
                    now=AT + timedelta(seconds=3),
                )
        entry_settings = EngineSettings(
            database_url=migrated_database_url,
            agent_paper_account_id=account_id,
            agent_paper_candidate_batch_id=cycle.candidate_batch_id,
        )
        receipt = await run_monitor(
            entry_settings, parser().parse_args(["install", "--output-id", str(row.id)])
        )
        async with factory() as session:
            plan = await session.get(MonitorPlan, UUID(receipt["plan_id"]))
        readback = await run_monitor(
            entry_settings, parser().parse_args(["inspect", "--plan-id", str(plan.id)])
        )
        assert readback["current"] and readback["coverage"] == "active"
        assert readback["source_output_id"] == str(row.id)
        assert readback["execution_locked"] and readback["model_requests"] == 0
        async with factory() as session, session.begin():
            observed_at = await session.scalar(select(func.clock_timestamp()))
            replay = await install_plan_from_output(
                session,
                account_id=account_id,
                output_id=row.id,
                expected_predecessor_id=None,
                now=observed_at,
            )
            assert replay.id == plan.id and replay.generation == 1
            assert replay.source_hash == row.content_hash
            assert replay.data_snapshot_id == run.data_snapshot_id
        with pytest.raises(ValueError, match="MONITOR_PLAN_CONFLICT"):
            async with factory() as session, session.begin():
                await install_plan_from_output(
                    session,
                    account_id=account_id,
                    output_id=row.id,
                    expected_predecessor_id=UUID(int=2),
                    now=observed_at,
                )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorPlan)) == (
            0 if rejected else 3
        )
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
    if rejected:
        return
    from nexus.monitoring.scanning import scan_account
    from nexus.ops.research_monitor import scan_configured_once
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.monitoring import MonitorCheck
    from nexus.persistence.models.runtime import Job

    async with factory() as session, session.begin():
        # Formal one-shot fixtures only need today's session. The resident service
        # also requires the next actual trading session; append it before scanning.
        monday = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
                created_at=AT - timedelta(days=1),
            )
        )
        jobs_before = await session.scalar(select(func.count()).select_from(Job))
    first = await scan_configured_once(entry_settings, calendar_authority_id=CALENDAR_ID)
    again = await scan_configured_once(entry_settings, calendar_authority_id=CALENDAR_ID)
    assert first["checks"] == again["checks"] == 3
    assert first["unknown"] == 0
    assert first["model_requests"] == first["provider_requests"] == 0
    async with factory() as session, session.begin():
        closed = await scan_account(
            session,
            account_id=account_id,
            calendar_authority_id=CALENDAR_ID,
            now=AT.replace(hour=12),
        )
        assert closed["status"] == "market_closed" and closed["checks"] == 0
        stale = await scan_account(
            session,
            account_id=account_id,
            calendar_authority_id=CALENDAR_ID,
            now=AT + timedelta(minutes=5),
        )
        assert stale["unknown"] == 3
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorCheck)) == 6
        assert await session.scalar(select(func.count()).select_from(Job)) == jobs_before
