"""A validated all-defer pool must not activate inherited targets.

Synthetic research fixtures, real synthesis Worker/Attempt and normal cycle/API.
This is regression evidence, not acceptance of a private historical observation.
"""

import os
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from helpers.paper_api import app, headers
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis, synthesis_intent
from nexus.agents.portfolio_synthesis_worker import run_synthesis_job_once
from nexus.agents.responses_budget import UsageSnapshot
from nexus.paper import agent_cycle
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperOrderRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_partial_all_defer_finishes_without_target_activation(migrated_database_url):
    engine, sessions, args = await seed_synthesis(
        migrated_database_url, candidate_instruments=INSTRUMENTS
    )
    try:
        from integration.test_candidate_multistock_path import _set_test_insert_clock

        await _set_test_insert_clock(sessions, args["now"])
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            before = [
                await session.scalar(select(func.count()).select_from(model))
                for model in (StrategyDecisionRecord, PaperTargetVersionRecord, PaperOrderRecord)
            ]
            pending = await agent_cycle._resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await agent_cycle._events(session, cycle.id),
                now=args["now"],
                daily_limit_usd=Decimal("1"),
            )
            assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
            run = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            raw = synthesis_intent(run).model_dump_json()
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )

        calls = []

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                calls.append(payload["input_hash"])
                return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

        now = args["now"] + timedelta(seconds=2)
        await _set_test_insert_clock(sessions, now)
        worker_args = dict(
            transport=Transport(),
            worker_id="partial-closeout",
            clock=lambda: now,
            job_ids=(job_id,),
        )
        assert await run_synthesis_job_once(sessions, **worker_args)
        assert not await run_synthesis_job_once(sessions, **worker_args)
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            count_before = len(await agent_cycle._events(session, cycle.id))
            for _ in range(2):
                result = await agent_cycle._resume_research(
                    session,
                    cycle=cycle,
                    authority=None,
                    events=await agent_cycle._events(session, cycle.id),
                    now=now,
                )
                assert (result.status, result.result_code) == (
                    "completed",
                    "PORTFOLIO_ASSESSMENT_DEFERRED",
                )
                assert result.strategy_decision_id is None
                assert result.portfolio_synthesis_output_id is not None
            events = await agent_cycle._events(session, cycle.id)
            assert len(events) == count_before + 1
            assert events[-1].occurred_at == now
            assert [
                await session.scalar(select(func.count()).select_from(model))
                for model in (StrategyDecisionRecord, PaperTargetVersionRecord, PaperOrderRecord)
            ] == before
            job = await session.get(Job, job_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job_id))
            assert job.status == attempt.outcome == "succeeded"
            assert attempt.finished_at is not None
            from nexus.paper.manual_request import require_settled_account_cycle_history
            from nexus.persistence.models.runtime import JobLease

            # Closing a non-executing result does not free a retained lease.
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_settled_account_cycle_history(
                    session, account_id=cycle.account_id, now=now
                )
            expiry = await session.scalar(select(func.max(JobLease.expires_at)))
            assert expiry > now
            await require_settled_account_cycle_history(
                session, account_id=cycle.account_id, now=expiry
            )

        # The persisted terminal must also reject a late execution writer.
        async with sessions.begin() as session:
            await session.execute(
                text("CREATE TABLE deferred_late_writer_probe(agent_paper_cycle_id uuid)")
            )
            await session.execute(
                text("""CREATE TRIGGER deferred_late_writer_probe
                BEFORE INSERT ON deferred_late_writer_probe FOR EACH ROW
                EXECUTE FUNCTION nexus_deferred_assessment_execution_guard()""")
            )
        with pytest.raises(DBAPIError, match="DEFERRED_ASSESSMENT_EXECUTION_FORBIDDEN"):
            async with sessions.begin() as session:
                await session.execute(
                    text("INSERT INTO deferred_late_writer_probe VALUES (:id)"),
                    {"id": args["cycle_id"]},
                )

        path = f"/internal/v1/paper/management/cycles/{args['cycle_id']}"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=now)),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.json()
            data = response.json()["data"]
            assert data["blocker"] is None
            assert data["activation_result"] is None
            assert data["proposal"] is None
            assert data["orders"] == []
            assert data["cycle"]["result_code"] == "PORTFOLIO_ASSESSMENT_DEFERRED"
            assert all(s["quality_status"] == "partial" for s in data["security_research"])
            assert all(c["action"] == "defer" for c in data["synthesis"]["choices"])
        assert len(calls) == 1
        if os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1":
            from helpers.controlled_browser import verify_controlled_browser

            await verify_controlled_browser(
                migrated_database_url,
                api_now=now,
                cycle_id=args["cycle_id"],
                order_ids=(),
                candidate_count=3,
                expected_defer_count=3,
                deferred_closeout=True,
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prior", ["researching", "risk_evaluated", "awaiting_market", "partially_filled"]
)
async def test_deferred_code_cannot_use_unvalidated_old_transitions(migrated_database_url, prior):
    engine, sessions, args = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            states = [
                "researching",
                "decision_recorded",
                "risk_evaluated",
                "awaiting_market",
                "partially_filled",
            ]
            for previous, following in zip(states, states[1:], strict=False):
                if previous == prior:
                    break
                await agent_cycle._append_event(session, cycle, previous, following, "FIXTURE", {})
        with pytest.raises(DBAPIError, match="event chain is invalid"):
            async with sessions.begin() as session:
                cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
                await agent_cycle._append_event(
                    session,
                    cycle,
                    prior,
                    "completed",
                    "PORTFOLIO_ASSESSMENT_DEFERRED",
                    {"portfolio_synthesis_output_id": "00000000-0000-4000-8000-000000000001"},
                    occurred_at=args["now"],
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_guard_uses_same_account_lock(migrated_database_url):
    # Exercise the real guard on an isolated probe table so unrelated strategy
    # authority checks cannot hide a failure to take the cycle's account lock.
    engine, sessions, args = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            account_id = cycle.account_id
            await session.execute(
                text("CREATE TABLE deferred_guard_probe(agent_paper_cycle_id uuid)")
            )
            await session.execute(
                text("""CREATE TRIGGER deferred_guard_probe
                BEFORE INSERT ON deferred_guard_probe FOR EACH ROW
                EXECUTE FUNCTION nexus_deferred_assessment_execution_guard()""")
            )
        key = {"key": f"nexus:agent-paper-cycle:{account_id}"}
        async with sessions.begin() as owner:
            await owner.execute(
                text("INSERT INTO deferred_guard_probe VALUES (:id)"), {"id": args["cycle_id"]}
            )
            async with sessions.begin() as contender:
                assert not await contender.scalar(
                    text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))"), key
                )
        async with sessions.begin() as contender:
            assert await contender.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))"), key
            )
    finally:
        await engine.dispose()
