"""A vanished formal Worker is fenced and closed without a second model dispatch."""

from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as seed_cycle,
)
from nexus.jobs import worker
from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


class VanishedWorker(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "active",
        "expired",
        "after_close",
        "bad_binding",
        "bad_fence",
        "locked",
        "rollback",
        "manual",
    ],
)
async def test_formal_research_orphan_preserves_completed_and_unknown(
    migrated_database_url, monkeypatch, tmp_path, case
):
    original = worker.execute_and_finalize_job
    dispatches = []

    async def interrupted(session, **kwargs):
        dispatches.append(kwargs["claim"].job_id)
        if len(dispatches) == 2:
            raise VanishedWorker
        return await original(session, **kwargs)

    monkeypatch.setattr(worker, "execute_and_finalize_job", interrupted)
    with pytest.raises(VanishedWorker):
        await seed_cycle(
            migrated_database_url,
            monkeypatch,
            count=3,
            trade=False,
            profile="formal_v11",
            manual_entry=case == "manual",
            tmp_path=tmp_path,
        )
    assert len(dispatches) == 2
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            cycle_id = cycle.id
            job = await session.get(Job, dispatches[1])
            run_id = UUID(job.payload["run_id"])
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            now = lease.expires_at + timedelta(seconds=-1 if case == "active" else 1)
            if case == "after_close":
                now += timedelta(days=2)
            completed = await session.get(
                AgentResearchRunRecord,
                UUID((await session.get(Job, dispatches[0])).payload["run_id"]),
            )
            completed_before = (
                completed.status,
                completed.input_tokens,
                completed.output_tokens,
                completed.settled_cost_usd,
            )
        if case in {"active", "expired"}:
            from nexus.ops import agent_paper_loop as loop

            calendar = SimpleNamespace(
                calendar_authority_id=UUID(int=1),
                market_open_at=now.replace(hour=13, minute=30),
                decision_window_start_at=now.replace(hour=14, minute=0),
                market_close_at=now.replace(hour=20, minute=0),
                next_decision_window_start_at=now.replace(hour=14, minute=0) + timedelta(days=1),
                next_market_open_at=now.replace(hour=13, minute=30) + timedelta(days=1),
            )
            async with sessions() as session:
                snapshot = await loop._inspect_portfolio_batch(
                    session,
                    account_id=cycle.account_id,
                    now=now,
                    calendar=calendar,
                    performance_due_at=None,
                )
                assert snapshot.research_job_id == dispatches[1]
                assert snapshot.research_job_status == (
                    "running" if case == "active" else "running_expired"
                )
                assert loop.choose_loop_action(snapshot) == (
                    loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
                    if case == "active"
                    else loop.AgentPaperLoopAction.RESUME
                )
        if case == "bad_binding":
            async with sessions.begin() as session:
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                await session.execute(
                    text("UPDATE jobs SET input_hash=:hash WHERE id=:id"),
                    {"hash": "0" * 64, "id": dispatches[1]},
                )
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
        if case == "bad_fence":
            async with sessions.begin() as session:
                # Deliberately corrupt only the isolated fixture beyond the DB guard.
                await session.execute(text("ALTER TABLE job_leases DISABLE TRIGGER USER"))
                await session.execute(
                    text("UPDATE job_leases SET fence_token=fence_token+1 WHERE job_id=:id"),
                    {"id": dispatches[1]},
                )
                await session.execute(text("ALTER TABLE job_leases ENABLE TRIGGER USER"))
        if case == "rollback":
            from nexus.paper import research_orphan

            persist = research_orphan._persist_terminal

            async def fail_terminal(*args, **kwargs):
                await persist(*args, **kwargs)
                raise RuntimeError("controlled terminal rollback")

            monkeypatch.setattr(research_orphan, "_persist_terminal", fail_terminal)

        async def resume():
            async with sessions.begin() as session:
                return await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=cycle_id,
                    idempotency_key="same-orphan-resume",
                    now=now,
                    formal_stage_capacity=True,
                )

        if case in {"bad_binding", "bad_fence", "rollback"}:
            with pytest.raises(
                ValueError if case != "rollback" else RuntimeError,
                match="ORPHAN_BINDING_INVALID"
                if case != "rollback"
                else "controlled terminal rollback",
            ):
                await resume()
        elif case == "locked":
            async with sessions.begin() as owner:
                await owner.scalar(select(Job).where(Job.id == dispatches[1]).with_for_update())
                result = await resume()
                assert result.status == "researching"
        else:
            result = await resume()
            assert result.status == ("researching" if case == "active" else "unavailable")
            again = await resume()
            assert again.status == result.status
        closed = case in {"expired", "after_close", "manual"}
        async with sessions() as session:
            job = await session.get(Job, dispatches[1])
            run = await session.get(AgentResearchRunRecord, run_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run_id
                )
            )
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
            )
            assert job.attempt_count == 1 and job.manual_retry_count == 0
            assert job.status == ("dead_letter" if closed else "running")
            assert attempt.outcome == ("lost" if closed else "running")
            assert run.status == ("failed" if closed else "prepared")
            assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            assert budget.state == "reserved" and budget.settled_usd is None
            if closed:
                assert job.last_error_code == attempt.error_code == "LEASE_EXPIRED"
                assert output.status == "failed" and output.intent is None
                assert output.payload["reason"] == "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"
            else:
                assert output is None
            completed = await session.get(AgentResearchRunRecord, completed.id)
            assert (
                completed.status,
                completed.input_tokens,
                completed.output_tokens,
                completed.settled_cost_usd,
            ) == completed_before
        if case == "manual":
            from nexus.paper.manual_request import (
                close_failed_manual_request,
                require_manual_account_available,
            )
            from nexus.persistence.models.manual_paper import (
                ManualPaperRequest,
                ManualPaperRequestEvent,
            )

            async with sessions.begin() as session:
                request = await session.scalar(select(ManualPaperRequest))
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await close_failed_manual_request(session, request_id=request.id)
                receipt = await session.scalar(
                    select(ManualPaperRequestEvent)
                    .where(ManualPaperRequestEvent.request_id == request.id)
                    .order_by(ManualPaperRequestEvent.sequence.desc())
                    .limit(1)
                )
                assert receipt.status == "active"
                untouched = await session.scalar(
                    select(Job).where(
                        Job.job_type == "agent.paper.research", Job.id.not_in(dispatches)
                    )
                )
                assert untouched.status == "skipped"
                unused = await session.get(
                    AgentResearchRunRecord, UUID(untouched.payload["run_id"])
                )
                assert unused.input_tokens == unused.output_tokens == 0
                unused_budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == unused.id
                    )
                )
                assert unused_budget.state == "released"
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=cycle.account_id, now=now
                    )
        if closed:
            import httpx
            from helpers.paper_api import app, headers

            application = app(migrated_database_url, now=now)
            path = f"/internal/v1/paper/management/cycles/{cycle_id}"
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=application), base_url="http://testserver"
                ) as client:
                    response = await client.get(path, headers=headers("GET", path))
                    assert response.status_code == 200
                    payload = response.json()["data"]["cycle"]
                    assert payload["status"] == "unavailable"
                    assert payload["result_code"] == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
                    assert payload["execution_locked"] is True
            finally:
                await application.state.session_factory.kw["bind"].dispose()
        assert len(dispatches) == 2
    finally:
        await engine.dispose()
