"""Formal synthesis cancellation closes only expired, exactly bound attempts."""

from datetime import timedelta
from pathlib import Path

import pytest
from integration.test_formal_synthesis_admission import (
    AT,
)
from integration.test_formal_synthesis_admission import (
    test_formal_multistock_research_admits_exact_portfolio_synthesis as seed_formal,
)
from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport
from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


class SimulatedWorkerExit(BaseException):
    """A vanished worker cannot execute cooperative cancellation finalization."""


async def seed_running_formal_orphan(url, monkeypatch, *, seed=seed_formal):
    """Run the normal formal fixture to one dispatch, then simulate process exit.

    A caller may provide its v11 normal-chain fixture with the same seed contract.
    No SDK, host process or real model is invoked by this controlled transport.
    """
    calls = []

    async def terminated(self, payload, **kwargs):
        calls.append(1)
        raise SimulatedWorkerExit

    monkeypatch.setattr(CodexSynthesisTransport, "invoke", terminated)
    with pytest.raises(SimulatedWorkerExit):
        await seed(url, monkeypatch, count=3, trade=False, protected=False)
    return calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["active", "expired", "after_close", "bad_binding", "v2", "rollback"]
)
async def test_formal_exited_synthesis_recovers_only_expired_claim(
    migrated_database_url, monkeypatch, tmp_path, case, profile="formal_v8"
):
    expired = case != "active"
    seed = seed_formal
    if profile == "formal_v11":
        from integration.test_candidate_multistock_path import (
            test_fresh_multistock_candidate_cycle_freezes_native_jobs,
        )

        async def seed(url, patch, *, count, trade, protected):
            await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
                url, patch, count=count, trade=trade, profile="formal_v11"
            )

    calls = await seed_running_formal_orphan(migrated_database_url, monkeypatch, seed=seed)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            run = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            job = await session.scalar(
                select(Job).where(Job.job_type == "agent.portfolio_synthesis")
            )
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            assert run.status == "running"
            now = lease.expires_at + timedelta(seconds=1 if expired else -1)
            run_id, cycle_id, job_id = run.id, run.cycle_id, job.id
            if case == "after_close":
                now = AT + timedelta(days=40)
        if case in {"bad_binding", "v2"}:
            async with sessions.begin() as session:
                # Corrupt only the owned synthetic database; prove exact binding
                # rejection / a frozen future version independently of dispatch.
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                await session.execute(
                    text("UPDATE jobs SET input_hash=:value WHERE id=:id")
                    if case == "bad_binding"
                    else text("UPDATE jobs SET schema_version=:value WHERE id=:id"),
                    {
                        "id": job_id,
                        "value": "0" * 64
                        if case == "bad_binding"
                        else "portfolio_synthesis_job_v2",
                    },
                )
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
        if case == "bad_binding":
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="ORPHAN_BINDING_INVALID"):
                    await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=cycle_id,
                        idempotency_key="bad-orphan",
                        now=now,
                        formal_stage_capacity=True,
                    )
                assert (await session.get(Job, job_id)).status == "running"
                assert (
                    await session.get(AgentPortfolioSynthesisRunRecord, run_id)
                ).status == "running"
                assert await session.scalar(select(AgentPortfolioSynthesisOutputRecord)) is None
            assert calls == [1]
            return
        if case == "rollback":
            from nexus.paper import synthesis_orphan

            original_persist = synthesis_orphan.persist_portfolio_synthesis_terminal

            async def failed_terminal(*args, **kwargs):
                await original_persist(*args, **kwargs)
                raise RuntimeError("test terminal transaction failure")

            monkeypatch.setattr(
                synthesis_orphan, "persist_portfolio_synthesis_terminal", failed_terminal
            )
            with pytest.raises(RuntimeError, match="test terminal transaction failure"):
                async with sessions.begin() as session:
                    await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=cycle_id,
                        idempotency_key="rollback-orphan",
                        now=now,
                        formal_stage_capacity=True,
                    )
            async with sessions() as session:
                assert (await session.get(Job, job_id)).status == "running"
                assert (
                    await session.get(AgentPortfolioSynthesisRunRecord, run_id)
                ).status == "running"
                assert (
                    await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job_id))
                ).outcome == "running"
                assert await session.scalar(select(AgentPortfolioSynthesisOutputRecord)) is None
                reservation = await session.scalar(
                    select(AgentPortfolioSynthesisBudgetReservationRecord)
                )
                assert reservation.state == "reserved" and reservation.settled_usd is None
            assert calls == [1]
            return
        async with sessions.begin() as session:
            result = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=cycle_id,
                idempotency_key="orphan-resume",
                now=now,
                formal_stage_capacity=True,
            )
            assert result.result_code == (
                "PORTFOLIO_SYNTHESIS_UNAVAILABLE" if expired else "PORTFOLIO_SYNTHESIS_PENDING"
            )
        async with sessions() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            job = await session.get(Job, job_id)
            attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job_id))
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord)
            )
            output = await session.scalar(select(AgentPortfolioSynthesisOutputRecord))
            assert reservation.state == "reserved" and reservation.settled_usd is None
            assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            assert job.attempt_count == 1
            if expired:
                assert run.status == output.status == "failed" and output.intent is None
                from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure

                diagnostic = await read_synthesis_failure(session, run_id=run.id)
                assert diagnostic.state == "valid"
                assert diagnostic.diagnostic.observation.local == "orphan_running"
                assert diagnostic.diagnostic.attempt_no == 1
                diagnostic_attempt = await session.get(JobAttempt, diagnostic.diagnostic.attempt_id)
                assert diagnostic_attempt.job_id == job.id
                assert diagnostic_attempt.fence_token == diagnostic.diagnostic.fence_token

                assert job.status == "dead_letter" and job.last_error_code == "LEASE_EXPIRED"
                assert attempt.outcome == "lost" and attempt.error_code == "LEASE_EXPIRED"
            else:
                assert output is None and run.status == "running" and job.status == "running"
        if expired:
            now += timedelta(minutes=10)  # All completed research leases must also expire.
            from nexus.paper import synthesis_usage_acceptance as acceptance
            from nexus.persistence.models.paper import AgentPaperCycleRecord

            def stopped(*args, **kwargs):
                return None

            monkeypatch.setattr(acceptance, "require_preparation_producers_stopped", stopped)
            from nexus.paper.manual_request import (
                require_manual_account_available,
                require_settled_account_cycle_history,
            )

            async with sessions.begin() as session:
                cycle = await session.get(AgentPaperCycleRecord, cycle_id)
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=cycle.account_id, now=now
                    )
                receipt = await acceptance.accept_failed_synthesis_usage(
                    session,
                    account_id=cycle.account_id,
                    cycle_id=cycle_id,
                    run_id=run_id,
                    now=now,
                    repo_root=Path.cwd(),
                    env_file=tmp_path / "fixture.env",
                    accept_unknown_usage=True,
                )
                assert receipt.status == "accepted_usage_unknown"
                assert await acceptance.synthesis_usage_accepted(
                    session, cycle_id=cycle_id, run_id=run_id, now=now
                )
                await require_manual_account_available(
                    session, account_id=cycle.account_id, now=now
                )
                await require_settled_account_cycle_history(
                    session,
                    account_id=cycle.account_id,
                    now=now,
                    allow_scheduled_completion=True,
                )
            import httpx
            from helpers.paper_api import app, headers

            path = f"/internal/v1/paper/management/cycles/{cycle_id}"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app(migrated_database_url, now=now)),
                base_url="http://testserver",
            ) as client:
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                detail = response.json()["data"]
                assert detail["cycle"]["status"] == "unavailable"
                assert detail["synthesis"]["status"] == "failed"
                assert detail["synthesis_usage_disposition"] == dict(
                    status="accepted",
                    usage="unknown",
                    budget_preserved=True,
                    execution_allowed=False,
                )
        assert calls == [1]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["expired", "after_close", "rollback"])
async def test_formal_v11_orphan_preserves_unknown_and_next_admission(
    migrated_database_url, monkeypatch, tmp_path, case
):
    await test_formal_exited_synthesis_recovers_only_expired_claim(
        migrated_database_url, monkeypatch, tmp_path, case, profile="formal_v11"
    )
