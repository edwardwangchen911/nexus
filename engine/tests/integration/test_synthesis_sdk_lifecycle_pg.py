"""Normal synthesis Worker and SDK lifecycle with isolated DB and SDK client doubles.

Only the synthetic fixture model identity is allowed by the test. Request
projection, invoke_codex_portfolio, SDK timer/cleanup, leases and terminal writes
are product code. These tests do not establish real provider timing or quality.
"""

import asyncio
import json
from datetime import datetime, timedelta

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.jobs.repository import LeaseLostError
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _sdk
from unit.agents.test_portfolio_synthesis_worker import _decode_portfolio_view


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        "success",
        "checkpoint_extended",
        "sdk_timeout",
        "incomplete",
        "session_expired",
        "lease_expired",
        "terminal_lease_expired",
        "terminal_session_expired",
        "finalize_session_expired",
        "finalize_lease_expired",
        "preterminal_session_expired",
        "cancelled",
    ],
)
async def test_normal_synthesis_sdk_terminal_and_lease_boundaries(
    migrated_database_url, monkeypatch, outcome
):
    from nexus import worker
    from nexus.agents import portfolio_synthesis_worker

    engine, sessions, options = await _seed_normal_synthesis(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS
    )
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **options)).run
            run_id = run.id
            raw = synthesis_intent(run).model_dump_json()
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run_id))
            )
            reserved = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord.reserved_usd).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
                )
            )
        now = [options["now"] + timedelta(seconds=1)]
        if outcome.endswith("session_expired"):
            now[0] = datetime.fromisoformat(run.frozen_input["expires_at"]) - timedelta(seconds=1)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return now[0]

        monkeypatch.setattr(worker, "datetime", Clock)
        monkeypatch.setattr(worker, "session_factory", lambda settings: sessions)
        if outcome == "checkpoint_extended":
            original_checkpoint = portfolio_synthesis_worker.checkpoint_job

            async def delayed_checkpoint(*args, **kwargs):
                now[0] += timedelta(seconds=95)
                return await original_checkpoint(*args, **(kwargs | {"now": now[0]}))

            monkeypatch.setattr(portfolio_synthesis_worker, "checkpoint_job", delayed_checkpoint)
        if outcome in {"terminal_lease_expired", "terminal_session_expired"}:
            original_persist = portfolio_synthesis_worker.persist_portfolio_synthesis_terminal

            async def delayed_terminal(*args, **kwargs):
                stored = await original_persist(*args, **kwargs)
                now[0] += timedelta(seconds=121 if outcome == "terminal_lease_expired" else 2)
                return stored

            monkeypatch.setattr(
                portfolio_synthesis_worker,
                "persist_portfolio_synthesis_terminal",
                delayed_terminal,
            )
        if outcome in {
            "finalize_session_expired",
            "preterminal_session_expired",
            "finalize_lease_expired",
        }:
            original_finalize = portfolio_synthesis_worker.execute_and_finalize_job
            delayed = False

            async def delayed_finalize(*args, **kwargs):
                nonlocal delayed
                first = not delayed
                delayed = True
                if first and outcome == "preterminal_session_expired":
                    now[0] += timedelta(seconds=2)
                result = await original_finalize(*args, **kwargs)
                if first and outcome in {"finalize_session_expired", "finalize_lease_expired"}:
                    now[0] += timedelta(seconds=121 if outcome == "finalize_lease_expired" else 2)
                return result

            monkeypatch.setattr(
                portfolio_synthesis_worker, "execute_and_finalize_job", delayed_finalize
            )
        monkeypatch.setattr(
            portfolio_synthesis_worker.CodexSynthesisTransport,
            "supports",
            lambda self, model: model.model_id == "fixture-agent-paper",
        )
        client, thread, response, _ = _sdk(monkeypatch)
        response.final_response = raw
        response.usage.total.input_tokens = 10
        response.usage.total.output_tokens = 20
        if outcome == "incomplete":
            response.final_response = None
        closed = asyncio.Event()
        client.close.side_effect = closed.set
        timers = []
        original_timeout = asyncio.timeout

        def observe_timeout(delay):
            timer = original_timeout(delay)
            if delay == 45:
                timers.append(timer)
            return timer

        monkeypatch.setattr(asyncio, "timeout", observe_timeout)
        dispatched = []

        async def turn(wire, **kwargs):
            document = json.loads(wire)
            frozen = _decode_portfolio_view(document["frozen_input"])
            assert len(frozen["security_outputs"]) == 3
            assert document["input_hash"] == run.input_hash
            assert len(wire.encode()) <= 64000
            async with sessions() as session:
                assert (
                    await session.get(AgentPortfolioSynthesisRunRecord, run_id)
                ).status == "running"
                assert (await session.get(Job, job_id)).attempt_count == 1
                lease = await session.scalar(select(JobLease).where(JobLease.job_id == job_id))
                assert lease.expires_at - lease.heartbeat_at == timedelta(
                    seconds=30 if outcome == "checkpoint_extended" else 120
                )
            dispatched.append(1)
            if outcome == "sdk_timeout":
                # Deterministic expiration of the real timer; no subsecond sleep.
                assert len(timers) == 1
                timers[0].reschedule(asyncio.get_running_loop().time())
                await closed.wait()
            elif outcome == "cancelled":
                task.cancel()
                await closed.wait()
            elif outcome == "session_expired":
                now[0] += timedelta(seconds=2)
            elif outcome == "checkpoint_extended":
                now[0] += timedelta(seconds=28)
            elif outcome == "lease_expired":
                now[0] += timedelta(seconds=121)
            return response

        thread.run.side_effect = turn
        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        task = asyncio.create_task(
            worker.run_portfolio_synthesis_worker_once(settings, job_ids=(job_id,))
        )
        if outcome in {
            "lease_expired",
            "terminal_lease_expired",
            "finalize_lease_expired",
            "cancelled",
        }:
            with pytest.raises(
                asyncio.CancelledError if outcome == "cancelled" else LeaseLostError
            ):
                await task
            async with sessions() as session:
                assert (
                    await session.get(AgentPortfolioSynthesisRunRecord, run_id)
                ).status == "running"
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AgentPortfolioSynthesisOutputRecord)
                        .where(AgentPortfolioSynthesisOutputRecord.run_id == run_id)
                    )
                    == 0
                )
            if outcome == "cancelled":
                now[0] += timedelta(seconds=121)
            if outcome == "terminal_lease_expired":
                monkeypatch.setattr(
                    portfolio_synthesis_worker,
                    "persist_portfolio_synthesis_terminal",
                    original_persist,
                )
        else:
            assert await task
        client.close.assert_awaited_once()
        thread.run.assert_awaited_once()
        # Lost lease/cancellation reconciles facts; it must never dispatch again.
        assert not await worker.run_portfolio_synthesis_worker_once(settings, job_ids=(job_id,))
        assert dispatched == [1]
        thread.run.assert_awaited_once()
        async with sessions() as session:
            stored = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == run_id
                )
            )
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
                )
            )
            job = await session.get(Job, job_id)
            unknown = outcome in {
                "sdk_timeout",
                "lease_expired",
                "terminal_lease_expired",
                "finalize_lease_expired",
                "cancelled",
            }
            assert (
                stored.status
                == output.status
                == ("completed" if outcome in {"success", "checkpoint_extended"} else "failed")
            )
            assert reservation.reserved_usd == reserved
            assert reservation.state == ("reserved" if unknown else "settled")
            assert (stored.input_tokens, stored.output_tokens) == (
                (None, None) if unknown else (10, 20)
            )
            if outcome not in {"success", "checkpoint_extended"}:
                assert output.intent is None
            if unknown:
                assert stored.settled_cost_usd is None
            assert job.attempt_count == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == job_id)
                )
                == 1
            )
            if outcome == "sdk_timeout":
                assert job.last_error_code == "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
            if outcome.endswith("session_expired"):
                assert job.last_error_code == "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
            if outcome in {
                "lease_expired",
                "terminal_lease_expired",
                "finalize_lease_expired",
                "cancelled",
            }:
                assert job.status == "dead_letter"
    finally:
        await engine.dispose()
