"""Real leases, output persistence and unknown usage with an isolated fake transport."""

from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis, synthesis_intent
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        "valid",
        "invalid",
        "unknown",
        "preflight",
        "sdk_preflight",
        "sdk_preflight_expired",
        "token_limit",
        "error_token_limit",
        "expired",
    ],
)
async def test_one_call_terminal_and_exact_replay(migrated_database_url, outcome):
    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.agents.portfolio_synthesis_worker import run_synthesis_job_once

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            admitted = await admit_portfolio_synthesis(session, **kwargs)
            run_id = admitted.run.id
            raw = synthesis_intent(admitted.run).model_dump_json()
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run_id))
            )
        calls = []

        class Transport:
            def supports(self, model):
                return outcome != "preflight"

            async def invoke(self, payload):
                calls.append(payload)
                assert payload["input_hash"] == admitted.run.input_hash
                if outcome == "unknown":
                    raise CodexInvocationError("CODEX_TIMEOUT", phase="turn_run")
                if outcome == "sdk_preflight":
                    raise CodexInvocationError("CODEX_INPUT_TOO_LARGE", phase="preflight")
                if outcome == "sdk_preflight_expired":
                    kwargs["now"] += timedelta(seconds=2)
                    raise CodexInvocationError("CODEX_INPUT_TOO_LARGE", phase="preflight")
                if outcome == "token_limit":
                    return raw, UsageSnapshot(input_tokens=101, output_tokens=20, provider_calls=1)
                if outcome == "error_token_limit":
                    raise CodexInvocationError(
                        "CODEX_RESPONSE_INCOMPLETE",
                        usage=UsageSnapshot(input_tokens=101, output_tokens=20, provider_calls=1),
                    )
                if outcome == "expired":
                    kwargs["now"] += timedelta(seconds=2)
                return (
                    raw if outcome == "valid" else '{"target_weight":"0.5"}',
                    UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1),
                )

        if outcome in {"expired", "sdk_preflight_expired"}:
            kwargs["now"] += timedelta(minutes=57, seconds=58)
        arguments = dict(
            transport=Transport(),
            worker_id="synthesis-test",
            clock=lambda: kwargs["now"] + timedelta(seconds=1),
            job_ids=(job_id,),
        )
        assert await run_synthesis_job_once(sessions, **arguments)
        assert not await run_synthesis_job_once(sessions, **arguments)
        assert len(calls) == (0 if outcome == "preflight" else 1)
        async with sessions() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == run_id
                )
            )
            reserve = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
                )
            )
            job = await session.get(Job, job_id)
            assert (
                run.status
                == output.status
                == {
                    "valid": "completed",
                    "invalid": "abstained",
                    "unknown": "failed",
                    "preflight": "failed",
                    "sdk_preflight": "failed",
                    "sdk_preflight_expired": "failed",
                    "token_limit": "failed",
                    "error_token_limit": "failed",
                    "expired": "failed",
                }[outcome]
            )
            assert (
                job.status
                == {
                    "valid": "succeeded",
                    "invalid": "skipped",
                    "unknown": "failed",
                    "preflight": "failed",
                    "sdk_preflight": "failed",
                    "sdk_preflight_expired": "failed",
                    "token_limit": "failed",
                    "error_token_limit": "failed",
                    "expired": "failed",
                }[outcome]
            )
            assert (
                reserve.state
                == {
                    "valid": "settled",
                    "invalid": "settled",
                    "unknown": "reserved",
                    "preflight": "released",
                    "sdk_preflight": "released",
                    "sdk_preflight_expired": "released",
                    "token_limit": "reserved",
                    "error_token_limit": "reserved",
                    "expired": "settled",
                }[outcome]
            )
            if outcome in {"valid", "invalid"}:
                assert (run.input_tokens, run.output_tokens) == (10, 20)
                assert reserve.settled_usd == Decimal("0.00003000")
            if outcome == "unknown":
                assert run.settled_cost_usd is None
            if outcome in {"token_limit", "error_token_limit"}:
                assert job.last_error_code == "PORTFOLIO_SYNTHESIS_TOKEN_LIMIT_EXCEEDED"
                assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            if outcome in {"sdk_preflight", "sdk_preflight_expired"}:
                assert job.last_error_code == "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_PREFLIGHT"
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisOutputRecord)
                )
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cycle_admits_once_and_resumes_exact_output_without_legacy_decision(
    migrated_database_url,
):
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import AgentPaperCycleRecord, StrategyDecisionRecord

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            events = await _events(session, cycle.id)
            result = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=events,
                now=kwargs["now"],
                daily_limit_usd=Decimal("1"),
            )
            assert result.status == "researching"
            assert result.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
            run = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            assert run is not None
            run_id = run.id
        async with sessions.begin() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            run.status = "running"
            await session.flush()
            output = await persist_portfolio_synthesis_terminal(
                session,
                run_id=run.id,
                input_hash=run.input_hash,
                status="completed",
                intent=synthesis_intent(run),
                input_tokens=10,
                output_tokens=20,
                now=kwargs["now"],
            )
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            job.status = "running"
            job.attempt_count = 1
            job.started_at = kwargs["now"]
            job.version += 1
            await session.flush()
            job.status = "succeeded"
            job.finished_at = kwargs["now"]
            job.version += 1
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            events = await _events(session, cycle.id)
            for _ in range(2):
                result = await _resume_research(session, cycle=cycle, authority=None, events=events)
                assert result.status == "researching"
                assert result.result_code == "PORTFOLIO_PROPOSAL_UNAVAILABLE"
                assert result.portfolio_synthesis_output_id == output.id
            assert len(await _events(session, cycle.id)) == len(events)
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionRecord)
                    .where(StrategyDecisionRecord.strategy_version == "agent_paper_decision_v1")
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("running", [False, True])
async def test_expired_claim_is_terminalized_without_second_dispatch(
    migrated_database_url, running
):
    from nexus.agents.portfolio_synthesis_worker import run_synthesis_job_once
    from nexus.jobs.models import JobType, QueueName
    from nexus.jobs.repository import claim_next_job

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="lost-worker",
                now=kwargs["now"],
                lease_seconds=30,
                job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
                job_ids=(job_id,),
            )
            assert claim is not None
            if running:
                run.status = "running"

        class NeverCall:
            def supports(self, model):
                raise AssertionError("orphan must never be redispatched")

            async def invoke(self, payload):
                raise AssertionError("orphan must never call model")

        assert not await run_synthesis_job_once(
            sessions,
            transport=NeverCall(),
            worker_id="recover",
            clock=lambda: kwargs["now"] + timedelta(seconds=31),
            job_ids=(job_id,),
        )
        async with sessions() as session:
            recovered = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
                )
            )
            assert recovered.status == "failed"
            assert reservation.state == ("reserved" if running else "released")
            assert (await session.get(Job, job_id)).status == "dead_letter"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["budget", "missing_output", "failed", "abstained"])
async def test_cycle_unavailable_is_terminal_without_repeat_admission(
    migrated_database_url, failure
):
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        if failure != "budget":
            async with sessions.begin() as session:
                run = (await admit_portfolio_synthesis(session, **kwargs)).run
                run.status = "running"
                await session.flush()
                if failure != "missing_output":
                    await persist_portfolio_synthesis_terminal(
                        session,
                        run_id=run.id,
                        input_hash=run.input_hash,
                        status=failure,
                        intent=None,
                        input_tokens=10,
                        output_tokens=20,
                        now=kwargs["now"],
                    )
                job = await session.scalar(
                    select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
                )
                job.status = "running"
                job.started_at = kwargs["now"]
                job.attempt_count = 1
                job.version += 1
                await session.flush()
                job.status = (
                    "succeeded"
                    if failure == "missing_output"
                    else "skipped"
                    if failure == "abstained"
                    else "failed"
                )
                job.finished_at = kwargs["now"]
                job.version += 1
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            events = await _events(session, cycle.id)
            result = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=events,
                now=kwargs["now"],
                daily_limit_usd=Decimal("0.000001"),
            )
            assert result.status == "unavailable"
            assert result.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
            assert len(await _events(session, cycle.id)) == len(events) + 1
            assert await session.scalar(
                select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
            ) == (0 if failure == "budget" else 1)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["once", "host"])
async def test_product_dispatch_executes_synthesis_and_recovers_committed_output(
    migrated_database_url, monkeypatch, tmp_path, entry
):
    from datetime import datetime

    from nexus import worker
    from nexus.agents import portfolio_synthesis_worker as synth
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.settings import EngineSettings

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            result = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=kwargs["now"],
                daily_limit_usd=Decimal("1"),
            )
            assert result.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
            run = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            raw = synthesis_intent(run).model_dump_json()
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
        calls = []

        class Transport:
            def supports(self, model):
                return model.model_id == "fixture-agent-paper"

            async def invoke(self, payload):
                calls.append(payload)
                return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return kwargs["now"] + timedelta(seconds=1)

        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        monkeypatch.setattr(synth, "CodexSynthesisTransport", Transport)
        monkeypatch.setattr(worker, "datetime", Clock)
        monkeypatch.setattr(once, "local_settings", lambda *args, **kw: settings)
        monkeypatch.setattr(loop, "local_settings", lambda *args, **kw: settings)

        async def ready():
            return None

        monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker_async", ready)
        if entry == "once":
            result = await once.run_research_once(
                settings,
                once.AgentPaperRunResearchRequest(
                    action="run-research", cycle_id=kwargs["cycle_id"]
                ),
                repo_root=tmp_path,
            )
            assert result.job_id == job_id
        else:
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
                snapshot=loop.AgentPaperLoopSnapshot(
                    now=kwargs["now"],
                    cycle_id=cycle.id,
                    cycle_status="researching",
                    research_job_id=job_id,
                    research_job_status="queued",
                ),
            )
        assert len(calls) == 1
        # A disconnected caller after commit must recover facts without a second model call.
        assert not await worker.run_portfolio_synthesis_worker_once(settings, job_ids=(job_id,))
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            replay = await _resume_research(
                session, cycle=cycle, authority=None, events=await _events(session, cycle.id)
            )
            assert replay.result_code == "PORTFOLIO_PROPOSAL_UNAVAILABLE"
            assert replay.portfolio_synthesis_output_id is not None
        assert len(calls) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_pre_dispatch_claim_cannot_release_known_usage_or_rewrite_unknown_terminal(
    migrated_database_url,
):
    from nexus.agents.portfolio_synthesis_persistence import persist_portfolio_synthesis_terminal

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            run.status = "running"
            await session.flush()
            terminal = dict(
                run_id=run.id,
                input_hash=run.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=kwargs["now"],
            )
            with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_USAGE_INVALID"):
                await persist_portfolio_synthesis_terminal(
                    session,
                    **(
                        terminal
                        | {
                            "input_tokens": 1,
                            "output_tokens": 1,
                            "provider_call_started": False,
                        }
                    ),
                )
            with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_USAGE_INVALID"):
                await persist_portfolio_synthesis_terminal(
                    session,
                    **(
                        terminal
                        | {
                            "status": "completed",
                            "intent": synthesis_intent(run),
                            "provider_call_started": False,
                        }
                    ),
                )
            output = await persist_portfolio_synthesis_terminal(session, **terminal)
            replay = await persist_portfolio_synthesis_terminal(
                session, **terminal, provider_call_started=False
            )
            assert output.id == replay.id
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                    AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
                )
            )
            assert reservation.state == "reserved"
            assert run.settled_cost_usd is None
    finally:
        await engine.dispose()
