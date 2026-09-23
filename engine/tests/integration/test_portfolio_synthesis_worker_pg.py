"""Real leases, output persistence and unknown usage with an isolated fake transport."""

from datetime import timedelta
from decimal import Decimal

import pytest
from helpers.paper_batch import INSTRUMENTS
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


async def _seed_normal_synthesis(
    url, monkeypatch, *, complete_research=True, candidate_instruments=INSTRUMENTS[:2]
):
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_portfolio_same_day_research_pg import seed_normal_path
    from nexus.paper.performance import publish_paper_performance_snapshot
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    normal = await seed_normal_path(url, monkeypatch, candidate_instruments=candidate_instruments)
    # The real host publishes a due prior close before starting new research.
    # Match that order so a late historical publication cannot alter this test's
    # already-frozen research projection during an unrelated resume assertion.
    _, sessions, authority, _ = normal
    async with sessions.begin() as session:
        prior_close = await session.scalar(
            select(func.max(ExchangeCalendarSessionRecord.market_close_at)).where(
                ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                ExchangeCalendarSessionRecord.market_close_at <= authority.decision_time,
            )
        )
        if prior_close is not None:
            await publish_paper_performance_snapshot(
                session, account_id=authority.account_id, as_of=prior_close
            )
    seeded = await seed_synthesis(
        url,
        normal_path=normal,
        complete_research=complete_research,
        candidate_instruments=candidate_instruments,
    )
    _, sessions, kwargs = seeded
    async with sessions.begin() as session:
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=(kwargs["now"] + timedelta(days=1)).date(),
                market_open_at=kwargs["now"].replace(hour=13, minute=30) + timedelta(days=1),
                market_close_at=kwargs["now"].replace(hour=20, minute=0) + timedelta(days=1),
                decision_at=kwargs["now"].replace(hour=21, minute=0) + timedelta(days=1),
                created_at=kwargs["now"],
            )
        )
    return seeded


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
                    raise CodexInvocationError(
                        "CODEX_INPUT_TOO_LARGE", phase="preflight", provider_call_started=False
                    )
                if outcome == "sdk_preflight_expired":
                    kwargs["now"] += timedelta(seconds=2)
                    raise CodexInvocationError(
                        "CODEX_INPUT_TOO_LARGE", phase="preflight", provider_call_started=False
                    )
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
@pytest.mark.parametrize("failure", ["cleanup", "input", "phase_only"])
async def test_adapter_dispatch_fact_controls_reservation(
    migrated_database_url, monkeypatch, failure
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus.agents import codex_sdk
    from nexus.agents.portfolio_synthesis_worker import (
        CodexSynthesisTransport,
        run_synthesis_job_once,
    )
    from openai_codex.generated.v2_all import ChatgptAccount

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
        thread = SimpleNamespace(run=AsyncMock(return_value=object()))
        client = SimpleNamespace(
            account=AsyncMock(
                return_value=SimpleNamespace(
                    account=SimpleNamespace(root=ChatgptAccount.model_construct())
                )
            ),
            _client=SimpleNamespace(
                request=AsyncMock(
                    return_value=SimpleNamespace(config=SimpleNamespace(model_extra={}))
                )
            ),
            thread_start=AsyncMock(return_value=thread),
            close=AsyncMock(),
        )
        monkeypatch.setattr(codex_sdk, "AsyncCodex", lambda config: client)
        original = codex_sdk.TemporaryDirectory

        class BrokenCleanup(original):
            def __exit__(self, *args):
                super().__exit__(*args)
                raise OSError("cleanup-private-canary")

        if failure == "cleanup":
            monkeypatch.setattr(codex_sdk, "TemporaryDirectory", BrokenCleanup)
        if failure == "input":
            monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *args: 1000000)

        class Transport(CodexSynthesisTransport):
            def supports(self, model):
                return True

            async def invoke(self, payload):
                if failure == "phase_only":
                    raise codex_sdk.CodexInvocationError(
                        "CODEX_UNEXPECTED_EXCEPTION", phase="preflight"
                    )
                return await super().invoke(payload)

        args = dict(
            transport=Transport(),
            worker_id="dispatch-fact-test",
            clock=lambda: kwargs["now"],
            job_ids=(job_id,),
        )
        assert await run_synthesis_job_once(sessions, **args)
        assert not await run_synthesis_job_once(sessions, **args)
        assert thread.run.await_count == (1 if failure == "cleanup" else 0)
        async with sessions() as session:
            reserve = await session.scalar(select(AgentPortfolioSynthesisBudgetReservationRecord))
            terminal = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
            job = await session.get(Job, job_id)
            assert terminal.status == job.status == "failed"
            assert reserve.state == ("released" if failure == "input" else "reserved")
            assert ("PRE_DISPATCH" in job.last_error_code) is (failure == "input")
            assert terminal.input_tokens is terminal.output_tokens is None
            if failure != "input":
                assert terminal.settled_cost_usd is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["once", "host"])
@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("already_reclaimed", [False, True])
async def test_normal_resume_reconciles_exact_expired_synthesis_claim(
    migrated_database_url, monkeypatch, tmp_path, entry, running, already_reclaimed
):
    from datetime import datetime
    from types import SimpleNamespace

    from helpers.paper_batch import CALENDAR_ID
    from nexus.agents import portfolio_synthesis_worker as synth
    from nexus.jobs.models import JobType, QueueName
    from nexus.jobs.repository import claim_next_job, reclaim_expired_jobs
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.paper.agent_cycle import _events
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.settings import EngineSettings
    from sqlalchemy import literal

    engine, sessions, kwargs = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            job_id = await session.scalar(
                select(Job.id).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="crashed-synthesis-worker",
                now=kwargs["now"],
                lease_seconds=30,
                job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
                job_ids=(job_id,),
            )
            assert claim is not None
            if running:
                run.status = "running"
        recovery_at = kwargs["now"] + timedelta(seconds=31)
        if already_reclaimed:
            async with sessions.begin() as session:
                await reclaim_expired_jobs(
                    session, queue=QueueName.GENERAL, now=recovery_at, job_ids=(job_id,)
                )
                assert (await session.get(Job, job_id)).status == "dead_letter"
        calls = []

        class NeverCall:
            def supports(self, model):
                raise AssertionError("resume must not dispatch")

            async def invoke(self, payload):
                calls.append(payload)
                raise AssertionError("resume must not call model")

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return recovery_at

        monkeypatch.setattr(synth, "CodexSynthesisTransport", NeverCall)
        monkeypatch.setattr(once, "datetime", Clock)
        monkeypatch.setattr(
            loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(recovery_at), max=func.max),
        )
        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        request = once.AgentPaperResumeRequest(
            action="resume", cycle_id=cycle.id, idempotency_key="recover-synthesis"
        )
        if not already_reclaimed:
            recovery_at = kwargs["now"] + timedelta(seconds=1)
            pending = await once.resume_once(settings, request)
            assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
            if entry == "host":
                active = await loop.inspect_agent_paper_loop(
                    settings,
                    account_id=cycle.account_id,
                    expected_calendar_authority_id=CALENDAR_ID,
                )
                assert active.research_job_status == "running"
                assert (
                    loop.choose_loop_action(active) == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
                )
            recovery_at = kwargs["now"] + timedelta(seconds=31)
        if entry == "once":
            result = await once.resume_once(settings, request)
            assert result.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
        else:
            snapshot = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert snapshot.research_job_id == job_id
            assert snapshot.research_job_status == (
                "dead_letter" if already_reclaimed else "running_expired"
            )
            action = loop.choose_loop_action(snapshot)
            assert action == loop.AgentPaperLoopAction.RESUME
            with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_UNAVAILABLE$"):
                await loop.execute_agent_paper_loop_action(
                    settings,
                    account_id=cycle.account_id,
                    repo_root=tmp_path,
                    action=action,
                    snapshot=snapshot,
                )
        replay = await once.resume_once(settings, request)
        assert replay.status == "unavailable"
        assert replay.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
        assert calls == []
        async with sessions() as session:
            terminal = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
            output = await session.scalar(select(AgentPortfolioSynthesisOutputRecord))
            reserve = await session.scalar(select(AgentPortfolioSynthesisBudgetReservationRecord))
            job = await session.get(Job, job_id)
            assert terminal.status == output.status == "failed"
            assert output.run_id == run.id
            assert job.status == "dead_letter"
            assert job.attempt_count == 1
            assert reserve.state == ("reserved" if running else "released")
            assert reserve.settled_usd == (None if running else Decimal("0"))
            assert terminal.input_tokens is terminal.output_tokens is None
            assert (await _events(session, cycle.id))[-1].to_status == "unavailable"
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
                result = await _resume_research(
                    session,
                    cycle=cycle,
                    authority=None,
                    events=await _events(session, cycle.id),
                    now=kwargs["now"],
                )
                assert result.status == "researching"
                assert result.result_code == "POLICY_UNCONFIRMED"
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
    from types import SimpleNamespace

    from helpers.paper_batch import CALENDAR_ID
    from nexus import worker
    from nexus.agents import portfolio_synthesis_worker as synth
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.settings import EngineSettings
    from sqlalchemy import literal

    engine, sessions, kwargs = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
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
        monkeypatch.setattr(once, "datetime", Clock)
        monkeypatch.setattr(
            loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(Clock.now()), max=func.max),
        )
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
            snapshot = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert snapshot.research_job_id == job_id
            assert snapshot.research_job_status == "queued"
            assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
                snapshot=snapshot,
            )
        assert len(calls) == 1
        # A disconnected caller after commit must recover facts without a second model call.
        assert not await worker.run_portfolio_synthesis_worker_once(settings, job_ids=(job_id,))
        if entry == "host":
            snapshot = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert snapshot.research_job_status == "succeeded"
            assert snapshot.cycle_result_code == "POLICY_UNCONFIRMED"
            assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
        replay = await once.resume_once(
            settings,
            once.AgentPaperResumeRequest(
                action="resume", cycle_id=cycle.id, idempotency_key="terminal-replay"
            ),
        )
        assert replay.result_code == "POLICY_UNCONFIRMED"
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


@pytest.mark.asyncio
async def test_host_from_security_jobs_through_synthesis_handoff(
    migrated_database_url, monkeypatch, tmp_path
):
    from datetime import datetime
    from types import SimpleNamespace

    from helpers.paper_batch import CALENDAR_ID
    from integration.test_agent_execution_pg import _ActionableFixtureCommittee
    from nexus import worker
    from nexus.agents import portfolio_synthesis_worker as synth
    from nexus.agents.committee import PromptAsset
    from nexus.agents.execution_authority import load_frozen_prompts
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.settings import EngineSettings
    from sqlalchemy import literal

    engine, sessions, kwargs = await _seed_normal_synthesis(
        migrated_database_url, monkeypatch, complete_research=False
    )
    try:
        async with sessions() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).order_by(AgentResearchRunRecord.instrument_id)
                )
            )
            assert len(runs) == 2
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 0
            )
        calls = []

        class Committee(_ActionableFixtureCommittee):
            def __init__(self, run_id, prompts):
                self.run_id = run_id
                self.prompts = prompts
                self.outputs = None

            async def analyze(self, role, request, evidence_refs, prompt):
                if self.outputs is None:
                    # One fake provider batch, then read cached role results.
                    calls.append(self.run_id)
                    self.outputs = {}
                    for key, asset in self.prompts.items():
                        self.outputs[key] = await super().analyze(
                            key, request, evidence_refs, asset
                        )
                return self.outputs[role]

        async def committee(session, settings, run, **kw):
            prompts = await load_frozen_prompts(session, run)
            return Committee(
                run.id,
                {
                    role: PromptAsset(
                        role=role,
                        version=item.version,
                        content=item.content,
                        content_hash=item.content_hash,
                    )
                    for role, item in prompts.items()
                },
            )

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                calls.append("synthesis")
                async with sessions() as session:
                    run = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
                    return synthesis_intent(run).model_dump_json(), UsageSnapshot(
                        input_tokens=10, output_tokens=20, provider_calls=1
                    )

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return kwargs["now"] + timedelta(seconds=1)

        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        monkeypatch.setattr(worker, "_committee", committee)
        monkeypatch.setattr(worker, "datetime", Clock)
        monkeypatch.setattr(worker, "_ONCE_STARTED_AT", kwargs["now"])
        monkeypatch.setattr(once, "datetime", Clock)
        monkeypatch.setattr(loop, "local_settings", lambda *a, **kw: settings)
        monkeypatch.setattr(synth, "CodexSynthesisTransport", Transport)
        monkeypatch.setattr(
            loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(Clock.now()), max=func.max),
        )

        async def ready():
            return None

        monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker_async", ready)
        expected = [loop.AgentPaperLoopAction.RUN_RESEARCH_JOB] * 2 + [
            loop.AgentPaperLoopAction.RESUME,
            loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            loop.AgentPaperLoopAction.WAIT_FOR_SLOT,
        ]
        for index, action in enumerate(expected):
            snapshot = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert loop.choose_loop_action(snapshot) == action
            if action == loop.AgentPaperLoopAction.WAIT_FOR_SLOT:
                assert snapshot.cycle_result_code == "POLICY_UNCONFIRMED"
                continue
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=action,
                snapshot=snapshot,
            )
        assert calls == [runs[0].id, runs[1].id, "synthesis"]
        for _ in range(2):
            replay = await once.resume_once(
                settings,
                once.AgentPaperResumeRequest(
                    action="resume", cycle_id=cycle.id, idempotency_key="full-host-replay"
                ),
            )
            assert replay.result_code == "POLICY_UNCONFIRMED"
        assert len(calls) == 3
        from nexus.paper.agent_cycle import _events
        from nexus.paper.projection import load_paper_projection
        from nexus.paper.target_service import approve_initial_target_projection

        async with sessions.begin() as session:
            original_jobs = set(await session.scalars(select(Job.id)))
            original_cycles = set(await session.scalars(select(AgentPaperCycleRecord.id)))
            prior_events = len(await _events(session, cycle.id))
            projection = await load_paper_projection(
                session,
                account_id=cycle.account_id,
                as_of=cycle.decision_time,
            )
            await approve_initial_target_projection(
                session,
                account_id=cycle.account_id,
                projection_as_of=projection.as_of,
                expected_projection_hash=projection.projection_hash,
                approved_by="local-user",
                approved_at=Clock.now(),
                idempotency_key="host-policy-approval",
            )
        snapshot = await loop.inspect_agent_paper_loop(
            settings,
            account_id=cycle.account_id,
            expected_calendar_authority_id=CALENDAR_ID,
        )
        assert snapshot.cycle_id == cycle.id
        assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RESUME
        assert (
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=loop.AgentPaperLoopAction.RESUME,
                snapshot=snapshot,
            )
            is None
        )
        async with sessions() as session:
            events = await _events(session, cycle.id)
            assert len(events) == prior_events + 2
            assert events[-2].to_status == "decision_recorded"
            assert events[-2].result_code == "TARGET_ACTIVATION_PENDING"
            assert events[-1].to_status == "completed"
            assert events[-1].result_code == "NO_ORDER_REQUIRED"
            assert set(await session.scalars(select(Job.id))) == original_jobs
            assert set(await session.scalars(select(AgentPaperCycleRecord.id))) == original_cycles
        assert len(calls) == 3
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing_intent", "invalid_hash"])
async def test_host_does_not_admit_missing_or_ineligible_security_fact(
    migrated_database_url, monkeypatch, tmp_path, invalid
):
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.settings import EngineSettings
    from sqlalchemy import literal, text

    engine, sessions, kwargs = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            # Deliberately corrupt only isolated synthetic test evidence.
            await session.execute(text("ALTER TABLE agent_research_outputs DISABLE TRIGGER USER"))
            assignment = (
                "intent=NULL" if invalid == "missing_intent" else "content_hash=repeat('a',64)"
            )
            await session.execute(
                text(f"UPDATE agent_research_outputs SET {assignment} WHERE role IS NULL")
            )
            await session.execute(text("ALTER TABLE agent_research_outputs ENABLE TRIGGER USER"))

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return kwargs["now"] + timedelta(seconds=1)

        never = AsyncMock(side_effect=AssertionError("invalid research must not call any worker"))
        monkeypatch.setattr(loop, "run_cycle_job_once", never)
        monkeypatch.setattr(once, "datetime", Clock)
        monkeypatch.setattr(
            loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(Clock.now()), max=func.max),
        )
        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        snapshot = await loop.inspect_agent_paper_loop(
            settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
        )
        assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RESUME
        with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_UNAVAILABLE$"):
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=loop.AgentPaperLoopAction.RESUME,
                snapshot=snapshot,
            )
        never.assert_not_awaited()
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 0
            )
    finally:
        await engine.dispose()
