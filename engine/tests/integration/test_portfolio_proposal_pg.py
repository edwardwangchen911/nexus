from datetime import timedelta

import pytest
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis, synthesis_intent
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.agents.portfolio_synthesis_worker import read_synthesis_job, run_synthesis_job_once
from nexus.agents.responses_budget import UsageSnapshot
from nexus.paper.agent_decision_store import load_portfolio_proposal, persist_portfolio_proposal
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from sqlalchemy import func, select


async def proposal_source(
    url, *, approve=True, seeded=None, intent_factory=synthesis_intent, approval_offset=0
):
    engine, sessions, kwargs = seeded or await seed_synthesis(url)
    async with sessions.begin() as session:
        cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
        if approve:
            approval_time = cycle.decision_time
            approval_hash = kwargs["projection_hash"]
            if approval_offset:
                from nexus.paper.projection import load_paper_projection

                approved_projection = await load_paper_projection(
                    session,
                    account_id=cycle.account_id,
                    as_of=cycle.decision_time + timedelta(seconds=approval_offset),
                )
                approval_time, approval_hash = (
                    approved_projection.as_of,
                    approved_projection.projection_hash,
                )
            await approve_initial_target_projection(
                session,
                account_id=cycle.account_id,
                projection_as_of=approval_time,
                expected_projection_hash=approval_hash,
                approved_by="local-user",
                approved_at=approval_time if approval_offset < 0 else kwargs["now"],
                idempotency_key="first-baseline",
            )
        admission = await admit_portfolio_synthesis(session, **kwargs)
        raw = intent_factory(admission.run).model_dump_json()
        job = await read_synthesis_job(session, cycle_id=kwargs["cycle_id"])
        assert job is not None

    class Transport:
        def supports(self, model):
            return True

        async def invoke(self, payload):
            return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

    assert await run_synthesis_job_once(
        sessions,
        transport=Transport(),
        worker_id="proposal-test",
        job_ids=(job.id,),
        clock=lambda: kwargs["now"] + timedelta(seconds=1),
    )
    async with sessions() as session:
        output_id = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord.id).where(
                AgentPortfolioSynthesisOutputRecord.run_id == admission.run.id
            )
        )
        assert output_id is not None
    return (
        engine,
        sessions,
        dict(
            cycle_id=kwargs["cycle_id"],
            synthesis_output_id=output_id,
            now=kwargs["now"] + timedelta(seconds=2),
        ),
    )


@pytest.mark.asyncio
async def test_approval_one_second_before_cycle_remains_authoritative(migrated_database_url):
    from nexus.paper.agent_cycle import _events, _resume_research

    engine, sessions, args = await proposal_source(migrated_database_url, approval_offset=-1)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            result = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert result.result_code == "NO_ORDER_REQUIRED"
            stored = await load_portfolio_proposal(
                session, account_id=cycle.account_id, decision_id=result.strategy_decision_id
            )
            assert stored.proposal.baseline.initial_approval_id is not None
            assert stored.proposal.cash_weight == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_proposal_persists_exact_full_target_replay_and_no_execution(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            ledger_count = await session.scalar(
                select(func.count()).select_from(PaperLedgerTransactionRecord)
            )
            first = await persist_portfolio_proposal(session, **args)
        async with sessions.begin() as session:
            replay = await persist_portfolio_proposal(session, **args)
            read = await load_portfolio_proposal(
                session, account_id=first.account_id, decision_id=first.decision_id
            )
            assert first == replay == read
            assert first.proposal.cash_weight == 1
            assert all(weight == 0 for weight in first.proposal.weights.values())
            assert first.proposal.baseline.initial_approval_id is not None
            from nexus.persistence.models.paper import StrategyDecisionAgentOutputRecord

            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionAgentOutputRecord)
                    .where(
                        StrategyDecisionAgentOutputRecord.strategy_decision_id == first.decision_id
                    )
                )
                == 2
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionRecord)
                    .where(StrategyDecisionRecord.agent_paper_cycle_id == args["cycle_id"])
                )
                == 1
            )
            for model in (
                PaperTargetVersionRecord,
                PaperOrderBatchRecord,
                PaperOrderRecord,
                PaperFillRecord,
                PaperReservationRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(PaperLedgerTransactionRecord))
                == ledger_count
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["missing_approval", "expired", "changed_content"])
async def test_proposal_fail_closed_authority(migrated_database_url, kind):
    engine, sessions, args = await proposal_source(
        migrated_database_url, approve=kind != "missing_approval"
    )
    try:
        if kind == "expired":
            args["now"] += timedelta(days=1)
        async with sessions.begin() as session:
            if kind == "changed_content":
                first = await persist_portfolio_proposal(session, **args)
                args["proposal"] = first.proposal.model_copy(update={"cash_weight": "0.9"})
            with pytest.raises(ValueError, match="POLICY_UNCONFIRMED|EXPIRED|CONFLICT"):
                await persist_portfolio_proposal(session, **args)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("approved", [True, False])
async def test_cycle_handoff_is_recorded_once_and_never_executes(migrated_database_url, approved):
    from nexus.paper.agent_cycle import _events, _resume_batch, _resume_portfolio_synthesis

    engine, sessions, args = await proposal_source(migrated_database_url, approve=approved)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"],
                daily_limit_usd=None,
            )
            assert result.result_code == ("NO_ORDER_REQUIRED" if approved else "POLICY_UNCONFIRMED")
            assert result.status == ("completed" if approved else "researching")
            if approved:
                count = len(await _events(session, cycle.id))
                replay = await _resume_batch(
                    session, cycle=cycle, events=await _events(session, cycle.id)
                )
                assert replay.result_code == "NO_ORDER_REQUIRED"
                assert replay.strategy_decision_id == result.strategy_decision_id
                assert len(await _events(session, cycle.id)) == count
            assert await session.scalar(
                select(func.count()).select_from(PaperTargetVersionRecord)
            ) == (1 if approved else 0)
            for model in (PaperOrderBatchRecord, PaperOrderRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_terminal", [False, True])
async def test_persisted_policy_block_survives_restart_and_next_slot(
    migrated_database_url,
    legacy_terminal,
):
    from dataclasses import replace

    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.paper.agent_cycle import _append_event, _events, _resume_portfolio_synthesis
    from nexus.paper.agent_cycle_authority import ServiceCalendarAuthority
    from nexus.persistence.models.runtime import Job

    engine, sessions, args = await proposal_source(migrated_database_url, approve=False)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            original_events = len(await _events(session, cycle.id))
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"],
                daily_limit_usd=None,
            )
            assert result.result_code == "POLICY_UNCONFIRMED"
            assert result.status == "researching"
            assert len(await _events(session, cycle.id)) == original_events
            if legacy_terminal:
                await _append_event(
                    session,
                    cycle,
                    "researching",
                    "unavailable",
                    "POLICY_UNCONFIRMED",
                    {"portfolio_synthesis_output_id": str(args["synthesis_output_id"])},
                )
            original_jobs = set(await session.scalars(select(Job.id)))
            original_cycles = set(await session.scalars(select(AgentPaperCycleRecord.id)))
        trigger_calls = []
        for at in (
            args["now"],
            args["now"] + timedelta(seconds=1),
            args["now"] + timedelta(days=1),
        ):
            calendar = ServiceCalendarAuthority(
                calendar_authority_id=CALENDAR_ID,
                as_of=at,
                local_date=at.date(),
                session=at.date(),
                decision_window_start_at=at.replace(hour=11, minute=0, second=0),
                market_open_at=at.replace(hour=13, minute=30, second=0),
                market_close_at=at.replace(hour=22, minute=0, second=0),
                next_session=(at + timedelta(days=1)).date(),
                next_decision_window_start_at=at.replace(hour=11, minute=0, second=0)
                + timedelta(days=1),
                next_market_close_at=at.replace(hour=22, minute=0, second=0) + timedelta(days=1),
            )
            async with sessions() as session:
                state = await loop._inspect_portfolio_batch(
                    session,
                    account_id=cycle.account_id,
                    now=at,
                    calendar=calendar,
                    performance_due_at=None,
                )
                assert state is not None
                assert state.cycle_result_code == "POLICY_UNCONFIRMED"
                assert state.cycle_id == cycle.id
                assert loop.choose_loop_action(state) == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
                # Also inspect at the next day's normal 10:00 NY decision instant.
                assert (
                    loop.choose_loop_action(
                        replace(
                            state,
                            now=at.replace(hour=14, minute=0, second=0),
                        )
                    )
                    == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
                )
            waited = False

            async def inspect():
                return replace(state, enabled=not waited)

            async def execute(*_args):
                trigger_calls.append(_args)
                pytest.fail("policy-blocked host must not dispatch any provider, Job or new cycle")

            async def sleep(_seconds):
                nonlocal waited
                waited = True

            async def heartbeat(_now):
                pass

            assert (
                await loop.run_foreground_loop(
                    inspect=inspect,
                    execute=execute,
                    sleep=sleep,
                    heartbeat=heartbeat,
                )
                == loop.AgentPaperLoopAction.STOP_DISABLED
            )
        assert trigger_calls == []
        if legacy_terminal:
            from nexus.paper.projection import load_paper_projection

            async with sessions.begin() as session:
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
                    approved_at=at,
                    idempotency_key="legacy-terminal-approval",
                )
            async with sessions() as session:
                state = await loop._inspect_portfolio_batch(
                    session,
                    account_id=cycle.account_id,
                    now=at,
                    calendar=calendar,
                    performance_due_at=None,
                )
                assert state is not None
                assert state.cycle_status == "unavailable"
                assert state.cycle_result_code == "POLICY_UNCONFIRMED"
                assert loop.choose_loop_action(state) == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
        async with sessions() as session:
            assert set(await session.scalars(select(Job.id))) == original_jobs
            assert set(await session.scalars(select(AgentPaperCycleRecord.id))) == original_cycles
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("approval_offset", [0, -1])
async def test_approval_resumes_original_cycle_from_completed_synthesis(
    migrated_database_url, approval_offset
):
    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.paper.agent_cycle_authority import ServiceCalendarAuthority
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.runtime import Job

    engine, sessions, args = await proposal_source(migrated_database_url, approve=False)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            original_events = len(await _events(session, cycle.id))
            original_jobs = set(await session.scalars(select(Job.id)))
            original_cycles = set(await session.scalars(select(AgentPaperCycleRecord.id)))
            blocked = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert blocked.result_code == "POLICY_UNCONFIRMED"
            assert blocked.status == "researching"
            assert len(await _events(session, cycle.id)) == original_events
            projection = await load_paper_projection(
                session,
                account_id=cycle.account_id,
                as_of=cycle.decision_time + timedelta(seconds=approval_offset),
            )
            await approve_initial_target_projection(
                session,
                account_id=cycle.account_id,
                projection_as_of=projection.as_of,
                expected_projection_hash=projection.projection_hash,
                approved_by="local-user",
                approved_at=args["now"],
                idempotency_key="approval-after-policy-wait",
            )
        at = args["now"] + timedelta(seconds=1)
        calendar = ServiceCalendarAuthority(
            calendar_authority_id=CALENDAR_ID,
            as_of=at,
            local_date=at.date(),
            session=at.date(),
            decision_window_start_at=cycle.decision_time,
            market_open_at=cycle.decision_time,
            market_close_at=cycle.decision_time + timedelta(hours=1),
            next_session=(at + timedelta(days=1)).date(),
            next_decision_window_start_at=cycle.decision_time + timedelta(days=1),
            next_market_close_at=cycle.decision_time + timedelta(days=1, hours=1),
        )
        async with sessions.begin() as session:
            snapshot = await loop._inspect_portfolio_batch(
                session,
                account_id=cycle.account_id,
                now=at,
                calendar=calendar,
                performance_due_at=None,
            )
            assert snapshot is not None
            assert snapshot.cycle_id == cycle.id
            assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RESUME
            resumed = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=at,
            )
            assert resumed.cycle_id == cycle.id
            assert resumed.status == "completed"
            assert resumed.result_code == "NO_ORDER_REQUIRED"
            assert resumed.portfolio_synthesis_output_id == args["synthesis_output_id"]
            assert len(await _events(session, cycle.id)) == original_events + 2
            assert set(await session.scalars(select(Job.id))) == original_jobs
            assert set(await session.scalars(select(AgentPaperCycleRecord.id))) == original_cycles
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("close_hour", [20, 17], ids=["normal-close", "half-day"])
@pytest.mark.parametrize("legacy_terminal", [False, True])
async def test_policy_wait_publishes_one_persisted_close_and_summary(
    migrated_database_url, monkeypatch, tmp_path, close_hour, legacy_terminal
):
    from types import SimpleNamespace

    from helpers.paper_batch import CALENDAR_ID
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
    from nexus.ops import agent_paper_loop as loop
    from nexus.paper.agent_cycle import _append_event
    from nexus.persistence.models.paper import PaperPerformanceSnapshotRecord
    from nexus.persistence.models.runtime import Job, OutboxEvent
    from nexus.settings import EngineSettings
    from sqlalchemy import literal, text

    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url, approve=False, seeded=seeded
    )
    try:
        close = args["now"].replace(hour=close_hour, minute=0, second=0)
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
            )
            await session.execute(
                text(
                    "UPDATE exchange_calendar_sessions SET market_close_at=:close, "
                    "decision_at=:close WHERE calendar_authority_id=:calendar AND session=:day"
                ),
                {"close": close, "calendar": CALENDAR_ID, "day": cycle.decision_session},
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
            )
            if legacy_terminal:
                await _append_event(
                    session,
                    cycle,
                    "researching",
                    "unavailable",
                    "POLICY_UNCONFIRMED",
                    {"portfolio_synthesis_output_id": str(args["synthesis_output_id"])},
                )
            jobs = set(await session.scalars(select(Job.id)))
            cycles = set(await session.scalars(select(AgentPaperCycleRecord.id)))

        settings = EngineSettings(database_url=migrated_database_url)
        monkeypatch.setattr(
            loop, "func", SimpleNamespace(clock_timestamp=lambda: literal(close), max=func.max)
        )
        monkeypatch.setattr(loop, "local_settings", lambda *a, **k: settings)
        publications = []
        for _ in range(3):
            state = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert state.cycle_result_code == "POLICY_UNCONFIRMED"
            action = loop.choose_loop_action(state)
            if not publications:
                assert action == loop.AgentPaperLoopAction.PUBLISH_PERFORMANCE
                assert state.performance_due_at == close
                await loop.execute_agent_paper_loop_action(
                    settings,
                    account_id=cycle.account_id,
                    repo_root=tmp_path,
                    action=action,
                    snapshot=state,
                )
                publications.append(close)
            else:
                assert action == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(PaperPerformanceSnapshotRecord)
                    .where(
                        PaperPerformanceSnapshotRecord.account_id == cycle.account_id,
                        PaperPerformanceSnapshotRecord.as_of == close,
                    )
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(
                        OutboxEvent.event_type == "paper.session.summary.ready",
                    )
                )
                == 1
            )
            assert set(await session.scalars(select(Job.id))) == jobs
            assert set(await session.scalars(select(AgentPaperCycleRecord.id))) == cycles
        assert publications == [close]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("rejection", ["EXIT_TURNOVER_CONFLICT", "PORTFOLIO_DEPENDENCY_CONFLICT"])
async def test_host_inspection_leaves_legal_exit_allocation_rejection_to_coordinator(
    migrated_database_url, monkeypatch, tmp_path, rejection
):
    """Real PG source/worker/host/coordinator; a synthetic frozen 15% exposure boundary."""
    from datetime import datetime
    from decimal import Decimal
    from types import SimpleNamespace

    from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
    from integration import test_portfolio_synthesis_persistence_pg as seed_module
    from nexus.agents import portfolio_synthesis_persistence as persistence
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.paper import agent_cycle, agent_decision_store, target_service
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.runtime import Job
    from nexus.risk.canonical import canonical_paper_projection_hash
    from nexus.risk.models import PaperPortfolioProjection, PaperPositionBalance
    from nexus.settings import EngineSettings
    from sqlalchemy import literal

    async def exposure(session, **kwargs):
        original = await load_paper_projection(session, **kwargs)
        data = original.model_dump()
        data.update(
            available_cash=Decimal("8500"),
            positions=(
                PaperPositionBalance(
                    instrument_id=INSTRUMENTS[0][0],
                    quantity=Decimal(15),
                    reserved_quantity=Decimal(0),
                    cost_basis=Decimal(1500),
                    reference_close=Decimal(100),
                ),
            ),
        )
        projection = PaperPortfolioProjection.model_validate(data)
        data["projection_hash"] = canonical_paper_projection_hash(projection)
        return PaperPortfolioProjection.model_validate(data)

    for module in (seed_module, persistence, agent_cycle, agent_decision_store, target_service):
        monkeypatch.setattr(module, "load_paper_projection", exposure)
    original_intent = seed_module._safe_no_trade_intent

    def complete_hold(**kwargs):
        source = original_intent(**kwargs)
        assessment = source.synthesis_assessment.model_copy(
            update={
                "stance": "hold",
                "review_conclusion": "sufficient",
                "reviews": tuple(
                    review.model_copy(
                        update={
                            "resolved": True,
                            "status": "accepted",
                            "missing_evidence": (),
                            "prevented_conclusion": None,
                        }
                    )
                    for review in source.synthesis_assessment.reviews
                ),
            }
        )
        return source.model_copy(
            update={
                "quality_status": "complete",
                "stance": "hold",
                "exposure_action": "maintain",
                "evidence_audit": "complete",
                "synthesis_assessment": assessment,
                "role_votes": tuple(
                    vote.model_copy(update={"vote": "support"}) for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_hold)

    def exit_intent(run):
        intent = synthesis_intent(run)
        choices = list(intent.choices)
        if rejection == "EXIT_TURNOVER_CONFLICT":
            choices[0] = choices[0].model_copy(update={"action": "exit", "role": "core"})
        else:
            choices[0] = choices[0].model_copy(
                update={"depends_on_instrument_ids": (choices[1].instrument_id,)}
            )
            choices[1] = choices[1].model_copy(
                update={"depends_on_instrument_ids": (choices[0].instrument_id,)}
            )
        return intent.model_copy(update={"choices": tuple(choices)})

    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis

    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url, seeded=seeded, intent_factory=exit_intent
    )
    try:

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return args["now"]

        settings = EngineSettings(database_url=migrated_database_url)
        monkeypatch.setattr(
            loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(args["now"]), max=func.max),
        )
        monkeypatch.setattr(loop, "local_settings", lambda *a, **k: settings)
        monkeypatch.setattr(once, "datetime", Clock)
        async with sessions() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            jobs = set(await session.scalars(select(Job.id)))
            cycles = set(await session.scalars(select(AgentPaperCycleRecord.id)))
            count = len(await agent_cycle._events(session, cycle.id))
            # Establish the genuine deterministic rejection, rather than mock a
            # builder error. The host must not execute this computation itself.
            with pytest.raises(ValueError, match=f"^{rejection}$"):
                await agent_decision_store._rebuild_portfolio_proposal(
                    session, cycle=cycle, output_id=args["synthesis_output_id"], now=args["now"]
                )

        def forbidden_allocation(**kwargs):
            pytest.fail("inspection must not perform allocation mathematics")

        with monkeypatch.context() as inspection:
            inspection.setattr(
                agent_decision_store, "build_portfolio_proposal", forbidden_allocation
            )
            state = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
        assert loop.choose_loop_action(state) == loop.AgentPaperLoopAction.RESUME
        with pytest.raises(ValueError, match="^PORTFOLIO_PROPOSAL_UNAVAILABLE$"):
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=cycle.account_id,
                repo_root=tmp_path,
                action=loop.AgentPaperLoopAction.RESUME,
                snapshot=state,
            )
        for _ in range(2):
            restarted = await loop.inspect_agent_paper_loop(
                settings, account_id=cycle.account_id, expected_calendar_authority_id=CALENDAR_ID
            )
            assert restarted.cycle_status == "unavailable"
            assert loop.choose_loop_action(restarted) not in {
                loop.AgentPaperLoopAction.RESUME,
                loop.AgentPaperLoopAction.TRIGGER,
                loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
            }
            result = await once.resume_once(
                settings,
                once.AgentPaperResumeRequest(
                    action="resume", cycle_id=cycle.id, idempotency_key="exit-rejection-replay"
                ),
            )
            assert result.status == "unavailable"
            assert result.result_code == "PORTFOLIO_PROPOSAL_UNAVAILABLE"
        async with sessions() as session:
            events = await agent_cycle._events(session, cycle.id)
            assert len(events) == count + 1
            assert events[-1].fact_ids == {
                "portfolio_synthesis_output_id": str(args["synthesis_output_id"]),
                "proposal_rejection_code": rejection,
            }
            assert set(await session.scalars(select(Job.id))) == jobs
            assert set(await session.scalars(select(AgentPaperCycleRecord.id))) == cycles
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["source_hash", "output_hash", "approval_hash"])
async def test_host_inspection_rejects_corrupt_policy_source_bindings(
    migrated_database_url, corruption
):
    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.paper.agent_cycle_authority import ServiceCalendarAuthority
    from sqlalchemy import text

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        table, field = {
            "source_hash": ("agent_portfolio_synthesis_runs", "input_hash"),
            "output_hash": ("agent_portfolio_synthesis_outputs", "content_hash"),
            "approval_hash": ("paper_initial_target_approvals", "approval_hash"),
        }[corruption]
        async with sessions.begin() as session:
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            await session.execute(text(f"UPDATE {table} SET {field}=:value"), {"value": "f" * 64})
            await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
        async with sessions() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            calendar = ServiceCalendarAuthority(
                calendar_authority_id=CALENDAR_ID,
                as_of=args["now"],
                local_date=cycle.decision_session,
                session=cycle.decision_session,
                decision_window_start_at=cycle.decision_time,
                market_open_at=cycle.decision_time,
                market_close_at=cycle.decision_time + timedelta(hours=1),
                next_session=cycle.decision_session + timedelta(days=1),
                next_decision_window_start_at=cycle.decision_time + timedelta(days=1),
                next_market_close_at=cycle.decision_time + timedelta(days=1, hours=1),
            )
            with pytest.raises(RuntimeError, match="^AGENT_PAPER_LOOP_RESULT_UNKNOWN$"):
                await loop._inspect_portfolio_batch(
                    session,
                    account_id=cycle.account_id,
                    now=args["now"],
                    calendar=calendar,
                    performance_due_at=None,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["maintain", "reduce", "exit", "increase", "include"])
@pytest.mark.parametrize("has_active_target", [False, True])
async def test_first_synthesis_knows_actual_exposure_without_granting_target_authority(
    migrated_database_url, action, has_active_target
):
    from decimal import Decimal
    from uuid import uuid4

    from nexus.agents.portfolio_synthesis_contracts import (
        PortfolioSynthesisInputV1,
        PortfolioSynthesisIntentV1,
        canonical_portfolio_synthesis_hash,
        validate_portfolio_synthesis_intent,
    )
    from nexus.risk.canonical import canonical_paper_projection_hash
    from nexus.risk.models import PaperPortfolioProjection, PaperPositionBalance

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
            key = frozen.candidate_instrument_ids[0]
            data = frozen.projection.model_dump()
            data["positions"] = (
                PaperPositionBalance(
                    instrument_id=key,
                    quantity=Decimal(1),
                    reserved_quantity=Decimal(0),
                    cost_basis=Decimal(100),
                    reference_close=Decimal(100),
                ),
            )
            data["available_cash"] -= Decimal(100)
            projection = PaperPortfolioProjection.model_validate(data)
            data["projection_hash"] = canonical_paper_projection_hash(projection)
            projection = PaperPortfolioProjection.model_validate(data)
            doc = frozen.model_dump()
            doc["projection"] = projection
            doc["projection_hash"] = projection.projection_hash
            if has_active_target:
                # Actual drift must not rewrite the active all-cash target's semantics.
                doc.update(
                    active_target_version_id=uuid4(),
                    active_target_content_hash="a" * 64,
                    active_target_cash_weight=Decimal(1),
                )
            doc["security_outputs"][0]["intent"].update(
                quality_status="complete",
                stance="buy_candidate" if action in {"include", "increase"} else "hold",
            )
            frozen = PortfolioSynthesisInputV1.model_validate(doc)
            choice_doc = synthesis_intent(run).model_dump()
            choice_doc["input_hash"] = canonical_portfolio_synthesis_hash(frozen)
            choice_doc["choices"][0].update(
                action=action, role="core", rank=1 if action in {"include", "increase"} else None
            )
            intent = PortfolioSynthesisIntentV1.model_validate(choice_doc)
            if (action == "include") != has_active_target:
                with pytest.raises(ValueError, match="exceeds"):
                    validate_portfolio_synthesis_intent(intent, frozen)
            else:
                validate_portfolio_synthesis_intent(intent, frozen)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "activate_first", [True, False], ids=["active-target", "first-cycle-rejected"]
)
async def test_later_proposal_uses_active_target_and_preserves_historical_replay(
    migrated_database_url,
    activate_first,
):
    """Seed a second immutable research checkpoint, then use real synthesis/proposal services."""
    import json
    from copy import deepcopy
    from decimal import Decimal
    from hashlib import sha256
    from uuid import uuid4

    from nexus.paper.agent_cycle import _append_event, _events, _resume_research
    from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisRunRecord,
        AgentResearchOutputEvidenceRecord,
        AgentResearchOutputRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.runtime import Job

    def first_intent(run):
        result = synthesis_intent(run)
        if activate_first:
            return result
        choices = list(result.choices)
        choices[0] = choices[0].model_copy(
            update={"depends_on_instrument_ids": (choices[1].instrument_id,)}
        )
        choices[1] = choices[1].model_copy(
            update={"depends_on_instrument_ids": (choices[0].instrument_id,)}
        )
        return result.model_copy(update={"choices": tuple(choices)})

    engine, sessions, args = await proposal_source(
        migrated_database_url, intent_factory=first_intent
    )

    def values(record):
        return {
            column.name: deepcopy(getattr(record, column.name))
            for column in record.__table__.columns
            if column.name != "id"
        }

    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            target = None
            if activate_first:
                first = await persist_portfolio_proposal(session, **args)
                target = PaperTargetVersionRecord(
                    account_id=first.account_id,
                    portfolio_id=cycle.portfolio_id,
                    version_seq=1,
                    strategy_decision_id=first.decision_id,
                    previous_target_version_id=None,
                    target_content_hash=canonical_portfolio_target_content_hash(first.proposal),
                    activation_hash="a" * 64,
                    activated_at=args["now"],
                    created_at=args["now"],
                )
                session.add(target)
                await session.flush()
            else:
                rejected = await _resume_research(
                    session,
                    cycle=cycle,
                    authority=None,
                    events=await _events(session, cycle.id),
                    now=args["now"],
                )
                assert rejected.result_code == "PORTFOLIO_PROPOSAL_UNAVAILABLE"
                assert rejected.status == "unavailable"
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(StrategyDecisionRecord)
                        .where(StrategyDecisionRecord.agent_paper_cycle_id == cycle.id)
                    )
                    == 0
                )
            shift = timedelta(minutes=5)
            new_time = cycle.decision_time + shift
            second = AgentPaperCycleRecord(
                id=uuid4(),
                **(
                    values(cycle)
                    | {
                        "decision_time": new_time,
                        "decision_slot_at": cycle.decision_slot_at + shift,
                        "created_at": new_time,
                    }
                ),
            )
            session.add(second)
            await session.flush()
            synthesis_run = await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
                )
            )
            run_ids, output_ids = [], []
            for source_id in synthesis_run.research_output_ids:
                old_output = await session.get(AgentResearchOutputRecord, source_id)
                old_run = await session.get(AgentResearchRunRecord, old_output.run_id)
                new_run = AgentResearchRunRecord(
                    id=uuid4(),
                    **(
                        values(old_run)
                        | {
                            "decision_time": new_time,
                            "idempotency_key": f"agent-paper:{second.id}:{old_run.instrument_id}",
                            "created_at": new_time,
                        }
                    ),
                )
                session.add(new_run)
                await session.flush()
                intent_doc = deepcopy(old_output.intent)
                intent_doc["decision_time"] = new_time.isoformat()
                intent_doc["decision_id"] = str(new_run.id)
                from nexus.strategy.intents import ResearchIntent

                intent_doc = ResearchIntent.model_validate(intent_doc).model_dump(mode="json")
                content = {
                    "status": "completed",
                    "role": None,
                    "payload": old_output.payload,
                    "intent": intent_doc,
                }
                new_output = AgentResearchOutputRecord(
                    id=uuid4(),
                    **(
                        values(old_output)
                        | {
                            "run_id": new_run.id,
                            "intent": intent_doc,
                            "content_hash": sha256(
                                json.dumps(
                                    content, sort_keys=True, separators=(",", ":"), default=str
                                ).encode()
                            ).hexdigest(),
                            "created_at": new_time + timedelta(minutes=1),
                        }
                    ),
                )
                session.add(new_output)
                await session.flush()
                for evidence in await session.scalars(
                    select(AgentResearchOutputEvidenceRecord).where(
                        AgentResearchOutputEvidenceRecord.output_id == old_output.id
                    )
                ):
                    session.add(
                        AgentResearchOutputEvidenceRecord(
                            **(values(evidence) | {"output_id": new_output.id})
                        )
                    )
                old_job = await session.scalar(
                    select(Job).where(Job.payload["run_id"].as_string() == str(old_run.id))
                )
                session.add(
                    Job(
                        id=uuid4(),
                        **(
                            values(old_job)
                            | {
                                "job_key": f"second-research:{new_run.id}",
                                "payload": {"run_id": str(new_run.id)},
                                "effective_at": new_time,
                                "started_at": new_time,
                                "finished_at": new_time + timedelta(minutes=1),
                                "created_at": new_time,
                            }
                        ),
                    )
                )
                run_ids.append(new_run.id)
                output_ids.append(new_output.id)
            await session.flush()
            await _append_event(session, second, None, "prepared", "TEST_PREPARED", {})
            await _append_event(
                session,
                second,
                "prepared",
                "researching",
                "RESEARCH_JOBS_PREPARED",
                {"research_run_ids": list(map(str, run_ids))},
            )
            projection = await load_paper_projection(
                session, account_id=cycle.account_id, as_of=new_time
            )
            admitted = await admit_portfolio_synthesis(
                session,
                cycle_id=second.id,
                research_output_ids=tuple(output_ids),
                active_target_version_id=target.id if target is not None else None,
                projection_hash=projection.projection_hash,
                daily_limit_usd=Decimal(1),
                now=new_time + timedelta(minutes=2),
            )
            raw = synthesis_intent(admitted.run).model_dump_json()
            job = await read_synthesis_job(session, cycle_id=second.id)

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

        at = new_time + timedelta(minutes=2, seconds=1)
        assert await run_synthesis_job_once(
            sessions, transport=Transport(), worker_id="second", job_ids=(job.id,), clock=lambda: at
        )
        async with sessions.begin() as session:
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == admitted.run.id
                )
            )
            latest = await persist_portfolio_proposal(
                session,
                cycle_id=second.id,
                synthesis_output_id=output.id,
                now=at + timedelta(seconds=1),
            )
            if activate_first:
                assert latest.proposal.baseline.target_version_id == target.id
                assert latest.proposal.baseline.initial_approval_id is None
                assert latest.proposal.weights == first.proposal.weights
                assert latest.proposal.cash_weight == first.proposal.cash_weight
                assert await persist_portfolio_proposal(session, **args) == first
            else:
                from nexus.persistence.models.paper import PaperInitialTargetApprovalRecord

                approval = await session.scalar(select(PaperInitialTargetApprovalRecord))
                assert latest.proposal.baseline.initial_approval_id == approval.id
                assert approval.projection_as_of == cycle.decision_time < second.decision_time
                assert latest.proposal.cash_weight == 1
                assert (
                    await session.scalar(
                        select(func.count()).select_from(PaperInitialTargetApprovalRecord)
                    )
                    == 1
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(StrategyDecisionRecord)
                        .where(
                            StrategyDecisionRecord.agent_paper_cycle_id.in_((cycle.id, second.id))
                        )
                    )
                    == 1
                )
                for model in (
                    PaperTargetVersionRecord,
                    PaperOrderBatchRecord,
                    PaperOrderRecord,
                    PaperReservationRecord,
                    PaperFillRecord,
                ):
                    assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await engine.dispose()
