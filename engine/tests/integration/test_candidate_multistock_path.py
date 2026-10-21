"""Fresh dated candidate scenarios; these are controlled facts, not live acceptance."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_candidate_research_authority import OPTIONS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
FIVE = INSTRUMENTS + (
    (UUID(int=0x51000000000070008000000000000014), Decimal("70"), Decimal("0"), "industrials"),
    (UUID(int=0x51000000000070008000000000000015), Decimal("80"), Decimal("0"), "consumer_staples"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
@pytest.mark.parametrize("profile", ["v7", "v9", "formal_v9", "formal_v10"])
async def test_fresh_multistock_candidate_cycle_freezes_native_jobs(
    migrated_database_url,
    monkeypatch,
    count,
    trade,
    profile,
    manual_entry=False,
    tmp_path=None,
    fault=None,
    recover_unknown=False,
    decision_delay=None,
    sdk_lifecycle_case=None,
):
    if sdk_lifecycle_case == "legacy_success":
        # Reproduce an old immutable admission through its original v1 version;
        # no historical row is rewritten and the frozen request stays identical.
        monkeypatch.setattr(
            "nexus.agents.portfolio_synthesis_persistence.BOUNDED_SYNTHESIS_JOB",
            "portfolio_synthesis_job_v1",
        )
    post_research_at = AT + (decision_delay or timedelta())
    formal = profile in {"formal_v9", "formal_v10", "formal_v11"}
    consumer_mode = (
        {"formal_stage_capacity": True} if formal else {"candidate_stage_capacity": True}
    )
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        candidate_instruments=FIVE[:count],
        decision_time=AT,
    )
    calls = []

    clock_at = [AT + timedelta(seconds=1)]

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            at = clock_at[0]
            return at if tz is None else at.astimezone(tz)

    if formal and not manual_entry:
        from sqlalchemy.ext.asyncio import AsyncSession

        original_scalar = AsyncSession.scalar

        async def database_clock(self, statement, *args, **kwargs):
            # This dated scenario controls both clocks; a live database clock
            # would correctly reject admission after its historical session closed.
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return clock_at[0]
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", database_clock)

    async def invoke(payload, **kwargs):
        stage = payload["stage"]
        calls.append(stage)
        assert kwargs["timeout_seconds"] > 0
        if fault == "research_unknown" and stage == "critique":
            raise TimeoutError("controlled unknown model result")
        if stage == "synthesis":
            response = {
                "synthesis_assessment": {
                    "stance": "buy_candidate" if trade else "watch",
                    "claims": [{"text": "受控来源支持本场景保持观察。", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": claim,
                            "status": "accepted",
                            "rationale": "冻结证据已核对。",
                            "citations": [0],
                            "resolved": True,
                            "missing_evidence": [],
                            "prevented_conclusion": None,
                        }
                        for claim in ("bear:0", "evidence_audit:0")
                    ],
                    "review_conclusion": "sufficient",
                    "uncertainties": [],
                    "conflicting_facts": False,
                }
            }
        else:
            response = {
                "roles": [
                    {
                        "role": prompt["role"],
                        "vote": "support",
                        "claims": [{"text": "受控证据引用在冻结输入中。", "citations": [0]}],
                        "reviewed_claim_ids": [] if stage == "research" else ["bull:0"],
                        "conflicting_facts": False,
                    }
                    for prompt in payload["prompts"]
                ]
            }
        return json.dumps(response), UsageSnapshot(input_tokens=100, output_tokens=100)

    monkeypatch.setattr("nexus.worker._ONCE_STARTED_AT", AT)
    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            if profile != "v7":
                from nexus.agents.codex_capacity import capacity_config, capacity_config_hash
                from nexus.agents.codex_flexible_budget import (
                    flexible_stage_config,
                    flexible_stage_config_hash,
                )

                if profile == "formal_v11":
                    from nexus.agents.codex_bounded_budget import (
                        bounded_stage_config,
                        bounded_stage_config_hash,
                    )

                    config, config_hash = bounded_stage_config(), bounded_stage_config_hash()
                elif profile == "formal_v10":
                    config, config_hash = flexible_stage_config(), flexible_stage_config_hash()
                else:
                    config, config_hash = capacity_config(), capacity_config_hash()
                session.add(
                    AgentModelConfigRecord(
                        model_id=config["model_id"],
                        version=config["version"],
                        config_hash=config_hash,
                        max_input_tokens=96000,
                        max_output_tokens=8192,
                        max_tool_calls=0,
                        input_usd_per_million=Decimal(0),
                        output_usd_per_million=Decimal(0),
                        pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
                        pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
                        created_at=AT - timedelta(seconds=1),
                    )
                )
                await session.flush()
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version
                    == (
                        "codex-sdk-chatgpt-v11-bounded-stage-budget"
                        if profile == "formal_v11"
                        else "codex-sdk-chatgpt-v10-flexible-stage-budget"
                        if profile == "formal_v10"
                        else "codex-sdk-chatgpt-v9-full-input-capacity"
                        if profile != "v7"
                        else "codex-sdk-chatgpt-v7-stage-capacity-candidate"
                    )
                )
            )
            assert model.created_at < AT
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(entry.instrument_id for entry in manifest),
                idempotency_key=f"fresh-{count}",
                created_at=AT,
            )
            from nexus.paper.projection import load_paper_projection
            from nexus.paper.target_service import approve_initial_target_projection

            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=AT
            )
            await approve_initial_target_projection(
                session,
                account_id=authority.account_id,
                projection_as_of=AT,
                expected_projection_hash=projection.projection_hash,
                approved_by="controlled-fixture-user",
                approved_at=AT,
                idempotency_key="controlled-initial-baseline",
            )
        manual = None
        if manual_entry:
            assert profile in {"formal_v10", "formal_v11"} and tmp_path is not None
            manual = await _start_manual_http_request(
                settings,
                sessions,
                authority,
                batch,
                model,
                monkeypatch,
                clock_at,
                tmp_path,
                fault,
                recovery_entry=manifest[0] if recover_unknown else None,
            )
            if fault == "preparation_failure":
                assert manual is None and calls == []
                return
            settings, request_id, result, recovery = manual
        else:
            async with sessions.begin() as session:
                result = await start_schwab_agent_paper_cycle(
                    session,
                    authority=authority,
                    decision_slot_at=AT,
                    idempotency_key=f"candidate-{count}",
                    requested_instrument_ids=batch.instrument_ids,
                    candidate_batch_id=batch.batch_id,
                    **(
                        {"formal_stage_capacity": True, "formal_model_config_id": model.id}
                        if formal
                        else {
                            "candidate_stage_capacity": True,
                            "candidate_model_config_id": model.id,
                        }
                    ),
                )
        async with sessions() as session:
            assert result.result_code == "RESEARCH_JOBS_PREPARED", result
            assert len(result.research_run_ids) == count
            runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.in_(result.research_run_ids)
                    )
                )
            )
            jobs = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.payload["run_id"].astext.in_(
                            [str(run_id) for run_id in result.research_run_ids]
                        )
                    )
                )
            )
            assert len(runs) == len(jobs) == count
            assert {run.instrument_id for run in runs} == set(batch.instrument_ids)
            assert all(run.model_config_id == model.id for run in runs)
            assert all(
                job.job_type == "agent.paper.research" and job.attempt_count == 0 for job in jobs
            )
        await _set_test_insert_clock(sessions, clock_at[0])
        for job in jobs:
            research_mode = (
                {"formal_stage_capacity": True}
                if formal
                else {"full_input_capacity": True}
                if profile != "v7"
                else {"candidate_stage_capacity": True}
            )
            if manual:
                state = await _consume_manual_host_action(settings, "RUN_RESEARCH_JOB")
                assert state.research_job_id == job.id
            else:
                assert await run_agent_worker_once(settings, job_ids=(job.id,), **research_mode)
            assert not await run_agent_worker_once(settings, job_ids=(job.id,), **research_mode)
            if fault == "research_unknown":
                from nexus.ops.manual_paper_host import finish_configured_manual_request
                from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

                await _consume_manual_host_action(settings, "RESUME")
                async with sessions() as session:
                    failed = await session.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
                    assert failed.status == "failed" and failed.retry_count == 0
                    assert failed.settled_cost_usd is None
                    assert (failed.input_tokens, failed.output_tokens) == (100, 100)
                    budget = await session.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == failed.id
                        )
                    )
                    assert budget.settled_usd is None
                    before_close = await _manual_recovery_facts(session)
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    async with sessions.begin() as session:
                        await finish_configured_manual_request(
                            session, settings, request_id=request_id, cancel_unused=True
                        )
                async with sessions() as session:
                    assert await _manual_recovery_facts(session) == before_close
                await _verify_failed_manual_intent(settings, sessions, request_id, "active")
                assert calls == ["research", "critique"]
                return
        assert calls == ["research", "critique", "synthesis"] * count
        if decision_delay is not None:
            # Source snapshots stay frozen at AT. Move only the decision observer,
            # after all research jobs have already reached their terminal states.
            from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
            from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
            from nexus.persistence.models.paper import AgentPaperCycleEventRecord, PaperOrderRecord
            from sqlalchemy import func

            clock_at[0] = AT + decision_delay
            await _set_test_insert_clock(sessions, clock_at[0])
            async with sessions.begin() as session:
                observed = await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="bounded-delayed-research-observation",
                    daily_limit_usd=Decimal("3"),
                    now=clock_at[0],
                    formal_stage_capacity=True,
                )
                assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
                synthesis = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
                if decision_delay >= timedelta(hours=6):
                    assert observed.status == "unavailable"
                    assert observed.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
                    terminal = await session.scalar(
                        select(AgentPaperCycleEventRecord)
                        .where(AgentPaperCycleEventRecord.cycle_id == result.cycle_id)
                        .order_by(AgentPaperCycleEventRecord.sequence.desc())
                        .limit(1)
                    )
                    assert terminal.fact_ids["failure_phase"] == "synthesis_admission"
                    assert (
                        terminal.fact_ids["failure_code"] == "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
                    )
                    assert synthesis is None
                else:
                    assert observed.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
                    assert synthesis is not None and synthesis.status == "prepared"
                    assert synthesis.decision_time == AT
                assert all(
                    run.status == "completed"
                    for run in await session.scalars(select(AgentResearchRunRecord))
                )
            if decision_delay >= timedelta(hours=6):
                return
        clock_at[0] = post_research_at + timedelta(seconds=2)
        async with sessions() as session:
            for run_id in result.research_run_ids:
                run = await session.get(AgentResearchRunRecord, run_id)
                assert run.status == "completed" and run.retry_count == 0
                if profile != "v7":
                    from nexus.agents.actionable_research import ActionableResearchError

                    with pytest.raises(
                        ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"
                    ):
                        await load_actionable_research_output(
                            session,
                            run_id=run_id,
                            decision_time=post_research_at + timedelta(seconds=2),
                            allow_safe_no_trade=True,
                            **(
                                {"candidate_stage_capacity": True}
                                if formal
                                else {"formal_stage_capacity": True}
                            ),
                        )
                action = await load_actionable_research_output(
                    session,
                    run_id=run_id,
                    decision_time=post_research_at + timedelta(seconds=2),
                    allow_safe_no_trade=True,
                    **consumer_mode,
                )
                assert action.instrument_id in batch.instrument_ids
                assert action.stance == ("buy_candidate" if trade else "watch")
        from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
        from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
        from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
        from nexus.worker import run_portfolio_synthesis_worker_once

        await _set_test_insert_clock(sessions, post_research_at + timedelta(seconds=2))
        if manual:
            await _consume_manual_host_action(settings, "RESUME")
        async with sessions.begin() as session:
            pending = (
                await _read_cycle_result(session, result.cycle_id)
                if manual
                else await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="synthesis-ready",
                    daily_limit_usd=Decimal("3"),
                    now=post_research_at + timedelta(seconds=2),
                    **consumer_mode,
                )
            )
            if not manual:
                assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING", pending
            else:
                assert pending.status == "researching"
            synth = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            assert synth is not None and synth.status == "prepared"
            intent = synthesis_intent(synth)
            if trade:
                intent = intent.model_copy(
                    update={
                        "choices": tuple(
                            choice.model_copy(
                                update={"action": "include", "role": "tactical", "rank": index}
                            )
                            for index, choice in enumerate(intent.choices, start=1)
                        )
                    }
                )
            synth_job = await session.scalar(
                select(Job).where(Job.payload["run_id"].astext == str(synth.id))
            )

        portfolio_calls = []

        async def invoke_portfolio(payload, **kwargs):
            portfolio_calls.append(payload["input_hash"])
            if fault in {"synthesis_unknown", "synthesis_preflight"}:
                from nexus.agents.codex_sdk import CodexInvocationError

                raise CodexInvocationError(
                    "CODEX_TIMEOUT" if fault == "synthesis_unknown" else "CODEX_INPUT_TOO_LARGE",
                    phase="turn_run" if fault == "synthesis_unknown" else "preflight",
                    provider_call_started=None if fault == "synthesis_unknown" else False,
                )
            return intent.model_dump_json(), UsageSnapshot(
                input_tokens=100, output_tokens=100, provider_calls=1
            )

        if sdk_lifecycle_case is None:
            monkeypatch.setattr(
                "nexus.agents.portfolio_synthesis_worker.invoke_codex_portfolio", invoke_portfolio
            )
        else:
            import asyncio

            from nexus.agents import portfolio_synthesis_worker
            from unit.agents.test_codex_sdk import _sdk
            from unit.agents.test_portfolio_synthesis_worker import _decode_portfolio_view

            assert formal and profile == "formal_v11"
            assert synth_job.schema_version == (
                "portfolio_synthesis_job_v1"
                if sdk_lifecycle_case == "legacy_success"
                else "portfolio_synthesis_job_v2"
            )
            client, thread, response, _ = _sdk(monkeypatch)
            response.final_response = intent.model_dump_json()
            response.usage.total.input_tokens = 100
            response.usage.total.output_tokens = 100
            timers = []
            original_timeout_at = asyncio.timeout_at

            def capture_deadline(deadline):
                timer = original_timeout_at(deadline)
                timers.append(timer)
                return timer

            monkeypatch.setattr(asyncio, "timeout_at", capture_deadline)
            closed = asyncio.Event()
            client.close.side_effect = closed.set
            parent = [None]
            original_invoke = portfolio_synthesis_worker.invoke_codex_portfolio
            if sdk_lifecycle_case == "preparation_deadline":
                # Simulate time already consumed before model dispatch without
                # a fragile wall-clock wait or allowing another model budget.
                deadline_type = portfolio_synthesis_worker.SynthesisDeadline
                monkeypatch.setattr(
                    portfolio_synthesis_worker,
                    "SynthesisDeadline",
                    lambda started_at: deadline_type(started_at - 84),
                )

            async def observed_invoke(payload, **kwargs):
                parent[0] = asyncio.current_task()
                if sdk_lifecycle_case == "legacy_success":
                    assert "lifecycle_version" not in kwargs and "deadline_at" not in kwargs
                    return await original_invoke(payload, **kwargs)
                assert kwargs["lifecycle_version"] == "portfolio_synthesis_job_v2"
                remaining = kwargs["deadline_at"] - asyncio.get_running_loop().time()
                assert 0 < remaining <= 83
                # Compare absolute deadlines: subtraction across a float
                # precision boundary can round a valid seven-second window.
                assert kwargs["cleanup_deadline_at"] == kwargs["deadline_at"] + 7
                return await original_invoke(payload, **kwargs)

            monkeypatch.setattr(
                portfolio_synthesis_worker, "invoke_codex_portfolio", observed_invoke
            )

            async def turn(wire, **kwargs):
                document = json.loads(wire)
                assert document["input_hash"] == synth.input_hash
                assert (
                    len(_decode_portfolio_view(document["frozen_input"])["security_outputs"]) == 3
                )
                portfolio_calls.append(synth.input_hash)
                if sdk_lifecycle_case == "timeout":
                    # Expire the actual SDK deadline, not an interface stub.
                    work = min(
                        (timer for timer in timers if timer.when() is not None),
                        key=lambda timer: timer.when(),
                    )
                    work.reschedule(asyncio.get_running_loop().time())
                    await closed.wait()
                    raise ConnectionError("isolated SDK stopped without response")
                if sdk_lifecycle_case == "cancelled":
                    parent[0].cancel()
                    await closed.wait()
                    raise ConnectionError("isolated SDK stopped without response")
                if sdk_lifecycle_case == "incomplete":
                    response.final_response = None
                return response

            thread.run.side_effect = turn
            if sdk_lifecycle_case == "commit_ack_unknown":
                original_finish = portfolio_synthesis_worker._finish

                async def committed_but_acknowledgement_lost(*args, **kwargs):
                    await original_finish(*args, **kwargs)
                    raise TimeoutError("isolated lost commit acknowledgement")

                monkeypatch.setattr(
                    portfolio_synthesis_worker, "_finish", committed_but_acknowledgement_lost
                )
            if sdk_lifecycle_case == "cleanup_error":
                client.close.side_effect = RuntimeError("isolated close failed")
            elif sdk_lifecycle_case == "cancelled_known":

                async def cancel_during_close():
                    parent[0].cancel()

                client.close.side_effect = cancel_during_close
        clock_at[0] = post_research_at + timedelta(seconds=3)
        await _set_test_insert_clock(sessions, clock_at[0])
        if manual:
            state = await _consume_manual_host_action(settings, "RUN_RESEARCH_JOB")
            assert state.research_job_id == synth_job.id
        elif sdk_lifecycle_case == "commit_ack_unknown":
            with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN$"):
                await run_portfolio_synthesis_worker_once(
                    settings, job_ids=(synth_job.id,), **consumer_mode
                )
        elif sdk_lifecycle_case in {"cancelled", "cancelled_known"}:
            with pytest.raises(asyncio.CancelledError):
                await run_portfolio_synthesis_worker_once(
                    settings, job_ids=(synth_job.id,), **consumer_mode
                )
                # Distinguish an undispatched fixture from lost cancellation.
                async with sessions() as session:
                    terminal_job = await session.get(Job, synth_job.id)
                    from nexus.agents.portfolio_synthesis_diagnostics import read_synthesis_failure

                    diagnostic = await read_synthesis_failure(session, run_id=synth.id)
                    assert portfolio_calls == [synth.input_hash], (
                        terminal_job.status,
                        terminal_job.last_error_code,
                        client.account.await_count,
                        thread.run.await_count,
                        diagnostic.model_dump(mode="json"),
                    )
        else:
            assert await run_portfolio_synthesis_worker_once(
                settings, job_ids=(synth_job.id,), **consumer_mode
            )
        if sdk_lifecycle_case == "preparation_deadline":
            from nexus.persistence.models.agents import (
                AgentPortfolioSynthesisBudgetReservationRecord,
            )

            client.account.assert_not_awaited()
            thread.run.assert_not_awaited()
            client.close.assert_not_awaited()
            async with sessions() as session:
                terminal = await session.get(AgentPortfolioSynthesisRunRecord, synth.id)
                job = await session.get(Job, synth_job.id)
                reservation = await session.scalar(
                    select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                        AgentPortfolioSynthesisBudgetReservationRecord.run_id == synth.id
                    )
                )
                assert terminal.status == job.status == "failed"
                assert job.last_error_code == "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_PREFLIGHT"
                assert terminal.input_tokens is None
                assert terminal.settled_cost_usd == 0
                assert reservation.state == "released"
                assert job.attempt_count == 1
            assert portfolio_calls == []
            assert not await run_portfolio_synthesis_worker_once(
                settings, job_ids=(synth_job.id,), **consumer_mode
            )
            thread.run.assert_not_awaited()
            return
        if sdk_lifecycle_case is not None:
            client.close.assert_awaited_once()
            thread.run.assert_awaited_once()
            assert portfolio_calls == [synth.input_hash]
            if sdk_lifecycle_case not in {
                "success",
                "success_buy",
                "legacy_success",
                "commit_ack_unknown",
            }:
                from nexus.persistence.models.agents import (
                    AgentPortfolioSynthesisBudgetReservationRecord,
                    AgentPortfolioSynthesisOutputRecord,
                )
                from nexus.persistence.models.runtime import JobAttempt
                from sqlalchemy import func

                async with sessions() as session:
                    terminal = await session.get(AgentPortfolioSynthesisRunRecord, synth.id)
                    output = await session.scalar(
                        select(AgentPortfolioSynthesisOutputRecord).where(
                            AgentPortfolioSynthesisOutputRecord.run_id == synth.id
                        )
                    )
                    reservation = await session.scalar(
                        select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                            AgentPortfolioSynthesisBudgetReservationRecord.run_id == synth.id
                        )
                    )
                    job = await session.get(Job, synth_job.id)
                    assert terminal.status == "failed"
                    assert (
                        terminal.input_tokens is None
                        if sdk_lifecycle_case in {"timeout", "cancelled"}
                        else terminal.input_tokens == 100
                    )
                    assert (
                        reservation.state == "reserved"
                        if terminal.input_tokens is None
                        else reservation.state == "settled"
                    )
                    assert job.status == "failed"
                    assert (
                        await session.scalar(
                            select(func.count())
                            .select_from(JobAttempt)
                            .where(JobAttempt.job_id == job.id)
                        )
                        == 1
                    )
                assert not await run_portfolio_synthesis_worker_once(
                    settings, job_ids=(synth_job.id,), **consumer_mode
                )
                assert portfolio_calls == [synth.input_hash]
                assert output.status == "failed"
                from nexus.paper.manual_request import (
                    require_manual_account_available,
                    require_settled_account_cycle_history,
                )

                async with sessions.begin() as session:
                    closed_cycle = await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=result.cycle_id,
                        idempotency_key="sdk-failure-closeout",
                        now=post_research_at + timedelta(seconds=4),
                        **consumer_mode,
                    )
                    assert closed_cycle.status == "unavailable"
                    assert closed_cycle.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
                after_leases = post_research_at + timedelta(minutes=10)
                async with sessions.begin() as session:
                    if terminal.settled_cost_usd is None:
                        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                            await require_manual_account_available(
                                session, account_id=authority.account_id, now=after_leases
                            )
                    else:
                        await require_manual_account_available(
                            session, account_id=authority.account_id, now=after_leases
                        )
                        await require_settled_account_cycle_history(
                            session, account_id=authority.account_id, now=after_leases
                        )
                return
        if fault in {"synthesis_unknown", "synthesis_preflight"}:
            assert manual is not None
            await _verify_synthesis_retry_refused(settings, sessions, synth.id, synth_job.id, fault)
            assert portfolio_calls == [synth.input_hash]
            with pytest.raises(ValueError, match="^PORTFOLIO_SYNTHESIS_UNAVAILABLE$"):
                await _consume_manual_host_action(settings, "RESUME")
            async with sessions() as session:
                unavailable = await _read_cycle_result(session, result.cycle_id)
                assert unavailable.status == "unavailable"
                assert unavailable.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
            await _verify_failed_manual_intent(settings, sessions, request_id, "active")
            assert portfolio_calls == [synth.input_hash]
            return
        async with sessions() as session:
            completed_synth = await session.get(AgentPortfolioSynthesisRunRecord, synth.id)
            completed_job = await session.get(Job, synth_job.id)
            assert completed_synth.status == "completed", completed_synth.status
            assert completed_job.status == "succeeded", (
                completed_job.status,
                completed_job.error_code,
            )
        clock_at[0] = post_research_at + timedelta(seconds=4)
        if not trade and count == 3 and not manual:
            from sqlalchemy import event
            from sqlalchemy.exc import DBAPIError

            def corrupt_frozen_hash_after_reader(conn, cursor, statement, *args):
                if not statement.lower().startswith("insert into strategy_decision_agent_outputs"):
                    return
                # Fault injection only in this isolated test transaction. The output-binding
                # trigger stays enabled; rollback restores this deliberately invalid Run.
                conn.exec_driver_sql("ALTER TABLE agent_research_runs DISABLE TRIGGER USER")
                conn.exec_driver_sql("UPDATE agent_research_runs SET model_hash=repeat('0',64)")
                conn.exec_driver_sql("ALTER TABLE agent_research_runs ENABLE TRIGGER USER")

            event.listen(
                engine.sync_engine, "before_cursor_execute", corrupt_frozen_hash_after_reader
            )
            try:
                with pytest.raises(DBAPIError, match="agent decision output binding is invalid"):
                    async with sessions.begin() as session:
                        await resume_schwab_agent_paper_cycle(
                            session,
                            cycle_id=result.cycle_id,
                            idempotency_key="corrupt-frozen-model",
                            now=post_research_at + timedelta(seconds=4),
                            **consumer_mode,
                        )
            finally:
                event.remove(
                    engine.sync_engine, "before_cursor_execute", corrupt_frozen_hash_after_reader
                )
            async with sessions() as session:
                assert all(
                    row.model_hash == model.config_hash
                    for row in await session.scalars(select(AgentResearchRunRecord))
                )
        clock_at[0] = post_research_at + timedelta(seconds=4)
        await _set_test_insert_clock(sessions, clock_at[0])
        if manual:
            await _consume_manual_host_action(settings, "RESUME")
        async with sessions.begin() as session:
            finished = (
                await _read_cycle_result(session, result.cycle_id)
                if manual
                else await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="activate-target",
                    now=post_research_at + timedelta(seconds=4),
                    **consumer_mode,
                )
            )
            assert finished.result_code == (
                "PAPER_ORDERS_AWAITING_MARKET" if trade else "NO_ORDER_REQUIRED"
            ), finished
            assert bool(finished.order_ids) is trade
        from nexus.persistence.models.paper import (
            AgentPaperCycleRecord,
            PaperOrderRecord,
            PaperTargetVersionRecord,
            RiskEvaluationRecord,
            StrategyDecisionAgentOutputRecord,
        )
        from sqlalchemy import func

        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            assert cycle.execution_locked is True
            target = await session.scalar(select(PaperTargetVersionRecord))
            assert target.strategy_decision_id == finished.strategy_decision_id
            assert (
                await session.scalar(
                    select(func.count()).select_from(StrategyDecisionAgentOutputRecord)
                )
                == count
            )
            orders = tuple(await session.scalars(select(PaperOrderRecord)))
            assert len(orders) == (2 if trade else 0)
            assert {order.id for order in orders} == set(finished.order_ids)
            if trade:
                assert {order.instrument_id for order in orders} == {
                    choice.instrument_id for choice in intent.choices[:2]
                }
                assert all(order.target_weight == Decimal("0.05") for order in orders)
            assert all(
                order.instrument_id in batch.instrument_ids
                and order.side == "buy"
                and order.quantity > 0
                and order.risk_outcome in {"approved", "reduced"}
                for order in orders
            )
            assert await session.scalar(
                select(func.count()).select_from(RiskEvaluationRecord)
            ) == len(orders)
            tables = (Job, PaperOrderRecord, PaperTargetVersionRecord, RiskEvaluationRecord)
            counts_before = [
                await session.scalar(select(func.count()).select_from(table)) for table in tables
            ]
            replay = (
                await _read_cycle_result(session, result.cycle_id)
                if manual
                else await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="activate-target",
                    now=post_research_at + timedelta(seconds=5),
                    **consumer_mode,
                )
            )
            assert replay.order_ids == finished.order_ids
            assert replay.strategy_decision_id == finished.strategy_decision_id
            assert [
                await session.scalar(select(func.count()).select_from(table)) for table in tables
            ] == counts_before
        if trade and profile in {"formal_v10", "formal_v11"}:
            from nexus.paper.agent_decision_store import _load_portfolio_target_baseline

            async with sessions() as session:
                target = await session.scalar(
                    select(PaperTargetVersionRecord).where(
                        PaperTargetVersionRecord.strategy_decision_id
                        == finished.strategy_decision_id
                    )
                )
                assert target is not None
                baseline = await _load_portfolio_target_baseline(
                    session,
                    target=target,
                    account_id=authority.account_id,
                    candidate_stage_capacity=False,
                    formal_stage_capacity=True,
                )
                assert (
                    baseline is not None and baseline.decision_id == finished.strategy_decision_id
                )
        if trade:
            await _verify_internal_fills(
                settings,
                sessions,
                result.cycle_id,
                authority.account_id,
                orders,
                count,
                formal=formal,
                decision_observed_at=post_research_at + timedelta(seconds=4),
                database_clock_at=clock_at if manual else None,
                manual_settings=settings if manual else None,
                manual_request_id=request_id if manual else None,
                host_settings=settings if manual else None,
                monkeypatch=monkeypatch if manual else None,
                calendar_authority_id=authority.calendar_authority_id if manual else None,
            )
            if formal and not manual:
                from nexus.paper.manual_request import require_settled_account_cycle_history
                from nexus.paper.read_service import PaperReadService
                from nexus.persistence.models.paper import AgentPaperCycleEventRecord

                clock_at[0] = AT + timedelta(hours=1)
                async with sessions.begin() as session:
                    last = await session.scalar(
                        select(AgentPaperCycleEventRecord)
                        .where(AgentPaperCycleEventRecord.cycle_id == result.cycle_id)
                        .order_by(AgentPaperCycleEventRecord.sequence.desc())
                        .limit(1)
                    )
                    assert last.to_status == "awaiting_market"
                    detail = await PaperReadService(session, clock_at[0]).agent_cycle_detail(
                        result.cycle_id
                    )
                    assert detail.cycle.status == "completed"
                    assert detail.cycle.result_code == "PAPER_CYCLE_COMPLETED"
                    await require_settled_account_cycle_history(
                        session,
                        account_id=authority.account_id,
                        now=clock_at[0],
                        allow_scheduled_completion=True,
                    )
                    # Keep manual finalization semantics unchanged.
                    with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                        await require_settled_account_cycle_history(
                            session, account_id=authority.account_id, now=clock_at[0]
                        )
                from nexus.persistence.models.runtime import JobLease

                async with sessions() as session:
                    transaction = await session.begin()
                    try:
                        lease = await session.scalar(
                            select(JobLease).where(JobLease.job_id == jobs[0].id)
                        )
                        assert lease is not None
                        lease.expires_at = clock_at[0] + timedelta(minutes=1)
                        await session.flush()
                        # Projected execution completion never waives research leases.
                        with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                            await require_settled_account_cycle_history(
                                session,
                                account_id=authority.account_id,
                                now=clock_at[0],
                                allow_scheduled_completion=True,
                            )
                    finally:
                        await transaction.rollback()
        elif profile != "v7" and count == 3:
            import asyncio

            from alembic import command
            from alembic.config import Config
            from sqlalchemy import text
            from sqlalchemy.exc import DBAPIError

            migration_config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
            with pytest.raises(
                DBAPIError,
                match="V11_RUN_BINDINGS_EXIST"
                if profile == "formal_v11"
                else "V10_OUTPUT_BINDINGS_EXIST"
                if profile == "formal_v10"
                else "V9_OUTPUT_BINDINGS_EXIST",
            ):
                await asyncio.to_thread(
                    command.downgrade, migration_config, "0075_manual_paper_requests"
                )
            from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

            async with sessions() as session:
                assert (
                    await session.scalar(text("SELECT version_num FROM alembic_version"))
                    == CURRENT_MIGRATION_HEAD
                )
                assert (
                    await session.scalar(
                        select(func.count()).select_from(StrategyDecisionAgentOutputRecord)
                    )
                    == count
                )
        if manual:
            import httpx
            from helpers.paper_api import headers
            from nexus.main import create_engine_app
            from nexus.ops.manual_paper_host import finish_configured_manual_request

            async with sessions.begin() as session:
                view = await finish_configured_manual_request(
                    session, settings, request_id=request_id
                )
                assert view.status == "completed" and view.cycle_id == result.cycle_id
            path = "/internal/v1/paper/manual-requests"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_engine_app(settings)),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    path, headers={**headers("GET", path), "Idempotency-Key": "v10-entry-result"}
                )
                assert response.status_code == 200
                assert response.json()["data"] == view.model_dump(mode="json")
            if recovery is not None:
                old_id, old_facts = recovery
                async with sessions() as session:
                    after = await _manual_recovery_facts(session)
                for table, hashes in old_facts.items():
                    assert set(hashes).issubset(after[table]), table
                await _verify_acknowledged_request(settings, old_id)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
async def test_v10_manual_entry_to_result(migrated_database_url, monkeypatch, tmp_path, trade):
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url,
        monkeypatch,
        3,
        trade,
        "formal_v10",
        True,
        tmp_path,
        recover_unknown=trade,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["preparation_failure", "research_unknown", "synthesis_unknown", "synthesis_preflight"]
)
async def test_v10_manual_entry_failure_preserves_recovery_gates(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url, monkeypatch, 3, True, "formal_v10", True, tmp_path, fault
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
async def test_v11_manual_entry_to_result(
    migrated_database_url, monkeypatch, tmp_path, count, trade
):
    """Controlled model/source fixtures; normal admission, workers and execution stay real."""
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url,
        monkeypatch,
        count,
        trade,
        "formal_v11",
        True,
        tmp_path,
        recover_unknown=trade and count == 3,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["preparation_failure", "research_unknown", "synthesis_unknown", "synthesis_preflight"]
)
async def test_v11_manual_entry_failure_preserves_recovery_gates(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url, monkeypatch, 3, True, "formal_v11", True, tmp_path, fault
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "count,delay",
    [(3, timedelta(minutes=17, seconds=30)), (5, timedelta(minutes=28)), (3, timedelta(hours=6))],
    ids=["three-candidate-budget", "five-candidate-budget", "session-closed"],
)
async def test_v11_delayed_research_observation_keeps_session_boundary(
    migrated_database_url, monkeypatch, count, delay
):
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url, monkeypatch, count, True, "formal_v11", decision_delay=delay
    )


async def _verify_synthesis_retry_refused(settings, sessions, run_id, job_id, fault):
    import httpx
    from helpers.paper_api import headers
    from nexus.main import create_engine_app
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisBudgetReservationRecord,
        AgentPortfolioSynthesisRunRecord,
    )
    from nexus.persistence.models.runtime import IdempotencyRecord
    from nexus.worker import run_portfolio_synthesis_worker_once
    from sqlalchemy import func

    async with sessions() as session:
        job = await session.get(Job, job_id)
        run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
        reserve = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == run_id
            )
        )
        assert job.job_type == "agent.portfolio_synthesis"
        assert (job.status, job.retry_policy, job.max_attempts, job.attempt_count) == (
            "failed",
            "never",
            1,
            1,
        )
        assert job.manual_retry_count == 0 and run.status == "failed"
        assert job.last_error_code == (
            "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
            if fault == "synthesis_unknown"
            else "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_PREFLIGHT"
        )
        if fault == "synthesis_unknown":
            assert run.settled_cost_usd is None
            assert reserve.state == "reserved" and reserve.settled_usd is None
        else:
            assert run.settled_cost_usd == Decimal("0")
            assert reserve.state == "released" and reserve.settled_usd == Decimal("0")
        before = await _manual_recovery_facts(session)
        version = job.version
    assert not await run_portfolio_synthesis_worker_once(
        settings, job_ids=(job_id,), formal_stage_capacity=True
    )
    path = f"/internal/v1/jobs/{job_id}/retry"
    body = b"{}"
    app = create_engine_app(settings)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                path,
                content=body,
                headers={
                    **headers("POST", path, body),
                    "Idempotency-Key": "synthesis-must-not-replay",
                    "If-Match": f'"{version}"',
                },
            )
        assert response.status_code == 403, response.text
        assert response.json()["error"]["code"] == "FORBIDDEN"
        async with sessions() as session:
            assert await _manual_recovery_facts(session) == before
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(IdempotencyRecord)
                    .where(IdempotencyRecord.route == path)
                )
                == 0
            )
    finally:
        await app.state.session_factory.kw["bind"].dispose()


async def _verify_failed_manual_intent(settings, sessions, request_id, status):
    import httpx
    from helpers.paper_api import headers
    from nexus.main import create_engine_app
    from nexus.persistence.models.paper import PaperOrderRecord
    from sqlalchemy import func

    async with sessions() as session:
        before = await _manual_recovery_facts(session)
        assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
    path = "/internal/v1/paper/manual-requests"
    body = b'{"mode":"research_and_paper"}'
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_engine_app(settings)), base_url="http://test"
    ) as client:
        read = await client.get(
            path, headers={**headers("GET", path), "Idempotency-Key": "v10-entry-result"}
        )
        assert read.status_code == 200
        assert read.json()["data"]["request_id"] == str(request_id)
        assert read.json()["data"]["status"] == status
        original = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "v10-entry-result"},
        )
        assert original.json()["data"] == read.json()["data"]
        other = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "must-not-bypass-unknown"},
        )
        assert other.status_code == 409
        assert other.json()["error"]["details"]["reason_code"] == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
    async with sessions() as session:
        assert await _manual_recovery_facts(session) == before
    from nexus.ops import agent_paper_loop as loop

    state = await _inspect_manual_host(settings)
    assert loop.choose_loop_action(state) in {
        loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
        loop.AgentPaperLoopAction.FINISH_MANUAL,
    }


async def _manual_recovery_facts(session):
    from hashlib import sha256

    from sqlalchemy import text

    tables = (
        "manual_paper_requests",
        "manual_paper_request_events",
        "agent_paper_cycles",
        "agent_paper_cycle_events",
        "strategy_decisions",
        "agent_research_runs",
        "agent_research_outputs",
        "agent_portfolio_synthesis_runs",
        "agent_portfolio_synthesis_outputs",
        "jobs",
        "job_attempts",
        "job_leases",
        "agent_daily_budget_reservations",
        "agent_portfolio_synthesis_budget_reservations",
        "portfolio_decision_budget_reservations",
        "paper_orders",
        "paper_fills",
        "paper_ledger_transactions",
        "paper_ledger_entries",
    )
    return {
        table: sorted(
            sha256(row.encode()).hexdigest()
            for row in (
                await session.execute(text("SELECT row_to_json(t)::text FROM " + table + " t"))
            ).scalars()
        )
        for table in tables
    }


async def _start_manual_http_request(
    settings,
    sessions,
    authority,
    batch,
    model,
    monkeypatch,
    clock_at,
    root,
    fault=None,
    *,
    recovery_entry=None,
):
    import httpx
    from helpers.paper_api import headers
    from helpers.paper_api import settings as api_settings
    from helpers.paper_batch import CALENDAR_ID
    from nexus.main import create_engine_app
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_once as once
    from nexus.ops.schwab_market_stream import SchwabMarketStreamImportResult
    from nexus.paper import agent_cycle
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.manual_paper import ManualPaperRequest
    from nexus.persistence.models.market import LatestQuote
    from sqlalchemy.ext.asyncio import AsyncSession

    clock_at[0] = AT
    scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock_at[0]
        return await scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    async with sessions.begin() as session:
        later = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=later.date(),
                decision_at=later.replace(hour=21),
                market_open_at=later.replace(hour=13, minute=30),
                market_close_at=later.replace(hour=20),
                created_at=AT - timedelta(hours=1),
            )
        )
    settings = EngineSettings.model_validate(
        api_settings(settings.database_url).model_dump()
        | {
            "agent_runtime_mode": "codex_sdk",
            "agent_paper_account_id": authority.account_id,
            "agent_paper_candidate_batch_id": batch.batch_id,
            "agent_paper_formal_model_config_id": model.id,
            "agent_paper_cycle_enabled": True,
            "agent_paper_manual_enabled": True,
            "release_revision": "a" * 40,
        }
    )
    source_calls = []

    async def proofs(**kwargs):
        assert {x.instrument_id for x in kwargs["candidates"]} == set(batch.instrument_ids)
        source_calls.append("risk")
        if fault == "preparation_failure":
            raise ValueError("RISK_SOURCE_COLLECTION_FAILED")
        return {i: () for i in batch.instrument_ids}

    async def persisted_risk(*args, **kwargs):
        # Controlled risk source records were ingested by seed_normal_path;
        # their normal native authority/hash readers remain active.
        assert kwargs["proofs"] == ()
        source_calls.append("risk-import")

    async def quotes(session, store, **kwargs):
        from nexus.persistence.models.runtime import IdempotencyRecord

        claim = await session.scalar(
            select(IdempotencyRecord).where(IdempotencyRecord.idempotency_key == str(request_id))
        )
        assert claim is not None and claim.response_payload == {}
        source_calls.append("quotes")
        rows = tuple(
            await session.scalars(
                select(LatestQuote).where(
                    LatestQuote.instrument_id.in_(batch.instrument_ids),
                    LatestQuote.source == "schwab",
                )
            )
        )
        artifact = await session.get(DataArtifact, rows[0].data_artifact_id)
        assert set(kwargs["bindings"].values()) == set(batch.instrument_ids)
        return SchwabMarketStreamImportResult(
            raw_sha256=artifact.sha256,
            created_observations=len(rows),
            symbols_observed=len(rows),
            artifact_id=artifact.id,
            quote_ids={
                symbol: next(q.id for q in rows if q.instrument_id == i)
                for symbol, i in kwargs["bindings"].items()
            },
            trade_ids={},
        )

    monkeypatch.setattr(once, "collect_source_proofs_batch", proofs)
    monkeypatch.setattr(once, "persist_query_authorities", persisted_risk)
    monkeypatch.setattr(once, "capture_and_import_schwab_level_one_batch", quotes)
    monkeypatch.setattr(once, "S3ProviderEvidenceStore", lambda *_a, **_k: object())

    async def sdk_ready():
        return None

    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker_async", sdk_ready)
    monkeypatch.setattr(loop, "local_settings", lambda *_a, **_k: settings)
    import os

    browser_entry = os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1" and fault is None
    if browser_entry:
        import subprocess

        settings.release_revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    await loop.heartbeat_agent_paper_loop(
        settings,
        account_id=authority.account_id,
        worker_id=f"agent-paper-loop-{UUID(int=1)}",
        started_at=AT - timedelta(minutes=1),
        seen_at=AT,
    )
    recovery = None
    if recovery_entry is not None:
        recovery = await _accept_old_preparation(
            settings, sessions, model, recovery_entry, monkeypatch, root
        )
    app = create_engine_app(settings)
    path = "/internal/v1/paper/manual-requests"
    if browser_entry:
        from helpers.controlled_browser import verify_controlled_browser

        request_id = await verify_controlled_browser(
            settings.database_url,
            api_now=AT,
            cycle_id=None,
            order_ids=(),
            manual_settings=settings,
            submit_manual=True,
            recovered_manual_request_id=recovery[0] if recovery else None,
        )
        assert request_id is not None
    else:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            body = b'{"mode":"research_and_paper"}'
            response = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "v10-entry-result"},
            )
            assert response.status_code == 200, response.text
            request_id = UUID(response.json()["data"]["request_id"])
            assert response.json()["data"]["status"] == "queued"
    if recovery is not None:
        assert request_id != recovery[0]
    trigger_settings = once.AgentPaperTriggerSettings.model_validate(
        settings.model_dump()
        | {
            "object_store_endpoint": "http://127.0.0.1:9",
            "alpaca_key": "fixture",
            "alpaca_secret": "fixture",
        }
    )
    monkeypatch.setattr(
        loop,
        "load_agent_paper_service_environment",
        lambda *_a, **_k: trigger_settings.model_dump(),
    )
    from nexus.ops.manual_paper_host import inspect_manual_queue

    state = await loop.inspect_agent_paper_loop(
        settings, account_id=authority.account_id, expected_calendar_authority_id=CALENDAR_ID
    )
    state = await inspect_manual_queue(settings, account_id=authority.account_id, snapshot=state)
    assert loop.choose_loop_action(state) == loop.AgentPaperLoopAction.TRIGGER_MANUAL
    try:
        dispatched = await loop.execute_agent_paper_loop_action(
            settings,
            account_id=authority.account_id,
            repo_root=root,
            action=loop.AgentPaperLoopAction.TRIGGER_MANUAL,
            snapshot=state,
        )
    except ValueError as error:
        if fault == "preparation_failure":
            assert str(error) == "MANUAL_RESULT_UNKNOWN"
            assert vars(error)["_nexus_agent_paper_failure_phase"] == "trigger_provider"
            assert source_calls == ["risk"]
            await _verify_failed_manual_intent(settings, sessions, request_id, "unknown")
            return None
        raise AssertionError((vars(error), source_calls)) from error
    assert dispatched is None
    async with sessions() as session:
        row = await session.get(ManualPaperRequest, request_id)
        from nexus.paper.manual_request import read_manual_request

        view = await read_manual_request(session, row)
        assert view.status == "active" and view.cycle_id is not None
        result = agent_cycle._result(
            view.cycle_id, await agent_cycle._events(session, view.cycle_id), replayed=False
        )
    assert source_calls == ["risk", "quotes", *(["risk-import"] * len(batch.instrument_ids))]
    clock_at[0] = AT + timedelta(seconds=1)
    return settings, request_id, result, recovery


async def _accept_old_preparation(settings, sessions, model, entry, monkeypatch, root):
    """Accept one unknown in the same database that will run the new formal cycle."""
    from contextlib import nullcontext

    import httpx
    from helpers.paper_api import headers
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_current_preparation_recovery_pg import _seed_acceptance_budget
    from nexus.main import create_engine_app
    from nexus.ops import agent_paper_once, manual_paper, manual_paper_host
    from nexus.paper import current_preparation_recovery

    path = "/internal/v1/paper/manual-requests"
    body = b'{"mode":"research_and_paper"}'
    app = create_engine_app(settings)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "interrupted"},
            )
            assert response.status_code == 200, response.text
            old_id = UUID(response.json()["data"]["request_id"])
            assert response.json()["data"]["status"] == "queued"
    finally:
        await app.state.session_factory.kw["bind"].dispose()

    async def interrupted(*_a, **_k):
        raise ValueError("AGENT_PAPER_RESULT_UNKNOWN")

    trigger = agent_paper_once.AgentPaperTriggerSettings.model_validate(
        settings.model_dump()
        | {
            "object_store_endpoint": "http://127.0.0.1:9",
            "alpaca_key": "fixture",
            "alpaca_secret": "fixture",
        }
    )
    with monkeypatch.context() as m:
        # Fault at the existing preparation boundary; never replay this old request.
        m.setattr(agent_paper_once, "trigger_once", interrupted)
        with pytest.raises(ValueError, match="^MANUAL_RESULT_UNKNOWN$"):
            await manual_paper_host.dispatch_manual_request(
                settings,
                trigger,
                request_id=old_id,
                repo_root=root,
                calendar_authority_id=CALENDAR_ID,
                process_environment={},
            )
    async with sessions.begin() as session:
        await _seed_acceptance_budget(session, entry, model, "accept_prepared_closed")
    async with sessions() as session:
        before = await _manual_recovery_facts(session)
    args = manual_paper.parser().parse_args(
        [
            "--repo-root",
            str(root),
            "recover-preparation",
            "--request-id",
            str(old_id),
            "--accept-unknown-effects",
        ]
    )
    with monkeypatch.context() as m:
        # OS checkout/producer ownership are isolated substitutes; maintenance
        # locks, persisted proofs, account/job/budget checks and audit remain real.
        m.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
        m.setattr(manual_paper, "preparation_service_control_lock", lambda *_: nullcontext())
        m.setattr(
            current_preparation_recovery, "_require_execution_producer_stopped", lambda *_: None
        )
        accepted = await manual_paper.run(settings, args)
    assert accepted.status == "preparation_unknown_acknowledged" and not accepted.replayed
    async with sessions() as session:
        assert await _manual_recovery_facts(session) == before
    await _verify_acknowledged_request(settings, old_id)
    return old_id, before


async def _verify_acknowledged_request(settings, request_id):
    import httpx
    from helpers.paper_api import headers
    from nexus.main import create_engine_app

    app = create_engine_app(settings)
    path = f"/internal/v1/paper/manual-requests/{request_id}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["request_id"] == str(request_id)
        assert data["status"] == "unknown" and data["cycle_id"] is None
        assert data["preparation_disposition"] == "unknown_acknowledged"
        assert data["execution_locked"] is True
    finally:
        await app.state.session_factory.kw["bind"].dispose()


async def _inspect_manual_host(settings):
    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops.manual_paper_host import inspect_manual_close, inspect_manual_queue

    state = await loop.inspect_agent_paper_loop(
        settings,
        account_id=settings.agent_paper_account_id,
        expected_calendar_authority_id=CALENDAR_ID,
    )
    state = await inspect_manual_close(
        settings, account_id=settings.agent_paper_account_id, snapshot=state
    )
    return await inspect_manual_queue(
        settings, account_id=settings.agent_paper_account_id, snapshot=state
    )


async def _consume_manual_host_action(settings, expected):
    from contextlib import nullcontext
    from unittest.mock import patch

    from nexus.ops import agent_paper_loop as loop

    state = await _inspect_manual_host(settings)
    action = loop.choose_loop_action(state)
    assert action == getattr(loop.AgentPaperLoopAction, expected), (expected, action, state)

    class ResumeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return state.now if tz is None else state.now.astimezone(tz)

    with (
        patch("nexus.ops.agent_paper_once.datetime", ResumeClock)
        if expected == "RESUME"
        else nullcontext()
    ):
        assert (
            await loop.execute_agent_paper_loop_action(
                settings,
                account_id=settings.agent_paper_account_id,
                repo_root=Path.cwd(),
                action=action,
                snapshot=state,
            )
            is None
        )
    return state


async def _read_cycle_result(session, cycle_id):
    from nexus.paper import agent_cycle

    return agent_cycle._result(
        cycle_id, await agent_cycle._events(session, cycle_id), replayed=False
    )


async def _set_test_insert_clock(sessions, at):
    """Date fresh inserts before writes; never rewrite persisted synthetic facts."""
    from nexus.agents.codex_candidate import require_candidate_database
    from sqlalchemy import text

    assert at.tzinfo == UTC
    async with sessions.begin() as session:
        await require_candidate_database(session)
        await session.execute(
            text(
                "DO $$ DECLARE item record; BEGIN FOR item IN "
                "SELECT table_name FROM information_schema.columns "
                "WHERE table_schema='public' AND column_name='created_at' "
                "LOOP EXECUTE format('ALTER TABLE %I ALTER COLUMN created_at SET DEFAULT "
                f"''{at.isoformat()}''::timestamptz', item.table_name); END LOOP; END $$"
            )
        )


async def _verify_internal_fills(
    settings,
    sessions,
    cycle_id,
    account_id,
    orders,
    count,
    *,
    verify_reads=True,
    formal=False,
    host_settings=None,
    monkeypatch=None,
    protected_environment=None,
    calendar_authority_id=None,
    database_clock_at=None,
    manual_settings=None,
    manual_request_id=None,
    decision_observed_at=None,
):
    from integration.test_agent_paper_cycle_pg import _ObjectStore, _SchwabPriceHistoryProvider
    from nexus.api.errors import NexusError
    from nexus.ops.paper_once import PaperAdvanceRequest, read_agent_window_advance_result, run_once
    from nexus.paper.agent_market import (
        collect_agent_paper_window,
        resolve_agent_paper_market_scope,
    )
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.read_service import PaperReadService
    from nexus.persistence.models.paper import (
        AgentPaperCycleRecord,
        AgentPaperMarketWindowRecord,
        PaperExecutionWindowAuthorityRecord,
        PaperFillRecord,
        PaperLedgerEntryRecord,
        PaperLedgerTransactionRecord,
        PaperPositionAdjustmentEventRecord,
    )
    from sqlalchemy import func

    provider = _SchwabPriceHistoryProvider()
    if host_settings is not None:
        from nexus.ops.agent_paper_loop import AgentPaperLoopAction, execute_agent_paper_loop_action
        from unit.ops.test_agent_paper_loop import snapshot

        assert monkeypatch is not None
        market_clock = [AT]
        environment = host_settings.model_dump() | {"object_store_endpoint": "http://127.0.0.1:9"}
        if protected_environment is not None:
            environment |= protected_environment
            environment["object_store_endpoint"] = "http://127.0.0.1:9"
        monkeypatch.setattr(
            "nexus.ops.agent_paper_loop.load_agent_paper_service_environment",
            lambda *a, **kw: environment,
        )
        monkeypatch.setattr(
            "nexus.ops.agent_paper_once.SchwabPriceHistoryProvider", lambda *a, **kw: provider
        )
        monkeypatch.setattr(
            "nexus.ops.agent_paper_once.S3ProviderEvidenceStore", lambda *a, **kw: _ObjectStore()
        )
        monkeypatch.setattr("nexus.paper.agent_market._utc_now", lambda: market_clock[0])
    windows = []
    decision_observed_at = decision_observed_at or AT + timedelta(seconds=4)
    async with sessions() as session:
        before = await load_paper_projection(
            session, account_id=account_id, as_of=decision_observed_at + timedelta(seconds=1)
        )
    for order in orders:
        async with sessions() as session:
            scope = await resolve_agent_paper_market_scope(
                session, cycle_id=cycle_id, instrument_id=order.instrument_id
            )
            assert scope.window_start > decision_observed_at
        evaluated_at = scope.window_end + timedelta(seconds=2)
        if manual_settings is not None:
            from nexus.ops.agent_paper_loop import choose_loop_action

            evaluated_at = max(evaluated_at, database_clock_at[0])
            database_clock_at[0] = evaluated_at
            waiting = await _inspect_manual_host(manual_settings)
            if choose_loop_action(waiting).value == "wait_for_market":
                assert waiting.market_window_end_at is not None
                evaluated_at = waiting.market_window_end_at
        if database_clock_at is not None:
            database_clock_at[0] = evaluated_at
        await _set_test_insert_clock(sessions, evaluated_at)
        if host_settings is not None:
            market_clock[0] = evaluated_at
            if not windows and protected_environment is None and manual_settings is None:
                from integration.test_portfolio_management_e2e import _business_facts

                async with sessions() as session:
                    before_bad_config = await _business_facts(session)
                environment["agent_paper_formal_model_config_id"] = UUID(int=999)
                with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
                    await execute_agent_paper_loop_action(
                        host_settings,
                        account_id=account_id,
                        repo_root=Path.cwd(),
                        action=AgentPaperLoopAction.COLLECT_WINDOW,
                        snapshot=snapshot(
                            calendar_authority_id=calendar_authority_id,
                            now=evaluated_at,
                            cycle_id=cycle_id,
                            instrument_id=order.instrument_id,
                        ),
                    )
                environment["agent_paper_formal_model_config_id"] = (
                    host_settings.agent_paper_formal_model_config_id
                )
                assert provider.calls == 0
                async with sessions() as session:
                    assert await _business_facts(session) == before_bad_config
            if manual_settings is not None:
                selected = await _consume_manual_host_action(manual_settings, "COLLECT_WINDOW")
                assert selected.instrument_id == order.instrument_id
            else:
                assert (
                    await execute_agent_paper_loop_action(
                        host_settings,
                        account_id=account_id,
                        repo_root=Path.cwd(),
                        action=AgentPaperLoopAction.COLLECT_WINDOW,
                        snapshot=snapshot(
                            calendar_authority_id=calendar_authority_id,
                            now=evaluated_at,
                            cycle_id=cycle_id,
                            instrument_id=order.instrument_id,
                        ),
                    )
                    is None
                )
            window = await collect_agent_paper_window(
                sessions,
                cycle_id=cycle_id,
                instrument_id=order.instrument_id,
                provider=provider,
                object_store=_ObjectStore(),
                evaluated_at=evaluated_at,
            )
            assert window.replayed
        else:
            window = await collect_agent_paper_window(
                sessions,
                cycle_id=cycle_id,
                instrument_id=order.instrument_id,
                provider=provider,
                object_store=_ObjectStore(),
                evaluated_at=evaluated_at,
            )
        windows.append(window)
        assert window.source == "schwab" and window.purpose == "paper_fill"
        replay = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle_id,
            instrument_id=order.instrument_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at + timedelta(seconds=1),
        )
        assert replay.authority_id == window.authority_id and replay.replayed
        request = PaperAdvanceRequest(
            action="advance",
            account_id=account_id,
            order_id=order.id,
            evaluated_at=evaluated_at,
            agent_cycle_id=cycle_id,
            agent_market_window_id=window.authority_id,
            idempotency_key=f"candidate-window:{order.id}:{window.authority_id}",
        )
        if host_settings is not None:
            request = request.model_copy(
                update={
                    "idempotency_key": (
                        f"agent-paper-loop-advance-v2:{order.id}:{window.authority_id}"
                    )
                }
            )
            if manual_settings is not None:
                selected = await _consume_manual_host_action(manual_settings, "ADVANCE_ORDER")
                assert selected.order_id == order.id
            else:
                assert (
                    await execute_agent_paper_loop_action(
                        host_settings,
                        account_id=account_id,
                        repo_root=Path.cwd(),
                        action=AgentPaperLoopAction.ADVANCE_ORDER,
                        snapshot=snapshot(
                            calendar_authority_id=calendar_authority_id,
                            now=evaluated_at,
                            cycle_id=cycle_id,
                            order_id=order.id,
                            instrument_id=order.instrument_id,
                            market_window_id=window.authority_id,
                        ),
                    )
                    is None
                )
        if host_settings is not None:
            from integration.test_portfolio_management_e2e import _business_facts
            from nexus.persistence.models.paper import PaperOrderEventRecord

            async with sessions() as session:
                stored_order_event = await session.scalar(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq.desc())
                )
                assert stored_order_event.to_status == "filled"
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(PaperFillRecord)
                        .where(PaperFillRecord.order_id == order.id)
                    )
                    == 1
                )
                host_fill_receipt = await read_agent_window_advance_result(
                    session,
                    account_id=account_id,
                    cycle_id=cycle_id,
                    order_id=order.id,
                    market_window_id=window.authority_id,
                )
                assert host_fill_receipt is not None
                assert host_fill_receipt.action == host_fill_receipt.status == "filled"
                before_fill_readback = await _business_facts(session)
        filled = await run_once(settings, request)
        if host_settings is not None:
            async with sessions() as session:
                assert await _business_facts(session) == before_fill_readback
        assert filled["status"] == filled["action"] == "filled", filled
        if formal and manual_settings is None and len(windows) == 1 and count > 1:
            from nexus.paper.manual_request import require_settled_account_cycle_history

            async with sessions.begin() as session:
                # One actual fill with other orders pending is not completion.
                with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                    await require_settled_account_cycle_history(
                        session,
                        account_id=account_id,
                        now=evaluated_at + timedelta(hours=1),
                        allow_scheduled_completion=True,
                    )
        assert (
            await run_once(
                settings,
                request.model_copy(update={"evaluated_at": evaluated_at + timedelta(seconds=3)}),
            )
            == filled
        )
    assert provider.calls == len(orders)
    projection_at = max(
        max(window.window_end for window in windows) + timedelta(seconds=5),
        evaluated_at + timedelta(seconds=3),
    )
    if database_clock_at is not None:
        database_clock_at[0] = projection_at
    async with sessions() as session:
        fills = tuple(await session.scalars(select(PaperFillRecord)))
        assert len(fills) == len(orders)
        assert {fill.order_id for fill in fills} == {order.id for order in orders}
        for fill in fills:
            order = next(order for order in orders if order.id == fill.order_id)
            assert fill.quantity == order.quantity and fill.notional == fill.quantity * fill.price
            adjustments = tuple(
                await session.scalars(
                    select(PaperPositionAdjustmentEventRecord).where(
                        PaperPositionAdjustmentEventRecord.source_fill_id == fill.id
                    )
                )
            )
            assert len(adjustments) == 1
            assert adjustments[0].quantity_delta == fill.quantity
            assert adjustments[0].cost_delta == fill.notional
            entries = tuple(
                await session.scalars(
                    select(PaperLedgerEntryRecord).where(
                        PaperLedgerEntryRecord.transaction_id == fill.ledger_transaction_id
                    )
                )
            )
            assert entries and sum(entry.debit_amount for entry in entries) == sum(
                entry.credit_amount for entry in entries
            )
        projection = await load_paper_projection(
            session,
            account_id=account_id,
            as_of=projection_at,
        )
        assert projection.reserved_cash == 0
        assert projection.available_cash == before.available_cash + before.reserved_cash - sum(
            (fill.notional + fill.fee for fill in fills), start=Decimal(0)
        )
        assert not projection.open_orders
        initial_quantities = {
            position.instrument_id: position.quantity for position in before.positions
        }
        final_quantities = {
            position.instrument_id: position.quantity for position in projection.positions
        }
        for order in orders:
            assert (
                final_quantities[order.instrument_id]
                == initial_quantities.get(order.instrument_id, Decimal(0)) + order.quantity
            )
        assert (await session.get(AgentPaperCycleRecord, cycle_id)).execution_locked is True
        assert await session.scalar(
            select(func.count()).select_from(AgentPaperMarketWindowRecord)
        ) == len(orders)
        assert await session.scalar(
            select(func.count()).select_from(PaperExecutionWindowAuthorityRecord)
        ) == len(orders)
        assert await session.scalar(
            select(func.count())
            .select_from(PaperLedgerTransactionRecord)
            .where(PaperLedgerTransactionRecord.transaction_type == "fill")
        ) == len(orders)
        if verify_reads:
            if not formal:
                with pytest.raises(NexusError, match="order projection is incomplete"):
                    await PaperReadService(
                        session,
                        projection.as_of,
                        cursor_signing_secrets=("controlled-test-cursor",),
                    ).orders(status=None, cursor=None, limit=20)
            page, page_at = await PaperReadService(
                session,
                projection.as_of,
                cursor_signing_secrets=("controlled-test-cursor",),
                candidate_stage_capacity=not formal,
            ).orders(status=None, cursor=None, limit=20)
            assert page_at == projection.as_of and page.next_cursor is None
            assert {item.order_id for item in page.items} == {order.id for order in orders}
            for item in page.items:
                assert item.status == "filled" and not item.cancellable
                assert Decimal(item.remaining_quantity) == 0
                assert Decimal(item.filled_quantity) == Decimal(item.quantity)
                assert len(item.fills) == 1
    if not verify_reads:
        return
    if manual_request_id is not None:
        from nexus.ops.manual_paper_host import finish_configured_manual_request

        await _consume_manual_host_action(manual_settings, "FINISH_MANUAL")
        async with sessions.begin() as session:
            view = await finish_configured_manual_request(
                session, manual_settings, request_id=manual_request_id
            )
            assert view.status == "completed" and view.cycle_id == cycle_id
    from integration.test_portfolio_management_e2e import _business_facts

    async with sessions() as session:
        before_reads = (
            await _manual_recovery_facts(session)
            if manual_request_id
            else await _business_facts(session)
        )
    await _verify_http_reads(
        settings.database_url, cycle_id, orders, projection.as_of, formal=formal
    )
    import os

    if os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1":
        from helpers.controlled_browser import verify_controlled_browser

        await verify_controlled_browser(
            settings.database_url,
            api_now=projection.as_of,
            cycle_id=cycle_id,
            order_ids=tuple(order.id for order in orders),
            candidate_count=count,
            candidate_stage_capacity_reads=not formal,
            formal_readback=formal,
            expected_defer_count=0,
            manual_settings=manual_settings,
            manual_request_id=manual_request_id,
        )
    async with sessions() as session:
        after_reads = (
            await _manual_recovery_facts(session)
            if manual_request_id
            else await _business_facts(session)
        )
        assert after_reads == before_reads


async def _verify_http_reads(database_url, cycle_id, orders, api_now, *, formal=False):
    import httpx
    from helpers.paper_api import app, headers

    paths = (
        "/internal/v1/paper/orders?limit=50",
        "/internal/v1/paper/actual",
        f"/internal/v1/paper/management/cycles/{cycle_id}",
        "/internal/v1/paper/management/portfolio",
        "/internal/v1/paper/cycles?limit=20",
    )
    envelopes = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(database_url, now=api_now)),
        base_url="http://testserver",
    ) as client:
        path = paths[0] + "&candidate_stage_capacity=true"
        rejected = await client.get(path, headers=headers("GET", path))
        if not formal:
            assert (
                rejected.status_code == 409 and rejected.json()["error"]["code"] == "DATA_PARTIAL"
            )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app(database_url, now=api_now, candidate_stage_capacity_reads=not formal)
        ),
        base_url="http://testserver",
    ) as client:
        for path in paths:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, (path, response.json())
            envelopes.append(response.json()["data"])
        unauthenticated = await client.get(paths[0])
        assert unauthenticated.status_code == 401
        if not formal:
            cancel_path = f"/internal/v1/paper/orders/{UUID(int=404)}/cancel"
            body = b'{"expected_event_seq":1}'
            signed = headers("POST", cancel_path, body)
            signed["Idempotency-Key"] = "candidate-read-only-cancel"
            rejected = await client.post(cancel_path, headers=signed, content=body)
            assert rejected.status_code == 409
            assert rejected.json()["error"]["message"] == "Candidate Paper API is read-only."
    assert {item["order_id"] for item in envelopes[0]["items"]} == {
        str(order.id) for order in orders
    }
    assert all(item["status"] == "filled" for item in envelopes[0]["items"])
    assert envelopes[1]["data_state"] == "READY" and len(envelopes[1]["positions"]) == 2
    assert envelopes[2]["cycle"]["cycle_id"] == str(cycle_id)
    assert envelopes[2]["cycle"]["execution_locked"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        "legacy_success",
        "preparation_deadline",
        "success",
        "success_buy",
        "commit_ack_unknown",
        "timeout",
        "cancelled",
        "cancelled_clock_boundary",
        "incomplete",
        "cleanup_error",
        "cancelled_known",
    ],
)
async def test_v11_formal_synthesis_sdk_lifecycle(migrated_database_url, monkeypatch, outcome):
    """Formal immutable model/job authority, actual Worker and SDK; only SDK client doubled."""
    if outcome == "cancelled_clock_boundary":
        import asyncio
        import math

        from nexus.agents import portfolio_synthesis_worker

        loop = asyncio.get_running_loop()
        monotonic = loop.time
        deadline_type = portfolio_synthesis_worker.SynthesisDeadline
        shifted = False

        def at_clock_boundary(started_at):
            nonlocal shifted
            if not shifted:
                # Monotonic origins are arbitrary. Exercise the actual Worker
                # across a binary-float precision boundary without consuming
                # extra time or changing any lifecycle duration.
                origin = math.nextafter(2048.0 - 86.0, math.inf)
                real_origin = monotonic()
                monkeypatch.setattr(loop, "time", lambda: origin + (monotonic() - real_origin))
                started_at = origin
                shifted = True
            return deadline_type(started_at)

        monkeypatch.setattr(portfolio_synthesis_worker, "SynthesisDeadline", at_clock_boundary)
        outcome = "cancelled"
    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url,
        monkeypatch,
        3,
        outcome == "success_buy",
        "formal_v11",
        sdk_lifecycle_case=outcome,
    )
