"""Manual identity flows through native admission without live provider/model calls."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle, manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.parametrize("candidate_count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.asyncio
async def test_manual_native_cycle_binds_request_and_replays_without_new_jobs(
    migrated_database_url,
    monkeypatch,
    candidate_count,
    mode,
    failure_status=None,
    cancel_unstarted=False,
    cancel_fault=False,
    profile="v8",
):
    now = AT + timedelta(minutes=17, seconds=31)
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(
            (V9_OPTIONS | {"formal_flexible_stage_budget": True})
            if profile == "v10"
            else V9_OPTIONS
            if profile == "v9"
            else OPTIONS
        ),
    )
    candidates = (
        INSTRUMENTS
        if candidate_count == 3
        else INSTRUMENTS
        + (
            (UUID(int=101), Decimal("70"), Decimal("0"), "industrials"),
            (UUID(int=102), Decimal("80"), Decimal("0"), "consumer_staples"),
        )
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=candidates, decision_time=now
    )
    scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return now
        return await scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    try:
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version
                    == (
                        "codex-sdk-chatgpt-v10-flexible-stage-budget"
                        if profile == "v10"
                        else "codex-sdk-chatgpt-v9-full-input-capacity"
                        if profile == "v9"
                        else "codex-sdk-chatgpt-v8-stage-capacity"
                    )
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="manual-full-batch",
                created_at=now,
            )
            request = await manual_request.admit_manual_request(
                session,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode=mode,
                idempotency_key="manual-start",
            )
            view, allowed = await manual_request.claim_manual_request(
                session, request_id=request.request_id
            )
            assert allowed and view.status == "preparing"
        args = dict(
            account_id=authority.account_id,
            candidate_batch_id=batch.batch_id,
            client_idempotency_key=f"manual-entry:{request.request_id}",
            trigger_mode="manual",
            manual_request_id=request.request_id,
            formal_stage_capacity=True,
            formal_model_config_id=model.id,
        )
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session, now=now, **args
            )
            assert (
                reservation.decision_slot_at == now
                and reservation.manual_request_id == request.request_id
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session, now=now + timedelta(seconds=1), **args
                )
        start_args = dict(
            authority=authority,
            decision_slot_at=now,
            idempotency_key=args["client_idempotency_key"],
            daily_limit_usd=Decimal("3"),
            requested_instrument_ids=batch.instrument_ids,
            candidate_batch_id=batch.batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=model.id,
            manual_request_id=request.request_id,
        )
        async with sessions.begin() as session:
            result = await agent_cycle.start_schwab_agent_paper_cycle(session, **start_args)
            assert (
                result.status == "researching" and len(result.research_run_ids) == candidate_count
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=reservation, result=result
            )
            row = await session.get(ManualPaperRequest, request.request_id)
            view = await manual_request.read_manual_request(session, row)
            assert view.status == "active" and view.cycle_id == result.cycle_id
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            if mode == "research_only":
                with pytest.raises(ValueError, match="MANUAL_RESEARCH_ONLY_EXECUTION_FORBIDDEN"):
                    await manual_request.require_cycle_execution_mode(
                        session, cycle_id=result.cycle_id
                    )
            else:
                await manual_request.require_cycle_execution_mode(session, cycle_id=result.cycle_id)
        if failure_status is not None:
            from nexus.ops.manual_paper_host import finish_configured_manual_request
            from nexus.persistence.models.agents import AgentResearchRunRecord
            from nexus.persistence.models.runtime import Job
            from sqlalchemy import update

            close_settings = EngineSettings(
                database_url=migrated_database_url, agent_paper_account_id=authority.account_id
            )
            async with sessions.begin() as session:
                cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
                events = await agent_cycle._events(session, cycle.id)
                if failure_status == "failed":
                    await agent_cycle._stop_failed(
                        session, cycle, events, "RESEARCH_OUTPUT_NOT_ACTIONABLE"
                    )
                else:
                    await agent_cycle._stop_unavailable(session, cycle, events)
            now += timedelta(seconds=5)
            if cancel_unstarted:
                from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
                from nexus.persistence.models.runtime import JobAttempt

                if cancel_fault:
                    async with sessions.begin() as session:
                        first_run = await session.scalar(
                            select(AgentResearchRunRecord)
                            .order_by(AgentResearchRunRecord.id)
                            .limit(1)
                        )
                        first_run.input_tokens = 1
                    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                        async with sessions.begin() as session:
                            await finish_configured_manual_request(
                                session,
                                close_settings,
                                request_id=request.request_id,
                                cancel_unused=True,
                            )
                    async with sessions() as session:
                        row = await session.get(ManualPaperRequest, request.request_id)
                        assert (
                            await manual_request.read_manual_request(session, row)
                        ).status == "active"
                        assert all(
                            job.status == "queued" and job.attempt_count == 0
                            for job in await session.scalars(select(Job))
                        )
                        assert (
                            await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
                        )
                        assert all(
                            budget.state == "reserved"
                            for budget in await session.scalars(
                                select(AgentDailyBudgetReservationRecord)
                            )
                        )
                    return
                async with sessions.begin() as session:
                    terminal = await finish_configured_manual_request(
                        session, close_settings, request_id=request.request_id, cancel_unused=True
                    )
                    assert terminal.status == "unavailable"
                    jobs = tuple(await session.scalars(select(Job)))
                    assert len(jobs) == candidate_count
                    assert all(
                        job.status == "skipped"
                        and job.result_code == "MANUAL_UNUSED_RESEARCH_CANCELLED"
                        for job in jobs
                    )
                    attempts = tuple(await session.scalars(select(JobAttempt)))
                    assert len(attempts) == candidate_count and all(
                        attempt.outcome == "skipped" for attempt in attempts
                    )
                    runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
                    assert all(
                        run.status == "abstained"
                        and run.settled_cost_usd == 0
                        and run.input_tokens == run.output_tokens == 0
                        for run in runs
                    )
                    budgets = tuple(
                        await session.scalars(select(AgentDailyBudgetReservationRecord))
                    )
                    assert all(budget.state == "released" for budget in budgets)
                    assert (
                        await finish_configured_manual_request(
                            session,
                            close_settings,
                            request_id=request.request_id,
                            cancel_unused=True,
                        )
                        == terminal
                    )
                    independent = await manual_request.admit_manual_request(
                        session,
                        account_id=authority.account_id,
                        batch_id=batch.batch_id,
                        model_config_id=model.id,
                        mode=mode,
                        idempotency_key="after-native-cancellation",
                    )
                    assert (
                        independent.request_id != request.request_id
                        and independent.status == "queued"
                    )
                return
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await finish_configured_manual_request(
                        session, close_settings, request_id=request.request_id
                    )
                row = await session.get(ManualPaperRequest, request.request_id)
                assert (await manual_request.read_manual_request(session, row)).status == "active"
            # Controlled terminal fixtures test the guard; no model execution is implied.
            async with sessions.begin() as session:
                await session.execute(
                    update(Job).values(status="running", version=Job.version + 1, started_at=now)
                )
                await session.execute(
                    update(Job).values(status="failed", version=Job.version + 1, finished_at=now)
                )
            async with sessions.begin() as session:
                unknown_fixture = await session.begin_nested()
                await session.execute(
                    update(AgentResearchRunRecord).values(status="failed", settled_cost_usd=None)
                )
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await finish_configured_manual_request(
                        session, close_settings, request_id=request.request_id, cancel_unused=True
                    )
                await unknown_fixture.rollback()
            async with sessions.begin() as session:
                await session.execute(
                    update(AgentResearchRunRecord).values(
                        status="failed", settled_cost_usd=Decimal("0")
                    )
                )
            async with sessions.begin() as session:
                terminal = await finish_configured_manual_request(
                    session, close_settings, request_id=request.request_id
                )
                assert terminal.status == "unavailable" and terminal.cycle_id == result.cycle_id
                assert terminal.code == "MANUAL_INPUTS_UNAVAILABLE"
                assert (
                    await finish_configured_manual_request(
                        session, close_settings, request_id=request.request_id
                    )
                    == terminal
                )
                assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
                independent = await manual_request.admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode=mode,
                    idempotency_key="after-confirmed-terminal",
                )
                assert independent.request_id != request.request_id
                assert independent.status == "queued" and independent.cycle_id is None
            return
        async with sessions.begin() as session:
            replay = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session, now=now + timedelta(seconds=2), **args
            )
            assert replay.replay.cycle_id == result.cycle_id and replay.replay.replayed
            assert await agent_cycle.start_schwab_agent_paper_cycle(
                session, **start_args
            ) == result.model_copy(update={"replayed": True})
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("candidate_count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.parametrize("failure_status", ["failed", "unavailable"])
@pytest.mark.asyncio
async def test_manual_native_terminal_reconciliation_requires_settled_jobs(
    migrated_database_url, monkeypatch, candidate_count, mode, failure_status
):
    await test_manual_native_cycle_binds_request_and_replays_without_new_jobs(
        migrated_database_url, monkeypatch, candidate_count, mode, failure_status=failure_status
    )


@pytest.mark.parametrize("candidate_count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.parametrize("cancel_fault", [False, True])
@pytest.mark.asyncio
async def test_manual_unstarted_research_cancels_without_model_or_budget_usage(
    migrated_database_url, monkeypatch, candidate_count, mode, cancel_fault
):
    await test_manual_native_cycle_binds_request_and_replays_without_new_jobs(
        migrated_database_url,
        monkeypatch,
        candidate_count,
        mode,
        failure_status="failed",
        cancel_unstarted=True,
        cancel_fault=cancel_fault,
    )


@pytest.mark.parametrize("candidate_count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.parametrize("cancel_fault", [False, True])
@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["v9", "v10"])
async def test_manual_v9_unstarted_research_cancels_without_usage(
    migrated_database_url, monkeypatch, candidate_count, mode, cancel_fault, profile
):
    await test_manual_native_cycle_binds_request_and_replays_without_new_jobs(
        migrated_database_url,
        monkeypatch,
        candidate_count,
        mode,
        failure_status="failed",
        cancel_unstarted=True,
        cancel_fault=cancel_fault,
        profile=profile,
    )


@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.asyncio
async def test_manual_research_only_completes_native_outputs_without_execution(
    migrated_database_url, monkeypatch, count
):
    from integration.test_formal_synthesis_admission import (
        test_formal_multistock_research_admits_exact_portfolio_synthesis,
    )

    await test_formal_multistock_research_admits_exact_portfolio_synthesis(
        migrated_database_url,
        monkeypatch,
        count=count,
        trade=True,
        protected=False,
        manual_mode="research_only",
    )


@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.asyncio
async def test_manual_paper_native_host_order_fill_ledger_readback(
    migrated_database_url, monkeypatch, count
):
    from integration.test_formal_synthesis_admission import (
        test_formal_multistock_research_admits_exact_portfolio_synthesis,
    )

    await test_formal_multistock_research_admits_exact_portfolio_synthesis(
        migrated_database_url,
        monkeypatch,
        count=count,
        trade=True,
        protected=False,
        manual_mode="research_and_paper",
    )


@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.asyncio
async def test_manual_successful_research_consumed_by_normal_foreground(
    migrated_database_url, monkeypatch, count, mode
):
    from integration.test_formal_synthesis_admission import (
        test_formal_multistock_research_admits_exact_portfolio_synthesis,
    )

    await test_formal_multistock_research_admits_exact_portfolio_synthesis(
        migrated_database_url,
        monkeypatch,
        count=count,
        trade=True,
        protected=False,
        manual_mode=mode,
        manual_consumer=True,
    )
