"""Explicit intraday consent is inert until the resident host admits it."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_workbench_activity_boundaries_pg import AT, scope_fixture
from nexus.ops import manual_paper
from nexus.ops.agent_paper_loop import (
    AgentPaperLoopAction,
    AgentPaperLoopSnapshot,
    choose_loop_action,
    execute_agent_paper_loop_action,
    heartbeat_agent_paper_loop,
)
from nexus.ops.manual_paper_host import claim_configured_manual_request, inspect_manual_queue
from nexus.paper.formal_intraday_intent import advance_intent, consume_intent
from nexus.paper.manual_entry import start_configured_manual_request
from nexus.paper.manual_request import record_manual_request_event
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession


async def effect_counts(sessions):
    tables = (
        "agent_paper_cycles",
        "jobs",
        "job_leases",
        "agent_research_runs",
        "agent_research_outputs",
        "research_stage_authorization",
        "agent_daily_budget_reservations",
        "agent_portfolio_synthesis_budget_reservations",
        "portfolio_decision_budget_reservations",
    )
    async with sessions() as session:
        result = await session.execute(
            text("SELECT " + ", ".join(f"(SELECT count(*) FROM {table})" for table in tables))
        )
        return tuple(result.one())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "admit",
        "sol_admit",
        "cancel",
        "expired",
        "queued_expiry",
        "config_change",
        "unknown",
        "conversion_rolled_back",
        "conversion_committed",
        "first_commit_lost",
        "body_busy_rollback_lost",
        "body_busy_rollback_confirmed",
        "maintenance",
        "economic_first",
        "conversion_first",
        "natural_first",
        "intent_first",
        "close_while_waiting",
        "cancel_race",
    ],
)
async def test_registered_intent_is_durable_without_work_or_second_start(
    migrated_database_url, monkeypatch, case
):
    engine, sessions, authority, _, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    now = [AT]
    scalar = AsyncSession.scalar

    async def clock(session, statement, *args, **kwargs):
        if str(statement) in {
            "SELECT clock_timestamp() AS clock_timestamp_1",
            "SELECT clock_timestamp()",
        }:
            return now[0]
        return await scalar(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_runtime_mode="codex_sdk",
        agent_paper_account_id=authority.account_id,
        agent_paper_candidate_batch_id=batch.batch_id,
        agent_paper_formal_model_config_id=model.id,
        agent_paper_cycle_enabled=True,
        agent_paper_manual_enabled=True,
        release_revision="a" * 40,
    )
    if case in {"sol_admit", "economic_first", "conversion_first"}:
        from integration.test_candidate_multistock_path import _set_test_insert_clock
        from integration.test_sol_profile_pg import current_record, register
        from nexus.settings import AgentRuntimeMode

        await _set_test_insert_clock(sessions, AT - timedelta(seconds=1))
        sol_id = await register(sessions, await current_record(sessions))
        settings = settings.model_copy(
            update={
                "agent_paper_formal_model_config_id": sol_id,
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
                "openai_reasoning_model": "gpt-6.1-sol",
                "openai_extraction_model": "gpt-6.1-sol",
            }
        )
    try:
        async with sessions.begin() as session:
            next_at = AT + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=AT - timedelta(hours=1),
                )
            )
        await heartbeat_agent_paper_loop(
            settings,
            account_id=authority.account_id,
            worker_id=f"agent-paper-loop-{UUID(int=1)}",
            started_at=AT - timedelta(minutes=1),
            seen_at=AT,
        )

        async def invoke(action):
            args = manual_paper.parser().parse_args([action, "--idempotency-key", "one-consent"])
            return await manual_paper.run(settings, args)

        async with sessions.begin() as session:
            prior = await start_configured_manual_request(
                session, settings, mode="research_and_paper", idempotency_key="prior"
            )
        original_effects = await effect_counts(sessions)
        registered = await invoke("register-formal-intent")
        assert await effect_counts(sessions) == original_effects
        assert registered.status == "pending"
        assert await invoke("read-formal-intent") == registered
        assert await invoke("register-formal-intent") == registered
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
                await start_configured_manual_request(
                    session, settings, mode="research_and_paper", idempotency_key="one-consent"
                )
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 1
            prior_row = await session.get(ManualPaperRequest, prior.request_id)
            assert prior_row is not None
            if case == "unknown":
                await record_manual_request_event(
                    session, prior_row, status="preparing", code="MANUAL_PREPARATION_STARTED"
                )
                await record_manual_request_event(
                    session, prior_row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
                )
            else:
                await record_manual_request_event(
                    session, prior_row, status="unavailable", code="MANUAL_INPUTS_UNAVAILABLE"
                )

        async def drive():
            snapshot = AgentPaperLoopSnapshot(
                now=now[0],
                calendar_authority_id=CALENDAR_ID,
                market_open_at=AT - timedelta(minutes=30),
                decision_window_start_at=AT,
                decision_window_end_at=AT + timedelta(hours=6),
                next_decision_window_start_at=AT + timedelta(days=3),
                host_prepared_market_open_at=AT - timedelta(minutes=30),
            )
            pending = await inspect_manual_queue(
                settings, account_id=authority.account_id, snapshot=snapshot
            )
            action = choose_loop_action(pending)
            assert action == AgentPaperLoopAction.ADMIT_FORMAL_INTENT

            async def execute_snapshot():
                await execute_agent_paper_loop_action(
                    settings,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=action,
                    snapshot=pending,
                )

            await asyncio.gather(execute_snapshot(), execute_snapshot())

        if case in {"natural_first", "intent_first"}:
            from nexus.paper.agent_cycle import reserve_schwab_agent_paper_trigger
            from nexus.paper.target_service import lock_portfolio_account

            waiting = asyncio.Event()
            original_execute = AsyncSession.execute

            async def observe(session, statement, *args, **kwargs):
                if session is not owner and "pg_advisory_xact_lock" in str(statement):
                    waiting.set()
                return await original_execute(session, statement, *args, **kwargs)

            async def reserve(session):
                return await reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                    now=now[0],
                    client_idempotency_key="one-natural-slot",
                )

            async def competitor():
                async with sessions.begin() as session:
                    if case == "natural_first":
                        return await advance_intent(
                            session, settings, intent_id=registered.intent_id, convert=False
                        )
                    await lock_portfolio_account(session, authority.account_id)
                    return await reserve(session)

            async with sessions.begin() as owner:
                await lock_portfolio_account(owner, authority.account_id)
                if case == "natural_first":
                    reservation = await reserve(owner)
                else:
                    assert not await advance_intent(
                        owner, settings, intent_id=registered.intent_id, convert=False
                    )
                monkeypatch.setattr(AsyncSession, "execute", observe)
                task = asyncio.create_task(competitor())
                await asyncio.wait_for(waiting.wait(), 10)
            other = await asyncio.wait_for(task, 10)
            if case == "intent_first":
                reservation = other
            else:
                assert other is False
            assert reservation.trigger_mode == "scheduled" and reservation.decision_slot_at == AT
            assert (await invoke("read-formal-intent")).code == "FORMAL_INTENT_DAILY_PRIORITY"
        if case != "unknown":
            await consume_intent(settings, intent_id=registered.intent_id)
            assert (await invoke("read-formal-intent")).code == "FORMAL_INTENT_DAILY_PRIORITY"
        now[0] = AT + timedelta(minutes=7)
        await heartbeat_agent_paper_loop(
            settings,
            account_id=authority.account_id,
            worker_id=f"agent-paper-loop-{UUID(int=1)}",
            started_at=AT - timedelta(minutes=1),
            seen_at=now[0],
        )
        if case in {"natural_first", "intent_first"}:
            await consume_intent(settings, intent_id=registered.intent_id)
            result = await invoke("read-formal-intent")
            assert result.status == "pending" and result.request_id is None
            assert result.code == "MANUAL_PREPARATION_ACK_BLOCKED"
            return
        if case == "cancel":
            assert (await invoke("cancel-formal-intent")).status == "cancelled"
            await consume_intent(settings, intent_id=registered.intent_id)
            assert (await invoke("read-formal-intent")).request_id is None
            return
        if case == "expired":
            now[0] = registered.expires_at
            assert (await invoke("read-formal-intent")).status == "expired"
            await consume_intent(settings, intent_id=registered.intent_id)
            assert (await invoke("read-formal-intent")).request_id is None
            return
        if case == "config_change":
            settings.agent_daily_budget_usd += 1
            await consume_intent(settings, intent_id=registered.intent_id)
            assert (await invoke("read-formal-intent")).status == "rejected"
            return
        if case == "unknown":
            await consume_intent(settings, intent_id=registered.intent_id)
            blocked = await invoke("read-formal-intent")
            assert blocked.status == "pending" and blocked.code == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
            assert blocked.request_id is None
            return
        if case == "maintenance":
            from nexus.agents import research_maintenance

            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
            assert (await invoke("read-formal-intent")).status == "pending"
            with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
                await consume_intent(settings, intent_id=registered.intent_id)
            return
        if case.startswith("body_busy_"):
            from nexus.agents.budget import ResearchAccountingBusy
            from nexus.paper import formal_intraday_intent as intent

            original = intent.advance_intent

            async def busy(session, settings, *, intent_id, convert):
                if convert:
                    raise ResearchAccountingBusy()
                return await original(session, settings, intent_id=intent_id, convert=convert)

            with monkeypatch.context() as fault:
                fault.setattr(intent, "advance_intent", busy)
                if case == "body_busy_rollback_lost":
                    rollback = AsyncSession.rollback

                    async def lost_rollback(session):
                        await rollback(session)
                        raise ResearchAccountingBusy()

                    fault.setattr(AsyncSession, "rollback", lost_rollback)
                    with pytest.raises(ResearchAccountingBusy):
                        await consume_intent(settings, intent_id=registered.intent_id)
                else:
                    await consume_intent(settings, intent_id=registered.intent_id)
            result = await invoke("read-formal-intent")
            assert result.status == ("checking" if case.endswith("lost") else "pending")
            assert result.request_id is None
            return
        if case in {"conversion_committed", "conversion_rolled_back", "first_commit_lost"}:
            from nexus.persistence import session as storage

            class LostAcknowledgement:
                def __init__(self, real):
                    self.real = real
                    self.kw = real.kw
                    self.transactions = 0

                @asynccontextmanager
                async def begin(self):
                    self.transactions += 1
                    if self.transactions != (1 if case == "first_commit_lost" else 2):
                        async with self.real.begin() as session:
                            yield session
                        return
                    async with self.real() as session:
                        await session.begin()
                        yield session
                        if case in {"conversion_committed", "first_commit_lost"}:
                            await session.commit()
                        else:
                            await session.rollback()
                        raise TimeoutError("simulated acknowledgement loss")

            original_factory = storage.session_factory
            with monkeypatch.context() as fault:
                fault.setattr(
                    storage,
                    "session_factory",
                    lambda config: LostAcknowledgement(original_factory(config)),
                )
                with pytest.raises(TimeoutError, match="simulated acknowledgement loss"):
                    await consume_intent(settings, intent_id=registered.intent_id)
            # No commit result is provided to the next host. Only read persisted facts.
            await consume_intent(settings, intent_id=registered.intent_id)
            result = await invoke("read-formal-intent")
            assert result.status == ("converted" if case == "conversion_committed" else "checking")
            async with sessions.begin() as session:
                assert (
                    await session.get(ManualPaperRequest, registered.intent_id) is not None
                ) == (case == "conversion_committed")
            if case == "first_commit_lost":
                # Another session's registration can expire the fenced identity
                # only after taking the conversion lock and proving no request exists.
                now[0] = AT + timedelta(days=3)
                async with sessions.begin() as session:
                    next_at = now[0] + timedelta(days=1)
                    session.add(
                        ExchangeCalendarSessionRecord(
                            calendar_authority_id=CALENDAR_ID,
                            session=next_at.date(),
                            decision_at=next_at.replace(hour=21),
                            market_open_at=next_at.replace(hour=13, minute=30),
                            market_close_at=next_at.replace(hour=20),
                            created_at=AT - timedelta(hours=1),
                        )
                    )
                args = manual_paper.parser().parse_args(
                    ["register-formal-intent", "--idempotency-key", "next-day-explicit"]
                )
                later = await manual_paper.run(settings, args)
                assert later.intent_id != registered.intent_id and later.status == "pending"
                expired = await invoke("read-formal-intent")
                assert expired.status == "expired" and expired.code == "FORMAL_INTENT_EXPIRED"
                await consume_intent(settings, intent_id=registered.intent_id)
                async with sessions() as session:
                    from nexus.persistence.models.manual_paper import FormalIntradayIntent

                    assert (
                        await session.get(FormalIntradayIntent, registered.intent_id)
                    ).state == "expired"
                    assert await session.get(ManualPaperRequest, registered.intent_id) is None
            return
        if case in {"economic_first", "close_while_waiting", "cancel_race", "conversion_first"}:
            from decimal import Decimal

            from nexus.agents.persistence import _lock_budget_day
            from nexus.audit.service import append_audit_event
            from nexus.paper.formal_budget_protection import apply_protection
            from nexus.paper.target_service import lock_portfolio_account
            from nexus.persistence.models.agents import FormalBudgetBindingRecord

            waited = asyncio.Event()
            execute = AsyncSession.execute

            async def observe(session, statement, *args, **kwargs):
                if session is not holder and "pg_advisory_xact_lock" in str(statement):
                    waited.set()
                return await execute(session, statement, *args, **kwargs)

            async with sessions.begin() as holder:
                monkeypatch.setattr(AsyncSession, "execute", observe)
                if case == "conversion_first":
                    assert await advance_intent(
                        holder, settings, intent_id=registered.intent_id, convert=False
                    )
                else:
                    await lock_portfolio_account(holder, authority.account_id)
                    task = asyncio.create_task(
                        consume_intent(settings, intent_id=registered.intent_id)
                    )
                    await asyncio.wait_for(waited.wait(), 10)
                    if case == "economic_first":
                        await _lock_budget_day(holder, now[0].date().isoformat())
                        audit = await append_audit_event(
                            holder,
                            actor_id="fixture",
                            action="fixture.binding",
                            schema_version="fixture",
                            payload={},
                            occurred_at=now[0],
                        )
                        holder.add(
                            FormalBudgetBindingRecord(
                                budget_domain="agent_global_v1",
                                business_date=now[0].date(),
                                sequence=1,
                                previous_id=None,
                                idempotency_key="fixture-binding",
                                content_hash="b" * 64,
                                schema_version="formal_budget_binding_v1",
                                daily_limit_usd=3,
                                payload={},
                                applied_at=now[0],
                                audit_event_id=audit.id,
                            )
                        )
                    elif case == "cancel_race":
                        from nexus.paper.formal_intraday_intent import intent_command

                        assert (
                            await intent_command(
                                holder, settings, action="cancel-formal-intent", key="one-consent"
                            )
                        ).status == "cancelled"
                    else:
                        now[0] = registered.expires_at
            if case == "conversion_first":

                async def bind():
                    async with sessions.begin() as session:
                        await apply_protection(
                            session,
                            account_id=authority.account_id,
                            batch_id=batch.batch_id,
                            enabled=True,
                            daily_limit_usd=Decimal("1000"),
                            configuration_hash="c" * 64,
                            idempotency_key="economic",
                            now=now[0],
                            formal_model_config_id=model.id,
                        )

                async with sessions.begin() as holder:
                    assert await advance_intent(
                        holder, settings, intent_id=registered.intent_id, convert=True
                    )
                    waited.clear()
                    writer = asyncio.create_task(bind())
                    await asyncio.wait_for(waited.wait(), 10)
                with pytest.raises(ValueError, match="FORMAL_BUDGET_SLOT_ALREADY_CLAIMED"):
                    await writer
                assert (await invoke("read-formal-intent")).status == "converted"
            else:
                await asyncio.wait_for(task, 10)
                result = await invoke("read-formal-intent")
                assert result.request_id is None
                assert (
                    result.code
                    == {
                        "economic_first": "FORMAL_BUDGET_SLOT_ALREADY_CLAIMED",
                        "cancel_race": "FORMAL_INTENT_CANCELLED",
                        "close_while_waiting": "FORMAL_INTENT_EXPIRED",
                    }[case]
                )
            return
        # Two execution fragments holding the same inspection may race.
        await drive()
        admitted = await invoke("read-formal-intent")
        assert admitted.status == "converted" and admitted.request_id == registered.intent_id

        await asyncio.gather(
            *[consume_intent(settings, intent_id=registered.intent_id) for _ in range(2)]
        )

        async def claim():
            async with sessions.begin() as session:
                return await claim_configured_manual_request(
                    session, settings, request_id=admitted.request_id
                )

        if case == "queued_expiry":
            now[0] = registered.expires_at
            terminal, claimed = await claim()
            assert not claimed and terminal.status == "unavailable"
            return
        results = await asyncio.gather(claim(), claim())
        assert sorted(owned for _, owned in results) == [False, True]
        assert all(view.status == "preparing" for view, _ in results)
        assert await effect_counts(sessions) == original_effects
        assert await invoke("register-formal-intent") == admitted
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 2
    finally:
        await engine.dispose()
