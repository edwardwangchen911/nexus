"""Explicit no-trigger closure preserves unknown history and admits a distinct request."""

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops import manual_paper
from nexus.ops.openai_local import prepare_database
from nexus.paper import manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_entry import manual_host_scope
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "claim",
        "pending-claim",
        "malformed-pending",
        "version",
        "heartbeat-before-preparing",
        "ambiguous",
        "ambiguous-late",
        "fresh",
        "live",
        "late-live",
        "late-claim",
        "native-cycle",
    ],
)
@pytest.mark.asyncio
async def test_no_trigger_close_retains_unknown_and_fences_new_admission(
    migrated_database_url, monkeypatch, fault
):
    from nexus.paper import manual_preparation_close as close

    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(V9_OPTIONS | {"formal_flexible_stage_budget": True}),
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT
    )
    clock = [AT]
    original = AsyncSession.scalar

    async def db_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock[0]
        return await original(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", db_clock)
    try:
        async with sessions.begin() as s:
            next_at = AT + timedelta(days=3)
            s.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=AT - timedelta(hours=1),
                )
            )
            model = await s.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                s,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(i.instrument_id for i in manifest),
                idempotency_key="full-pool",
                created_at=AT,
            )
            old = await manual_request.admit_manual_request(
                s,
                account_id=authority.account_id,
                batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                idempotency_key="failed-preparation",
            )
            await manual_request.claim_manual_request(s, request_id=old.request_id)
            row = await s.get(ManualPaperRequest, old.request_id)
            cycle_id = None
            if fault == "native-cycle":
                from nexus.paper import agent_cycle

                result = await agent_cycle.start_schwab_agent_paper_cycle(
                    s,
                    authority=authority,
                    decision_slot_at=AT,
                    idempotency_key="manual-entry:" + str(row.id),
                    daily_limit_usd=__import__("decimal").Decimal("3"),
                    requested_instrument_ids=batch.instrument_ids,
                    candidate_batch_id=batch.batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                    manual_request_id=row.id,
                )
                cycle_id = result.cycle_id
            await manual_request.record_manual_request_event(
                s, row, status="unknown", code="MANUAL_RESULT_UNKNOWN", cycle_id=cycle_id
            )
            settings = EngineSettings(
                database_url=migrated_database_url,
                release_revision="a" * 40,
                agent_runtime_mode="codex_sdk",
                agent_paper_account_id=authority.account_id,
                agent_paper_candidate_batch_id=batch.batch_id,
                agent_paper_formal_model_config_id=model.id,
                agent_paper_manual_enabled=True,
                agent_paper_cycle_enabled=True,
            )
            scope = manual_host_scope(settings)
            h = WorkerHeartbeat(
                queue="agent_paper",
                worker_id=f"agent-paper-loop-{UUID(int=10)}:manual:{scope}",
                process_version="nexus-manual-paper-v1+"
                + ("a" * 40 if fault == "version" else close.QUALIFIED_ORIGINAL_REVISION),
                migration_head="0078_formal_v10_output_binding",
                started_at=AT - timedelta(minutes=1),
                seen_at=AT - timedelta(milliseconds=482)
                if fault == "heartbeat-before-preparing"
                else AT,
            )
            s.add(h)
            if fault in {"ambiguous", "ambiguous-late"}:
                s.add(
                    WorkerHeartbeat(
                        queue="agent_paper",
                        worker_id=f"agent-paper-loop-{UUID(int=11)}:manual:{scope}",
                        process_version=(
                            "nexus-manual-paper-v1+" + "b" * 40
                            if fault == "ambiguous-late"
                            else h.process_version
                        ),
                        migration_head=h.migration_head,
                        started_at=h.started_at,
                        seen_at=h.seen_at
                        + (timedelta(seconds=1) if fault == "ambiguous-late" else timedelta()),
                    )
                )
            if fault in {"claim", "pending-claim", "malformed-pending"}:
                s.add(
                    IdempotencyRecord(
                        actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                        route="agent-paper-explicit-manual-v1",
                        idempotency_key=str(old.request_id) if fault == "claim" else "other",
                        request_hash="a" * 64,
                        schema_version="agent_paper_explicit_manual_v1",
                        response_payload={"invalid": "unknown"}
                        if fault == "malformed-pending"
                        else {},
                    )
                )
        from argparse import Namespace
        from contextlib import nullcontext

        monkeypatch.setattr(
            manual_paper, "preparation_service_control_lock", lambda *_a: nullcontext()
        )
        monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_a: None)
        args = Namespace(
            action="close-unstarted-preparation",
            request_id=old.request_id,
            repo_root=Path.cwd(),
            env_file=Path("infra/.env.local"),
        )
        async with sessions() as s:
            initial_budgets = [
                (str(b.id), b.state, b.settled_usd)
                for b in await s.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ]
        clock[0] = AT + timedelta(seconds=5 if fault == "fresh" else 120)
        calls = []

        def process_guard(*args, **kwargs):
            calls.append("inspect")
            if fault == "live" or fault == "late-live" and len(calls) == 2:
                raise ValueError("MANUAL_PREPARATION_CLOSE_BLOCKED")

        monkeypatch.setattr(close, "require_preparation_producers_stopped", process_guard)
        async with sessions.begin() as s:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await manual_request.require_manual_account_available(
                    s, account_id=authority.account_id, now=clock[0]
                )
        if fault not in {"none", "late-claim"}:
            with pytest.raises(ValueError):
                async with sessions.begin() as s:
                    await manual_paper.run(settings, args)
        else:
            async with sessions.begin() as s:
                receipt = await manual_paper.run(settings, args)
                assert receipt.status == "unknown_preparation_closed" and not receipt.replayed
                again = await manual_paper.run(settings, args)
                assert again.replayed
            async with sessions.begin() as s:
                row = await s.get(ManualPaperRequest, old.request_id)
                assert (await manual_request.read_manual_request(s, row)).status == "unknown"
                assert (
                    await s.scalar(
                        select(func.count())
                        .select_from(ManualPaperRequestEvent)
                        .where(ManualPaperRequestEvent.request_id == row.id)
                    )
                    == 3
                )
                assert (
                    await s.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == close.ACTION)
                    )
                    == 1
                )
                if fault == "late-claim":
                    s.add(
                        IdempotencyRecord(
                            actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                            route="agent-paper-explicit-manual-v1",
                            idempotency_key=str(row.id),
                            request_hash="a" * 64,
                            schema_version="agent_paper_explicit_manual_v1",
                            response_payload={},
                        )
                    )
            async with sessions.begin() as s:
                if fault == "late-claim":
                    with pytest.raises(ValueError):
                        await manual_request.require_manual_account_available(
                            s, account_id=authority.account_id, now=clock[0]
                        )
                else:
                    start_args = Namespace(
                        action="start", mode="research_and_paper", idempotency_key="distinct-v10"
                    )
                    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
                        await manual_paper.run(settings, start_args)
                    async with sessions.begin() as host_session:
                        host_session.add(
                            WorkerHeartbeat(
                                queue="agent_paper",
                                worker_id=f"agent-paper-loop-{UUID(int=12)}:manual:{scope}",
                                process_version="nexus-manual-paper-v1+" + "a" * 40,
                                migration_head=CURRENT_MIGRATION_HEAD,
                                started_at=clock[0],
                                seen_at=clock[0],
                            )
                        )
                    fresh = await manual_paper.run(settings, start_args)
                    assert fresh.request_id != old.request_id
                    from nexus.ops.manual_paper_host import peek_manual_request

                    assert (
                        await peek_manual_request(s, account_id=authority.account_id)
                    ).request_id == fresh.request_id
                    view, claimed = await manual_request.claim_manual_request(
                        s, request_id=fresh.request_id
                    )
                    assert claimed and view.status == "preparing"
                    from nexus.paper import agent_cycle

                    with pytest.raises(ValueError, match="MANUAL_REQUEST_TERMINAL_OR_UNKNOWN"):
                        await agent_cycle.reserve_schwab_agent_paper_trigger(
                            s,
                            account_id=authority.account_id,
                            candidate_batch_id=batch.batch_id,
                            client_idempotency_key="manual-entry:" + str(old.request_id),
                            trigger_mode="manual",
                            manual_request_id=old.request_id,
                            formal_stage_capacity=True,
                            formal_model_config_id=model.id,
                            now=clock[0],
                        )
                    reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                        s,
                        account_id=authority.account_id,
                        candidate_batch_id=batch.batch_id,
                        client_idempotency_key="manual-entry:" + str(fresh.request_id),
                        trigger_mode="manual",
                        manual_request_id=fresh.request_id,
                        formal_stage_capacity=True,
                        formal_model_config_id=model.id,
                        now=clock[0],
                    )
                    assert reservation.manual_request_id == fresh.request_id
        if fault not in {"none", "late-claim"}:
            async with sessions() as s:
                assert (
                    await s.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == close.ACTION)
                    )
                    == 0
                )
                assert (
                    await manual_request.read_manual_request(
                        s, await s.get(ManualPaperRequest, old.request_id)
                    )
                ).status == "unknown"
        async with sessions() as s:
            final_budgets = [
                (str(b.id), b.state, b.settled_usd)
                for b in await s.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ]
            assert final_budgets == initial_budgets
    finally:
        await engine.dispose()
