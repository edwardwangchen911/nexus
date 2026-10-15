"""Five explicit historical scheduling exceptions never settle unknown effects."""

import asyncio
from argparse import Namespace
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops import manual_paper
from nexus.ops.openai_local import prepare_database
from nexus.paper import manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "missing",
        "extra",
        "extra-malformed",
        "wrong-account",
        "nonempty",
        "future",
        "paid",
        "protected",
        "live",
        "late-live",
        "late-claim",
        "changed-claim",
        "audit-tamper",
        "concurrent-claim",
        "concurrent-budget",
        "occupied",
        "active-cycle",
        "wrong-cycle",
        "unaccepted",
    ],
)
@pytest.mark.asyncio
async def test_explicit_unknown_batch_is_atomic_and_does_not_replay(
    migrated_database_url, monkeypatch, fault
):
    from nexus.paper import manual_preparation_close as close
    from nexus.paper import preparation_unknown_ack as ack

    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **(V9_OPTIONS | {"formal_flexible_stage_budget": True}),
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        decision_time=AT,
        candidate_instruments=INSTRUMENTS,
        occupied=fault == "occupied",
    )
    clock = [AT]
    original = AsyncSession.scalar

    async def db_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock[0]
        return await original(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", db_clock)
    calls = []

    def stopped(*_a, **_k):
        calls.append("inspect")
        if fault == "live" or fault == "late-live" and len(calls) == 2:
            raise ValueError(ack.BLOCKED)

    monkeypatch.setattr(close, "require_preparation_producers_stopped", stopped)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_a: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_a: nullcontext())
    try:
        async with sessions.begin() as s:
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
                idempotency_key="ack-full-pool",
                created_at=AT,
            )
            if fault in {"active-cycle", "wrong-cycle"}:
                from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle

                cycle_result = await start_schwab_agent_paper_cycle(
                    s,
                    authority=authority,
                    decision_slot_at=AT,
                    idempotency_key="earlier-active-cycle",
                    requested_instrument_ids=batch.instrument_ids,
                    candidate_batch_id=batch.batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
            if fault in {"occupied", "active-cycle", "wrong-cycle"}:
                # Seed a historical unknown alongside an existing business occupancy.
                # The maintenance entry must not use its exception to bypass that occupancy.
                historical = ManualPaperRequest(
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    model_config_id=model.id,
                    model_hash=model.config_hash,
                    batch_hash=batch.batch_hash,
                    mode="research_and_paper",
                    key_hash="a" * 64,
                    request_hash=manual_request.manual_request_hash(
                        account_id=authority.account_id,
                        batch_id=batch.batch_id,
                        mode="research_and_paper",
                    ),
                    requested_at=AT,
                )
                s.add(historical)
                await s.flush()
                await manual_request._append(
                    s, historical, status="queued", code="MANUAL_REQUEST_QUEUED", at=AT
                )
                old = await manual_request._append(
                    s, historical, status="preparing", code="MANUAL_PREPARATION_STARTED", at=AT
                )
            else:
                old = await manual_request.admit_manual_request(
                    s,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_and_paper",
                    idempotency_key="unknown",
                )
                await manual_request.claim_manual_request(s, request_id=old.request_id)
            row = await s.get(ManualPaperRequest, old.request_id)
            await manual_request.record_manual_request_event(
                s, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
            )
            claims = []
            for i in range(4 + (fault in {"extra", "extra-malformed"})):
                slot = AT - timedelta(days=2, hours=i)
                c = IdempotencyRecord(
                    actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                    route="agent-paper-schwab-trigger-v2",
                    schema_version="agent_paper_schwab_trigger_v2",
                    idempotency_key=slot.isoformat(),
                    request_hash=str(i + 1) * 64,
                    response_payload={},
                    created_at=slot,
                )
                if fault == "extra-malformed" and i == 4:
                    c.response_payload = {"outcome": "completed"}
                s.add(c)
                claims.append(c)
            if fault == "wrong-cycle":
                extra = IdempotencyRecord(
                    actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                    route="agent-paper-schwab-trigger-v2",
                    schema_version="agent_paper_schwab_trigger_v2",
                    idempotency_key=(AT - timedelta(days=3)).isoformat(),
                    request_hash="e" * 64,
                    response_payload={
                        "outcome": "completed",
                        "result": cycle_result.model_dump(mode="json"),
                    },
                )
                s.add(extra)
                await s.flush()
                assert not await ack._terminal_claim(s, extra, account_id=authority.account_id)
                correct = IdempotencyRecord(
                    actor_id=extra.actor_id,
                    route=extra.route,
                    schema_version=extra.schema_version,
                    idempotency_key=AT.isoformat(),
                    request_hash="e" * 64,
                    response_payload=extra.response_payload,
                )
                assert await ack._terminal_claim(s, correct, account_id=authority.account_id)
            await s.flush()
            ids = tuple(c.id for c in claims[:4])
            if fault == "nonempty":
                claims[0].response_payload = {
                    "outcome": "failed",
                    "error_code": "RISK_INPUTS_UNAVAILABLE",
                }
            if fault == "future":
                claims[0].idempotency_key = (AT + timedelta(days=1)).isoformat()
            if fault == "wrong-account":
                claims[0].actor_id = "other-account"
            selected_model_id = model.id
            if fault == "paid":
                values = {
                    c.key: getattr(model, c.key)
                    for c in model.__mapper__.column_attrs
                    if c.key != "id"
                }
                values.update(version="paid-fixture", input_usd_per_million=1)
                paid_model = AgentModelConfigRecord(**values)
                s.add(paid_model)
                await s.flush()
                selected_model_id = paid_model.id
            prompt = await s.scalar(
                select(AgentPromptVersionRecord)
                .where(AgentPromptVersionRecord.role == "orchestrator")
                .limit(1)
            )
            independent_run = AgentResearchRunRecord(
                instrument_id=manifest[0].instrument_id,
                data_snapshot_id=manifest[0].research_snapshot_id,
                prompt_version_id=prompt.id,
                model_config_id=model.id,
                decision_time=AT,
                actor_id="independent-budget-fixture",
                idempotency_key="unsettled-existing",
                status="failed",
                schema_hash="a" * 64,
                model_hash=model.config_hash,
                prompt_hash=prompt.content_hash,
                prompt_manifest={},
                evidence_hash="b" * 64,
                evidence_manifest=[{"source": "fixture"}],
                tool_hash="c" * 64,
                input_hash="d" * 64,
                reserved_cost_usd=0,
                settled_cost_usd=None,
                pricing_version=model.version,
                input_tokens=None,
                output_tokens=None,
            )
            s.add(independent_run)
            await s.flush()
            s.add(
                AgentDailyBudgetReservationRecord(
                    business_date=AT.date(),
                    run_id=independent_run.id,
                    pricing_version=model.version,
                    reserved_usd=0,
                    settled_usd=None,
                    state="reserved",
                )
            )
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
        settings = EngineSettings(
            database_url=migrated_database_url,
            release_revision="a" * 40,
            agent_runtime_mode="codex_sdk",
            agent_paper_account_id=authority.account_id,
            agent_paper_candidate_batch_id=batch.batch_id,
            agent_paper_formal_model_config_id=selected_model_id,
            agent_paper_manual_enabled=True,
            agent_paper_cycle_enabled=True,
            formal_budget_protection_enabled=fault == "protected",
        )
        args = Namespace(
            action="acknowledge-unknown-preparations",
            request_id=old.request_id,
            claim_ids=list(ids[:-1] if fault == "missing" else ids),
            accept_unknown_effects=fault != "unaccepted",
            repo_root=Path("/fixture"),
            env_file=Path("/fixture/infra/.env.local"),
        )
        clock[0] = AT
        async with sessions() as s:
            before = [
                (str(x.id), x.state, x.reserved_usd, x.settled_usd)
                for x in await s.scalars(select(AgentDailyBudgetReservationRecord))
            ]
        successful = fault in {
            "none",
            "late-claim",
            "changed-claim",
            "audit-tamper",
            "concurrent-claim",
            "concurrent-budget",
        }
        if not successful:
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
        else:
            receipt = await manual_paper.run(settings, args)
            assert (
                receipt.status == "preparation_unknown_acknowledged"
                and receipt.acknowledged_items == 5
            )
            assert not receipt.replayed and receipt.historical_usage == "unknown"
            assert (await manual_paper.run(settings, args)).replayed
        async with sessions.begin() as s:
            audits = tuple(
                await s.scalars(select(AuditEvent).where(AuditEvent.action == ack.ACTION))
            )
            assert len(audits) == int(successful)
            assert (
                await manual_request.read_manual_request(
                    s, await s.get(ManualPaperRequest, old.request_id)
                )
            ).status == "unknown"
            for c in await s.scalars(
                select(IdempotencyRecord).where(IdempotencyRecord.id.in_(ids))
            ):
                assert c.response_payload == (
                    {"outcome": "failed", "error_code": "RISK_INPUTS_UNAVAILABLE"}
                    if fault == "nonempty" and c.id == ids[0]
                    else {}
                )
            if fault == "late-claim":
                s.add(
                    IdempotencyRecord(
                        actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                        route="agent-paper-explicit-manual-v1",
                        schema_version="agent_paper_explicit_manual_v1",
                        idempotency_key=str(old.request_id),
                        request_hash="a" * 64,
                        response_payload={},
                    )
                )
            if fault == "changed-claim":
                (await s.get(IdempotencyRecord, ids[0])).request_hash = "f" * 64
            if fault == "audit-tamper":
                audits[0].payload = {**audits[0].payload, "usage": "zero"}
                with s.no_autoflush, pytest.raises(ValueError):
                    await ack._validate(s, audits[0], now=clock[0])
                s.expunge(audits[0])
        if fault == "concurrent-claim":
            from nexus.paper.target_service import lock_portfolio_account

            ready = asyncio.Event()
            worker_session = []
            original_execute = AsyncSession.execute

            async def observed_execute(self, statement, *a, **kw):
                if (
                    worker_session
                    and self is worker_session[0]
                    and "hashtextextended(:scope, 1818)" in str(statement)
                ):
                    ready.set()
                return await original_execute(self, statement, *a, **kw)

            monkeypatch.setattr(AsyncSession, "execute", observed_execute)

            async def waiting_guard():
                async with sessions.begin() as worker:
                    worker_session.append(worker)
                    await manual_request.require_no_running_manual_request(
                        worker, account_id=authority.account_id
                    )

            async with sessions.begin() as holder:
                await lock_portfolio_account(holder, authority.account_id)
                task = asyncio.create_task(waiting_guard())
                await asyncio.wait_for(ready.wait(), 10)
                holder.add(
                    IdempotencyRecord(
                        actor_id="local-agent-paper-schwab:" + str(authority.account_id),
                        route="agent-paper-explicit-manual-v1",
                        schema_version="agent_paper_explicit_manual_v1",
                        idempotency_key="late-other-request",
                        request_hash="e" * 64,
                        response_payload={},
                    )
                )
            with pytest.raises(ValueError):
                await asyncio.wait_for(task, 10)
        if fault == "concurrent-budget":
            from nexus.agents.persistence import _lock_budget_day
            from nexus.audit.service import append_audit_event
            from nexus.paper import formal_budget_protection as protection
            from nexus.persistence.models.agents import FormalBudgetBindingRecord

            ready = asyncio.Event()
            committed = asyncio.Event()
            worker_session = []
            original_execute = AsyncSession.execute
            original_binding = protection.latest_binding

            async def observed_execute(self, statement, *a, **kw):
                if (
                    worker_session
                    and self is worker_session[0]
                    and "pg_advisory_xact_lock(hashtext(:key))" in str(statement)
                ):
                    ready.set()
                return await original_execute(self, statement, *a, **kw)

            async def observed_binding(session, **kwargs):
                binding = await original_binding(session, **kwargs)
                if worker_session and session is worker_session[0]:
                    ready.set()
                    await committed.wait()
                return binding

            monkeypatch.setattr(AsyncSession, "execute", observed_execute)
            monkeypatch.setattr(protection, "latest_binding", observed_binding)

            async def waiting_scope():
                async with sessions.begin() as worker:
                    worker_session.append(worker)
                    await ack.require_acknowledged_operation_scope(
                        worker,
                        account_id=authority.account_id,
                        model_config_id=model.id,
                        now=clock[0],
                    )

            async with sessions.begin() as holder:
                await _lock_budget_day(holder, clock[0].date().isoformat())
                task = asyncio.create_task(waiting_scope())
                await asyncio.wait_for(ready.wait(), 10)
                audit = await append_audit_event(
                    holder,
                    actor_id="fixture",
                    action="fixture.binding",
                    schema_version="fixture",
                    payload={},
                    occurred_at=clock[0],
                )
                holder.add(
                    FormalBudgetBindingRecord(
                        budget_domain="agent_global_v1",
                        business_date=clock[0].date(),
                        sequence=1,
                        previous_id=None,
                        idempotency_key="fixture-binding",
                        content_hash="b" * 64,
                        schema_version="formal_budget_binding_v1",
                        daily_limit_usd=3,
                        payload={},
                        applied_at=clock[0],
                        audit_event_id=audit.id,
                    )
                )
            committed.set()
            with pytest.raises(ValueError):
                await asyncio.wait_for(task, 10)
        async with sessions.begin() as s:
            if fault not in {"none", "audit-tamper", "concurrent-budget"}:
                with pytest.raises(ValueError):
                    await manual_request.require_manual_account_available(
                        s, account_id=authority.account_id, now=clock[0]
                    )
            else:
                await manual_request.require_manual_account_available(
                    s, account_id=authority.account_id, now=clock[0]
                )
                from nexus.ops.manual_paper_host import peek_manual_request

                assert await peek_manual_request(s, account_id=authority.account_id) is None
                from nexus.paper.manual_entry import manual_host_scope

                s.add(
                    WorkerHeartbeat(
                        queue="agent_paper",
                        worker_id="agent-paper-loop-"
                        + str(UUID(int=12))
                        + ":manual:"
                        + manual_host_scope(settings),
                        process_version="nexus-manual-paper-v1+" + "a" * 40,
                        migration_head="0078_formal_v10_output_binding",
                        started_at=clock[0],
                        seen_at=clock[0],
                    )
                )
        if fault == "none":
            fresh = await manual_paper.run(
                settings,
                Namespace(action="start", mode="research_and_paper", idempotency_key="distinct"),
            )
            assert fresh.request_id != old.request_id
            from nexus.ops import agent_paper_once
            from nexus.ops.manual_paper_host import dispatch_manual_request
            from nexus.paper.agent_cycle import reserve_schwab_agent_paper_trigger
            from nexus.persistence.models.runtime import Job

            source_calls = []

            async def controlled_authority(*_args, **kwargs):
                source_calls.append(kwargs["idempotency_key"])
                return authority

            monkeypatch.setattr(
                agent_paper_once, "prepare_schwab_decision_authority", controlled_authority
            )
            monkeypatch.setattr(
                agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object()
            )
            trigger_settings = agent_paper_once.AgentPaperTriggerSettings.model_validate(
                settings.model_dump()
                | {
                    "object_store_endpoint": "http://localhost:9000",
                    "alpaca_key": "fixture",
                    "alpaca_secret": "fixture",
                }
            )
            async with sessions.begin() as s:
                with pytest.raises(ValueError, match="MANUAL_REQUEST_TERMINAL_OR_UNKNOWN"):
                    await reserve_schwab_agent_paper_trigger(
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
            for _ in range(2):
                await dispatch_manual_request(
                    settings,
                    trigger_settings,
                    request_id=fresh.request_id,
                    repo_root=Path("/fixture"),
                    calendar_authority_id=CALENDAR_ID,
                    process_environment={},
                )
            assert source_calls == ["manual-entry:" + str(fresh.request_id)]
            async with sessions() as s:
                view = await manual_request.read_manual_request(
                    s, await s.get(ManualPaperRequest, fresh.request_id)
                )
                assert view.status == "active" and view.cycle_id is not None
                from nexus.ops.manual_paper_host import peek_manual_request

                assert (
                    await peek_manual_request(s, account_id=authority.account_id)
                ).request_id == fresh.request_id
                jobs = tuple(await s.scalars(select(Job)))
                assert len(jobs) == 3 and all(
                    j.status == "queued" and j.attempt_count == 0 for j in jobs
                )
                assert (
                    await manual_request.read_manual_request(
                        s, await s.get(ManualPaperRequest, old.request_id)
                    )
                ).status == "unknown"
        async with sessions() as s:
            after = [
                (str(x.id), x.state, x.reserved_usd, x.settled_usd)
                for x in await s.scalars(select(AgentDailyBudgetReservationRecord))
            ]
            before_ids = {item[0] for item in before}
            assert [item for item in after if item[0] in before_ids] == before
            assert before and all(item[1] == "reserved" and item[3] is None for item in before)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_completed_receipts_keep_exact_historical_bindings(
    migrated_database_url, monkeypatch
):
    from datetime import datetime, time
    from zoneinfo import ZoneInfo

    from nexus.paper import agent_cycle
    from nexus.paper.preparation_unknown_ack import _terminal_claim
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT, candidate_instruments=INSTRUMENTS
    )
    try:
        async with sessions.begin() as s:
            batch = await create_agent_paper_candidate_batch(
                s,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(i.instrument_id for i in manifest),
                idempotency_key="legacy-pool",
                created_at=AT,
            )
            result = await agent_cycle.start_schwab_agent_paper_cycle(
                s,
                candidate_batch_id=batch.batch_id,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="legacy-base",
                requested_instrument_ids=tuple(i.instrument_id for i in manifest),
            )
            base = await s.get(AgentPaperCycleRecord, result.cycle_id)
            values = {
                c.key: getattr(base, c.key) for c in base.__mapper__.column_attrs if c.key != "id"
            }

            async def clone(offset):
                target = AgentPaperCycleRecord(
                    **(
                        values
                        | {
                            "created_at": base.created_at + timedelta(minutes=offset),
                            "decision_slot_at": AT + timedelta(minutes=5 * offset),
                        }
                    )
                )
                s.add(target)
                await s.flush()
                await agent_cycle._append_event(s, target, None, "prepared", "CYCLE_PREPARED", {})
                return target

            target = await clone(2)
            await clone(4)  # Later legitimate facts must not invalidate old receipts.
            actor = "local-agent-paper-schwab:" + str(authority.account_id)
            scheduled_slot = datetime.combine(
                AT.date(), time(10), tzinfo=ZoneInfo("America/New_York")
            ).astimezone(AT.tzinfo)
            scheduled = IdempotencyRecord(
                actor_id=actor,
                route="agent-paper-schwab-trigger-v2",
                schema_version="agent_paper_schwab_trigger_v2",
                idempotency_key=scheduled_slot.isoformat(),
                request_hash="d" * 64,
                response_payload={"outcome": "failed", "error_code": "SCHWAB_L1_INCOMPLETE"},
                created_at=base.created_at - timedelta(minutes=2),
            )
            manual = IdempotencyRecord(
                actor_id=actor,
                route="agent-paper-manual-trigger-v1",
                schema_version="agent_paper_manual_trigger_v1",
                idempotency_key="manual-recovery:" + AT.date().isoformat(),
                request_hash="e" * 64,
                response_payload={"outcome": "completed", "result": result.model_dump(mode="json")},
                created_at=base.created_at - timedelta(minutes=1),
            )
            synth = IdempotencyRecord(
                actor_id=actor,
                route="agent-paper-synthesis-recovery-v1",
                schema_version="agent_paper_synth_recovery_v1",
                idempotency_key="synthesis-recovery:" + AT.date().isoformat(),
                request_hash="f" * 64,
                response_payload={
                    "outcome": "completed",
                    "result": result.model_dump(mode="json") | {"cycle_id": str(target.id)},
                },
                created_at=base.created_at + timedelta(minutes=1),
            )
            s.add_all([scheduled, manual, synth])
            await s.flush()
            assert await _terminal_claim(s, manual, account_id=authority.account_id)
            assert await _terminal_claim(s, synth, account_id=authority.account_id)
            with s.no_autoflush:
                good = synth.response_payload
                synth.response_payload = manual.response_payload
                assert not await _terminal_claim(s, synth, account_id=authority.account_id)
                synth.response_payload = good
                good_manual = manual.response_payload
                manual.response_payload = synth.response_payload
                assert not await _terminal_claim(s, manual, account_id=authority.account_id)
                assert not await _terminal_claim(s, synth, account_id=authority.account_id)
                manual.response_payload = {
                    "outcome": "failed",
                    "error_code": "SCHWAB_L1_INCOMPLETE",
                }
                assert not await _terminal_claim(s, synth, account_id=authority.account_id)
                manual.response_payload = {"outcome": "completed"}
                assert not await _terminal_claim(s, synth, account_id=authority.account_id)
                manual.response_payload = good_manual
                scheduled.response_payload = {"outcome": "failed", "error_code": "UNKNOWN"}
                assert not await _terminal_claim(s, manual, account_id=authority.account_id)
                assert not await _terminal_claim(s, synth, account_id=authority.account_id)
            await s.rollback()
    finally:
        await engine.dispose()
