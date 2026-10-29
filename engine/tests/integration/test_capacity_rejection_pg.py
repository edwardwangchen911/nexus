"""Normal host rejection closes only an untouched complete formal v9 pool."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops import agent_paper_loop as loop
from nexus.ops.agent_capacity_rejection import close_capacity_rejected_cycle_once
from nexus.ops.openai_local import prepare_database
from nexus.paper import agent_cycle, manual_request
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

AT = datetime(2026, 10, 13, 14, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("fault", [None, "claimed", "unknown-code", "job-busy", "budget-busy"])
async def test_host_preclaim_rejection_closes_without_model_or_order(
    migrated_database_url, monkeypatch, manual, fault
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS, decision_time=AT
    )
    try:
        await _set_test_insert_clock(sessions, AT - timedelta(minutes=1))
        await prepare_database(settings, **V9_OPTIONS)
        scalar = AsyncSession.scalar
        current = [AT]

        async def clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return current[0]
            return await scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v9-full-input-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="capacity-pool",
                created_at=AT,
            )
            req = None
            reservation = None
            if manual:
                req = await manual_request.admit_manual_request(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    model_config_id=model.id,
                    mode="research_and_paper",
                    idempotency_key="capacity-manual",
                )
                await manual_request.claim_manual_request(session, request_id=req.request_id)
                reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    now=AT,
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    client_idempotency_key=f"manual-entry:{req.request_id}",
                    trigger_mode="manual",
                    manual_request_id=req.request_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
            started = await agent_cycle.start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=reservation.decision_slot_at if manual else AT,
                idempotency_key=f"manual-entry:{req.request_id}" if manual else "capacity-cycle",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
                manual_request_id=req.request_id if manual else None,
            )
            if reservation is not None:
                await agent_cycle.finish_schwab_agent_paper_trigger(
                    session, reservation=reservation, result=started
                )
            jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
            original_events = await agent_cycle._events(session, started.cycle_id)
            original_hashes = tuple(event.event_hash for event in original_events)
        if fault == "claimed":
            from helpers.native_execution import claim_without_dispatch

            current[0] = AT + timedelta(seconds=1)
            await claim_without_dispatch(settings, monkeypatch, job_id=jobs[0].id, now=current[0])
        settings = settings.model_copy(
            update={
                "agent_paper_account_id": authority.account_id,
                "agent_paper_candidate_batch_id": batch.batch_id,
                "agent_paper_formal_model_config_id": model.id,
                "agent_paper_manual_enabled": True,
                "agent_paper_cycle_enabled": True,
            }
        )
        current[0] = AT + timedelta(seconds=1)
        dispatches = []
        code = (
            "UNKNOWN_PRIVATE_ERROR"
            if fault == "unknown-code"
            else "CODEX_CAPACITY_POOL_WIRE_EXCEEDED"
        )

        async def reject(*args, **kwargs):
            dispatches.append(kwargs["job_id"])
            raise ValueError(code)

        async def ready():
            return None

        monkeypatch.setattr(loop, "run_cycle_job_once", reject)
        monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker_async", ready)
        monkeypatch.setattr(loop, "local_settings", lambda *args, **kwargs: settings)
        locked = None
        if fault in {"job-busy", "budget-busy"}:
            locked = sessions()
            await locked.begin()
            if fault == "job-busy":
                await locked.scalar(select(Job.id).where(Job.id == jobs[1].id).with_for_update())
            else:
                from nexus.agents.persistence import _lock_budget_day

                await _lock_budget_day(locked, AT.date().isoformat())
        action = loop.execute_agent_paper_loop_action(
            settings,
            account_id=authority.account_id,
            repo_root=Path.cwd(),
            action=loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            snapshot=loop.AgentPaperLoopSnapshot(
                now=AT + timedelta(seconds=1),
                cycle_id=started.cycle_id,
                cycle_status="researching",
                research_job_id=jobs[0].id,
                research_job_status="queued",
            ),
        )
        try:
            if fault:
                with pytest.raises(ValueError):
                    await action
            else:
                assert await action is None
        finally:
            if locked is not None:
                await locked.rollback()
                await locked.close()
        assert dispatches == [jobs[0].id]
        async with sessions.begin() as session:
            events = await agent_cycle._events(session, started.cycle_id)
            assert tuple(event.event_hash for event in events[:2]) == original_hashes
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            actual_jobs = tuple(await session.scalars(select(Job)))
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            if fault:
                assert len(events) == 2 and events[-1].to_status == "researching"
                assert all(run.settled_cost_usd is None for run in runs)
                assert sum(job.attempt_count for job in actual_jobs) == (
                    1 if fault == "claimed" else 0
                )
            else:
                assert len(events) == 3 and events[-1].to_status == "unavailable"
                assert events[-1].fact_ids["failure_code"] == code
                assert events[-1].fact_ids["unused_research_cancelled"] == 3
                assert all(job.status == "skipped" for job in actual_jobs)
                assert all(
                    run.input_tokens == run.output_tokens == run.settled_cost_usd == 0
                    for run in runs
                )
                if manual:
                    row = await session.get(ManualPaperRequest, req.request_id)
                    assert (
                        await manual_request.read_manual_request(session, row)
                    ).status == "active"
                cycle = await session.get(AgentPaperCycleRecord, started.cycle_id)
                view = await PaperReadService(session, current[0])._agent_cycle(cycle)
                assert view.status == "unavailable"
                assert view.order_ids == ()
        if fault is None and manual:
            from nexus.ops.manual_paper_host import inspect_manual_close

            next_snapshot = await inspect_manual_close(
                settings,
                account_id=authority.account_id,
                snapshot=loop.AgentPaperLoopSnapshot(
                    now=current[0], cycle_id=started.cycle_id, cycle_status="unavailable"
                ),
            )
            next_action = loop.choose_loop_action(next_snapshot)
            assert next_action is loop.AgentPaperLoopAction.FINISH_MANUAL
            assert (
                await loop.execute_agent_paper_loop_action(
                    settings,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=next_action,
                    snapshot=next_snapshot,
                )
                is None
            )
            async with sessions.begin() as session:
                row = await session.get(ManualPaperRequest, req.request_id)
                assert (
                    await manual_request.read_manual_request(session, row)
                ).status == "unavailable"
        if fault is None:
            await close_capacity_rejected_cycle_once(
                settings,
                account_id=authority.account_id,
                cycle_id=started.cycle_id,
                job_id=jobs[0].id,
                code=code,
            )
            async with sessions.begin() as session:
                assert len(await agent_cycle._events(session, started.cycle_id)) == 3
                await manual_request.require_manual_account_available(
                    session, account_id=authority.account_id, now=AT + timedelta(seconds=1)
                )
    finally:
        await engine.dispose()
