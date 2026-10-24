"""Normal three-stock admission: an unpublished typed bundle may become one gap.

Controlled exceptions exercise rollback ordering; actual-wire modes retain the
normal preview and add bounded synthetic bytes only to a tool projection. Native
snapshot, claim, Job, budget and transactions remain real; no Worker or SDK runs.
"""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from integration.test_daily_v2_frozen_context_pg import (
    CALENDAR_ID,
    _set_test_insert_clock,
    append_fresh_inputs,
    prepared_materials,
)
from nexus.agents import codex_staged
from nexus.agents.tools import ReadOnlyTools
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.monitoring import MonitorDailyEventReceipt, MonitorTrigger
from nexus.persistence.models.runtime import IdempotencyRecord, Job, JobLease
from sqlalchemy import func, select


async def normal_slot(factory, settings, captured, pointer):
    """Same normal preparation as frozen-context PG, without asserting an outcome."""
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        now = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        candidates = await load_agent_paper_candidate_batch(
            session, batch_id=settings.agent_paper_candidate_batch_id
        )
        scope = tuple(await session.scalars(select(LatestQuote.instrument_id).distinct()))
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    for instrument in scope:
        await append_fresh_inputs(factory, instrument, now)
    async with factory() as session, session.begin():
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=pointer.account_id,
            now=now,
            expected_calendar_authority_id=CALENDAR_ID,
            expected_universe_snapshot_id=candidates.universe_snapshot_id,
        )
    return now, dict(
        authority=authority,
        decision_slot_at=now.replace(second=0, microsecond=0),
        idempotency_key="daily-capacity-normal",
        requested_instrument_ids=candidates.instrument_ids,
        candidate_batch_id=candidates.batch_id,
        formal_stage_capacity=True,
        formal_model_config_id=parent.model_config_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "typed_only",
        "base_too_large",
        "noncapacity",
        "late_member",
        "published_replay",
        "actual_wire",
        "actual_base_wire",
        "deadline",
        "deadline_exit",
    ],
)
async def test_unpublished_daily_capacity_rollback_is_exact(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    factory, settings, _, captured, pointer, batch_id, _ = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.monitoring import daily_v2_capacity as module

    now, arguments = await normal_slot(factory, settings, captured, pointer)
    async with factory() as session:
        before = {
            model: set(await session.scalars(select(model.id)))
            for model in (
                AgentResearchRunRecord,
                Job,
                JobLease,
                IdempotencyRecord,
                DataSnapshot,
                AgentDailyBudgetReservationRecord,
                MonitorDailyEventReceipt,
            )
        }
        original_triggers = [
            (row.id, row.status, row.research_run_id, row.batch_id)
            for row in await session.scalars(
                select(MonitorTrigger)
                .where(MonitorTrigger.batch_id == batch_id)
                .order_by(MonitorTrigger.id)
            )
        ]
        prior_budget = [
            (row.run_id, row.state, row.reserved_usd, row.settled_usd)
            for row in await session.scalars(
                select(AgentDailyBudgetReservationRecord).order_by(
                    AgentDailyBudgetReservationRecord.run_id
                )
            )
        ]
    seen = []
    rolled_ids, rolled_snapshots = set(), set()
    staged_counts = []
    sdk_calls = []
    actual_wire = mode in {"actual_wire", "actual_base_wire"}
    real_preview = module.preview_daily_event_capacity
    measured_wire = []
    deadlines = []
    timeout_waiter = asyncio.Event()
    timeout_exited = []
    admission_task = asyncio.current_task()
    if mode == "deadline_exit":
        original_exit = module.DailyEventCapacityScope.__aexit__
        real_monotonic = module._monotonic

        async def expired_exit(scope, exc_type, exc_value, traceback):
            if exc_type is None and scope.deadline is not None:
                # The real scope performs its final synchronous expiry check on
                # exit. Advance only its clock, after all native Job/hold writes.
                monkeypatch.setattr(module, "_monotonic", lambda: scope.deadline + 1)
            try:
                return await original_exit(scope, exc_type, exc_value, traceback)
            finally:
                monkeypatch.setattr(module, "_monotonic", real_monotonic)

        monkeypatch.setattr(module.DailyEventCapacityScope, "__aexit__", expired_exit)
    if actual_wire:
        original_tools = module.database_read_only_tools
        original_envelope = module.candidate_capacity_envelope

        async def bounded_projection(session, run, **kwargs):
            tools = await original_tools(session, run, **kwargs)
            frozen = await session.get(DataSnapshot, run.data_snapshot_id)
            paper = frozen.payload["agent_paper_context"]
            if run.instrument_id != pointer.instrument_id or (
                mode == "actual_wire" and paper.get("pending_monitoring_v2") is None
            ):
                return tools

            async def security(payload):
                result = await tools.execute("get_security_snapshot", payload)
                # Controlled tool projection only: preserve every original item,
                # reference and scope. No DataSnapshot or source bytes are edited.
                return {**result, "synthetic_capacity_padding": "x" * 120000}

            return ReadOnlyTools(
                executors={**tools._executors, "get_security_snapshot": security},
                allow_sec_amendments=tools.allow_sec_amendments,
            )

        def measured_envelope(committee, **kwargs):
            result = original_envelope(committee, **kwargs)
            measured_wire.append((result["wire_upper_bytes"], committee._max_wire_bytes))
            return result

        monkeypatch.setattr(module, "database_read_only_tools", bounded_projection)
        monkeypatch.setattr(module, "candidate_capacity_envelope", measured_envelope)

    async def no_sdk(*args, **kwargs):
        sdk_calls.append(True)
        raise AssertionError("admission must never invoke a model")

    async def preview(session, run):
        frozen = await session.get(DataSnapshot, run.data_snapshot_id)
        paper = frozen.payload.get("agent_paper_context", {})
        bundle = paper.get("pending_monitoring_v2")
        gap = paper.get("pending_monitoring_v2_gap")
        seen.append((run.id, run.instrument_id, bundle is not None, gap is not None))
        if mode in {"typed_only", "late_member", "deadline", "deadline_exit"}:
            scope = module._current_scope.get()
            assert scope is not None, "normal pool must own the common capacity deadline"
            if bundle is not None or gap is not None:
                scope.activate()
            if scope.deadline is not None:
                deadlines.append(scope.deadline)
        if run.instrument_id != pointer.instrument_id:
            return
        if bundle is not None:
            assert len(bundle["trigger_checks"]) == 3
            assert {UUID(item["trigger_id"]) for item in bundle["trigger_checks"]} == {
                row[0] for row in original_triggers
            }
            if mode == "noncapacity":
                raise ValueError("CONTROLLED_AUTHORITY_REFUSAL")
            current = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.not_in(before[AgentResearchRunRecord])
                    )
                )
            )
            rolled_ids.update(row.id for row in current)
            rolled_snapshots.update(row.data_snapshot_id for row in current)
            staged_counts.append(
                (
                    await session.scalar(
                        select(func.count()).select_from(Job).where(Job.id.not_in(before[Job]))
                    ),
                    await session.scalar(
                        select(func.count())
                        .select_from(AgentDailyBudgetReservationRecord)
                        .where(
                            AgentDailyBudgetReservationRecord.id.not_in(
                                before[AgentDailyBudgetReservationRecord]
                            )
                        )
                    ),
                )
            )
            if mode == "deadline":
                assert staged_counts == [(2, 2)]
                assert asyncio.current_task() is admission_task
                assert scope._timeout is not None
                # Event control, not a subsecond wall-clock correctness window:
                # cancel on the next loop tick, with two unpublished Jobs/holds.
                scope._timeout.reschedule(asyncio.get_running_loop().time() - 1)
                try:
                    await timeout_waiter.wait()
                finally:
                    timeout_exited.append(True)
                raise AssertionError("expired admission scope must cancel the waiting preview")
            if actual_wire:
                await real_preview(session, run)
                return
            raise module.DailyEventCapacityExceeded(
                run.instrument_id, len(bundle["trigger_checks"])
            )
        assert gap is not None and gap["reason"] == "MONITOR_CONTENT_CAPACITY"
        if actual_wire:
            await real_preview(session, run)
        if mode == "base_too_large":
            raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

    monkeypatch.setattr(module, "preview_daily_event_capacity", preview, raising=False)
    monkeypatch.setattr(codex_staged, "invoke_codex", no_sdk)
    if mode in {"late_member", "deadline", "deadline_exit"}:
        # Change only deterministic loop iteration, not the frozen candidate set,
        # identity, hash, materials or admission entry; exercise two prior holds.
        parse = agent_cycle.parse_candidate_manifest

        def target_last(*args, **kwargs):
            return tuple(
                sorted(
                    parse(*args, **kwargs),
                    key=lambda row: row.instrument_id == pointer.instrument_id,
                )
            )

        monkeypatch.setattr(agent_cycle, "parse_candidate_manifest", target_last)
    try:
        async with factory() as session, session.begin():
            result = await agent_cycle.start_schwab_agent_paper_cycle(session, **arguments)
        success = mode not in {
            "base_too_large",
            "actual_base_wire",
            "noncapacity",
            "deadline",
            "deadline_exit",
        }
        assert result.result_code == (
            "RESEARCH_JOBS_PREPARED" if success else "RESEARCH_ADMISSION_UNAVAILABLE"
        ), result
        assert sdk_calls == []
        target_calls = [row for row in seen if row[1] == pointer.instrument_id]
        assert [row[2:] for row in target_calls] == (
            [(True, False), (False, True)]
            if mode not in {"noncapacity", "deadline"}
            else [(True, False)]
        )
        if mode in {"typed_only", "late_member"}:
            assert len(deadlines) >= 2 and len(set(deadlines)) == 1
        if mode == "deadline":
            assert timeout_exited == [True]
            assert not timeout_waiter.is_set()
            checkpoint = list(seen)
            timeout_waiter.set()
            await asyncio.sleep(0)
            assert seen == checkpoint and asyncio.current_task() is admission_task
            assert module._current_scope.get() is None
        if actual_wire:
            assert len(measured_wire) == 2
            assert measured_wire[0][0] > measured_wire[0][1]
            assert (measured_wire[1][0] > measured_wire[1][1]) is (mode == "actual_base_wire")
            assert all(limit == 100479 for _, limit in measured_wire)
        if mode == "late_member":
            assert staged_counts == [(2, 2)]
        async with factory() as session:
            after = {model: set(await session.scalars(select(model.id))) for model in before}
            runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.not_in(before[AgentResearchRunRecord])
                    )
                )
            )
            assert len(runs) == (3 if success else 0)
            assert rolled_ids.isdisjoint(after[AgentResearchRunRecord])
            assert rolled_snapshots.isdisjoint(after[DataSnapshot])
            for model in (Job, AgentDailyBudgetReservationRecord):
                assert len(after[model] - before[model]) == (3 if success else 0)
            assert after[JobLease] == before[JobLease]
            assert after[MonitorDailyEventReceipt] == before[MonitorDailyEventReceipt]
            originals = [
                (row.id, row.status, row.research_run_id, row.batch_id)
                for row in await session.scalars(
                    select(MonitorTrigger)
                    .where(MonitorTrigger.batch_id == batch_id)
                    .order_by(MonitorTrigger.id)
                )
            ]
            assert originals == original_triggers
            assert [
                (row.run_id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord)
                    .where(
                        AgentDailyBudgetReservationRecord.id.in_(
                            before[AgentDailyBudgetReservationRecord]
                        )
                    )
                    .order_by(AgentDailyBudgetReservationRecord.run_id)
                )
            ] == prior_budget
            claims = tuple(
                await session.scalars(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.actor_id == "local-agent-paper-cycle",
                        IdempotencyRecord.idempotency_key.startswith(
                            f"agent-paper:{result.cycle_id}:"
                        ),
                    )
                )
            )
            assert len(claims) == (3 if success else 0)
            assert {UUID(row.response_payload["run_id"]) for row in claims} == {
                row.id for row in runs
            }
            frozen = tuple(
                await session.scalars(
                    select(DataSnapshot).where(
                        DataSnapshot.payload["agent_paper_context"]["cycle_id"].astext
                        == str(result.cycle_id)
                    )
                )
            )
            assert len(frozen) == (3 if success else 0)
            if success:
                assert set(result.research_run_ids) == {row.id for row in runs}
                target = next(row for row in runs if row.instrument_id == pointer.instrument_id)
                snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
                paper = snapshot.payload["agent_paper_context"]
                assert paper.get("pending_monitoring_v2") is None
                assert paper["pending_monitoring_v2_gap"]["pending_count"] == 3
                assert (
                    paper["pending_monitoring_v2_gap"]["observed_at"].replace("Z", "+00:00")
                    == now.isoformat()
                )
                assert paper["pending_monitoring_v2_gap"]["reason"] == "MONITOR_CONTENT_CAPACITY"
                assert paper["execution_locked"] is True
                jobs = tuple(await session.scalars(select(Job).where(Job.id.not_in(before[Job]))))
                assert {UUID(job.payload["run_id"]) for job in jobs} == set(result.research_run_ids)
                assert all(
                    job.status == "queued" and job.attempt_count == 0 and job.max_attempts == 1
                    for job in jobs
                )
                holds = tuple(
                    await session.scalars(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id.in_(result.research_run_ids)
                        )
                    )
                )
                assert len(holds) == 3 and all(row.state == "reserved" for row in holds)
        if mode == "published_replay":
            checkpoint = list(seen)
            async with factory() as session, session.begin():
                replayed = await agent_cycle.start_schwab_agent_paper_cycle(session, **arguments)
            assert replayed.replayed and set(replayed.research_run_ids) == set(
                result.research_run_ids
            )
            assert seen == checkpoint and sdk_calls == []
            async with factory() as session:
                assert {
                    model: set(await session.scalars(select(model.id))) for model in before
                } == after
    finally:
        await factory.kw["bind"].dispose()
