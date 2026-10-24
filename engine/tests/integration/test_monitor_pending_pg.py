"""Frozen pending-group boundaries through normal admission and original Worker."""

from datetime import timedelta
from uuid import uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import ready_trigger
from nexus.persistence.models.monitoring import MonitorCheck, MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import func, select


async def pending_members(factory, original, count, *, start=1):
    """Explicit boundary fixture: immutable episodes, not claimed source collection."""
    ids = [original.id]
    async with factory() as session, session.begin():
        check = await session.get(MonitorCheck, original.check_id)
        for index in range(start, count):
            tid, oid = uuid4(), uuid4()
            copied = MonitorCheck(
                account_id=original.account_id,
                instrument_id=original.instrument_id,
                plan_id=check.plan_id,
                scheduled_at=check.scheduled_at + timedelta(microseconds=index),
                checked_at=check.checked_at,
                result="true",
                reason=None,
                observation=check.observation,
            )
            session.add(copied)
            session.add(
                OutboxEvent(
                    id=oid,
                    aggregate_id=tid,
                    event_type="research.monitor.triggered",
                    schema_version="research_price_trigger_v1",
                    payload={"trigger_id": str(tid)},
                    available_at=check.checked_at,
                )
            )
            await session.flush()
            session.add(
                MonitorTrigger(
                    id=tid,
                    account_id=original.account_id,
                    instrument_id=original.instrument_id,
                    condition_hash=original.condition_hash,
                    episode=index + 1,
                    check_id=copied.id,
                    cause="crossed",
                    status="pending",
                    outbox_id=oid,
                )
            )
            ids.append(tid)
    return ids


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 32, 33])
async def test_pending_stock_is_frozen_whole_or_explicitly_rejected(
    migrated_database_url, monkeypatch, count
):
    from nexus.monitoring.context import read_monitor_context
    from nexus.ops.research_monitor import parser, run
    from nexus.persistence.models.agents import AgentResearchRunRecord

    factory, settings, original, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    ids = await pending_members(factory, original, count)
    args = parser().parse_args(
        ["admit", "--trigger-id", str(ids[-1]), "--calendar-authority-id", str(CALENDAR_ID)]
    )
    if count == 2:
        import asyncio

        alternate = parser().parse_args(
            ["admit", "--trigger-id", str(ids[0]), "--calendar-authority-id", str(CALENDAR_ID)]
        )
        first, second = await asyncio.gather(run(settings, args), run(settings, alternate))
        assert first["run_id"] == second["run_id"]
        result = first
    else:
        result = await run(settings, args)
    if count > 32:
        assert result["status"] == "blocked"
        assert result["error_code"] == "MONITOR_PENDING_OVERFLOW"
    else:
        assert result["status"] == "admitted", result
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            native = await session.get(AgentResearchRunRecord, member.run_id)
            bound = await read_monitor_context(
                session,
                snapshot_id=native.data_snapshot_id,
                instrument_id=parent.instrument_id,
                as_of=now,
            )
            assert {item.trigger_id for item in bound[0].trigger_checks} == set(ids)
            rows = list(await session.scalars(select(MonitorTrigger)))
            assert {row.research_run_id for row in rows} == {native.id}
            assert member.reserved_sdk_calls == 3
        repeated = await run(
            settings,
            parser().parse_args(
                ["admit", "--trigger-id", str(ids[0]), "--calendar-authority-id", str(CALENDAR_ID)]
            ),
        )
        assert repeated["run_id"] == result["run_id"]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == (
            0 if count > 32 else 1
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown, changed", [(False, False), (True, False), (False, True)])
async def test_group_dispatch_from_secondary_finishes_exact_members(
    migrated_database_url, monkeypatch, unknown, changed
):
    from nexus.agents import codex_staged
    from nexus.ops.monitor_dispatch import dispatch_configured_once
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

    factory, settings, original, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    ids = await pending_members(factory, original, 3)
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    native = codex_staged.invoke_codex
    calls = []

    async def invoke(payload, **kwargs):
        calls.append(payload["stage"])
        if unknown and payload["stage"] == "critique":
            raise TimeoutError("controlled unknown")
        raw, usage = await native(payload, **kwargs)
        if changed and payload["stage"] == "research":
            import json

            value = json.loads(raw)
            for role in value["roles"]:
                if role.get("monitoring_proposal") is not None:
                    role["monitoring_proposal"]["condition"]["threshold"] = "200000"
            raw = json.dumps(value)
        return raw, usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    from nexus.ops.agent_paper_loop import AgentPaperLoopSnapshot
    from nexus.ops.monitor_dispatch import inspect_monitor_queue
    from nexus.ops.research_monitor import parser, run

    admitted = await run(
        settings,
        parser().parse_args(
            ["admit", "--trigger-id", str(ids[-1]), "--calendar-authority-id", str(CALENDAR_ID)]
        ),
    )
    assert admitted["status"] == "admitted"
    late = (await pending_members(factory, original, 4, start=3))[-1]
    snapshot = AgentPaperLoopSnapshot(
        now=now,
        calendar_authority_id=CALENDAR_ID,
        market_open_at=now - timedelta(hours=1),
        decision_window_start_at=now - timedelta(minutes=30),
        decision_window_end_at=now + timedelta(hours=4),
        next_market_open_at=now + timedelta(days=3),
        next_decision_window_start_at=now + timedelta(days=3),
    )
    queued = await inspect_monitor_queue(
        settings, account_id=original.account_id, snapshot=snapshot
    )
    assert str(queued.monitor_trigger_id) == admitted["trigger_id"]
    assert queued.monitor_trigger_status == "admitted"
    result = await dispatch_configured_once(
        settings, trigger_id=ids[-1], calendar_authority_id=CALENDAR_ID
    )
    expected = "unknown" if unknown else "completed"
    assert result["status"] == expected, result
    async with factory() as session:
        rows = list(await session.scalars(select(MonitorTrigger)))
        assert len(rows) == 4
        pending = next(row for row in rows if row.id == late)
        assert pending.status == "pending" and pending.research_run_id is None
        group = [row for row in rows if row.id != late]
        assert {row.status for row in group} == {expected}
        assert len({row.research_run_id for row in group}) == 1
        member = await session.scalar(select(MonitorIncrement))
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == member.run_id
            )
        )
        assert reservation.state == ("reserved" if unknown else "settled")
        assert member.reserved_sdk_calls == 3
    again = await dispatch_configured_once(
        settings, trigger_id=ids[0], calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == expected
    assert again["trigger_id"] == admitted["trigger_id"]
    assert calls == (["research", "critique"] if unknown else ["research", "critique", "synthesis"])
    if not unknown:
        from uuid import UUID

        from integration.test_candidate_multistock_path import _set_test_insert_clock
        from integration.test_monitor_increment_pg import append_fresh_inputs
        from nexus.monitoring.context import read_monitor_context
        from nexus.persistence.models.agents import AgentResearchRunRecord
        from sqlalchemy.ext.asyncio import AsyncSession

        later = now + timedelta(seconds=1)
        await _set_test_insert_clock(factory, later)
        await append_fresh_inputs(factory, parent.instrument_id, later)
        old_scalar = AsyncSession.scalar

        async def clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return later
            return await old_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        carried = await run(
            settings,
            parser().parse_args(
                ["admit", "--trigger-id", str(late), "--calendar-authority-id", str(CALENDAR_ID)]
            ),
        )
        if changed:
            assert carried["status"] == "blocked", carried
            assert carried["error_code"] == "MONITOR_PENDING_CONDITION_UNRESOLVED"
            async with factory() as session:
                pending = await session.get(MonitorTrigger, late)
                assert pending.research_run_id is None
                assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == 1
            await factory.kw["bind"].dispose()
            return
        assert carried["status"] == "admitted", carried
        async with factory() as session:
            native_run = await session.get(AgentResearchRunRecord, UUID(carried["run_id"]))
            bound = await read_monitor_context(
                session,
                snapshot_id=native_run.data_snapshot_id,
                instrument_id=parent.instrument_id,
                as_of=later,
            )
            assert [item.trigger_id for item in bound[0].trigger_checks] == [late]
            original_check = await session.get(MonitorCheck, bound[0].trigger_checks[0].check_id)
            assert original_check.plan_id != bound[0].plan_id
            assert bound[0].parent_run_id != parent.id
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_eligibility_bucket_is_per_stock_even_when_new_member_arrives(
    migrated_database_url, monkeypatch
):
    from nexus.audit.service import append_audit_event
    from nexus.ops.monitor_dispatch import DISPATCH_ACTION, unchecked

    factory, settings, original, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action=DISPATCH_ACTION,
            schema_version="monitor_dispatch_v1",
            occurred_at=now,
            payload={"trigger_id": str(original.id)},
        )
    ids = await pending_members(factory, original, 2)
    async with factory() as session:
        assert not list(await session.scalars(select(MonitorTrigger.id).where(unchecked(now))))
        assert set(
            await session.scalars(
                select(MonitorTrigger.id).where(unchecked(now + timedelta(minutes=5)))
            )
        ) == set(ids)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_secondary_capacity_rejection_closes_group_with_original_proof(
    migrated_database_url, monkeypatch
):
    from nexus.ops.monitor_dispatch import dispatch_configured_once
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.runtime import AuditEvent

    factory, settings, original, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    ids = await pending_members(factory, original, 3)
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    calls = []

    async def capacity(*args, **kwargs):
        calls.append(kwargs["job_ids"])
        raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

    monkeypatch.setattr("nexus.worker.run_agent_worker_once", capacity)
    result = await dispatch_configured_once(
        settings, trigger_id=ids[-1], calendar_authority_id=CALENDAR_ID
    )
    assert result["status"] == "failed", result
    async with factory() as session:
        member = await session.scalar(select(MonitorIncrement))
        rows = list(await session.scalars(select(MonitorTrigger)))
        assert {(row.status, row.reason, row.research_run_id) for row in rows} == {
            ("failed", "MONITOR_CAPACITY_REJECTED", member.run_id)
        }
        assert member.reserved_sdk_calls == 3
        run = await session.get(AgentResearchRunRecord, member.run_id)
        assert (run.input_tokens, run.output_tokens, run.settled_cost_usd) == (0, 0, 0)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run.id
            )
        )
        assert reservation.state == "released"
        audit = await session.scalar(
            select(AuditEvent).where(AuditEvent.action == "research.monitor.worker_dispatched")
        )
        assert audit.payload["trigger_id"] == str(member.trigger_id)
    again = await dispatch_configured_once(
        settings, trigger_id=ids[0], calendar_authority_id=CALENDAR_ID
    )
    assert again["status"] == "failed" and len(calls) == 1
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_scans_generate_two_episodes_then_one_frozen_research(
    migrated_database_url, monkeypatch
):
    from decimal import Decimal

    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_increment_pg import append_fresh_inputs
    from nexus.monitoring.context import read_monitor_context
    from nexus.ops.research_monitor import parser, run
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from sqlalchemy.ext.asyncio import AsyncSession

    factory, settings, original, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    current = [now]
    old_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return current[0]
        return await old_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    for offset, price, expected in [(5, "200000", "false"), (10, "100", "true")]:
        current[0] = now + timedelta(minutes=offset)
        await _set_test_insert_clock(factory, current[0])
        await append_fresh_inputs(
            factory, parent.instrument_id, current[0], midpoint=Decimal(price)
        )
        scanned = await run(
            settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
        )
        assert scanned["model_requests"] == scanned["provider_requests"] == 0
        async with factory() as session:
            check = await session.scalar(
                select(MonitorCheck)
                .where(MonitorCheck.instrument_id == parent.instrument_id)
                .order_by(MonitorCheck.checked_at.desc())
                .limit(1)
            )
            assert check.result == expected
    result = await run(
        settings,
        parser().parse_args(
            ["admit", "--trigger-id", str(original.id), "--calendar-authority-id", str(CALENDAR_ID)]
        ),
    )
    assert result["status"] == "admitted", result
    async with factory() as session:
        rows = list(
            await session.scalars(
                select(MonitorTrigger).where(MonitorTrigger.instrument_id == parent.instrument_id)
            )
        )
        assert len(rows) == 2 and {row.episode for row in rows} == {1, 2}
        member = await session.scalar(select(MonitorIncrement))
        assert {row.research_run_id for row in rows} == {member.run_id}
        native = await session.get(AgentResearchRunRecord, member.run_id)
        bound = await read_monitor_context(
            session,
            snapshot_id=native.data_snapshot_id,
            instrument_id=parent.instrument_id,
            as_of=current[0],
        )
        assert {item.trigger_id for item in bound[0].trigger_checks} == {row.id for row in rows}
        assert len({row.outbox_id for row in rows}) == 2
    await factory.kw["bind"].dispose()
