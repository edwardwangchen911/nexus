"""Normal host reconstruction in isolated OS processes over one durable database."""

import pytest
from integration.test_monitor_observation_tick_pg import (
    test_observe_cli_to_real_persisted_checks_preserves_research_and_deduplicates as observe,
)


@pytest.mark.asyncio
async def test_normal_host_process_reconstruction_preserves_durable_no_trigger(
    migrated_database_url, monkeypatch, tmp_path
):
    await observe(migrated_database_url, monkeypatch, tmp_path, "host_process")


@pytest.mark.asyncio
async def test_reconstructed_host_preserves_typed_unknown_usage_and_never_reinvokes(
    migrated_database_url, monkeypatch, tmp_path
):
    import argparse
    import json
    from datetime import timedelta
    from hashlib import sha256
    from uuid import UUID

    from helpers.mixed_stage import add_mixed_fields
    from helpers.monitor_host_process import (
        run_process,
        write_environment,
    )
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_mixed_host_pg import ready_host_batch
    from integration.test_monitor_session_budget_pg import preserved_rows, workload_counts
    from nexus.agents import codex_staged
    from nexus.monitoring import filing_archive
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops.research_monitor import run
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import OutboxEvent
    from nexus.worker import run_agent_worker_once
    from sqlalchemy import func, select, text
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, captured, pointer, batch_id, normal = await ready_host_batch(
        migrated_database_url, monkeypatch, tmp_path
    )
    prepared = await run(settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id))
    assert prepared["status"] == "prepared"
    admitted = await run(
        settings,
        argparse.Namespace(
            action="admit-batch",
            batch_id=batch_id,
            materials_snapshot_id=UUID(prepared["snapshot_id"]),
            calendar_authority_id=CALENDAR_ID,
        ),
    )
    assert admitted["status"] == "admitted" and admitted["execution_locked"] is True
    async with factory() as session:
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.batch_id == batch_id)
        )
        assert str(member.run_id) == admitted["run_id"]
    native = codex_staged.invoke_codex
    calls = []

    async def unknown(payload, **kwargs):
        wire = _restore_view(payload)
        calls.append(wire["stage"])
        if wire["stage"] == "critique":
            raise TimeoutError("controlled unknown usage")
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        add_mixed_fields(body, wire)
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", unknown)
    assert await run_agent_worker_once(
        settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
    )
    assert calls == ["research", "critique"]
    captured["clock"][0] += timedelta(minutes=2)
    await _set_test_insert_clock(factory, captured["clock"][0])
    terminal = await run(
        settings, argparse.Namespace(action="finish", trigger_id=member.trigger_id)
    )
    assert terminal["status"] == "unknown"
    async with factory() as session:
        hold = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
            )
        )
        assert hold.state == "reserved" and hold.settled_usd is None

    async def notifications():
        async with factory() as session:
            return await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.event_type.in_(
                        ("research.monitor.triggered", "research.monitor.batch_triggered")
                    )
                )
            )

    write_environment(settings, tmp_path / "host.env")
    async with factory() as session:
        store = filing_archive.configured_exhibit_store(session)
    for key, content in store.objects.items():
        path = tmp_path / "host-sec-evidence" / sha256(key.encode()).hexdigest()
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(content)
    (tmp_path / "host-fixture.json").write_text(json.dumps({"midpoint": 1, "other_midpoint": 1}))
    protected = await preserved_rows(factory)
    financial_tables = (
        "paper_performance_snapshots",
        "paper_position_snapshots",
        "paper_performance_attributions",
    )

    async def financial_rows():
        async with factory() as session:
            return {
                table: set(
                    await session.scalars(text(f'SELECT to_jsonb(t)::text FROM "{table}" t'))
                )
                for table in financial_tables
            }

    financial_before = await financial_rows()
    counts, events = await workload_counts(factory), await notifications()
    pids = []
    for iteration in range(2):
        await factory.kw["bind"].dispose()
        rows = await run_process(settings, tmp_path, captured["clock"][0], signal_stop=False)
        assert rows[-1]["action"] == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN.value, rows
        assert rows[-1]["actions"] == (["publish_performance"] if iteration == 0 else []), rows
        assert rows[0]["result"]["model_requests"] == 0
        assert rows[0]["result"].get("new_triggers", 0) == 0
        after = await preserved_rows(factory)
        assert all(old <= after[table] for table, old in protected.items())
        assert await workload_counts(factory) == counts
        assert await notifications() == events
        financial_after = await financial_rows()
        assert all(old <= financial_after[table] for table, old in financial_before.items())
        assert (
            len(financial_after["paper_performance_snapshots"])
            == len(financial_before["paper_performance_snapshots"]) + 1
        )
        pids.append(rows[-1]["pid"])
    assert pids[0] != pids[1] and calls == ["research", "critique"]
