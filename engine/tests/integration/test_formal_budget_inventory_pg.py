"""Read-only inventory from normal synthetic candidates and native records."""

from datetime import UTC, datetime

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
from nexus.paper.formal_budget_inventory import inspect_inventory
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_future_three_candidate_slot_is_visible_without_jobs_for_that_day(
    migrated_database_url,
):
    engine, sessions, seed = await seed_synthesis(
        migrated_database_url, candidate_instruments=INSTRUMENTS, complete_research=False
    )
    try:
        # Normal candidate fixture predates this synthetic future slot. Its calendar
        # server timestamp is backdated only in this isolated fixture, not in runtime.
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(
                text("UPDATE exchange_calendar_authorities SET created_at='2026-08-26T20:00:00Z'")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
            )
            await session.execute(
                text("UPDATE exchange_calendar_sessions SET created_at='2026-08-26T20:00:00Z'")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
            )
            cycle = await session.get(AgentPaperCycleRecord, seed["cycle_id"])
            account_id, batch_id = cycle.account_id, cycle.candidate_batch_id
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            before = (
                await session.scalar(select(func.count()).select_from(Job)),
                await session.scalar(select(func.count()).select_from(AuditEvent)),
            )
            report = await inspect_inventory(
                session,
                account_id=account_id,
                batch_id=batch_id,
                enabled=True,
                now=datetime(2026, 8, 27, 13, tzinfo=UTC),
                configuration_hash="a" * 64,
            )
            future = [task for task in report.tasks if task.source == "ordinary_slot"]
            assert len(future) == 4
            assert sum(task.kind == "research" for task in future) == 3
            assert sum(task.kind == "synthesis" for task in future) == 1
            assert all(task.slot_at == datetime(2026, 8, 27, 14, tzinfo=UTC) for task in future)
            assert all(task.native_reservation_ref is None for task in future)
            assert report.protection_status == "protection_not_enforced"
            assert report.activation_permitted is False
            assert report.source_completeness == "observed_only"
            assert before == (
                await session.scalar(select(func.count()).select_from(Job)),
                await session.scalar(select(func.count()).select_from(AuditEvent)),
            )
            assert str(account_id) not in report.model_dump_json()
            assert str(batch_id) not in report.model_dump_json()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "complete,break_association", [(False, False), (True, False), (False, True)]
)
async def test_native_coverage_is_exact_and_global_usage_is_retained(
    migrated_database_url, complete, break_association
):
    from decimal import Decimal

    from nexus.agents.persistence import settle_daily_budget
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

    engine, sessions, seed = await seed_synthesis(
        migrated_database_url, candidate_instruments=INSTRUMENTS, complete_research=False
    )
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, seed["cycle_id"])
            account_id, batch_id = cycle.account_id, cycle.candidate_batch_id
            reservations = tuple(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(reservations) == 3
            if complete:
                for reservation in reservations:
                    await settle_daily_budget(
                        session,
                        run_id=reservation.run_id,
                        actual_usd=reservation.reserved_usd / 2,
                        input_tokens=1,
                        output_tokens=1,
                    )
                # Settle before terminal immutability, then traverse the existing
                # state contract without disabling its (now deferred) guards.
                for state in ("running", "validated", "audited", "synthesized", "completed"):
                    await session.execute(
                        text("UPDATE agent_research_runs SET status=:state"), {"state": state}
                    )
            if break_association:
                # Isolated damaged association: do not repair/relabel runtime evidence.
                await session.execute(
                    text("ALTER TABLE agent_paper_cycle_events DISABLE TRIGGER USER")
                )
                await session.execute(
                    text(
                        "UPDATE agent_paper_cycle_events SET event_hash=:bad "
                        "WHERE result_code='RESEARCH_JOBS_PREPARED'"
                    ),
                    {"bad": "f" * 64},
                )
                await session.execute(
                    text("ALTER TABLE agent_paper_cycle_events ENABLE TRIGGER USER")
                )
            expected = sum(
                (r.settled_usd if complete else r.reserved_usd for r in reservations), Decimal(0)
            )
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            report = await inspect_inventory(
                session,
                account_id=account_id,
                batch_id=batch_id,
                enabled=False,
                now=seed["now"],
                configuration_hash="b" * 64,
            )
            research = [t for t in report.tasks if t.kind == "research"]
            assert len(research) == 3
            assert report.native_reservation_count == 3
            assert report.conservative_native_occupied_usd == expected
            if break_association:
                assert all(t.coverage == "unknown" for t in research)
                assert all(t.native_reservation_ref is None for t in research)
                assert report.unassigned_native_reservation_count == 3
                assert "TASK_ASSOCIATION_UNKNOWN" in report.blocking_codes
            else:
                assert all(t.coverage == ("settled" if complete else "reserved") for t in research)
                assert all(t.observed_uncovered_usd == 0 for t in research)
                assert sum((t.native_occupied_usd for t in research), Decimal(0)) == expected
                assert report.unassigned_native_reservation_count == 0
            assert report.activation_permitted is False
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("configuration_changed", [False, True])
async def test_normal_reader_uses_database_clock_and_readonly_repeatable_snapshot(
    migrated_database_url, tmp_path, monkeypatch, configuration_changed
):
    from uuid import uuid4

    from helpers.paper_api import settings
    from nexus.ops import formal_budget_inventory as cli

    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").touch()
    owner, batch = uuid4(), uuid4()
    reads = 0

    def isolated_configuration(root):
        nonlocal reads
        assert root == tmp_path
        reads += 1
        fingerprint = "d" * 64 if configuration_changed and reads > 1 else "c" * 64
        return settings(migrated_database_url), owner, batch, False, fingerprint

    async def observed_reader(session, **kwargs):
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert "now" not in kwargs
        return await inspect_inventory(session, **kwargs)

    monkeypatch.setattr(cli, "configuration", isolated_configuration)
    monkeypatch.setattr(cli, "inspect_inventory", observed_reader)
    report = await cli.inspect(tmp_path)
    assert reads == 2
    assert report.budget_day_utc == report.as_of.date().isoformat()
    assert report.activation_permitted is False
    assert (
        "FORMAL_CONFIGURATION_CHANGED_DURING_READ" in report.blocking_codes
    ) == configuration_changed
    if configuration_changed:
        assert report.observed_uncovered_formal_usd is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage_context_binding", [False, True])
async def test_normal_same_day_derived_snapshot_is_bound_without_false_unknown(
    migrated_database_url, monkeypatch, damage_context_binding
):
    from integration.test_portfolio_same_day_research_pg import seed_normal_path
    from nexus.agents.paper_context import SOURCE
    from nexus.paper.agent_cycle_models import parse_candidate_manifest
    from nexus.persistence.models.agents import AgentResearchRunRecord

    normal = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS
    )
    engine, sessions, seed = await seed_synthesis(
        migrated_database_url,
        normal_path=normal,
        candidate_instruments=INSTRUMENTS,
        complete_research=False,
    )
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, seed["cycle_id"])
            account_id, batch_id = cycle.account_id, cycle.candidate_batch_id
            bases = {
                entry.research_snapshot_id
                for entry in parse_candidate_manifest(cycle.candidate_manifest, allow_legacy=False)
            }
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            assert len(runs) == 3
            assert all(run.data_snapshot_id not in bases for run in runs)
            if damage_context_binding:
                await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
                for run in runs:
                    damaged = [dict(item) for item in run.evidence_manifest]
                    assert any(item["source"] == SOURCE for item in damaged)
                    for item in damaged:
                        if item["source"] == SOURCE:
                            item["content_hash"] = "f" * 64
                    run.evidence_manifest = damaged
                await session.flush()
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            report = await inspect_inventory(
                session,
                account_id=account_id,
                batch_id=batch_id,
                enabled=False,
                now=seed["now"],
                configuration_hash="e" * 64,
            )
            research = [t for t in report.tasks if t.kind == "research"]
            assert len(research) == 3
            assert all(
                t.coverage == ("unknown" if damage_context_binding else "reserved")
                for t in research
            )
            assert report.unassigned_native_reservation_count == (
                3 if damage_context_binding else 0
            )
            assert report.native_reservation_count == 3
    finally:
        await engine.dispose()
