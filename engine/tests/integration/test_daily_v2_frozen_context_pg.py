"""Independent normal daily admission freezes exact typed material or a bounded gap."""

from datetime import timedelta

import pytest
from helpers.monitor_materials import prepared_materials
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.monitoring import daily_v2_context as daily
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import JobLease
from sqlalchemy import func, select


async def new_daily(factory, settings, captured, pointer):
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        now = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        candidates = await load_agent_paper_candidate_batch(
            session, batch_id=settings.agent_paper_candidate_batch_id
        )
        quote_scope = tuple(await session.scalars(select(LatestQuote.instrument_id).distinct()))
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    for instrument in quote_scope:
        await append_fresh_inputs(factory, instrument, now)
    async with factory() as session, session.begin():
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=pointer.account_id,
            now=now,
            expected_calendar_authority_id=CALENDAR_ID,
            expected_universe_snapshot_id=candidates.universe_snapshot_id,
        )
        result = await start_schwab_agent_paper_cycle(
            session,
            authority=authority,
            decision_slot_at=now.replace(second=0, microsecond=0),
            idempotency_key="daily-v2-normal",
            requested_instrument_ids=candidates.instrument_ids,
            candidate_batch_id=candidates.batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=parent.model_config_id,
        )
        assert result.result_code == "RESEARCH_JOBS_PREPARED", result
    return now, result


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "corrupt", "overflow", "legacy"])
async def test_normal_daily_freezes_typed_package_or_explicit_gap(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    factory, settings, store, captured, pointer, batch_id, material_id = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    if fault in {"overflow", "legacy"}:
        from integration.test_monitor_pending_pg import pending_members

        async with factory() as session:
            original = await session.scalar(
                select(MonitorTrigger).where(
                    MonitorTrigger.batch_id == batch_id, MonitorTrigger.kind == "price"
                )
            )
        await pending_members(factory, original, 31 if fault == "overflow" else 2)
    if fault == "corrupt":

        async def corrupt(*args, **kwargs):
            return None

        monkeypatch.setattr(daily, "read_batch_inputs", corrupt)
    complete_freezes = []
    real_capacities = []
    if fault == "legacy":
        from nexus.monitoring import daily_v2_capacity as capacity

        freeze = daily.freeze_daily_events
        envelope = capacity.candidate_capacity_envelope

        async def capture_freeze(*args, **kwargs):
            value = await freeze(*args, **kwargs)
            if value[0] is not None:
                complete_freezes.append(value[0])
            return value

        def capture_capacity(committee, **kwargs):
            value = envelope(committee, **kwargs)
            real_capacities.append((value["wire_upper_bytes"], committee._max_wire_bytes))
            return value

        monkeypatch.setattr(daily, "freeze_daily_events", capture_freeze)
        monkeypatch.setattr(capacity, "candidate_capacity_envelope", capture_capacity)
    try:
        now, result = await new_daily(factory, settings, captured, pointer)
        async with factory() as session:
            run = await session.scalar(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids),
                    AgentResearchRunRecord.instrument_id == pointer.instrument_id,
                )
            )
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=pointer.instrument_id,
                as_of=now,
            )
            assert bound is not None
            context = bound[0]
            assert context.pending_monitoring is None and context.pending_monitoring_gap is None
            if fault in {"corrupt", "overflow"}:
                assert context.pending_monitoring_v2 is None
                assert context.pending_monitoring_v2_gap.reason == (
                    "MONITOR_PENDING_OVERFLOW" if fault == "overflow" else "MONITOR_CONTENT_INVALID"
                )
                assert context.pending_monitoring_v2_gap.pending_count == (
                    33 if fault == "overflow" else 3
                )
            else:
                if fault == "legacy":
                    # Full freezing and optional model-input admission are separate boundaries.
                    # Keep every original member assertion; the real wire gate may omit only
                    # the entire optional bundle before any Job is published.
                    assert complete_freezes
                    frozen = complete_freezes[0]
                    assert any(size > limit for size, limit in real_capacities), real_capacities
                    assert context.pending_monitoring_v2 is None
                    assert context.pending_monitoring_v2_gap.reason == "MONITOR_CONTENT_CAPACITY"
                    assert context.pending_monitoring_v2_gap.pending_count == 4
                    assert len(result.research_run_ids) == 3
                else:
                    frozen = context.pending_monitoring_v2
                assert frozen is not None, context.model_dump(mode="json")
                assert frozen.materials[0].snapshot_id == material_id
                assert len(frozen.trigger_checks) == (4 if fault == "legacy" else 3)
                assert len(frozen.materials[0].payload.contents) == 2
                assert (frozen.legacy is not None) == (fault == "legacy")
                assert all(
                    row.original_run_id is None and row.batch_id in (None, batch_id)
                    for row in frozen.trigger_checks
                )
                assert await daily.validate_daily_events(session, frozen)
                assert set(frozen.predecessors) == {"price", "disclosure"}
                # Mutable assignment after freezing must not rewrite historical input.
                from types import SimpleNamespace
                from uuid import uuid4

                original_get = session.get

                async def later_assignment(model, identity, *args, **kwargs):
                    row = await original_get(model, identity, *args, **kwargs)
                    if model is MonitorTrigger and row is not None:
                        return SimpleNamespace(
                            **{
                                **{
                                    column.name: getattr(row, column.name)
                                    for column in row.__table__.columns
                                },
                                "research_run_id": uuid4(),
                            }
                        )
                    return row

                with monkeypatch.context() as changed:
                    changed.setattr(session, "get", later_assignment)
                    assert await daily.validate_daily_events(session, frozen)
                altered = frozen.materials[0].model_copy(update={"content_hash": "f" * 64})
                assert not await daily.validate_daily_events(
                    session, frozen.model_copy(update={"materials": (altered,)})
                )
            originals = tuple(
                await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            )
            assert len(originals) == 3 and all(
                row.research_run_id is None and row.status == "pending" for row in originals
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_original_known_unresolved_is_frozen_by_later_normal_daily(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration import test_monitor_mixed_increment_pg as original
    from nexus.persistence.models.agents import AgentResearchOutputRecord

    saved = []
    prepare = original.prepared_materials

    async def capture(*args, **kwargs):
        value = await prepare(*args, **kwargs)
        saved.append(value)
        return value

    monkeypatch.setattr(original, "prepared_materials", capture)
    await original.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
        migrated_database_url, monkeypatch, tmp_path, "unresolved"
    )
    factory, settings, store, captured, pointer, batch_id, material_id = saved[0]
    try:
        async with factory() as session:
            originals = tuple(
                await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            )
            old_run_ids = {row.research_run_id for row in originals}
            old_output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id.in_(old_run_ids),
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            old_hash = old_output.content_hash
        now, result = await new_daily(factory, settings, captured, pointer)
        async with factory() as session:
            run = await session.scalar(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids),
                    AgentResearchRunRecord.instrument_id == pointer.instrument_id,
                )
            )
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=pointer.instrument_id,
                as_of=now,
            )
            assert bound is not None
            context = bound[0].pending_monitoring_v2
            assert context is not None, bound[0].model_dump(mode="json")
            assert len(context.trigger_checks) == 1
            item = context.trigger_checks[0]
            assert item.original_run_id in old_run_ids and item.original_output_id == old_output.id
            assert item.materials_snapshot_id == material_id and item.batch_id == batch_id
            assert await daily.validate_daily_events(session, context)
            old_disclosure = next(
                row for row in context.materials[0].payload.parents if row["kind"] == "disclosure"
            )
            assert str(context.predecessors["disclosure"].plan_id) != old_disclosure["plan_id"]
            assert (
                await session.get(AgentResearchOutputRecord, old_output.id)
            ).content_hash == old_hash
            assert {
                row.research_run_id
                for row in await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            } == old_run_ids
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_missing_prepared_originals_is_a_gap_not_a_daily_admission_failure(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_monitor_batches_pg import installed
    from nexus.monitoring.batches import scan_stock_batch

    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path
    )
    try:
        async with factory() as session, session.begin():
            await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
        now, result = await new_daily(factory, settings, captured, pointer)
        async with factory() as session:
            run = await session.scalar(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids),
                    AgentResearchRunRecord.instrument_id == pointer.instrument_id,
                )
            )
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=pointer.instrument_id,
                as_of=now,
            )
            assert bound is not None and bound[0].pending_monitoring_v2 is None
            assert bound[0].pending_monitoring_v2_gap.reason == "MONITOR_CONTENT_MISSING"
            assert bound[0].pending_monitoring_v2_gap.pending_count == 3
    finally:
        await factory.kw["bind"].dispose()
