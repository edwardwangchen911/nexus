"""One validation operation must not repeatedly expand identical frozen sources."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from helpers.monitor_materials import prepared_materials
from integration.test_monitor_daily_v2_native_pg import unresolved_then_daily
from nexus.monitoring import source_qualification
from nexus.monitoring.batch_inputs import read_batch_inputs
from nexus.monitoring.daily_v2_context import DailyEventContext, validate_daily_events
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import select


@pytest.mark.asyncio
async def test_one_material_read_qualifies_shared_parent_output_once(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, _, _, captured, pointer, _, material_id = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    original = source_qualification.load_qualified_source
    calls = []

    async def counted(session, **kwargs):
        calls.append((kwargs["output_id"], kwargs["now"]))
        return await original(session, **kwargs)

    monkeypatch.setattr(source_qualification, "load_qualified_source", counted)
    try:
        async with factory() as session:
            material = await session.get(DataSnapshot, material_id)
            parents = material.payload["parents"]
            assert {row["kind"] for row in parents} == {"price", "disclosure"}
            assert len({row["output_id"] for row in parents}) == 1
            frozen = await read_batch_inputs(
                session, snapshot_id=material_id, as_of=captured["clock"][0]
            )
            assert frozen is not None
            assert len(frozen.members) == 3 and len(frozen.parents) == 2
            parent_output = parents[0]["output_id"]
            assert sum(str(output) == parent_output for output, _ in calls) == 1
            second = await session.get(MonitorPlan, UUID(parents[-1]["plan_id"]))
            original_hash = second.source_hash
            # Corrupt only the loaded synthetic candidate and suppress autoflush;
            # the persisted immutable plan is never updated.
            with session.no_autoflush:
                try:
                    second.source_hash = "0" * 64
                    assert (
                        await read_batch_inputs(
                            session, snapshot_id=material_id, as_of=captured["clock"][0]
                        )
                        is None
                    )
                finally:
                    second.source_hash = original_hash
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_one_daily_validation_reuses_original_source_but_rechecks_events_and_next_call(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, _, pointer, _, _, old_run_id, _, now, result, _ = await unresolved_then_daily(
        migrated_database_url, monkeypatch, tmp_path
    )
    original = source_qualification.load_qualified_source
    calls = []

    async def counted(session, **kwargs):
        calls.append((kwargs["output_id"], kwargs["now"]))
        return await original(session, **kwargs)

    monkeypatch.setattr(source_qualification, "load_qualified_source", counted)
    try:
        async with factory() as session:
            run = await session.scalar(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids),
                    AgentResearchRunRecord.instrument_id == pointer.instrument_id,
                )
            )
            snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
            before = snapshot.revision
            context = DailyEventContext.model_validate(
                snapshot.payload["agent_paper_context"]["pending_monitoring_v2"]
            )
            assert len(context.trigger_checks) == 3
            assert {item.original_run_id for item in context.trigger_checks} == {old_run_id}
            original_outputs = {item.original_output_id for item in context.trigger_checks}
            assert len(original_outputs) == 1
            assert await validate_daily_events(session, context)
            assert sum(output in original_outputs for output, _ in calls) == 1
            # The last event is checked even after its source was qualified for
            # earlier members. Only the new in-memory candidate is malformed.
            broken = context.model_copy(
                update={
                    "trigger_checks": (
                        *context.trigger_checks[:-1],
                        context.trigger_checks[-1].model_copy(update={"check_id": uuid4()}),
                    )
                }
            )
            assert not await validate_daily_events(session, broken)

            lease = await session.scalar(
                select(JobLease)
                .join(Job, Job.id == JobLease.job_id)
                .where(Job.payload["run_id"].astext == str(old_run_id))
            )
            job = await session.get(Job, lease.job_id)
            prior_calls = sum(output in original_outputs for output, _ in calls)
            probe = await session.begin_nested()
            try:
                # An adversarial lease move in the isolated DB must invalidate
                # the next read even in this same AsyncSession. No loader is
                # replaced, and the finished Run/output/budget remain untouched.
                lease.heartbeat_at = job.finished_at + timedelta(microseconds=1)
                await session.flush()
                assert not await validate_daily_events(session, context)
                assert sum(output in original_outputs for output, _ in calls) == prior_calls + 1
            finally:
                await probe.rollback()
            assert await validate_daily_events(session, context)
            assert snapshot.revision == before and context.decision_time == now
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_shared_parent_still_rejects_current_pricing_expiry_between_members(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.persistence.models.agents import AgentModelConfigRecord

    factory, _, _, captured, _, _, material_id = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    original = source_qualification.load_qualified_source
    as_of = captured["clock"][0]
    qualified = []

    async def expire_after_qualification(session, **kwargs):
        result = await original(session, **kwargs)
        qualified.append(result.output.id)
        model = await session.get(AgentModelConfigRecord, result.run.model_config_id)
        captured["clock"][0] = model.pricing_expires_at
        return result

    monkeypatch.setattr(source_qualification, "load_qualified_source", expire_after_qualification)
    try:
        async with factory() as session:
            assert await read_batch_inputs(session, snapshot_id=material_id, as_of=as_of) is None
            assert len(qualified) == 1
    finally:
        captured["clock"][0] = as_of
        await factory.kw["bind"].dispose()
