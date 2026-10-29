"""Database fences for typed monitor increments, independent of model execution."""

import asyncio
import json
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


async def columns(url):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return set(
                await connection.scalars(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name='research_monitor_increments'"
                    )
                )
            )
    finally:
        await engine.dispose()


def test_mixed_increment_column_and_empty_roundtrip(migrated_database_url, monkeypatch):
    assert "batch_id" in asyncio.run(columns(migrated_database_url))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    command.downgrade(config, "0094_monitor_batches")
    assert "batch_id" not in asyncio.run(columns(migrated_database_url))
    command.upgrade(config, "head")
    assert "batch_id" in asyncio.run(columns(migrated_database_url))


def clone(row, **changes):
    return type(row)(
        **{
            **{c.key: getattr(row, c.key) for c in row.__table__.columns},
            **({"id": uuid4()} if "id" in row.__table__.columns else {}),
            **changes,
        }
    )


def runtime_head(monkeypatch):
    from nexus.ops import research_monitor
    from nexus.persistence import migrations

    assert research_monitor.CURRENT_MIGRATION_HEAD == migrations.CURRENT_MIGRATION_HEAD


async def mixed_fixture(url, monkeypatch, tmp_path):
    from integration.test_monitor_batches_pg import installed
    from nexus.monitoring.batches import scan_stock_batch
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import (
        MonitorCheck,
        MonitorObservationBatch,
        MonitorPlan,
        MonitorTrigger,
    )

    runtime_head(monkeypatch)
    factory, settings, store, captured, pointer = await installed(url, monkeypatch, tmp_path)
    async with factory() as session, session.begin():
        report = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    now = captured["clock"][0] + timedelta(seconds=1)
    async with factory() as session, session.begin():
        batch = await session.get(MonitorObservationBatch, UUID(report["batch_id"]))
        parents, members = {}, []
        for tid in batch.trigger_ids:
            t = await session.get(MonitorTrigger, UUID(tid))
            k = await session.get(MonitorCheck, t.check_id)
            p = await session.get(MonitorPlan, k.plan_id)
            o = await session.get(AgentResearchOutputRecord, p.source_output_id)
            roles = tuple(
                await session.scalars(
                    select(AgentResearchOutputRecord)
                    .where(
                        AgentResearchOutputRecord.run_id == p.source_run_id,
                        AgentResearchOutputRecord.role.is_not(None),
                    )
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            parents[p.kind] = dict(
                kind=p.kind,
                plan_id=str(p.id),
                generation=p.generation,
                run_id=str(p.source_run_id),
                output_id=str(o.id),
                output_hash=o.content_hash,
                intent=o.intent,
                role_outputs=[row.payload for row in roles],
            )
            members.append(
                dict(
                    trigger_id=tid,
                    check_id=str(k.id),
                    plan_id=str(p.id),
                    kind=t.kind,
                    event_key=t.event_key,
                    cause=t.cause,
                    checked_at=k.checked_at.isoformat(),
                    observation=k.observation,
                )
            )
        # Relational fixture only. This cannot qualify filing content for models;
        # application tests independently verify real archive bytes and receipts.
        materials = dict(
            schema_version="monitor_batch_inputs_v1",
            account_id=str(batch.account_id),
            instrument_id=str(batch.instrument_id),
            batch_id=str(batch.id),
            prepared_at=now.isoformat(),
            parents=sorted(parents.values(), key=lambda p: p["plan_id"]),
            members=members,
            contents=[],
            impact_review="not_performed",
            execution_locked=True,
        )
        digest = sha256(
            json.dumps(materials, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        ms = DataSnapshot(
            source="nexus_monitor_batch_inputs",
            provider_object_id=str(batch.id),
            revision=digest,
            observed_at=now,
            available_at=now,
            received_at=now,
            schema_version="monitor_batch_inputs_v1",
            payload=materials,
        )
        session.add(ms)
        await session.flush()
        p = parents["price"]
        context = dict(
            purpose="monitor_increment_v2",
            account_id=str(batch.account_id),
            instrument_id=str(batch.instrument_id),
            batch_id=str(batch.id),
            materials_snapshot_id=str(ms.id),
            materials_hash=digest,
            materials=materials,
            plan_id=p["plan_id"],
            plan_generation=p["generation"],
            parent_run_id=p["run_id"],
            parent_output_id=p["output_id"],
            parent_output_hash=p["output_hash"],
            parent_intent=p["intent"],
            decision_time=now.isoformat(),
            decision_quote={},
            candidate={},
            authority={},
            paper_portfolio={},
            execution_locked=True,
            trigger_checks=[{k: v for k, v in row.items() if k != "plan_id"} for row in members],
            disclosure_monitoring=dict(
                schema_version="disclosure_monitor_context_v1",
                instrument_id=str(batch.instrument_id),
                research_as_of=now.isoformat(),
                baseline=None,
                gap_reason="SEC_BASELINE_MISSING",
            ),
        )
    return factory, batch, now, context


def snapshot(context, now):
    from nexus.persistence.models.data import DataSnapshot

    payload = dict(purpose="monitor_increment_v2", monitor_context=context)
    return DataSnapshot(
        source="nexus_monitor_increment_v2",
        provider_object_id=context["instrument_id"],
        schema_version="monitor_increment_v2",
        payload=payload,
        revision=sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        observed_at=now,
        available_at=now,
        received_at=now,
    )


@pytest.mark.asyncio
async def test_mixed_members_materials_scope_and_late_binding_fences(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.persistence.models.agents import (
        AgentDataSnapshotEvidenceRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
    from nexus.persistence.models.research import ResearchEvidenceRef
    from nexus.persistence.models.runtime import Job

    factory, batch, now, context = await mixed_fixture(migrated_database_url, monkeypatch, tmp_path)
    # Every mutation starts from a valid typed three-member envelope.
    mutations = [
        (lambda c: c["trigger_checks"].pop(), "MONITOR_MIXED_MATERIALS_INVALID"),
        (
            lambda c: c["trigger_checks"][0].update(
                kind="disclosure" if c["trigger_checks"][0]["kind"] == "price" else "price"
            ),
            "MONITOR_MIXED_MEMBERS_INVALID",
        ),
        (lambda c: c.update(account_id=str(uuid4())), "MONITOR_MIXED_BATCH_INVALID"),
        (lambda c: c.update(materials_hash="0" * 64), "MONITOR_MIXED_MATERIALS_INVALID"),
        (
            lambda c: c["trigger_checks"][0].update(event_key="foreign"),
            "MONITOR_MIXED_MEMBERS_INVALID",
        ),
        (lambda c: c.update(disclosure_monitoring=None), "MONITOR_MIXED_BASELINE_INVALID"),
        (lambda c: c["trigger_checks"].reverse(), "MONITOR_MIXED_MATERIALS_INVALID"),
        (lambda c: c.update(parent_output_hash="0" * 64), "MONITOR_MIXED_PARENT_INVALID"),
    ]
    for mutate, reason in mutations:
        c = deepcopy(context)
        mutate(c)
        with pytest.raises(DBAPIError, match=reason):
            async with factory() as session, session.begin():
                session.add(snapshot(c, now))
                await session.flush()
                await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    async with factory() as session, session.begin():
        frozen = snapshot(context, now)
        session.add(frozen)
        await session.flush()
    digest = sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()
    async with factory() as session, session.begin():
        evidence = ResearchEvidenceRef(
            source="nexus_monitor_increment_v2",
            provider_object_id=batch.trigger_ids[0],
            revision=digest,
            content_hash=digest,
            available_at=now,
            location=f"memory://sha256/{digest}",
            byte_length=100,
        )
        session.add(evidence)
        await session.flush()
        binding = AgentDataSnapshotEvidenceRecord(
            data_snapshot_id=frozen.id,
            instrument_id=batch.instrument_id,
            evidence_ref_id=evidence.id,
            authority_kind="research",
            selected_at=None,
            source=evidence.source,
            provider_object_id=evidence.provider_object_id,
            revision=digest,
            content_hash=digest,
            available_at=now,
            location=evidence.location,
        )
        session.add(binding)
    with pytest.raises(DBAPIError, match="not owned by the instrument"):
        async with factory() as session, session.begin():
            bad = clone(
                evidence,
                provider_object_id=str(uuid4()),
                revision="e" * 64,
                content_hash="e" * 64,
                location="memory://sha256/" + "e" * 64,
            )
            session.add(bad)
            await session.flush()
            session.add(
                clone(
                    binding,
                    evidence_ref_id=bad.id,
                    provider_object_id=bad.provider_object_id,
                    revision=bad.revision,
                    content_hash=bad.content_hash,
                    location=bad.location,
                )
            )
            await session.flush()
    async with factory() as session, session.begin():
        parent = await session.get(AgentResearchRunRecord, UUID(context["parent_run_id"]))
        native = clone(
            parent,
            data_snapshot_id=frozen.id,
            decision_time=now,
            idempotency_key="typed-structural-run",
            status="prepared",
            settled_cost_usd=None,
            input_tokens=None,
            output_tokens=None,
        )
        session.add(native)
        await session.flush()
        old_job = await session.scalar(
            select(Job).where(Job.payload["run_id"].astext == str(parent.id))
        )
        job = clone(
            old_job,
            job_key=str(uuid4()),
            payload={"run_id": str(native.id)},
            effective_at=now,
            status="queued",
            attempt_count=0,
            started_at=None,
            finished_at=None,
        )
        session.add(job)
        await session.flush()
        member = MonitorIncrement(
            batch_id=batch.id,
            account_id=batch.account_id,
            instrument_id=batch.instrument_id,
            trigger_id=UUID(batch.trigger_ids[0]),
            plan_id=UUID(context["plan_id"]),
            run_id=native.id,
            job_id=job.id,
            snapshot_id=frozen.id,
            research_as_of=now,
            session_date=now.date(),
            reserved_sdk_calls=3,
            daily_sdk_call_limit=90,
            context_hash=digest,
            manifest_hash="b" * 64,
        )
        session.add(member)
        await session.execute(
            update(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch.id)
            .values(status="admitted", research_run_id=native.id)
        )
    with pytest.raises(DBAPIError, match="MONITOR_MIXED_BINDING_INVALID"):
        async with factory() as session, session.begin():
            session.add(clone(native, idempotency_key="forbidden-second-run"))
            await session.flush()
            await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    with pytest.raises(DBAPIError, match="MONITOR_MIXED_BINDING_INVALID"):
        async with factory() as session, session.begin():
            await session.execute(
                update(MonitorTrigger)
                .where(MonitorTrigger.id == UUID(batch.trigger_ids[0]))
                .values(status="unknown")
            )
    async with factory() as session, session.begin():
        await session.execute(
            update(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch.id)
            .values(status="unknown")
        )
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    with pytest.raises(DBAPIError, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(
            command.downgrade,
            Config(str(Path(__file__).parents[2] / "alembic.ini")),
            "0094_monitor_batches",
        )
    from helpers.migration_guard import historical_downgrade_guard

    with pytest.raises(DBAPIError, match="MONITOR_MIXED_HISTORY_DOWNGRADE_FORBIDDEN"):
        await historical_downgrade_guard(migrated_database_url, "0095_monitor_mixed_increments")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_legacy_single_price_admission_stays_valid(migrated_database_url, monkeypatch):
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_monitor_increment_pg import ready_trigger
    from nexus.ops.research_monitor import parser, run
    from nexus.persistence.models.monitoring import MonitorIncrement

    runtime_head(monkeypatch)
    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    result = await run(
        settings,
        parser().parse_args(
            ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
        ),
    )
    assert result["status"] == "admitted"
    async with factory() as session:
        member = await session.scalar(select(MonitorIncrement))
        assert member.batch_id is None and member.reserved_sdk_calls == 3
    await factory.kw["bind"].dispose()
