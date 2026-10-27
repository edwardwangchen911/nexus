"""Daily v2 proof guards use isolated native lifecycle facts, never live calls."""

import copy
import hashlib
import json

import pytest
from integration.test_monitor_daily_receipts_pg import clone
from integration.test_monitor_increment_pg import ready_trigger
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_daily_v2_rejects_unbound_snapshot(migrated_database_url, monkeypatch):
    factory, _, _, parent, _ = await ready_trigger(migrated_database_url, monkeypatch)
    async with factory() as session:
        original = await session.get(DataSnapshot, parent.data_snapshot_id)
    payload = copy.deepcopy(original.payload)
    payload["agent_paper_context"]["pending_monitoring_v2"] = {"purpose": "fake_daily"}
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_V2_CONTEXT_INVALID"):
        async with factory() as session, session.begin():
            session.add(clone(original, payload=payload, revision="f" * 64))
    await factory.kw["bind"].dispose()


async def v2_receipt_source(url, monkeypatch, *, fault=None):
    """Structural DB fixture, using native claim/finalize and budget settlement.

    Model judgments are synthetic; this grants no application qualification.
    """
    from datetime import timedelta
    from decimal import Decimal
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from uuid import uuid4

    from helpers.monitor_materials import prepared_materials
    from nexus.agents.actionable_research import _hash
    from nexus.agents.execution import _advance
    from nexus.agents.persistence import reserve_daily_budget, settle_daily_budget
    from nexus.jobs.models import JobKeyV1, QueueName, RetryPolicy
    from nexus.jobs.repository import claim_next_job, enqueue_job, finalize_job
    from nexus.paper.agent_cycle import _append_event
    from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
    from nexus.persistence.models.monitoring import (
        MonitorCheck,
        MonitorDailyEventReceipt,
        MonitorPlan,
        MonitorTrigger,
    )
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import select

    with TemporaryDirectory(prefix="daily-v2-originals-") as directory:
        factory, settings, _, captured, pointer, batch_id, materials_id = await prepared_materials(
            url, monkeypatch, Path(directory)
        )
    now = captured["clock"][0] + timedelta(minutes=5)
    async with factory() as session:
        material = await session.get(DataSnapshot, materials_id)
        trigger = next(
            row
            for row in await session.scalars(
                select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
            )
            if row.kind == "price"
        )
        check = await session.get(MonitorCheck, trigger.check_id)
        source_plan = await session.get(MonitorPlan, check.plan_id)
        parent = await session.get(AgentResearchRunRecord, source_plan.source_run_id)
    async with factory() as session, session.begin():
        base = await session.get(DataSnapshot, parent.data_snapshot_id)
        prior_cycle = await session.get(
            AgentPaperCycleRecord, base.payload["agent_paper_context"]["cycle_id"]
        )
        check = await session.get(MonitorCheck, trigger.check_id)
        plan = await session.get(MonitorPlan, check.plan_id)
        parent_output = await session.get(AgentResearchOutputRecord, plan.source_output_id)
        cycle = clone(
            prior_cycle,
            id=uuid4(),
            decision_time=now,
            decision_slot_at=now.replace(second=0, microsecond=0),
        )
        session.add(cycle)
        await session.flush()
        context = {
            "purpose": "daily_monitor_context_v2",
            "cycle_id": str(cycle.id),
            "account_id": str(trigger.account_id),
            "instrument_id": str(trigger.instrument_id),
            "decision_time": now.isoformat(),
            "trigger_checks": [
                item
                | {
                    "original_run_id": None,
                    "original_output_id": None,
                    "batch_id": str(batch_id),
                    "materials_snapshot_id": str(materials_id),
                }
                for item in material.payload["members"]
            ],
            "materials": [
                {
                    "snapshot_id": str(materials_id),
                    "content_hash": material.revision,
                    "payload": material.payload,
                }
            ],
            "legacy": None,
            "predecessors": {
                kind: next(
                    (
                        {"plan_id": item["plan_id"], "generation": item["generation"]}
                        for item in material.payload["parents"]
                        if item["kind"] == kind
                    ),
                    {"plan_id": None, "generation": None},
                )
                for kind in ("price", "disclosure")
            },
        }
        payload = copy.deepcopy(base.payload)
        paper = payload["agent_paper_context"]
        paper.update(
            cycle_id=str(cycle.id), decision_time=now.isoformat(), pending_monitoring_v2=context
        )
        snapshot = clone(
            base,
            id=uuid4(),
            payload=payload,
            observed_at=now,
            available_at=now,
            received_at=now,
            revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        )
        session.add(snapshot)
        await session.flush()
        from nexus.persistence.models.research import ResearchEvidenceRef

        paper_hash = _hash(paper)
        ref = {
            "source": "nexus_agent_paper_context",
            "object_id": f"{cycle.id}:{trigger.instrument_id}",
            "available_at": now.isoformat(),
            "content_hash": paper_hash,
            "location": f"memory://sha256/{paper_hash}",
        }
        evidence = ResearchEvidenceRef(
            source=ref["source"],
            provider_object_id=ref["object_id"],
            revision=paper_hash,
            available_at=now,
            content_hash=paper_hash,
            byte_length=len(json.dumps(paper, sort_keys=True, separators=(",", ":")).encode()),
            location=ref["location"],
        )
        session.add(evidence)
        await session.flush()
        run = clone(
            parent,
            id=uuid4(),
            data_snapshot_id=snapshot.id,
            decision_time=now,
            idempotency_key=f"agent-paper:{cycle.id}:{parent.instrument_id}",
            input_hash=hashlib.sha256(str(uuid4()).encode()).hexdigest(),
            status="prepared",
            settled_cost_usd=None,
            input_tokens=None,
            output_tokens=None,
        )
        session.add(run)
        await session.flush()
        await reserve_daily_budget(
            session,
            run_id=run.id,
            now=now,
            daily_limit_usd=Decimal(10),
            worst_case_usd=run.reserved_cost_usd,
            pricing_version=run.pricing_version,
            pricing_fresh=True,
        )
        await _append_event(
            session,
            cycle,
            None,
            "prepared",
            "CYCLE_PREPARED",
            {
                "formal_stage_capacity": True,
                "formal_model_config_id": str(run.model_config_id),
                "formal_model_hash": run.model_hash,
            },
        )
        await _append_event(
            session,
            cycle,
            "prepared",
            "researching",
            "RESEARCH_ADMITTED",
            {"research_run_ids": [str(run.id)]},
        )
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type="agent.paper.research",
                scope_kind="instrument",
                scope_id=str(run.instrument_id),
                effective_at=now,
                input_hash=run.input_hash,
            ),
            schema_version="agent_research_job_v1",
            payload={"run_id": str(run.id)},
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=1,
            available_at=now,
        )

        async def structural_guard(_session, locked_job):
            return locked_job.id == job.id and locked_job.payload == {"run_id": str(run.id)}

        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="daily-db-fixture",
            now=now,
            lease_seconds=330,
            job_ids=(job.id,),
            job_types=("agent.paper.research",),
            research_budget_profile=run.pricing_version,
            locked_job_guard=structural_guard,
        )
        assert claim is not None
        await _advance(session, run, "running")
        context_hash = _hash(context)
        proposal = {
            "context_hash": context_hash,
            "items": [
                {
                    "trigger_id": member["trigger_id"],
                    "status": "unresolved" if fault == "unresolved" else "addressed",
                    "rationale": "Reviewed synthetic evidence",
                    "evidence_refs": [ref],
                }
                for member in context["trigger_checks"]
            ],
        }
        proposal_hash = _hash(proposal)
        roles = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(
                    AgentResearchOutputRecord.run_id == parent.id,
                    AgentResearchOutputRecord.role.is_not(None),
                )
                .order_by(AgentResearchOutputRecord.sequence)
            )
        )
        reviews = {}
        for sequence, source in enumerate(roles, 1):
            role_payload = copy.deepcopy(source.payload)
            if source.role == "orchestrator":
                role_payload["daily_pending_proposal"] = proposal
            elif source.role in {"bear", "evidence_audit"}:
                role_payload["daily_pending_review"] = {
                    "proposal_hash": proposal_hash,
                    "decision": "rejected" if fault == source.role else "accepted",
                    "rationale": "Checked synthetic original",
                    "evidence_refs": proposal["items"][0]["evidence_refs"],
                }
                reviews[source.role] = role_payload["daily_pending_review"]
                if source.role == "evidence_audit":
                    role_payload["vote"] = "oppose" if fault == "audit_vote" else "support"
            elif source.role == "portfolio":
                role_payload["daily_pending_adoption_hash"] = (
                    "e" * 64 if fault == "adoption" else proposal_hash
                )
            session.add(
                clone(
                    source,
                    id=uuid4(),
                    run_id=run.id,
                    sequence=sequence,
                    payload=role_payload,
                    content_hash=_hash(
                        {
                            "status": source.status,
                            "role": source.role,
                            "payload": role_payload,
                            "intent": None,
                        }
                    ),
                )
            )
        for state in ("validated", "audited", "synthesized"):
            await _advance(session, run, state)
        intent = copy.deepcopy(parent_output.intent)
        intent["daily_pending_proposal"] = copy.deepcopy(proposal)
        if fault == "proposal":
            intent["daily_pending_proposal"]["items"][0]["rationale"] = (
                "Synthesis substituted its own claim"
            )
        intent["decision_time"] = now.isoformat()
        output = clone(
            parent_output,
            id=uuid4(),
            run_id=run.id,
            sequence=9,
            intent=intent,
            content_hash=_hash(
                {
                    "status": "completed",
                    "role": None,
                    "payload": parent_output.payload,
                    "intent": intent,
                }
            ),
        )
        session.add(output)
        if fault != "unknown":
            await settle_daily_budget(
                session, run_id=run.id, actual_usd=Decimal(0), input_tokens=0, output_tokens=0
            )
        await _advance(session, run, "completed")
        await finalize_job(
            session,
            claim=claim,
            now=now + timedelta(seconds=1),
            outcome="succeeded",
            result_code="TEST_DAILY_FINISHED",
        )
        await session.flush()
        receipt = MonitorDailyEventReceipt(
            trigger_id=trigger.id,
            account_id=trigger.account_id,
            instrument_id=trigger.instrument_id,
            cycle_id=cycle.id,
            run_id=run.id,
            output_id=output.id,
            snapshot_id=snapshot.id,
            context_hash=context_hash,
            proposal_hash=proposal_hash,
            review_hash=_hash(reviews),
            kind=trigger.kind,
            event_key=trigger.event_key,
            original_run_id=None,
            original_output_id=None,
            batch_id=batch_id,
            materials_snapshot_id=materials_id,
            observed_at=now + timedelta(seconds=2),
        )
    return factory, receipt, trigger, now, plan.id


async def add_pair(session, receipt, now, prior, *, missing=False):
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt

    frozen = await session.get(DataSnapshot, receipt.snapshot_id)
    predecessors = frozen.payload["agent_paper_context"]["pending_monitoring_v2"]["predecessors"]
    from uuid import UUID

    for kind in ("price",) if missing else ("price", "disclosure"):
        session.add(
            MonitorCoverageReceipt(
                account_id=receipt.account_id,
                instrument_id=receipt.instrument_id,
                run_id=receipt.run_id,
                output_id=receipt.output_id,
                kind=kind,
                prior_plan_id=None
                if predecessors[kind]["plan_id"] is None
                else UUID(predecessors[kind]["plan_id"]),
                active_plan_id=None,
                research_as_of=now,
                observed_at=receipt.observed_at,
                status="gap",
                reason="MONITOR_NO_NEW_PLAN",
                quality_status=None,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "unknown",
        "audit_vote",
        "bear",
        "adoption",
        "proposal",
        "unresolved",
        "missing_kind",
        "wrong_hash",
        "wrong_kind",
    ],
)
async def test_daily_v2_native_receipt_contract(migrated_database_url, monkeypatch, fault):
    from nexus.persistence.models.monitoring import MonitorTrigger
    from sqlalchemy import text

    factory, receipt, trigger, now, prior = await v2_receipt_source(
        migrated_database_url, monkeypatch, fault=fault
    )
    if fault == "wrong_hash":
        receipt.context_hash = "0" * 64
    if fault == "wrong_kind":
        receipt.kind = "disclosure"

    async def consume():
        async with factory() as session, session.begin():
            await add_pair(session, receipt, now, prior, missing=fault == "missing_kind")
            session.add(receipt)

    if fault is None:
        await consume()
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(
                text("SELECT nexus_validate_daily_event_receipt(:id)"), {"id": receipt.id}
            )
            original = await session.get(MonitorTrigger, trigger.id)
            assert original.research_run_id is None and original.status == trigger.status
        with pytest.raises(DBAPIError):
            async with factory() as session, session.begin():
                await session.execute(
                    text(
                        "UPDATE research_monitor_daily_event_receipts "
                        "SET event_key=:key WHERE id=:id"
                    ),
                    {"key": "tampered", "id": receipt.id},
                )
    else:
        with pytest.raises(DBAPIError, match="MONITOR_DAILY"):
            await consume()
        async with factory() as session:
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_monitor_daily_event_receipts")
                )
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_monitor_coverage WHERE run_id=:id"),
                    {"id": receipt.run_id},
                )
                == 0
            )
    await factory.kw["bind"].dispose()


def test_daily_v2_empty_roundtrip(migrated_database_url, monkeypatch):
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    async def table():
        engine = create_async_engine(migrated_database_url)
        try:
            async with engine.connect() as conn:
                return await conn.scalar(
                    text("SELECT to_regclass('research_monitor_daily_event_receipts')::text")
                )
        finally:
            await engine.dispose()

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    assert asyncio.run(table()) is not None
    command.downgrade(config, "0096_monitor_event_receipts")
    assert asyncio.run(table()) is None
    command.upgrade(config, "head")
    assert asyncio.run(table()) is not None


@pytest.mark.asyncio
async def test_daily_v2_keeps_v1_and_excludes_double_consumption(
    migrated_database_url, monkeypatch
):
    from integration.test_monitor_daily_receipts_pg import receipt_source

    factory, old_receipt, trigger = await receipt_source(migrated_database_url, monkeypatch)
    from nexus.persistence.models.monitoring import MonitorDailyEventReceipt

    new_receipt = MonitorDailyEventReceipt(
        account_id=old_receipt.account_id,
        instrument_id=old_receipt.instrument_id,
        trigger_id=trigger.id,
        kind=trigger.kind,
        event_key=trigger.event_key,
        cycle_id=old_receipt.cycle_id,
        run_id=old_receipt.run_id,
        output_id=old_receipt.output_id,
        snapshot_id=old_receipt.snapshot_id,
        context_hash=old_receipt.context_hash,
        proposal_hash=old_receipt.review_bundle_hash,
        review_hash="0" * 64,
        observed_at=old_receipt.observed_at,
    )
    # The old public daily path still commits unchanged.
    async with factory() as session, session.begin():
        session.add(old_receipt)
    with pytest.raises(DBAPIError, match="MONITOR_EVENT_ALREADY_CONSUMED"):
        async with factory() as session, session.begin():
            session.add(new_receipt)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_daily_v2_snapshot_freezes_exact_legacy_and_time(migrated_database_url, monkeypatch):
    from uuid import uuid4

    factory, receipt, _, _, _ = await v2_receipt_source(migrated_database_url, monkeypatch)
    async with factory() as session:
        original = await session.get(DataSnapshot, receipt.snapshot_id)
    for mutate in ("member", "kind", "future", "parent", "original_run"):
        payload = copy.deepcopy(original.payload)
        context = payload["agent_paper_context"]["pending_monitoring_v2"]
        if mutate == "member":
            context["trigger_checks"][0]["check_id"] = str(uuid4())
        if mutate == "kind":
            context["trigger_checks"][0]["kind"] = "disclosure"
        if mutate == "future":
            context["decision_time"] = "2099-01-01T00:00:00Z"
        if mutate == "parent":
            context["materials"][0]["payload"]["parents"] = []
        if mutate == "original_run":
            context["trigger_checks"][0]["original_run_id"] = str(uuid4())
        with pytest.raises(DBAPIError, match="MONITOR_DAILY_V2"):
            async with factory() as session, session.begin():
                session.add(
                    clone(
                        original,
                        payload=payload,
                        revision=hashlib.sha256(
                            json.dumps(payload, sort_keys=True).encode()
                        ).hexdigest(),
                    )
                )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["all_unresolved", "valid", "unknown"])
async def test_daily_v2_original_mixed_terminal_binding(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from datetime import timedelta
    from uuid import UUID, uuid4

    from integration import test_monitor_event_receipts_pg as mixed_fixture
    from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
    from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import select, text, update

    factory, member, mixed, _, _, clock = await mixed_fixture.native_result(
        migrated_database_url, monkeypatch, tmp_path, mode
    )
    now = clock + timedelta(seconds=2)
    async with factory() as session:
        plan = await session.get(MonitorPlan, member.plan_id)
        parent = await session.get(AgentResearchRunRecord, plan.source_run_id)
        base = await session.get(DataSnapshot, parent.data_snapshot_id)
        old_cycle = await session.get(
            AgentPaperCycleRecord, UUID(base.payload["agent_paper_context"]["cycle_id"])
        )
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == member.run_id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
    cycle = clone(
        old_cycle,
        id=uuid4(),
        decision_time=now,
        decision_slot_at=now.replace(second=0, microsecond=0),
    )
    members = {x["trigger_id"]: x for x in mixed["materials"]["members"]}
    context = {
        "purpose": "daily_monitor_context_v2",
        "cycle_id": str(cycle.id),
        "account_id": str(member.account_id),
        "instrument_id": str(member.instrument_id),
        "decision_time": now.isoformat(),
        "trigger_checks": [
            x
            | {
                "plan_id": members[x["trigger_id"]]["plan_id"],
                "original_run_id": str(member.run_id),
                "original_output_id": str(output.id if output else uuid4()),
                "batch_id": str(member.batch_id),
                "materials_snapshot_id": mixed["materials_snapshot_id"],
            }
            for x in mixed["trigger_checks"]
        ],
        "materials": [
            {
                "snapshot_id": mixed["materials_snapshot_id"],
                "content_hash": mixed["materials_hash"],
                "payload": mixed["materials"],
            }
        ],
        "legacy": None,
        "predecessors": {
            kind: next(
                (
                    {"plan_id": x["plan_id"], "generation": x["generation"]}
                    for x in mixed["materials"]["parents"]
                    if x["kind"] == kind
                ),
                {"plan_id": None, "generation": None},
            )
            for kind in ("price", "disclosure")
        },
    }
    payload = copy.deepcopy(base.payload)
    payload["agent_paper_context"].update(
        cycle_id=str(cycle.id), decision_time=now.isoformat(), pending_monitoring_v2=context
    )
    snapshot = clone(
        base,
        payload=payload,
        observed_at=now,
        available_at=now,
        received_at=now,
        revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
    )

    async def freeze():
        async with factory() as session, session.begin():
            # No trigger ownership is changed. Normal known terminal closure only.
            if mode != "unknown":
                await session.execute(
                    update(MonitorTrigger)
                    .where(MonitorTrigger.batch_id == member.batch_id)
                    .values(status="completed")
                )
            session.add(cycle)
            await session.flush()
            session.add(snapshot)

    if mode == "all_unresolved":
        await freeze()
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(
                text("SELECT nexus_validate_daily_monitor_v2(:id)"), {"id": snapshot.id}
            )
    else:
        with pytest.raises(DBAPIError, match="MONITOR_DAILY_V2_ORIGINAL_INVALID"):
            await freeze()
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "missing_proof", "unknown"])
async def test_daily_v2_plan_requires_atomic_consumption(migrated_database_url, monkeypatch, mode):
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan

    factory, receipt, _, now, prior = await v2_receipt_source(
        migrated_database_url, monkeypatch, fault="unknown" if mode == "unknown" else None
    )
    async with factory() as session:
        old = await session.get(MonitorPlan, prior)
        output = await session.get(AgentResearchOutputRecord, receipt.output_id)
        frozen = await session.get(DataSnapshot, receipt.snapshot_id)
        disclosure_prior = frozen.payload["agent_paper_context"]["pending_monitoring_v2"][
            "predecessors"
        ]["disclosure"]["plan_id"]
        from uuid import UUID
    new_plan = clone(
        old,
        generation=old.generation + 1,
        predecessor_id=old.id,
        source_run_id=receipt.run_id,
        source_output_id=receipt.output_id,
        data_snapshot_id=receipt.snapshot_id,
        source_hash=output.content_hash,
        effective_at=receipt.observed_at,
    )

    async def install():
        async with factory() as session, session.begin():
            session.add(new_plan)
            await session.flush()
            for kind in ("price", "disclosure"):
                session.add(
                    MonitorCoverageReceipt(
                        account_id=receipt.account_id,
                        instrument_id=receipt.instrument_id,
                        run_id=receipt.run_id,
                        output_id=receipt.output_id,
                        kind=kind,
                        prior_plan_id=prior if kind == "price" else UUID(disclosure_prior),
                        active_plan_id=new_plan.id if kind == "price" else None,
                        research_as_of=now,
                        observed_at=receipt.observed_at,
                        status="installed" if kind == "price" else "gap",
                        reason="MONITOR_TEST",
                        quality_status=None,
                    )
                )
            if mode == "valid":
                session.add(receipt)

    if mode == "valid":
        await install()
    else:
        with pytest.raises(DBAPIError, match="MONITOR_DAILY_V2_PLAN_INVALID"):
            await install()
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_daily_v2_rejects_rehashed_parentless_original(migrated_database_url, monkeypatch):
    from uuid import uuid4

    from nexus.monitoring.mixed_contracts import canonical_hash

    factory, receipt, _, _, _ = await v2_receipt_source(migrated_database_url, monkeypatch)
    async with factory() as session:
        frozen = await session.get(DataSnapshot, receipt.snapshot_id)
        original = await session.get(DataSnapshot, receipt.materials_snapshot_id)
    corrupt = copy.deepcopy(original.payload)
    corrupt["parents"] = []
    replacement = clone(original, id=uuid4(), payload=corrupt, revision=canonical_hash(corrupt))
    payload = copy.deepcopy(frozen.payload)
    context = payload["agent_paper_context"]["pending_monitoring_v2"]
    context["materials"] = [
        {
            "snapshot_id": str(replacement.id),
            "content_hash": replacement.revision,
            "payload": corrupt,
        }
    ]
    for member in context["trigger_checks"]:
        member["materials_snapshot_id"] = str(replacement.id)
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_V2_MATERIAL_CLOSED_SET_INVALID"):
        async with factory() as session, session.begin():
            session.add(replacement)
            await session.flush()
            session.add(
                clone(
                    frozen,
                    payload=payload,
                    revision=hashlib.sha256(
                        json.dumps(payload, sort_keys=True).encode()
                    ).hexdigest(),
                )
            )
    await factory.kw["bind"].dispose()
