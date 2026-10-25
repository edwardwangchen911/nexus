"""Daily receipts bind original frozen facts without changing pending triggers."""

import copy
import hashlib
import json

import pytest
from integration.test_monitor_increment_pg import ready_trigger
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_daily_pending_snapshot_cannot_smuggle_unbound_context(
    migrated_database_url, monkeypatch
):
    factory, _settings, _trigger, parent, _now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    async with factory() as session:
        original = await session.get(DataSnapshot, parent.data_snapshot_id)
    payload = copy.deepcopy(original.payload)
    payload["agent_paper_context"]["pending_monitoring"] = {
        "purpose": "fake_daily",
        "parents": [],
        "trigger_checks": [],
    }
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_CONTEXT_INVALID"):
        async with factory() as session, session.begin():
            session.add(
                DataSnapshot(
                    source=original.source,
                    schema_version=original.schema_version,
                    provider_object_id=original.provider_object_id,
                    revision=hashlib.sha256(
                        json.dumps(payload, sort_keys=True).encode()
                    ).hexdigest(),
                    observed_at=original.observed_at,
                    available_at=original.available_at,
                    received_at=original.received_at,
                    payload=payload,
                )
            )
    await factory.kw["bind"].dispose()


def clone(row, **changes):
    fields = {
        column.key: getattr(row, column.key)
        for column in row.__table__.columns
        if column.key not in {"id", "created_at"}
    }
    return type(row)(**(fields | changes))


async def receipt_source(url, monkeypatch, *, fault=None):
    """Structural DB fixture, using native claim/finalize and budget settlement.

    Model judgments are synthetic; this grants no application qualification.
    """
    from datetime import timedelta
    from decimal import Decimal
    from uuid import uuid4

    from nexus.agents.actionable_research import _hash
    from nexus.agents.execution import _advance
    from nexus.agents.persistence import reserve_daily_budget, settle_daily_budget
    from nexus.jobs.models import JobKeyV1, QueueName, RetryPolicy
    from nexus.jobs.repository import claim_next_job, enqueue_job, finalize_job
    from nexus.paper.agent_cycle import _append_event
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import (
        MonitorCheck,
        MonitorDailyTriggerReceipt,
        MonitorPlan,
    )
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import select

    factory, settings, trigger, parent, now = await ready_trigger(url, monkeypatch)
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
            "purpose": "daily_monitor_context_v1",
            "cycle_id": str(cycle.id),
            "account_id": str(trigger.account_id),
            "instrument_id": str(trigger.instrument_id),
            "decision_time": now.isoformat(),
            "parents": [
                {
                    "plan_id": str(plan.id),
                    "generation": plan.generation,
                    "condition": plan.condition,
                    "condition_hash": plan.condition_hash,
                    "parent_run_id": str(parent.id),
                    "parent_output_id": str(parent_output.id),
                    "parent_output_hash": parent_output.content_hash,
                    "parent_intent": parent_output.intent,
                }
            ],
            "trigger_checks": [
                {
                    "trigger_id": str(trigger.id),
                    "check_id": str(check.id),
                    "plan_id": str(plan.id),
                    "cause": trigger.cause,
                    "checked_at": check.checked_at.isoformat(),
                    "observation": check.observation,
                }
            ],
        }
        payload = copy.deepcopy(base.payload)
        paper = payload["agent_paper_context"]
        paper.update(
            cycle_id=str(cycle.id), decision_time=now.isoformat(), pending_monitoring=context
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
                    "trigger_id": str(trigger.id),
                    "status": "unresolved" if fault == "unresolved" else "addressed",
                    "rationale": "Reviewed synthetic evidence",
                    "evidence_refs": parent_output.intent["evidence_refs"][:1],
                }
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
        receipt = MonitorDailyTriggerReceipt(
            trigger_id=trigger.id,
            account_id=trigger.account_id,
            instrument_id=trigger.instrument_id,
            cycle_id=cycle.id,
            run_id=run.id,
            output_id=output.id,
            snapshot_id=snapshot.id,
            context_hash=context_hash,
            review_bundle_hash=proposal_hash,
            observed_at=now + timedelta(seconds=2),
        )
    return factory, receipt, trigger


@pytest.mark.asyncio
async def test_daily_receipt_is_append_only_and_keeps_trigger_original(
    migrated_database_url, monkeypatch
):
    from nexus.persistence.models.monitoring import MonitorTrigger

    factory, receipt, trigger = await receipt_source(migrated_database_url, monkeypatch)
    async with factory() as session, session.begin():
        from nexus.persistence.models.agents import AgentResearchOutputRecord
        from sqlalchemy import select

        saved_roles = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == receipt.run_id,
                    AgentResearchOutputRecord.role.is_not(None),
                )
            )
        )
        assert len(saved_roles) == 8
        assert sorted(row.payload.get("role", "MISSING") for row in saved_roles) == sorted(
            [
                "orchestrator",
                "fundamental",
                "market_state",
                "macro_sector",
                "bull",
                "bear",
                "evidence_audit",
                "portfolio",
            ]
        ), [(row.role, row.status, row.payload.keys()) for row in saved_roles]
        session.add(receipt)
    async with factory() as session:
        original = await session.get(MonitorTrigger, trigger.id)
        assert original.status == "pending" and original.research_run_id is None
        assert original.outbox_id == trigger.outbox_id
    from sqlalchemy import text

    with pytest.raises(DBAPIError, match="MONITOR_FACT_IMMUTABLE"):
        async with factory() as session, session.begin():
            await session.execute(
                text("DELETE FROM research_monitor_daily_trigger_receipts WHERE id=:id"),
                {"id": receipt.id},
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["unresolved", "bear", "evidence_audit", "audit_vote", "adoption", "proposal", "unknown"],
)
async def test_daily_receipt_requires_original_complete_adoption(
    migrated_database_url, monkeypatch, fault
):
    factory, receipt, _trigger = await receipt_source(
        migrated_database_url, monkeypatch, fault=fault
    )
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_RECEIPT_INVALID"):
        async with factory() as session, session.begin():
            session.add(receipt)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_daily_receipt_duplicate_race_and_reverse_assignment_fence(
    migrated_database_url, monkeypatch
):
    import asyncio

    from nexus.persistence.models.monitoring import MonitorDailyTriggerReceipt, MonitorTrigger
    from sqlalchemy import func, select, text

    factory, receipt, trigger = await receipt_source(migrated_database_url, monkeypatch)
    ready = asyncio.Event()
    arrivals = 0

    async def write():
        nonlocal arrivals
        async with factory() as session, session.begin():
            arrivals += 1
            if arrivals == 2:
                ready.set()
            await ready.wait()
            session.add(clone(receipt))

    results = await asyncio.gather(write(), write(), return_exceptions=True)
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, DBAPIError) for result in results) == 1
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(MonitorDailyTriggerReceipt)) == 1
        )
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_TRIGGER_ALREADY_COVERED"):
        async with factory() as session, session.begin():
            row = await session.get(MonitorTrigger, trigger.id)
            row.status, row.research_run_id = "admitted", receipt.run_id
            await session.flush()
            # Check this cross-path fence before the independent 0091 identity
            # fence; no constraint is disabled, no invalid assignment is committed.
            await session.execute(text("SET CONSTRAINTS trg_daily_monitor_exclusive IMMEDIATE"))
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_assignment_wins_receipt_rejects_and_rolls_back(migrated_database_url, monkeypatch):
    from nexus.persistence.models.monitoring import MonitorTrigger

    factory, receipt, trigger = await receipt_source(migrated_database_url, monkeypatch)
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_RECEIPT_INVALID_TRIGGER"):
        async with factory() as session, session.begin():
            row = await session.get(MonitorTrigger, trigger.id)
            row.status, row.research_run_id = "admitted", receipt.run_id
            await session.flush()
            session.add(receipt)
            await session.flush()
    async with factory() as session:
        row = await session.get(MonitorTrigger, trigger.id)
        assert row.status == "pending" and row.research_run_id is None
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_frozen_daily_history_blocks_downgrade_without_any_receipt(
    migrated_database_url, monkeypatch
):
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    factory, _receipt, _trigger = await receipt_source(migrated_database_url, monkeypatch)
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(DBAPIError, match="MONITOR_DAILY_HISTORY_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(command.downgrade, config, "0091_monitor_frozen_members")
    await factory.kw["bind"].dispose()
