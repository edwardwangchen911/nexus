"""Database fences the exact immutable group, independently of Python readers."""

from datetime import timedelta
from uuid import uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import ready_trigger
from integration.test_monitor_preclaim_pg import admitted
from nexus.persistence.models.monitoring import MonitorCheck, MonitorTrigger
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


async def extra_trigger(session, original):
    checked = await session.get(MonitorCheck, original.check_id)
    check = MonitorCheck(
        account_id=original.account_id,
        instrument_id=original.instrument_id,
        plan_id=checked.plan_id,
        scheduled_at=checked.scheduled_at + timedelta(microseconds=1),
        checked_at=checked.checked_at,
        result="true",
        observation=checked.observation,
    )
    session.add(check)
    event_id, trigger_id = uuid4(), uuid4()
    session.add(
        OutboxEvent(
            id=event_id,
            aggregate_id=trigger_id,
            event_type="research.monitor.triggered",
            schema_version="research_price_trigger_v1",
            payload={"trigger_id": str(trigger_id)},
            available_at=checked.checked_at,
        )
    )
    await session.flush()
    trigger = MonitorTrigger(
        id=trigger_id,
        account_id=original.account_id,
        instrument_id=original.instrument_id,
        condition_hash=original.condition_hash,
        episode=original.episode + 1,
        check_id=check.id,
        cause=original.cause,
        status="pending",
        outbox_id=event_id,
    )
    session.add(trigger)
    await session.flush()
    return trigger, check


@pytest.mark.asyncio
async def test_unfrozen_extra_trigger_cannot_attach_to_original_run(
    migrated_database_url, monkeypatch
):
    factory, _settings, trigger, run_id, _job_id, _now = await admitted(
        migrated_database_url, monkeypatch
    )
    async with factory() as session, session.begin():
        extra, _ = await extra_trigger(session, trigger)
        extra_id = extra.id
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_INVALID"):
        async with factory() as session, session.begin():
            extra = await session.get(MonitorTrigger, extra_id)
            extra.research_run_id, extra.status = run_id, "admitted"
    await factory.kw["bind"].dispose()


async def prepare_group(database_url, monkeypatch):
    from nexus.monitoring.context import MonitorResearchContext, TriggerCheck
    from nexus.monitoring.contracts import PriceObservation

    factory, settings, anchor, _parent, now = await ready_trigger(database_url, monkeypatch)
    async with factory() as session, session.begin():
        extra, check = await extra_trigger(session, anchor)
        item = TriggerCheck(
            trigger_id=extra.id,
            check_id=check.id,
            cause=extra.cause,
            checked_at=check.checked_at,
            observation=PriceObservation.model_validate(check.observation),
        )
    original_init = MonitorResearchContext.__init__

    def expanded(self, **data):
        if len(data.get("trigger_checks", ())) == 1:
            data["trigger_checks"] = (*data["trigger_checks"], item)
        original_init(self, **data)

    monkeypatch.setattr(MonitorResearchContext, "__init__", expanded)
    return factory, settings, anchor, extra, now


async def admit_group(session, settings, anchor, now):
    from nexus.monitoring.admission import admit_trigger

    return await admit_trigger(
        session,
        account_id=anchor.account_id,
        trigger_id=anchor.id,
        candidate_batch_id=settings.agent_paper_candidate_batch_id,
        calendar_authority_id=CALENDAR_ID,
        model_config_id=settings.agent_paper_formal_model_config_id,
        daily_limit_usd=settings.agent_daily_budget_usd,
        daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
        now=now,
    )


@pytest.mark.asyncio
async def test_missing_group_member_rejected_at_commit(migrated_database_url, monkeypatch):
    factory, settings, anchor, _extra, now = await prepare_group(migrated_database_url, monkeypatch)
    from nexus.monitoring import admission, pending
    from nexus.persistence.models.monitoring import MonitorIncrement

    native_pending = pending.pending_stock

    async def omitted_member(*args, **kwargs):
        return tuple(
            pair for pair in await native_pending(*args, **kwargs) if pair[0].id == anchor.id
        )

    async def bypass_python_binding(session, run):
        return await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == run.id)
        )

    # Deliberately broken producer: the frozen context has two members, but the
    # producer associates only one. Bypass the separate Python reader so the
    # independent database commit fence, not application validation, is exercised.
    monkeypatch.setattr(pending, "pending_stock", omitted_member)
    monkeypatch.setattr(admission, "require_increment_binding", bypass_python_binding)
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_INVALID"):
        async with factory() as session, session.begin():
            await admit_group(session, settings, anchor, now)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_group_status_is_atomic_but_each_row_may_flush(migrated_database_url, monkeypatch):
    factory, settings, anchor, extra, now = await prepare_group(migrated_database_url, monkeypatch)
    async with factory() as session, session.begin():
        member = await admit_group(session, settings, anchor, now)
        other = await session.get(MonitorTrigger, extra.id)
        other.research_run_id, other.status = member.run_id, "admitted"
        await session.flush()
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_INVALID"):
        async with factory() as session, session.begin():
            row = await session.get(MonitorTrigger, anchor.id)
            row.status, row.reason = "unknown", "MONITOR_UNKNOWN_USAGE"
    async with factory() as session, session.begin():
        for trigger_id in (anchor.id, extra.id):
            row = await session.get(MonitorTrigger, trigger_id)
            row.status, row.reason = "unknown", "MONITOR_UNKNOWN_USAGE"
            await session.flush()
        await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    async with factory() as session:
        statuses = tuple(
            await session.scalars(
                select(MonitorTrigger.status).where(MonitorTrigger.research_run_id == member.run_id)
            )
        )
        assert statuses == ("unknown", "unknown")
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(command.downgrade, config, "0090_monitor_coverage")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["empty", "duplicate", "overflow", "cause", "observation", "scope"]
)
async def test_snapshot_rejects_malformed_or_foreign_members(
    migrated_database_url, monkeypatch, fault
):
    import copy
    import hashlib
    import json

    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.data import DataSnapshot

    factory, _settings, _anchor, run_id, _job, _now = await admitted(
        migrated_database_url, monkeypatch
    )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        source = await session.get(DataSnapshot, run.data_snapshot_id)
    payload = copy.deepcopy(source.payload)
    context = payload["monitor_context"]
    items = context["trigger_checks"]
    if fault == "empty":
        context["trigger_checks"] = []
    elif fault == "duplicate":
        items.append(copy.deepcopy(items[0]))
    elif fault == "overflow":
        context["trigger_checks"] = [copy.deepcopy(items[0]) for _ in range(33)]
    elif fault == "cause":
        items[0]["cause"] = "crossed" if items[0]["cause"] == "initial_match" else "initial_match"
    elif fault == "observation":
        items[0]["observation"]["content_hash"] = "d" * 64
    else:
        context["account_id"] = str(uuid4())
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_INVALID"):
        async with factory() as session, session.begin():
            session.add(
                DataSnapshot(
                    source=source.source,
                    provider_object_id=source.provider_object_id,
                    revision=hashlib.sha256(
                        json.dumps(payload, sort_keys=True).encode()
                    ).hexdigest(),
                    observed_at=source.observed_at,
                    available_at=source.available_at,
                    received_at=source.received_at,
                    schema_version=source.schema_version,
                    payload=payload,
                )
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "generation", "condition", "intermediate_condition"])
async def test_context_ownership_accepts_only_same_condition_contiguous_ancestor(
    migrated_database_url, monkeypatch, fault
):
    from nexus.monitoring.admission import _freeze_increment_snapshot
    from nexus.monitoring.context import MonitorResearchContext
    from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import MonitorPlan

    factory, _settings, _anchor, run_id, _job, _now = await admitted(
        migrated_database_url, monkeypatch
    )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        frozen = await session.get(DataSnapshot, run.data_snapshot_id)
        context = MonitorResearchContext.model_validate(frozen.payload["monitor_context"])
        prior = await session.get(MonitorPlan, context.plan_id)
        source = await session.get(AgentResearchOutputRecord, context.parent_output_id)
    async with factory() as session, session.begin():
        # Structural DB fixture: append another original-shaped output, not a newly
        # qualified research result; these tests grant no application install authority.
        output = AgentResearchOutputRecord(
            run_id=source.run_id,
            sequence=100,
            status=source.status,
            role=source.role,
            content_hash=source.content_hash,
            payload=source.payload,
            intent=source.intent,
        )
        session.add(output)
        await session.flush()
        plan = MonitorPlan(
            account_id=prior.account_id,
            instrument_id=prior.instrument_id,
            generation=3 if fault == "generation" else 2,
            source_run_id=prior.source_run_id,
            source_output_id=output.id,
            data_snapshot_id=prior.data_snapshot_id,
            predecessor_id=prior.id,
            condition_hash="d" * 64 if fault == "condition" else prior.condition_hash,
            condition=prior.condition,
            source_hash=output.content_hash,
            effective_at=prior.effective_at,
            review_due_at=prior.review_due_at,
            valid_until=prior.valid_until,
        )
        if fault == "intermediate_condition":
            middle_output = AgentResearchOutputRecord(
                run_id=source.run_id,
                sequence=101,
                status=source.status,
                role=source.role,
                content_hash=source.content_hash,
                payload=source.payload,
                intent=source.intent,
            )
            session.add(middle_output)
            await session.flush()
            middle = MonitorPlan(
                account_id=prior.account_id,
                instrument_id=prior.instrument_id,
                generation=2,
                source_run_id=prior.source_run_id,
                source_output_id=middle_output.id,
                data_snapshot_id=prior.data_snapshot_id,
                predecessor_id=prior.id,
                condition_hash="d" * 64,
                condition={**prior.condition, "threshold": "200000"},
                source_hash=middle_output.content_hash,
                effective_at=prior.effective_at,
                review_due_at=prior.review_due_at,
                valid_until=prior.valid_until,
            )
            session.add(middle)
            await session.flush()
            plan.generation, plan.predecessor_id = 3, middle.id
        session.add(plan)
        await session.flush()
        inherited = context.model_copy(
            update={
                "plan_id": plan.id,
                "plan_generation": plan.generation,
                "parent_output_id": output.id,
                "parent_output_hash": output.content_hash,
            }
        )
    if fault is None:
        async with factory() as session, session.begin():
            await _freeze_increment_snapshot(session, inherited)
            assert await session.scalar(
                text("SELECT public.nexus_monitor_same_condition_ancestor(:source,:current)"),
                {"source": prior.id, "current": plan.id},
            )
    else:
        with pytest.raises(DBAPIError):
            async with factory() as session, session.begin():
                await _freeze_increment_snapshot(session, inherited)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_frozen_snapshot_cannot_be_reused_by_second_run(migrated_database_url, monkeypatch):
    from nexus.persistence.models.agents import AgentResearchRunRecord

    factory, _settings, _anchor, run_id, _job, _now = await admitted(
        migrated_database_url, monkeypatch
    )
    with pytest.raises(DBAPIError, match="MONITOR_FROZEN_MEMBERS_INVALID"):
        async with factory() as session, session.begin():
            original = await session.get(AgentResearchRunRecord, run_id)
            fields = {
                column.name: getattr(original, column.name)
                for column in original.__table__.columns
                if column.name not in {"id", "created_at", "idempotency_key"}
            }
            session.add(
                AgentResearchRunRecord(
                    **fields, id=uuid4(), idempotency_key="forbidden-second-monitor-run"
                )
            )
    await factory.kw["bind"].dispose()
