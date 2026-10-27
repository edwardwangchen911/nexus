"""Kind separation is enforced in SQL and the normal legacy price entry points.

The disclosure rows here deliberately retain price-shaped JSON. They test that a
kind cannot be relabelled by shape; they are not approved SEC research outputs.
"""

from datetime import timedelta
from uuid import uuid4

import pytest
from integration.test_monitor_increment_pg import ready_trigger
from nexus.api.research_monitoring import read_monitoring_view
from nexus.monitoring.pending import pending_stock
from nexus.monitoring.storage import record_check
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorCoverageReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import Job, OutboxEvent
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


def clone(row, **changes):
    values = {
        c.name: getattr(row, c.name)
        for c in row.__table__.columns
        if c.name not in {"id", "created_at"}
    }
    return type(row)(**{**values, "id": uuid4(), **changes})


async def structural_baseline(session, parent):
    # Only a relational fixture: original bytes and capture canonical are tested
    # separately by the source reader. This cannot qualify a research proposal.
    from nexus.persistence.models.data import DataSnapshot

    original = await session.get(DataSnapshot, parent.data_snapshot_id)
    row = clone(
        original,
        source="sec_disclosure_index",
        schema_version="sec_disclosure_index_v1",
        payload={"index": {"instrument_id": str(parent.instrument_id)}},
        created_at=original.created_at,
    )
    session.add(row)
    await session.flush()
    return row


@pytest.mark.asyncio
async def test_kind_scoped_plans_checks_events_and_legacy_entry_points(
    migrated_database_url, monkeypatch
):
    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    async with factory() as session, session.begin():
        columns = set(
            await session.scalars(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='research_monitor_plans'"
                )
            )
        )
        assert "kind" in columns, "two monitor kinds need isolated durable identities"
        price = await session.scalar(
            select(MonitorPlan).where(MonitorPlan.source_run_id == parent.id)
        )
        baseline = await structural_baseline(session, parent)
        disclosure = clone(
            price,
            kind="disclosure",
            baseline_snapshot_id=baseline.id,
            baseline_as_of=parent.decision_time,
        )
        session.add(disclosure)
        await session.flush()
        assert price.generation == disclosure.generation and price.kind == "price"
        session.add(
            MonitorCoverageReceipt(
                account_id=price.account_id,
                instrument_id=price.instrument_id,
                run_id=parent.id,
                output_id=price.source_output_id,
                prior_plan_id=None,
                active_plan_id=disclosure.id,
                research_as_of=parent.decision_time,
                observed_at=now,
                kind="disclosure",
                status="installed",
                reason="synthetic_kind_test",
                quality_status="complete",
            )
        )
        await session.flush()
        view = await read_monitoring_view(session, parent, now=now)
        assert view.plan_id == price.id and view.state == "valid"
        with pytest.raises(ValueError, match="MONITOR_KIND_UNSUPPORTED"):
            await record_check(
                session,
                plan_id=disclosure.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
                checked_at=now,
                observation=None,
            )
        original_check = await session.get(MonitorCheck, trigger.check_id)
        key = "sec:0000000001:0000000001-26-000001"
        checked = clone(original_check, plan_id=disclosure.id, kind="disclosure", event_key=key)
        second = clone(
            original_check,
            plan_id=disclosure.id,
            kind="disclosure",
            event_key="sec:0000000001:0000000001-26-000002",
        )
        session.add_all((checked, second))
        await session.flush()
        source_outbox = await session.get(OutboxEvent, trigger.outbox_id)
        outbox = clone(source_outbox)
        session.add(outbox)
        await session.flush()
        event = clone(
            trigger,
            check_id=checked.id,
            kind="disclosure",
            event_key=key,
            episode=None,
            cause="disclosure_added",
            outbox_id=outbox.id,
        )
        session.add(event)
        await session.flush()
        assert event.batch_id is None and event.research_run_id is None
        from nexus.audit.service import append_audit_event
        from nexus.ops.monitor_dispatch import DISPATCH_ACTION, unchecked

        await append_audit_event(
            session,
            actor_id="research-monitor",
            action=DISPATCH_ACTION,
            schema_version="monitor_dispatch_check_v1",
            occurred_at=now,
            payload={"trigger_id": str(event.id)},
        )
        eligible_price = await session.scalar(
            select(MonitorTrigger.id).where(MonitorTrigger.id == trigger.id, unchecked(now))
        )
        assert eligible_price == trigger.id, "a SEC eligibility audit cannot suppress price-v1"
        # SEC members are neither silently consumed nor offered to price-v1 jobs.
        pending = await pending_stock(
            session, account_id=price.account_id, instrument_id=price.instrument_id, now=now
        )
        assert [item.id for item, _ in pending] == [trigger.id]
        view = await read_monitoring_view(session, parent, now=now)
        assert view.trigger_count == 1 and view.triggers[0].trigger_id == trigger.id
        # Same accession on another capture/bucket remains globally the same event.
        later = clone(
            checked,
            scheduled_at=checked.scheduled_at + timedelta(minutes=5),
            checked_at=checked.checked_at + timedelta(minutes=5),
        )
        session.add(later)
        await session.flush()
        extra_outbox = clone(source_outbox)
        session.add(extra_outbox)
        await session.flush()
        original_output = await session.get(AgentResearchOutputRecord, price.source_output_id)
        extra_output = clone(original_output, sequence=10000)
        price_null_check = clone(
            original_check,
            scheduled_at=checked.scheduled_at + timedelta(minutes=10),
            checked_at=checked.checked_at + timedelta(minutes=10),
        )
        session.add_all((extra_output, price_null_check))
        await session.flush()
        invalid = (
            (
                clone(event, check_id=later.id, outbox_id=extra_outbox.id),
                "uq_monitor_disclosure_event",
            ),
            (
                clone(
                    original_check,
                    kind="price",
                    plan_id=disclosure.id,
                    scheduled_at=checked.scheduled_at + timedelta(minutes=15),
                    checked_at=checked.checked_at + timedelta(minutes=15),
                ),
                "fk_monitor_check_plan_kind",
            ),
            (
                clone(
                    price,
                    generation=price.generation + 1,
                    predecessor_id=disclosure.id,
                    source_output_id=extra_output.id,
                ),
                "fk_monitor_plan_predecessor_kind",
            ),
            (
                clone(
                    disclosure, generation=2, source_output_id=extra_output.id, baseline_as_of=None
                ),
                "ck_research_monitor_plans_baseline_kind",
            ),
            (
                clone(
                    trigger, episode=None, check_id=price_null_check.id, outbox_id=extra_outbox.id
                ),
                "ck_research_monitor_triggers_kind_episode",
            ),
        )
        invalid += (
            (
                clone(
                    disclosure,
                    generation=2,
                    source_output_id=extra_output.id,
                    baseline_as_of=parent.decision_time + timedelta(seconds=1),
                ),
                "MONITOR_DISCLOSURE_BASELINE_INVALID",
            ),
        )
        for row, expected_constraint in invalid:
            with pytest.raises(DBAPIError, match=expected_constraint):
                async with session.begin_nested():
                    session.add(row)
                    await session.flush()
        scope = (price.account_id, event.id)

    async def facts():
        async with factory() as session:
            return [
                await session.scalar(select(func.count()).select_from(model))
                for model in (
                    AgentResearchRunRecord,
                    AgentDailyBudgetReservationRecord,
                    Job,
                    MonitorPlan,
                    MonitorTrigger,
                )
            ]

    before = await facts()
    with pytest.raises(ValueError, match="MONITOR_TRIGGER_UNAVAILABLE"):
        await run(
            settings,
            parser().parse_args(
                [
                    "admit",
                    "--trigger-id",
                    str(scope[1]),
                    "--calendar-authority-id",
                    "50000000-0000-7000-8000-000000000001",
                ]
            ),
        )
    # Finish also serves typed SEC batches, but this unassigned legacy-shaped
    # event has no original increment to finish.
    with pytest.raises(ValueError, match="MONITOR_INCREMENT_UNAVAILABLE"):
        await run(settings, parser().parse_args(["finish", "--trigger-id", str(scope[1])]))
    assert await facts() == before
    await factory.kw["bind"].dispose()
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(Exception, match="MONITOR_KIND_HISTORY_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(command.downgrade, config, "0092_monitor_daily_receipts")
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == "0098_independent_monitor"
        )
        assert (await session.get(MonitorTrigger, scope[1])).kind == "disclosure"
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_disclosure_shape_cannot_enter_legacy_daily_sql_receipts(
    migrated_database_url, monkeypatch
):
    import copy
    import hashlib
    import json

    from integration.test_monitor_daily_receipts_pg import receipt_source
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import MonitorDailyTriggerReceipt
    from sqlalchemy.exc import DBAPIError

    factory, receipt, original = await receipt_source(migrated_database_url, monkeypatch)
    async with factory() as session, session.begin():
        checked = await session.get(MonitorCheck, original.check_id)
        plan = await session.get(MonitorPlan, checked.plan_id)
        parent = await session.get(AgentResearchRunRecord, plan.source_run_id)
        baseline = await structural_baseline(session, parent)
        disclosure = clone(
            plan,
            kind="disclosure",
            baseline_snapshot_id=baseline.id,
            baseline_as_of=parent.decision_time,
        )
        session.add(disclosure)
        await session.flush()
        key = "sec:0000000001:0000000001-26-000001"
        sec_check = clone(checked, kind="disclosure", plan_id=disclosure.id, event_key=key)
        outbox = clone(await session.get(OutboxEvent, original.outbox_id))
        session.add_all((sec_check, outbox))
        await session.flush()
        sec_trigger = clone(
            original,
            check_id=sec_check.id,
            kind="disclosure",
            event_key=key,
            episode=None,
            cause="disclosure_added",
            outbox_id=outbox.id,
        )
        session.add(sec_trigger)
        await session.flush()
        # Both controls matter: exact price ancestry works; SEC-shaped price JSON
        # is forbidden even when asking whether a plan is its own ancestor.
        assert (
            await session.scalar(
                text("SELECT nexus_monitor_same_condition_ancestor(:p,:p)"), {"p": plan.id}
            )
            is True
        )
        assert (
            await session.scalar(
                text("SELECT nexus_monitor_same_condition_ancestor(:p,:p)"), {"p": disclosure.id}
            )
            is False
        )
        snapshot = await session.get(DataSnapshot, receipt.snapshot_id)
        payload = copy.deepcopy(snapshot.payload)
        pending = payload["agent_paper_context"]["pending_monitoring"]
        pending["parents"][0]["plan_id"] = str(disclosure.id)
        item = pending["trigger_checks"][0]
        item.update(
            plan_id=str(disclosure.id),
            check_id=str(sec_check.id),
            trigger_id=str(sec_trigger.id),
            cause=sec_trigger.cause,
        )
        invalid_snapshot = clone(
            snapshot,
            payload=payload,
            revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        )
        with pytest.raises(DBAPIError, match="MONITOR_DAILY_CONTEXT_INVALID"):
            async with session.begin_nested():
                session.add(invalid_snapshot)
                await session.flush()
                await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        with pytest.raises(DBAPIError, match="MONITOR_DAILY_RECEIPT_INVALID_TRIGGER"):
            async with session.begin_nested():
                session.add(clone(receipt, trigger_id=sec_trigger.id))
                await session.flush()
        assert (
            await session.scalar(select(func.count()).select_from(MonitorDailyTriggerReceipt)) == 0
        )
    await factory.kw["bind"].dispose()
