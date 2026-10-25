"""Real cross-table receipt exclusion under the normal READ COMMITTED factory.

Only the winning receipt is qualified. The rival SQL insert deliberately cannot
claim a second business authorization; it probes the shared defensive DB guard.
"""

import asyncio
from time import monotonic
from uuid import UUID, uuid4

import pytest
from integration.test_monitor_daily_events_pg import add_pair, v2_receipt_source
from integration.test_monitor_daily_receipts_pg import clone, receipt_source
from nexus.audit.service import append_audit_event
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorDailyTriggerReceipt,
    MonitorIncrement,
    MonitorMixedEventReceipt,
    MonitorTrigger,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

MODELS = {
    "legacy": MonitorDailyTriggerReceipt,
    "mixed": MonitorMixedEventReceipt,
    "daily_v2": MonitorDailyEventReceipt,
}


async def winner_source(url, monkeypatch, tmp_path, kind):
    if kind == "legacy":
        factory, receipt, trigger = await receipt_source(url, monkeypatch)

        async def prepare(session):
            session.add(receipt)

    elif kind == "daily_v2":
        factory, receipt, trigger, now, prior = await v2_receipt_source(url, monkeypatch)

        async def prepare(session):
            await add_pair(session, receipt, now, prior)
            session.add(receipt)

    else:
        from integration import test_monitor_event_receipts_pg as mixed

        monkeypatch.setattr(mixed, "HEAD", "0097_monitor_daily_events")
        factory, member, context, proposal_hash, review_hash, now = await mixed.native_result(
            url, monkeypatch, tmp_path, "valid"
        )
        async with factory() as session:
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == member.run_id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            trigger = await session.get(
                MonitorTrigger, UUID(context["trigger_checks"][0]["trigger_id"])
            )
        receipts = [
            MonitorMixedEventReceipt(
                account_id=member.account_id,
                instrument_id=member.instrument_id,
                batch_id=member.batch_id,
                trigger_id=UUID(item["trigger_id"]),
                run_id=member.run_id,
                output_id=output.id,
                materials_snapshot_id=UUID(context["materials_snapshot_id"]),
                context_hash=member.context_hash,
                proposal_hash=proposal_hash,
                review_hash=review_hash,
                observed_at=now,
            )
            for item in context["trigger_checks"]
        ]
        receipt = receipts[0]

        async def prepare(session):
            await session.execute(
                update(MonitorTrigger)
                .where(MonitorTrigger.batch_id == member.batch_id)
                .values(status="completed")
            )
            for coverage_kind in ("price", "disclosure"):
                session.add(
                    MonitorCoverageReceipt(
                        account_id=member.account_id,
                        instrument_id=member.instrument_id,
                        run_id=member.run_id,
                        output_id=output.id,
                        kind=coverage_kind,
                        prior_plan_id=None,
                        active_plan_id=None,
                        research_as_of=member.research_as_of,
                        observed_at=now,
                        status="gap",
                        reason="MONITOR_NO_NEW_PLAN",
                        quality_status=None,
                    )
                )
            session.add_all(receipts)

    return factory, receipt, trigger, prepare


async def rival_receipt(session, winner, trigger, kind):
    model = MODELS[kind]
    if isinstance(winner, model):
        return clone(winner)
    run = await session.get(AgentResearchRunRecord, winner.run_id)
    cycle_id = await session.scalar(select(AgentPaperCycleRecord.id).limit(1))
    values = {
        "account_id": winner.account_id,
        "instrument_id": winner.instrument_id,
        "trigger_id": trigger.id,
        "cycle_id": getattr(winner, "cycle_id", cycle_id),
        "run_id": winner.run_id,
        "output_id": winner.output_id,
        "snapshot_id": getattr(winner, "snapshot_id", run.data_snapshot_id),
        "context_hash": winner.context_hash,
        "review_bundle_hash": getattr(winner, "review_bundle_hash", "1" * 64),
        "proposal_hash": getattr(winner, "proposal_hash", "2" * 64),
        "review_hash": getattr(winner, "review_hash", "3" * 64),
        "observed_at": winner.observed_at,
        "batch_id": getattr(winner, "batch_id", None),
        "materials_snapshot_id": getattr(winner, "materials_snapshot_id", None),
        "kind": trigger.kind,
        "event_key": trigger.event_key,
        "original_run_id": trigger.research_run_id,
        "original_output_id": None,
    }
    # The rival intentionally remains unqualified. The legacy identity guard
    # may refuse a typed trigger before the shared exclusive guard runs.
    if kind == "mixed" and values["batch_id"] is None:
        values["batch_id"] = uuid4()
        values["materials_snapshot_id"] = run.data_snapshot_id
    return model(
        **{
            column.key: values[column.key]
            for column in model.__table__.columns
            if column.key in values
        }
    )


async def usage_facts(session):
    queries = (
        select(
            AgentDailyBudgetReservationRecord.id,
            AgentDailyBudgetReservationRecord.state,
            AgentDailyBudgetReservationRecord.reserved_usd,
            AgentDailyBudgetReservationRecord.settled_usd,
        ).order_by(AgentDailyBudgetReservationRecord.id),
        select(
            AgentResearchRunRecord.id,
            AgentResearchRunRecord.input_tokens,
            AgentResearchRunRecord.output_tokens,
            AgentResearchRunRecord.settled_cost_usd,
        ).order_by(AgentResearchRunRecord.id),
        select(
            MonitorIncrement.id,
            MonitorIncrement.reserved_sdk_calls,
            MonitorIncrement.daily_sdk_call_limit,
        ).order_by(MonitorIncrement.id),
    )
    return [tuple((await session.execute(query)).all()) for query in queries]


@pytest.mark.asyncio
@pytest.mark.parametrize("winning", MODELS)
@pytest.mark.parametrize("losing", MODELS)
async def test_receipt_competition_has_one_committed_proof_and_rolls_back_rival(
    migrated_database_url, monkeypatch, tmp_path, winning, losing
):
    factory, winner, trigger, prepare = await winner_source(
        migrated_database_url, monkeypatch, tmp_path, winning
    )
    started = asyncio.Event()
    state = {}
    action = "monitor.test.uncommitted_receipt_rival"
    task = None
    locker = factory()
    transaction = None
    try:
        async with factory() as session:
            before_usage = await usage_facts(session)
            rival = await rival_receipt(session, winner, trigger, losing)
        await locker.__aenter__()
        transaction = await locker.begin()
        assert await locker.scalar(text("SHOW transaction_isolation")) == "read committed"
        holder = await locker.scalar(text("SELECT pg_backend_pid()"))
        await prepare(locker)
        await locker.flush()
        # The winner must satisfy the entire native DB contract before competing.
        await locker.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

        async def insert_rival():
            async with factory() as session, session.begin():
                assert await session.scalar(text("SHOW transaction_isolation")) == "read committed"
                state["pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
                await append_audit_event(
                    session,
                    actor_id="monitor-receipt-test",
                    action=action,
                    schema_version="synthetic_receipt_competition_v1",
                    payload={"winning": winning, "losing": losing},
                    occurred_at=winner.observed_at,
                )
                session.add(rival)
                started.set()
                await session.flush()
                await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

        task = asyncio.create_task(insert_rival())
        await asyncio.wait_for(started.wait(), timeout=30)
        deadline = monotonic() + 30
        async with factory() as observer:
            while True:
                blockers = await observer.scalar(
                    text("SELECT pg_blocking_pids(:pid)"), {"pid": state["pid"]}
                )
                if holder in blockers:
                    break
                assert not task.done(), "rival must wait for the winning trigger ownership lock"
                assert monotonic() < deadline, "database did not expose the shared trigger lock"
                await asyncio.sleep(0.05)
        await transaction.commit()
        transaction = None
        with pytest.raises(DBAPIError) as rejected:
            await asyncio.wait_for(task, timeout=30)
        if winning == losing:
            assert getattr(rejected.value.orig, "sqlstate", None) == "23505"
            assert "trigger_id" in str(rejected.value)
        elif losing == "legacy":
            # Its earlier BEFORE guard already owns the same trigger lock and
            # excludes typed batches before the newer cross-table guard runs.
            assert "MONITOR_DAILY_RECEIPT_INVALID_TRIGGER" in str(rejected.value)
        else:
            assert "MONITOR_EVENT_ALREADY_CONSUMED" in str(rejected.value)
        async with factory() as session:
            for kind, model in MODELS.items():
                rows = tuple(
                    await session.scalars(select(model).where(model.trigger_id == trigger.id))
                )
                assert len(rows) == int(kind == winning)
                if rows:
                    assert rows[0].id == winner.id
            assert (
                await session.scalar(
                    select(func.count()).select_from(AuditEvent).where(AuditEvent.action == action)
                )
                == 0
            )
            after_usage = await usage_facts(session)
            assert after_usage == before_usage
    finally:
        if transaction is not None:
            await transaction.rollback()
        await locker.__aexit__(None, None, None)
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await factory.kw["bind"].dispose()
