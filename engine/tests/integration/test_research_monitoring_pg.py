"""Real PG episode concurrency and trigger durability; no model or live services."""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.paper_batch import seed_paper_batch
from integration.test_agent_persistence_pg import (
    _INSTRUMENT_ID,
    _NOW,
    _RUN_IDS,
    _SNAPSHOT_ID,
    _seed_authority,
    _seed_runs,
)
from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
from nexus.monitoring.storage import record_check
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorEpisode,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def seed(database_url):
    account = await seed_paper_batch(database_url)
    factory = session_factory(EngineSettings(database_url=database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, (_RUN_IDS[0],))
    condition = PriceReviewCondition(
        signal="schwab.l1.bid_ask_midpoint",
        operator="gte",
        threshold="100",
        unit="currency",
        currency="USD",
        purpose="review",
        source="schwab",
        feed="l1",
        price_basis="unadjusted_bid_ask_midpoint",
        max_age_seconds=60,
    )
    async with factory() as session, session.begin():
        output = AgentResearchOutputRecord(
            run_id=_RUN_IDS[0],
            sequence=1,
            status="completed",
            content_hash="a" * 64,
            payload={},
            intent={},
        )
        session.add(output)
        await session.flush()
        plan = MonitorPlan(
            account_id=account.account_id,
            instrument_id=_INSTRUMENT_ID,
            generation=1,
            source_run_id=_RUN_IDS[0],
            source_output_id=output.id,
            data_snapshot_id=_SNAPSHOT_ID,
            predecessor_id=None,
            condition_hash=condition.semantic_hash,
            condition=condition.model_dump(mode="json"),
            source_hash=output.content_hash,
            effective_at=_NOW,
            review_due_at=_NOW + timedelta(hours=1),
            valid_until=_NOW + timedelta(hours=2),
        )
        session.add(plan)
        await session.flush()
    return factory, plan


def quote(plan, now, matched=True):
    # The DB-authority loader is tested separately; this boundary receives its exact projection.
    from decimal import Decimal

    return PriceObservation(
        instrument_id=plan.instrument_id,
        quote_id=UUID(int=9),
        artifact_id=UUID(int=10),
        content_hash="b" * 64,
        source="schwab",
        feed="l1",
        currency="USD",
        price_basis="unadjusted_bid_ask_midpoint",
        delivery_mode="real_time",
        evidence_status="verified",
        bid=Decimal("100" if matched else "98"),
        ask=Decimal("100"),
        observed_at=now,
        available_at=now,
        received_at=now,
    )


@pytest.mark.asyncio
async def test_concurrent_ticks_persist_one_episode_notification_and_pending_trigger(
    migrated_database_url,
):
    factory, plan = await seed(migrated_database_url)

    async def tick(now, observation):
        async with factory() as session, session.begin():
            return await record_check(
                session, plan_id=plan.id, scheduled_at=now, checked_at=now, observation=observation
            )

    await asyncio.gather(*(tick(_NOW, quote(plan, _NOW)) for _ in range(3)))
    for minutes, observed in ((5, None), (10, True), (15, False), (20, True)):
        now = _NOW + timedelta(minutes=minutes)
        await tick(now, quote(plan, now, observed) if observed is not None else None)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorCheck)) == 5
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 2
        assert (
            await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.event_type == "research.monitor.triggered")
            )
            == 2
        )
        triggers = (await session.scalars(select(MonitorTrigger))).all()
        assert all(t.status == "pending" and t.research_run_id is None for t in triggers)
        state = await session.scalar(select(MonitorEpisode))
        assert state.sequence == 2 and state.latched


@pytest.mark.asyncio
async def test_no_match_and_unknown_do_not_schedule_or_notify(migrated_database_url):
    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW, False),
        )
        later = _NOW + timedelta(minutes=5)
        await record_check(
            session, plan_id=plan.id, scheduled_at=later, checked_at=later, observation=None
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
        assert (
            await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.event_type == "research.monitor.triggered")
            )
            == 0
        )


@pytest.mark.asyncio
async def test_budget_rejection_preserves_trigger_notification_and_audit(migrated_database_url):
    from nexus.monitoring.storage import mark_trigger_blocked
    from nexus.persistence.models.runtime import AuditEvent

    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW),
        )
    # Capture commits before admission; exhausted budget cannot erase the trigger.
    async with factory() as session, session.begin():
        trigger = await session.scalar(select(MonitorTrigger))
        await mark_trigger_blocked(
            session, trigger_id=trigger.id, reason="MONITOR_DAILY_LIMIT_REACHED", now=_NOW
        )
    async with factory() as session:
        trigger = await session.scalar(select(MonitorTrigger))
        assert trigger.status == "blocked" and trigger.research_run_id is None
        assert trigger.reason == "MONITOR_DAILY_LIMIT_REACHED"
        assert await session.get(OutboxEvent, trigger.outbox_id) is not None
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "research.monitor.admission_blocked")
            )
            == 1
        )


@pytest.mark.asyncio
async def test_plan_replacement_preserves_latch_and_fences_old_writer(migrated_database_url):
    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW),
        )
        new_output = AgentResearchOutputRecord(
            run_id=plan.source_run_id,
            sequence=2,
            status="completed",
            content_hash="c" * 64,
            payload={},
            intent={},
        )
        session.add(new_output)
        await session.flush()
        new_plan = MonitorPlan(
            account_id=plan.account_id,
            instrument_id=plan.instrument_id,
            generation=2,
            source_run_id=plan.source_run_id,
            source_output_id=new_output.id,
            data_snapshot_id=plan.data_snapshot_id,
            predecessor_id=plan.id,
            condition_hash=plan.condition_hash,
            condition=plan.condition,
            source_hash=new_output.content_hash,
            effective_at=_NOW + timedelta(minutes=1),
            review_due_at=plan.review_due_at,
            valid_until=plan.valid_until,
        )
        session.add(new_plan)
    later = _NOW + timedelta(minutes=5)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MONITOR_PLAN_NOT_CURRENT"):
            await record_check(
                session,
                plan_id=plan.id,
                scheduled_at=later,
                checked_at=later,
                observation=quote(plan, later),
            )
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=new_plan.id,
            scheduled_at=later,
            checked_at=later,
            observation=quote(plan, later),
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("same_time_conflict", [False, True])
async def test_price_loader_uses_exact_schwab_quote_not_conflicting_trade(
    migrated_database_url, same_time_conflict
):
    from decimal import Decimal

    from nexus.monitoring.observations import read_latest_price
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote

    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        artifact = DataArtifact(
            uri="fixture:schwab-transcript",
            sha256="d" * 64,
            byte_size=1,
            mime_type="application/json",
            schema_version="schwab_level_one_stream_v1",
            source="schwab",
            observed_at=_NOW,
            available_at=_NOW,
            received_at=_NOW,
            created_at=_NOW,
        )
        session.add(artifact)
        await session.flush()
        for kind, bid, ask, last in (("quote", "98", "100", None), ("trade", None, None, "200")):
            session.add(
                LatestQuote(
                    instrument_id=plan.instrument_id,
                    source="schwab",
                    feed="l1",
                    delivery_mode="real_time",
                    event_kind=kind,
                    evidence_status="verified",
                    data_artifact_id=artifact.id,
                    bid=None if bid is None else Decimal(bid),
                    ask=None if ask is None else Decimal(ask),
                    last=None if last is None else Decimal(last),
                    observed_at=_NOW,
                    available_at=_NOW,
                    received_at=_NOW,
                    created_at=_NOW,
                    revision=kind,
                    quality_flags=[],
                    content_hash=("e" if kind == "quote" else "f") * 64,
                )
            )
        if same_time_conflict:
            for offset in (1, 2):
                session.add(
                    LatestQuote(
                        id=UUID(int=(1 << 128) - offset),
                        instrument_id=plan.instrument_id,
                        source="schwab",
                        feed="l1",
                        delivery_mode="real_time",
                        event_kind="quote",
                        evidence_status="verified",
                        data_artifact_id=artifact.id,
                        bid=Decimal("200"),
                        ask=Decimal("200"),
                        last=None,
                        observed_at=_NOW,
                        available_at=_NOW,
                        received_at=_NOW,
                        created_at=_NOW,
                        revision=f"conflict-{offset}",
                        quality_flags=[],
                        content_hash=str(offset) * 64,
                    )
                )
    async with factory() as session, session.begin():
        observed = await read_latest_price(session, instrument_id=plan.instrument_id, now=_NOW)
        if same_time_conflict:
            assert observed is None
        else:
            assert observed is not None and observed.bid == Decimal("98")
        check = await record_check(
            session, plan_id=plan.id, scheduled_at=_NOW, checked_at=_NOW, observation=observed
        )
        assert check.result == ("unknown" if same_time_conflict else "false")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["rewind", "delete", "skip"])
async def test_episode_history_cannot_be_erased_or_rewound(migrated_database_url, mutation):
    from sqlalchemy import delete, update
    from sqlalchemy.exc import DBAPIError

    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW),
        )
    statement = (
        delete(MonitorEpisode)
        if mutation == "delete"
        else update(MonitorEpisode).values(sequence=0 if mutation == "rewind" else 3)
    )
    with pytest.raises(DBAPIError, match="MONITOR_EPISODE_TRANSITION_INVALID"):
        async with factory() as session, session.begin():
            await session.execute(statement)
    async with factory() as session:
        episode = await session.scalar(select(MonitorEpisode))
        assert episode.sequence == 1 and episode.latched


@pytest.mark.asyncio
async def test_unknown_trigger_cannot_reopen_and_late_check_cannot_change_facts(
    migrated_database_url,
):
    from sqlalchemy import update
    from sqlalchemy.exc import DBAPIError

    factory, plan = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        original = await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW),
        )
        trigger = await session.scalar(select(MonitorTrigger))
        # Controlled lifecycle fixture: the future admission service must bind a new run.
        trigger.status, trigger.research_run_id = "admitted", plan.source_run_id
        await session.flush()
        trigger.status = "unknown"
    with pytest.raises(DBAPIError, match="MONITOR_TRIGGER_TRANSITION_INVALID"):
        async with factory() as session, session.begin():
            await session.execute(
                update(MonitorTrigger).values(status="pending", research_run_id=None)
            )
    async with factory() as session, session.begin():
        replay = await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW, False),
        )
        assert replay.id == original.id and replay.result == "true"
        trigger = await session.scalar(select(MonitorTrigger))
        assert trigger.status == "unknown" and trigger.research_run_id == plan.source_run_id
