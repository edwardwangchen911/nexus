"""Real publication/outbox transactions in disposable PostgreSQL databases."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from helpers.paper_batch import PORTFOLIO_ID, UNIVERSE_ID
from helpers.paper_performance import MARKET_CLOSE, seed_paper_performance, seed_second_paper_close
from nexus.domain.ids import new_uuid7
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.market.universe import stable_instrument_id
from nexus.notifications.worker import load_bark_worker_settings, run_bark_worker
from nexus.paper.agent_cycle_models import candidate_manifest_hash, floor_decision_slot
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.execution_service import cancel_internal_paper_order
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from sqlalchemy import func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

EVENT = "paper.session.summary.ready"


@pytest.fixture(autouse=True)
def historical_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "nexus.paper.performance.current_paper_performance_price_authority_available", lambda: True
    )


async def add_cycle(
    session,
    account_id,
    close=MARKET_CLOSE,
    *,
    status="completed",
    code="NO_ORDER_REQUIRED",
    minutes=60,
    created_at=None,
):
    decision_time = close - timedelta(minutes=minutes)
    cycle = AgentPaperCycleRecord(
        id=new_uuid7(),
        account_id=account_id,
        portfolio_id=PORTFOLIO_ID,
        decision_session=decision_time.date(),
        decision_time=decision_time,
        decision_slot_at=floor_decision_slot(decision_time),
        execution_session=decision_time.date(),
        universe_snapshot_id=UNIVERSE_ID,
        quality_report_id=None,
        decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
        candidate_manifest=[],
        candidate_hash=candidate_manifest_hash(()),
        sizing_policy_version="agent_paper_sizing_v1",
        sizing_policy_hash="8" * 64,
        execution_locked=True,
        created_at=created_at,
    )
    session.add(cycle)
    await session.flush()
    states = [] if status is None else ["prepared", "researching"]
    if status in {"completed", "awaiting_market"}:
        states += ["decision_recorded", "risk_evaluated"]
    if status is not None and status != states[-1]:
        states.append(status)
    for index, state in enumerate(states):
        session.add(
            AgentPaperCycleEventRecord(
                cycle_id=cycle.id,
                sequence=index + 1,
                from_status=states[index - 1] if index else None,
                to_status=state,
                result_code=code if state == status else "TEST_TRANSITION",
                fact_ids={},
                event_hash="9" * 64,
                occurred_at=decision_time,
            )
        )
        await session.flush()
    return cycle


async def summaries(session):
    return list(
        await session.scalars(
            select(OutboxEvent).where(OutboxEvent.event_type == EVENT).order_by(OutboxEvent.id)
        )
    )


async def add_spy_closes(session: AsyncSession, *, available_at: datetime) -> None:
    instrument_id = stable_instrument_id("SPY")
    session.add(
        Instrument(
            id=instrument_id,
            asset_type="etf",
            exchange="NYSE",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="etf",
        )
    )
    artifact = DataArtifact(
        id=new_uuid7(),
        uri="memory://summary/spy",
        sha256="a" * 64,
        byte_size=100,
        mime_type="application/x-parquet",
        schema_version="v1",
        source="alpaca",
        observed_at=MARKET_CLOSE,
        available_at=available_at,
        received_at=available_at,
        created_at=available_at,
    )
    session.add(artifact)
    manifest = PriceSeriesManifest(
        id=new_uuid7(),
        evidence_ref_id=None,
        data_artifact_id=artifact.id,
        instrument=str(instrument_id),
        source="alpaca",
        feed="sip",
        delivery_mode="delayed",
        adjustment="all",
        interval="1Day",
        start_at=MARKET_CLOSE - timedelta(days=1),
        end_at=MARKET_CLOSE,
        row_count=2,
        created_at=available_at,
    )
    session.add(manifest)
    await session.flush()
    for days, price in ((1, Decimal("100")), (0, Decimal("101"))):
        at = MARKET_CLOSE - timedelta(days=days)
        session.add(
            PriceBarRecord(
                id=new_uuid7(),
                price_series_manifest_id=manifest.id,
                instrument_id=instrument_id,
                session=at.date(),
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Decimal("1000000"),
                observed_at=at,
                available_at=available_at,
                received_at=available_at,
                created_at=available_at,
                revision="v1",
                quality_flags=[],
            )
        )
    await session.flush()
    assert (
        await finalize_price_bar_set(
            session, manifest, source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID
        )
        is not None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("unavailable_count", 1),
        ("spy_nav", "10100"),
        ("spy_return_ratio", "0.01"),
    ],
)
async def test_summary_uses_close_facts_available_by_publication_not_just_at_close(
    migrated_database_url: str,
    field: str,
    expected: object,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            cycle = await add_cycle(session, seed.execution.account_id, status="researching")
            # Add a terminal event after close, before the later publication.
            session.add(
                AgentPaperCycleEventRecord(
                    cycle_id=cycle.id,
                    sequence=3,
                    from_status="researching",
                    to_status="unavailable",
                    result_code="RESEARCH_UNAVAILABLE",
                    fact_ids={},
                    event_hash="7" * 64,
                    occurred_at=MARKET_CLOSE + timedelta(minutes=1),
                    created_at=MARKET_CLOSE + timedelta(minutes=1),
                )
            )
            await add_spy_closes(session, available_at=MARKET_CLOSE + timedelta(minutes=5))
            await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            events = await summaries(session)
            frozen = events[0].payload["notification_snapshot"]
            assert frozen[field] == expected
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cancel_remaining", "completed", "pending"), [(True, 1, 0), (False, 0, 1)]
)
async def test_awaiting_market_cycle_with_all_orders_terminal_counts_as_completed(
    migrated_database_url: str,
    cancel_remaining: bool,
    completed: int,
    pending: int,
) -> None:
    # Existing execution service leaves the cycle awaiting_market after filling its orders.
    seed = await seed_paper_performance(
        migrated_database_url,
        source="alpaca",
        feed="sip",
        delivery_mode="delayed",
        adjustment="all",
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            other_order = await session.scalar(
                select(PaperOrderRecord.id).where(
                    PaperOrderRecord.account_id == seed.execution.account_id,
                    PaperOrderRecord.risk_outcome == "approved",
                )
            )
            assert other_order is not None
            sequence = await session.scalar(
                select(func.max(PaperOrderEventRecord.event_seq)).where(
                    PaperOrderEventRecord.order_id == other_order
                )
            )
            if cancel_remaining:
                await cancel_internal_paper_order(
                    session,
                    order_id=other_order,
                    requested_at=seed.as_of + timedelta(minutes=1),
                    expected_event_seq=sequence,
                    idempotency_key="summary-eod-cancel",
                )
            cycle = await add_cycle(
                session,
                seed.execution.account_id,
                status="awaiting_market",
                code="PAPER_ORDERS_AWAITING_MARKET",
            )
            # Bind the existing synthetic execution fixture to this Agent lane. This changes
            # only test facts; all referenced orders, fills and latest terminal events are real.
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("""
                    UPDATE strategy_decisions SET decision_origin='agent_research',
                      strategy_id='agent_research',strategy_version='agent_paper_decision_v1',
                      strategy_definition_id=NULL,registry_event_id=NULL,champion_generation=NULL,
                      quality_report_id=NULL,reason='AGENT_RESEARCH',agent_paper_cycle_id=:cycle,
                      source_policy_id=:decision_policy,execution_source_policy_id=:execution_policy,
                      decision_session=:day,execution_session=:day,decision_time=:at
                    WHERE id=(SELECT strategy_decision_id FROM paper_orders WHERE id=:order_id)
                """),
                    {
                        "cycle": cycle.id,
                        "decision_policy": PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        "execution_policy": PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                        "day": cycle.decision_session,
                        "at": cycle.decision_time,
                        "order_id": seed.execution.order_id,
                    },
                )
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
            await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            frozen = (await summaries(session))[0].payload["notification_snapshot"]
            assert frozen["completed_count"] == completed
            assert frozen["no_order_count"] == 0
            assert frozen["pending_count"] == pending
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publication_freezes_one_summary_and_same_day_replay_cannot_refresh_it(
    migrated_database_url: str,
) -> None:
    # Missing atomic enqueue, wrong final-state fold, and mutable delayed delivery all break this.
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await add_cycle(session, seed.execution.account_id)
            await add_cycle(
                session,
                seed.execution.account_id,
                status="unavailable",
                code="RESEARCH_UNAVAILABLE",
                minutes=55,
            )
            await add_cycle(session, seed.execution.account_id, status=None, minutes=50)
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
        async with sessions.begin() as session:
            events = await summaries(session)
            assert len(events) == 1
            frozen = events[0].payload["notification_snapshot"]
            assert frozen["cycle_count"] == 3
            assert frozen["completed_count"] == 1
            assert frozen["unavailable_count"] == 1
            assert frozen["pending_count"] == 1
            assert frozen["no_order_count"] == 1
            assert Decimal(frozen["nav"]) == publication.performance.nav
            assert frozen["daily_pnl"] is None and frozen["daily_return_ratio"] is None
            assert frozen["fill_count"] == 1
            # The existing fixture is one zero-commission buy fill.
            assert Decimal(frozen["daily_fees"]) == 0
            assert frozen["spy_nav"] is None and frozen["spy_return_ratio"] is None
            assert Decimal(frozen["hold_current_return_ratio"]) == 0
            assert "content_hash" not in frozen and "evidence" not in frozen
            await add_cycle(session, seed.execution.account_id, minutes=45)
            await add_spy_closes(session, available_at=MARKET_CLOSE + timedelta(minutes=5))
        async with sessions.begin() as session:
            replay = await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            assert replay.replayed
        async with sessions() as session:
            events = await summaries(session)
            assert len(events) == 1
            assert events[0].payload["notification_snapshot"] == frozen
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_no_cycle_means_no_empty_summary(migrated_database_url: str) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            assert await summaries(session) == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_evaluation_performance_can_suppress_session_summary(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await add_cycle(session, seed.execution.account_id)
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
                enqueue_session_summary=False,
            )
            assert publication.replayed is False
            assert await summaries(session) == []

        async with AsyncSession(engine) as session, session.begin():
            replay = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
                enqueue_session_summary=False,
            )
            assert replay.replayed is True
            assert await summaries(session) == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_summary_is_not_eligible_before_the_market_close(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await add_cycle(session, seed.execution.account_id)
        with monkeypatch.context() as clock_patch:
            clock_patch.setattr(
                "nexus.paper.performance.func",
                SimpleNamespace(
                    max=func.max, clock_timestamp=lambda: literal(seed.as_of - timedelta(seconds=1))
                ),
            )
            with pytest.raises(PaperPerformanceError, match="PAPER_PERFORMANCE_INVALID"):
                async with AsyncSession(engine) as session, session.begin():
                    await publish_paper_performance_snapshot(
                        session, account_id=seed.execution.account_id, as_of=seed.as_of
                    )
        async with AsyncSession(engine) as session, session.begin():
            await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            assert len(await summaries(session)) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_runtime_counts_respect_the_frozen_publication_cutoff(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    monkeypatch.setattr(
        "nexus.paper.session_summary_publication.func",
        SimpleNamespace(
            max=func.max, clock_timestamp=lambda: literal(seed.as_of + timedelta(minutes=5))
        ),
    )
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await add_cycle(session, seed.execution.account_id)
            await add_cycle(
                session,
                seed.execution.account_id,
                minutes=55,
                created_at=seed.as_of + timedelta(minutes=6),
            )
            await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
            frozen = (await summaries(session))[0].payload["notification_snapshot"]
            assert frozen["cycle_count"] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_price", [False, True])
async def test_failed_performance_transaction_leaves_no_summary(
    migrated_database_url: str,
    missing_price: bool,
) -> None:
    seed = await seed_paper_performance(
        migrated_database_url, close=None if missing_price else Decimal("110")
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await add_cycle(session, seed.execution.account_id)
        with pytest.raises(PaperPerformanceError if missing_price else RuntimeError):
            async with sessions.begin() as session:
                await publish_paper_performance_snapshot(
                    session, account_id=seed.execution.account_id, as_of=seed.as_of
                )
                raise RuntimeError("caller transaction aborted")
        async with sessions() as session:
            assert await summaries(session) == []
            assert list(await session.scalars(select(PaperPerformanceSnapshotRecord))) == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_next_session_gets_its_own_summary_and_daily_pnl(migrated_database_url: str) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    next_close = await seed_second_paper_close(
        migrated_database_url, execution=seed.execution, close=Decimal("111")
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        for close in (seed.as_of, next_close):
            async with sessions.begin() as session:
                await add_cycle(session, seed.execution.account_id, close)
                await publish_paper_performance_snapshot(
                    session, account_id=seed.execution.account_id, as_of=close
                )
        async with sessions() as session:
            events = await summaries(session)
            assert len(events) == 2
            first, second = [row.payload["notification_snapshot"] for row in events]
            assert first["session_date"] == "2026-08-27"
            assert second["session_date"] == "2026-08-28"
            # Fixture fills 14 shares; a $1 close change earns $14.
            assert Decimal(second["daily_pnl"]) == Decimal("14")
            assert Decimal(second["daily_return_ratio"]) > 0
            assert Decimal(second["daily_fees"]) == 0
            assert second["fill_count"] == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_worker_failure_keeps_performance_and_frozen_summary_committed(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await add_cycle(session, seed.execution.account_id)
            publication = await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
                "NEXUS_BARK_DEVICE_KEY": "fixture-device",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": "2025-12-31T23:59:59Z",
            }
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(503, text="private upstream detail")
            )
        ) as client:
            # Fixed clock prevents delayed retries; drain each distinct eligible event once.
            for _ in range(10):
                if not await run_bark_worker(
                    settings,
                    run_forever=False,
                    client=client,
                    clock=lambda: datetime(2027, 1, 1, tzinfo=UTC),
                ):
                    break
        async with sessions() as session:
            events = await summaries(session)
            assert len(events) == 1
            assert events[0].attempt_count == 1
            assert events[0].status == "queued"
            attempts = list(
                await session.scalars(
                    select(OutboxDeliveryAttempt).where(
                        OutboxDeliveryAttempt.outbox_event_id == events[0].id
                    )
                )
            )
            assert len(attempts) == 1 and attempts[0].outcome == "failed"
            fact = await session.get(PaperPerformanceSnapshotRecord, publication.snapshot_id)
            assert fact is not None and fact.content_hash == publication.performance.content_hash
            assert events[0].last_error_code == "PUBLISHER_FAILED"
    finally:
        await engine.dispose()
