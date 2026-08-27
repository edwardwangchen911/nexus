import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.canonical import canonical_forward_strategy_decision_hash
from nexus.strategy.baseline import StrategyDecision, StrategyTarget
from nexus.strategy.forward import (
    ForwardDecisionConflictError,
    build_forward_strategy_decision,
    load_forward_strategy_decision,
    persist_forward_strategy_decision,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")
SOURCE_POLICY_ID = UUID("30000000-0000-0000-0000-000000000001")
CALENDAR_ID = UUID("30000000-0000-0000-0000-000000000002")
UNIVERSE_ID = UUID("30000000-0000-0000-0000-000000000003")
DATA_SNAPSHOT_ID = UUID("30000000-0000-0000-0000-000000000004")
QUALITY_REPORT_ID = UUID("30000000-0000-0000-0000-000000000005")
CURRENT_ID = UUID("30000000-0000-0000-0000-000000000006")
NEW_ID = UUID("30000000-0000-0000-0000-000000000007")
EXIT_ID = UUID("30000000-0000-0000-0000-000000000008")
DECISION_TIME = datetime(2026, 8, 21, 20, tzinfo=UTC)
BASELINE_DEFINITION_ID = UUID("019d0000-0000-7000-8000-000000002200")
GENESIS_EVENT_ID = UUID("019d0000-0000-7000-8000-000000002201")
BASELINE_EXPRESSION_HASH = "f643694ad6d31f0dd504061fbd06068a93f9fb679aba9ba5b75337e2e169d13d"


async def _seed_authority(database_url: str, *, published_at: datetime) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for instrument_id in (CURRENT_ID, NEW_ID, EXIT_ID):
                await connection.execute(
                    text(
                        "INSERT INTO instruments "
                        "(id, asset_type, exchange, currency, tick_size, risk_bucket) "
                        "VALUES (:id, 'common_stock', 'NASDAQ', 'USD', 0.01, 'technology')"
                    ),
                    {"id": instrument_id},
                )
            await connection.execute(
                text(
                    "INSERT INTO source_policies "
                    "(id, key, version, schema_version, payload) "
                    "VALUES (:id, 'paper-forward', 'v1', 'v1', '{}'::jsonb)"
                ),
                {"id": SOURCE_POLICY_ID},
            )
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_authorities "
                    "(id, exchange, version, source, range_start, range_end, "
                    "query_parameters, completeness_status, content_hash, published_at) "
                    "VALUES (:id, 'XNYS', 'forward-v1', 'fixture', '2026-08-01', "
                    "'2026-08-31', '{}'::jsonb, 'complete', :hash, :published_at)"
                ),
                {"id": CALENDAR_ID, "hash": "a" * 64, "published_at": published_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id, session, decision_at) VALUES "
                    "(:id, '2026-08-21', '2026-08-21T20:00:00Z'), "
                    "(:id, '2026-08-24', '2026-08-24T20:00:00Z'), "
                    "(:id, '2026-08-25', '2026-08-25T20:00:00Z')"
                ),
                {"id": CALENDAR_ID},
            )
            await connection.execute(
                text(
                    "INSERT INTO universe_snapshots "
                    "(id, key, version, effective_from, source, content_hash, published_at) "
                    "VALUES (:id, 'paper-forward', 'v1', '2026-08-01', 'fixture', "
                    ":hash, NULL)"
                ),
                {"id": UNIVERSE_ID, "hash": "b" * 64, "published_at": published_at},
            )
            for index, instrument_id in enumerate((CURRENT_ID, NEW_ID, EXIT_ID), start=1):
                await connection.execute(
                    text(
                        "INSERT INTO universe_members "
                        "(id, universe_snapshot_id, instrument_id, inclusion_reason) "
                        "VALUES (:id, :universe_id, :instrument_id, 'TEST')"
                    ),
                    {
                        "id": UUID(int=100 + index),
                        "universe_id": UNIVERSE_ID,
                        "instrument_id": instrument_id,
                    },
                )
            await connection.execute(
                text("UPDATE universe_snapshots SET published_at = :published_at WHERE id = :id"),
                {"id": UNIVERSE_ID, "published_at": published_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO data_snapshots "
                    "(id, source, provider_object_id, revision, observed_at, available_at, "
                    "received_at, schema_version, payload) VALUES "
                    "(:id, 'fixture', 'forward-quality', 'v1', :published_at, "
                    ":published_at, :published_at, 'v1', '{}'::jsonb)"
                ),
                {"id": DATA_SNAPSHOT_ID, "published_at": published_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO quality_reports "
                    "(id, data_snapshot_id, status, schema_version, payload) "
                    "VALUES (:id, :snapshot_id, 'PASS', 'v1', '{}'::jsonb)"
                ),
                {"id": QUALITY_REPORT_ID, "snapshot_id": DATA_SNAPSHOT_ID},
            )
    finally:
        await engine.dispose()


def _decision():
    return build_forward_strategy_decision(
        StrategyDecision(
            decision_session=date(2026, 8, 21),
            targets=(
                StrategyTarget(
                    instrument_id=CURRENT_ID,
                    weight=Decimal("0.125"),
                    rank=1,
                    momentum_126_skip5=Decimal("0.2"),
                ),
                StrategyTarget(
                    instrument_id=NEW_ID,
                    weight=Decimal("0.125"),
                    rank=2,
                    momentum_126_skip5=Decimal("0.1"),
                ),
            ),
            exits=(EXIT_ID,),
            cash_weight=Decimal("0.75"),
            reason="WEEKLY_REBALANCE",
        ),
        account_id=PAPER_ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        current_positions=(CURRENT_ID, EXIT_ID),
        whitelisted_instrument_ids=(CURRENT_ID, NEW_ID, EXIT_ID),
        decision_time=DECISION_TIME,
        execution_session=date(2026, 8, 24),
        source_policy_id=SOURCE_POLICY_ID,
        calendar_authority_id=CALENDAR_ID,
        universe_snapshot_id=UNIVERSE_ID,
        quality_report_id=QUALITY_REPORT_ID,
        portfolio_projection_hash="c" * 64,
        authority_hash="d" * 64,
        strategy_parameters_hash=BASELINE_EXPRESSION_HASH,
        strategy_definition_id=BASELINE_DEFINITION_ID,
        registry_event_id=GENESIS_EVENT_ID,
        champion_generation=1,
    )


async def _persist_and_reload(database_url: str):
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await persist_forward_strategy_decision(session, _decision())
        async with sessions.begin() as session:
            second = await persist_forward_strategy_decision(session, _decision())
            loaded = await load_forward_strategy_decision(
                session,
                account_id=PAPER_ACCOUNT_ID,
                strategy_version="baseline_v1",
                decision_session=date(2026, 8, 21),
            )
        return first, second, loaded
    finally:
        await engine.dispose()


def test_forward_decision_persists_exactly_and_is_idempotent(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
        )
    )
    first, second, loaded = asyncio.run(_persist_and_reload(migrated_database_url))

    assert first.decision_id == second.decision_id
    assert loaded == first
    assert loaded.decision == _decision()


async def _persist_conflict(database_url: str) -> None:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await persist_forward_strategy_decision(session, _decision())
        changed = _decision().model_copy(update={"strategy_parameters_hash": "f" * 64})
        changed = changed.model_copy(
            update={"decision_hash": canonical_forward_strategy_decision_hash(changed)}
        )
        async with sessions.begin() as session:
            await persist_forward_strategy_decision(session, changed)
    finally:
        await engine.dispose()


def test_same_decision_key_with_different_content_conflicts(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
        )
    )
    with pytest.raises(ForwardDecisionConflictError, match="content conflict"):
        asyncio.run(_persist_conflict(migrated_database_url))


async def _persist_late_authority(database_url: str) -> None:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await persist_forward_strategy_decision(session, _decision())
    finally:
        await engine.dispose()


def test_authority_published_after_decision_time_is_rejected(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 21, tzinfo=UTC),
        )
    )
    with pytest.raises(ValueError, match="available after decision time"):
        asyncio.run(_persist_late_authority(migrated_database_url))


async def _count_decisions(database_url: str) -> int:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return int(await connection.scalar(text("SELECT count(*) FROM strategy_decisions")))
    finally:
        await engine.dispose()


def test_not_week_end_produces_no_strategy_decision_row(
    migrated_database_url: str,
) -> None:
    not_week_end = StrategyDecision(
        decision_session=date(2026, 8, 20),
        targets=(),
        exits=(),
        cash_weight=None,
        reason="NOT_WEEK_END",
    )
    assert (
        build_forward_strategy_decision(
            not_week_end,
            account_id=PAPER_ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            current_positions=(),
            whitelisted_instrument_ids=(),
            decision_time=DECISION_TIME,
            execution_session=date(2026, 8, 21),
            source_policy_id=SOURCE_POLICY_ID,
            calendar_authority_id=CALENDAR_ID,
            universe_snapshot_id=UNIVERSE_ID,
            quality_report_id=QUALITY_REPORT_ID,
            portfolio_projection_hash="a" * 64,
            authority_hash="b" * 64,
            strategy_parameters_hash="c" * 64,
            strategy_definition_id=BASELINE_DEFINITION_ID,
            registry_event_id=GENESIS_EVENT_ID,
            champion_generation=1,
        )
        is None
    )
    assert asyncio.run(_count_decisions(migrated_database_url)) == 0
