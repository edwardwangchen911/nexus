import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import nexus.strategy.forward as forward_module
import pytest
from nexus.quality.source_policy import SOURCE_POLICY_V2_ID
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
LEGACY_SOURCE_POLICY_ID = UUID("30000000-0000-0000-0000-000000000001")
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


@pytest.fixture
def adjusted_v2_authority_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "nexus.strategy.forward.source_policy_v2_supports_adjusted_prices",
        lambda: True,
    )


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
                {"id": LEGACY_SOURCE_POLICY_ID},
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


def _decision(*, source_policy_id: UUID = SOURCE_POLICY_V2_ID):
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
        source_policy_id=source_policy_id,
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
    adjusted_v2_authority_available: None,
) -> None:
    del adjusted_v2_authority_available
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
    adjusted_v2_authority_available: None,
) -> None:
    del adjusted_v2_authority_available
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
        )
    )
    with pytest.raises(ForwardDecisionConflictError, match="content conflict"):
        asyncio.run(_persist_conflict(migrated_database_url))


async def _persist_concurrently(
    database_url: str,
    decisions,
    monkeypatch: pytest.MonkeyPatch,
):
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    original_authority_check = forward_module.require_paper_decision_authority
    all_validated = asyncio.Event()
    validated = 0

    async def synchronized_authority_check(session, decision):
        nonlocal validated
        await original_authority_check(session, decision)
        validated += 1
        if validated == len(decisions):
            all_validated.set()
        await asyncio.wait_for(all_validated.wait(), timeout=5)

    monkeypatch.setattr(
        forward_module,
        "require_paper_decision_authority",
        synchronized_authority_check,
    )

    async def persist(decision):
        async with sessions.begin() as session:
            return await persist_forward_strategy_decision(session, decision)

    try:
        return await asyncio.gather(
            *(persist(decision) for decision in decisions),
            return_exceptions=True,
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_identical_decision_key_replays_one_fact(
    migrated_database_url: str,
    adjusted_v2_authority_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del adjusted_v2_authority_available
    await _seed_authority(
        migrated_database_url,
        published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
    )

    first, second = await _persist_concurrently(
        migrated_database_url,
        (_decision(), _decision()),
        monkeypatch,
    )

    assert not isinstance(first, BaseException)
    assert not isinstance(second, BaseException)
    assert first.decision_id == second.decision_id
    assert await _count_decisions(migrated_database_url) == 1


@pytest.mark.asyncio
async def test_concurrent_different_decision_key_returns_typed_conflict(
    migrated_database_url: str,
    adjusted_v2_authority_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del adjusted_v2_authority_available
    await _seed_authority(
        migrated_database_url,
        published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
    )
    changed = _decision().model_copy(update={"authority_hash": "e" * 64})
    changed = changed.model_copy(
        update={"decision_hash": canonical_forward_strategy_decision_hash(changed)}
    )

    results = await _persist_concurrently(
        migrated_database_url,
        (_decision(), changed),
        monkeypatch,
    )

    persisted = [item for item in results if not isinstance(item, BaseException)]
    conflicts = [item for item in results if isinstance(item, ForwardDecisionConflictError)]
    unexpected = [
        item
        for item in results
        if isinstance(item, BaseException) and not isinstance(item, ForwardDecisionConflictError)
    ]
    assert len(persisted) == len(conflicts) == 1
    assert unexpected == []
    assert str(conflicts[0]) == "forward decision content conflict"
    assert await _count_decisions(migrated_database_url) == 1


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
    adjusted_v2_authority_available: None,
) -> None:
    del adjusted_v2_authority_available
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 21, tzinfo=UTC),
        )
    )
    with pytest.raises(ValueError, match="available after decision time"):
        asyncio.run(_persist_late_authority(migrated_database_url))


async def _persist_legacy_policy_decision(database_url: str) -> None:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await persist_forward_strategy_decision(
                session,
                _decision(source_policy_id=LEGACY_SOURCE_POLICY_ID),
            )
    finally:
        await engine.dispose()


def test_new_forward_decision_rejects_legacy_source_policy(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
        )
    )
    with pytest.raises(ValueError, match="source_policy_v2"):
        asyncio.run(_persist_legacy_policy_decision(migrated_database_url))
    assert asyncio.run(_count_decisions(migrated_database_url)) == 0


def test_current_raw_only_v2_cannot_persist_new_forward_decision(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _seed_authority(
            migrated_database_url,
            published_at=datetime(2026, 8, 21, 19, tzinfo=UTC),
        )
    )

    with pytest.raises(
        ValueError,
        match="adjusted price authority is unavailable",
    ):
        asyncio.run(_persist_late_authority(migrated_database_url))

    assert asyncio.run(_count_decisions(migrated_database_url)) == 0


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
            source_policy_id=LEGACY_SOURCE_POLICY_ID,
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


RANK_TIME = datetime(2026, 8, 28, 20, tzinfo=UTC)


async def _seed_rank_authority(database_url):
    await _seed_authority(database_url, published_at=DECISION_TIME)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id, session, decision_at) VALUES "
                    "(:id, '2026-08-28', '2026-08-28T20:00:00Z'), "
                    "(:id, '2026-08-31', '2026-08-31T20:00:00Z')"
                ),
                {"id": CALENDAR_ID},
            )
    finally:
        await engine.dispose()


async def _rank_checkpoint_inputs(session):
    from nexus.paper.projection import load_paper_projection
    from nexus.strategy.baseline import BaselineInstrumentSnapshot, StrategyDataSnapshot
    from nexus.strategy.rank_checkpoint import RankCheckpointAuthority

    projection = await load_paper_projection(session, account_id=PAPER_ACCOUNT_ID, as_of=RANK_TIME)
    snapshot = StrategyDataSnapshot(
        decision_session=RANK_TIME.date(),
        next_session=date(2026, 8, 31),
        instruments=(
            BaselineInstrumentSnapshot(
                instrument_id=CURRENT_ID,
                symbol="SYNTHETIC",
                asset_type="stock",
                sector_exposures=({"sector": "information_technology", "weight": Decimal(1)},),
                raw_close=Decimal(200),
                closes=(Decimal(100),) * 195 + (Decimal(200),) * 5,
                dollar_volumes=(Decimal(60000000),) * 20,
            ),
        ),
    )
    authority = RankCheckpointAuthority(
        account_id=PAPER_ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        decision_time=RANK_TIME,
        execution_session=date(2026, 8, 31),
        source_policy_id=SOURCE_POLICY_V2_ID,
        calendar_authority_id=CALENDAR_ID,
        universe_snapshot_id=UNIVERSE_ID,
        quality_report_id=QUALITY_REPORT_ID,
        portfolio_projection_hash=projection.projection_hash,
        authority_hash="d" * 64,
    )
    return snapshot, authority


@pytest.mark.asyncio
async def test_rank_checkpoint_atomic_rollback_and_double_object_idempotency(
    migrated_database_url,
    adjusted_v2_authority_available,
    monkeypatch,
):
    from nexus.strategy.rank_checkpoint import evaluate_and_persist_rank_checkpoint
    from sqlalchemy import event

    await _seed_rank_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            snapshot, authority = await _rank_checkpoint_inputs(session)

            def fail_checkpoint(conn, cursor, statement, parameters, context, executemany):
                if statement.startswith("INSERT INTO strategy_rank_checkpoints"):
                    raise RuntimeError("synthetic checkpoint failure")

            event.listen(engine.sync_engine, "before_cursor_execute", fail_checkpoint)
            try:
                with pytest.raises(RuntimeError, match="synthetic checkpoint failure"):
                    await evaluate_and_persist_rank_checkpoint(
                        session, snapshot=snapshot, authority=authority
                    )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", fail_checkpoint)
            assert await session.scalar(text("SELECT count(*) FROM strategy_decisions")) == 0
            assert await session.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints")) == 0
            first = await evaluate_and_persist_rank_checkpoint(
                session, snapshot=snapshot, authority=authority
            )
            assert first is not None
            stored = await session.scalar(
                text("SELECT observations FROM strategy_rank_checkpoints WHERE decision_id=:id"),
                {"id": first.forward.decision_id},
            )
            assert "input_references" not in stored  # Preserve the pre-reference JSON/hash.

        # A later registry activation need not be visible at this historical cutoff.
        async def no_current_event(*args, **kwargs):
            return None

        monkeypatch.setattr(
            "nexus.strategy.rank_checkpoint._active_registry_event", no_current_event
        )
        async with factory.begin() as session:
            second = await evaluate_and_persist_rank_checkpoint(
                session, snapshot=snapshot, authority=authority
            )
            assert second == first
            assert await session.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints")) == 1
            changed = snapshot.model_copy(
                update={
                    "instruments": (
                        snapshot.instruments[0].model_copy(update={"symbol": "DIFFERENT"}),
                    )
                }
            )
            with pytest.raises(ValueError, match="RANK_CHECKPOINT_CONTENT_CONFLICT"):
                await evaluate_and_persist_rank_checkpoint(
                    session, snapshot=changed, authority=authority
                )
        async with factory.begin() as session:
            with pytest.raises(Exception, match="immutable|append.only"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            "UPDATE strategy_rank_checkpoints SET checkpoint_hash = repeat('f', 64)"
                        )
                    )
            with pytest.raises(Exception, match="immutable|append.only"):
                async with session.begin_nested():
                    await session.execute(text("DELETE FROM strategy_rank_checkpoints"))
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
        with pytest.raises(RuntimeError, match="rank checkpoint history prevents downgrade"):
            await asyncio.to_thread(command.downgrade, config, "0036_strategy_rnd_usage_unknown")
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints")) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_rank_checkpoint_ignores_agent_decisions_in_registered_timeline(
    migrated_database_url,
    adjusted_v2_authority_available,
):
    from nexus.paper.agent_cycle_models import candidate_manifest_hash
    from nexus.persistence.models.paper import (
        PAPER_DECISION_SOURCE_POLICY_ID,
        PAPER_EXECUTION_SOURCE_POLICY_ID,
        AgentPaperCycleRecord,
        StrategyDecisionRecord,
    )
    from nexus.strategy.rank_checkpoint import evaluate_and_persist_rank_checkpoint

    await _seed_rank_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            for index, (decision_session, decision_time, execution_session) in enumerate(
                (
                    (date(2026, 8, 28), RANK_TIME, date(2026, 8, 31)),
                    (
                        date(2026, 8, 31),
                        datetime(2026, 8, 31, 20, tzinfo=UTC),
                        date(2026, 9, 1),
                    ),
                ),
                start=1,
            ):
                cycle_id = UUID(int=0x63000000000000000000000000001000 + index)
                session.add(
                    AgentPaperCycleRecord(
                        id=cycle_id,
                        account_id=PAPER_ACCOUNT_ID,
                        portfolio_id=PORTFOLIO_ID,
                        decision_session=decision_session,
                        decision_time=decision_time,
                        decision_slot_at=decision_time,
                        execution_session=execution_session,
                        universe_snapshot_id=UNIVERSE_ID,
                        quality_report_id=None,
                        decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                        execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                        candidate_manifest=[],
                        candidate_hash=candidate_manifest_hash(()),
                        sizing_policy_version="agent_paper_sizing_v1",
                        sizing_policy_hash="a" * 64,
                        execution_locked=True,
                    )
                )
                await session.flush()
                session.add(
                    StrategyDecisionRecord(
                        id=UUID(int=0x63000000000000000000000000002000 + index),
                        account_id=PAPER_ACCOUNT_ID,
                        portfolio_id=PORTFOLIO_ID,
                        strategy_id="agent_research",
                        strategy_version="agent_paper_decision_v1",
                        strategy_definition_id=None,
                        registry_event_id=None,
                        champion_generation=None,
                        decision_origin="agent_research",
                        agent_paper_cycle_id=cycle_id,
                        decision_session=decision_session,
                        decision_time=decision_time,
                        execution_session=execution_session,
                        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                        execution_source_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                        calendar_authority_id=CALENDAR_ID,
                        universe_snapshot_id=UNIVERSE_ID,
                        quality_report_id=None,
                        portfolio_projection_hash="b" * 64,
                        authority_hash="c" * 64,
                        strategy_parameters_hash="d" * 64,
                        decision_hash=f"{index + 4}" * 64,
                        cash_weight=Decimal(1),
                        reason="AGENT_RESEARCH",
                    )
                )
            await session.flush()

            snapshot, authority = await _rank_checkpoint_inputs(session)
            persisted = await evaluate_and_persist_rank_checkpoint(
                session, snapshot=snapshot, authority=authority
            )

            assert persisted is not None
            stored = await session.get(StrategyDecisionRecord, persisted.forward.decision_id)
            assert stored is not None
            assert stored.decision_origin == "registered_strategy"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("same_session", [True, False])
async def test_rank_checkpoint_refuses_old_decision_without_checkpoint(
    migrated_database_url,
    adjusted_v2_authority_available,
    same_session,
):
    from nexus.strategy.rank_checkpoint import evaluate_and_persist_rank_checkpoint

    await _seed_rank_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine)
    try:
        async with factory.begin() as session:
            old = _decision()
            if same_session:
                old = old.model_copy(
                    update={
                        "decision_session": RANK_TIME.date(),
                        "decision_time": RANK_TIME,
                        "execution_session": date(2026, 8, 31),
                    }
                )
                old = old.model_copy(
                    update={"decision_hash": canonical_forward_strategy_decision_hash(old)}
                )
            await persist_forward_strategy_decision(session, old)
            snapshot, authority = await _rank_checkpoint_inputs(session)
            with pytest.raises(ValueError, match="RANK_CHECKPOINT_HISTORY_UNAVAILABLE"):
                await evaluate_and_persist_rank_checkpoint(
                    session, snapshot=snapshot, authority=authority
                )
            assert await session.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints")) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["adjusted", "legacy", "nonweek", "projection"])
async def test_rank_checkpoint_keeps_original_admission_and_nonweek_no_write(
    migrated_database_url,
    kind,
):
    from dataclasses import replace

    from nexus.strategy.rank_checkpoint import evaluate_and_persist_rank_checkpoint

    await _seed_rank_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine)
    try:
        async with factory.begin() as session:
            snapshot, authority = await _rank_checkpoint_inputs(session)
            if kind == "legacy":
                authority = replace(authority, source_policy_id=LEGACY_SOURCE_POLICY_ID)
            if kind == "projection":
                authority = replace(authority, portfolio_projection_hash="f" * 64)
            if kind == "nonweek":
                snapshot = snapshot.model_copy(
                    update={
                        "decision_session": date(2026, 8, 31),
                        "next_session": date(2026, 9, 1),
                    }
                )
                authority = replace(
                    authority,
                    decision_time=datetime(2026, 8, 31, 20, tzinfo=UTC),
                    execution_session=date(2026, 9, 1),
                )
                assert (
                    await evaluate_and_persist_rank_checkpoint(
                        session, snapshot=snapshot, authority=authority
                    )
                    is None
                )
            else:
                reason = {
                    "adjusted": "adjusted price authority",
                    "legacy": "exact source_policy_v2",
                    "projection": "RANK_CHECKPOINT_PROJECTION_MISMATCH",
                }[kind]
                with pytest.raises(ValueError, match=reason):
                    await evaluate_and_persist_rank_checkpoint(
                        session, snapshot=snapshot, authority=authority
                    )
            assert await session.scalar(text("SELECT count(*) FROM strategy_decisions")) == 0
            assert await session.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints")) == 0
    finally:
        await engine.dispose()


def test_rank_checkpoint_empty_migration_roundtrip(migrated_database_url):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    assert ScriptDirectory.from_config(config).get_heads() == [CURRENT_MIGRATION_HEAD]
    command.downgrade(config, "0036_strategy_rnd_usage_unknown")
    command.upgrade(config, CURRENT_MIGRATION_HEAD)

    async def check():
        engine = create_async_engine(migrated_database_url)
        try:
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(text("SELECT version_num FROM alembic_version"))
                    == CURRENT_MIGRATION_HEAD
                )
                assert (
                    await connection.scalar(text("SELECT count(*) FROM strategy_rank_checkpoints"))
                    == 0
                )
        finally:
            await engine.dispose()

    asyncio.run(check())


@pytest.mark.asyncio
async def test_rank_checkpoint_links_the_previous_decision_atomically(
    migrated_database_url,
    adjusted_v2_authority_available,
):
    from dataclasses import replace

    from nexus.paper.projection import load_paper_projection
    from nexus.strategy.rank_checkpoint import evaluate_and_persist_rank_checkpoint

    await _seed_rank_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            snapshot, authority = await _rank_checkpoint_inputs(session)
            first = await evaluate_and_persist_rank_checkpoint(
                session, snapshot=snapshot, authority=authority
            )
            await session.execute(
                text(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id, session, decision_at) VALUES "
                    "(:id, '2026-09-04', '2026-09-04T20:00:00Z'), "
                    "(:id, '2026-09-07', '2026-09-07T20:00:00Z')"
                ),
                {"id": CALENDAR_ID},
            )
        async with factory.begin() as session:
            cutoff = datetime(2026, 9, 4, 20, tzinfo=UTC)
            projection = await load_paper_projection(
                session, account_id=PAPER_ACCOUNT_ID, as_of=cutoff
            )
            second = await evaluate_and_persist_rank_checkpoint(
                session,
                snapshot=snapshot.model_copy(
                    update={"decision_session": cutoff.date(), "next_session": date(2026, 9, 7)}
                ),
                authority=replace(
                    authority,
                    decision_time=cutoff,
                    execution_session=date(2026, 9, 7),
                    portfolio_projection_hash=projection.projection_hash,
                ),
            )
            assert second is not None and first is not None
            parent_id = await session.scalar(
                text(
                    "SELECT previous_decision_id FROM strategy_rank_checkpoints "
                    "WHERE decision_id=:id"
                ),
                {"id": second.forward.decision_id},
            )
            assert parent_id == first.forward.decision_id
            assert second.checkpoint_hash != first.checkpoint_hash
    finally:
        await engine.dispose()
