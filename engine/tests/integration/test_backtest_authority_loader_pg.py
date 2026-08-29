"""Round-trip coverage for the database-owned backtest authority loader."""

import pytest
from nexus.backtest.authority_loader import (
    BACKTEST_AUTHORITY_INCOMPLETE,
    load_point_in_time_dataset,
)
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.backtest.engine import run_backtest
from nexus.backtest.persistence import BacktestPersistenceAuthority, persist_backtest_result
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from sqlalchemy import text

from engine.tests.integration.test_backtest_writer import (
    AUTHORITY_BUNDLE_ID,
    COMPLETED_AT,
    DATA_SNAPSHOT_ID,
    _dataset,
    _raw_inputs,
    _seed_data_snapshot,
    _seed_upstream_authorities,
)


@pytest.mark.asyncio
async def test_loader_reconstructs_and_replays_a_sealed_baseline(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    async with factory() as session, session.begin():
        record = await persist_backtest_result(
            session, strategy, data, protocol, result, authority
        )
        run_id = record.id
        as_of = record.authority_frozen_at

    async with factory() as session, session.begin():
        loaded = await load_point_in_time_dataset(session, run_id, as_of)

    assert loaded.protocol == protocol
    assert loaded.strategy == strategy
    assert run_backtest(loaded.strategy, loaded.data, loaded.protocol) == result


@pytest.mark.asyncio
async def test_loader_fails_closed_when_an_exact_set_member_is_missing(
    migrated_database_url: str,
) -> None:
    strategy, bars, sessions, protocol = _raw_inputs()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    result = run_backtest(strategy, data, protocol)
    await _seed_data_snapshot(factory, data, protocol, result)
    authority = BacktestPersistenceAuthority(
        authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        completed_at=COMPLETED_AT,
    )
    async with factory() as session, session.begin():
        record = await persist_backtest_result(
            session, strategy, data, protocol, result, authority
        )
        run_id = record.id
        as_of = record.authority_frozen_at

    async with factory() as session, session.begin():
        await session.execute(
            text("ALTER TABLE backtest_price_authority_members DISABLE TRIGGER USER")
        )
        await session.execute(
            text(
                "DELETE FROM backtest_price_authority_members "
                "WHERE bundle_id = :bundle AND price_bar_set_authority_id = :member"
            ),
            {
                "bundle": AUTHORITY_BUNDLE_ID,
                "member": price_authorities[0].price_bar_set_authority_id,
            },
        )
        await session.execute(
            text("ALTER TABLE backtest_price_authority_members ENABLE TRIGGER USER")
        )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match=BACKTEST_AUTHORITY_INCOMPLETE):
            await load_point_in_time_dataset(session, run_id, as_of)
