from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from helpers.broker_facts import position, read_set
from helpers.broker_read import ScriptedBrokerProvider
from nexus.ops.fosun_local_refresh import (
    _ensure_market_bindings,
    _insert_calendar,
    _persist_ready_broker_snapshot,
)
from nexus.persistence.models.portfolio import (
    BrokerSyncRun,
    PortfolioSnapshotRecord,
)
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.reconciliation.broker_snapshot import freeze_broker_snapshot
from nexus.reconciliation.facts import canonical_position_hash
from nexus.settings import EngineSettings
from sqlalchemy import select


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="test-key",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET="test-secret",
    )


@pytest.mark.asyncio
async def test_local_market_bindings_are_reusable_across_successful_refreshes(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    first_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    try:
        async with factory() as session, session.begin():
            first_quote, first_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at,
                delivery_mode="unknown",
            )
        async with factory() as session, session.begin():
            second_quote, second_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at + timedelta(minutes=1),
                delivery_mode="unknown",
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second_quote.provider_capability_id == first_quote.provider_capability_id
    assert second_bars.provider_capability_id == first_bars.provider_capability_id
    assert second_quote.provider_status_id == first_quote.provider_status_id
    assert second_quote.provider_status_version == first_quote.provider_status_version + 1


@pytest.mark.asyncio
async def test_local_calendar_authority_is_reused_for_the_same_completed_window(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    published_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    rows = (
        (
            published_at.date(),
            published_at - timedelta(hours=10),
            published_at - timedelta(hours=3),
        ),
    )
    try:
        async with factory() as session, session.begin():
            first = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at,
            )
        async with factory() as session, session.begin():
            second = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at + timedelta(minutes=1),
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second.id == first.id


@pytest.mark.asyncio
async def test_local_ready_broker_snapshot_is_finalized_atomically(
    migrated_database_url: str,
) -> None:
    account_id = UUID("0198d600-1000-7000-8000-000000000001")
    portfolio_id = UUID("0198d600-1000-7000-8000-000000000002")
    sync_run_id = UUID("0198d600-1000-7000-8000-000000000003")
    instrument_id = UUID("0198d600-1000-7000-8000-000000000004")
    instant = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    stable = read_set(positions=(position(instrument_id),), orders=())
    frozen = await freeze_broker_snapshot(
        account_id,
        ScriptedBrokerProvider([(stable, stable)]),
        portfolio_id=portfolio_id,
        sync_run_id=sync_run_id,
        max_attempts=1,
    )
    holding = PositionItem(
        kind="position",
        external_account_id="redacted-test-account",
        instrument_code="usAAPL",
        currency="USD",
        quantity="10",
        available_quantity="10",
        frozen_quantity="0",
        average_cost="100",
        diluted_cost=None,
        current_price="120",
        provider_as_of=instant,
    )
    factory = session_factory(_settings(migrated_database_url))
    try:
        async with factory() as session, session.begin():
            stored = await _persist_ready_broker_snapshot(
                session,
                account_id=account_id,
                portfolio_id=portfolio_id,
                frozen=frozen,
                holdings=(holding,),
                instrument_ids={"usAAPL": instrument_id},
                valid_from=date(2026, 8, 31),
                completed_at=instant,
            )
        async with factory() as session:
            run = await session.get(BrokerSyncRun, sync_run_id)
            snapshot = await session.scalar(
                select(PortfolioSnapshotRecord).where(
                    PortfolioSnapshotRecord.id == stored.id
                )
            )
    finally:
        await factory.kw["bind"].dispose()

    assert run is not None and run.status == "READY"
    assert run.trigger_kind == "MANUAL"
    assert run.attempt_count == 1
    assert run.cash_flow_quality == "NOT_RUN"
    assert snapshot is not None and snapshot.status == "READY"
    assert snapshot.position_hash == canonical_position_hash(
        frozen.portfolio_snapshot.positions
    )
