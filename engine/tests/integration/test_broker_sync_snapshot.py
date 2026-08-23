from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from helpers.broker_facts import order, position, read_set
from helpers.broker_read import ScriptedBrokerProvider
from helpers.broker_seed import _seed_account_and_portfolio
from helpers.broker_snapshot import _snapshot, _sync_result
from nexus.persistence.models.portfolio import (
    BrokerOrderSnapshot,
    BrokerSyncAttempt,
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.reconciliation.broker_snapshot import freeze_broker_snapshot
from nexus.reconciliation.persistence import (
    BrokerSyncPersistenceError,
    BrokerSyncRepository,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_ID = UUID("0198d400-1000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("0198d400-1000-7000-8000-000000000002")
READY_SNAPSHOT_ID = UUID("0198d400-1000-7000-8000-000000000003")
INSTRUMENT_ID = UUID("0198d400-1000-7000-8000-000000000004")
AEAD_KEY = "c3ludGhldGljLWJyb2tlci1hZWFkLWtleS0zMmJ5dGU="
BLIND_KEY = "c3ludGhldGljLWJyb2tlci1ibGluZC1rZXktMzJieXQ="


@pytest.mark.asyncio
async def test_two_page_ab_orders_finalize_atomically_from_frozen_evidence(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument_id = await _seed_account_and_portfolio(factory)
    stable = read_set(
        positions=(position(instrument_id),),
        orders=(
            order("synthetic-order-1", instrument_id=instrument_id),
            order("synthetic-order-2", instrument_id=instrument_id),
        ),
    )

    async with factory() as session, session.begin():
        first = await BrokerSyncRepository(session).admit(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            trigger_kind="AUTOMATIC",
            actor_id="synthetic-worker",
            route="broker-sync",
            idempotency_key="two-page-orders",
            request_hash="1" * 64,
            accepted_at=NOW,
        )
    provider = ScriptedBrokerProvider([(stable, stable)], page_size=1)
    frozen = await freeze_broker_snapshot(
        ACCOUNT_ID,
        provider,
        portfolio_id=PORTFOLIO_ID,
        sync_run_id=first.run.id,
    )
    assert [(call[3], call[4]) for call in provider.calls if call[0] == "orders"] == [
        (None, 100),
        ("1", 100),
        (None, 100),
        ("1", 100),
    ]
    assert [item["phase"] for item in frozen.attempt_traces[0]["source_observations"]].count(
        "orders_a"
    ) == 2
    assert [item["phase"] for item in frozen.attempt_traces[0]["source_observations"]].count(
        "orders_b"
    ) == 2
    async with factory() as session, session.begin():
        await BrokerSyncRepository(session).finalize(frozen, completed_at=NOW)
    async with factory() as session:
        stored_orders = (
            await session.scalars(
                select(BrokerOrderSnapshot)
                .where(BrokerOrderSnapshot.portfolio_snapshot_id == frozen.portfolio_snapshot.id)
                .order_by(BrokerOrderSnapshot.provider_order_id)
            )
        ).all()
    assert [item.provider_order_id for item in stored_orders] == [
        "synthetic-order-1",
        "synthetic-order-2",
    ]

    async with factory() as session, session.begin():
        second = await BrokerSyncRepository(session).admit(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            trigger_kind="AUTOMATIC",
            actor_id="synthetic-worker",
            route="broker-sync",
            idempotency_key="tampered-two-page-orders",
            request_hash="2" * 64,
            accepted_at=NOW + timedelta(seconds=1),
        )
    tampered = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider([(stable, stable)], page_size=1),
        portfolio_id=PORTFOLIO_ID,
        sync_run_id=second.run.id,
    )
    trace = deepcopy(tampered.attempt_traces[0])
    trace["raw_facts"]["orders_b"][1]["provider_order_id"] = "tampered-order"
    tampered = tampered.model_copy(update={"attempt_traces": (trace,)})
    async with factory() as session, session.begin():
        with pytest.raises(BrokerSyncPersistenceError):
            await BrokerSyncRepository(session).finalize(tampered, completed_at=NOW)
        assert await session.scalar(
            select(func.count())
            .select_from(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.sync_run_id == second.run.id)
        ) == 0
        for child in (CashSnapshot, PositionSnapshot, BrokerOrderSnapshot):
            assert await session.scalar(
                select(func.count())
                .select_from(child)
                .where(child.portfolio_snapshot_id == tampered.portfolio_snapshot.id)
            ) == 0


@pytest.mark.asyncio
async def test_snapshot_children_and_terminal_runs_are_append_only(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument_id = await _seed_account_and_portfolio(factory)
    async with factory() as session:
        async with session.begin():
            repository = BrokerSyncRepository(session)
            admission = await repository.admit(
                account_id=ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                trigger_kind="AUTOMATIC",
                actor_id="synthetic-worker",
                route="broker-sync",
                idempotency_key="synthetic-key-immutable",
                request_hash="f" * 64,
                accepted_at=NOW,
            )
            await repository.finalize(
                _sync_result(
                    admission.run.id,
                    _snapshot(instrument_id, READY_SNAPSHOT_ID, "READY"),
                ),
                completed_at=NOW + timedelta(seconds=1),
            )

    async with factory() as session:
        attempt = await session.scalar(
            select(BrokerSyncAttempt).where(BrokerSyncAttempt.sync_run_id == admission.run.id)
        )
        assert attempt is not None
        assert attempt.raw_facts["cash_a"][0]["cash_balance"] == "1000"

    statements = (
        "UPDATE accounts SET status = 'DISABLED' WHERE id = :account_id",
        "UPDATE portfolios SET name = 'Changed' WHERE id = :portfolio_id",
        "DELETE FROM broker_sync_attempts WHERE sync_run_id = "
        "(SELECT sync_run_id FROM portfolio_snapshots WHERE id = :id)",
        "UPDATE portfolio_snapshots SET status = 'PARTIAL' WHERE id = :id",
        "DELETE FROM cash_snapshots WHERE portfolio_snapshot_id = :id",
        "UPDATE position_snapshots SET quantity = 11 WHERE portfolio_snapshot_id = :id",
        "DELETE FROM broker_order_snapshots WHERE portfolio_snapshot_id = :id",
        "UPDATE broker_sync_runs SET status = 'PARTIAL' WHERE id = "
        "(SELECT sync_run_id FROM portfolio_snapshots WHERE id = :id)",
    )
    for statement in statements:
        async with factory() as session:
            with pytest.raises(DBAPIError, match="immutable"):
                await session.execute(
                    text(statement),
                    {
                        "id": READY_SNAPSHOT_ID,
                        "account_id": ACCOUNT_ID,
                        "portfolio_id": PORTFOLIO_ID,
                    },
                )
                await session.commit()
