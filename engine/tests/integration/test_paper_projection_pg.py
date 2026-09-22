import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import seed_paper_batch
from helpers.paper_performance import seed_paper_performance
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
)
from nexus.risk.canonical import (
    canonical_paper_ledger_transaction_hash,
    canonical_paper_projection_hash,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")


@pytest.fixture(autouse=True)
def _historical_order_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep legacy projection mechanics explicit behind the historical test gate."""
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available",
        lambda: True,
    )


async def _load_seeded_projection(database_url: str):
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            return await load_paper_projection(
                session,
                account_id=PAPER_ACCOUNT_ID,
                as_of=datetime(2026, 8, 26, 1, tzinfo=UTC),
            )
    finally:
        await engine.dispose()


def test_seeded_paper_account_loads_as_one_reconciled_projection(
    migrated_database_url: str,
) -> None:
    projection = asyncio.run(_load_seeded_projection(migrated_database_url))

    assert projection.account_id == PAPER_ACCOUNT_ID
    assert projection.portfolio_id == PORTFOLIO_ID
    assert projection.available_cash == Decimal("10000")
    assert projection.reserved_cash == Decimal("0")
    assert projection.positions == ()
    assert projection.open_orders == ()
    assert projection.nav == Decimal("10000")
    assert projection.peak_nav == Decimal("10000")
    assert projection.drawdown_state == "unlocked"
    assert projection.projection_hash == canonical_paper_projection_hash(projection)


@pytest.mark.asyncio
async def test_projection_after_published_close_includes_later_ledger_fact(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "nexus.paper.performance.current_paper_performance_price_authority_available",
        lambda: True,
    )
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    later = seed.as_of + timedelta(minutes=5)
    transaction_id = UUID("66000000-0000-0000-0000-000000000101")
    entries = (
        LedgerEntry(
            entry_id=UUID("66000000-0000-0000-0000-000000000102"),
            entry_seq=1,
            account_code="asset.cash.available.usd",
            currency="USD",
            debit_amount=Decimal("1"),
            credit_amount=Decimal("0"),
        ),
        LedgerEntry(
            entry_id=UUID("66000000-0000-0000-0000-000000000103"),
            entry_seq=2,
            account_code="income.dividend.usd",
            currency="USD",
            debit_amount=Decimal("0"),
            credit_amount=Decimal("1"),
        ),
    )
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=seed.execution.account_id,
        transaction_type="adjustment",
        reference_type="adjustment",
        reference_hash="b" * 64,
        occurred_at=later,
        entries=entries,
        transaction_hash="0" * 64,
    )
    transaction = draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )
    try:
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session, account_id=seed.execution.account_id, as_of=seed.as_of
            )
        async with sessions.begin() as session:
            accounts = {
                row.code: row
                for row in await session.scalars(
                    select(PaperLedgerAccountRecord).where(
                        PaperLedgerAccountRecord.paper_account_id == seed.execution.account_id
                    )
                )
            }
            session.add(
                PaperLedgerTransactionRecord(
                    id=transaction_id,
                    account_id=seed.execution.account_id,
                    order_id=seed.execution.order_id,
                    transaction_type="adjustment",
                    reference_type="adjustment",
                    reference_hash=transaction.reference_hash,
                    occurred_at=later,
                    transaction_hash=transaction.transaction_hash,
                )
            )
            await session.flush()
            session.add_all(
                PaperLedgerEntryRecord(
                    id=entry.entry_id,
                    transaction_id=transaction_id,
                    account_id=seed.execution.account_id,
                    ledger_account_id=accounts[entry.account_code].id,
                    account_code=entry.account_code,
                    entry_seq=entry.entry_seq,
                    currency="USD",
                    debit_amount=entry.debit_amount,
                    credit_amount=entry.credit_amount,
                )
                for entry in entries
            )
        async with sessions() as session:
            projection = await load_paper_projection(
                session, account_id=seed.execution.account_id, as_of=later
            )
        assert (
            projection.available_cash + projection.reserved_cash
            == publication.performance.cash + Decimal("1")
        )
        assert projection.positions[0].quantity == seed.execution.quantity
        assert projection.nav == publication.performance.nav + Decimal("1")
        assert projection.peak_nav == projection.nav
    finally:
        await engine.dispose()


async def _load_before_first_order_event(database_url: str):
    seed = await seed_paper_batch(database_url)
    boundary = datetime.now(UTC) + timedelta(seconds=1)
    request_time = boundary + timedelta(days=1)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-projection-business-time",
                request_time=request_time,
            )
        async with sessions.begin() as session:
            return await load_paper_projection(
                session,
                account_id=seed.account_id,
                as_of=boundary,
            )
    finally:
        await engine.dispose()


def test_projection_uses_first_business_event_time_not_database_created_at(
    migrated_database_url: str,
) -> None:
    projection = asyncio.run(_load_before_first_order_event(migrated_database_url))

    assert projection.available_cash == Decimal("10000")
    assert projection.reserved_cash == Decimal("0")
    assert projection.open_orders == ()
