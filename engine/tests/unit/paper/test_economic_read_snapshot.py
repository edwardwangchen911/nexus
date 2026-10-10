"""Order projections are reusable only inside the exact immutable read transaction."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from nexus.paper.economic_cycle import EconomicLaneRef
from nexus.paper.economic_read import _EconomicLaneReadService
from nexus.paper.read_service import PaperReadService


def service(session, snapshot):
    reader = _EconomicLaneReadService(
        session,
        datetime.now(UTC),
        ref=EconomicLaneRef(uuid4(), "incumbent"),
        expected_database="nexus_test_example",
    )
    # The normal producer obtains this token only after PostgreSQL confirms RR READ ONLY.
    reader._snapshot_transaction = snapshot
    return reader


@pytest.mark.asyncio
async def test_orders_are_validated_once_in_the_same_read_snapshot(monkeypatch):
    transaction = object()
    session = Mock()
    session.get_transaction.return_value = transaction
    read = AsyncMock(return_value=())
    monkeypatch.setattr(PaperReadService, "_orders_for_cycle", read)
    reader = service(session, transaction)
    cycle = uuid4()
    await reader._orders_for_cycle(cycle)
    await reader._orders_for_cycle(cycle)
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_transaction_change_never_reuses_previous_order_facts(monkeypatch):
    transaction = object()
    session = Mock()
    session.get_transaction.return_value = transaction
    read = AsyncMock(return_value=())
    monkeypatch.setattr(PaperReadService, "_orders_for_cycle", read)
    reader = service(session, transaction)
    cycle = uuid4()
    await reader._orders_for_cycle(cycle)
    session.get_transaction.return_value = object()
    await reader._orders_for_cycle(cycle)
    assert read.await_count == 2


@pytest.mark.asyncio
async def test_no_readonly_proof_or_new_reader_never_reuses_orders(monkeypatch):
    transaction = object()
    session = Mock()
    session.get_transaction.return_value = transaction
    read = AsyncMock(return_value=())
    monkeypatch.setattr(PaperReadService, "_orders_for_cycle", read)
    cycle = uuid4()
    unproven = service(session, None)
    await unproven._orders_for_cycle(cycle)
    await unproven._orders_for_cycle(cycle)
    first, second = service(session, transaction), service(session, transaction)
    await first._orders_for_cycle(cycle)
    await second._orders_for_cycle(cycle)
    assert read.await_count == 4


@pytest.mark.asyncio
async def test_failed_validation_is_not_cached(monkeypatch):
    transaction = object()
    session = Mock()
    session.get_transaction.return_value = transaction
    read = AsyncMock(side_effect=[ValueError("UNKNOWN"), ()])
    monkeypatch.setattr(PaperReadService, "_orders_for_cycle", read)
    reader = service(session, transaction)
    cycle = uuid4()
    with pytest.raises(ValueError, match="UNKNOWN"):
        await reader._orders_for_cycle(cycle)
    await reader._orders_for_cycle(cycle)
    assert read.await_count == 2
