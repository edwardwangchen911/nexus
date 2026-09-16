"""Atomic PostgreSQL acceptance tests for Task 19 internal-paper fills."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_execution import FIRST_EVALUATION, seed_paper_execution
from nexus.paper.errors import PaperExecutionError
from nexus.paper.execution_service import (
    PaperExecutionHooks,
    advance_internal_paper_order,
    cancel_internal_paper_order,
)
from nexus.paper.notification import parse_notification_snapshot
from nexus.persistence.models.data import (
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.paper import (
    PaperExecutionWindowAuthorityRecord,
    PaperExecutionWindowMemberRecord,
    PaperFillRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
    PaperRequestIdempotencyRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
)
from nexus.persistence.models.runtime import AuditEvent, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


@pytest.fixture(autouse=True)
def _historical_execution_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the historical execution state machine behind an explicit test gate."""
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available",
        lambda _source_policy_id: True,
    )


@pytest.mark.parametrize("legacy_market_policy", (False, True))
@pytest.mark.parametrize(
    "cancel_at",
    (
        FIRST_EVALUATION + timedelta(minutes=20),
        FIRST_EVALUATION + timedelta(hours=7),
    ),
    ids=("ten-fifteen", "end-of-day"),
)
@pytest.mark.asyncio
async def test_order_without_current_execution_authority_never_fills_and_releases_at_cutoff(
    migrated_database_url: str,
    legacy_market_policy: bool,
    cancel_at,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        legacy_market_policy=legacy_market_policy,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available",
        lambda _source_policy_id: False,
    )
    async with factory.begin() as session:
        unavailable = await advance_internal_paper_order(
            session,
            order_id=seed.order_id,
            evaluated_at=seed.evaluation_times[0],
            idempotency_key="unavailable-price-authority",
        )
    async with factory.begin() as session:
        cancelled = await advance_internal_paper_order(
            session,
            order_id=seed.order_id,
            evaluated_at=cancel_at,
            idempotency_key="unavailable-price-authority-cutoff",
        )
    async with factory() as session:
        window_count = int(
            await session.scalar(
                select(func.count())
                .select_from(PaperExecutionWindowAuthorityRecord)
                .where(PaperExecutionWindowAuthorityRecord.order_id == seed.order_id)
            )
            or 0
        )
        fill_count = int(
            await session.scalar(
                select(func.count())
                .select_from(PaperFillRecord)
                .where(PaperFillRecord.order_id == seed.order_id)
            )
            or 0
        )
        idempotency_count = int(
            await session.scalar(
                select(func.count())
                .select_from(PaperRequestIdempotencyRecord)
                .where(PaperRequestIdempotencyRecord.order_id == seed.order_id)
            )
            or 0
        )
        reservation = await session.scalar(
            select(PaperReservationRecord).where(PaperReservationRecord.order_id == seed.order_id)
        )
        assert reservation is not None
        reservation_events = tuple(
            await session.scalars(
                select(PaperReservationEventRecord)
                .where(PaperReservationEventRecord.reservation_id == reservation.id)
                .order_by(PaperReservationEventRecord.event_seq)
            )
        )
        order_events = tuple(
            await session.scalars(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq)
            )
        )

    assert (unavailable.action, unavailable.status) == ("no_data", "submitted")
    assert (cancelled.action, cancelled.status) == ("cancelled", "cancelled")
    assert window_count == 0
    assert fill_count == 0
    assert idempotency_count == 2
    assert [event.event_type for event in reservation_events] == [
        "reserved",
        "released",
    ]
    assert sum(event.amount for event in reservation_events) == reservation.amount * 2
    assert order_events[-1].reason_code == "paper_execution_price_authority_unavailable"
    assert order_events[-1].actor_kind == "task19"


@pytest.mark.asyncio
async def test_first_unquoted_window_fills_and_atomically_reconciles_all_facts(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            result = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-first-window",
            )
        async with factory() as session:
            fill = await session.scalar(
                select(PaperFillRecord).where(PaperFillRecord.order_id == seed.order_id)
            )
            window = await session.scalar(
                select(PaperExecutionWindowAuthorityRecord).where(
                    PaperExecutionWindowAuthorityRecord.order_id == seed.order_id
                )
            )
            member_count = await session.scalar(
                select(func.count())
                .select_from(PaperExecutionWindowMemberRecord)
                .where(PaperExecutionWindowMemberRecord.window_authority_id == window.id)
            )
            order_events = tuple(
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == seed.order_id)
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            )
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )
            position_event = await session.scalar(
                select(PaperPositionAdjustmentEventRecord).where(
                    PaperPositionAdjustmentEventRecord.source_fill_id == fill.id
                )
            )
            transaction = await session.scalar(
                select(PaperLedgerTransactionRecord).where(
                    PaperLedgerTransactionRecord.id == fill.ledger_transaction_id
                )
            )
            entries = tuple(
                await session.scalars(
                    select(PaperLedgerEntryRecord)
                    .where(PaperLedgerEntryRecord.transaction_id == transaction.id)
                    .order_by(PaperLedgerEntryRecord.entry_seq)
                )
            )
            audit_action = await session.scalar(
                select(AuditEvent.action).where(AuditEvent.schema_version == "paper_execution_v1")
            )
            outbox = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.schema_version == "paper_execution_v1"
                )
            )
            actual_cash = await session.scalar(
                select(
                    func.sum(
                        PaperLedgerEntryRecord.debit_amount
                        - PaperLedgerEntryRecord.credit_amount
                    )
                )
                .join(
                    PaperLedgerAccountRecord,
                    PaperLedgerAccountRecord.id
                    == PaperLedgerEntryRecord.ledger_account_id,
                )
                .where(
                    PaperLedgerAccountRecord.paper_account_id == seed.account_id,
                    PaperLedgerAccountRecord.code == "asset.cash.available.usd",
                )
            )

        assert result.action == "filled"
        assert result.status == "filled"
        assert result.filled_quantity == seed.quantity
        assert result.remaining_quantity == 0
        assert fill is not None and window is not None and reservation is not None
        assert fill.quantity == seed.quantity
        assert fill.price == Decimal("100.250000000000")
        assert fill.slippage_bps == 25
        assert fill.execution_window_authority_id == window.id
        assert member_count == 5
        assert [event.to_status for event in order_events] == [
            "proposed",
            "risk_approved",
            "submitted",
            "filled",
        ]
        assert [event.event_type for event in reservation_events] == [
            "reserved",
            "consumed",
            "released",
        ]
        assert sum(event.amount for event in reservation_events[1:]) == reservation.amount
        assert position_event is not None
        assert position_event.quantity_delta == fill.quantity
        assert position_event.cost_delta == fill.notional
        assert transaction is not None and transaction.transaction_type == "fill"
        assert sum(entry.debit_amount for entry in entries) == sum(
            entry.credit_amount for entry in entries
        )
        assert audit_action == "paper.execution.advance.filled"
        assert outbox is not None and outbox.event_type == audit_action
        snapshot = parse_notification_snapshot(outbox.payload)
        assert snapshot.status == "filled"
        assert snapshot.symbol == "TST1"
        assert snapshot.filled_quantity == fill.quantity
        assert snapshot.price.value == fill.price
        assert snapshot.notional == fill.notional
        assert snapshot.fee == fill.fee
        assert snapshot.cash.available == actual_cash
        assert snapshot.position.quantity == position_event.quantity_delta
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_two_windows_partially_fill_then_close_reservation_exactly(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("100"), Decimal("2000")),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            first = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-partial-one",
            )
        async with factory.begin() as session:
            second = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[1],
                idempotency_key="advance-partial-two",
            )
        async with factory() as session:
            fills = tuple(
                await session.scalars(
                    select(PaperFillRecord)
                    .where(PaperFillRecord.order_id == seed.order_id)
                    .order_by(PaperFillRecord.fill_seq)
                )
            )
            windows = tuple(
                await session.scalars(
                    select(PaperExecutionWindowAuthorityRecord)
                    .where(PaperExecutionWindowAuthorityRecord.order_id == seed.order_id)
                    .order_by(PaperExecutionWindowAuthorityRecord.window_seq)
                )
            )
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )

        assert first.action == "partially_filled"
        assert first.filled_quantity == 5
        assert first.remaining_quantity == seed.quantity - 5
        assert second.action == "filled"
        assert second.filled_quantity == seed.quantity
        assert [fill.quantity for fill in fills] == [5, seed.quantity - 5]
        assert [window.window_seq for window in windows] == [1, 2]
        assert len({(window.source, window.feed) for window in windows}) == 1
        assert [event.event_type for event in reservation_events] == [
            "reserved",
            "consumed",
            "consumed",
            "released",
        ]
        assert sum(event.amount for event in reservation_events[1:]) == reservation.amount
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_repeated_partial_fill_events_bind_each_window_to_its_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(
            Decimal("40"),
            Decimal("40"),
            Decimal("40"),
        ),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        results = []
        for index, evaluated_at in enumerate(seed.evaluation_times, start=1):
            async with factory.begin() as session:
                results.append(
                    await advance_internal_paper_order(
                        session,
                        order_id=seed.order_id,
                        evaluated_at=evaluated_at,
                        idempotency_key=f"advance-partial-repeat-{index}",
                    )
                )
        async with factory() as session:
            fill_events = tuple(
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id == seed.order_id,
                        PaperOrderEventRecord.source_fill_id.is_not(None),
                    )
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            )
        assert [result.action for result in results] == [
            "partially_filled",
            "partially_filled",
            "partially_filled",
        ]
        assert [event.to_status for event in fill_events] == [
            "partially_filled",
            "partially_filled",
            "partially_filled",
        ]
        assert len({event.source_fill_id for event in fill_events}) == 3
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_missing_first_window_is_noop_then_cancels_at_ten_fifteen(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url, window_bar_volumes=())
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            waiting = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=FIRST_EVALUATION,
                idempotency_key="advance-missing-wait",
            )
        async with factory.begin() as session:
            cancelled = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=FIRST_EVALUATION + timedelta(minutes=20),
                idempotency_key="advance-missing-cancel",
            )
        async with factory() as session:
            fill_count = await session.scalar(
                select(func.count())
                .select_from(PaperFillRecord)
                .where(PaperFillRecord.order_id == seed.order_id)
            )
            window_count = await session.scalar(
                select(func.count())
                .select_from(PaperExecutionWindowAuthorityRecord)
                .where(PaperExecutionWindowAuthorityRecord.order_id == seed.order_id)
            )
            last_event = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            execution_audits = tuple(
                await session.scalars(
                    select(AuditEvent.action)
                    .where(AuditEvent.schema_version == "paper_execution_v1")
                    .order_by(AuditEvent.action)
                )
            )
        assert waiting.action == "no_data"
        assert waiting.status == "submitted"
        assert cancelled.action == "cancelled"
        assert cancelled.status == "cancelled"
        assert fill_count == window_count == 0
        assert last_event is not None
        assert last_event.reason_code == "missing_fill_data"
        assert execution_audits == (
            "paper.execution.advance.cancelled",
            "paper.execution.advance.no_data",
        )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_half_day_calendar_close_cancels_only_the_unfilled_remainder(
    migrated_database_url: str,
) -> None:
    half_day_close = FIRST_EVALUATION + timedelta(hours=3, minutes=5)
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("40"),),
        market_close_at=half_day_close,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            partial = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-half-day-partial",
            )
        async with factory.begin() as session:
            cancelled = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=half_day_close,
                idempotency_key="advance-half-day-close",
            )
        async with factory() as session:
            last_event = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            closed_amount = await session.scalar(
                select(func.sum(PaperReservationEventRecord.amount)).where(
                    PaperReservationEventRecord.reservation_id == reservation.id,
                    PaperReservationEventRecord.event_type.in_(("consumed", "released")),
                )
            )
            fills = tuple(
                await session.scalars(
                    select(PaperFillRecord)
                    .where(PaperFillRecord.order_id == seed.order_id)
                    .order_by(PaperFillRecord.fill_seq)
                )
            )
            outbox = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == seed.order_id,
                    OutboxEvent.event_type == "paper.execution.advance.cancelled",
                )
            )
            actual_cash = await session.scalar(
                select(
                    func.sum(
                        PaperLedgerEntryRecord.debit_amount
                        - PaperLedgerEntryRecord.credit_amount
                    )
                )
                .join(
                    PaperLedgerAccountRecord,
                    PaperLedgerAccountRecord.id
                    == PaperLedgerEntryRecord.ledger_account_id,
                )
                .where(
                    PaperLedgerAccountRecord.paper_account_id == seed.account_id,
                    PaperLedgerAccountRecord.code == "asset.cash.available.usd",
                )
            )
            actual_position = await session.scalar(
                select(func.sum(PaperPositionAdjustmentEventRecord.quantity_delta)).where(
                    PaperPositionAdjustmentEventRecord.account_id == seed.account_id,
                    PaperPositionAdjustmentEventRecord.instrument_id == seed.instrument_id,
                )
            )
        assert partial.action == "partially_filled"
        assert cancelled.action == "cancelled"
        assert cancelled.filled_quantity == partial.filled_quantity
        assert last_event is not None
        assert last_event.reason_code == "paper_eod_remainder"
        assert closed_amount == reservation.amount
        assert outbox is not None
        snapshot = parse_notification_snapshot(outbox.payload)
        total_quantity = sum((fill.quantity for fill in fills), start=Decimal(0))
        total_notional = sum((fill.notional for fill in fills), start=Decimal(0))
        total_fee = sum((fill.fee for fill in fills), start=Decimal(0))
        assert snapshot.status == "cancelled"
        assert snapshot.filled_quantity == total_quantity
        assert snapshot.remaining_quantity == seed.quantity - total_quantity
        assert snapshot.price.value == total_notional / total_quantity
        assert snapshot.notional == total_notional
        assert snapshot.fee == total_fee
        assert snapshot.cash.available == actual_cash
        assert snapshot.position.quantity == actual_position
        assert snapshot.risk_reason == "交易时段结束，未成交部分已取消。"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_reservation_that_cannot_fund_one_share_cancels_without_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        closes=(Decimal("20000"),),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            result = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-insufficient-reservation",
            )
        async with factory() as session:
            fill_count = await session.scalar(
                select(func.count())
                .select_from(PaperFillRecord)
                .where(PaperFillRecord.order_id == seed.order_id)
            )
            window_count = await session.scalar(
                select(func.count())
                .select_from(PaperExecutionWindowAuthorityRecord)
                .where(PaperExecutionWindowAuthorityRecord.order_id == seed.order_id)
            )
            last_event = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
        assert result.action == "cancelled"
        assert fill_count == 0
        assert window_count == 1
        assert last_event is not None
        assert last_event.reason_code == "paper_reservation_insufficient"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_same_idempotency_key_replays_and_different_request_conflicts(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            first = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-idempotent",
            )
        async with factory.begin() as session:
            replay = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-idempotent",
            )
        with pytest.raises(PaperExecutionError) as captured:
            async with factory.begin() as session:
                await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[0] + timedelta(seconds=1),
                    idempotency_key="advance-idempotent",
                )
        async with factory() as session:
            fill_count = await session.scalar(
                select(func.count())
                .select_from(PaperFillRecord)
                .where(PaperFillRecord.order_id == seed.order_id)
            )
        assert replay == first
        assert captured.value.code == "PAPER_EXECUTION_IDEMPOTENCY_CONFLICT"
        assert fill_count == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_worker_request_cannot_move_execution_time_backwards(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("10"), Decimal("10")),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    first_evaluated_at = seed.evaluation_times[1] + timedelta(minutes=5)
    try:
        async with factory.begin() as session:
            first = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=first_evaluated_at,
                idempotency_key="advance-monotonic-first",
            )

        with pytest.raises(PaperExecutionError) as captured:
            async with factory.begin() as session:
                await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[1],
                    idempotency_key="advance-monotonic-backward",
                )

        async with factory.begin() as session:
            second = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=first_evaluated_at + timedelta(minutes=5),
                idempotency_key="advance-monotonic-second",
            )
        async with factory.begin() as session:
            replay = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=first_evaluated_at,
                idempotency_key="advance-monotonic-first",
            )

        async with factory() as session:
            windows = tuple(
                await session.scalars(
                    select(PaperExecutionWindowAuthorityRecord)
                    .where(PaperExecutionWindowAuthorityRecord.order_id == seed.order_id)
                    .order_by(PaperExecutionWindowAuthorityRecord.window_seq)
                )
            )

        assert captured.value.code == "PAPER_EXECUTION_CONFLICT"
        assert first.action == second.action == "no_capacity"
        assert replay == first
        assert [window.evaluated_at for window in windows] == [
            first_evaluated_at,
            first_evaluated_at + timedelta(minutes=5),
        ]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_eight_workers_with_one_key_converge_to_one_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def invoke():
        async with factory.begin() as session:
            return await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-concurrent",
            )

    try:
        results = await asyncio.gather(*(invoke() for _ in range(8)))
        async with factory() as session:
            fill_count = await session.scalar(
                select(func.count())
                .select_from(PaperFillRecord)
                .where(PaperFillRecord.order_id == seed.order_id)
            )
            request_count = await session.scalar(
                select(func.count())
                .select_from(PaperRequestIdempotencyRecord)
                .where(PaperRequestIdempotencyRecord.order_id == seed.order_id)
            )
        assert len({result.result_hash for result in results}) == 1
        assert len({result.fill_id for result in results}) == 1
        assert fill_count == request_count == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_user_cancel_replays_and_releases_the_whole_reservation(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            first = await cancel_internal_paper_order(
                session,
                order_id=seed.order_id,
                expected_event_seq=3,
                requested_at=FIRST_EVALUATION - timedelta(minutes=1),
                idempotency_key="cancel-idempotent",
            )
        async with factory.begin() as session:
            replay = await cancel_internal_paper_order(
                session,
                order_id=seed.order_id,
                expected_event_seq=3,
                requested_at=FIRST_EVALUATION,
                idempotency_key="cancel-idempotent",
            )
        async with factory() as session:
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )
            last_event = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            outbox = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == seed.order_id,
                    OutboxEvent.event_type == "paper.execution.cancelled",
                )
            )
        assert first.replayed is False
        assert replay.replayed is True
        assert replay.result_hash == first.result_hash
        assert replay.result_event_seq == first.result_event_seq
        assert first.status == "cancelled"
        assert first.filled_quantity == 0
        assert first.unfilled_quantity == seed.quantity
        assert [event.event_type for event in reservation_events] == [
            "reserved",
            "released",
        ]
        assert last_event is not None
        assert last_event.actor_kind == "user"
        assert last_event.reason_code == "paper_user_cancelled"
        assert outbox is not None
        snapshot = parse_notification_snapshot(outbox.payload)
        assert snapshot.status == "cancelled"
        assert snapshot.filled_quantity == 0
        assert snapshot.remaining_quantity == seed.quantity
        assert snapshot.price.kind == "reference"
        assert snapshot.notional == 0
        assert snapshot.fee is None
        assert snapshot.risk_reason == "用户取消了订单。"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_duplicate_user_cancel_reports_one_locked_replay(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def invoke():
        async with factory.begin() as session:
            return await cancel_internal_paper_order(
                session,
                order_id=seed.order_id,
                expected_event_seq=3,
                requested_at=FIRST_EVALUATION - timedelta(minutes=1),
                idempotency_key="cancel-concurrent-idempotent",
            )

    try:
        results = await asyncio.gather(invoke(), invoke())
        async with factory() as session:
            request_count = await session.scalar(
                select(func.count())
                .select_from(PaperRequestIdempotencyRecord)
                .where(
                    PaperRequestIdempotencyRecord.order_id == seed.order_id,
                    PaperRequestIdempotencyRecord.operation == "cancel",
                )
            )
            cancelled_event_count = await session.scalar(
                select(func.count())
                .select_from(PaperOrderEventRecord)
                .where(
                    PaperOrderEventRecord.order_id == seed.order_id,
                    PaperOrderEventRecord.to_status == "cancelled",
                )
            )
        assert sorted(result.replayed for result in results) == [False, True]
        assert len({result.result_hash for result in results}) == 1
        assert len({result.result_event_seq for result in results}) == 1
        assert request_count == cancelled_event_count == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_worker_and_user_cancel_race_has_one_consistent_terminal_outcome(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def advance():
        async with factory.begin() as session:
            return await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-race",
            )

    async def cancel():
        async with factory.begin() as session:
            return await cancel_internal_paper_order(
                session,
                order_id=seed.order_id,
                expected_event_seq=3,
                requested_at=seed.evaluation_times[0],
                idempotency_key="cancel-race",
            )

    try:
        outcomes = await asyncio.gather(advance(), cancel(), return_exceptions=True)
        async with factory() as session:
            fill_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperFillRecord)
                    .where(PaperFillRecord.order_id == seed.order_id)
                )
                or 0
            )
            last_event = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == seed.order_id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            released_or_consumed = await session.scalar(
                select(func.sum(PaperReservationEventRecord.amount)).where(
                    PaperReservationEventRecord.reservation_id == reservation.id,
                    PaperReservationEventRecord.event_type.in_(("consumed", "released")),
                )
            )
        assert last_event is not None and reservation is not None
        assert last_event.to_status in {"filled", "cancelled"}
        assert fill_count == (1 if last_event.to_status == "filled" else 0)
        assert released_or_consumed == reservation.amount
        assert sum(isinstance(outcome, PaperExecutionError) for outcome in outcomes) <= 1
    finally:
        await factory.kw["bind"].dispose()


_FAULT_PHASES = (
    "window_persisted",
    "ledger_persisted",
    "fill_persisted",
    "reservation_persisted",
    "position_persisted",
    "order_event_persisted",
    "idempotency_persisted",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_phase", _FAULT_PHASES)
async def test_failure_at_any_execution_phase_rolls_back_the_whole_fill(
    migrated_database_url: str,
    fail_phase: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    def fail_at(phase: str) -> None:
        if phase == fail_phase:
            raise RuntimeError("injected Task 19 failure")

    try:
        with pytest.raises(RuntimeError, match="injected Task 19 failure"):
            async with factory.begin() as session:
                await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[0],
                    idempotency_key=f"advance-fault-{fail_phase}",
                    hooks=PaperExecutionHooks(after_phase=fail_at),
                )
        async with factory() as session:
            count_values: list[int] = []
            for model in (
                PaperExecutionWindowAuthorityRecord,
                PaperFillRecord,
                PaperPositionAdjustmentEventRecord,
                PaperRequestIdempotencyRecord,
            ):
                scope = (
                    model.order_id == seed.order_id
                    if hasattr(model, "order_id")
                    else model.account_id == seed.account_id
                )
                count_values.append(
                    int(
                        await session.scalar(select(func.count()).select_from(model).where(scope))
                        or 0
                    )
                )
            counts = tuple(count_values)
            order_event_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == seed.order_id)
                )
                or 0
            )
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == seed.order_id
                )
            )
            reservation_event_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                )
                or 0
            )
            order_ledger_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperLedgerTransactionRecord)
                    .where(PaperLedgerTransactionRecord.order_id == seed.order_id)
                )
                or 0
            )
            execution_audit_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.schema_version == "paper_execution_v1")
                )
                or 0
            )
            execution_outbox_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.schema_version == "paper_execution_v1")
                )
                or 0
            )
        assert counts == (0, 0, 0, 0)
        assert order_event_count == 3
        assert reservation_event_count == 1
        assert order_ledger_count == 1
        assert execution_audit_count == execution_outbox_count == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_database_rejects_balanced_but_unrelated_entries_appended_to_a_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-ledger-seal",
            )
        with pytest.raises(DBAPIError, match="ledger entries are inconsistent"):
            async with factory.begin() as session:
                fill = await session.scalar(
                    select(PaperFillRecord).where(PaperFillRecord.order_id == seed.order_id)
                )
                await session.execute(
                    text(
                        "INSERT INTO paper_ledger_entries "
                        "(id, transaction_id, account_id, entry_seq, ledger_account_id, "
                        "account_code, currency, debit_amount, credit_amount) "
                        "SELECT '66000000-0000-0000-0000-000000000001', :transaction_id, "
                        ":account_id, 3, id, code, 'USD', 1, 0 "
                        "FROM paper_ledger_accounts WHERE paper_account_id = :account_id "
                        "AND code = 'asset.cash.available.usd'"
                    ),
                    {
                        "transaction_id": fill.ledger_transaction_id,
                        "account_id": seed.account_id,
                    },
                )
                await session.execute(
                    text(
                        "INSERT INTO paper_ledger_entries "
                        "(id, transaction_id, account_id, entry_seq, ledger_account_id, "
                        "account_code, currency, debit_amount, credit_amount) "
                        "SELECT '66000000-0000-0000-0000-000000000002', :transaction_id, "
                        ":account_id, 4, id, code, 'USD', 0, 1 "
                        "FROM paper_ledger_accounts WHERE paper_account_id = :account_id "
                        "AND code = 'equity.contributed_capital.usd'"
                    ),
                    {
                        "transaction_id": fill.ledger_transaction_id,
                        "account_id": seed.account_id,
                    },
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_database_rejects_a_second_position_event_for_one_fill(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            result = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-position-seal",
            )
        with pytest.raises(DBAPIError):
            async with factory.begin() as session:
                session.add(
                    PaperPositionAdjustmentEventRecord(
                        account_id=seed.account_id,
                        instrument_id=seed.instrument_id,
                        event_seq=2,
                        event_type="fill",
                        quantity_delta=Decimal("1"),
                        cost_delta=Decimal("1"),
                        source_fill_id=result.fill_id,
                        occurred_at=seed.evaluation_times[0],
                        payload_hash="e" * 64,
                    )
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_database_rejects_terminal_order_with_open_reservation(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url, window_bar_volumes=())
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        with pytest.raises(DBAPIError, match="reservation must be fully closed"):
            async with factory.begin() as session:
                session.add(
                    PaperOrderEventRecord(
                        order_id=seed.order_id,
                        account_id=seed.account_id,
                        event_seq=4,
                        from_status="submitted",
                        to_status="cancelled",
                        actor_kind="task19",
                        reason_code="INVALID_UNCLOSED_CANCEL",
                        occurred_at=FIRST_EVALUATION,
                        payload_hash="f" * 64,
                        source_fill_id=None,
                    )
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_database_rejects_switching_feed_after_first_frozen_window(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(Decimal("2000"), Decimal("2000")),
        feeds=("fixture", "alternate"),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-freeze-feed",
            )
        with pytest.raises(DBAPIError, match="source scope cannot switch"):
            async with factory.begin() as session:
                authority, manifest = (
                    await session.execute(
                        select(PriceBarSetAuthority, PriceSeriesManifest)
                        .join(
                            PriceSeriesManifest,
                            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                        )
                        .where(PriceSeriesManifest.feed == "alternate")
                    )
                ).one()
                bars = tuple(
                    await session.scalars(
                        select(PriceBarRecord)
                        .where(PriceBarRecord.price_series_manifest_id == manifest.id)
                        .order_by(PriceBarRecord.observed_at)
                    )
                )
                root = PaperExecutionWindowAuthorityRecord(
                    id=UUID("66000000-0000-0000-0000-000000000010"),
                    order_id=seed.order_id,
                    account_id=seed.account_id,
                    instrument_id=seed.instrument_id,
                    window_seq=2,
                    window_start_at=bars[0].observed_at,
                    window_end_at=bars[0].observed_at + timedelta(minutes=5),
                    evaluated_at=seed.evaluation_times[1],
                    data_available_at=authority.available_at,
                    source_authority_id=authority.id,
                    source=manifest.source,
                    source_content_hash=authority.content_hash,
                    feed=manifest.feed,
                    delivery_mode=manifest.delivery_mode,
                    vwap=Decimal("100"),
                    volume=Decimal("10000"),
                    capacity=Decimal("100"),
                    quote_id=None,
                    quote_revision=None,
                    bid=None,
                    ask=None,
                    half_spread=Decimal(0),
                    content_hash="d" * 64,
                )
                session.add(root)
                session.add_all(
                    PaperExecutionWindowMemberRecord(
                        window_authority_id=root.id,
                        bar_id=bar.id,
                        member_seq=index,
                    )
                    for index, bar in enumerate(bars, start=1)
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_qualified_same_source_quote_uses_half_spread_and_ten_bps(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url, with_quote=True)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            result = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-quoted-window",
            )
        async with factory() as session:
            fill = await session.scalar(
                select(PaperFillRecord).where(PaperFillRecord.order_id == seed.order_id)
            )
            window = await session.get(
                PaperExecutionWindowAuthorityRecord,
                fill.execution_window_authority_id if fill is not None else None,
            )
        assert result.action == "filled"
        assert fill is not None and window is not None
        assert fill.slippage_bps == 10
        assert fill.half_spread == Decimal("0.100000000000")
        assert fill.price == Decimal("100.200000000000")
        assert window.quote_id is not None
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_sell_fill_uses_prior_average_cost_and_posts_realized_loss(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    sell_order_id = UUID("66000000-0000-0000-0000-000000000100")
    sell_reservation_id = UUID("66000000-0000-0000-0000-000000000101")
    try:
        async with factory.begin() as session:
            await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-buy-before-sell",
            )
        async with factory.begin() as session:
            buy_order = await session.get(PaperOrderRecord, seed.order_id)
            session.add(
                PaperOrderRecord(
                    id=sell_order_id,
                    batch_id=buy_order.batch_id,
                    account_id=seed.account_id,
                    strategy_decision_id=buy_order.strategy_decision_id,
                    client_order_id="task19-sell-fixture",
                    instrument_id=seed.instrument_id,
                    side="sell",
                    quantity=Decimal("5"),
                    target_weight=Decimal("0"),
                    reference_close=Decimal("100"),
                    worst_price=Decimal("99"),
                    risk_outcome="approved",
                )
            )
            await session.flush()
            session.add_all(
                PaperOrderEventRecord(
                    id=UUID(int=0x66000000000000000000000000000200 + sequence),
                    order_id=sell_order_id,
                    account_id=seed.account_id,
                    event_seq=sequence,
                    from_status=source,
                    to_status=target,
                    actor_kind="task18",
                    reason_code=reason,
                    occurred_at=FIRST_EVALUATION - timedelta(hours=1),
                    payload_hash=f"{sequence}" * 64,
                    source_fill_id=None,
                )
                for sequence, (source, target, reason) in enumerate(
                    (
                        (None, "proposed", "PAPER_ORDER_PROPOSED"),
                        ("proposed", "risk_approved", "PAPER_RISK_APPROVED"),
                        ("risk_approved", "submitted", "PAPER_ORDER_SUBMITTED"),
                    ),
                    start=1,
                )
            )
            session.add(
                PaperReservationRecord(
                    id=sell_reservation_id,
                    order_id=sell_order_id,
                    account_id=seed.account_id,
                    ledger_transaction_id=None,
                    reservation_type="position",
                    currency=None,
                    instrument_id=seed.instrument_id,
                    amount=Decimal("5"),
                )
            )
            await session.flush()
            session.add(
                PaperReservationEventRecord(
                    id=UUID("66000000-0000-0000-0000-000000000102"),
                    reservation_id=sell_reservation_id,
                    account_id=seed.account_id,
                    event_seq=1,
                    event_type="reserved",
                    amount=Decimal("5"),
                    occurred_at=FIRST_EVALUATION - timedelta(hours=1),
                    payload_hash="a" * 64,
                    source_fill_id=None,
                )
            )
        async with factory.begin() as session:
            result = await advance_internal_paper_order(
                session,
                order_id=sell_order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="advance-sell-average-cost",
            )
        async with factory() as session:
            sell_fill = await session.scalar(
                select(PaperFillRecord).where(PaperFillRecord.order_id == sell_order_id)
            )
            sell_position = await session.scalar(
                select(PaperPositionAdjustmentEventRecord).where(
                    PaperPositionAdjustmentEventRecord.source_fill_id == sell_fill.id
                )
            )
            realized = await session.scalar(
                select(
                    func.coalesce(
                        func.sum(
                            PaperLedgerEntryRecord.credit_amount
                            - PaperLedgerEntryRecord.debit_amount
                        ),
                        0,
                    )
                ).where(
                    PaperLedgerEntryRecord.account_id == seed.account_id,
                    PaperLedgerEntryRecord.account_code == "income.realized_pnl.usd",
                )
            )
        assert result.action == "filled"
        assert sell_fill is not None and sell_position is not None
        assert sell_fill.quantity == 5
        assert sell_fill.price == Decimal("99.750000000000")
        assert sell_position.quantity_delta == -5
        assert sell_position.cost_delta == Decimal("-501.250000000000")
        assert realized == Decimal("-2.500000000000")
    finally:
        await factory.kw["bind"].dispose()
