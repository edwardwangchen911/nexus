from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import NO_CHANGE_INSTRUMENT, seed_paper_batch
from helpers.paper_execution import FIRST_EVALUATION, seed_paper_execution
from nexus.paper.agent_cycle import AgentPaperCycleError, advance_agent_paper_cycle
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    RiskNoChangeEvaluationRecord,
    RiskPolicyRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


async def _policy_fingerprint(session: AsyncSession) -> tuple[tuple[object, ...], ...]:
    rows = await session.execute(
        select(
            RiskPolicyRecord.id,
            RiskPolicyRecord.content_hash,
            RiskPolicyRecord.single_name_limit,
            RiskPolicyRecord.sector_limit,
            RiskPolicyRecord.minimum_cash_ratio,
            RiskPolicyRecord.drawdown_lock_ratio,
            RiskPolicyRecord.drawdown_recovery_ratio,
        ).order_by(RiskPolicyRecord.id)
    )
    return tuple(tuple(row) for row in rows)


@pytest.mark.asyncio
async def test_agent_cycle_rejects_non_internal_account_without_persisting_a_claim(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            with pytest.raises(AgentPaperCycleError, match="AGENT_PAPER_ACCOUNT_INVALID"):
                await advance_agent_paper_cycle(
                    session,
                    account_id=uuid4(),
                    decision_time=datetime(2026, 8, 26, 21, tzinfo=UTC),
                    idempotency_key="reject-non-internal-account",
                )
            await session.rollback()
        async with sessions() as session:
            assert await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            ) == 0
            assert await session.scalar(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.actor_id.like("local-agent-paper-cycle:%"))
            ) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "case",
    [
        "buy_partial_then_full",
        "sell",
        "hold_no_order",
        "risk_reduced",
        "risk_rejected",
        "market_unavailable_no_fill",
    ],
)
@pytest.mark.asyncio
async def test_controlled_loop_preserves_policy_and_reaches_honest_outcomes(
    case: str,
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available", lambda _: True
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            before = await _policy_fingerprint(session)

        if case == "market_unavailable_no_fill":
            seed = await seed_paper_execution(
                migrated_database_url,
                window_bar_volumes=(),
            )
            async with sessions.begin() as session:
                unavailable = await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=FIRST_EVALUATION,
                    idempotency_key="controlled-market-unavailable",
                )
            assert unavailable.action == "no_data"
            assert unavailable.fill_id is None
            async with sessions() as session:
                assert await session.scalar(
                    select(func.count())
                    .select_from(PaperFillRecord)
                    .where(PaperFillRecord.order_id == seed.order_id)
                ) == 0
        elif case == "buy_partial_then_full":
            seed = await seed_paper_execution(
                migrated_database_url,
                window_bar_volumes=(Decimal("100"), Decimal("2000")),
            )
            async with sessions.begin() as session:
                partial = await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[0],
                    idempotency_key="controlled-buy-partial",
                )
            async with sessions.begin() as session:
                filled = await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[1],
                    idempotency_key="controlled-buy-full",
                )
            assert (partial.action, filled.action) == ("partially_filled", "filled")
        elif case == "sell":
            seed = await seed_paper_execution(migrated_database_url)
            async with sessions.begin() as session:
                await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=seed.evaluation_times[0],
                    idempotency_key="controlled-buy-before-sell",
                )
            sell_order_id = UUID("67000000-0000-0000-0000-000000000100")
            reservation_id = UUID("67000000-0000-0000-0000-000000000101")
            async with sessions.begin() as session:
                buy = await session.get(PaperOrderRecord, seed.order_id)
                assert buy is not None
                session.add(
                    PaperOrderRecord(
                        id=sell_order_id,
                        batch_id=buy.batch_id,
                        account_id=seed.account_id,
                        strategy_decision_id=buy.strategy_decision_id,
                        client_order_id="controlled-agent-sell",
                        instrument_id=seed.instrument_id,
                        side="sell",
                        quantity=Decimal("1"),
                        target_weight=Decimal("0"),
                        reference_close=Decimal("100"),
                        worst_price=Decimal("99"),
                        risk_outcome="approved",
                    )
                )
                await session.flush()
                session.add_all(
                    PaperOrderEventRecord(
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
                        id=reservation_id,
                        order_id=sell_order_id,
                        account_id=seed.account_id,
                        ledger_transaction_id=None,
                        reservation_type="position",
                        currency=None,
                        instrument_id=seed.instrument_id,
                        amount=Decimal("1"),
                    )
                )
                await session.flush()
                session.add(
                    PaperReservationEventRecord(
                        reservation_id=reservation_id,
                        account_id=seed.account_id,
                        event_seq=1,
                        event_type="reserved",
                        amount=Decimal("1"),
                        occurred_at=FIRST_EVALUATION - timedelta(hours=1),
                        payload_hash="a" * 64,
                        source_fill_id=None,
                    )
                )
            async with sessions.begin() as session:
                sold = await advance_internal_paper_order(
                    session,
                    order_id=sell_order_id,
                    evaluated_at=seed.evaluation_times[0],
                    idempotency_key="controlled-agent-sell-fill",
                )
            assert sold.action == "filled"
            async with sessions() as session:
                adjustment = await session.scalar(
                    select(PaperPositionAdjustmentEventRecord)
                    .join(
                        PaperFillRecord,
                        PaperFillRecord.id
                        == PaperPositionAdjustmentEventRecord.source_fill_id,
                    )
                    .where(PaperFillRecord.order_id == sell_order_id)
                )
                assert adjustment is not None and adjustment.quantity_delta == Decimal("-1")
        else:
            seed = await seed_paper_batch(
                migrated_database_url, include_no_change=case == "hold_no_order"
            )
            async with sessions.begin() as session:
                result = await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key=f"controlled-{case}",
                    request_time=seed.request_time,
                )
            if case == "hold_no_order":
                async with sessions() as session:
                    held = await session.scalar(
                        select(RiskNoChangeEvaluationRecord).where(
                            RiskNoChangeEvaluationRecord.instrument_id == NO_CHANGE_INSTRUMENT[0]
                        )
                    )
                    order = await session.scalar(
                        select(PaperOrderRecord).where(
                            PaperOrderRecord.instrument_id == NO_CHANGE_INSTRUMENT[0]
                        )
                    )
                assert held is not None and order is None
            else:
                expected = "reduced" if case == "risk_reduced" else "rejected"
                assert expected in {item.outcome for item in result.evaluations}

        async with sessions() as session:
            assert await _policy_fingerprint(session) == before
            imbalance = await session.scalar(
                select(func.count()).select_from(
                    select(PaperLedgerEntryRecord.transaction_id)
                    .group_by(PaperLedgerEntryRecord.transaction_id)
                    .having(
                        func.sum(PaperLedgerEntryRecord.debit_amount)
                        != func.sum(PaperLedgerEntryRecord.credit_amount)
                    )
                    .subquery()
                )
            )
            assert imbalance == 0
    finally:
        await engine.dispose()
