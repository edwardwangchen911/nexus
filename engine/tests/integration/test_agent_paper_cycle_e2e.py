from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import (
    CALENDAR_ID,
    DECISION_SESSION,
    NO_CHANGE_INSTRUMENT,
    UNIVERSE_ID,
    seed_paper_batch,
)
from helpers.paper_execution import FIRST_EVALUATION, seed_paper_execution
from nexus.paper.agent_cycle import (
    AgentPaperCycleError,
    _resume_batch,
    advance_agent_paper_cycle,
)
from nexus.paper.agent_cycle_models import (
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    floor_decision_slot,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
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
    StrategyDecisionRecord,
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


@pytest.mark.asyncio
async def test_same_day_resume_fallback_is_scoped_to_the_cycle(
    migrated_database_url: str,
) -> None:
    """A missing event fact must not bind another intraday cycle's decision."""
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    first_cycle_id = UUID("68000000-0000-0000-0000-000000000101")
    second_cycle_id = UUID("68000000-0000-0000-0000-000000000102")
    first_decision_id = UUID("68000000-0000-0000-0000-000000000201")
    second_decision_id = UUID("68000000-0000-0000-0000-000000000202")
    first_time = datetime(2026, 8, 26, 14, 30, tzinfo=UTC)
    second_time = first_time + timedelta(minutes=5)

    def cycle(cycle_id: UUID, decision_time: datetime) -> AgentPaperCycleRecord:
        return AgentPaperCycleRecord(
            id=cycle_id,
            account_id=seed.account_id,
            portfolio_id=seed.portfolio_id,
            decision_session=DECISION_SESSION,
            decision_time=decision_time,
            decision_slot_at=floor_decision_slot(decision_time),
            execution_session=DECISION_SESSION,
            universe_snapshot_id=UNIVERSE_ID,
            quality_report_id=None,
            decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
            candidate_manifest=[],
            candidate_hash=candidate_manifest_hash(()),
            sizing_policy_version="agent_paper_sizing_v1",
            sizing_policy_hash="8" * 64,
            execution_locked=True,
        )

    def decision(
        decision_id: UUID,
        cycle_id: UUID,
        decision_time: datetime,
        hash_digit: str,
    ) -> StrategyDecisionRecord:
        return StrategyDecisionRecord(
            id=decision_id,
            account_id=seed.account_id,
            portfolio_id=seed.portfolio_id,
            strategy_id="agent_research",
            strategy_version="agent_paper_decision_v1",
            strategy_definition_id=None,
            registry_event_id=None,
            champion_generation=None,
            decision_origin="agent_research",
            agent_paper_cycle_id=cycle_id,
            decision_session=DECISION_SESSION,
            decision_time=decision_time,
            execution_session=DECISION_SESSION,
            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            execution_source_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
            calendar_authority_id=CALENDAR_ID,
            universe_snapshot_id=UNIVERSE_ID,
            quality_report_id=None,
            portfolio_projection_hash=hash_digit * 64,
            authority_hash=hash_digit * 64,
            strategy_parameters_hash=hash_digit * 64,
            decision_hash=("a" if hash_digit == "1" else "b") * 64,
            cash_weight=Decimal("1"),
            reason="AGENT_RESEARCH",
        )

    def event(
        sequence: int,
        from_status: str | None,
        to_status: str,
        result_code: str,
        fact_ids: dict[str, object],
    ) -> AgentPaperCycleEventRecord:
        occurred_at = second_time + timedelta(seconds=sequence)
        return AgentPaperCycleEventRecord(
            cycle_id=second_cycle_id,
            sequence=sequence,
            from_status=from_status,
            to_status=to_status,
            result_code=result_code,
            fact_ids=fact_ids,
            occurred_at=occurred_at,
            event_hash=canonical_cycle_event_hash(
                cycle_id=second_cycle_id,
                sequence=sequence,
                from_status=from_status,
                to_status=to_status,
                result_code=result_code,
                fact_ids=fact_ids,
                occurred_at=occurred_at,
            ),
        )

    try:
        async with sessions.begin() as session:
            first_cycle = cycle(first_cycle_id, first_time)
            second_cycle = cycle(second_cycle_id, second_time)
            session.add_all((first_cycle, second_cycle))
            await session.flush()
            session.add_all(
                (
                    decision(first_decision_id, first_cycle_id, first_time, "1"),
                    decision(second_decision_id, second_cycle_id, second_time, "2"),
                    event(1, None, "prepared", "CYCLE_PREPARED", {}),
                    event(2, "prepared", "researching", "RESEARCH_JOBS_PREPARED", {}),
                    event(
                        3,
                        "researching",
                        "decision_recorded",
                        "AGENT_DECISION_RECORDED",
                        {},
                    ),
                    event(
                        4,
                        "decision_recorded",
                        "risk_evaluated",
                        "PAPER_BATCH_EVALUATED",
                        {
                            "batch_id": "68000000-0000-0000-0000-000000000301",
                            "order_ids": [],
                        },
                    ),
                )
            )
            await session.flush()
            events = tuple(
                await session.scalars(
                    select(AgentPaperCycleEventRecord)
                    .where(AgentPaperCycleEventRecord.cycle_id == second_cycle_id)
                    .order_by(AgentPaperCycleEventRecord.sequence)
                )
            )

            result = await _resume_batch(session, cycle=second_cycle, events=events)

        assert result.cycle_id == second_cycle_id
        assert result.status == "completed"
        assert result.result_code == "NO_ORDER_REQUIRED"
        assert result.strategy_decision_id == second_decision_id
    finally:
        await engine.dispose()
