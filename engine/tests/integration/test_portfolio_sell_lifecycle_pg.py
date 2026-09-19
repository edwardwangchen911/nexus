from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_agent_paper_cycle_pg import _ObjectStore, _SchwabPriceHistoryProvider
from integration.test_portfolio_management_e2e import _PartialSchwabProvider
from integration.test_portfolio_proposal_pg import proposal_source
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis, synthesis_intent
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
)
from nexus.paper.agent_decision_store import persist_portfolio_proposal
from nexus.paper.agent_market import collect_agent_paper_window, resolve_agent_paper_market_scope
from nexus.paper.execution_service import (
    advance_internal_paper_order,
    cancel_internal_paper_order,
)
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.paper.target_service import activate_portfolio_target
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.data import PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
    PaperPositionSnapshotRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash
from sqlalchemy import func, select, text


def _exit_first(run) -> PortfolioSynthesisIntentV1:
    document = synthesis_intent(run).model_dump(mode="json")
    frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
    held = next(position.instrument_id for position in frozen.projection.positions)
    choice = next(
        item for item in document["choices"] if item["instrument_id"] == str(held)
    )
    choice.update(action="exit", role="none", rank=None)
    return PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["partial_cancel", "full_fill"])
async def test_persisted_proposal_creates_and_conserves_real_sell_lifecycle(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    import integration.test_portfolio_synthesis_persistence_pg as seed_module

    normal = await seed_normal_path(migrated_database_url, monkeypatch)
    engine, sessions, authority, manifest = normal
    held_instrument = manifest[0].instrument_id
    initial_quantity = Decimal("10")
    try:
        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE paper_accounts DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE paper_accounts SET initial_capital=11000 "
                        "WHERE id=:account_id"
                    ),
                    {"account_id": authority.account_id},
                )
            finally:
                await session.execute(text("ALTER TABLE paper_accounts ENABLE TRIGGER USER"))
            price_authority_id = await session.scalar(
                select(PriceBarSetAuthority.id)
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(PriceSeriesManifest.instrument == str(held_instrument))
                .order_by(PriceBarSetAuthority.available_at.desc())
                .limit(1)
            )
            assert price_authority_id is not None
            session.add(
                PaperPositionAdjustmentEventRecord(
                    account_id=authority.account_id,
                    instrument_id=held_instrument,
                    event_seq=1,
                    event_type="correction",
                    quantity_delta=initial_quantity,
                    cost_delta=Decimal("1000"),
                    source_fill_id=None,
                    occurred_at=authority.decision_time - timedelta(seconds=1),
                    payload_hash="8" * 64,
                )
            )
            await session.execute(
                text("ALTER TABLE paper_position_snapshots DISABLE TRIGGER USER")
            )
            try:
                session.add(
                    PaperPositionSnapshotRecord(
                        account_id=authority.account_id,
                        instrument_id=held_instrument,
                        as_of=authority.decision_time,
                        quantity=initial_quantity,
                        reserved_quantity=Decimal(0),
                        cost_basis=Decimal("1000"),
                        reference_close=Decimal("100"),
                        price_authority_id=price_authority_id,
                        projection_hash="9" * 64,
                    )
                )
                await session.flush()
            finally:
                await session.execute(
                    text("ALTER TABLE paper_position_snapshots ENABLE TRIGGER USER")
                )

        original_intent = seed_module._safe_no_trade_intent

        def complete_hold(**kwargs):
            source = original_intent(**kwargs)
            assessment = source.synthesis_assessment.model_copy(
                update={
                    "stance": "hold",
                    "review_conclusion": "sufficient",
                    "reviews": tuple(
                        review.model_copy(
                            update={
                                "resolved": True,
                                "status": "accepted",
                                "missing_evidence": (),
                                "prevented_conclusion": None,
                            }
                        )
                        for review in source.synthesis_assessment.reviews
                    ),
                }
            )
            return source.model_copy(
                update={
                    "quality_status": "complete",
                    "stance": "hold",
                    "exposure_action": "maintain",
                    "evidence_audit": "complete",
                    "synthesis_assessment": assessment,
                    "role_votes": tuple(
                        vote.model_copy(update={"vote": "support"})
                        for vote in source.role_votes
                    ),
                }
            )

        monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_hold)
        seeded = await seed_synthesis(migrated_database_url, normal_path=normal)
        _, _, args = await proposal_source(
            migrated_database_url,
            seeded=seeded,
            intent_factory=_exit_first,
        )
        async with sessions.begin() as session:
            output = await session.get(
                AgentPortfolioSynthesisOutputRecord, args["synthesis_output_id"]
            )
            run = None if output is None else await session.get(
                AgentPortfolioSynthesisRunRecord, output.run_id
            )
            cycle_check = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            job = None if run is None else await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            assert output is not None and run is not None and cycle_check is not None
            assert (output.status, run.status, cycle_check.sizing_policy_version) == (
                "completed",
                "completed",
                "portfolio_allocation_v1",
            ), (
                output.status,
                run.status,
                cycle_check.sizing_policy_version,
                None if job is None else job.last_error_code,
                output.intent,
            )
            proposal = await persist_portfolio_proposal(session, **args)
            assert proposal.proposal.baseline.weights[held_instrument] > 0
            assert proposal.proposal.weights[held_instrument] == 0
            activated = await activate_portfolio_target(
                session,
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key=f"sell-lifecycle-{terminal}",
            )
            assert activated.batch_id is not None
            order = await session.scalar(
                select(PaperOrderRecord).where(
                    PaperOrderRecord.batch_id == activated.batch_id,
                    PaperOrderRecord.instrument_id == held_instrument,
                )
            )
            assert order is not None
            assert order.side == "sell"
            reservation = await session.scalar(
                select(PaperReservationRecord).where(
                    PaperReservationRecord.order_id == order.id
                )
            )
            assert reservation is not None
            assert reservation.reservation_type == "position"
            assert reservation.amount == order.quantity == initial_quantity
            ledger_accounts = {
                account.code: account
                for account in await session.scalars(
                    select(PaperLedgerAccountRecord).where(
                        PaperLedgerAccountRecord.paper_account_id == proposal.account_id
                    )
                )
            }
            transaction_id = uuid4()
            draft = LedgerTransaction(
                transaction_id=transaction_id,
                account_id=proposal.account_id,
                transaction_type="adjustment",
                reference_type="adjustment",
                reference_hash="7" * 64,
                occurred_at=args["now"] + timedelta(seconds=2),
                entries=(
                    LedgerEntry(
                        entry_id=uuid4(),
                        entry_seq=1,
                        account_code="asset.position_cost.usd",
                        currency="USD",
                        debit_amount=Decimal("1000"),
                        credit_amount=Decimal(0),
                    ),
                    LedgerEntry(
                        entry_id=uuid4(),
                        entry_seq=2,
                        account_code="equity.contributed_capital.usd",
                        currency="USD",
                        debit_amount=Decimal(0),
                        credit_amount=Decimal("1000"),
                    ),
                ),
                transaction_hash="0" * 64,
            )
            transaction = draft.model_copy(
                update={
                    "transaction_hash": canonical_paper_ledger_transaction_hash(draft)
                }
            )
            session.add(
                PaperLedgerTransactionRecord(
                    id=transaction.transaction_id,
                    account_id=transaction.account_id,
                    order_id=order.id,
                    transaction_type=transaction.transaction_type,
                    reference_type=transaction.reference_type,
                    reference_hash=transaction.reference_hash,
                    occurred_at=transaction.occurred_at,
                    transaction_hash=transaction.transaction_hash,
                )
            )
            await session.flush()
            session.add_all(
                PaperLedgerEntryRecord(
                    id=entry.entry_id,
                    transaction_id=transaction.transaction_id,
                    account_id=transaction.account_id,
                    entry_seq=entry.entry_seq,
                    ledger_account_id=ledger_accounts[entry.account_code].id,
                    account_code=entry.account_code,
                    currency=entry.currency,
                    debit_amount=entry.debit_amount,
                    credit_amount=entry.credit_amount,
                )
                for entry in transaction.entries
            )
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            initial_available_cash = await session.scalar(
                select(
                    func.sum(
                        PaperLedgerEntryRecord.debit_amount
                        - PaperLedgerEntryRecord.credit_amount
                    )
                ).where(
                    PaperLedgerEntryRecord.account_id == proposal.account_id,
                    PaperLedgerEntryRecord.account_code == "asset.cash.available.usd",
                )
            )
            assert initial_available_cash is not None

        async with sessions() as session:
            scope = await resolve_agent_paper_market_scope(
                session,
                cycle_id=cycle.id,
                instrument_id=order.instrument_id,
            )
        evaluated_at = scope.window_end + timedelta(seconds=2)
        provider = (
            _PartialSchwabProvider()
            if terminal == "partial_cancel"
            else _SchwabPriceHistoryProvider()
        )
        await collect_agent_paper_window(
            sessions,
            cycle_id=cycle.id,
            instrument_id=order.instrument_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        async with sessions.begin() as session:
            advanced = await advance_internal_paper_order(
                session,
                order_id=order.id,
                evaluated_at=evaluated_at,
                idempotency_key=f"sell-advance-{terminal}",
            )
        if terminal == "partial_cancel":
            assert advanced.action == "partially_filled"
            async with sessions.begin() as session:
                cancelled = await cancel_internal_paper_order(
                    session,
                    order_id=order.id,
                    expected_event_seq=advanced.result_event_seq,
                    requested_at=evaluated_at + timedelta(seconds=1),
                    idempotency_key="sell-partial-cancel",
                )
            assert cancelled.status == "cancelled"
        else:
            assert advanced.action == "filled"

        as_of = evaluated_at + timedelta(seconds=2)
        async with sessions() as session:
            fills = tuple(
                await session.scalars(
                    select(PaperFillRecord).where(PaperFillRecord.order_id == order.id)
                )
            )
            events = tuple(
                await session.scalars(
                    select(PaperOrderEventRecord)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq)
                )
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord)
                    .where(PaperReservationEventRecord.reservation_id == reservation.id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )
            imbalances = (
                await session.execute(
                    select(PaperLedgerEntryRecord.transaction_id)
                    .group_by(PaperLedgerEntryRecord.transaction_id)
                    .having(
                        func.sum(PaperLedgerEntryRecord.debit_amount)
                        != func.sum(PaperLedgerEntryRecord.credit_amount)
                    )
                )
            ).all()
            position_quantity = await session.scalar(
                select(func.sum(PaperPositionAdjustmentEventRecord.quantity_delta)).where(
                    PaperPositionAdjustmentEventRecord.account_id == proposal.account_id,
                    PaperPositionAdjustmentEventRecord.instrument_id == held_instrument,
                    PaperPositionAdjustmentEventRecord.occurred_at <= as_of,
                )
            )
            available_cash = await session.scalar(
                select(
                    func.sum(
                        PaperLedgerEntryRecord.debit_amount
                        - PaperLedgerEntryRecord.credit_amount
                    )
                ).where(
                    PaperLedgerEntryRecord.account_id == proposal.account_id,
                    PaperLedgerEntryRecord.account_code == "asset.cash.available.usd",
                )
            )
            reserved_cash = await session.scalar(
                select(
                    func.sum(
                        PaperLedgerEntryRecord.debit_amount
                        - PaperLedgerEntryRecord.credit_amount
                    )
                ).where(
                    PaperLedgerEntryRecord.account_id == proposal.account_id,
                    PaperLedgerEntryRecord.account_code == "asset.cash.reserved.usd",
                )
            )

        sold_quantity = sum((fill.quantity for fill in fills), start=Decimal(0))
        net_proceeds = sum(
            (fill.notional - fill.fee for fill in fills), start=Decimal(0)
        )
        assert sold_quantity == advanced.filled_quantity
        assert position_quantity == initial_quantity - sold_quantity
        assert reserved_cash in (None, 0)
        assert available_cash == initial_available_cash + net_proceeds
        assert imbalances == []
        assert sum(
            (
                event.amount
                for event in reservation_events
                if event.event_type in {"consumed", "released"}
            ),
            start=Decimal(0),
        ) == reservation.amount
        assert events[-1].to_status == (
            "cancelled" if terminal == "partial_cancel" else "filled"
        )
    finally:
        await engine.dispose()
