import asyncio
from collections import defaultdict
from decimal import Decimal
from typing import Literal, cast

from helpers.paper_batch import seed_paper_batch
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionAdjustmentEventRecord,
    PaperPositionSnapshotRecord,
    PaperReservationRecord,
    RiskEvaluationPriceInputRecord,
    RiskEvaluationQueryProofRecord,
    RiskEvaluationRecord,
    RiskEvaluationRuleRecord,
    RiskEvaluationSectorInputRecord,
    RiskPolicyRecord,
)
from nexus.persistence.models.portfolio import BrokerOrderSnapshot
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, OutboxEvent
from nexus.risk.authority import build_risk_market_snapshot
from nexus.risk.canonical import (
    canonical_forward_strategy_decision_hash,
    canonical_paper_ledger_transaction_hash,
    canonical_paper_order_batch_hash,
    canonical_paper_projection_hash,
    canonical_risk_batch_input_hash,
    canonical_risk_evaluation_result_hash,
)
from nexus.risk.evaluate import derive_order_candidates
from nexus.risk.models import (
    AuthorityQueryProof,
    RiskEvaluation,
    RiskPrice,
    RiskRuleEvidence,
    RiskSectorExposure,
)
from nexus.risk.policy import RiskPolicy
from nexus.strategy.forward import load_forward_strategy_decision
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def _policy(row: RiskPolicyRecord) -> RiskPolicy:
    return RiskPolicy(
        version="risk_policy_v1",
        active=row.active,
        single_name_limit=row.single_name_limit,
        sector_limit=row.sector_limit,
        minimum_cash_ratio=row.minimum_cash_ratio,
        drawdown_lock_ratio=row.drawdown_lock_ratio,
        drawdown_recovery_ratio=row.drawdown_recovery_ratio,
        minimum_buy_notional=row.minimum_buy_notional,
        buy_reserve_markup=row.buy_reserve_markup,
        commission=row.commission,
        supported_market="US",
        supported_currency="USD",
        supported_asset_types=("ETF", "common_stock"),
        content_hash=row.content_hash,
    )


async def _verify_raw_hashes_and_rollback(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            transaction = await session.begin()
            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="task18-acceptance-rollback",
                request_time=seed.request_time,
            )
            await session.flush()

            persisted = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=seed.decision_time.date(),
            )
            assert persisted is not None
            decision = persisted.decision
            assert canonical_forward_strategy_decision_hash(decision) == result.decision_hash

            projection = await load_paper_projection(
                session,
                account_id=seed.account_id,
                as_of=seed.decision_time,
            )
            assert canonical_paper_projection_hash(projection) == result.projection_hash

            evaluation_rows = tuple(
                await session.scalars(
                    select(RiskEvaluationRecord).order_by(
                        RiskEvaluationRecord.strategy_priority,
                        RiskEvaluationRecord.target_weight.desc(),
                        RiskEvaluationRecord.id,
                    )
                )
            )
            first_evaluation_id = evaluation_rows[0].id
            price_rows = tuple(
                await session.scalars(
                    select(RiskEvaluationPriceInputRecord)
                    .where(
                        RiskEvaluationPriceInputRecord.risk_evaluation_id
                        == first_evaluation_id
                    )
                    .order_by(RiskEvaluationPriceInputRecord.instrument_id)
                )
            )
            sector_rows = tuple(
                await session.scalars(
                    select(RiskEvaluationSectorInputRecord)
                    .where(
                        RiskEvaluationSectorInputRecord.risk_evaluation_id
                        == first_evaluation_id
                    )
                    .order_by(
                        RiskEvaluationSectorInputRecord.instrument_id,
                        RiskEvaluationSectorInputRecord.sector,
                    )
                )
            )
            proof_rows = tuple(
                await session.scalars(
                    select(RiskEvaluationQueryProofRecord)
                    .where(
                        RiskEvaluationQueryProofRecord.risk_evaluation_id
                        == first_evaluation_id
                    )
                    .order_by(RiskEvaluationQueryProofRecord.kind)
                )
            )
            required = tuple(row.instrument_id for row in price_rows)
            market = build_risk_market_snapshot(
                decision=decision,
                required_instrument_ids=required,
                whitelisted_instrument_ids=required,
                prices=tuple(
                    RiskPrice(
                        instrument_id=row.instrument_id,
                        authority_id=row.price_authority_id,
                        market=cast(Literal["US"], row.market),
                        currency=cast(Literal["USD"], row.currency),
                        asset_type=cast(Literal["ETF", "common_stock"], row.asset_type),
                        close=row.close,
                        source=row.source,
                        delivery_mode=row.delivery_mode,
                        available_at=row.available_at,
                        content_hash=row.content_hash,
                    )
                    for row in price_rows
                ),
                sector_exposures=tuple(
                    RiskSectorExposure(
                        instrument_id=row.instrument_id,
                        authority_id=row.sector_exposure_id,
                        sector=row.sector,
                        weight=row.weight,
                        content_hash=row.content_hash,
                    )
                    for row in sector_rows
                ),
                query_proofs=tuple(
                    AuthorityQueryProof(
                        kind=cast(
                            Literal["corporate_action", "suspension", "delisting"],
                            row.kind,
                        ),
                        authority_id=row.authority_id,
                        complete=row.complete,
                        blocked=row.blocked,
                        available_at=row.available_at,
                        content_hash=row.content_hash,
                    )
                    for row in proof_rows
                ),
            )
            assert market.authority_hash == decision.authority_hash
            policy_row = await session.scalar(
                select(RiskPolicyRecord).where(RiskPolicyRecord.active)
            )
            assert policy_row is not None
            candidates = derive_order_candidates(decision, projection, market)
            assert (
                canonical_risk_batch_input_hash(
                    candidates=candidates,
                    portfolio=projection,
                    market=market,
                    policy_version="risk_policy_v1",
                    policy_hash=_policy(policy_row).content_hash,
                )
                == result.risk_input_hash
            )

            orders = {
                row.id: row for row in await session.scalars(select(PaperOrderRecord))
            }
            rules_by_evaluation: dict[object, list[RiskEvaluationRuleRecord]] = defaultdict(list)
            for row in await session.scalars(
                select(RiskEvaluationRuleRecord).order_by(
                    RiskEvaluationRuleRecord.risk_evaluation_id,
                    RiskEvaluationRuleRecord.rule_seq,
                )
            ):
                rules_by_evaluation[row.risk_evaluation_id].append(row)
            raw_evaluations = tuple(
                sorted(
                    (
                        RiskEvaluation(
                            instrument_id=orders[row.order_id].instrument_id,
                            side=cast(Literal["buy", "sell"], orders[row.order_id].side),
                            outcome=cast(
                                Literal["approved", "reduced", "rejected"], row.outcome
                            ),
                            desired_quantity=row.desired_quantity,
                            approved_quantity=row.approved_quantity,
                            target_weight=row.target_weight,
                            strategy_priority=row.strategy_priority,
                            reference_close=row.reference_close,
                            worst_price=row.worst_price,
                            reserve_amount=row.reserve_amount,
                            projected_available_cash=row.projected_available_cash,
                            projected_cash_ratio=row.projected_cash_ratio,
                            projected_name_ratio=row.projected_name_ratio,
                            projected_sector_ratios=tuple(
                                (sector, Decimal(value))
                                for sector, value in sorted(
                                    row.projected_sector_ratios.items()
                                )
                            ),
                            rules=tuple(
                                RiskRuleEvidence(
                                    rule_code=rule.rule_code,
                                    passed=rule.passed,
                                    limit=rule.limit_value,
                                    observed=rule.observed_value,
                                    remaining=rule.remaining_value,
                                    authority_ids=tuple(rule.authority_ids),
                                )
                                for rule in rules_by_evaluation[row.id]
                            ),
                            result_hash=row.result_hash,
                        )
                        for row in evaluation_rows
                    ),
                    key=lambda item: (
                        item.strategy_priority,
                        -item.target_weight,
                        item.instrument_id.int,
                    ),
                )
            )
            assert all(
                canonical_risk_evaluation_result_hash((value,)) == value.result_hash
                for value in raw_evaluations
            )
            assert canonical_risk_evaluation_result_hash(raw_evaluations) == result.result_hash
            assert (
                canonical_paper_order_batch_hash(
                    account_id=seed.account_id,
                    decision_hash=decision.decision_hash,
                    risk_input_hash=result.risk_input_hash,
                    evaluations=raw_evaluations,
                )
                == result.batch_hash
            )

            ledger_rows = tuple(
                await session.scalars(
                    select(PaperLedgerTransactionRecord).where(
                        PaperLedgerTransactionRecord.transaction_type != "funding"
                    )
                )
            )
            for ledger_row in ledger_rows:
                entry_rows = tuple(
                    await session.scalars(
                        select(PaperLedgerEntryRecord)
                        .where(PaperLedgerEntryRecord.transaction_id == ledger_row.id)
                        .order_by(PaperLedgerEntryRecord.entry_seq)
                    )
                )
                raw_transaction = LedgerTransaction(
                    transaction_id=ledger_row.id,
                    account_id=ledger_row.account_id,
                    transaction_type=cast(
                        Literal["funding", "cash_reservation", "fill", "release", "adjustment"],
                        ledger_row.transaction_type,
                    ),
                    reference_type=cast(
                        Literal[
                            "initial_funding",
                            "cash_reservation",
                            "fill",
                            "release",
                            "adjustment",
                        ],
                        ledger_row.reference_type,
                    ),
                    reference_hash=ledger_row.reference_hash,
                    occurred_at=ledger_row.occurred_at,
                    entries=tuple(
                        LedgerEntry(
                            entry_id=entry.id,
                            entry_seq=entry.entry_seq,
                            account_code=entry.account_code,
                            currency="USD",
                            debit_amount=entry.debit_amount,
                            credit_amount=entry.credit_amount,
                        )
                        for entry in entry_rows
                    ),
                    transaction_hash=ledger_row.transaction_hash,
                )
                assert (
                    canonical_paper_ledger_transaction_hash(raw_transaction)
                    == ledger_row.transaction_hash
                )
                assert sum(entry.debit_amount for entry in entry_rows) == sum(
                    entry.credit_amount for entry in entry_rows
                )

            events_by_order: dict[object, list[PaperOrderEventRecord]] = defaultdict(list)
            for row in await session.scalars(
                select(PaperOrderEventRecord).order_by(
                    PaperOrderEventRecord.order_id,
                    PaperOrderEventRecord.event_seq,
                )
            ):
                events_by_order[row.order_id].append(row)
            reservations = {
                row.order_id: row
                for row in await session.scalars(select(PaperReservationRecord))
            }
            evaluation_by_order = {row.order_id: row for row in evaluation_rows}
            for order_id, order in orders.items():
                chain = events_by_order[order_id]
                assert [event.event_seq for event in chain] == list(
                    range(1, len(chain) + 1)
                )
                if order.risk_outcome == "rejected":
                    assert order_id not in reservations
                    assert chain[-1].to_status == "risk_rejected"
                else:
                    assert chain[-1].to_status == "submitted"
                    assert reservations[order_id].amount == (
                        evaluation_by_order[order_id].reserve_amount
                    )

            task19_counts: list[int] = []
            for model in (
                PaperFillRecord,
                PaperPositionAdjustmentEventRecord,
                PaperPositionSnapshotRecord,
                PaperPerformanceSnapshotRecord,
            ):
                task19_counts.append(
                    int(
                        await session.scalar(select(func.count()).select_from(model))
                        or 0
                    )
                )
            broker_order_count = int(
                await session.scalar(select(func.count()).select_from(BrokerOrderSnapshot))
                or 0
            )
            assert task19_counts == [0, 0, 0, 0]
            assert broker_order_count == 0
            await transaction.rollback()

        async with sessions() as session:
            residual = (
                int(
                    await session.scalar(
                        select(func.count()).select_from(PaperOrderBatchRecord)
                    )
                    or 0
                ),
                int(
                    await session.scalar(select(func.count()).select_from(PaperOrderRecord))
                    or 0
                ),
                int(
                    await session.scalar(
                        select(func.count()).select_from(PaperReservationRecord)
                    )
                    or 0
                ),
                int(
                    await session.scalar(
                        select(func.count())
                        .select_from(PaperLedgerTransactionRecord)
                        .where(PaperLedgerTransactionRecord.transaction_type != "funding")
                    )
                    or 0
                ),
                int(
                    await session.scalar(select(func.count()).select_from(IdempotencyRecord))
                    or 0
                ),
                int(
                    await session.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.schema_version == "paper_event_v1")
                    )
                    or 0
                ),
                int(
                    await session.scalar(
                        select(func.count())
                        .select_from(OutboxEvent)
                        .where(OutboxEvent.schema_version == "paper_event_v1")
                    )
                    or 0
                ),
            )
        return residual
    finally:
        await engine.dispose()


def test_task18_raw_hashes_accounting_boundaries_and_rollback(
    migrated_database_url: str,
) -> None:
    residual = asyncio.run(_verify_raw_hashes_and_rollback(migrated_database_url))

    assert residual == (0, 0, 0, 0, 0, 0, 0)
