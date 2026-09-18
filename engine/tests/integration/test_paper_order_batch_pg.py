import asyncio
import copy
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS, NO_CHANGE_INSTRUMENT, seed_paper_batch
from nexus.market.price_bar_authority import SOURCE_POLICY_ID
from nexus.paper.errors import PaperBatchError
from nexus.paper.notification import parse_notification_snapshot
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    RiskEvaluationRecord,
    RiskEvaluationRuleRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, OutboxEvent
from nexus.risk.canonical import (
    canonical_forward_strategy_decision_hash,
    canonical_paper_order_batch_hash,
    canonical_risk_evaluation_result_hash,
)
from nexus.strategy.forward import load_forward_strategy_decision
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _historical_order_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise historical paper-order mechanics behind an explicit test gate."""
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available",
        lambda: True,
    )


async def _create_and_inspect(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-happy-v1",
                request_time=seed.request_time,
            )
        async with sessions() as session:
            orders = tuple(await session.scalars(select(PaperOrderRecord)))
            events = tuple(
                await session.scalars(
                    select(PaperOrderEventRecord).order_by(
                        PaperOrderEventRecord.order_id,
                        PaperOrderEventRecord.event_seq,
                    )
                )
            )
            reservations = tuple(await session.scalars(select(PaperReservationRecord)))
            transactions = tuple(
                await session.scalars(select(PaperLedgerTransactionRecord))
            )
            entries = tuple(await session.scalars(select(PaperLedgerEntryRecord)))
            evaluation_count = await session.scalar(
                select(func.count()).select_from(RiskEvaluationRecord)
            )
            rule_count = await session.scalar(
                select(func.count()).select_from(RiskEvaluationRuleRecord)
            )
            audit_actions = tuple(
                await session.scalars(
                    select(AuditEvent.action)
                    .where(AuditEvent.schema_version == "paper_event_v1")
                    .order_by(AuditEvent.action)
                )
            )
            audit_payloads = tuple(
                (
                    await session.execute(
                        select(AuditEvent.action, AuditEvent.payload)
                        .where(AuditEvent.schema_version == "paper_event_v1")
                        .order_by(AuditEvent.action, AuditEvent.id)
                    )
                ).tuples()
            )
            outbox_types = tuple(
                await session.scalars(
                    select(OutboxEvent.event_type)
                    .where(OutboxEvent.schema_version == "paper_event_v1")
                    .order_by(OutboxEvent.event_type)
                )
            )
            rejected_outbox = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "paper.order.risk_rejected",
                    OutboxEvent.schema_version == "paper_event_v1",
                )
            )
            assert rejected_outbox is not None
            unpublished = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.schema_version == "paper_event_v1",
                    OutboxEvent.published_at.is_(None),
                )
            )
            fill_count = await session.scalar(select(func.count()).select_from(PaperFillRecord))
        return (
            seed,
            result,
            orders,
            events,
            reservations,
            transactions,
            entries,
            evaluation_count,
            rule_count,
            audit_actions,
            audit_payloads,
            outbox_types,
            unpublished,
            fill_count,
            rejected_outbox.payload,
        )
    finally:
        await engine.dispose()


def test_complete_fixture_creates_one_atomic_deterministic_paper_batch(
    migrated_database_url: str,
) -> None:
    (
        seed,
        result,
        orders,
        events,
        reservations,
        transactions,
        entries,
        evaluation_count,
        rule_count,
        audit_actions,
        audit_payloads,
        outbox_types,
        unpublished,
        fill_count,
        rejected_payload,
    ) = asyncio.run(_create_and_inspect(migrated_database_url))

    assert {evaluation.instrument_id: evaluation.outcome for evaluation in result.evaluations} == (
        seed.expected_outcomes
    )
    assert canonical_risk_evaluation_result_hash(result.evaluations) == result.result_hash
    assert canonical_paper_order_batch_hash(
        account_id=seed.account_id,
        decision_hash=result.decision_hash,
        risk_input_hash=result.risk_input_hash,
        evaluations=result.evaluations,
    ) == result.batch_hash
    assert len(orders) == 3
    assert evaluation_count == 3
    assert rule_count == 18

    events_by_order: dict[object, list[str]] = defaultdict(list)
    for event in events:
        events_by_order[event.order_id].append(event.to_status)
    outcome_by_order = {order.id: order.risk_outcome for order in orders}
    assert all(
        chain == (
            ["proposed", "risk_rejected"]
            if outcome_by_order[order_id] == "rejected"
            else ["proposed", "risk_approved", "submitted"]
        )
        for order_id, chain in events_by_order.items()
    )
    assert len(reservations) == 2

    new_transactions = [row for row in transactions if row.transaction_type != "funding"]
    assert len(new_transactions) == 2
    entries_by_transaction: dict[object, list[object]] = defaultdict(list)
    for entry in entries:
        entries_by_transaction[entry.transaction_id].append(entry)
    assert all(
        sum(entry.debit_amount for entry in entries_by_transaction[row.id])
        == sum(entry.credit_amount for entry in entries_by_transaction[row.id])
        for row in new_transactions
    )
    assert Counter(audit_actions) == Counter(outbox_types) + Counter(
        {
            "strategy_decision.persisted": 1,
            "paper.order_batch.evaluated": 1,
        }
    )
    assert "strategy_decision.persisted" not in outbox_types
    assert "paper.order_batch.evaluated" not in outbox_types
    assert "paper.order.risk_rejected" in outbox_types
    assert "strategy_decision.persisted" in audit_actions
    assert {
        payload["transaction_hash"]
        for action, payload in audit_payloads
        if action == "paper.ledger_transaction.posted"
    } == {row.transaction_hash for row in new_transactions}
    assert all(
        len(payload["reservation_event_hash"]) == 64
        for action, payload in audit_payloads
        if action == "paper.reservation.created"
    )
    assert unpublished == len(outbox_types)
    assert fill_count == 0
    rejected_order = next(order for order in orders if order.risk_outcome == "rejected")
    snapshot = parse_notification_snapshot(rejected_payload)
    assert snapshot.order_id == rejected_order.id
    assert snapshot.symbol == "TST3"
    assert snapshot.side == rejected_order.side
    assert snapshot.requested_quantity == rejected_order.quantity
    assert snapshot.cash.kind == "unchanged"
    assert snapshot.cash.available == Decimal("10000")
    assert snapshot.position.kind == "unchanged"
    assert snapshot.position.quantity == Decimal("0")
    assert snapshot.position.portfolio_ratio == Decimal("0")
    assert snapshot.agent_reason is None
    assert snapshot.risk_reason == "订单金额低于最小金额 $100。"
    frozen = rejected_payload["notification_snapshot"]
    assert isinstance(frozen, dict)
    assert not {
        "account_id",
        "strategy_decision_id",
        "result_hash",
        "evidence",
    }.intersection(frozen)


@pytest.mark.asyncio
async def test_missing_historical_ticker_marks_rejection_unavailable_without_rolling_back_batch(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(
        migrated_database_url,
        expired_ticker_instrument_id=INSTRUMENTS[2][0],
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-missing-ticker-notification",
                request_time=seed.request_time,
            )

        async with sessions() as session:
            orders = tuple(await session.scalars(select(PaperOrderRecord)))
            reservations = tuple(await session.scalars(select(PaperReservationRecord)))
            rejected_outbox = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.event_type == "paper.order.risk_rejected")
            )
            rejected_audit = await session.scalar(
                select(AuditEvent).where(AuditEvent.action == "paper.order.risk_rejected")
            )

        assert {
            evaluation.instrument_id: evaluation.outcome for evaluation in result.evaluations
        } == seed.expected_outcomes
        assert len(orders) == 3
        assert len(reservations) == 2
        assert rejected_outbox is not None
        assert rejected_audit is not None
        assert rejected_outbox.payload == rejected_audit.payload
        assert rejected_outbox.payload["notification_delivery"] == "unavailable"
        assert "notification_snapshot" not in rejected_outbox.payload
    finally:
        await engine.dispose()


async def _create_batch_with_no_change(database_url: str):
    seed = await seed_paper_batch(database_url, include_no_change=True)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-no-change-v1",
                request_time=seed.request_time,
            )
        async with sessions() as session:
            evidence = (
                await session.execute(
                    text(
                        "SELECT instrument_id, current_quantity, target_quantity, "
                        "reason_code, input_hash, evidence_hash "
                        "FROM risk_no_change_evaluations"
                    )
                )
            ).one()
            order_instruments = tuple(
                await session.scalars(select(PaperOrderRecord.instrument_id))
            )
        return result, evidence, order_instruments
    finally:
        await engine.dispose()


def test_no_change_target_persists_evidence_without_creating_an_order(
    migrated_database_url: str,
) -> None:
    result, evidence, order_instruments = asyncio.run(
        _create_batch_with_no_change(migrated_database_url)
    )

    assert evidence.instrument_id == NO_CHANGE_INSTRUMENT[0]
    assert evidence.current_quantity == evidence.target_quantity == 0
    assert evidence.reason_code == "NO_CHANGE"
    assert evidence.input_hash == result.risk_input_hash
    assert len(evidence.evidence_hash) == 64
    assert NO_CHANGE_INSTRUMENT[0] not in order_instruments


async def _attempt_with_missing_query_authority(database_url: str):
    seed = await seed_paper_batch(
        database_url,
        omit_persisted_query_kind="suspension",
    )
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        with pytest.raises(PaperBatchError) as captured:
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="paper-missing-query",
                    request_time=seed.request_time,
                )
        async with sessions() as session:
            counts = tuple(
                int(value or 0)
                for value in (
                    await session.scalar(select(func.count()).select_from(PaperOrderRecord)),
                    await session.scalar(
                        select(func.count()).select_from(PaperReservationRecord)
                    ),
                    await session.scalar(
                        select(func.count())
                        .select_from(PaperLedgerTransactionRecord)
                        .where(PaperLedgerTransactionRecord.transaction_type != "funding")
                    ),
                )
            )
        return captured.value.code, counts
    finally:
        await engine.dispose()


def test_missing_exact_query_authority_fails_closed_before_any_order(
    migrated_database_url: str,
) -> None:
    code, counts = asyncio.run(
        _attempt_with_missing_query_authority(migrated_database_url)
    )

    assert code == "RISK_AUTHORITY_INCOMPLETE"
    assert counts == (0, 0, 0)


async def _legacy_decision_batch_attempt(database_url: str) -> tuple[str, int, int]:
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            persisted = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=date(2026, 8, 26),
            )
            assert persisted is not None
            legacy = persisted.decision.model_copy(
                update={"source_policy_id": SOURCE_POLICY_ID}
            )
            legacy = legacy.model_copy(
                update={
                    "decision_hash": canonical_forward_strategy_decision_hash(legacy)
                }
            )
            for trigger_name in (
                "trg_strategy_decisions_created_at_immutable",
                "trg_strategy_decisions_immutable",
            ):
                await session.execute(
                    text(
                        f"ALTER TABLE strategy_decisions DISABLE TRIGGER {trigger_name}"
                    )
                )
            await session.execute(
                text(
                    "UPDATE strategy_decisions SET source_policy_id = :policy_id, "
                    "decision_hash = :decision_hash WHERE id = :decision_id"
                ),
                {
                    "policy_id": SOURCE_POLICY_ID,
                    "decision_hash": legacy.decision_hash,
                    "decision_id": seed.decision_id,
                },
            )
            for trigger_name in (
                "trg_strategy_decisions_created_at_immutable",
                "trg_strategy_decisions_immutable",
            ):
                await session.execute(
                    text(
                        f"ALTER TABLE strategy_decisions ENABLE TRIGGER {trigger_name}"
                    )
                )

        async with sessions.begin() as session:
            try:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="legacy-policy-must-not-write",
                    request_time=seed.request_time,
                )
            except PaperBatchError as error:
                error_code = error.code
            else:
                raise AssertionError("legacy decision unexpectedly created a paper batch")

        async with sessions() as session:
            idempotency_count = int(
                await session.scalar(
                    text(
                        "SELECT count(*) FROM idempotency_records "
                        "WHERE idempotency_key = 'legacy-policy-must-not-write'"
                    )
                )
                or 0
            )
            batch_count = int(
                await session.scalar(text("SELECT count(*) FROM paper_order_batches"))
                or 0
            )
        return error_code, idempotency_count, batch_count
    finally:
        await engine.dispose()


def test_new_paper_batch_rejects_legacy_policy_without_persisting_claim(
    migrated_database_url: str,
) -> None:
    error_code, idempotency_count, batch_count = asyncio.run(
        _legacy_decision_batch_attempt(migrated_database_url)
    )
    assert error_code == "STRATEGY_DECISION_INVALID"
    assert idempotency_count == 0
    assert batch_count == 0


@pytest.mark.asyncio
async def test_new_paper_batch_rejects_raw_only_v2_before_domain_writes(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available",
        lambda: False,
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        with pytest.raises(PaperBatchError) as captured:
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="raw-v2-must-not-write",
                    request_time=seed.request_time,
                )
        async with sessions() as session:
            counts = tuple(
                int(value or 0)
                for value in (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM idempotency_records "
                            "WHERE idempotency_key = 'raw-v2-must-not-write'"
                        )
                    ),
                    await session.scalar(
                        select(func.count()).select_from(PaperOrderRecord)
                    ),
                    await session.scalar(
                        text("SELECT count(*) FROM paper_order_batches")
                    ),
                )
            )
    finally:
        await engine.dispose()

    assert captured.value.code == "STRATEGY_DECISION_INVALID"
    assert counts == (0, 0, 0)


async def _attempt_superseded_decision(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            original = await session.get(StrategyDecisionRecord, seed.decision_id)
            assert original is not None
            session.add(
                StrategyDecisionRecord(
                    id=UUID("52000000-0000-0000-0000-000000000001"),
                    account_id=seed.account_id,
                    portfolio_id=seed.portfolio_id,
                    strategy_id="baseline",
                    strategy_version="baseline_v1",
                    strategy_definition_id=original.strategy_definition_id,
                    registry_event_id=original.registry_event_id,
                    champion_generation=original.champion_generation,
                    decision_session=date(2026, 8, 27),
                    decision_time=datetime(2026, 8, 27, 21, tzinfo=UTC),
                    execution_session=date(2026, 8, 28),
                    source_policy_id=original.source_policy_id,
                    calendar_authority_id=original.calendar_authority_id,
                    universe_snapshot_id=original.universe_snapshot_id,
                    quality_report_id=original.quality_report_id,
                    portfolio_projection_hash="6" * 64,
                    authority_hash="7" * 64,
                    strategy_parameters_hash="8" * 64,
                    decision_hash="9" * 64,
                    cash_weight=1,
                    reason="WEEKLY_REBALANCE",
                )
            )
        with pytest.raises(PaperBatchError) as captured:
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="paper-superseded-decision",
                    request_time=seed.request_time,
                )
        async with sessions() as session:
            order_count = int(
                await session.scalar(select(func.count()).select_from(PaperOrderRecord)) or 0
            )
        return captured.value.code, order_count
    finally:
        await engine.dispose()


def test_superseded_strategy_decision_fails_closed_before_creating_orders(
    migrated_database_url: str,
) -> None:
    code, order_count = asyncio.run(_attempt_superseded_decision(migrated_database_url))

    assert code == "PAPER_BATCH_CONFLICT"
    assert order_count == 0


async def _attempt_corrupt_existing_batch_replay(
    database_url: str,
    *,
    corruption: str,
) -> str | None:
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-corrupt-replay",
                request_time=seed.request_time,
            )
        async with sessions.begin() as session:
            if corruption == "current_decision_hash":
                await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
                await session.execute(
                    text(
                        "UPDATE strategy_decisions SET decision_hash=repeat('a', 64) "
                        "WHERE id=:decision_id"
                    ),
                    {"decision_id": seed.decision_id},
                )
            else:
                record = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.route == "paper.order_batch.create",
                        IdempotencyRecord.idempotency_key == "paper-corrupt-replay",
                    )
                )
                assert record is not None
                payload = copy.deepcopy(record.response_payload)
                if corruption == "missing_field":
                    payload.pop("batch_id")
                elif corruption == "batch_id":
                    payload["batch_id"] = "52000000-0000-0000-0000-000000000101"
                elif corruption == "account_id":
                    payload["account_id"] = "52000000-0000-0000-0000-000000000102"
                elif corruption == "strategy_decision_id":
                    payload["strategy_decision_id"] = (
                        "52000000-0000-0000-0000-000000000103"
                    )
                elif corruption in {
                    "decision_hash",
                    "projection_hash",
                    "request_hash",
                    "risk_input_hash",
                    "result_hash",
                    "batch_hash",
                }:
                    payload[corruption] = "a" * 64
                elif corruption == "evaluated_at":
                    payload["evaluated_at"] = "2020-01-01T00:00:00Z"
                elif corruption == "order_ids":
                    payload["order_ids"] = []
                elif corruption == "evaluations":
                    payload["evaluations"][0]["approved_quantity"] = "999"
                elif corruption == "noncanonical_decimal":
                    payload["evaluations"][0]["desired_quantity"] = int(
                        Decimal(payload["evaluations"][0]["desired_quantity"])
                    )
                elif corruption == "extra_no_changes":
                    payload["no_changes"] = []
                elif corruption == "extra_replayed":
                    payload["replayed"] = True
                else:
                    raise AssertionError(f"unsupported corruption: {corruption}")
                record.response_payload = payload
        try:
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="paper-corrupt-replay",
                    request_time=seed.request_time,
                )
        except PaperBatchError as error:
            return error.code
        return None
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    (
        "current_decision_hash",
        "missing_field",
        "batch_id",
        "account_id",
        "strategy_decision_id",
        "decision_hash",
        "projection_hash",
        "request_hash",
        "risk_input_hash",
        "result_hash",
        "batch_hash",
        "evaluated_at",
        "order_ids",
        "evaluations",
        "noncanonical_decimal",
        "extra_no_changes",
        "extra_replayed",
    ),
)
def test_existing_batch_replay_rejects_decision_and_response_payload_corruption(
    migrated_database_url: str,
    corruption: str,
) -> None:
    code = asyncio.run(
        _attempt_corrupt_existing_batch_replay(
            migrated_database_url,
            corruption=corruption,
        )
    )

    assert code == "PAPER_BATCH_CONFLICT"
