"""Persist one validated, inseparable broker synchronization result."""

from datetime import datetime
from typing import cast

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.portfolio import (
    Account,
    BrokerOrderSnapshot,
    BrokerSyncAttempt,
    BrokerSyncRun,
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.providers.broker.base import BrokerSyncResult
from nexus.reconciliation.cash_flow_persistence import CashFlowQuality, persist_cash_flows
from nexus.reconciliation.cash_flows import CashFlowSyncResult
from nexus.reconciliation.facts import (
    ORDER_HASH_VERSION,
    POSITION_HASH_VERSION,
    canonical_order_hash,
    canonical_position_hash,
    cash_by_currency,
)
from nexus.reconciliation.finalization import InvalidBrokerSyncResult, validate_broker_sync_result
from nexus.reconciliation.persistence_errors import BrokerSyncPersistenceError

_CASH_FLOW_ARCHIVE = TypeAdapter(CashFlowSyncResult)


async def finalize_broker_sync(
    session: AsyncSession,
    result: BrokerSyncResult,
    *,
    completed_at: datetime,
    cash_flow_result: CashFlowSyncResult | None = None,
) -> PortfolioSnapshotRecord:
    completed_at = require_utc(completed_at)
    try:
        validate_broker_sync_result(result)
    except InvalidBrokerSyncResult as error:
        raise BrokerSyncPersistenceError(str(error)) from None
    snapshot = result.portfolio_snapshot
    run = await session.scalar(
        select(BrokerSyncRun).where(BrokerSyncRun.id == result.sync_run_id).with_for_update()
    )
    if run is None or run.status != "ACTIVE":
        raise BrokerSyncPersistenceError("broker sync run is not active")
    if snapshot.portfolio_id != run.portfolio_id:
        raise BrokerSyncPersistenceError("snapshot portfolio does not match sync run")
    if cash_flow_result is not None and snapshot.status != "READY":
        raise BrokerSyncPersistenceError("cash flows require a READY broker snapshot")
    if snapshot.status == "STALE":
        stale_source = await session.scalar(
            select(PortfolioSnapshotRecord.id).where(
                PortfolioSnapshotRecord.id == snapshot.stale_from_snapshot_id,
                PortfolioSnapshotRecord.portfolio_id == snapshot.portfolio_id,
                PortfolioSnapshotRecord.status == "READY",
            )
        )
        if stale_source is None:
            raise BrokerSyncPersistenceError("STALE snapshot source is not same-portfolio READY")
    cash_by_currency(snapshot.cash)
    record = PortfolioSnapshotRecord(
        id=snapshot.id,
        portfolio_id=snapshot.portfolio_id,
        sync_run_id=run.id,
        stale_from_snapshot_id=snapshot.stale_from_snapshot_id,
        status=snapshot.status,
        as_of=snapshot.as_of,
        position_hash_version=POSITION_HASH_VERSION,
        position_hash=canonical_position_hash(snapshot.positions),
        order_hash_version=ORDER_HASH_VERSION,
        order_hash=canonical_order_hash(snapshot.open_orders),
        source_request_ids=list(snapshot.source_request_ids),
        source_observations=[
            observation.model_dump(mode="json") for observation in snapshot.source_observations
        ],
    )
    session.add(record)
    await session.flush([record])
    for cash in snapshot.cash:
        session.add(
            CashSnapshot(
                id=new_uuid7(),
                portfolio_snapshot_id=record.id,
                currency=cash.currency,
                cash_balance=cash.cash_balance,
                available_cash=cash.available_cash,
                provider_as_of=cash.provider_as_of,
            )
        )
    for position in snapshot.positions:
        session.add(
            PositionSnapshot(
                id=new_uuid7(),
                portfolio_snapshot_id=record.id,
                instrument_id=position.instrument_id,
                currency=position.currency,
                quantity=position.quantity,
                available_quantity=position.available_quantity,
                frozen_quantity=position.frozen_quantity,
                avg_cost=position.avg_cost,
                diluted_cost=position.diluted_cost,
                current_price=position.current_price,
                provider_as_of=position.provider_as_of,
            )
        )
    provider = await session.scalar(select(Account.provider).where(Account.id == run.account_id))
    if provider is None:
        raise BrokerSyncPersistenceError("broker account disappeared during finalization")
    for order in snapshot.open_orders:
        session.add(
            BrokerOrderSnapshot(
                id=new_uuid7(),
                portfolio_snapshot_id=record.id,
                provider=provider,
                provider_order_id=order.provider_order_id,
                instrument_id=order.instrument_id,
                side=order.side,
                quantity=order.quantity,
                filled_quantity=order.filled_quantity,
                price=order.price,
                status=order.status,
                updated_at=order.updated_at,
            )
        )
    quality: CashFlowQuality = "NOT_RUN"
    if cash_flow_result is not None:
        quality = await persist_cash_flows(session, run, provider, cash_flow_result)
    for index, trace in enumerate(result.attempt_traces, start=1):
        attempt = _attempt_record(run.id, index, trace)
        if cash_flow_result is not None and index == result.attempt_count:
            # A protected observation archive, outside the six-step reconciliation.
            # Keep each new read even when its ledger identity already exists; never
            # backfill the old immutable ledger or infer fees from these opaque codes.
            attempt.raw_facts["_cash_flows"] = {
                "schema_version": "cash_flow_observation_v1",
                "ledger_quality": quality,
                "result": _CASH_FLOW_ARCHIVE.dump_python(cash_flow_result, mode="json"),
            }
        session.add(attempt)
    await session.flush()
    run.status = snapshot.status
    run.completed_at = completed_at
    run.attempt_count = result.attempt_count
    run.cash_flow_quality = cast(str, quality)
    await session.flush()
    return record


def _attempt_record(run_id: object, attempt: int, trace: dict[str, object]) -> BrokerSyncAttempt:
    phase = trace.get("phase")
    reason = trace.get("reason")
    source_request_ids = trace.get("source_request_ids")
    source_observations = trace.get("source_observations")
    raw_facts = trace.get("raw_facts")
    return BrokerSyncAttempt(
        id=new_uuid7(),
        sync_run_id=run_id,
        attempt=attempt,
        phase=phase if isinstance(phase, str) else "unknown",
        completed=trace.get("completed") is True,
        reason=reason if isinstance(reason, str) else "unknown",
        source_request_ids=(
            list(source_request_ids)
            if isinstance(source_request_ids, tuple | list)
            and all(isinstance(item, str) for item in source_request_ids)
            else []
        ),
        source_observations=(
            [item for item in source_observations if isinstance(item, dict)]
            if isinstance(source_observations, tuple | list)
            else []
        ),
        raw_facts=(
            {str(key): value for key, value in raw_facts.items()}
            if isinstance(raw_facts, dict)
            else {}
        ),
    )
