"""Authoritative full-snapshot reconciliation for broker simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StringConstraints
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.broker_simulation.domain import (
    CommandStatus,
    IntentStatus,
    OrderStatus,
    canonical_document_hash,
    require_command_transition,
    require_intent_transition,
    require_order_transition,
)
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerCommandRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
    BrokerReconciliationDiffRecord,
    BrokerReconciliationRunRecord,
)
from nexus.providers.broker.simulation_contracts import (
    SimulationPortfolioRequest,
    SimulationQueryRequest,
    SimulationSnapshotPage,
)
from nexus.providers.broker.simulation_gateway import SimulationGateway

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class ReconciliationError(ValueError):
    """Stable reconciliation admission or state failure."""


class ReconciliationExpectedSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    orders_hash: Hash64
    positions_hash: Hash64
    cash_hash: Hash64
    cash_flows_hash: Hash64


@dataclass(frozen=True)
class ReconciliationResult:
    run_id: UUID
    account_binding_id: UUID
    status: str
    diff_count: int


def reconciliation_snapshot_hashes(
    orders: SimulationSnapshotPage,
    portfolio: SimulationSnapshotPage,
    cash_flows: SimulationSnapshotPage,
) -> ReconciliationExpectedSnapshot:
    positions = tuple(item for item in portfolio.items if item.get("kind") == "position")
    cash = tuple(item for item in portfolio.items if item.get("kind") == "cash")
    return ReconciliationExpectedSnapshot(
        orders_hash=canonical_document_hash({"items": orders.items}),
        positions_hash=canonical_document_hash({"items": positions}),
        cash_hash=canonical_document_hash({"items": cash}),
        cash_flows_hash=canonical_document_hash({"items": cash_flows.items}),
    )


async def run_simulation_reconciliation(
    factory: async_sessionmaker[AsyncSession],
    *,
    run_id: UUID,
    gateway: SimulationGateway,
    expected: ReconciliationExpectedSnapshot,
    now: datetime,
) -> ReconciliationResult:
    """Poll all four fact domains and unlock only when the complete snapshot matches."""
    now = require_utc(now)
    async with factory() as session:
        run = await session.get(BrokerReconciliationRunRecord, run_id)
        if run is None:
            raise ReconciliationError("RECONCILIATION_RUN_NOT_FOUND")
        account_binding_id = run.account_binding_id
    orders = await _collect_pages(gateway, account_binding_id, kind="orders")
    portfolio = await gateway.portfolio(
        SimulationPortfolioRequest(account_binding_id=account_binding_id)
    )
    cash_flows = await _collect_pages(gateway, account_binding_id, kind="cash_flows")
    actual = reconciliation_snapshot_hashes(orders, portfolio, cash_flows)
    async with factory() as session, session.begin():
        run = await session.scalar(
            select(BrokerReconciliationRunRecord)
            .where(BrokerReconciliationRunRecord.id == run_id)
            .with_for_update()
        )
        if run is None or run.status not in {"pending", "running"}:
            raise ReconciliationError("RECONCILIATION_RUN_NOT_ACTIVE")
        binding = await session.scalar(
            select(BrokerAccountBindingRecord)
            .where(BrokerAccountBindingRecord.id == run.account_binding_id)
            .with_for_update()
        )
        if binding is None:
            raise ReconciliationError("ACCOUNT_BINDING_NOT_FOUND")
        run.status = "running"
        run.orders_hash = actual.orders_hash
        run.positions_hash = actual.positions_hash
        run.cash_hash = actual.cash_hash
        run.cash_flows_hash = actual.cash_flows_hash
        differences = tuple(
            (kind, getattr(expected, f"{kind}_hash"), getattr(actual, f"{kind}_hash"))
            for kind in ("orders", "positions", "cash", "cash_flows")
            if getattr(expected, f"{kind}_hash") != getattr(actual, f"{kind}_hash")
        )
        for kind, internal_hash, broker_hash in differences:
            session.add(
                BrokerReconciliationDiffRecord(
                    id=new_uuid7(),
                    run_id=run.id,
                    diff_kind=kind,
                    internal_hash=internal_hash,
                    broker_hash=broker_hash,
                    details={"safe_code": "SNAPSHOT_HASH_MISMATCH"},
                    resolved=False,
                )
            )
        commands = tuple(
            await session.scalars(
                select(BrokerCommandRecord)
                .where(
                    BrokerCommandRecord.account_binding_id == binding.id,
                    BrokerCommandRecord.status == "result_unknown",
                )
                .with_for_update()
            )
        )
        intent_ids: set[UUID] = set()
        for command in commands:
            order = await session.scalar(
                select(BrokerExecutionIntentOrderRecord)
                .where(BrokerExecutionIntentOrderRecord.id == command.intent_order_id)
                .with_for_update()
            )
            if order is None:
                raise ReconciliationError("RECONCILIATION_ORDER_NOT_FOUND")
            intent_ids.add(order.intent_id)
            if differences:
                require_command_transition(
                    CommandStatus.RESULT_UNKNOWN, CommandStatus.RECONCILIATION_REQUIRED
                )
                command.status = "reconciliation_required"
            else:
                require_command_transition(CommandStatus.RESULT_UNKNOWN, CommandStatus.RECONCILED)
                command.status = "reconciled"
                require_order_transition(OrderStatus(order.status), OrderStatus.ACKNOWLEDGED)
                order.status = "acknowledged"
            command.version += 1
        for intent_id in intent_ids:
            intent = await session.scalar(
                select(BrokerExecutionIntentRecord)
                .where(BrokerExecutionIntentRecord.id == intent_id)
                .with_for_update()
            )
            if intent is None or intent.status != "submission_unknown":
                raise ReconciliationError("RECONCILIATION_INTENT_NOT_UNKNOWN")
            target = IntentStatus.RECONCILING if differences else IntentStatus.RECONCILED
            require_intent_transition(IntentStatus.SUBMISSION_UNKNOWN, target)
            intent.status = target.value
            intent.version += 1
        run.completed_at = now
        if differences:
            run.status = "conflict"
            binding.locked = True
            binding.lock_reason = "reconciliation_conflict"
            binding.reconciliation_status = "conflict"
        else:
            run.status = "matched"
            binding.reconciliation_status = "matched"
            binding.last_reconciled_at = now
            binding.locked = False
            binding.lock_reason = None
        binding.version += 1
        await session.flush()
        return ReconciliationResult(
            run_id=run.id,
            account_binding_id=binding.id,
            status=run.status,
            diff_count=len(differences),
        )


async def acknowledge_reconciliation(
    session: AsyncSession,
    *,
    run_id: UUID,
    actor_id: str,
    acknowledged_at: datetime,
) -> ReconciliationResult:
    acknowledged_at = require_utc(acknowledged_at)
    run = await session.scalar(
        select(BrokerReconciliationRunRecord)
        .where(BrokerReconciliationRunRecord.id == run_id)
        .with_for_update()
    )
    if run is None or run.status not in {"matched", "conflict"}:
        raise ReconciliationError("RECONCILIATION_NOT_ACKNOWLEDGEABLE")
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .where(BrokerAccountBindingRecord.id == run.account_binding_id)
        .with_for_update()
    )
    if binding is None:
        raise ReconciliationError("ACCOUNT_BINDING_NOT_FOUND")
    prior_status = run.status
    run.status = "acknowledged"
    run.acknowledged_by = actor_id
    run.acknowledged_at = acknowledged_at
    if prior_status == "conflict":
        binding.locked = True
        binding.lock_reason = "acknowledged_reconciliation_conflict"
        binding.reconciliation_status = "acknowledged"
        binding.version += 1
    elif not (
        binding.reconciliation_status == "matched"
        and binding.last_reconciled_at is not None
        and not binding.locked
    ):
        raise ReconciliationError("MATCHED_SNAPSHOT_REQUIRED_FOR_UNLOCK")
    await session.flush()
    return ReconciliationResult(run.id, binding.id, run.status, 0)


async def _collect_pages(
    gateway: SimulationGateway,
    account_binding_id: UUID,
    *,
    kind: str,
) -> SimulationSnapshotPage:
    cursor: str | None = None
    seen: set[str] = set()
    items: list[dict[str, object]] = []
    last_page: SimulationSnapshotPage | None = None
    for _ in range(10):
        request = SimulationQueryRequest(
            account_binding_id=account_binding_id, cursor=cursor, limit=100
        )
        page = (
            await gateway.list_orders(request)
            if kind == "orders"
            else await gateway.cash_flows(request)
        )
        items.extend(page.items)
        last_page = page
        if not page.has_more:
            return page.model_copy(update={"items": tuple(items)})
        if page.next_cursor is None or page.next_cursor in seen:
            raise ReconciliationError("RECONCILIATION_CURSOR_CYCLE")
        seen.add(page.next_cursor)
        cursor = page.next_cursor
    if last_page is None:
        raise ReconciliationError("RECONCILIATION_EMPTY_PAGE_LOOP")
    raise ReconciliationError("RECONCILIATION_PAGE_LIMIT")
