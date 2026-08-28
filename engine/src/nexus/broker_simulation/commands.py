"""Persist-before-send command dispatch with a strict no-resend uncertainty policy."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
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
from nexus.broker_simulation.intents import (
    BrokerApprovalAuthority,
    revalidate_approved_intent,
)
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerBatchApprovalRecord,
    BrokerCommandAttemptRecord,
    BrokerCommandRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
    BrokerOrderEventRecord,
    BrokerReconciliationRunRecord,
)
from nexus.persistence.models.market import InstrumentAlias
from nexus.providers.broker.simulation_contracts import SimulationCreateOrderRequest
from nexus.providers.broker.simulation_gateway import (
    GatewayCommandResult,
    SimulationGateway,
)


class BrokerCommandError(ValueError):
    """Stable fail-closed command admission error."""


@dataclass(frozen=True)
class DispatchSummary:
    intent_id: UUID
    status: str
    command_ids: tuple[UUID, ...]
    gateway_calls: int
    reconciliation_run_id: UUID | None


@dataclass(frozen=True)
class PreparedFollowupCommand:
    command_id: UUID
    kind: str
    status: str
    replayed: bool


@dataclass(frozen=True)
class _SendClaim:
    command_id: UUID
    order_id: UUID
    request: SimulationCreateOrderRequest | None
    existing_status: str | None


async def dispatch_approved_intent(
    factory: async_sessionmaker[AsyncSession],
    *,
    intent_id: UUID,
    gateway: SimulationGateway,
    authority: BrokerApprovalAuthority,
    now: datetime,
) -> DispatchSummary:
    """Send each canonical order once, stopping on the first non-success."""
    now = require_utc(now)
    command_ids: list[UUID] = []
    gateway_calls = 0
    async with factory() as session:
        intent = await session.get(BrokerExecutionIntentRecord, intent_id)
        if intent is None:
            raise BrokerCommandError("INTENT_NOT_FOUND")
        if intent.status in {"submission_unknown", "reconciling"}:
            active_reconciliation_run_id = await session.scalar(
                select(BrokerReconciliationRunRecord.id)
                .where(
                    BrokerReconciliationRunRecord.account_binding_id
                    == intent.account_binding_id
                )
                .order_by(BrokerReconciliationRunRecord.created_at.desc())
            )
            return DispatchSummary(
                intent_id=intent.id,
                status=intent.status,
                command_ids=(),
                gateway_calls=0,
                reconciliation_run_id=active_reconciliation_run_id,
            )
        order_ids = tuple(
            await session.scalars(
                select(BrokerExecutionIntentOrderRecord.id)
                .where(BrokerExecutionIntentOrderRecord.intent_id == intent_id)
                .order_by(BrokerExecutionIntentOrderRecord.ordinal)
            )
        )
    reconciliation_run_id: UUID | None = None
    for order_id in order_ids:
        claim = await _claim_create_attempt(
            factory,
            intent_id=intent_id,
            order_id=order_id,
            authority=authority,
            now=now,
        )
        command_ids.append(claim.command_id)
        if claim.request is None:
            if claim.existing_status == "acknowledged":
                continue
            if claim.existing_status in {"sending", "result_unknown"}:
                reconciliation_run_id = await _mark_unknown(
                    factory,
                    intent_id=intent_id,
                    order_id=order_id,
                    command_id=claim.command_id,
                    now=now,
                )
            break
        try:
            gateway_calls += 1
            response = await gateway.create_order(claim.request)
        except Exception:
            reconciliation_run_id = await _mark_unknown(
                factory,
                intent_id=intent_id,
                order_id=order_id,
                command_id=claim.command_id,
                now=now,
            )
            break
        should_continue = await _record_definite_result(
            factory,
            intent_id=intent_id,
            order_id=order_id,
            command_id=claim.command_id,
            response=response,
            now=now,
        )
        if not should_continue:
            break
    async with factory() as session:
        final_status = await session.scalar(
            select(BrokerExecutionIntentRecord.status).where(
                BrokerExecutionIntentRecord.id == intent_id
            )
        )
    if not isinstance(final_status, str):
        raise BrokerCommandError("INTENT_NOT_FOUND")
    return DispatchSummary(
        intent_id=intent_id,
        status=final_status,
        command_ids=tuple(command_ids),
        gateway_calls=gateway_calls,
        reconciliation_run_id=reconciliation_run_id,
    )


async def _claim_create_attempt(
    factory: async_sessionmaker[AsyncSession],
    *,
    intent_id: UUID,
    order_id: UUID,
    authority: BrokerApprovalAuthority,
    now: datetime,
) -> _SendClaim:
    async with factory() as session, session.begin():
        intent, _, _ = await revalidate_approved_intent(
            session, intent_id=intent_id, authority=authority, at=now
        )
        order = await session.scalar(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.id == order_id)
            .with_for_update()
        )
        if order is None or order.intent_id != intent.id:
            raise BrokerCommandError("INTENT_ORDER_NOT_FOUND")
        command = await session.scalar(
            select(BrokerCommandRecord)
            .where(
                BrokerCommandRecord.intent_order_id == order.id,
                BrokerCommandRecord.kind == "create",
            )
            .with_for_update()
        )
        if command is not None:
            return _SendClaim(
                command_id=command.id,
                order_id=order.id,
                request=None,
                existing_status=command.status,
            )
        approval = await session.scalar(
            select(BrokerBatchApprovalRecord).where(
                BrokerBatchApprovalRecord.intent_id == intent.id
            )
        )
        if approval is None or approval.expires_at <= now:
            raise BrokerCommandError("APPROVAL_EXPIRED")
        command_id = new_uuid7()
        command_payload = {
            "command_id": command_id,
            "account_binding_id": intent.account_binding_id,
            "intent_order_id": order.id,
            "kind": "create",
            "order_hash": order.order_hash,
            "approval_id": approval.id,
        }
        command = BrokerCommandRecord(
            id=command_id,
            intent_order_id=order.id,
            account_binding_id=intent.account_binding_id,
            approval_id=approval.id,
            kind="create",
            status="prepared",
            version=1,
            expected_event_sequence=await _current_event_sequence(
                session, intent.account_binding_id
            ),
            command_hash=canonical_document_hash(command_payload),
            idempotency_key=f"create:{intent.id}:{order.id}",
            lease_token=None,
            lease_expires_at=None,
        )
        session.add(command)
        await session.flush()
        require_command_transition(CommandStatus.PREPARED, CommandStatus.SENDING)
        command.status = "sending"
        command.version += 1
        command.lease_token = str(new_uuid7())
        command.lease_expires_at = now + timedelta(seconds=30)
        session.add(
            BrokerCommandAttemptRecord(
                id=new_uuid7(),
                command_id=command.id,
                attempt_number=1,
                started_at=now,
                completed_at=None,
                safe_outcome_code=None,
                result_hash=None,
            )
        )
        if intent.status == IntentStatus.APPROVED.value:
            require_intent_transition(IntentStatus.APPROVED, IntentStatus.DISPATCHING)
            intent.status = IntentStatus.DISPATCHING.value
            intent.version += 1
        await session.flush()
        instrument_code = await session.scalar(
            select(InstrumentAlias.symbol).where(
                InstrumentAlias.instrument_id == order.instrument_id,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= now.date(),
                (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= now.date())),
            )
        )
        if not isinstance(instrument_code, str):
            raise BrokerCommandError("INSTRUMENT_ALIAS_NOT_FOUND")
        request = SimulationCreateOrderRequest(
            command_id=command.id,
            account_binding_id=intent.account_binding_id,
            intent_order_id=order.id,
            idempotency_key=command.idempotency_key,
            instrument_code=instrument_code,
            side=order.side,  # type: ignore[arg-type]
            quantity=_decimal_string(order.quantity),
            limit_price=_decimal_string(order.limit_price),
        )
        return _SendClaim(
            command_id=command.id,
            order_id=order.id,
            request=request,
            existing_status=None,
        )


async def _record_definite_result(
    factory: async_sessionmaker[AsyncSession],
    *,
    intent_id: UUID,
    order_id: UUID,
    command_id: UUID,
    response: GatewayCommandResult,
    now: datetime,
) -> bool:
    async with factory() as session, session.begin():
        command = await session.scalar(
            select(BrokerCommandRecord)
            .where(BrokerCommandRecord.id == command_id)
            .with_for_update()
        )
        order = await session.scalar(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.id == order_id)
            .with_for_update()
        )
        intent = await session.scalar(
            select(BrokerExecutionIntentRecord)
            .where(BrokerExecutionIntentRecord.id == intent_id)
            .with_for_update()
        )
        attempt = await session.scalar(
            select(BrokerCommandAttemptRecord)
            .where(BrokerCommandAttemptRecord.command_id == command_id)
            .with_for_update()
        )
        if command is None or order is None or intent is None or attempt is None:
            raise BrokerCommandError("COMMAND_AUTHORITY_MISSING")
        if command.status != "sending" or response.command_id != command.id:
            raise BrokerCommandError("COMMAND_RESULT_CONFLICT")
        target = CommandStatus(response.outcome)
        require_command_transition(CommandStatus.SENDING, target)
        command.status = target.value
        command.version += 1
        command.lease_token = None
        command.lease_expires_at = None
        attempt.completed_at = now
        attempt.safe_outcome_code = response.outcome
        attempt.result_hash = canonical_document_hash(response)
        if response.outcome == "acknowledged":
            require_order_transition(OrderStatus(order.status), OrderStatus.ACKNOWLEDGED)
            order.status = "acknowledged"
            await _append_event(
                session,
                command=command,
                order=order,
                status="acknowledged",
                source="gateway",
                provider_order_digest=response.provider_order_digest,
                now=now,
            )
            remaining = await session.scalar(
                select(func.count(BrokerExecutionIntentOrderRecord.id)).where(
                    BrokerExecutionIntentOrderRecord.intent_id == intent.id,
                    BrokerExecutionIntentOrderRecord.status == "not_sent",
                )
            )
            if remaining == 0:
                require_intent_transition(IntentStatus.DISPATCHING, IntentStatus.COMPLETED)
                intent.status = "completed"
                intent.version += 1
            return True
        if response.outcome == "rejected":
            require_order_transition(OrderStatus(order.status), OrderStatus.REJECTED)
            order.status = "rejected"
            event_status = "rejected"
        else:
            event_status = "not_sent"
        await _append_event(
            session,
            command=command,
            order=order,
            status=event_status,
            source="gateway",
            provider_order_digest=None,
            now=now,
        )
        acknowledged = await session.scalar(
            select(func.count(BrokerExecutionIntentOrderRecord.id)).where(
                BrokerExecutionIntentOrderRecord.intent_id == intent.id,
                BrokerExecutionIntentOrderRecord.status == "acknowledged",
            )
        )
        target_intent = (
            IntentStatus.PARTIALLY_COMPLETED if acknowledged else IntentStatus.FAILED
        )
        require_intent_transition(IntentStatus.DISPATCHING, target_intent)
        intent.status = target_intent.value
        intent.version += 1
        return False


async def _mark_unknown(
    factory: async_sessionmaker[AsyncSession],
    *,
    intent_id: UUID,
    order_id: UUID,
    command_id: UUID,
    now: datetime,
) -> UUID:
    async with factory() as session, session.begin():
        command = await session.scalar(
            select(BrokerCommandRecord)
            .where(BrokerCommandRecord.id == command_id)
            .with_for_update()
        )
        order = await session.scalar(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.id == order_id)
            .with_for_update()
        )
        intent = await session.scalar(
            select(BrokerExecutionIntentRecord)
            .where(BrokerExecutionIntentRecord.id == intent_id)
            .with_for_update()
        )
        if command is None or order is None or intent is None:
            raise BrokerCommandError("COMMAND_AUTHORITY_MISSING")
        binding = await session.scalar(
            select(BrokerAccountBindingRecord)
            .where(BrokerAccountBindingRecord.id == command.account_binding_id)
            .with_for_update()
        )
        attempt = await session.scalar(
            select(BrokerCommandAttemptRecord)
            .where(BrokerCommandAttemptRecord.command_id == command.id)
            .with_for_update()
        )
        if binding is None or attempt is None:
            raise BrokerCommandError("COMMAND_AUTHORITY_MISSING")
        if command.status == "sending":
            require_command_transition(CommandStatus.SENDING, CommandStatus.RESULT_UNKNOWN)
            command.status = "result_unknown"
            command.version += 1
            command.lease_token = None
            command.lease_expires_at = None
            attempt.completed_at = now
            attempt.safe_outcome_code = "result_unknown"
            attempt.result_hash = canonical_document_hash(
                {"command_id": command.id, "outcome": "result_unknown"}
            )
        require_order_transition(OrderStatus(order.status), OrderStatus.RECONCILING)
        order.status = "reconciling"
        binding.locked = True
        binding.lock_reason = "result_unknown"
        binding.reconciliation_status = "pending"
        binding.version += 1
        if intent.status == "dispatching":
            require_intent_transition(IntentStatus.DISPATCHING, IntentStatus.SUBMISSION_UNKNOWN)
            intent.status = "submission_unknown"
            intent.version += 1
        run_id = new_uuid7()
        run_hash = canonical_document_hash(
            {
                "run_id": run_id,
                "account_binding_id": binding.id,
                "command_id": command.id,
                "trigger": "result_unknown",
            }
        )
        session.add(
            BrokerReconciliationRunRecord(
                id=run_id,
                account_binding_id=binding.id,
                trigger_reason="result_unknown",
                status="pending",
                started_at=now,
                completed_at=None,
                orders_hash=None,
                positions_hash=None,
                cash_hash=None,
                cash_flows_hash=None,
                run_hash=run_hash,
                acknowledged_by=None,
                acknowledged_at=None,
            )
        )
        await _append_event(
            session,
            command=command,
            order=order,
            status="reconciling",
            source="recovery",
            provider_order_digest=None,
            now=now,
        )
        await session.flush()
        return run_id


async def prepare_modify_command(
    session: AsyncSession,
    *,
    order_id: UUID,
    actor_id: str,
    idempotency_key: str,
    expected_event_sequence: int,
    reason: Literal["replace_price", "replace_quantity"],
) -> PreparedFollowupCommand:
    return await _prepare_followup_command(
        session,
        order_id=order_id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        expected_event_sequence=expected_event_sequence,
        kind="modify",
        reason=reason,
    )


async def prepare_cancel_command(
    session: AsyncSession,
    *,
    order_id: UUID,
    actor_id: str,
    idempotency_key: str,
    expected_event_sequence: int,
    reason: Literal["user_cancel", "risk_cancel"],
) -> PreparedFollowupCommand:
    return await _prepare_followup_command(
        session,
        order_id=order_id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        expected_event_sequence=expected_event_sequence,
        kind="cancel",
        reason=reason,
    )


async def _prepare_followup_command(
    session: AsyncSession,
    *,
    order_id: UUID,
    actor_id: str,
    idempotency_key: str,
    expected_event_sequence: int,
    kind: Literal["modify", "cancel"],
    reason: str,
) -> PreparedFollowupCommand:
    existing = await session.scalar(
        select(BrokerCommandRecord).where(
            BrokerCommandRecord.idempotency_key == idempotency_key
        )
    )
    if existing is not None:
        return PreparedFollowupCommand(existing.id, existing.kind, existing.status, True)
    order = await session.scalar(
        select(BrokerExecutionIntentOrderRecord)
        .where(BrokerExecutionIntentOrderRecord.id == order_id)
        .with_for_update()
    )
    if order is None or order.status not in {"acknowledged", "working", "partially_filled"}:
        raise BrokerCommandError("ORDER_NOT_MODIFIABLE")
    intent = await session.get(BrokerExecutionIntentRecord, order.intent_id)
    if intent is None:
        raise BrokerCommandError("INTENT_NOT_FOUND")
    approval = await session.scalar(
        select(BrokerBatchApprovalRecord)
        .where(BrokerBatchApprovalRecord.intent_id == intent.id)
        .order_by(BrokerBatchApprovalRecord.approved_at.desc())
    )
    if approval is None:
        raise BrokerCommandError("APPROVAL_REQUIRED")
    current_sequence = await _current_event_sequence(session, intent.account_binding_id)
    if current_sequence != expected_event_sequence:
        raise BrokerCommandError("EVENT_SEQUENCE_CONFLICT")
    command_id = new_uuid7()
    command = BrokerCommandRecord(
        id=command_id,
        intent_order_id=order.id,
        account_binding_id=intent.account_binding_id,
        approval_id=approval.id,
        kind=kind,
        status="prepared",
        version=1,
        expected_event_sequence=expected_event_sequence,
        command_hash=canonical_document_hash(
            {
                "command_id": command_id,
                "order_id": order.id,
                "actor_id": actor_id,
                "kind": kind,
                "reason": reason,
                "expected_event_sequence": expected_event_sequence,
            }
        ),
        idempotency_key=idempotency_key,
        lease_token=None,
        lease_expires_at=None,
    )
    session.add(command)
    await session.flush()
    return PreparedFollowupCommand(command.id, command.kind, command.status, False)


async def _append_event(
    session: AsyncSession,
    *,
    command: BrokerCommandRecord,
    order: BrokerExecutionIntentOrderRecord,
    status: str,
    source: str,
    provider_order_digest: str | None,
    now: datetime,
) -> None:
    latest = await session.scalar(
        select(BrokerOrderEventRecord)
        .where(BrokerOrderEventRecord.account_binding_id == command.account_binding_id)
        .order_by(BrokerOrderEventRecord.event_sequence.desc())
        .limit(1)
    )
    sequence = 1 if latest is None else latest.event_sequence + 1
    previous_hash = "0" * 64 if latest is None else latest.event_hash
    payload_hash = canonical_document_hash(
        {
            "command_id": command.id,
            "order_id": order.id,
            "status": status,
            "source": source,
            "sequence": sequence,
        }
    )
    event_hash = hashlib.sha256((previous_hash + payload_hash).encode()).hexdigest()
    session.add(
        BrokerOrderEventRecord(
            id=new_uuid7(),
            account_binding_id=command.account_binding_id,
            intent_order_id=order.id,
            command_id=command.id,
            event_sequence=sequence,
            source=source,
            order_status=status,
            provider_order_id_digest=provider_order_digest,
            occurred_at=now,
            payload_hash=payload_hash,
            previous_hash=previous_hash,
            event_hash=event_hash,
        )
    )


async def _current_event_sequence(session: AsyncSession, account_binding_id: UUID) -> int:
    value = await session.scalar(
        select(func.coalesce(func.max(BrokerOrderEventRecord.event_sequence), 0)).where(
            BrokerOrderEventRecord.account_binding_id == account_binding_id
        )
    )
    return int(value or 0)


def _decimal_string(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered
