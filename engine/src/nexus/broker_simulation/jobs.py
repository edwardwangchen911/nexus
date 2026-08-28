"""Closed broker-only Job handlers for read-only and simulation capabilities."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StringConstraints
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.broker_simulation.commands import dispatch_approved_intent
from nexus.broker_simulation.domain import BrokerOrderDocument
from nexus.broker_simulation.intents import BrokerApprovalAuthority
from nexus.broker_simulation.reconciliation import (
    ReconciliationExpectedSnapshot,
    run_simulation_reconciliation,
)
from nexus.jobs.daily_payloads import STAGE_SCHEMA, DailyStagePayload
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.jobs.worker import UnsafeJobError
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
)
from nexus.providers.broker.simulation_gateway import SimulationGateway

_UUID = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}$"
        )
    ),
]
_HASH64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_DISPATCH_SCHEMA = "broker_sim_dispatch_v1"
_RECONCILE_SCHEMA = "broker_sim_reconcile_v1"

CapabilityGuard = Callable[[AsyncSession, datetime], Awaitable[None]]


class BrokerSimulationDispatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    intent_id: _UUID


class BrokerSimulationReconcilePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: _UUID
    orders_hash: _HASH64
    positions_hash: _HASH64
    cash_hash: _HASH64
    cash_flows_hash: _HASH64


async def _offline_guard(_session: AsyncSession, _now: datetime) -> None:
    return None


def build_broker_job_registry(
    *,
    gateway: SimulationGateway,
    factory: async_sessionmaker[AsyncSession] | None,
    clock: Callable[[], datetime],
    capability_guard: CapabilityGuard = _offline_guard,
) -> JobRegistry:
    """Build the exact broker queue registry without importing Agent authorities."""

    async def read_only_not_activated(
        _context: JobHandlerContext,
    ) -> JobHandlerResult:
        return JobHandlerResult(
            outcome="skipped", result_code="READ_ONLY_EXTERNAL_ACTIVATION_BLOCKED"
        )

    async def dispatch(context: JobHandlerContext) -> JobHandlerResult:
        if factory is None:
            raise UnsafeJobError("BROKER_RUNTIME_FACTORY_MISSING")
        payload = cast(BrokerSimulationDispatchPayload, context.payload)
        now = clock()
        await capability_guard(context.session, now)
        authority = await load_current_approval_authority(
            context.session, intent_id=UUID(payload.intent_id)
        )
        result = await dispatch_approved_intent(
            factory,
            intent_id=UUID(payload.intent_id),
            gateway=gateway,
            authority=authority,
            now=now,
        )
        if result.status in {"submission_unknown", "reconciling"}:
            return JobHandlerResult(
                outcome="succeeded",
                result_code="SIMULATION_RESULT_UNKNOWN_RECONCILIATION_REQUIRED",
            )
        return JobHandlerResult(
            outcome="succeeded", result_code="SIMULATION_DISPATCH_RECORDED"
        )

    async def reconcile(context: JobHandlerContext) -> JobHandlerResult:
        if factory is None:
            raise UnsafeJobError("BROKER_RUNTIME_FACTORY_MISSING")
        payload = cast(BrokerSimulationReconcilePayload, context.payload)
        now = clock()
        await capability_guard(context.session, now)
        result = await run_simulation_reconciliation(
            factory,
            run_id=UUID(payload.run_id),
            gateway=gateway,
            expected=ReconciliationExpectedSnapshot(
                orders_hash=payload.orders_hash,
                positions_hash=payload.positions_hash,
                cash_hash=payload.cash_hash,
                cash_flows_hash=payload.cash_flows_hash,
            ),
            now=now,
        )
        return JobHandlerResult(
            outcome="succeeded",
            result_code=(
                "SIMULATION_RECONCILIATION_MATCHED"
                if result.status == "matched"
                else "SIMULATION_RECONCILIATION_CONFLICT"
            ),
        )

    return JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.BROKER_SYNC,
                schema_version=STAGE_SCHEMA,
                payload_model=DailyStagePayload,
                queue=QueueName.BROKER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace="broker.sync",
                handler=read_only_not_activated,
            ),
            JobHandlerSpec(
                job_type=JobType.BROKER_RECONCILE,
                schema_version=STAGE_SCHEMA,
                payload_model=DailyStagePayload,
                queue=QueueName.BROKER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace="broker.reconcile",
                handler=read_only_not_activated,
            ),
            JobHandlerSpec(
                job_type=JobType.BROKER_SIMULATION_DISPATCH,
                schema_version=_DISPATCH_SCHEMA,
                payload_model=BrokerSimulationDispatchPayload,
                queue=QueueName.BROKER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace="broker.simulation.dispatch",
                handler=dispatch,
            ),
            JobHandlerSpec(
                job_type=JobType.BROKER_SIMULATION_RECONCILE,
                schema_version=_RECONCILE_SCHEMA,
                payload_model=BrokerSimulationReconcilePayload,
                queue=QueueName.BROKER,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace="broker.simulation.reconcile",
                handler=reconcile,
            ),
        )
    )


async def load_current_approval_authority(
    session: AsyncSession, *, intent_id: UUID
) -> BrokerApprovalAuthority:
    """Load only persisted current facts; request payloads cannot supply order authority."""
    intent = await session.get(BrokerExecutionIntentRecord, intent_id)
    if intent is None:
        raise UnsafeJobError("SIMULATION_INTENT_NOT_FOUND")
    binding = await session.get(BrokerAccountBindingRecord, intent.account_binding_id)
    if binding is None:
        raise UnsafeJobError("SIMULATION_BINDING_NOT_FOUND")
    records = tuple(
        await session.scalars(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.intent_id == intent.id)
            .order_by(BrokerExecutionIntentOrderRecord.ordinal)
        )
    )
    orders = tuple(
        BrokerOrderDocument.model_validate(
            {
                "instrument_id": record.instrument_id,
                "side": record.side,
                "quantity": int(record.quantity),
                "limit_price": record.limit_price,
                "currency": record.currency,
                "order_type": record.order_type,
                "time_in_force": record.time_in_force,
                "session": record.session,
                "sort_key": record.ordinal,
            }
        )
        for record in records
    )
    return BrokerApprovalAuthority(
        account_binding_id=binding.id,
        account_binding_version=binding.version,
        account_snapshot_identity_hash=binding.snapshot_identity_hash,
        capability_subject=binding.capability_subject,
        evidence_hashes=tuple(intent.evidence_hashes),
        risk_policy_hash=intent.risk_policy_hash,
        execution_model_hash=intent.execution_model_hash,
        orders=orders,
    )
