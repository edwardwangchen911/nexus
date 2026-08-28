from datetime import UTC, datetime

import pytest
from helpers.broker_simulation import approval_authority, preparation, seed_broker_authority
from nexus.broker_simulation.commands import (
    BrokerCommandError,
    dispatch_approved_intent,
    prepare_cancel_command,
    prepare_modify_command,
)
from nexus.broker_simulation.intents import approve_execution_intent, prepare_execution_intent
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerCommandAttemptRecord,
    BrokerCommandRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
    BrokerReconciliationRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.providers.broker.simulation_gateway import (
    FixtureSimulationGatewayClient,
    SimulationGatewayUnknownResult,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select

_NOW = datetime(2026, 8, 28, 14, 1, tzinfo=UTC)


async def _approved(database_url: str, *, order_count: int = 1):
    await seed_broker_authority(database_url, order_count=order_count)
    factory = session_factory(EngineSettings(database_url=database_url))
    prepared_authority = preparation(order_count=order_count)
    async with factory() as session:
        prepared = await prepare_execution_intent(session, prepared_authority)
        await session.commit()
    async with factory() as session:
        await approve_execution_intent(
            session,
            intent_id=prepared.intent_id,
            actor_id="private-user",
            expected_version=1,
            if_match=prepared.body_hash,
            request_body_hash="f" * 64,
            idempotency_key=f"approve-{order_count}",
            approved_at=_NOW,
            authority=approval_authority(prepared_authority),
        )
        await session.commit()
    return factory, prepared, prepared_authority


@pytest.mark.asyncio
async def test_serial_dispatch_completes_acknowledged_batch(
    migrated_database_url: str,
) -> None:
    factory, prepared, authority = await _approved(migrated_database_url, order_count=2)
    gateway = FixtureSimulationGatewayClient(outcomes=("acknowledged", "acknowledged"))

    result = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=gateway,
        authority=approval_authority(authority),
        now=_NOW,
    )

    assert result.status == "completed"
    assert gateway.call_count == 2
    async with factory() as session:
        statuses = tuple(
            await session.scalars(
                select(BrokerExecutionIntentOrderRecord.status)
                .where(BrokerExecutionIntentOrderRecord.intent_id == prepared.intent_id)
                .order_by(BrokerExecutionIntentOrderRecord.ordinal)
            )
        )
    assert statuses == ("acknowledged", "acknowledged")


@pytest.mark.asyncio
async def test_dispatch_stops_after_first_rejection_and_leaves_rest_not_sent(
    migrated_database_url: str,
) -> None:
    factory, prepared, authority = await _approved(migrated_database_url, order_count=2)
    gateway = FixtureSimulationGatewayClient(outcomes=("rejected", "acknowledged"))

    result = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=gateway,
        authority=approval_authority(authority),
        now=_NOW,
    )

    assert result.status == "failed"
    assert gateway.call_count == 1
    async with factory() as session:
        statuses = tuple(
            await session.scalars(
                select(BrokerExecutionIntentOrderRecord.status)
                .where(BrokerExecutionIntentOrderRecord.intent_id == prepared.intent_id)
                .order_by(BrokerExecutionIntentOrderRecord.ordinal)
            )
        )
    assert statuses == ("rejected", "not_sent")


@pytest.mark.asyncio
async def test_unknown_result_locks_account_starts_reconciliation_and_never_resends(
    migrated_database_url: str,
) -> None:
    factory, prepared, authority = await _approved(migrated_database_url)

    class UnknownGateway(FixtureSimulationGatewayClient):
        async def create_order(self, request):
            self._call_count += 1
            raise SimulationGatewayUnknownResult("fixture unknown")

    gateway = UnknownGateway(outcomes=())
    first = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=gateway,
        authority=approval_authority(authority),
        now=_NOW,
    )
    second = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=gateway,
        authority=approval_authority(authority),
        now=_NOW,
    )

    assert first.status == second.status == "submission_unknown"
    assert gateway.call_count == 1
    async with factory() as session:
        intent = await session.get(BrokerExecutionIntentRecord, prepared.intent_id)
        binding = await session.get(BrokerAccountBindingRecord, authority.account_binding_id)
        attempt_count = await session.scalar(select(func.count(BrokerCommandAttemptRecord.id)))
        command_status = await session.scalar(select(BrokerCommandRecord.status))
        reconciliation_count = await session.scalar(
            select(func.count(BrokerReconciliationRunRecord.id))
        )
    assert intent is not None and intent.status == "submission_unknown"
    assert binding is not None and binding.locked is True
    assert attempt_count == 1
    assert command_status == "result_unknown"
    assert reconciliation_count == 1


@pytest.mark.asyncio
async def test_transport_failed_is_definite_and_not_retried_automatically(
    migrated_database_url: str,
) -> None:
    factory, prepared, authority = await _approved(migrated_database_url)
    gateway = FixtureSimulationGatewayClient(outcomes=("transport_failed",))

    result = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=gateway,
        authority=approval_authority(authority),
        now=_NOW,
    )

    assert result.status == "failed"
    assert gateway.call_count == 1
    async with factory() as session:
        attempt = await session.scalar(select(BrokerCommandAttemptRecord))
    assert attempt is not None and attempt.safe_outcome_code == "transport_failed"


@pytest.mark.asyncio
async def test_modify_and_cancel_are_explicit_approval_bound_commands(
    migrated_database_url: str,
) -> None:
    factory, prepared, authority = await _approved(migrated_database_url)
    await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=FixtureSimulationGatewayClient(outcomes=("acknowledged",)),
        authority=approval_authority(authority),
        now=_NOW,
    )
    async with factory() as session:
        order_id = await session.scalar(
            select(BrokerExecutionIntentOrderRecord.id).where(
                BrokerExecutionIntentOrderRecord.intent_id == prepared.intent_id
            )
        )
        assert order_id is not None
        modify = await prepare_modify_command(
            session,
            order_id=order_id,
            actor_id="private-user",
            idempotency_key="modify-fixture-1",
            expected_event_sequence=1,
            reason="replace_price",
        )
        with pytest.raises(BrokerCommandError, match="EVENT_SEQUENCE_CONFLICT"):
            await prepare_cancel_command(
                session,
                order_id=order_id,
                actor_id="private-user",
                idempotency_key="cancel-fixture-stale",
                expected_event_sequence=0,
                reason="user_cancel",
            )
        cancel = await prepare_cancel_command(
            session,
            order_id=order_id,
            actor_id="private-user",
            idempotency_key="cancel-fixture-1",
            expected_event_sequence=1,
            reason="user_cancel",
        )
        await session.commit()

    assert modify.kind == "modify" and modify.status == "prepared"
    assert cancel.kind == "cancel" and cancel.status == "prepared"
