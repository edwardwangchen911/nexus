from datetime import UTC, datetime

import pytest
from helpers.broker_simulation import approval_authority, preparation, seed_broker_authority
from nexus.broker_simulation.commands import dispatch_approved_intent
from nexus.broker_simulation.intents import approve_execution_intent, prepare_execution_intent
from nexus.broker_simulation.reconciliation import (
    acknowledge_reconciliation,
    reconciliation_snapshot_hashes,
    run_simulation_reconciliation,
)
from nexus.broker_simulation.webhooks import (
    InMemoryWebhookEvidenceStore,
    NotAdmittedWebhookAuthenticator,
    WebhookContractError,
    receive_webhook,
)
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerCommandRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
    BrokerWebhookReceiptRecord,
)
from nexus.persistence.session import session_factory
from nexus.providers.broker.simulation_contracts import (
    SimulationPortfolioRequest,
    SimulationQueryRequest,
)
from nexus.providers.broker.simulation_gateway import (
    FixtureSimulationGatewayClient,
    SimulationGatewayUnknownResult,
)
from nexus.settings import EngineSettings
from sqlalchemy import select

_NOW = datetime(2026, 8, 28, 14, 1, tzinfo=UTC)


async def _unknown(database_url: str):
    await seed_broker_authority(database_url)
    factory = session_factory(EngineSettings(database_url=database_url))
    authority = preparation()
    async with factory() as session:
        prepared = await prepare_execution_intent(session, authority)
        await session.commit()
    async with factory() as session:
        await approve_execution_intent(
            session,
            intent_id=prepared.intent_id,
            actor_id="private-user",
            expected_version=1,
            if_match=prepared.body_hash,
            request_body_hash="f" * 64,
            idempotency_key="approve-reconcile",
            approved_at=_NOW,
            authority=approval_authority(authority),
        )
        await session.commit()

    class UnknownGateway(FixtureSimulationGatewayClient):
        async def create_order(self, request):
            self._call_count += 1
            raise SimulationGatewayUnknownResult("fixture unknown")

    dispatch = await dispatch_approved_intent(
        factory,
        intent_id=prepared.intent_id,
        gateway=UnknownGateway(outcomes=()),
        authority=approval_authority(authority),
        now=_NOW,
    )
    assert dispatch.reconciliation_run_id is not None
    return factory, prepared.intent_id, dispatch.reconciliation_run_id


async def _fixture_expected(gateway: FixtureSimulationGatewayClient):
    account_binding_id = preparation().account_binding_id
    orders = await gateway.list_orders(
        SimulationQueryRequest(account_binding_id=account_binding_id)
    )
    portfolio = await gateway.portfolio(
        SimulationPortfolioRequest(account_binding_id=account_binding_id)
    )
    cash_flows = await gateway.cash_flows(
        SimulationQueryRequest(account_binding_id=account_binding_id)
    )
    return reconciliation_snapshot_hashes(orders, portfolio, cash_flows)


@pytest.mark.asyncio
async def test_complete_matched_poll_reconciles_unknown_and_unlocks(
    migrated_database_url: str,
) -> None:
    factory, intent_id, run_id = await _unknown(migrated_database_url)
    gateway = FixtureSimulationGatewayClient(outcomes=())
    expected = await _fixture_expected(gateway)

    result = await run_simulation_reconciliation(
        factory,
        run_id=run_id,
        gateway=gateway,
        expected=expected,
        now=datetime(2026, 8, 28, 14, 2, tzinfo=UTC),
    )

    assert result.status == "matched"
    async with factory() as session:
        intent = await session.get(BrokerExecutionIntentRecord, intent_id)
        binding = await session.get(
            BrokerAccountBindingRecord, preparation().account_binding_id
        )
        command = await session.scalar(select(BrokerCommandRecord))
        order = await session.scalar(select(BrokerExecutionIntentOrderRecord))
    assert intent is not None and intent.status == "reconciled"
    assert binding is not None and binding.locked is False
    assert command is not None and command.status == "reconciled"
    assert order is not None and order.status == "acknowledged"


@pytest.mark.asyncio
async def test_conflict_and_acknowledgement_cannot_unlock_without_match(
    migrated_database_url: str,
) -> None:
    factory, _, run_id = await _unknown(migrated_database_url)
    gateway = FixtureSimulationGatewayClient(outcomes=())
    expected = await _fixture_expected(gateway)
    conflict = expected.model_copy(update={"cash_hash": "9" * 64})

    result = await run_simulation_reconciliation(
        factory,
        run_id=run_id,
        gateway=gateway,
        expected=conflict,
        now=datetime(2026, 8, 28, 14, 2, tzinfo=UTC),
    )
    async with factory() as session:
        acknowledged = await acknowledge_reconciliation(
            session,
            run_id=run_id,
            actor_id="private-user",
            acknowledged_at=datetime(2026, 8, 28, 14, 3, tzinfo=UTC),
        )
        await session.commit()
    async with factory() as session:
        binding = await session.get(
            BrokerAccountBindingRecord, preparation().account_binding_id
        )

    assert result.status == "conflict"
    assert acknowledged.status == "acknowledged"
    assert binding is not None and binding.locked is True


@pytest.mark.asyncio
async def test_webhook_receipt_is_idempotent_and_collision_locks_binding(
    migrated_database_url: str,
) -> None:
    await seed_broker_authority(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = InMemoryWebhookEvidenceStore()
    body = (
        b'{"event_id":"synthetic-event-001","occurred_at":"2026-08-28T14:00:00Z",'
        b'"provider_order_ref":"synthetic-order-001","order_status":"working"}'
    )
    async with factory() as session:
        first = await receive_webhook(
            session,
            account_binding_id=preparation().account_binding_id,
            raw_body=body,
            headers={},
            received_at=_NOW,
            authenticator=NotAdmittedWebhookAuthenticator(),
            evidence_store=store,
        )
        await session.commit()
    async with factory() as session:
        replay = await receive_webhook(
            session,
            account_binding_id=preparation().account_binding_id,
            raw_body=body,
            headers={},
            received_at=_NOW,
            authenticator=NotAdmittedWebhookAuthenticator(),
            evidence_store=store,
        )
        await session.commit()
    assert first.duplicate is False and replay.duplicate is True
    assert len(store.objects) == 1
    async with factory() as session:
        with pytest.raises(WebhookContractError, match="WEBHOOK_EVENT_COLLISION"):
            await receive_webhook(
                session,
                account_binding_id=preparation().account_binding_id,
                raw_body=body.replace(b"working", b"filled"),
                headers={},
                received_at=_NOW,
                authenticator=NotAdmittedWebhookAuthenticator(),
                evidence_store=store,
            )
        await session.commit()
    async with factory() as session:
        receipts = tuple(await session.scalars(select(BrokerWebhookReceiptRecord)))
        binding = await session.get(
            BrokerAccountBindingRecord, preparation().account_binding_id
        )
    assert len(receipts) == 1
    assert binding is not None and binding.locked is True
    assert len(store.objects) == 2
