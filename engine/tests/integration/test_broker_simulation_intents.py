import asyncio
import inspect
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.broker_simulation.domain import BrokerOrderDocument
from nexus.broker_simulation.intents import (
    BrokerApprovalAuthority,
    BrokerIntentAuthorityChanged,
    BrokerIntentPreparation,
    approve_execution_intent,
    prepare_execution_intent,
)
from nexus.broker_simulation.risk import (
    BrokerRiskContext,
    BrokerRiskInstrument,
    BrokerRiskPosition,
)
from nexus.persistence.models.broker_simulation import BrokerAccountBindingRecord
from nexus.persistence.models.market import Instrument
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select

_NOW = datetime(2026, 8, 28, 14, tzinfo=UTC)
_BINDING_ID = UUID(int=1001)
_INSTRUMENT_ID = UUID(int=1002)


async def _seed(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        session.add_all(
            (
                Instrument(
                    id=_INSTRUMENT_ID,
                    asset_type="common_stock",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                ),
                BrokerAccountBindingRecord(
                    id=_BINDING_ID,
                    provider="fosun",
                    environment="simulation",
                    account_type="simulation",
                    redacted_label="Fixture SIM",
                    external_account_id_digest="a" * 64,
                    snapshot_identity_hash="b" * 64,
                    capability_subject="fixture-contract-v1",
                    capability_expires_at=datetime(2026, 8, 28, 15, tzinfo=UTC),
                    reconciliation_status="matched",
                    locked=False,
                    lock_reason=None,
                    version=1,
                    last_reconciled_at=_NOW,
                ),
            )
        )
        await session.commit()


def _preparation(*, proposal_group_id: UUID = UUID(int=1100)) -> BrokerIntentPreparation:
    order = BrokerOrderDocument(
        instrument_id=_INSTRUMENT_ID,
        side="buy",
        quantity=10,
        limit_price=Decimal("100.00"),
        sort_key=1,
    )
    return BrokerIntentPreparation(
        proposal_group_id=proposal_group_id,
        strategy_decision_id=UUID(int=1101),
        portfolio_snapshot_id=UUID(int=1102),
        evidence_hashes=("b" * 64, "c" * 64),
        risk_policy_hash="d" * 64,
        execution_model_hash="e" * 64,
        account_binding_id=_BINDING_ID,
        orders=(order,),
        risk_context=BrokerRiskContext(
            available_cash=Decimal("10000"),
            nav=Decimal("20000"),
            drawdown_locked=False,
            minimum_cash_ratio=Decimal("0.10"),
            single_name_limit=Decimal("0.20"),
            sector_limit=Decimal("0.30"),
            positions=(
                BrokerRiskPosition(instrument_id=_INSTRUMENT_ID, quantity=Decimal("0")),
            ),
            instruments=(
                BrokerRiskInstrument(
                    instrument_id=_INSTRUMENT_ID,
                    asset_type="common_stock",
                    sector="technology",
                    whitelisted=True,
                    existing_notional=Decimal("0"),
                ),
            ),
            sector_notionals={"technology": Decimal("0")},
        ),
        prepared_at=_NOW,
        quote_expires_at=datetime(2026, 8, 28, 14, 2, tzinfo=UTC),
        session_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
    )


def _approval(preparation: BrokerIntentPreparation) -> BrokerApprovalAuthority:
    return BrokerApprovalAuthority(
        account_binding_id=preparation.account_binding_id,
        account_binding_version=1,
        account_snapshot_identity_hash="b" * 64,
        capability_subject="fixture-contract-v1",
        evidence_hashes=preparation.evidence_hashes,
        risk_policy_hash=preparation.risk_policy_hash,
        execution_model_hash=preparation.execution_model_hash,
        orders=preparation.orders,
    )


@pytest.mark.asyncio
async def test_prepare_and_approve_are_server_authority_bound(
    migrated_database_url: str,
) -> None:
    await _seed(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    preparation = _preparation()
    async with factory() as session:
        prepared = await prepare_execution_intent(session, preparation)
        await session.commit()
    async with factory() as session:
        approved = await approve_execution_intent(
            session,
            intent_id=prepared.intent_id,
            actor_id="private-user",
            expected_version=prepared.version,
            if_match=prepared.body_hash,
            request_body_hash="f" * 64,
            idempotency_key="approve-fixture-1",
            approved_at=datetime(2026, 8, 28, 14, 1, tzinfo=UTC),
            authority=_approval(preparation),
        )
        await session.commit()

    assert prepared.status == "prepared"
    assert approved.status == "approved"
    assert approved.version == 2
    assert approved.replayed is False


@pytest.mark.asyncio
async def test_approval_rejects_binding_snapshot_drift(migrated_database_url: str) -> None:
    await _seed(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    preparation = _preparation()
    async with factory() as session:
        prepared = await prepare_execution_intent(session, preparation)
        binding = await session.scalar(
            select(BrokerAccountBindingRecord).where(BrokerAccountBindingRecord.id == _BINDING_ID)
        )
        assert binding is not None
        binding.snapshot_identity_hash = "9" * 64
        binding.version = 2
        await session.commit()

    async with factory() as session:
        with pytest.raises(BrokerIntentAuthorityChanged, match="INTENT_AUTHORITY_CHANGED"):
            await approve_execution_intent(
                session,
                intent_id=prepared.intent_id,
                actor_id="private-user",
                expected_version=1,
                if_match=prepared.body_hash,
                request_body_hash="f" * 64,
                idempotency_key="approve-fixture-drift",
                approved_at=datetime(2026, 8, 28, 14, 1, tzinfo=UTC),
                authority=_approval(preparation),
            )


@pytest.mark.asyncio
async def test_concurrent_identical_approval_yields_one_fact_and_stable_replay(
    migrated_database_url: str,
) -> None:
    await _seed(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    preparation = _preparation()
    async with factory() as session:
        prepared = await prepare_execution_intent(session, preparation)
        await session.commit()

    async def approve_once():
        async with factory() as session:
            result = await approve_execution_intent(
                session,
                intent_id=prepared.intent_id,
                actor_id="private-user",
                expected_version=1,
                if_match=prepared.body_hash,
                request_body_hash="f" * 64,
                idempotency_key="approve-fixture-concurrent",
                approved_at=datetime(2026, 8, 28, 14, 1, tzinfo=UTC),
                authority=_approval(preparation),
            )
            await session.commit()
            return result

    first, second = await asyncio.gather(approve_once(), approve_once())

    assert {first.replayed, second.replayed} == {False, True}
    assert first.approval_id == second.approval_id


def test_browser_order_facts_are_absent_from_approval_signature() -> None:
    parameters = set(inspect.signature(approve_execution_intent).parameters)
    assert {"price", "quantity", "symbol", "account_id"}.isdisjoint(parameters)
