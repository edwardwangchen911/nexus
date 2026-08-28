"""Shared persisted authorities for broker-simulation integration tests."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.broker_simulation.domain import BrokerOrderDocument
from nexus.broker_simulation.intents import BrokerApprovalAuthority, BrokerIntentPreparation
from nexus.broker_simulation.risk import (
    BrokerRiskContext,
    BrokerRiskInstrument,
    BrokerRiskPosition,
)
from nexus.persistence.models.broker_simulation import BrokerAccountBindingRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

NOW = datetime(2026, 8, 28, 14, tzinfo=UTC)
BINDING_ID = UUID(int=1001)
INSTRUMENT_IDS = (UUID(int=1002), UUID(int=1003))


async def seed_broker_authority(database_url: str, *, order_count: int = 1) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        session.add_all(
            Instrument(
                id=instrument_id,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="large_cap",
            )
            for instrument_id in INSTRUMENT_IDS[:order_count]
        )
        session.add_all(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol="AAPL.US" if index == 0 else "MSFT.US",
                alias_type="ticker",
                valid_from=NOW.date(),
                valid_to=None,
            )
            for index, instrument_id in enumerate(INSTRUMENT_IDS[:order_count])
        )
        session.add(
            BrokerAccountBindingRecord(
                id=BINDING_ID,
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
                last_reconciled_at=NOW,
            )
        )
        await session.commit()


def preparation(*, order_count: int = 1) -> BrokerIntentPreparation:
    orders = tuple(
        BrokerOrderDocument(
            instrument_id=instrument_id,
            side="buy",
            quantity=10,
            limit_price=Decimal("100.00"),
            sort_key=ordinal,
        )
        for ordinal, instrument_id in enumerate(INSTRUMENT_IDS[:order_count], start=1)
    )
    return BrokerIntentPreparation(
        proposal_group_id=UUID(int=1100 + order_count),
        strategy_decision_id=UUID(int=1101),
        portfolio_snapshot_id=UUID(int=1102),
        evidence_hashes=("b" * 64, "c" * 64),
        risk_policy_hash="d" * 64,
        execution_model_hash="e" * 64,
        account_binding_id=BINDING_ID,
        orders=orders,
        risk_context=BrokerRiskContext(
            available_cash=Decimal("10000"),
            nav=Decimal("20000"),
            drawdown_locked=False,
            minimum_cash_ratio=Decimal("0.10"),
            single_name_limit=Decimal("0.20"),
            sector_limit=Decimal("0.30"),
            positions=tuple(
                BrokerRiskPosition(instrument_id=item, quantity=Decimal("0"))
                for item in INSTRUMENT_IDS[:order_count]
            ),
            instruments=tuple(
                BrokerRiskInstrument(
                    instrument_id=item,
                    asset_type="common_stock",
                    sector="technology" if index == 0 else "healthcare",
                    whitelisted=True,
                    existing_notional=Decimal("0"),
                )
                for index, item in enumerate(INSTRUMENT_IDS[:order_count])
            ),
            sector_notionals={"technology": Decimal("0"), "healthcare": Decimal("0")},
        ),
        prepared_at=NOW,
        quote_expires_at=datetime(2026, 8, 28, 14, 2, tzinfo=UTC),
        session_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
    )


def approval_authority(authority: BrokerIntentPreparation) -> BrokerApprovalAuthority:
    return BrokerApprovalAuthority(
        account_binding_id=authority.account_binding_id,
        account_binding_version=1,
        account_snapshot_identity_hash="b" * 64,
        capability_subject="fixture-contract-v1",
        evidence_hashes=authority.evidence_hashes,
        risk_policy_hash=authority.risk_policy_hash,
        execution_model_hash=authority.execution_model_hash,
        orders=authority.orders,
    )
