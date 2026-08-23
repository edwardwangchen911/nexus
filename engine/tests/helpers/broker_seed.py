from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.portfolio import (
    Account,
    Portfolio,
)
from nexus.persistence.models.runtime import IntegrationStatus

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_ID = UUID("0198d400-1000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("0198d400-1000-7000-8000-000000000002")
READY_SNAPSHOT_ID = UUID("0198d400-1000-7000-8000-000000000003")
INSTRUMENT_ID = UUID("0198d400-1000-7000-8000-000000000004")


async def _seed_account_and_portfolio(factory, *, account_status: str = "ACTIVE") -> UUID:
    async with factory() as session:
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
            )
        )
        session.add(
            Account(
                id=ACCOUNT_ID,
                provider="fosun",
                status=account_status,
            )
        )
        session.add(Portfolio(id=PORTFOLIO_ID, account_id=ACCOUNT_ID, name="Broker"))
        await session.commit()
        return INSTRUMENT_ID


async def _seed_fosun_capability(
    factory,
    *,
    expires_at: datetime,
    operations: tuple[str, ...] = ("fosun_sync_portfolio", "fosun_list_orders"),
) -> None:
    async with factory() as session:
        for operation in operations:
            session.add(
                ProviderCapability(
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash="a" * 64,
                    expires_at=expires_at,
                    schema_version="1",
                    payload={"operation": operation, "market_scope": "BROKER"},
                    created_at=NOW - timedelta(hours=1),
                )
            )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=NOW - timedelta(hours=2),
                last_success_at=NOW,
            )
        )
        await session.commit()
