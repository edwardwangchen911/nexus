import base64
from datetime import UTC, date, datetime
from uuid import UUID

import pytest
from helpers.broker_storage import _insert_run, _insert_snapshot, _seed_accounts
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_A = UUID("0198d400-5000-7000-8000-000000000001")
ACCOUNT_B = UUID("0198d400-5000-7000-8000-000000000002")
PORTFOLIO_A = UUID("0198d400-5000-7000-8000-000000000003")
PORTFOLIO_B = UUID("0198d400-5000-7000-8000-000000000004")
OLD_KEY = base64.urlsafe_b64encode(b"o" * 32).decode()
CURRENT_KEY = base64.urlsafe_b64encode(b"c" * 32).decode()
BLIND_KEY = base64.urlsafe_b64encode(b"b" * 32).decode()


@pytest.mark.asyncio
async def test_stale_snapshot_database_constraint_requires_same_portfolio_ready_source(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory)
    ready_run = UUID("0198d400-5000-7000-8000-000000000010")
    stale_run = UUID("0198d400-5000-7000-8000-000000000011")
    ready_snapshot = UUID("0198d400-5000-7000-8000-000000000012")
    async with factory() as session:
        async with session.begin():
            await _insert_run(session, ready_run, ACCOUNT_B, PORTFOLIO_B, "READY", "a")
            await _insert_run(session, stale_run, ACCOUNT_A, PORTFOLIO_A, "ACTIVE", "b")
            await _insert_snapshot(session, ready_snapshot, PORTFOLIO_B, ready_run, "READY", None)
    async with factory() as session:
        with pytest.raises(DBAPIError, match="same-portfolio READY"):
            async with session.begin():
                await _insert_snapshot(
                    session,
                    UUID("0198d400-5000-7000-8000-000000000013"),
                    PORTFOLIO_A,
                    stale_run,
                    "STALE",
                    ready_snapshot,
                )


@pytest.mark.asyncio
async def test_watermark_quality_version_and_monotonic_update_are_database_enforced(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory, include_second=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    "INSERT INTO broker_cash_flow_watermarks "
                    "(id, account_id, provider_business_date, identity_quality, version) "
                    "VALUES (:id, :account_id, :business_date, 'STABLE', 1)"
                ),
                {
                    "id": UUID("0198d400-5000-7000-8000-000000000020"),
                    "account_id": ACCOUNT_A,
                    "business_date": date(2026, 8, 23),
                },
            )
    async with factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    "UPDATE broker_cash_flow_watermarks SET version = 2, "
                    "identity_quality = 'FALLBACK_PARTIAL' WHERE account_id = :account_id"
                ),
                {"account_id": ACCOUNT_A},
            )
    async with factory() as session:
        downgraded = (
            await session.execute(
                text(
                    "SELECT provider_business_date, identity_quality, version "
                    "FROM broker_cash_flow_watermarks WHERE account_id = :account_id"
                ),
                {"account_id": ACCOUNT_A},
            )
        ).one()
    assert downgraded == (date(2026, 8, 23), "FALLBACK_PARTIAL", 2)
    for statement in (
        "UPDATE broker_cash_flow_watermarks SET version = 4, "
        "provider_business_date = '2026-08-24' WHERE account_id = :account_id",
        "UPDATE broker_cash_flow_watermarks SET version = 3, "
        "provider_business_date = '2026-08-22' WHERE account_id = :account_id",
        "UPDATE broker_cash_flow_watermarks SET version = 3, identity_quality = 'STABLE' "
        "WHERE account_id = :account_id",
    ):
        async with factory() as session:
            with pytest.raises(DBAPIError, match="regression"):
                await session.execute(text(statement), {"account_id": ACCOUNT_A})
                await session.commit()
    async with factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                text(
                    "INSERT INTO broker_cash_flow_watermarks "
                    "(id, account_id, provider_business_date, identity_quality, version) "
                    "VALUES (:id, :account_id, '2026-08-23', 'UNKNOWN', 0)"
                ),
                {
                    "id": UUID("0198d400-5000-7000-8000-000000000021"),
                    "account_id": ACCOUNT_A,
                },
            )
            await session.commit()
    async with factory() as session:
        with pytest.raises(DBAPIError, match="deletion is forbidden"):
            await session.execute(
                text("DELETE FROM broker_cash_flow_watermarks WHERE account_id = :account_id"),
                {"account_id": ACCOUNT_A},
            )
            await session.commit()
