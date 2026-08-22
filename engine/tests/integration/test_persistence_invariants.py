from datetime import UTC, datetime, timedelta, timezone

import pytest
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, StatementError


@pytest.mark.asyncio
async def test_datetime_persistence_requires_aware_values_and_reads_utc(
    migrated_database_url: str,
) -> None:
    """The ORM rejects naive time and canonicalizes a non-UTC instant through PostgreSQL."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            IntegrationStatus(
                provider="naive-provider",
                status="ready",
                last_success_at=datetime(2026, 8, 22, 9, 30),
            )
        )
        with pytest.raises(StatementError, match="timezone-aware"):
            await session.commit()
        await session.rollback()

    eastern = timezone(-timedelta(hours=4))
    async with factory() as session:
        status = IntegrationStatus(
            provider="aware-provider",
            status="ready",
            last_success_at=datetime(2026, 8, 22, 9, 30, tzinfo=eastern),
        )
        session.add(status)
        await session.commit()
        await session.refresh(status)

        assert status.last_success_at == datetime(2026, 8, 22, 13, 30, tzinfo=UTC)


@pytest.mark.asyncio
async def test_created_at_cannot_change_through_orm_or_raw_sql(
    migrated_database_url: str,
) -> None:
    """Audit creation instants resist both unit-of-work and direct SQL updates."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        status = IntegrationStatus(provider="immutable-provider", status="ready")
        session.add(status)
        await session.commit()
        await session.refresh(status)
        original_created_at = status.created_at

        status.created_at = original_created_at + timedelta(seconds=1)
        with pytest.raises(ValueError, match="created_at is immutable"):
            await session.commit()
        await session.rollback()

    bind = factory.kw["bind"]
    assert bind is not None
    async with bind.begin() as connection:
        with pytest.raises(DBAPIError, match="created_at is immutable"):
            await connection.execute(
                text(
                    "UPDATE integration_status SET created_at = CURRENT_TIMESTAMP "
                    "WHERE provider = :provider"
                ),
                {"provider": "immutable-provider"},
            )

    async with bind.connect() as connection:
        persisted_created_at = await connection.scalar(
            text("SELECT created_at FROM integration_status WHERE provider = :provider"),
            {"provider": "immutable-provider"},
        )

    assert persisted_created_at == original_created_at
