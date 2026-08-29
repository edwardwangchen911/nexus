"""Read-only PostgreSQL coverage for the Provider startup catalog probe."""

import pytest
from nexus.persistence.session import session_factory
from nexus.provider_worker import _assert_provider_database_role
from nexus.settings import EngineSettings


@pytest.mark.asyncio
async def test_provider_role_probe_queries_catalogs_before_rejecting_admin_role(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        with pytest.raises(RuntimeError, match="PROVIDER_DATABASE_"):
            await _assert_provider_database_role(session)
