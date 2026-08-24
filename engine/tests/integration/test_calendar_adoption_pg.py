from datetime import UTC, datetime

import pytest
from helpers.portfolio_api import settings
from nexus.diagnostics.calendar_policy import (
    XNYS_CALENDAR_ADOPTION_ID,
    XNYS_CALENDAR_ADOPTION_PAYLOAD,
    load_xnys_calendar_adoption,
)
from nexus.persistence.session import session_factory


@pytest.mark.asyncio
async def test_migration_seeds_loadable_calendar_adoption(
    migrated_database_url: str,
) -> None:
    observation_time = datetime.now(UTC)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        adoption = await load_xnys_calendar_adoption(session, observation_time)

    assert adoption.row_id == XNYS_CALENDAR_ADOPTION_ID
    assert adoption.payload.model_dump(mode="json") == XNYS_CALENDAR_ADOPTION_PAYLOAD
    assert adoption.created_at <= observation_time
    assert adoption.ref.available_at == adoption.created_at
