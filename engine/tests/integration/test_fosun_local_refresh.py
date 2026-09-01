from datetime import UTC, datetime, timedelta

import pytest
from nexus.ops.fosun_local_refresh import _ensure_market_bindings, _insert_calendar
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="test-key",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET="test-secret",
    )


@pytest.mark.asyncio
async def test_local_market_bindings_are_reusable_across_successful_refreshes(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    first_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    try:
        async with factory() as session, session.begin():
            first_quote, first_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at,
                delivery_mode="unknown",
            )
        async with factory() as session, session.begin():
            second_quote, second_bars = await _ensure_market_bindings(
                session,
                authorized_at=first_at + timedelta(minutes=1),
                delivery_mode="unknown",
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second_quote.provider_capability_id == first_quote.provider_capability_id
    assert second_bars.provider_capability_id == first_bars.provider_capability_id
    assert second_quote.provider_status_id == first_quote.provider_status_id
    assert second_quote.provider_status_version == first_quote.provider_status_version + 1


@pytest.mark.asyncio
async def test_local_calendar_authority_is_reused_for_the_same_completed_window(
    migrated_database_url: str,
) -> None:
    factory = session_factory(_settings(migrated_database_url))
    published_at = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    rows = (
        (
            published_at.date(),
            published_at - timedelta(hours=10),
            published_at - timedelta(hours=3),
        ),
    )
    try:
        async with factory() as session, session.begin():
            first = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at,
            )
        async with factory() as session, session.begin():
            second = await _insert_calendar(
                session,
                rows=rows,
                range_start=published_at.date(),
                range_end=published_at.date(),
                published_at=published_at + timedelta(minutes=1),
            )
    finally:
        await factory.kw["bind"].dispose()

    assert second.id == first.id
