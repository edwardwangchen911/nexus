"""Prepared Fosun push authority is explicit, bounded, and archive-bindable."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from nexus.ops.fosun_local_refresh import _ensure_market_bindings
from nexus.ops.fosun_push_local import prepare_push_authority
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

PREPARED_AT = datetime(2026, 9, 9, 9, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_prepare_push_authority_adds_only_subscription_capability(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _ensure_market_bindings(
            session,
            authorized_at=PREPARED_AT - timedelta(seconds=1),
            delivery_mode="unknown",
        )
    async with factory() as session, session.begin():
        prepared = await prepare_push_authority(session, prepared_at=PREPARED_AT)

    document = json.loads(prepared.payload)
    assert prepared.sha256 == sha256(prepared.payload).hexdigest()
    assert document == {
        "capability_expires_at": "2027-09-09T09:00:00Z",
        "capability_id": document["capability_id"],
        "contract_hash": sha256(b"nexus.mac-local-fosun-read-only.v2").hexdigest(),
        "delivery_mode": "unknown",
        "operation": "fosun_subscribe_quotes",
        "prepared_at": "2026-09-09T09:00:00Z",
        "provider": "fosun",
        "schema": "nexus_fosun_push_authority_v1",
        "status_id": document["status_id"],
        "status_version": 1,
    }
    async with factory() as session:
        operations = tuple(
            await session.scalars(
                select(ProviderCapability.payload["operation"].as_string()).order_by(
                    ProviderCapability.payload["operation"].as_string()
                )
            )
        )
        count = await session.scalar(select(func.count()).select_from(ProviderCapability))
    assert count == 3
    assert operations == ("fosun_get_bars", "fosun_get_quotes", "fosun_subscribe_quotes")


@pytest.mark.asyncio
async def test_prepare_push_authority_requires_existing_read_only_status(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="FOSUN_PUSH_AUTHORITY_REQUIRED"):
            await prepare_push_authority(session, prepared_at=PREPARED_AT)

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProviderCapability)) == 0
