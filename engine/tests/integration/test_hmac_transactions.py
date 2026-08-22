import asyncio

import pytest
from fastapi import Request
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.api.auth import VerifiedServiceActor, verify_hmac_v1
from nexus.api.errors import ErrorCode, NexusError
from nexus.persistence.models.runtime import AuditEvent, UsedNonce
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


def make_request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/internal/v1/test",
            "raw_path": b"/internal/v1/test",
            "query_string": b"",
            "headers": [(key.lower().encode(), value.encode()) for key, value in headers.items()],
        }
    )


def hmac_settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


@pytest.mark.asyncio
async def test_hmac_nonce_and_business_record_share_one_outer_transaction(
    migrated_database_url: str,
) -> None:
    """A replay cannot add business data, and a failed business action leaves no nonce."""
    settings = hmac_settings(migrated_database_url)
    factory = session_factory(settings)
    body = b"{}"
    first_headers = build_test_hmac_headers("POST", "/internal/v1/test", body)

    async with factory() as session:
        async with session.begin():
            actor = await verify_hmac_v1(make_request(first_headers), body, session, settings)
            assert actor == VerifiedServiceActor(
                actor_id="nexus-test-user",
                request_id=actor.request_id,
                key_id=TEST_HMAC_KEY_ID,
            )
            session.add(
                AuditEvent(
                    actor_id=actor.actor_id,
                    action="internal_test_succeeded",
                    schema_version="1",
                    payload={"result": "ok"},
                )
            )

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 1
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1

    async with factory() as session:
        async with session.begin():
            with pytest.raises(NexusError) as exc_info:
                await verify_hmac_v1(make_request(first_headers), body, session, settings)
            assert exc_info.value.code is ErrorCode.UNAUTHENTICATED

    failed_headers = build_test_hmac_headers(
        "POST", "/internal/v1/test", body, nonce="0198f7a0-1111-7777-8111-111111111116"
    )
    with pytest.raises(RuntimeError, match="business failed"):
        async with factory() as session:
            async with session.begin():
                failed_actor = await verify_hmac_v1(
                    make_request(failed_headers), body, session, settings
                )
                session.add(
                    AuditEvent(
                        actor_id=failed_actor.actor_id,
                        action="internal_test_failed",
                        schema_version="1",
                        payload={"result": "rollback"},
                    )
                )
                raise RuntimeError("business failed")

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 1
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_previous_hmac_key_verifies_during_rotation(migrated_database_url: str) -> None:
    """The previous key remains verification-only while BFF signing uses the current key."""
    previous_secret = "nexus-previous-hmac-key-32-bytes!"
    settings = EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=migrated_database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
        NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID="test-previous",
        NEXUS_ENGINE_HMAC_PREVIOUS_SECRET=previous_secret,
    )
    factory = session_factory(settings)
    body = b"{}"
    headers = build_test_hmac_headers(
        "POST", "/internal/v1/test", body, key_id="test-previous", secret=previous_secret
    )

    async with factory() as session:
        async with session.begin():
            actor = await verify_hmac_v1(make_request(headers), body, session, settings)
            assert actor.key_id == "test-previous"

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 1


@pytest.mark.asyncio
async def test_concurrent_replay_allows_exactly_one_business_side_effect(
    migrated_database_url: str,
) -> None:
    """Concurrent identical requests leave one durable nonce and one business record."""
    settings = hmac_settings(migrated_database_url)
    factory = session_factory(settings)
    body = b"{}"
    headers = build_test_hmac_headers("POST", "/internal/v1/test", body)

    async def attempt() -> bool:
        async with factory() as session:
            async with session.begin():
                try:
                    actor = await verify_hmac_v1(make_request(headers), body, session, settings)
                except NexusError:
                    return False
                session.add(
                    AuditEvent(
                        actor_id=actor.actor_id,
                        action="concurrent_hmac_test",
                        schema_version="1",
                        payload={"result": "ok"},
                    )
                )
                return True

    outcomes = await asyncio.gather(attempt(), attempt())
    assert outcomes.count(True) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 1
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
