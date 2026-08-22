"""Authenticated integration-status API contract tests."""

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IdempotencyRecord, IntegrationStatus, UsedNonce
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

_CLOCK = datetime(2026, 8, 23, 12, tzinfo=UTC)
_CONTRACT_HASH = "a" * 64


def _settings(migrated_database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=migrated_database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _headers(method: str, path: str, body: bytes = b"") -> dict[str, str]:
    headers = build_test_hmac_headers(method, path, body, nonce=str(new_uuid7()))
    if body:
        headers["Content-Type"] = "application/json"
    return headers


def _mode_headers(
    path: str,
    body: bytes,
    *,
    idempotency_key: str,
    version: int,
) -> dict[str, str]:
    headers = _headers("POST", path, body)
    headers["Idempotency-Key"] = idempotency_key
    headers["If-Match"] = f'"{version}"'
    return headers


def _test_headers(
    path: str,
    body: bytes = b"",
    *,
    idempotency_key: str,
    version: int,
) -> dict[str, str]:
    headers = _headers("POST", path, body)
    headers["Idempotency-Key"] = idempotency_key
    headers["If-Match"] = f'"{version}"'
    return headers


def _app(migrated_database_url: str):
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _CLOCK
    return app


async def _add_capability(
    migrated_database_url: str,
    *,
    provider: str = "fosun",
    contract_hash: str = _CONTRACT_HASH,
    expires_at: datetime,
    created_at: datetime | None = None,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        capability = ProviderCapability(
            provider=provider,
            capability="READ_ONLY",
            contract_hash=contract_hash,
            expires_at=expires_at,
            schema_version="1",
            payload={},
        )
        if created_at is not None:
            capability.created_at = created_at
        session.add(capability)
        await session.commit()


async def _add_read_only_status(migrated_database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(IntegrationStatus(provider="fosun", status="read_only"))
        await session.commit()


@pytest.mark.asyncio
async def test_lists_only_redacted_fixture_integration_statuses(
    migrated_database_url: str,
) -> None:
    """A missing fixture status or leaked configuration field must fail this test."""
    app = create_engine_app(_settings(migrated_database_url))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.get(
            "/internal/v1/integrations",
            headers=_headers("GET", "/internal/v1/integrations"),
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["schema_version"] == "1"
    statuses = {status["provider"]: status for status in payload["data"]}
    assert statuses["fosun"] == {
        "provider": "fosun",
        "mode": "fixture",
        "quality": "PASS",
        "as_of": "2026-08-22T00:00:00Z",
        "capability_expires_at": None,
        "contract_hash": None,
        "next_action": "Read-only admission is required.",
        "version": 0,
    }
    assert {provider: status["mode"] for provider, status in statuses.items()} == {
        "fosun": "fixture",
        "alpaca": "disabled",
        "sec": "disabled",
        "fred": "disabled",
        "openai": "disabled",
    }
    assert not any(
        forbidden in field.lower()
        for status in statuses.values()
        for field in status
        for forbidden in ("secret", "credential", "endpoint", "account", "header", "token", "key")
    )


@pytest.mark.asyncio
async def test_fixture_connectivity_returns_refreshed_status_and_replay_is_rejected(
    migrated_database_url: str,
) -> None:
    """A route without the HMAC dependency or a non-fixture result must fail this test."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/test"
    headers = _test_headers(path, idempotency_key="fixture-first", version=0)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, headers=headers)
        replay = await client.post(path, headers=headers)

    assert response.status_code == 200
    assert response.json()["data"]["provider"] == "fosun"
    assert response.json()["data"]["mode"] == "fixture"
    assert response.json()["data"]["as_of"] == "2026-08-23T12:00:00Z"
    assert response.json()["meta"]["as_of"] == "2026-08-23T12:00:00Z"
    assert replay.status_code == 401
    assert replay.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_fixture_connectivity_refresh_advances_status_and_meta_time(
    migrated_database_url: str,
) -> None:
    """Connectivity refresh must persist and expose the injected current UTC clock."""
    clock = {"now": _CLOCK}
    app = _app(migrated_database_url)
    app.state.utc_clock = lambda: clock["now"]
    path = "/internal/v1/integrations/fosun/test"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-first", version=0),
        )
        clock["now"] = _CLOCK + timedelta(minutes=5)
        second = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-second", version=1),
        )

    assert first.status_code == second.status_code == 200
    assert first.json()["data"]["as_of"] == "2026-08-23T12:00:00Z"
    assert second.json()["data"]["as_of"] == "2026-08-23T12:05:00Z"
    assert second.json()["meta"]["as_of"] == "2026-08-23T12:05:00Z"
    assert second.json()["data"]["version"] == 2
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        status = await session.scalar(
            select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
        )
    assert status is not None
    assert status.last_success_at == _CLOCK + timedelta(minutes=5)


@pytest.mark.asyncio
async def test_safe_mode_change_persists_once_and_replay_cannot_change_it(
    migrated_database_url: str,
) -> None:
    """A non-persistent mode change or duplicate-provider write must fail this test."""
    app = _app(migrated_database_url)
    mode_path = "/internal/v1/integrations/fosun/mode"
    list_path = "/internal/v1/integrations"
    disabled_body = b'{"mode":"disabled"}'
    fixture_body = b'{"mode":"fixture"}'
    headers = _mode_headers(
        mode_path,
        disabled_body,
        idempotency_key="disable-fosun",
        version=0,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        changed = await client.post(mode_path, content=disabled_body, headers=headers)
        replay = await client.post(mode_path, content=disabled_body, headers=headers)
        restored = await client.post(
            mode_path,
            content=fixture_body,
            headers=_mode_headers(
                mode_path,
                fixture_body,
                idempotency_key="restore-fosun",
                version=1,
            ),
        )
        listed = await client.get(list_path, headers=_headers("GET", list_path))

    assert changed.status_code == 200
    assert changed.json()["data"]["mode"] == "disabled"
    assert changed.json()["data"]["version"] == 1
    assert changed.headers["etag"] == '"1"'
    assert replay.status_code == 401
    assert replay.json()["error"]["code"] == "UNAUTHENTICATED"
    assert restored.status_code == 200
    assert restored.json()["data"]["mode"] == "fixture"
    assert restored.json()["data"]["version"] == 2
    assert listed.status_code == 200
    statuses = {status["provider"]: status for status in listed.json()["data"]}
    assert statuses["fosun"]["mode"] == "fixture"
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        persisted = await session.scalar(
            select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
        )
    assert persisted is not None
    assert persisted.admitted_contract_hash is None
    assert persisted.mode_changed_at == _CLOCK


@pytest.mark.asyncio
async def test_disabled_provider_test_is_unavailable(migrated_database_url: str) -> None:
    """Returning a fixture success for an unbuilt provider must fail this test."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/alpaca/test"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="alpaca-test", version=0),
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "PROVIDER_UNAVAILABLE"


@pytest.mark.asyncio
async def test_fosun_read_only_mode_requires_valid_capability(
    migrated_database_url: str,
) -> None:
    """Removing the admission gate must fail this exact contract anchor."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="fosun-read-only", version=0),
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "CAPABILITY_EXPIRED"


@pytest.mark.asyncio
async def test_unimplemented_supplemental_read_only_mode_is_unavailable(
    migrated_database_url: str,
) -> None:
    """An unimplemented supplemental provider is unavailable, not broker-forbidden."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/alpaca/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="alpaca-read-only", version=0),
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "PROVIDER_UNAVAILABLE"


@pytest.mark.asyncio
async def test_rejects_unknown_provider_and_mode_body_fields(
    migrated_database_url: str,
) -> None:
    """Relaxing provider or body validation must fail this test."""
    app = _app(migrated_database_url)
    unknown_path = "/internal/v1/integrations/unknown/test"
    invalid_mode_path = "/internal/v1/integrations/fosun/mode"
    invalid_mode = b'{"mode":"live"}'
    invalid_body = b'{"mode":"fixture","unexpected":true}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        unknown = await client.post(
            unknown_path,
            headers=_headers("POST", unknown_path),
        )
        invalid_mode_response = await client.post(
            invalid_mode_path,
            content=invalid_mode,
            headers=_mode_headers(
                invalid_mode_path,
                invalid_mode,
                idempotency_key="invalid-mode",
                version=0,
            ),
        )
        invalid = await client.post(
            invalid_mode_path,
            content=invalid_body,
            headers=_mode_headers(
                invalid_mode_path,
                invalid_body,
                idempotency_key="invalid-body",
                version=0,
            ),
        )

    assert unknown.status_code == 422
    assert unknown.json()["error"]["code"] == "VALIDATION_ERROR"
    assert invalid_mode_response.status_code == 422
    assert invalid_mode_response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_unimplemented_provider_cannot_be_promoted_to_fixture(
    migrated_database_url: str,
) -> None:
    """A fake PASS fixture for an unbuilt provider must be rejected without a status row."""
    app = _app(migrated_database_url)
    mode_path = "/internal/v1/integrations/alpaca/mode"
    list_path = "/internal/v1/integrations"
    body = b'{"mode":"fixture"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            mode_path,
            content=body,
            headers=_mode_headers(mode_path, body, idempotency_key="alpaca-fixture", version=0),
        )
        listed = await client.get(list_path, headers=_headers("GET", list_path))

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"
    assert listed.status_code == 200
    statuses = {status["provider"]: status for status in listed.json()["data"]}
    assert statuses["alpaca"]["mode"] == "disabled"
    assert statuses["alpaca"]["version"] == 0


@pytest.mark.asyncio
async def test_mode_requires_idempotency_key_and_if_match(
    migrated_database_url: str,
) -> None:
    """A mode write without both concurrency controls must fail before changing state."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"disabled"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=_headers("POST", path, body))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_connectivity_requires_idempotency_key_and_if_match(
    migrated_database_url: str,
) -> None:
    """A status-changing fixture test cannot bypass the write concurrency controls."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/test"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, headers=_headers("POST", path))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_same_idempotency_key_with_different_mode_conflicts(
    migrated_database_url: str,
) -> None:
    """Reusing a key for a different body must not mutate the mode twice."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    disabled = b'{"mode":"disabled"}'
    fixture = b'{"mode":"fixture"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            content=disabled,
            headers=_mode_headers(path, disabled, idempotency_key="same-key", version=0),
        )
        conflict = await client.post(
            path,
            content=fixture,
            headers=_mode_headers(path, fixture, idempotency_key="same-key", version=0),
        )

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_same_idempotency_key_and_request_replays_original_response(
    migrated_database_url: str,
) -> None:
    """A retried mode request returns its committed result without a second write."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"disabled"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="retry-key", version=0),
        )
        replay = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="retry-key", version=0),
        )

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert replay.headers["etag"] == '"1"'
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IntegrationStatus)) == 1


@pytest.mark.asyncio
async def test_mode_idempotency_key_binds_the_if_match_precondition(
    migrated_database_url: str,
) -> None:
    """Changing If-Match with the same key must not replay a stale mode result."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"disabled"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="if-match-key", version=0),
        )
        conflict = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="if-match-key", version=1),
        )

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_concurrent_same_mode_idempotency_key_replays_one_committed_result(
    migrated_database_url: str,
) -> None:
    """Concurrent same-key retries cannot create duplicate status or idempotency rows."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"disabled"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first, retry = await asyncio.gather(
            client.post(
                path,
                content=body,
                headers=_mode_headers(path, body, idempotency_key="same-concurrent", version=0),
            ),
            client.post(
                path,
                content=body,
                headers=_mode_headers(path, body, idempotency_key="same-concurrent", version=0),
            ),
        )

    assert first.status_code == retry.status_code == 200
    assert first.json() == retry.json()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IntegrationStatus)) == 1
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 2


@pytest.mark.asyncio
async def test_concurrent_opposing_mode_writes_return_one_version_conflict(
    migrated_database_url: str,
) -> None:
    """Concurrent writes from baseline version zero cannot silently become last-writer-wins."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    disabled = b'{"mode":"disabled"}'
    fixture = b'{"mode":"fixture"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first, second = await asyncio.gather(
            client.post(
                path,
                content=disabled,
                headers=_mode_headers(
                    path, disabled, idempotency_key="concurrent-disabled", version=0
                ),
            ),
            client.post(
                path,
                content=fixture,
                headers=_mode_headers(
                    path, fixture, idempotency_key="concurrent-fixture", version=0
                ),
            ),
        )

    assert sorted((first.status_code, second.status_code)) == [200, 409]
    conflict = next(response for response in (first, second) if response.status_code == 409)
    assert conflict.json()["error"]["code"] == "VERSION_CONFLICT"


@pytest.mark.asyncio
async def test_connectivity_mutation_replays_and_rejects_stale_preconditions(
    migrated_database_url: str,
) -> None:
    """Fixture refresh uses the same replay and optimistic-lock contract as mode writes."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/test"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-key", version=0),
        )
        replay = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-key", version=0),
        )
        key_conflict = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-key", version=1),
        )
        version_conflict = await client.post(
            path,
            headers=_test_headers(path, idempotency_key="refresh-stale", version=0),
        )

    assert first.status_code == replay.status_code == 200
    assert replay.json() == first.json()
    assert replay.headers["etag"] == '"1"'
    assert key_conflict.status_code == 409
    assert key_conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert version_conflict.status_code == 409
    assert version_conflict.json()["error"]["code"] == "VERSION_CONFLICT"
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IntegrationStatus)) == 1
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 2


@pytest.mark.asyncio
async def test_valid_read_only_capability_enables_and_projects_latest_contract(
    migrated_database_url: str,
) -> None:
    """Read-only mode must be derived from the latest valid admission record."""
    await _add_capability(
        migrated_database_url,
        contract_hash="b" * 64,
        expires_at=_CLOCK + timedelta(hours=1),
    )
    await _add_capability(migrated_database_url, expires_at=_CLOCK + timedelta(hours=2))
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="valid-read-only", version=0),
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "provider": "fosun",
        "mode": "read_only",
        "quality": "PARTIAL",
        "as_of": None,
        "capability_expires_at": "2026-08-23T14:00:00Z",
        "contract_hash": _CONTRACT_HASH,
        "next_action": "A successful read-only connectivity check is required.",
        "version": 1,
    }


@pytest.mark.asyncio
async def test_newest_expired_capability_blocks_an_older_valid_admission(
    migrated_database_url: str,
) -> None:
    """A newly revoked READ_ONLY record may not fall back to an older valid record."""
    await _add_capability(
        migrated_database_url,
        contract_hash="b" * 64,
        expires_at=_CLOCK + timedelta(days=7),
        created_at=_CLOCK - timedelta(days=2),
    )
    await _add_capability(
        migrated_database_url,
        contract_hash="c" * 64,
        expires_at=_CLOCK - timedelta(seconds=1),
        created_at=_CLOCK - timedelta(days=1),
    )
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="newest-expired", version=0),
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "CAPABILITY_EXPIRED"


@pytest.mark.asyncio
async def test_newest_short_lived_capability_is_persisted_without_expiry_fallback(
    migrated_database_url: str,
) -> None:
    """Authority order is immutable admission time, never the longest expiry."""
    await _add_capability(
        migrated_database_url,
        contract_hash="b" * 64,
        expires_at=_CLOCK + timedelta(days=7),
        created_at=_CLOCK - timedelta(days=2),
    )
    await _add_capability(
        migrated_database_url,
        contract_hash="c" * 64,
        expires_at=_CLOCK + timedelta(minutes=1),
        created_at=_CLOCK - timedelta(days=1),
    )
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="newest-short", version=0),
        )

    assert response.status_code == 200
    assert response.json()["data"]["contract_hash"] == "c" * 64
    assert response.json()["data"]["capability_expires_at"] == "2026-08-23T12:01:00Z"
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        status = await session.scalar(
            select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
        )
    assert status is not None
    assert status.admitted_contract_hash == "c" * 64


@pytest.mark.asyncio
async def test_newer_admission_disables_old_read_only_until_it_is_reprobed(
    migrated_database_url: str,
) -> None:
    """A new authoritative admission invalidates a status bound to the old hash."""
    await _add_capability(
        migrated_database_url,
        contract_hash="b" * 64,
        expires_at=_CLOCK + timedelta(days=7),
        created_at=_CLOCK - timedelta(days=2),
    )
    app = _app(migrated_database_url)
    mode_path = "/internal/v1/integrations/fosun/mode"
    list_path = "/internal/v1/integrations"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        enabled = await client.post(
            mode_path,
            content=body,
            headers=_mode_headers(mode_path, body, idempotency_key="old-admission", version=0),
        )
        await _add_capability(
            migrated_database_url,
            contract_hash="c" * 64,
            expires_at=_CLOCK + timedelta(minutes=1),
            created_at=_CLOCK - timedelta(days=1),
        )
        stale = await client.get(list_path, headers=_headers("GET", list_path))
        reprobed = await client.post(
            mode_path,
            content=body,
            headers=_mode_headers(mode_path, body, idempotency_key="new-admission", version=1),
        )

    assert enabled.status_code == 200
    stale_status = {item["provider"]: item for item in stale.json()["data"]}["fosun"]
    assert stale_status["mode"] == "disabled"
    assert stale_status["contract_hash"] == "c" * 64
    assert reprobed.status_code == 200
    assert reprobed.json()["data"]["contract_hash"] == "c" * 64


@pytest.mark.asyncio
async def test_disabled_mode_requires_a_newer_admission_before_read_only_reprobe(
    migrated_database_url: str,
) -> None:
    """Disabling invalidates every capability admitted before the disable epoch."""
    await _add_capability(
        migrated_database_url,
        expires_at=_CLOCK + timedelta(days=7),
        created_at=_CLOCK - timedelta(days=1),
    )
    clock = {"now": _CLOCK}
    app = _app(migrated_database_url)
    app.state.utc_clock = lambda: clock["now"]
    path = "/internal/v1/integrations/fosun/mode"
    disabled = b'{"mode":"disabled"}'
    fixture = b'{"mode":"fixture"}'
    read_only = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        disabled_response = await client.post(
            path,
            content=disabled,
            headers=_mode_headers(path, disabled, idempotency_key="disable-epoch", version=0),
        )
        blocked = await client.post(
            path,
            content=fixture,
            headers=_mode_headers(
                path, fixture, idempotency_key="fixture-after-disable", version=1
            ),
        )
        old_reprobe = await client.post(
            path,
            content=read_only,
            headers=_mode_headers(path, read_only, idempotency_key="old-reprobe", version=2),
        )
        clock["now"] = _CLOCK + timedelta(seconds=2)
        await _add_capability(
            migrated_database_url,
            contract_hash="b" * 64,
            expires_at=_CLOCK + timedelta(days=1),
            created_at=_CLOCK + timedelta(seconds=1),
        )
        enabled = await client.post(
            path,
            content=read_only,
            headers=_mode_headers(path, read_only, idempotency_key="new-reprobe", version=2),
        )

    assert disabled_response.status_code == 200
    assert blocked.status_code == 200
    assert blocked.json()["data"]["mode"] == "fixture"
    assert old_reprobe.status_code == 409
    assert old_reprobe.json()["error"]["code"] == "CAPABILITY_EXPIRED"
    assert enabled.status_code == 200
    assert enabled.json()["data"]["contract_hash"] == "b" * 64


@pytest.mark.asyncio
async def test_fixture_connectivity_preserves_admission_epochs_and_hash(
    migrated_database_url: str,
) -> None:
    """Fixture refresh is a connectivity write, not a mode transition or admission reset."""
    clock = {"now": _CLOCK}
    app = _app(migrated_database_url)
    app.state.utc_clock = lambda: clock["now"]
    mode_path = "/internal/v1/integrations/fosun/mode"
    test_path = "/internal/v1/integrations/fosun/test"
    disabled = b'{"mode":"disabled"}'
    fixture = b'{"mode":"fixture"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        disabled_response = await client.post(
            mode_path,
            content=disabled,
            headers=_mode_headers(
                mode_path, disabled, idempotency_key="refresh-disable", version=0
            ),
        )
        clock["now"] = _CLOCK + timedelta(minutes=1)
        fixture_response = await client.post(
            mode_path,
            content=fixture,
            headers=_mode_headers(mode_path, fixture, idempotency_key="refresh-fixture", version=1),
        )
        factory = session_factory(EngineSettings(database_url=migrated_database_url))
        async with factory() as session:
            before = await session.scalar(
                select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
            )
        clock["now"] = _CLOCK + timedelta(minutes=2)
        refreshed = await client.post(
            test_path,
            headers=_test_headers(test_path, idempotency_key="refresh-state", version=2),
        )

    assert disabled_response.status_code == 200
    assert fixture_response.status_code == 200
    assert refreshed.status_code == 200
    assert before is not None
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        after = await session.scalar(
            select(IntegrationStatus).where(IntegrationStatus.provider == "fosun")
        )
    assert after is not None
    assert after.last_success_at == _CLOCK + timedelta(minutes=2)
    assert after.mode_changed_at == before.mode_changed_at
    assert after.capability_invalidated_at == before.capability_invalidated_at
    assert after.admitted_contract_hash == before.admitted_contract_hash


@pytest.mark.asyncio
async def test_persisted_read_only_with_expired_capability_fails_closed(
    migrated_database_url: str,
) -> None:
    """A stale admission may remain visible but cannot make a read-only status effective."""
    await _add_capability(migrated_database_url, expires_at=_CLOCK - timedelta(seconds=1))
    await _add_read_only_status(migrated_database_url)
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    statuses = {status["provider"]: status for status in response.json()["data"]}
    assert statuses["fosun"]["mode"] == "disabled"
    assert statuses["fosun"]["quality"] == "FAIL"
    assert statuses["fosun"]["capability_expires_at"] == "2026-08-23T11:59:59Z"
    assert statuses["fosun"]["contract_hash"] == _CONTRACT_HASH


@pytest.mark.asyncio
async def test_connectivity_body_rejects_extra_fields(migrated_database_url: str) -> None:
    """The fixture connectivity request must remain an explicitly empty strict schema."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/test"
    body = b'{"unexpected":true}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_test_headers(path, body, idempotency_key="invalid-connectivity", version=0),
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_mode_idempotency_and_hmac_nonce_share_the_business_transaction(
    migrated_database_url: str,
) -> None:
    """A successful mode mutation commits exactly one nonce, idempotency row, and status row."""
    app = _app(migrated_database_url)
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"disabled"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            path,
            content=body,
            headers=_mode_headers(path, body, idempotency_key="atomic-mode", version=0),
        )

    assert response.status_code == 200
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(UsedNonce)) == 1
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(IntegrationStatus)) == 1
