"""Authenticated integration-status API contract tests."""

import httpx
import pytest
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET, build_test_hmac_headers
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.settings import EngineSettings


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
        "contract_hash": "fixture-v1",
        "next_action": "Read-only admission is required.",
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
    app = create_engine_app(_settings(migrated_database_url))
    path = "/internal/v1/integrations/fosun/test"
    headers = _headers("POST", path)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, headers=headers)
        replay = await client.post(path, headers=headers)

    assert response.status_code == 200
    assert response.json()["data"]["provider"] == "fosun"
    assert response.json()["data"]["mode"] == "fixture"
    assert replay.status_code == 401
    assert replay.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_safe_mode_change_persists_once_and_replay_cannot_change_it(
    migrated_database_url: str,
) -> None:
    """A non-persistent mode change or duplicate-provider write must fail this test."""
    app = create_engine_app(_settings(migrated_database_url))
    mode_path = "/internal/v1/integrations/fosun/mode"
    list_path = "/internal/v1/integrations"
    disabled_body = b'{"mode":"disabled"}'
    fixture_body = b'{"mode":"fixture"}'
    headers = _headers("POST", mode_path, disabled_body)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        changed = await client.post(mode_path, content=disabled_body, headers=headers)
        replay = await client.post(mode_path, content=disabled_body, headers=headers)
        restored = await client.post(
            mode_path,
            content=fixture_body,
            headers=_headers("POST", mode_path, fixture_body),
        )
        listed = await client.get(list_path, headers=_headers("GET", list_path))

    assert changed.status_code == 200
    assert changed.json()["data"]["mode"] == "disabled"
    assert replay.status_code == 401
    assert replay.json()["error"]["code"] == "UNAUTHENTICATED"
    assert restored.status_code == 200
    assert restored.json()["data"]["mode"] == "fixture"
    assert listed.status_code == 200
    statuses = {status["provider"]: status for status in listed.json()["data"]}
    assert statuses["fosun"]["mode"] == "fixture"


@pytest.mark.asyncio
async def test_disabled_provider_test_is_unavailable(migrated_database_url: str) -> None:
    """Returning a fixture success for an unbuilt provider must fail this test."""
    app = create_engine_app(_settings(migrated_database_url))
    path = "/internal/v1/integrations/alpaca/test"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, headers=_headers("POST", path))

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "PROVIDER_UNAVAILABLE"


@pytest.mark.asyncio
async def test_fosun_read_only_mode_requires_valid_capability(
    migrated_database_url: str,
) -> None:
    """Removing the admission gate must fail this exact contract anchor."""
    app = create_engine_app(_settings(migrated_database_url))
    path = "/internal/v1/integrations/fosun/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=_headers("POST", path, body))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "CAPABILITY_EXPIRED"


@pytest.mark.asyncio
async def test_unadmitted_broker_read_only_mode_is_forbidden(
    migrated_database_url: str,
) -> None:
    """Allowing an unadmitted broker capability must fail this test."""
    app = create_engine_app(_settings(migrated_database_url))
    path = "/internal/v1/integrations/alpaca/mode"
    body = b'{"mode":"read_only"}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=_headers("POST", path, body))

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_rejects_unknown_provider_and_mode_body_fields(
    migrated_database_url: str,
) -> None:
    """Relaxing provider or body validation must fail this test."""
    app = create_engine_app(_settings(migrated_database_url))
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
            headers=_headers("POST", invalid_mode_path, invalid_mode),
        )
        invalid = await client.post(
            invalid_mode_path,
            content=invalid_body,
            headers=_headers("POST", invalid_mode_path, invalid_body),
        )

    assert unknown.status_code == 422
    assert unknown.json()["error"]["code"] == "VALIDATION_ERROR"
    assert invalid_mode_response.status_code == 422
    assert invalid_mode_response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"
