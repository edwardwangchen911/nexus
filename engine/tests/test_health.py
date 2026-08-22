import pytest
from fastapi.testclient import TestClient
from nexus.main import create_engine_app
from nexus.settings import EngineSettings, Environment
from nexus.worker import run_worker
from pydantic import ValidationError


def test_engine_liveness_is_minimal() -> None:
    """A healthy engine exposes only its identity, never configuration."""
    response = TestClient(create_engine_app(EngineSettings.for_test())).get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive", "service": "nexus-engine"}


def test_engine_settings_never_render_fixture_secrets() -> None:
    settings = EngineSettings.for_test()

    assert "fixture-engine-hmac-secret" not in repr(settings)
    assert "fixture-object-store-secret" not in repr(settings)


def test_engine_reads_nexus_environment_alias_for_production() -> None:
    settings = EngineSettings(
        NEXUS_ENV="production",
        DATABASE_URL="postgresql+asyncpg://safe:placeholder@db/nexus",
        OBJECT_STORE_ENDPOINT="https://object-store.invalid",
        OBJECT_STORE_REGION="us-east-1",
        OBJECT_STORE_BUCKET="nexus",
        OBJECT_STORE_ACCESS_KEY_ID="placeholder-access-key",
        OBJECT_STORE_SECRET_ACCESS_KEY="placeholder-secret-key",
        NEXUS_ENGINE_HMAC_SECRET="placeholder-engine-secret",
        FOSUN_GATEWAY_BASE_URL="https://gateway.invalid",
        FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
    )

    assert settings.environment is Environment.PRODUCTION


def test_engine_production_validation_hides_input_canary() -> None:
    canary = "synthetic-engine-validation-canary"

    with pytest.raises(ValidationError) as exc_info:
        EngineSettings(NEXUS_ENV="production", DATABASE_URL=canary)

    assert canary not in str(exc_info.value)


@pytest.mark.asyncio
async def test_worker_startup_is_fixture_safe() -> None:
    await run_worker(EngineSettings.for_test())
