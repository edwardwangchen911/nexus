from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from nexus.main import create_engine_app
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment
from nexus.worker import run_worker
from pydantic import SecretStr, ValidationError


def test_engine_liveness_is_minimal() -> None:
    """A healthy engine exposes only its identity, never configuration."""
    response = TestClient(create_engine_app(EngineSettings.for_test())).get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive", "service": "nexus-engine"}


def test_engine_404_uses_the_safe_error_envelope() -> None:
    """Framework-generated 404s must not bypass the public error contract."""
    response = TestClient(create_engine_app(EngineSettings.for_test())).get("/not-a-route")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "NOT_FOUND"
    assert payload["error"]["details"] == {}
    assert payload["error"]["request_id"].split("-")[2].startswith("7")


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
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="current",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET="placeholder-engine-secret",
        AGENT_RUNTIME_MODE="responses",
        OPENAI_API_KEY="placeholder-openai-key",
        OPENAI_EXTRACTION_MODEL="configured-extraction",
        OPENAI_REASONING_MODEL="configured-reasoning",
        FOSUN_GATEWAY_BASE_URL="https://gateway.invalid",
        FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
    )

    assert settings.environment is Environment.PRODUCTION


def test_engine_production_requires_a_current_hmac_key_pair() -> None:
    """A production engine without a signing key must refuse to start."""
    with pytest.raises(ValidationError, match="NEXUS_ENGINE_HMAC_CURRENT"):
        EngineSettings(
            NEXUS_ENV="production",
            DATABASE_URL="postgresql+asyncpg://safe:placeholder@db/nexus",
            OBJECT_STORE_ENDPOINT="https://object-store.invalid",
            OBJECT_STORE_REGION="us-east-1",
            OBJECT_STORE_BUCKET="nexus",
            OBJECT_STORE_ACCESS_KEY_ID="placeholder-access-key",
            OBJECT_STORE_SECRET_ACCESS_KEY="placeholder-secret-key",
            AGENT_RUNTIME_MODE="responses",
            OPENAI_API_KEY="placeholder-openai-key",
            OPENAI_EXTRACTION_MODEL="configured-extraction",
            OPENAI_REASONING_MODEL="configured-reasoning",
            FOSUN_GATEWAY_BASE_URL="https://gateway.invalid",
            FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
        )


def test_engine_production_rejects_an_incomplete_previous_hmac_key_pair() -> None:
    """Rotation cannot silently enable a key ID without its verification secret."""
    with pytest.raises(ValidationError, match="NEXUS_ENGINE_HMAC_PREVIOUS"):
        EngineSettings(
            NEXUS_ENV="production",
            DATABASE_URL="postgresql+asyncpg://safe:placeholder@db/nexus",
            OBJECT_STORE_ENDPOINT="https://object-store.invalid",
            OBJECT_STORE_REGION="us-east-1",
            OBJECT_STORE_BUCKET="nexus",
            OBJECT_STORE_ACCESS_KEY_ID="placeholder-access-key",
            OBJECT_STORE_SECRET_ACCESS_KEY="placeholder-secret-key",
            NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="current",
            NEXUS_ENGINE_HMAC_CURRENT_SECRET="placeholder-engine-secret",
            NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID="previous",
            AGENT_RUNTIME_MODE="responses",
            OPENAI_API_KEY="placeholder-openai-key",
            OPENAI_EXTRACTION_MODEL="configured-extraction",
            OPENAI_REASONING_MODEL="configured-reasoning",
            FOSUN_GATEWAY_BASE_URL="https://gateway.invalid",
            FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
        )


@pytest.mark.parametrize(
    ("current_key_id", "current_secret", "previous_key_id", "previous_secret"),
    [
        (" ", "placeholder-engine-secret", None, None),
        ("current", " ", None, None),
        ("current", "placeholder-engine-secret", "current", "previous-secret"),
        ("current", "placeholder-engine-secret", "previous", " "),
    ],
)
def test_engine_production_rejects_blank_or_duplicate_hmac_rotation_values(
    current_key_id: str,
    current_secret: str,
    previous_key_id: str | None,
    previous_secret: str | None,
) -> None:
    """Whitespace and repeated IDs would make key rotation ambiguous or disable signing."""
    with pytest.raises(ValidationError):
        EngineSettings(
            NEXUS_ENV="production",
            DATABASE_URL="postgresql+asyncpg://safe:placeholder@db/nexus",
            OBJECT_STORE_ENDPOINT="https://object-store.invalid",
            OBJECT_STORE_REGION="us-east-1",
            OBJECT_STORE_BUCKET="nexus",
            OBJECT_STORE_ACCESS_KEY_ID="placeholder-access-key",
            OBJECT_STORE_SECRET_ACCESS_KEY="placeholder-secret-key",
            NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=current_key_id,
            NEXUS_ENGINE_HMAC_CURRENT_SECRET=current_secret,
            NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID=previous_key_id,
            NEXUS_ENGINE_HMAC_PREVIOUS_SECRET=previous_secret,
            FOSUN_GATEWAY_BASE_URL="https://gateway.invalid",
            FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
        )


def test_engine_production_validation_hides_input_canary() -> None:
    canary = "synthetic-engine-validation-canary"

    with pytest.raises(ValidationError) as exc_info:
        EngineSettings(NEXUS_ENV="production", DATABASE_URL=canary)

    assert canary not in str(exc_info.value)


@pytest.mark.asyncio
async def test_worker_startup_is_fixture_safe() -> None:
    await run_worker(EngineSettings.for_test())


@pytest.mark.asyncio
async def test_long_running_worker_polls_general_after_portfolio_work(monkeypatch) -> None:
    class StopLoop(RuntimeError):
        pass

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def begin(self):
            return self

    class FakeOpenAI:
        def __init__(self, **kwargs):
            self.max_retries = kwargs["max_retries"]
            self.responses = object()

    settings = EngineSettings.for_test()
    settings.agent_runtime_mode = AgentRuntimeMode.RESPONSES
    settings.openai_api_key = SecretStr("fixture-openai-key")
    portfolio = AsyncMock(side_effect=(True, StopLoop("stop after one full cycle")))
    general = AsyncMock(return_value=False)
    monkeypatch.setattr("nexus.worker.session_factory", lambda _: lambda: FakeSession())
    monkeypatch.setattr("nexus.worker._general_registry", lambda *args, **kwargs: object())
    monkeypatch.setattr("nexus.worker.AsyncOpenAI", FakeOpenAI)
    monkeypatch.setattr("nexus.worker.schedule_due_daily_pipeline", AsyncMock())
    monkeypatch.setattr("nexus.worker._run_portfolio_decision_worker_once", portfolio)
    monkeypatch.setattr("nexus.worker.run_job_worker_once", general)

    with pytest.raises(StopLoop, match="one full cycle"):
        await run_worker(settings, run_forever=True)

    assert general.await_count == 1
