import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import Environment, GatewaySettings
from pydantic import ValidationError


def test_gateway_liveness_is_minimal() -> None:
    """A healthy gateway exposes only its identity, never configuration."""
    response = TestClient(create_gateway_app(GatewaySettings.for_test())).get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive", "service": "nexus-fosun-gateway"}


def test_gateway_settings_never_render_fixture_secrets() -> None:
    settings = GatewaySettings.for_test()

    assert "fixture-gateway-hmac-secret" not in repr(settings)


def test_gateway_reads_nexus_environment_alias_for_production() -> None:
    settings = GatewaySettings(
        NEXUS_ENV="production",
        FOSUN_GATEWAY_CAPABILITY="READ_ONLY",
        FOSUN_GATEWAY_HMAC_SECRET="placeholder-gateway-secret",
    )

    assert settings.environment is Environment.PRODUCTION


def test_gateway_production_validation_hides_input_canary() -> None:
    canary = "synthetic-gateway-validation-canary"

    with pytest.raises(ValidationError) as exc_info:
        GatewaySettings(NEXUS_ENV="production", FOSUN_GATEWAY_HMAC_SECRET=canary)

    assert canary not in str(exc_info.value)


@pytest.mark.parametrize("capability", ["LIVE_WRITE", ""])
def test_gateway_rejects_non_read_only_capability(capability: str) -> None:
    with pytest.raises(ValidationError):
        GatewaySettings(
            NEXUS_ENV="production",
            FOSUN_GATEWAY_CAPABILITY=capability,
            FOSUN_GATEWAY_HMAC_SECRET="synthetic-capability-test-canary",
        )
