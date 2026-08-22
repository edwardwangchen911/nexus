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

    assert "fixture-gateway-current-secret-32-bytes" not in repr(settings)


def test_gateway_reads_nexus_environment_alias_for_production() -> None:
    settings = GatewaySettings.model_validate(_production_settings())

    assert settings.environment is Environment.PRODUCTION


def test_gateway_production_validation_hides_input_canary() -> None:
    canary = "synthetic-gateway-validation-canary"

    with pytest.raises(ValidationError) as exc_info:
        GatewaySettings.model_validate(
            {
                "NEXUS_ENV": "production",
                "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": canary,
            }
        )

    assert canary not in str(exc_info.value)


@pytest.mark.parametrize("capability", ["LIVE_WRITE", ""])
def test_gateway_rejects_non_read_only_capability(capability: str) -> None:
    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(
            {**_production_settings(), "FOSUN_GATEWAY_CAPABILITY": capability}
        )


@pytest.mark.parametrize(
    "missing",
    [
        "FOSUN_GATEWAY_SERVER_CERT",
        "FOSUN_GATEWAY_SERVER_KEY",
        "FOSUN_GATEWAY_SERVER_CLIENT_CA",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
        "FSOPENAPI_BASE_URL",
        "FSOPENAPI_API_KEY",
        "FSOPENAPI_CLIENT_PRIVATE_KEY",
        "FSOPENAPI_SERVER_PUBLIC_KEY",
    ],
)
def test_gateway_production_settings_fail_closed_when_required_input_is_missing(
    missing: str,
) -> None:
    """Production must not start with incomplete TLS, HMAC, or provider admission config."""
    values = _production_settings()
    values.pop(missing)

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


def test_previous_gateway_hmac_identity_must_be_complete_and_distinct() -> None:
    """A partial or duplicate rotation identity makes authentication ambiguous."""
    partial = {
        **_production_settings(),
        "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "gateway-previous",
    }
    duplicate = {
        **_production_settings(),
        "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "gateway-current",
        "FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET": "synthetic-previous-secret",
    }

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(partial)
    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(duplicate)


@pytest.mark.parametrize(
    "secret",
    ["", "   ", "synthetic-weak", "x" * 31, f" {'x' * 32}"],
)
def test_production_rejects_empty_whitespace_or_weak_current_hmac_secret(
    secret: str,
) -> None:
    """Production authentication must require an unpadded 32-byte secret or stronger."""
    values = _production_settings()
    values["FOSUN_GATEWAY_HMAC_CURRENT_SECRET"] = secret

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


def test_production_rejects_equal_current_and_previous_hmac_secrets() -> None:
    """Distinct key IDs must not disguise reuse of the same gateway secret."""
    values = {
        **_production_settings(),
        "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "gateway-previous",
        "FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET": "x" * 32,
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "x" * 32,
    }

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


def test_development_read_only_mode_does_not_receive_fixture_hmac_defaults() -> None:
    """Switching transport mode must not carry fixture authentication into an SDK process."""
    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(
            {
                "NEXUS_ENV": "development",
                "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            }
        )


def _production_settings() -> dict[str, object]:
    return {
        "NEXUS_ENV": "production",
        "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
        "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "gateway-current",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
        "FOSUN_GATEWAY_SERVER_CERT": "/run/secrets/synthetic-server-cert",
        "FOSUN_GATEWAY_SERVER_KEY": "/run/secrets/synthetic-server-key",
        "FOSUN_GATEWAY_SERVER_CLIENT_CA": "/run/secrets/synthetic-client-ca",
        "FOSUN_SDK_ADMITTED": True,
        "FSOPENAPI_BASE_URL": "https://provider.invalid",
        "FSOPENAPI_API_KEY": "synthetic-api-key",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-client-private-key",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-public-key",
    }
