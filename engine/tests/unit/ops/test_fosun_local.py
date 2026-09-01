import pytest
from nexus.ops.fosun_local import doctor_local_environment, safe_failure
from nexus.providers.broker.gateway_contracts import GatewayClientError


def test_doctor_reports_names_without_exposing_values() -> None:
    canary = "secret-value-must-not-appear"
    result = doctor_local_environment(
        {
            "DATABASE_URL": canary,
            "FOSUN_GATEWAY_CLIENT_CERT": "/missing/cert.pem",
        },
        is_file=lambda _path: False,
    )

    rendered = repr(result)
    assert result["status"] == "configuration_incomplete"
    assert "DATABASE_URL" not in result["missing_environment"]
    assert result["tls_files"] == {"FOSUN_GATEWAY_CLIENT_CERT": "missing"}
    assert canary not in rendered
    assert "/missing/cert.pem" not in rendered


def test_doctor_can_confirm_complete_configuration_without_network_or_database() -> None:
    environment = {
        name: "configured"
        for name in (
            "DATABASE_URL",
            "OBJECT_STORE_ENDPOINT",
            "OBJECT_STORE_REGION",
            "OBJECT_STORE_BUCKET",
            "OBJECT_STORE_ACCESS_KEY_ID",
            "OBJECT_STORE_SECRET_ACCESS_KEY",
            "FOSUN_GATEWAY_BASE_URL",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
            "FOSUN_GATEWAY_CLIENT_CERT",
            "FOSUN_GATEWAY_CLIENT_KEY",
            "FOSUN_GATEWAY_CLIENT_CA",
            "FOSUN_MARKET_GATEWAY_BASE_URL",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
            "FOSUN_MARKET_GATEWAY_CLIENT_CERT",
            "FOSUN_MARKET_GATEWAY_CLIENT_KEY",
            "FOSUN_MARKET_GATEWAY_CLIENT_CA",
        )
    }

    result = doctor_local_environment(environment, is_file=lambda _path: True)

    assert result == {
        "status": "ready",
        "configured_environment_count": 18,
        "missing_environment": (),
        "tls_files": {
            "FOSUN_GATEWAY_CLIENT_CA": "present",
            "FOSUN_GATEWAY_CLIENT_CERT": "present",
            "FOSUN_GATEWAY_CLIENT_KEY": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_CA": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_CERT": "present",
            "FOSUN_MARKET_GATEWAY_CLIENT_KEY": "present",
        },
        "network_checked": False,
        "database_checked": False,
    }


def test_safe_failure_classifies_gateway_errors_without_echoing_messages() -> None:
    result = safe_failure(GatewayClientError("provider-secret-canary"))

    assert result == {
        "status": "failed",
        "category": "gateway",
        "code": "LOCAL_READ_ONLY_REFRESH_FAILED",
    }
    assert "provider-secret-canary" not in repr(result)


@pytest.mark.parametrize(
    ("message", "category"),
    (
        ("gateway rejected request: PROVIDER_AUTHENTICATION_FAILED", "auth"),
        ("gateway rejected request: PROVIDER_PERMISSION_DENIED", "permission"),
        ("gateway rejected request: PROVIDER_RATE_LIMITED", "rate_limit"),
        ("gateway rejected request: PROVIDER_APPLICATION_ERROR", "provider"),
        ("gateway request failed", "transport"),
        ("gateway response contract rejected", "contract"),
    ),
)
def test_safe_failure_preserves_stable_gateway_categories(
    message: str,
    category: str,
) -> None:
    assert safe_failure(GatewayClientError(message))["category"] == category
