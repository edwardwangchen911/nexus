from pathlib import Path

import nexus.ops.fosun_local as fosun_local
import pytest
from nexus.ops.fosun_local import doctor_local_environment, safe_failure
from nexus.providers.broker.gateway_contracts import GatewayClientError


def _write_local_env(repo_root: Path) -> None:
    infra = repo_root / "infra"
    infra.mkdir()
    (infra / ".env.local").write_text(
        "DATABASE_URL=postgresql+asyncpg://local-canary\n"
        "OBJECT_STORE_ENDPOINT=http://127.0.0.1:9000\n"
        "OBJECT_STORE_REGION=us-east-1\n"
        "OBJECT_STORE_BUCKET=nexus-local\n"
        "OBJECT_STORE_ACCESS_KEY_ID=access-canary\n"
        "OBJECT_STORE_SECRET_ACCESS_KEY=secret-canary\n",
        encoding="utf-8",
    )


def test_local_environment_loads_ignored_env_and_derives_loopback_tls(tmp_path: Path) -> None:
    _write_local_env(tmp_path)

    environment = fosun_local.load_local_environment(tmp_path, {})

    secret_root = tmp_path / ".secrets" / "nexus-fosun-local"
    assert environment["DATABASE_URL"] == "postgresql+asyncpg://local-canary"
    assert environment["FOSUN_GATEWAY_BASE_URL"] == "https://127.0.0.1:8081"
    assert environment["FOSUN_MARKET_GATEWAY_BASE_URL"] == "https://127.0.0.1:8082"
    assert environment["FOSUN_GATEWAY_CLIENT_CERT"] == str(secret_root / "broker-client.pem")
    assert environment["FOSUN_GATEWAY_CLIENT_KEY"] == str(secret_root / "broker-client.key")
    assert environment["FOSUN_GATEWAY_CLIENT_CA"] == str(secret_root / "server-ca.pem")
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_CERT"] == str(
        secret_root / "market-client.pem"
    )
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_KEY"] == str(
        secret_root / "market-client.key"
    )
    assert environment["FOSUN_MARKET_GATEWAY_CLIENT_CA"] == str(secret_root / "server-ca.pem")


def test_explicit_environment_overrides_ignored_local_file(tmp_path: Path) -> None:
    _write_local_env(tmp_path)

    environment = fosun_local.load_local_environment(
        tmp_path,
        {"DATABASE_URL": "postgresql+asyncpg://explicit-canary"},
    )

    assert environment["DATABASE_URL"] == "postgresql+asyncpg://explicit-canary"


@pytest.mark.asyncio
async def test_status_database_boundary_returns_a_stable_redacted_failure() -> None:
    async def unavailable_reader(_database_url: str) -> dict[str, object]:
        raise OSError("database-location-canary")

    with pytest.raises(Exception) as exc_info:
        await fosun_local.read_local_status_safely(
            "postgresql+asyncpg://database-secret-canary",
            reader=unavailable_reader,
        )

    assert fosun_local.safe_failure(exc_info.value) == {
        "status": "failed",
        "category": "database",
        "code": "LOCAL_DATABASE_UNAVAILABLE",
    }
    assert "canary" not in repr(fosun_local.safe_failure(exc_info.value))


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
