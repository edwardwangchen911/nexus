import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import nexus_fosun_gateway.main as main_module
import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.main import _serve_production, create_gateway_app
from nexus_fosun_gateway.settings import Environment, GatewaySettings
from pydantic import ValidationError


def test_gateway_liveness_is_minimal() -> None:
    """A healthy gateway exposes only its identity, never configuration."""
    response = TestClient(create_gateway_app(GatewaySettings.for_test())).get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive", "service": "nexus-fosun-gateway"}


def test_fixture_gateway_is_immediately_ready() -> None:
    response = TestClient(create_gateway_app(GatewaySettings.for_test())).get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "service": "nexus-fosun-gateway"}


class _UnavailableTransport:
    def __init__(self) -> None:
        self.close_calls = 0

    def is_ready(self) -> bool:
        return False

    async def aclose(self) -> None:
        self.close_calls += 1


def test_transport_poison_makes_gateway_unready_and_lifespan_closes_it() -> None:
    app = create_gateway_app(GatewaySettings.for_test())
    transport = _UnavailableTransport()
    app.state.transport = transport

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "draining", "service": "nexus-fosun-gateway"}
    assert transport.close_calls == 1


def test_http_readiness_turns_unhealthy_at_activation_expiry() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    current = [expires_at - timedelta(seconds=1)]
    app = create_gateway_app(GatewaySettings.for_test())
    app.state.transport = SdkFosunTransport(
        SimpleNamespace(),
        capability_expires_at=expires_at,
        activation_checked_at=current[0],
        activation_monotonic_anchor=100.0,
        clock=lambda: 100.0,
        utc_clock=lambda: current[0],
    )

    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == 200
        current[0] = expires_at
        response = client.get("/health/ready")
        current[0] = expires_at - timedelta(hours=1)
        rolled_back = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "draining", "service": "nexus-fosun-gateway"}
    assert rolled_back.status_code == 503


@pytest.mark.asyncio
async def test_dual_listener_shutdown_closes_transport_when_lifespan_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_gateway_app(GatewaySettings.for_test())
    transport = _UnavailableTransport()
    app.state.transport = transport
    listeners = (
        SimpleNamespace(port=8081, tls_kwargs={}),
        SimpleNamespace(port=8082, tls_kwargs={}),
    )

    class _Server:
        def __init__(self, _config: object) -> None:
            self.should_exit = False

        async def serve(self) -> None:
            await asyncio.sleep(0)

    monkeypatch.setattr(main_module, "production_listener_configs", lambda _settings: listeners)
    monkeypatch.setattr(main_module.uvicorn, "Server", _Server)

    await _serve_production(app, GatewaySettings.for_test())

    assert transport.close_calls == 1


def test_gateway_settings_never_render_fixture_secrets() -> None:
    settings = GatewaySettings.for_test()

    assert "fixture-gateway-current-secret-32-bytes" not in repr(settings)


def test_gateway_reads_nexus_environment_alias_for_production() -> None:
    settings = GatewaySettings.model_validate(_production_settings())

    assert settings.environment is Environment.PRODUCTION


@pytest.mark.parametrize("sdk_admitted", ["1", "TRUE", "yes", 1])
def test_gateway_requires_exact_sdk_admission_switch(sdk_admitted: object) -> None:
    values = _production_settings()
    values["FOSUN_SDK_ADMITTED"] = sdk_admitted

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


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


@pytest.mark.parametrize(
    "field_name",
    [
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE",
        "FOSUN_ACTIVATION_CONTRACT_HASH_FILE",
    ],
)
def test_gateway_requires_absolute_activation_control_file_paths(
    field_name: str,
) -> None:
    values = _production_settings()
    values[field_name] = "relative/control-file"

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


@pytest.mark.parametrize("capability", ["LIVE_WRITE", ""])
def test_gateway_rejects_non_read_only_capability(capability: str) -> None:
    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(
            {**_production_settings(), "FOSUN_GATEWAY_CAPABILITY": capability}
        )


@pytest.mark.parametrize("replica_count", [0, 2, 3, -1])
def test_gateway_rejects_more_or_fewer_than_one_replica(replica_count: int) -> None:
    """An in-memory replay cache is safe only under the fixed single-replica topology."""
    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(
            {
                **_production_settings(),
                "FOSUN_GATEWAY_REPLICA_COUNT": replica_count,
            }
        )


@pytest.mark.parametrize(
    "missing",
    [
        "FOSUN_GATEWAY_SERVER_CERT",
        "FOSUN_GATEWAY_SERVER_KEY",
        "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA",
        "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_GATEWAY_REPLAY_STORE_PATH",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
        "FSOPENAPI_BASE_URL",
        "FSOPENAPI_API_KEY",
        "FSOPENAPI_CLIENT_PRIVATE_KEY",
        "FSOPENAPI_SERVER_PUBLIC_KEY",
        "FOSUN_ACTIVATION_PROBE_ARTIFACT",
        "FOSUN_ACTIVATION_PROBE_RECEIPT",
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE",
        "FOSUN_ACTIVATION_CONTRACT_HASH_FILE",
        "FOSUN_PROVIDER_MANIFEST",
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
        GatewaySettings.model_validate({**_production_settings(), "NEXUS_ENV": "development"})


def _production_settings() -> dict[str, object]:
    return {
        "NEXUS_ENV": "production",
        "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
        "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "gateway-current",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
        "FOSUN_GATEWAY_REPLAY_STORE_PATH": "/synthetic/fosun-gateway/replay.sqlite3",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "market-gateway-current",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": ("synthetic-market-current-secret-32-bytes"),
        "FOSUN_GATEWAY_SERVER_CERT": "/run/secrets/synthetic-server-cert",
        "FOSUN_GATEWAY_SERVER_KEY": "/run/secrets/synthetic-server-key",
        "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA": "/run/secrets/synthetic-broker-ca",
        "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA": "/run/secrets/synthetic-market-ca",
        "FOSUN_SDK_ADMITTED": True,
        "FOSUN_ACTIVATION_PROBE_ARTIFACT": "/synthetic/fosun-probe.json",
        "FOSUN_ACTIVATION_PROBE_RECEIPT": "/synthetic/fosun-probe-receipt.json",
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE": (
            "/synthetic/fosun-publisher-public-key.b64u"
        ),
        "FOSUN_ACTIVATION_CONTRACT_HASH_FILE": (
            "/synthetic/fosun-activation-contract.sha256"
        ),
        "FOSUN_PROVIDER_MANIFEST": "/synthetic/fosun-provider-manifest.json",
        "FSOPENAPI_BASE_URL": "https://openapi.fosunxcz.com",
        "FSOPENAPI_API_KEY": "synthetic-api-key",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-client-private-key",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-public-key",
    }
