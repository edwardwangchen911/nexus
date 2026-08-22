from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.main import build_transport
from nexus_fosun_gateway.settings import GatewaySettings


def test_fixture_mode_never_imports_or_instantiates_fsopenapi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Offline fixture startup must not make the absent SDK executable."""

    def forbidden_import(name: str):
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr("nexus_fosun_gateway.main.importlib.import_module", forbidden_import)

    assert isinstance(build_transport(GatewaySettings.for_test()), FixtureFosunTransport)


def test_read_only_mode_without_admission_returns_stable_error() -> None:
    """An unadmitted SDK must fail closed rather than fall back to fixtures."""
    settings = GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "development",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret",
        }
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(settings)

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_admitted_config_with_missing_package_returns_stable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The intentionally absent package must never trigger a fixture fallback."""
    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        lambda name: (_ for _ in ()).throw(ModuleNotFoundError(name)),
    )
    settings = GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "development",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret",
            "FOSUN_SDK_ADMITTED": True,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
        }
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(settings)

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_read_only_sdk_construction_forces_all_sdk_logging_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production composition must never enable SDK summary or body logging."""
    captured: dict[str, object] = {}

    class FakeSdkClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        lambda name: SimpleNamespace(SDKClient=FakeSdkClient),
    )
    settings = GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "development",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret",
            "FOSUN_SDK_ADMITTED": True,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
        }
    )

    transport = build_transport(settings)

    assert isinstance(transport, SdkFosunTransport)
    assert captured == {
        "base_url": "https://provider.invalid",
        "api_key": "synthetic-api-key",
        "logging_enable": False,
        "log_body": False,
    }
