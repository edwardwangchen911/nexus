import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.main import build_transport
from nexus_fosun_gateway.settings import GatewaySettings

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"


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
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
        }
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(settings)

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_admitted_config_with_missing_package_returns_stable_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
            "FOSUN_SDK_ADMITTED": True,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
            "FOSUN_PROVIDER_MANIFEST": _write_admitted_manifest(tmp_path),
        }
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(settings)

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_runtime_boolean_cannot_override_committed_blocked_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runtime flag cannot import an SDK while reviewed admission is blocked."""
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        return SimpleNamespace(SDKClient=object)

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        forbidden_import,
    )
    settings = _read_only_settings(MANIFEST_PATH)

    with pytest.raises(GatewayError) as exc_info:
        build_transport(settings)

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is False


@pytest.mark.parametrize(
    "missing_evidence",
    [
        "written_permission",
        "provider_test_vector",
        "security_review",
        "tls_verification_remediation",
    ],
)
def test_admission_requires_every_reviewed_evidence_reference(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing_evidence: str,
) -> None:
    """An admitted label without all four reviewed evidence records remains blocked."""
    manifest_path = _write_admitted_manifest(tmp_path, missing_evidence=missing_evidence)
    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        lambda name: pytest.fail(f"unexpected SDK import: {name}"),
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(_read_only_settings(manifest_path))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_admission_record_must_match_exact_reviewed_sdk_and_openapi_subject(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Evidence for a different source revision must not admit this adapter."""
    manifest_path = _write_admitted_manifest(tmp_path, mismatched_subject=True)
    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        lambda name: pytest.fail(f"unexpected SDK import: {name}"),
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(_read_only_settings(manifest_path))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"


def test_read_only_sdk_construction_forces_all_sdk_logging_off(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
            "FOSUN_SDK_ADMITTED": True,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
            "FOSUN_PROVIDER_MANIFEST": _write_admitted_manifest(tmp_path),
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


def _read_only_settings(manifest_path: Path) -> GatewaySettings:
    return GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "development",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
            "FOSUN_SDK_ADMITTED": True,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
            "FOSUN_PROVIDER_MANIFEST": manifest_path,
        }
    )


def _write_admitted_manifest(
    tmp_path: Path,
    *,
    missing_evidence: str | None = None,
    mismatched_subject: bool = False,
) -> Path:
    document = json.loads(MANIFEST_PATH.read_text())
    admission = document["admission"]
    assert isinstance(admission, dict)
    admission["status"] = "admitted"
    admission["evidence"] = {
        "written_permission": "synthetic-evidence/permission",
        "provider_test_vector": "synthetic-evidence/test-vector",
        "security_review": "synthetic-evidence/security-review",
        "tls_verification_remediation": "synthetic-evidence/tls-remediation",
    }
    if missing_evidence is not None:
        admission["evidence"][missing_evidence] = None
    if mismatched_subject:
        admission["sdk_commit"] = "0000000000000000000000000000000000000000"
    path = tmp_path / f"admission-{missing_evidence or 'complete'}.json"
    path.write_text(json.dumps(document))
    return path
