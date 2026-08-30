import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.activation import REQUIRED_PROBE_OPERATIONS, evaluate_activation
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.main import build_transport
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import ValidationError

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
    settings = _read_only_settings(MANIFEST_PATH)

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
    settings = GatewaySettings.model_validate(_activated_read_only_values(tmp_path))

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


def test_admitted_manifest_without_activation_probe_cannot_import_sdk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        return SimpleNamespace(SDKClient=object)

    monkeypatch.setattr("nexus_fosun_gateway.main.importlib.import_module", forbidden_import)

    with pytest.raises(GatewayError) as exc_info:
        build_transport(_read_only_settings(_write_admitted_manifest(tmp_path)))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is False


def test_activation_contract_hash_mismatch_cannot_import_sdk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _activated_read_only_values(tmp_path)
    values["FOSUN_ACTIVATION_CONTRACT_HASH"] = "0" * 64
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        return SimpleNamespace(SDKClient=object)

    monkeypatch.setattr("nexus_fosun_gateway.main.importlib.import_module", forbidden_import)

    with pytest.raises(GatewayError) as exc_info:
        build_transport(GatewaySettings.model_validate(values))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is False


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://provider.invalid",
        "https://user:password@provider.invalid",
        "https://provider.invalid?redirect=elsewhere",
        "https://provider.invalid:notaport",
        "provider.invalid",
    ],
)
def test_provider_base_url_requires_unambiguous_https(
    tmp_path: Path,
    unsafe_url: str,
) -> None:
    values = _activated_read_only_values(tmp_path)
    values["FSOPENAPI_BASE_URL"] = unsafe_url

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)


def test_runtime_provider_base_url_must_match_the_admitted_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _activated_read_only_values(tmp_path)
    values["FSOPENAPI_BASE_URL"] = "https://different-provider.invalid"
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        return SimpleNamespace(SDKClient=object)

    monkeypatch.setattr("nexus_fosun_gateway.main.importlib.import_module", forbidden_import)

    with pytest.raises(GatewayError) as exc_info:
        build_transport(GatewaySettings.model_validate(values))

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
    tls_calls: list[tuple[str, object, object, object, object, object]] = []

    class FakeResponse:
        headers: dict[str, str] = {}

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            assert chunk_size > 0
            return (b'{"code":0}',)

        def close(self) -> None:
            return None

    class FakeSession:
        trust_env = True

        def request(self, *_args: object, **kwargs: object) -> object:
            tls_calls.append(
                (
                    "request",
                    kwargs.get("verify"),
                    kwargs.get("allow_redirects"),
                    kwargs.get("timeout"),
                    kwargs.get("stream"),
                    self.trust_env,
                )
            )
            return FakeResponse()

        def post(self, *_args: object, **kwargs: object) -> object:
            tls_calls.append(
                (
                    "session",
                    kwargs.get("verify"),
                    kwargs.get("allow_redirects"),
                    kwargs.get("timeout"),
                    kwargs.get("stream"),
                    self.trust_env,
                )
            )
            return FakeResponse()

    class FakeRequests:
        Session = FakeSession

        def post(self, *_args: object, **kwargs: object) -> object:
            pytest.fail("module-level requests.post must not be used")

    class FakeSdkClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)
            self._http_session = FakeSession()

    auth_module = SimpleNamespace(requests=FakeRequests())

    def sdk_module(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=FakeSdkClient)
        if name == "fsopenapi.auth":
            return auth_module
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        sdk_module,
    )
    settings = GatewaySettings.model_validate(_activated_read_only_values(tmp_path))

    transport = build_transport(settings)

    assert isinstance(transport, SdkFosunTransport)
    assert captured == {
        "base_url": "https://provider.invalid",
        "api_key": "synthetic-api-key",
        "logging_enable": False,
        "log_body": False,
    }
    transport._client._http_session.request(  # type: ignore[attr-defined]  # noqa: SLF001
        "POST", "https://provider.invalid", verify=False
    )
    auth_module.requests.post("https://provider.invalid", verify=False)
    assert tls_calls == [
        ("request", True, False, (5.0, 5.0), True, False),
        ("session", True, False, (5.0, 5.0), True, False),
    ]


@pytest.mark.asyncio
async def test_expired_activation_stops_every_sdk_operation_before_client_call() -> None:
    expired_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    called = False

    def list_accounts() -> object:
        nonlocal called
        called = True
        return {}

    transport = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        capability_expires_at=expired_at,
        utc_clock=lambda: expired_at,
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is False


@pytest.mark.asyncio
async def test_activation_expiry_is_rechecked_after_queueing_before_provider_call() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    times = iter((expires_at - timedelta(seconds=1), expires_at))
    called = False

    def list_accounts() -> object:
        nonlocal called
        called = True
        return {}

    transport = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        capability_expires_at=expires_at,
        utc_clock=lambda: next(times),
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is False


@pytest.mark.asyncio
async def test_activation_expiry_is_rechecked_after_provider_response() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    times = iter(
        (
            expires_at - timedelta(seconds=2),
            expires_at - timedelta(seconds=1),
            expires_at,
        )
    )
    called = False

    def list_accounts() -> object:
        nonlocal called
        called = True
        return {"code": 0, "data": {"subAccounts": []}, "requestId": "synthetic"}

    transport = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        capability_expires_at=expires_at,
        utc_clock=lambda: next(times),
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is True


def _read_only_settings(manifest_path: Path) -> GatewaySettings:
    return GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "production",
            "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_GATEWAY_REPLAY_STORE_PATH": "/synthetic/fosun-gateway/replay.sqlite3",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-market-current",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": (
                "synthetic-market-current-secret-32-bytes"
            ),
            "FOSUN_GATEWAY_SERVER_CERT": "/synthetic/server.crt",
            "FOSUN_GATEWAY_SERVER_KEY": "/synthetic/server.key",
            "FOSUN_GATEWAY_SERVER_CLIENT_CA": "/synthetic/client-ca.crt",
            "FOSUN_SDK_ADMITTED": True,
            "FOSUN_ACTIVATION_PROBE_ARTIFACT": "/synthetic/missing-probe.json",
            "FOSUN_ACTIVATION_CONTRACT_HASH": "a" * 64,
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
    document["sdk"]["license"] = "WRITTEN-PERMISSION"
    admission = document["admission"]
    assert isinstance(admission, dict)
    admission["status"] = "admitted"
    admission["provider_base_url"] = "https://provider.invalid"
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


def _activated_read_only_values(tmp_path: Path) -> dict[str, object]:
    manifest_path = _write_admitted_manifest(tmp_path)
    manifest_bytes = manifest_path.read_bytes()
    captured_at = datetime.now(UTC)
    manifest = json.loads(manifest_bytes)
    probe = {
        "schema_version": "nexus.fosun.sanitized-probe.v1",
        "provider": "fosun",
        "captured_at": captured_at.isoformat(),
        "capability_expires_at": (captured_at + timedelta(hours=1)).isoformat(),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
        "operations": [
            {
                "name": name,
                "request_count": 1,
                "passed": True,
                "response_contract_sha256": hashlib.sha256(name.encode()).hexdigest(),
            }
            for name in REQUIRED_PROBE_OPERATIONS
        ],
        "checks": {
            "permission_read_only": True,
            "history_window": True,
            "adjustment_semantics": True,
            "pagination": True,
            "rate_limit": True,
            "response_contract": True,
        },
    }
    probe_bytes = json.dumps(probe, sort_keys=True, separators=(",", ":")).encode()
    probe_path = tmp_path / "sanitized-probe.json"
    probe_path.write_bytes(probe_bytes)
    activation = evaluate_activation(manifest_bytes, probe_bytes, checked_at=captured_at)
    assert activation.status == "activated_read_only"
    return {
        "NEXUS_ENV": "production",
        "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
        "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
        "FOSUN_GATEWAY_REPLAY_STORE_PATH": tmp_path / "replay.sqlite3",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-market-current",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": ("synthetic-market-current-secret-32-bytes"),
        "FOSUN_GATEWAY_SERVER_CERT": "/synthetic/server.crt",
        "FOSUN_GATEWAY_SERVER_KEY": "/synthetic/server.key",
        "FOSUN_GATEWAY_SERVER_CLIENT_CA": "/synthetic/client-ca.crt",
        "FOSUN_SDK_ADMITTED": True,
        "FOSUN_ACTIVATION_PROBE_ARTIFACT": probe_path,
        "FOSUN_ACTIVATION_CONTRACT_HASH": activation.contract_hash,
        "FSOPENAPI_BASE_URL": "https://provider.invalid",
        "FSOPENAPI_API_KEY": "synthetic-api-key",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
        "FOSUN_PROVIDER_MANIFEST": manifest_path,
    }
