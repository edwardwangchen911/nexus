import base64
import hashlib
import http.client
import io
import json
import logging
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import nexus_fosun_gateway.main as main_module
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import REQUIRED_PROBE_OPERATIONS, evaluate_activation
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.main import build_transport
from nexus_fosun_gateway.probe_contract import probe_response_contract_sha256
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import ValidationError

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"
PUBLICATION_DOMAIN = b"NEXUS-FOSUN-PROBE-PUBLICATION-V1\0"
TEST_PUBLICATION_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))


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


def test_sdk_mode_rejects_ssl_key_log_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imported = False
    key_log = tmp_path / "tls-secrets.log"
    monkeypatch.setenv("SSLKEYLOGFILE", str(key_log))

    def forbidden_import(_name: str) -> object:
        nonlocal imported
        imported = True
        raise AssertionError

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        forbidden_import,
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(GatewaySettings.model_validate(_activated_read_only_values(tmp_path)))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is False
    assert not key_log.exists()


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
    Path(str(values["FOSUN_ACTIVATION_CONTRACT_HASH_FILE"])).write_text("0" * 64)
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


def test_publisher_pin_mismatch_cannot_import_sdk_even_with_its_contract_hash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _activated_read_only_values(tmp_path)
    alternate_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    alternate_public_key = _b64u(
        alternate_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    manifest_path = Path(str(values["FOSUN_PROVIDER_MANIFEST"]))
    probe_path = Path(str(values["FOSUN_ACTIVATION_PROBE_ARTIFACT"]))
    receipt_path = Path(str(values["FOSUN_ACTIVATION_PROBE_RECEIPT"]))
    blocked = evaluate_activation(
        manifest_path.read_bytes(),
        probe_path.read_bytes(),
        receipt_path.read_bytes(),
        trusted_publication_public_key_bytes=alternate_public_key.encode("ascii"),
        checked_at=datetime.now(UTC),
    )
    assert blocked.next_action == "REVIEW_PROBE_PUBLISHER_BINDING"
    Path(
        str(values["FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE"])
    ).write_text(alternate_public_key)
    Path(str(values["FOSUN_ACTIVATION_CONTRACT_HASH_FILE"])).write_text(
        blocked.contract_hash
    )
    imported = False

    def forbidden_import(_name: str) -> object:
        nonlocal imported
        imported = True
        raise AssertionError

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        forbidden_import,
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(GatewaySettings.model_validate(values))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is False


def test_missing_publication_receipt_file_cannot_import_sdk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _activated_read_only_values(tmp_path)
    receipt_path = Path(str(values["FOSUN_ACTIVATION_PROBE_RECEIPT"]))
    receipt_path.unlink()
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
    "attack_kind",
    [
        "publisher_symlink",
        "contract_hard_link",
        "publisher_writable",
        "contract_trailing_newline",
    ],
)
def test_activation_control_files_fail_closed_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    attack_kind: str,
) -> None:
    values = _activated_read_only_values(tmp_path)
    publisher_path = Path(
        str(values["FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE"])
    )
    contract_path = Path(str(values["FOSUN_ACTIVATION_CONTRACT_HASH_FILE"]))
    if attack_kind == "publisher_symlink":
        actual_path = tmp_path / "actual-publisher-public-key.b64u"
        actual_path.write_bytes(publisher_path.read_bytes())
        actual_path.chmod(0o600)
        publisher_path.unlink()
        publisher_path.symlink_to(actual_path)
    elif attack_kind == "contract_hard_link":
        hard_link_path = tmp_path / "hard-linked-activation-contract.sha256"
        hard_link_path.hardlink_to(contract_path)
        values["FOSUN_ACTIVATION_CONTRACT_HASH_FILE"] = hard_link_path
    elif attack_kind == "publisher_writable":
        publisher_path.chmod(0o666)
    else:
        contract_path.write_text(contract_path.read_text() + "\n")
    imported = False

    def forbidden_import(_name: str) -> object:
        nonlocal imported
        imported = True
        raise AssertionError

    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        forbidden_import,
    )

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
        " https://provider.invalid",
        "https://provider.invalid ",
        "https://provider.invalid/",
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


@pytest.mark.parametrize(
    "path_setting",
    [
        "FOSUN_PROVIDER_MANIFEST",
        "FOSUN_ACTIVATION_PROBE_ARTIFACT",
        "FOSUN_ACTIVATION_PROBE_RECEIPT",
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE",
        "FOSUN_ACTIVATION_CONTRACT_HASH_FILE",
    ],
)
def test_production_evidence_paths_reject_nul_before_runtime_read(
    tmp_path: Path,
    path_setting: str,
) -> None:
    values = _activated_read_only_values(tmp_path)
    values[path_setting] = "/run/nexus/invalid\x00path"

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


def test_manifest_is_read_once_for_admission_activation_and_base_url_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _activated_read_only_values(tmp_path)
    manifest_path = Path(str(values["FOSUN_PROVIDER_MANIFEST"]))
    manifest_bytes = manifest_path.read_bytes()
    real_reader = main_module._read_bounded_or_empty
    manifest_reads = 0
    imported = False

    def changing_reader(path: Path, *, max_bytes: int) -> bytes:
        nonlocal manifest_reads
        if path == manifest_path:
            manifest_reads += 1
            return manifest_bytes if manifest_reads == 1 else b"{}"
        return real_reader(path, max_bytes=max_bytes)

    def forbidden_import(_name: str) -> object:
        nonlocal imported
        imported = True
        raise ModuleNotFoundError

    monkeypatch.setattr(main_module, "_read_bounded_or_empty", changing_reader)
    monkeypatch.setattr(
        "nexus_fosun_gateway.main.importlib.import_module",
        forbidden_import,
    )

    with pytest.raises(GatewayError) as exc_info:
        build_transport(GatewaySettings.model_validate(values))

    assert exc_info.value.code == "SDK_NOT_ADMITTED"
    assert imported is True
    assert manifest_reads == 1


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
async def test_sdk_dependency_logs_are_suppressed_at_construction_and_call_edges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canaries = (
        "https://provider.invalid/private?api_key=synthetic-secret",
        "Authorization=Bearer synthetic-token",
        "body=synthetic-body",
        "cert=/private/synthetic-client.pem",
        "account=synthetic-account-001",
    )
    dependency_names = (
        "fsopenapi",
        "fsopenapi.auth",
        "fsopenapi.transport",
        "requests",
        "requests.sessions",
        "urllib3",
        "urllib3.connectionpool",
        "http.client",
    )
    captured = io.StringIO()
    handler = logging.StreamHandler(captured)
    for name in dependency_names:
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "filters", [])
        monkeypatch.setattr(logger, "handlers", [handler])
        monkeypatch.setattr(logger, "propagate", True)
        monkeypatch.setattr(logger, "disabled", False)
        monkeypatch.setattr(logger, "level", logging.DEBUG)
    monkeypatch.setattr(http.client.HTTPConnection, "debuglevel", 1)
    monkeypatch.setattr(http.client.HTTPSConnection, "debuglevel", 1)

    def emit_dependency_canaries() -> None:
        assert http.client.HTTPConnection.debuglevel == 0
        assert http.client.HTTPSConnection.debuglevel == 0
        for name in dependency_names:
            logging.getLogger(name).debug(" ".join(canaries))

    class FakeResponse:
        headers: dict[str, str] = {}

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            assert chunk_size > 0
            return (b'{"code":0}',)

        def close(self) -> None:
            return None

    class FakeSession:
        trust_env = True

        def request(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

    class FakeRequests:
        Session = FakeSession

        def post(self, *_args: object, **_kwargs: object) -> object:
            pytest.fail("module-level requests.post must not be used")

    class FakeSdkClient:
        def __init__(self, **_kwargs: object) -> None:
            emit_dependency_canaries()
            self._http_session = FakeSession()
            self.market = SimpleNamespace(quote=self.quote)

        def quote(self, *, codes: list[str]) -> object:
            assert codes == ["usAAPL"]
            emit_dependency_canaries()
            return {
                "code": 0,
                "data": {},
                "requestId": "synthetic-logging-request",
            }

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
    transport = build_transport(
        GatewaySettings.model_validate(_activated_read_only_values(tmp_path))
    )
    assert isinstance(transport, SdkFosunTransport)
    assert captured.getvalue() == ""

    # Simulate a runtime logging reconfiguration after construction. The
    # transport must restore the closed dependency boundary at every SDK call.
    for name in dependency_names:
        logger = logging.getLogger(name)
        logger.filters.clear()
        logger.handlers[:] = [handler]
        logger.disabled = False
        logger.propagate = True
        logger.setLevel(logging.DEBUG)
    http.client.HTTPConnection.debuglevel = 1
    http.client.HTTPSConnection.debuglevel = 1
    try:
        page = await transport.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    finally:
        await transport.aclose()

    assert page.source_request_id == "synthetic-logging-request"
    logged = captured.getvalue()
    for canary in canaries:
        assert canary not in logged


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
        activation_checked_at=expired_at,
        activation_monotonic_anchor=100.0,
        clock=lambda: 100.0,
        utc_clock=lambda: expired_at,
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is False


def test_activation_expiry_makes_sdk_transport_unready() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    checked_at = expires_at - timedelta(seconds=1)
    current = [checked_at]
    monotonic = [100.0]
    expired: list[datetime] = []
    transport = SdkFosunTransport(
        SimpleNamespace(),
        capability_expires_at=expires_at,
        activation_checked_at=checked_at,
        activation_monotonic_anchor=100.0,
        utc_clock=lambda: current[0],
        clock=lambda: monotonic[0],
        on_activation_expired=expired.append,
    )

    assert transport.is_ready()
    assert transport.readiness_expires_at() == expires_at
    current[0] = checked_at - timedelta(hours=1)
    monotonic[0] = 100.999
    assert transport.is_ready()
    monotonic[0] = 101.0
    assert not transport.is_ready()
    assert expired == [expires_at]

    current[0] = checked_at
    assert not transport.is_ready()
    assert expired == [expires_at]


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
        activation_checked_at=expires_at - timedelta(seconds=1),
        activation_monotonic_anchor=100.0,
        clock=lambda: 100.0,
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
            expires_at - timedelta(seconds=4),
            expires_at - timedelta(seconds=3),
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
        activation_checked_at=expires_at - timedelta(seconds=4),
        activation_monotonic_anchor=100.0,
        clock=lambda: 100.0,
        utc_clock=lambda: next(times),
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is True


def test_monotonic_watchdog_expires_without_a_wall_clock_observation() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    checked_at = expires_at - timedelta(milliseconds=30)
    expired = threading.Event()
    callback_threads: list[str] = []

    def on_expired(_expires_at: datetime) -> None:
        callback_threads.append(threading.current_thread().name)
        expired.set()

    anchor = time.monotonic()
    transport = SdkFosunTransport(
        SimpleNamespace(),
        capability_expires_at=expires_at,
        activation_checked_at=checked_at,
        activation_monotonic_anchor=anchor,
        utc_clock=lambda: checked_at - timedelta(hours=1),
        on_activation_expired=on_expired,
    )

    assert expired.wait(timeout=1)
    assert not transport.is_ready()
    assert callback_threads == ["nexus-fosun-activation-expiry"]


def test_expiry_persistence_failure_permanently_poisons_transport() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    checked_at = expires_at - timedelta(seconds=1)
    current = [checked_at]
    poisoned = threading.Event()

    def fail_persistence(_expires_at: datetime) -> None:
        raise RuntimeError("synthetic persistence failure")

    transport = SdkFosunTransport(
        SimpleNamespace(),
        capability_expires_at=expires_at,
        activation_checked_at=checked_at,
        activation_monotonic_anchor=100.0,
        clock=lambda: 100.0,
        utc_clock=lambda: current[0],
        on_poison=poisoned.set,
        on_activation_expired=fail_persistence,
    )

    current[0] = expires_at
    assert not transport.is_ready()
    current[0] = checked_at
    assert not transport.is_ready()
    assert poisoned.is_set()


@pytest.mark.asyncio
async def test_sdk_daemon_thread_can_irreversibly_latch_activation_expiry() -> None:
    expires_at = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
    checked_at = expires_at - timedelta(hours=1)
    callback_threads: list[str] = []
    called = False

    def utc_clock() -> datetime:
        if threading.current_thread().name == "nexus-fosun-sdk":
            return expires_at
        return checked_at

    def list_accounts() -> object:
        nonlocal called
        called = True
        return {}

    transport = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        capability_expires_at=expires_at,
        activation_checked_at=checked_at,
        activation_monotonic_anchor=time.monotonic(),
        utc_clock=utc_clock,
        on_activation_expired=lambda _expiry: callback_threads.append(
            threading.current_thread().name
        ),
    )

    with pytest.raises(GatewayError) as exc_info:
        await transport.execute("fosun_sync_accounts", {})

    assert exc_info.value.code == "ACTIVATION_EXPIRED"
    assert called is False
    assert callback_threads == ["nexus-fosun-sdk"]
    assert not transport.is_ready()


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
            "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA": "/synthetic/broker-client-ca.crt",
            "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA": "/synthetic/market-client-ca.crt",
            "FOSUN_SDK_ADMITTED": True,
            "FOSUN_ACTIVATION_PROBE_ARTIFACT": "/synthetic/missing-probe.json",
            "FOSUN_ACTIVATION_PROBE_RECEIPT": "/synthetic/missing-receipt.json",
            "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE": (
                "/synthetic/missing-publisher-public-key.b64u"
            ),
            "FOSUN_ACTIVATION_CONTRACT_HASH_FILE": (
                "/synthetic/missing-activation-contract.sha256"
            ),
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
    public_key = TEST_PUBLICATION_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    probe = {
        "schema_version": "nexus.fosun.sanitized-probe.v2",
        "provider": "fosun",
        "captured_at": captured_at.isoformat().replace("+00:00", "Z"),
        "capability_expires_at": (captured_at + timedelta(hours=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
        "publication_public_key_b64u": _b64u(public_key),
        "operations": [
            {
                "name": name,
                "request_count": 1,
                "passed": True,
                "response_contract_sha256": probe_response_contract_sha256(name),
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
    artifact_sha256 = hashlib.sha256(probe_bytes).hexdigest()
    receipt_bytes = json.dumps(
        {
            "schema_version": "nexus.fosun.probe-publication.v1",
            "artifact_sha256": artifact_sha256,
            "signature_b64u": _b64u(
                TEST_PUBLICATION_KEY.sign(
                    PUBLICATION_DOMAIN + bytes.fromhex(artifact_sha256)
                )
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    receipt_path = tmp_path / "sanitized-probe-receipt.json"
    receipt_path.write_bytes(receipt_bytes)
    trusted_key_bytes = _b64u(public_key).encode("ascii")
    activation = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        trusted_publication_public_key_bytes=trusted_key_bytes,
        checked_at=captured_at,
    )
    assert activation.status == "activated_read_only"
    trusted_key_path = tmp_path / "publisher-public-key.b64u"
    trusted_key_path.write_bytes(trusted_key_bytes)
    trusted_key_path.chmod(0o600)
    activation_contract_path = tmp_path / "activation-contract.sha256"
    activation_contract_path.write_text(activation.contract_hash)
    activation_contract_path.chmod(0o600)
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
        "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA": "/synthetic/broker-client-ca.crt",
        "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA": "/synthetic/market-client-ca.crt",
        "FOSUN_SDK_ADMITTED": True,
        "FOSUN_ACTIVATION_PROBE_ARTIFACT": probe_path,
        "FOSUN_ACTIVATION_PROBE_RECEIPT": receipt_path,
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE": trusted_key_path,
        "FOSUN_ACTIVATION_CONTRACT_HASH_FILE": activation_contract_path,
        "FSOPENAPI_BASE_URL": "https://provider.invalid",
        "FSOPENAPI_API_KEY": "synthetic-api-key",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
        "FOSUN_PROVIDER_MANIFEST": manifest_path,
    }


def _b64u(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()
