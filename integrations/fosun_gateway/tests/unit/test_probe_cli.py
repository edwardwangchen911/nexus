import base64
import json
from pathlib import Path

import nexus_fosun_gateway.probe_cli as probe_cli
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import REQUIRED_PROBE_OPERATIONS
from nexus_fosun_gateway.adapter.probe_sdk import ProbeExecutionEvidence, ProbeSdkError
from nexus_fosun_gateway.probe import _build_prepared_probe

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"
TEST_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))


def test_blocked_manifest_stops_before_settings_or_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        probe_cli,
        "build_probe_transport",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError),
    )

    exit_code = probe_cli.main(_arguments(MANIFEST_PATH))

    captured = capfd.readouterr()
    output = json.loads(captured.out)
    assert exit_code == 2
    assert output["status"] == "probe_failed"
    assert output["code"] == "COMPLETE_SDK_ADMISSION"
    assert output["publication_receipt"] is None
    assert captured.err == ""


def test_parser_error_never_echoes_unknown_operator_input(
    capfd: pytest.CaptureFixture[str],
) -> None:
    canary = "synthetic-cli-secret-canary"

    exit_code = probe_cli.main(["--api-key", canary])

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "INVALID_PROBE_PLAN"
    assert canary not in captured.out
    assert canary not in captured.err


def test_probe_failure_never_loads_the_publication_private_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(_json_bytes(_admitted_manifest()))
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    private_key_path = tmp_path / "publisher-private.b64u"
    public_key_b64u = _b64u(
        TEST_KEY.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    _set_probe_environment(
        monkeypatch,
        manifest_path,
        output_dir,
        private_key_path,
        public_key_b64u,
    )
    key_loads: list[Path] = []

    def track_key_load(path: Path, _trusted_key: str) -> Ed25519PrivateKey:
        key_loads.append(path)
        return TEST_KEY

    async def fail_before_artifact(*_args, **_kwargs):
        raise ProbeSdkError("PROBE_FAILED")

    monkeypatch.setattr(probe_cli, "load_probe_signing_key", track_key_load)
    monkeypatch.setattr(probe_cli, "build_probe_transport", lambda *_args: object())
    monkeypatch.setattr(probe_cli, "SdkProbeExecutor", lambda *_args: object())
    monkeypatch.setattr(probe_cli, "run_sanitized_probe", fail_before_artifact)

    exit_code = probe_cli.main(_arguments(manifest_path))

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "PROBE_FAILED"
    assert key_loads == []
    assert captured.err == ""


def test_success_publishes_before_signing_and_emits_receipt_last(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(_json_bytes(_admitted_manifest()))
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    private_key_path = tmp_path / "publisher-private.b64u"
    private_key_path.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    private_key_path.chmod(0o600)
    public_key_b64u = _b64u(
        TEST_KEY.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    _set_probe_environment(
        monkeypatch,
        manifest_path,
        output_dir,
        private_key_path,
        public_key_b64u,
    )

    monkeypatch.setattr(probe_cli, "build_probe_transport", lambda *_args: object())
    monkeypatch.setattr(probe_cli, "SdkProbeExecutor", lambda *_args: object())

    async def fake_run(
        manifest_bytes,
        plan,
        _executor,
        *,
        started_at,
        private_key_factory,
        utc_clock,
    ):
        del utc_clock
        return _build_prepared_probe(
            manifest_bytes,
            plan,
            ProbeExecutionEvidence(
                request_counts=tuple(
                    (name, 1) for name in REQUIRED_PROBE_OPERATIONS
                ),
                auth_successes=1,
                history_window=True,
                pagination=True,
                adjustment_semantics=True,
                response_contract=True,
            ),
            started_at=started_at,
            captured_at=started_at,
            private_key_factory=private_key_factory,
        )

    monkeypatch.setattr(probe_cli, "run_sanitized_probe", fake_run)

    exit_code = probe_cli.main(_arguments(manifest_path))

    captured = capfd.readouterr()
    output = json.loads(captured.out)
    assert exit_code == 0, output["code"]
    assert output["status"] == "probe_published"
    assert output["code"] == "OK"
    assert output["publication_receipt"]["artifact_sha256"] == output["artifact_sha256"]
    assert captured.out.rfind('"publication_receipt"') > captured.out.rfind(
        '"activation_contract_hash"'
    )
    assert (output_dir / "probe-20260831.json").exists()
    assert captured.err == ""


def test_low_level_frame_writer_does_not_retry_with_a_second_frame() -> None:
    calls = 0

    def failing_writer(_fd: int, content: memoryview) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return min(5, len(content))
        raise OSError("synthetic-stdout-canary")

    complete = probe_cli.write_result_frame(
        probe_cli.failure_result("PROBE_FAILED"),
        writer=failing_writer,
    )

    assert complete is False
    assert calls == 2


def _arguments(manifest_path: Path) -> list[str]:
    return [
        "--manifest",
        str(manifest_path),
        "--symbol",
        "usAAPL",
        "--session-date",
        "2026-08-28",
        "--cash-flow-date-from",
        "2026-08-24",
        "--cash-flow-date-to",
        "2026-08-28",
        "--limit",
        "1",
        "--capability-ttl-seconds",
        "3600",
        "--output-basename",
        "probe-20260831.json",
    ]


def _set_probe_environment(
    monkeypatch: pytest.MonkeyPatch,
    manifest_path: Path,
    output_dir: Path,
    private_key_path: Path,
    public_key_b64u: str,
) -> None:
    values = {
        "FOSUN_PROVIDER_MANIFEST": str(manifest_path),
        "FOSUN_SDK_ADMITTED": "true",
        "FOSUN_PROBE_OUTPUT_DIR": str(output_dir),
        "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE": str(private_key_path),
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U": public_key_b64u,
        "FSOPENAPI_BASE_URL": "https://provider.invalid",
        "FSOPENAPI_API_KEY": "synthetic-api-key-canary",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-client-private-key-canary",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-public-key-canary",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _admitted_manifest() -> dict[str, object]:
    manifest = json.loads(MANIFEST_PATH.read_bytes())
    manifest["sdk"]["license"] = "WRITTEN-PERMISSION"
    manifest["admission"]["status"] = "admitted"
    manifest["admission"]["provider_base_url"] = "https://provider.invalid"
    manifest["admission"]["evidence"] = {
        "written_permission": "synthetic/permission",
        "provider_test_vector": "synthetic/test-vector",
        "security_review": "synthetic/security-review",
        "tls_verification_remediation": "synthetic/tls-review",
    }
    return manifest


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _b64u(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
