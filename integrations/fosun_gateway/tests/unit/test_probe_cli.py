import base64
import json
import os
import subprocess
import sys
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
    settings_constructed = False

    def forbidden_settings(*_args: object, **_kwargs: object) -> object:
        nonlocal settings_constructed
        settings_constructed = True
        raise AssertionError

    monkeypatch.setattr(
        probe_cli.ProbeProviderSettings,
        "model_validate",
        forbidden_settings,
    )
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
    assert settings_constructed is False
    assert captured.err == ""


def test_blocked_manifest_precedes_nonsecret_plan_validation(
    capfd: pytest.CaptureFixture[str],
) -> None:
    arguments = _arguments(MANIFEST_PATH)
    arguments[arguments.index("--limit") + 1] = "0"

    exit_code = probe_cli.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "COMPLETE_SDK_ADMISSION"
    assert captured.err == ""


def test_probe_cli_subprocess_is_one_frame_and_offline_for_blocked_manifest() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "nexus_fosun_gateway.probe_cli",
            *_arguments(MANIFEST_PATH),
        ],
        check=False,
        capture_output=True,
        env={
            **os.environ,
            "NEXUS_OFFLINE_SOCKET_GUARD": "1",
        },
        timeout=10,
    )

    assert completed.returncode == 2
    assert completed.stderr == b""
    assert completed.stdout.count(b"\n") == 1
    assert json.loads(completed.stdout)["code"] == "COMPLETE_SDK_ADMISSION"


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


def test_manifest_path_length_is_rejected_before_any_evidence_read(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        probe_cli,
        "_read_bounded_or_empty",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError),
    )
    arguments = _arguments(MANIFEST_PATH)
    arguments[1] = "/" + "a" * 4096

    exit_code = probe_cli.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "INVALID_PROBE_PLAN"
    assert captured.err == ""


def test_manifest_environment_path_must_match_exact_cli_spelling(
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
    transport_built = False

    def forbidden_transport(*_args: object) -> object:
        nonlocal transport_built
        transport_built = True
        raise AssertionError

    monkeypatch.setattr(probe_cli, "build_probe_transport", forbidden_transport)
    arguments = _arguments(manifest_path)
    arguments[1] = f"{manifest_path.parent}/./{manifest_path.name}"

    exit_code = probe_cli.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "PROBE_SETTINGS_INVALID"
    assert transport_built is False
    assert captured.err == ""


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


def test_interrupt_after_artifact_publication_requires_quarantine(
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

    def interrupt_receipt(*_args: object, **_kwargs: object) -> bytes:
        raise KeyboardInterrupt

    monkeypatch.setattr(probe_cli, "run_sanitized_probe", fake_run)
    monkeypatch.setattr(
        probe_cli.PreparedProbeArtifact,
        "sign_publication_receipt",
        interrupt_receipt,
    )

    exit_code = probe_cli.main(_arguments(manifest_path))

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
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


def test_success_frame_writer_handles_short_writes_without_duplication() -> None:
    result = {
        **probe_cli.failure_result("OK"),
        "status": "probe_published",
        "publication_receipt": {"artifact_sha256": "0" * 64},
    }
    delivered = bytearray()

    def short_writer(_fd: int, content: memoryview) -> int:
        length = min(3, len(content))
        delivered.extend(content[:length])
        return length

    assert probe_cli.write_result_frame(result, writer=short_writer) is True
    assert delivered.count(b"\n") == 1
    assert json.loads(delivered)["status"] == "probe_published"


def test_success_frame_never_retries_before_during_or_after_receipt_failure() -> None:
    result = {
        **probe_cli.failure_result("OK"),
        "status": "probe_published",
        "publication_receipt": {"artifact_sha256": "0" * 64},
    }
    expected = json.dumps(result, separators=(",", ":")).encode() + b"\n"
    receipt_offset = expected.index(b'"publication_receipt"')

    for fail_after in (receipt_offset - 1, receipt_offset + 5, len(expected) - 1):
        delivered = bytearray()

        def failing_writer(_fd: int, content: memoryview) -> int:
            remaining = fail_after - len(delivered)
            if remaining <= 0:
                raise OSError("synthetic-delivery-canary")
            length = min(7, len(content), remaining)
            delivered.extend(content[:length])
            return length

        assert probe_cli.write_result_frame(result, writer=failing_writer) is False
        assert delivered == expected[:fail_after]


def test_main_maps_probe_interrupt_to_one_fixed_exit_130_frame(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    async def interrupt(_arguments: tuple[str, ...]) -> dict[str, object]:
        raise KeyboardInterrupt

    monkeypatch.setattr(probe_cli, "_execute_once", interrupt)

    exit_code = probe_cli.main([])

    captured = capfd.readouterr()
    assert exit_code == 130
    assert json.loads(captured.out)["code"] == "PROBE_INTERRUPTED"
    assert captured.out.count("\n") == 1
    assert captured.err == ""


def test_main_contains_interrupt_from_uncertain_stdout_delivery(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    async def fail(_arguments: tuple[str, ...]) -> dict[str, object]:
        return probe_cli.failure_result("PROBE_FAILED")

    monkeypatch.setattr(probe_cli, "_execute_once", fail)
    monkeypatch.setattr(
        probe_cli,
        "write_result_frame",
        lambda _result: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    assert probe_cli.main([]) == 2
    captured = capfd.readouterr()
    assert captured.out == ""
    assert captured.err == ""


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
