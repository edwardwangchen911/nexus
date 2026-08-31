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
OFFLINE_GUARD_PATH = Path(__file__).parents[4] / "scripts/offline_guard"
TEST_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
SUBPROCESS_SUCCESS_RESULT = {
    "status": "probe_published",
    "code": "OK",
    "artifact_sha256": "1" * 64,
    "byte_length": 123,
    "capability_expires_at": "2026-08-31T09:00:00Z",
    "activation_contract_hash": "2" * 64,
    "publication_receipt": {
        "schema_version": "nexus.fosun.probe-publication.v1",
        "artifact_sha256": "1" * 64,
        "signature_b64u": "A" * 86,
    },
}


def test_admitted_manifest_still_validates_plan_before_settings_or_sdk_import(
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
    assert output["code"] == "PROBE_SETTINGS_INVALID"
    assert output["publication_receipt"] is None
    assert settings_constructed is False
    assert captured.err == ""


def test_admitted_manifest_requires_a_valid_nonsecret_probe_plan(
    capfd: pytest.CaptureFixture[str],
) -> None:
    arguments = _arguments(MANIFEST_PATH)
    arguments[arguments.index("--limit") + 1] = "0"

    exit_code = probe_cli.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "INVALID_PROBE_PLAN"
    assert captured.err == ""


def test_failure_frame_contains_only_fixed_diagnostic_fields() -> None:
    frame = probe_cli.failure_result("PROBE_FAILED")

    assert frame == {
        "status": "probe_failed",
        "code": "PROBE_FAILED",
        "failure_stage": "probe",
        "failure_operation": "unknown",
        "failure_category": "unknown",
        "physical_request_count": 0,
        "artifact_sha256": None,
        "byte_length": None,
        "capability_expires_at": None,
        "activation_contract_hash": None,
        "publication_receipt": None,
    }


def test_failure_frame_preserves_only_closed_probe_diagnostics() -> None:
    frame = probe_cli.failure_result(
        ProbeSdkError(
            "PROBE_FAILED",
            operation="CashSummary",
            category="permission",
            physical_request_count=3,
        )
    )

    assert frame["failure_operation"] == "CashSummary"
    assert frame["failure_category"] == "permission"
    assert frame["physical_request_count"] == 3
    assert "synthetic-provider-secret" not in json.dumps(frame)


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("operation", "untrusted-operation"),
        ("category", "untrusted-category"),
        ("physical_request_count", -1),
        ("physical_request_count", True),
        ("physical_request_count", 25),
    ],
)
def test_failure_frame_fails_closed_for_invalid_diagnostics(
    attribute: str, value: object
) -> None:
    error = ProbeSdkError("PROBE_FAILED")
    setattr(error, attribute, value)

    frame = probe_cli.failure_result(error)

    assert frame["failure_operation"] == "unknown"
    assert frame["failure_category"] == "unknown"
    assert frame["physical_request_count"] == 0


def test_probe_cli_subprocess_is_one_frame_and_offline_without_secret_settings() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "nexus_fosun_gateway.probe_cli",
            *_arguments(MANIFEST_PATH),
        ],
        check=False,
        capture_output=True,
        env=_offline_subprocess_environment(),
        timeout=10,
    )

    assert completed.returncode == 2
    assert completed.stderr == b""
    assert completed.stdout.count(b"\n") == 1
    assert json.loads(completed.stdout)["code"] == "PROBE_SETTINGS_INVALID"


def test_probe_cli_success_contract_holds_in_a_real_subprocess() -> None:
    script = """
import json
import sys
import nexus_fosun_gateway.probe_cli as probe_cli

async def execute_once(_arguments):
    return json.loads(sys.argv[1])

probe_cli._execute_once = execute_once
raise SystemExit(probe_cli.main([]))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, json.dumps(SUBPROCESS_SUCCESS_RESULT)],
        check=False,
        capture_output=True,
        env=_offline_subprocess_environment(),
        timeout=10,
    )

    assert completed.returncode == 0
    assert completed.stderr == b""
    assert completed.stdout.count(b"\n") == 1
    assert json.loads(completed.stdout) == SUBPROCESS_SUCCESS_RESULT


def test_probe_cli_interrupt_contract_holds_in_a_real_subprocess() -> None:
    script = """
import nexus_fosun_gateway.probe_cli as probe_cli

async def execute_once(_arguments):
    raise KeyboardInterrupt

probe_cli._execute_once = execute_once
raise SystemExit(probe_cli.main([]))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        env=_offline_subprocess_environment(),
        timeout=10,
    )

    assert completed.returncode == 130
    assert completed.stderr == b""
    assert completed.stdout.count(b"\n") == 1
    assert json.loads(completed.stdout)["code"] == "PROBE_INTERRUPTED"


@pytest.mark.parametrize("failure_location", ["before_receipt", "inside_receipt", "end"])
def test_probe_cli_partial_success_frame_never_retries_in_a_real_subprocess(
    failure_location: str,
) -> None:
    expected = (
        json.dumps(SUBPROCESS_SUCCESS_RESULT, separators=(",", ":")).encode()
        + b"\n"
    )
    receipt_offset = expected.index(b'"publication_receipt"')
    fail_after = {
        "before_receipt": receipt_offset - 1,
        "inside_receipt": receipt_offset + 5,
        "end": len(expected) - 1,
    }[failure_location]
    script = """
import json
import os
import sys
import nexus_fosun_gateway.probe_cli as probe_cli

result = json.loads(sys.argv[1])
fail_after = int(sys.argv[2])
original_writer = probe_cli.write_result_frame
delivered = 0

async def execute_once(_arguments):
    return result

def partial_writer(file_descriptor, content):
    global delivered
    remaining = fail_after - delivered
    if remaining <= 0:
        raise OSError("synthetic-partial-frame")
    length = min(7, len(content), remaining)
    written = os.write(file_descriptor, content[:length])
    delivered += written
    return written

def write_once(value, *, file_descriptor=1):
    return original_writer(
        value,
        file_descriptor=file_descriptor,
        writer=partial_writer,
    )

probe_cli._execute_once = execute_once
probe_cli.write_result_frame = write_once
raise SystemExit(probe_cli.main([]))
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            json.dumps(SUBPROCESS_SUCCESS_RESULT),
            str(fail_after),
        ],
        check=False,
        capture_output=True,
        env=_offline_subprocess_environment(),
        timeout=10,
    )

    assert completed.returncode == 2
    assert completed.stderr == b""
    assert completed.stdout == expected[:fail_after]


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


def test_receipt_basename_is_rejected_before_transport_construction(
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
    arguments[arguments.index("--output-basename") + 1] = (
        "publication-receipt.json"
    )

    exit_code = probe_cli.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "INVALID_PROBE_PLAN"
    assert transport_built is False
    assert captured.err == ""


def test_ssl_key_log_environment_refuses_before_evidence_or_sdk_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    key_log = tmp_path / "tls-secrets.log"
    monkeypatch.setenv("SSLKEYLOGFILE", str(key_log))
    monkeypatch.setattr(
        probe_cli,
        "_read_bounded_or_empty",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError),
    )
    monkeypatch.setattr(
        probe_cli,
        "build_probe_transport",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError),
    )

    exit_code = probe_cli.main(_arguments(MANIFEST_PATH))

    captured = capfd.readouterr()
    assert exit_code == 2
    assert json.loads(captured.out)["code"] == "PROBE_SETTINGS_INVALID"
    assert captured.err == ""
    assert not key_log.exists()


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
                pagination=False,
                adjustment_semantics=False,
                response_contract=True,
            ),
            started_at=started_at,
            captured_at=started_at,
            private_key_factory=private_key_factory,
        )

    monkeypatch.setattr(probe_cli, "run_sanitized_probe", fake_run)
    real_manifest_reader = probe_cli._read_bounded_or_empty
    manifest_reads = 0

    def read_manifest_once(path: Path, *, max_bytes: int) -> bytes:
        nonlocal manifest_reads
        if path == manifest_path:
            manifest_reads += 1
            if manifest_reads > 1:
                raise OSError("manifest reopened")
        return real_manifest_reader(path, max_bytes=max_bytes)

    monkeypatch.setattr(probe_cli, "_read_bounded_or_empty", read_manifest_once)

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
    assert manifest_reads == 1
    assert captured.err == ""


def test_post_sequence_local_failure_keeps_only_physical_probe_facts(
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
        monkeypatch, manifest_path, output_dir, private_key_path, public_key_b64u
    )
    budget = probe_cli.PhysicalHttpBudget()
    budget._counts = {name: 1 for name in REQUIRED_PROBE_OPERATIONS}
    budget._total = 8
    budget._last_dispatched_label = "kline_minimal"
    monkeypatch.setattr(probe_cli, "PhysicalHttpBudget", lambda: budget)
    monkeypatch.setattr(probe_cli, "build_probe_transport", lambda *_args: object())
    monkeypatch.setattr(probe_cli, "SdkProbeExecutor", lambda *_args: object())

    async def fail_after_sequence(*_args: object, **_kwargs: object) -> object:
        raise ProbeSdkError("PROBE_FAILED")

    monkeypatch.setattr(probe_cli, "run_sanitized_probe", fail_after_sequence)

    assert probe_cli.main(_arguments(manifest_path)) == 2
    frame = json.loads(capfd.readouterr().out)
    assert frame["failure_operation"] == "kline_minimal"
    assert frame["physical_request_count"] == 8
    assert "publisher-private" not in json.dumps(frame)


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
                pagination=False,
                adjustment_semantics=False,
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

    writer_calls = 0

    def interrupt_writer(
        _result: dict[str, object],
        *,
        file_descriptor: int = 1,
    ) -> bool:
        nonlocal writer_calls
        writer_calls += 1
        assert file_descriptor == 1
        raise KeyboardInterrupt

    monkeypatch.setattr(probe_cli, "_execute_once", fail)
    monkeypatch.setattr(probe_cli, "write_result_frame", interrupt_writer)

    assert probe_cli.main([]) == 2
    captured = capfd.readouterr()
    assert writer_calls == 1
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
        "FOSUN_PROBE_HTTPS_PROXY_URL": "http://127.0.0.1:9090",
        "FSOPENAPI_BASE_URL": "https://openapi.fosunxcz.com",
        "FSOPENAPI_API_KEY": "synthetic-api-key-canary",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-client-private-key-canary",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-public-key-canary",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _admitted_manifest() -> dict[str, object]:
    manifest = json.loads(MANIFEST_PATH.read_bytes())
    manifest["sdk"]["license"] = "PROVIDER-AUTHORIZED-PRIVATE-USE"
    manifest["admission"]["status"] = "admitted"
    manifest["admission"]["provider_base_url"] = "https://openapi.fosunxcz.com"
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


def _offline_subprocess_environment() -> dict[str, str]:
    return {
        **os.environ,
        "NEXUS_OFFLINE_SOCKET_GUARD": "1",
        "PYTHONPATH": str(OFFLINE_GUARD_PATH),
    }
