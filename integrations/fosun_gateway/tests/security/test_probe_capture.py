import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import nexus_fosun_gateway.probe_capture as probe_capture
import nexus_fosun_gateway.probe_cli as probe_cli
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.adapter.probe_sdk import ProbeSdkError

MANIFEST_PATH = Path(__file__).parents[4] / "contracts/providers/fosun/manifest.json"
OFFLINE_GUARD_PATH = Path(__file__).parents[4] / "scripts/offline_guard"


def test_capture_cli_creates_one_protected_frame_without_console_output(
    tmp_path: Path,
) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    frame_name = "fosun-probe-20260831t010203z.frame.json"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "nexus_fosun_gateway.probe_capture",
            "--frame-output-dir",
            str(operator_dir),
            "--frame-basename",
            frame_name,
            *_probe_arguments(MANIFEST_PATH),
        ],
        check=False,
        capture_output=True,
        env=_offline_subprocess_environment(),
        timeout=10,
    )

    frame_path = operator_dir / frame_name
    assert completed.returncode == 2
    assert completed.stdout == b""
    assert completed.stderr == b""
    assert stat.S_IMODE(frame_path.stat().st_mode) == 0o600
    assert frame_path.read_bytes().count(b"\n") == 1
    assert json.loads(frame_path.read_bytes())["code"] == "PROBE_SETTINGS_INVALID"


def test_capture_is_create_only_and_never_invokes_probe_for_existing_name(
    tmp_path: Path,
) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    frame_name = "fosun-probe-20260831t010203z.frame.json"
    existing = operator_dir / frame_name
    existing.write_bytes(b"preserve")
    existing.chmod(0o600)
    invoked = False

    def forbidden_runner(_fd: int, _arguments: tuple[str, ...]) -> int:
        nonlocal invoked
        invoked = True
        raise AssertionError

    with pytest.raises(ProbeSdkError) as exc_info:
        probe_capture.capture_probe_result_frame(
            operator_dir,
            frame_name,
            tuple(_probe_arguments(MANIFEST_PATH)),
            runner=forbidden_runner,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_EXISTS"
    assert existing.read_bytes() == b"preserve"
    assert invoked is False


def test_capture_rejects_symlink_final_and_unsafe_frame_name(tmp_path: Path) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.write_bytes(b"preserve")
    frame_name = "fosun-probe-20260831t010203z.frame.json"
    (operator_dir / frame_name).symlink_to(target)

    for candidate in (frame_name, "FOSUN-PROBE.frame.json", "../probe.frame.json"):
        with pytest.raises(ProbeSdkError):
            probe_capture.capture_probe_result_frame(
                operator_dir,
                candidate,
                tuple(_probe_arguments(MANIFEST_PATH)),
                runner=lambda _fd, _arguments: 0,
            )

    assert target.read_bytes() == b"preserve"


def test_capture_preserves_partial_frame_and_returns_failure_for_quarantine(
    tmp_path: Path,
) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    frame_name = "fosun-probe-20260831t010203z.frame.json"

    def partial_runner(file_descriptor: int, _arguments: tuple[str, ...]) -> int:
        os.write(file_descriptor, b'{"status":"probe_published"')
        return 0

    result = probe_capture.capture_probe_result_frame(
        operator_dir,
        frame_name,
        tuple(_probe_arguments(MANIFEST_PATH)),
        runner=partial_runner,
    )

    assert result == 2
    assert (operator_dir / frame_name).read_bytes() == b'{"status":"probe_published"'


def test_capture_interrupt_after_complete_success_frame_still_returns_130(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    frame_name = "fosun-probe-20260831t010203z.frame.json"

    def write_then_interrupt(
        file_descriptor: int,
        _arguments: tuple[str, ...],
    ) -> int:
        frame = {
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
        os.write(
            file_descriptor,
            json.dumps(frame, separators=(",", ":")).encode() + b"\n",
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(probe_capture, "_run_probe_cli", write_then_interrupt)

    exit_code = probe_capture.main(
        [
            "--frame-output-dir",
            str(operator_dir),
            "--frame-basename",
            frame_name,
            *_probe_arguments(MANIFEST_PATH),
        ]
    )

    captured = capfd.readouterr()
    assert exit_code == 130
    assert captured.out == ""
    assert captured.err == ""
    assert json.loads((operator_dir / frame_name).read_bytes())["status"] == (
        "probe_published"
    )


def test_capture_requires_the_exact_eight_probe_arguments(tmp_path: Path) -> None:
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir(mode=0o700)
    arguments = _probe_arguments(MANIFEST_PATH)
    arguments[0] = "--api-key"

    with pytest.raises(ProbeSdkError) as exc_info:
        probe_capture.capture_probe_result_frame(
            operator_dir,
            "fosun-probe-20260831t010203z.frame.json",
            tuple(arguments),
            runner=lambda _fd, _arguments: 0,
        )

    assert exc_info.value.code == "INVALID_PROBE_PLAN"
    assert list(operator_dir.iterdir()) == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("failure_operation", "not-an-operation"),
        ("failure_category", "not-a-category"),
        ("physical_request_count", True),
        ("physical_request_count", 25),
        ("artifact_sha256", "sensitive-artifact"),
    ],
)
def test_capture_rejects_invalid_or_nonempty_failure_diagnostics(
    field: str, value: object
) -> None:
    frame = probe_cli.failure_result("PROBE_FAILED")
    frame[field] = value
    encoded = json.dumps(frame, separators=(",", ":")).encode() + b"\n"

    assert probe_capture._valid_frame_for_exit(encoded, 2) is False


def test_runbook_uses_lowercase_plan_names_and_secure_capture_command() -> None:
    runbook = (
        Path(__file__).parents[4] / "docs/runbooks/fosun-read-only-activation.md"
    ).read_text()

    assert "probe_run_id='fosun-probe-yyyymmddthhmmssz'" in runbook
    assert 'probe_frame_basename="${probe_run_id}.frame.json"' in runbook
    assert "nexus-fosun-capture-read-only-probe" in runbook
    assert "```bash\nset -eu\nprobe_run_id=" in runbook
    assert 'mkdir -m 700 -- "$probe_output_dir" "$probe_operator_dir"' in runbook
    assert "install -d -m 700 \"$probe_output_dir\"" not in runbook
    assert "uv run --package nexus-fosun-gateway nexus-fosun-capture" not in runbook
    assert '>"$probe_frame"' not in runbook
    assert "nexus-fosun-publish-probe-receipt" in runbook
    assert 'if [ "$probe_exit" -ne 0 ]; then' in runbook
    assert '--capture-exit "$probe_exit"' in runbook
    assert 'if [ "$receipt_exit" -ne 0 ]; then' in runbook
    assert "receipt publication uncertain; quarantine this run and stop" in runbook
    assert '"$probe_activation_bin"' not in runbook
    assert "continuous runtime deliberately" in runbook
    assert "<<'PY'" not in runbook
    future_procedure = runbook.split("## Future one-shot probe procedure", 1)[1].split(
        "## Activation and rollback",
        1,
    )[0]
    assert "uv run" not in future_procedure


def test_publisher_key_preflight_is_no_network_and_value_free(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    private_key_path = tmp_path / "publisher.seed"
    private_key_path.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    private_key_path.chmod(0o600)
    public_pin = _b64u(
        private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from nexus_fosun_gateway.probe_capture import key_preflight_run; "
            "key_preflight_run()",
        ],
        check=False,
        capture_output=True,
        env={
            **_offline_subprocess_environment(),
            "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE": str(private_key_path),
            "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U": public_pin,
        },
        timeout=10,
    )

    assert completed.returncode == 0
    assert completed.stdout == b"publisher_key_preflight=pass\n"
    assert completed.stderr == b""
    assert private_key_path.read_bytes() not in completed.stdout
    assert public_pin.encode() not in completed.stdout


def _b64u(value: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _offline_subprocess_environment() -> dict[str, str]:
    return {
        **os.environ,
        "NEXUS_OFFLINE_SOCKET_GUARD": "1",
        "PYTHONPATH": str(OFFLINE_GUARD_PATH),
    }


def _probe_arguments(manifest_path: Path) -> list[str]:
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
        "fosun-probe-20260831t010203z.json",
    ]
