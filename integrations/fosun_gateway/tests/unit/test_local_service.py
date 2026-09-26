"""The host service owns one exact Gateway without automatic renewal."""

import json
import plistlib
import subprocess
import sys
from pathlib import Path

import pytest
from nexus_fosun_gateway.local_service import (
    _service_control_lock,
    build_service_plist,
    inspect_launchctl_output,
    local_health_ready,
    main,
)


def test_plist_has_fixed_owner_no_secrets_and_no_restart(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    revision = "a" * 40

    value = build_service_plist(root, runtime, revision)

    assert value["Label"] == "com.nexus.fosun-local-gateway"
    assert value["ProgramArguments"] == [
        str(Path(sys.executable).resolve()),
        str(runtime),
        "run",
        "--repo-root",
        str(root),
    ]
    assert value["WorkingDirectory"] == str(Path(sys.executable).resolve().parent)
    assert value["KeepAlive"] is False
    assert value["RunAtLoad"] is False
    assert value["EnvironmentVariables"] == {
        "NEXUS_RELEASE_REVISION": revision,
        "__PYVENV_LAUNCHER__": str(root / ".venv/bin/python3"),
    }
    assert str(root) not in str(value["StandardOutPath"])
    assert str(root) not in str(value["StandardErrorPath"])
    assert "SECRET" not in str(value)


def test_owner_inspection_rejects_another_checkout(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    output = (
        "state = running\n"
        "pid = 123\n"
        f"working directory = {root}\n"
        "arguments = {\n"
        f"\t{runtime}\n\trun\n\t--repo-root\n\t{tmp_path / 'other'}\n"
        "}\n"
    )

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH"):
        inspect_launchctl_output(output, root, runtime)


def test_owner_inspection_reports_running_only_for_exact_registration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    output = (
        "state = running\n"
        "pid = 123\n"
        f"working directory = {root}\n"
        "arguments = {\n"
        f"\t{runtime}\n\trun\n\t--repo-root\n\t{root}\n"
        "}\n"
    )

    assert inspect_launchctl_output(output, root, runtime) == ("running", 123, None)


def test_owner_inspection_ignores_nested_launchctl_state(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    output = (
        "\tstate = running\n"
        f"\tworking directory = {root}\n"
        "\targuments = {\n"
        f"\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
        "\t}\n"
        "\tpid = 123\n"
        "\t\tstate = active\n"
    )

    assert inspect_launchctl_output(output, root, runtime) == ("running", 123, None)


def test_owner_inspection_rejects_release_drift(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    output = (
        "\tstate = running\n"
        f"\tworking directory = {root}\n"
        "\targuments = {\n"
        f"\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
        "\t}\n"
        "\tNEXUS_RELEASE_REVISION => " + "b" * 40 + "\n"
        "\tpid = 123\n"
    )

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_SERVICE_RELEASE_MISMATCH"):
        inspect_launchctl_output(output, root, runtime, expected_revision="a" * 40)


def test_health_requires_both_mtls_listener_roles(tmp_path: Path) -> None:
    calls: list[tuple[int, str]] = []

    def get(port: int, role: str) -> int:
        calls.append((port, role))
        return 200 if role == "broker" else 503

    assert local_health_ready(tmp_path, get=get) is False
    assert calls == [(8081, "broker"), (8082, "market")]


def test_health_rejects_marker_owned_by_another_process(tmp_path: Path) -> None:
    local = tmp_path / ".secrets/nexus-fosun-local"
    local.mkdir(parents=True)
    marker = local / "replay.sqlite3.ready"
    marker.write_text("v1 999 9999999999\n")
    marker.chmod(0o600)

    assert local_health_ready(tmp_path, expected_pid=123, get=lambda *_: 200) is False


def test_status_with_no_registration_is_stopped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        assert argv[1] == "print"
        return subprocess.CompletedProcess(argv, 113, b"", b"")

    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.shutil.which", lambda _: "/fake/launchctl"
    )
    monkeypatch.setattr("nexus_fosun_gateway.local_service.subprocess.run", run)

    assert main(["status", "--repo-root", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "stopped"}


def test_running_registration_without_both_health_checks_is_degraded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    revision = "a" * 40
    output = (
        "\tstate = running\n\tpid = 123\n"
        f"\tworking directory = {root}\n"
        "\targuments = {\n"
        f"\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
        "\t}\n"
        f"\t\tNEXUS_RELEASE_REVISION => {revision}\n"
    )
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.shutil.which", lambda _: "/fake/launchctl"
    )
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, output.encode(), b""),
    )
    monkeypatch.setattr("nexus_fosun_gateway.local_service._checkout_revision", lambda _: "a" * 40)
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.local_health_ready", lambda *_args, **_kwargs: False
    )

    assert main(["status", "--repo-root", str(root)]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "degraded"}


def test_explicit_start_registers_nonrenewing_private_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service._service_log_directory", lambda: tmp_path / "logs"
    )
    root = tmp_path
    local = root / ".secrets/nexus-fosun-local"
    local.mkdir(parents=True, mode=0o700)
    (root / ".secrets").chmod(0o700)
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    runtime.parent.mkdir(parents=True)
    runtime.write_text("#!/bin/sh\nexit 0\n")
    runtime.chmod(0o755)
    revision = "a" * 40
    registered: dict[str, object] | None = None
    commands: list[str] = []
    post_bootstrap_checks = 0

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal registered, post_bootstrap_checks
        commands.append(argv[1])
        if argv[0] == str(runtime):
            return subprocess.CompletedProcess(argv, 0, b'{"status":"ready"}', b"")
        if argv[1] == "print":
            if registered is None:
                return subprocess.CompletedProcess(argv, 113, b"", b"")
            post_bootstrap_checks += 1
            state = "inactive" if post_bootstrap_checks == 1 else "running"
            output = (
                f"\tstate = {state}\n"
                + ("\tpid = 123\n" if state == "running" else "")
                + f"\tworking directory = {root}\n"
                + "\targuments = {\n"
                + f"\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
                + "\t}\n"
                + f"\t\tNEXUS_RELEASE_REVISION => {revision}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output.encode(), b"")
        if argv[1] == "bootstrap":
            with Path(argv[3]).open("rb") as stream:
                registered = plistlib.load(stream)
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.shutil.which", lambda _: "/fake/launchctl"
    )
    monkeypatch.setattr("nexus_fosun_gateway.local_service.subprocess.run", run)
    monkeypatch.setattr("nexus_fosun_gateway.local_service._checkout_revision", lambda _: revision)
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.local_health_ready", lambda *_args, **_kwargs: True
    )

    assert main(["start", "--repo-root", str(root)]) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "started"}
    assert commands.count("bootstrap") == 1
    assert commands.count("kickstart") == 1
    assert registered is not None and registered["KeepAlive"] is False
    assert (local / "service.plist").stat().st_mode & 0o777 == 0o600


def test_stop_signals_only_exact_owner_then_boots_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service._service_log_directory", lambda: tmp_path / "logs"
    )
    root = tmp_path
    local = root / ".secrets/nexus-fosun-local"
    local.mkdir(parents=True, mode=0o700)
    (root / ".secrets").chmod(0o700)
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    revision = "b" * 40
    state = "running"
    commands: list[str] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal state
        commands.append(argv[1])
        if argv[1] == "print":
            if state == "stopped":
                return subprocess.CompletedProcess(argv, 113, b"", b"")
            output = (
                f"\tstate = {state}\n"
                + ("\tpid = 123\n" if state == "running" else "\tlast exit code = 0\n")
                + f"\tworking directory = {root}\n"
                + "\targuments = {\n"
                + f"\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
                + "\t}\n"
                + f"\t\tNEXUS_RELEASE_REVISION => {revision}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output.encode(), b"")
        if argv[1] == "kill":
            assert argv[2] == "SIGINT"
            state = "inactive"
        if argv[1] == "bootout":
            state = "stopped"
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.shutil.which", lambda _: "/fake/launchctl"
    )
    monkeypatch.setattr("nexus_fosun_gateway.local_service.subprocess.run", run)
    monkeypatch.setattr("nexus_fosun_gateway.local_service._checkout_revision", lambda _: "a" * 40)

    assert main(["stop", "--repo-root", str(root)]) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "stopped"}
    assert commands.count("kill") == 1
    assert commands.count("bootout") == 1


def test_control_lock_rejects_symlink(tmp_path: Path) -> None:
    local = tmp_path / ".secrets/nexus-fosun-local"
    local.mkdir(parents=True, mode=0o700)
    (tmp_path / ".secrets").chmod(0o700)
    (local / "service-control.lock").symlink_to(tmp_path / "elsewhere")

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE"):
        with _service_control_lock(tmp_path):
            pass


def test_control_lock_rejects_symlinked_secrets_parent(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    (outside / "nexus-fosun-local").mkdir(parents=True, mode=0o700)
    outside.chmod(0o700)
    (tmp_path / ".secrets").symlink_to(outside)

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE"):
        with _service_control_lock(tmp_path):
            pass


def test_start_checks_control_lock_before_launchctl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    local = tmp_path / ".secrets/nexus-fosun-local"
    local.mkdir(parents=True, mode=0o700)
    (tmp_path / ".secrets").chmod(0o700)
    (local / "service-control.lock").symlink_to(tmp_path / "elsewhere")
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.shutil.which", lambda _: "/fake/launchctl"
    )
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_service.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("launchctl ran before owner lock"),
    )

    assert main(["start", "--repo-root", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE"
    }


@pytest.mark.parametrize("launcher_matches", [True, False])
def test_external_python_registration_requires_exact_venv_binding(
    tmp_path: Path,
    launcher_matches: bool,
) -> None:
    root = tmp_path / "repo"
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    executable = Path(sys.executable).resolve()
    launcher = root / ".venv/bin/python3" if launcher_matches else tmp_path / "other/python3"
    output = (
        "\tstate = running\n\tpid = 123\n"
        f"\tworking directory = {executable.parent}\n"
        "\targuments = {\n"
        f"\t\t{executable}\n\t\t{runtime}\n\t\trun\n\t\t--repo-root\n\t\t{root}\n"
        "\t}\n"
        f"\t\t__PYVENV_LAUNCHER__ => {launcher}\n"
    )
    if launcher_matches:
        assert inspect_launchctl_output(output, root, runtime) == ("running", 123, None)
    else:
        with pytest.raises(ValueError, match="LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH"):
            inspect_launchctl_output(output, root, runtime)
