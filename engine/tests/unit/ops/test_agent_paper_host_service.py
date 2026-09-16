"""Behavioral tests for the transient macOS Agent paper loop service."""

import json
import os
import plistlib
import shutil
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[4]
SOURCE = ROOT / "scripts" / "macos" / "agent-paper-loop-service.py"


def _write_executable(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _service_root(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    assert SOURCE.is_file(), "host service script is missing"
    root = tmp_path / "repo"
    script = root / "scripts" / "macos" / SOURCE.name
    script.parent.mkdir(parents=True)
    shutil.copy2(SOURCE, script)
    record = tmp_path / "calls.log"
    _write_executable(
        root / ".venv" / "bin" / "nexus-engine-agent-paper-loop",
        "#!/bin/sh\nprintf 'engine:%s\\n' \"$*\" >> \"$NEXUS_TEST_RECORD\"\n"
        "if [ \"$1\" = --service-preflight ]; then\n"
        "  exit \"${NEXUS_TEST_PREFLIGHT_EXIT:-0}\"\n"
        "fi\n"
        "printf 'runtime:%s\\n' \"$AGENT_RUNTIME_MODE\" >> \"$NEXUS_TEST_RECORD\"\n"
        "exit \"${NEXUS_TEST_RUN_EXIT:-0}\"\n",
    )
    fake_bin = tmp_path / "bin"
    state_file = tmp_path / "launch-state"
    state_file.write_text("absent", encoding="utf-8")
    _write_executable(
        fake_bin / "launchctl",
        "#!/bin/sh\nprintf 'launchctl:%s\\n' \"$*\" >> \"$NEXUS_TEST_RECORD\"\n"
        "case \"$1\" in\n"
        "  bootstrap) printf 'inactive' > \"$NEXUS_TEST_LAUNCH_STATE\"; exit 0;;\n"
        "  kickstart)\n"
        "    printf '%s' \"${NEXUS_TEST_KICKSTART_STATE:-running}\" "
        "> \"$NEXUS_TEST_LAUNCH_STATE\"\n"
        "    exit 0;;\n"
        "  print)\n"
        "    state=$(cat \"$NEXUS_TEST_LAUNCH_STATE\")\n"
        "    [ \"$state\" = absent ] && exit 113\n"
        "    [ \"$state\" = error ] && exit 64\n"
        "    if [ \"$state\" = delayed_config_error ]; then\n"
        "      printf 'config_error' > \"$NEXUS_TEST_LAUNCH_STATE\"\n"
        "      printf 'state = running\\n'\n"
        "      exit 0\n"
        "    fi\n"
        "    if [ \"$state\" = config_error ]; then\n"
        "      printf 'state = not running\\nlast exit code = 78: EX_CONFIG\\n'\n"
        "      exit 0\n"
        "    fi\n"
        "    printf 'state = %s\\n' \"$state\"\n"
        "    [ \"$state\" = running ] && printf 'pid = 123\\n'\n"
        "    exit 0;;\n"
        "  kill) printf 'inactive' > \"$NEXUS_TEST_LAUNCH_STATE\"; exit 0;;\n"
        "  bootout) printf 'absent' > \"$NEXUS_TEST_LAUNCH_STATE\"; exit 0;;\n"
        "esac\n"
        "exit 64\n",
    )
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "NEXUS_TEST_RECORD": str(record),
        "NEXUS_TEST_LAUNCH_STATE": str(state_file),
    }
    return root, environment


def test_start_preflights_before_submitting_private_transient_job(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "label": "com.nexus.agent-paper-loop",
        "status": "started",
    }
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls[0] == "engine:--service-preflight"
    plist_path = root / ".secrets" / "nexus-agent-paper" / "service.plist"
    assert calls[1] == f"launchctl:bootstrap gui/{os.getuid()} {plist_path}"
    assert calls[2] == (
        f"launchctl:kickstart gui/{os.getuid()}/com.nexus.agent-paper-loop"
    )
    with plist_path.open("rb") as stream:
        service = plistlib.load(stream)
    assert service["Label"] == "com.nexus.agent-paper-loop"
    assert service["KeepAlive"] is False
    assert service["RunAtLoad"] is False
    assert service["ProgramArguments"][-1] == "run"
    assert "EnvironmentVariables" not in service
    private_directory = root / ".secrets" / "nexus-agent-paper"
    assert stat.S_IMODE(private_directory.stat().st_mode) == 0o700


def test_start_does_not_submit_when_product_preflight_fails(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    environment["NEXUS_TEST_PREFLIGHT_EXIT"] = "7"

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED"
    }
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["engine:--service-preflight"]


def test_start_reports_background_access_blocker_and_cleans_registration(
    tmp_path: Path,
) -> None:
    root, environment = _service_root(tmp_path)
    environment["NEXUS_TEST_KICKSTART_STATE"] = "delayed_config_error"

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED"
    }
    private_plist = root / ".secrets" / "nexus-agent-paper" / "service.plist"
    assert not private_plist.exists()
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls[-2:] == [
        f"launchctl:print gui/{os.getuid()}/com.nexus.agent-paper-loop",
        f"launchctl:bootout gui/{os.getuid()}/com.nexus.agent-paper-loop",
    ]


def test_start_fails_closed_when_launchd_state_cannot_be_confirmed(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    environment["NEXUS_TEST_KICKSTART_STATE"] = "error"

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_START_FAILED"}
    assert not (root / ".secrets" / "nexus-agent-paper" / "service.plist").exists()


def test_start_rejects_symlinked_private_parent_before_preflight(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    outside = tmp_path / "outside-secrets"
    outside.mkdir(mode=0o700)
    (root / ".secrets").symlink_to(outside, target_is_directory=True)

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"
    }
    assert not Path(environment["NEXUS_TEST_RECORD"]).exists()


def test_start_rejects_symlinked_log_before_preflight(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    private_directory = root / ".secrets" / "nexus-agent-paper"
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.parent.chmod(0o700)
    target = tmp_path / "outside.log"
    target.write_text("outside", encoding="utf-8")
    (private_directory / "service.stdout.log").symlink_to(target)

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"
    }
    assert not Path(environment["NEXUS_TEST_RECORD"]).exists()


def test_start_rejects_hardlinked_log_before_preflight(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    private_directory = root / ".secrets" / "nexus-agent-paper"
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.parent.chmod(0o700)
    target = tmp_path / "outside.log"
    target.write_text("outside", encoding="utf-8")
    target.chmod(0o600)
    os.link(target, private_directory / "service.stdout.log")

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"
    }
    assert not Path(environment["NEXUS_TEST_RECORD"]).exists()


def test_start_masks_private_plist_write_failure_before_launchctl(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    private_directory = root / ".secrets" / "nexus-agent-paper"
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.parent.chmod(0o700)
    (private_directory / "service.plist").mkdir()

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "start"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"
    }
    assert result.stderr == ""
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["engine:--service-preflight"]


def test_run_repreflights_then_executes_codex_sdk_loop(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "run"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["engine:--service-preflight", "engine:", "runtime:codex_sdk"]


def test_status_normalizes_launchd_state_without_echoing_details(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    Path(environment["NEXUS_TEST_LAUNCH_STATE"]).write_text("running", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "status"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "label": "com.nexus.agent-paper-loop",
        "status": "running",
    }
    assert "pid" not in result.stdout


def test_status_does_not_misreport_unexpected_launchctl_failure_as_stopped(
    tmp_path: Path,
) -> None:
    root, environment = _service_root(tmp_path)
    Path(environment["NEXUS_TEST_LAUNCH_STATE"]).write_text("error", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "status"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "error_code": "AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE"
    }


def test_stop_interrupts_running_job_before_removing_registration(tmp_path: Path) -> None:
    root, environment = _service_root(tmp_path)
    Path(environment["NEXUS_TEST_LAUNCH_STATE"]).write_text("running", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), "stop"],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "label": "com.nexus.agent-paper-loop",
        "status": "stopped",
    }
    calls = Path(environment["NEXUS_TEST_RECORD"]).read_text(encoding="utf-8").splitlines()
    assert calls == [
        f"launchctl:print gui/{os.getuid()}/com.nexus.agent-paper-loop",
        f"launchctl:kill SIGINT gui/{os.getuid()}/com.nexus.agent-paper-loop",
        f"launchctl:print gui/{os.getuid()}/com.nexus.agent-paper-loop",
        f"launchctl:bootout gui/{os.getuid()}/com.nexus.agent-paper-loop",
    ]
