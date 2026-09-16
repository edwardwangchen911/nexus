#!/usr/bin/env python3
"""Manage one transient host-side Agent paper loop without copying credentials."""

from __future__ import annotations

import json
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path
from tempfile import NamedTemporaryFile

LABEL = "com.nexus.agent-paper-loop"


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True))


def _private_directory(root: Path) -> Path:
    secrets = root / ".secrets"
    directory = secrets / "nexus-agent-paper"
    for path in (secrets, directory):
        if path.is_symlink():
            raise ValueError("AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE")
        path.mkdir(exist_ok=True, mode=0o700)
        info = path.stat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise ValueError("AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE")
    for name in ("service.stdout.log", "service.stderr.log"):
        descriptor = os.open(
            directory / name,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
        )
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
            ):
                raise ValueError("AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE")
        finally:
            os.close(descriptor)
    return directory


def _write_service_plist(root: Path, private_directory: Path) -> Path:
    path = private_directory / "service.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": [sys.executable, str(Path(__file__).resolve()), "run"],
        "WorkingDirectory": str(root),
        "StandardOutPath": str(private_directory / "service.stdout.log"),
        "StandardErrorPath": str(private_directory / "service.stderr.log"),
        "RunAtLoad": False,
        "KeepAlive": False,
    }
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            dir=private_directory, prefix="service.plist.", delete=False
        ) as stream:
            temporary_path = Path(stream.name)
            plistlib.dump(payload, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary_path.chmod(0o600)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return path


def _start(root: Path) -> int:
    engine = root / ".venv" / "bin" / "nexus-engine-agent-paper-loop"
    launchctl = shutil.which("launchctl")
    if not engine.is_file() or not os.access(engine, os.X_OK) or launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        private_directory = _private_directory(root)
    except (OSError, ValueError):
        _emit({"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"})
        return 1
    environment = {**os.environ, "AGENT_RUNTIME_MODE": "codex_sdk"}
    preflight = subprocess.run(
        [str(engine), "--service-preflight"],
        cwd=root,
        env=environment,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if preflight.returncode != 0:
        _emit({"error_code": "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED"})
        return 1
    try:
        plist_path = _write_service_plist(root, private_directory)
    except (OSError, ValueError):
        _emit({"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"})
        return 1
    domain = f"gui/{os.getuid()}"
    bootstrapped = subprocess.run(
        [launchctl, "bootstrap", domain, str(plist_path)],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if bootstrapped.returncode != 0:
        _emit({"error_code": "AGENT_PAPER_SERVICE_BOOTSTRAP_FAILED"})
        return 1
    started = subprocess.run(
        [launchctl, "kickstart", f"{domain}/{LABEL}"],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if started.returncode != 0:
        subprocess.run(
            [launchctl, "bootout", f"{domain}/{LABEL}"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
        return 1
    last_exit_code: int | None = None
    inspection_failed = False
    for attempt in range(2):
        try:
            _, last_exit_code = _launchctl_inspection(launchctl)
        except RuntimeError:
            inspection_failed = True
            break
        if last_exit_code == 78:
            break
        if attempt == 0:
            time.sleep(0.25)
    if inspection_failed or last_exit_code == 78:
        subprocess.run(
            [launchctl, "bootout", f"{domain}/{LABEL}"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        plist_path.unlink(missing_ok=True)
        code = (
            "AGENT_PAPER_SERVICE_START_FAILED"
            if inspection_failed
            else "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED"
        )
        _emit({"error_code": code})
        return 1
    _emit({"label": LABEL, "status": "started"})
    return 0


def _run(root: Path) -> int:
    engine = root / ".venv" / "bin" / "nexus-engine-agent-paper-loop"
    if not engine.is_file() or not os.access(engine, os.X_OK):
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    environment = {**os.environ, "AGENT_RUNTIME_MODE": "codex_sdk"}
    preflight = subprocess.run(
        [str(engine), "--service-preflight"],
        cwd=root,
        env=environment,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if preflight.returncode != 0:
        _emit({"error_code": "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED"})
        return 1
    os.chdir(root)
    os.execve(str(engine), [str(engine)], environment)
    return 1


def _launchctl_inspection(launchctl: str) -> tuple[str, int | None]:
    inspected = subprocess.run(
        [launchctl, "print", f"gui/{os.getuid()}/{LABEL}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspected.returncode == 113:
        return "stopped", None
    if inspected.returncode != 0:
        raise RuntimeError("AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE")
    match = re.search(r"^\s*last exit code = (\d+)(?::|$)", inspected.stdout, re.MULTILINE)
    last_exit_code = int(match.group(1)) if match is not None else None
    state = "running" if "state = running" in inspected.stdout else "inactive"
    return state, last_exit_code


def _launchctl_state(launchctl: str) -> str:
    state, _ = _launchctl_inspection(launchctl)
    return state


def _status() -> int:
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        status = _launchctl_state(launchctl)
    except RuntimeError:
        _emit({"error_code": "AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE"})
        return 1
    _emit({"label": LABEL, "status": status})
    return 0


def _stop() -> int:
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    domain_label = f"gui/{os.getuid()}/{LABEL}"
    try:
        state = _launchctl_state(launchctl)
    except RuntimeError:
        _emit({"error_code": "AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE"})
        return 1
    if state == "running":
        interrupted = subprocess.run(
            [launchctl, "kill", "SIGINT", domain_label],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if interrupted.returncode != 0:
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_FAILED"})
            return 1
        for _ in range(50):
            try:
                state = _launchctl_state(launchctl)
            except RuntimeError:
                _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
                return 1
            if state != "running":
                break
            time.sleep(0.2)
        if state == "running":
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
            return 1
    if state != "stopped":
        removed = subprocess.run(
            [launchctl, "bootout", domain_label],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if removed.returncode != 0:
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_FAILED"})
            return 1
    (Path(__file__).resolve().parents[2] / ".secrets/nexus-agent-paper/service.plist").unlink(
        missing_ok=True
    )
    _emit({"label": LABEL, "status": "stopped"})
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    root = Path(__file__).resolve().parents[2]
    if arguments == ["start"]:
        return _start(root)
    if arguments == ["run"]:
        return _run(root)
    if arguments == ["status"]:
        return _status()
    if arguments == ["stop"]:
        return _stop()
    _emit({"error_code": "AGENT_PAPER_SERVICE_ACTION_INVALID"})
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
