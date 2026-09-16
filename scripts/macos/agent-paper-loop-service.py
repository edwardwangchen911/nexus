#!/usr/bin/env python3
"""Manage one transient host-side Agent paper loop without copying credentials."""

from __future__ import annotations

import argparse
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


def _validate_config(root: Path, env_file: Path, project: str) -> tuple[Path, Path]:
    root = root.resolve()
    if not root.is_dir() or not (root / "infra/compose.yaml").is_file():
        raise ValueError("REPOSITORY_ROOT_INVALID")
    if re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project) is None:
        raise ValueError("PROJECT_NAME_INVALID")
    if not env_file.is_absolute():
        env_file = root / env_file
    if env_file.is_symlink() or not env_file.is_file():
        raise ValueError("LOCAL_ENV_UNSAFE" if env_file.is_symlink() else "LOCAL_ENV_REQUIRED")
    resolved = env_file.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        raise ValueError("LOCAL_ENV_UNSAFE") from None
    tracked = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--error-unmatch", "--", str(relative)],
        check=False,
        capture_output=True,
    )
    ignored = subprocess.run(
        ["git", "-C", str(root), "check-ignore", "-q", "--", str(relative)],
        check=False,
        capture_output=True,
    )
    if tracked.returncode == 0 or ignored.returncode != 0:
        raise ValueError("LOCAL_ENV_UNSAFE")
    return root, resolved


def _runtime_environment() -> dict[str, str]:
    allowed = {
        "HOME",
        "PATH",
        "TMPDIR",
        "LANG",
        "LANGUAGE",
        "LC_ALL",
        "LC_CTYPE",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "NODE_EXTRA_CA_CERTS",
    }
    return {name: value for name, value in os.environ.items() if name in allowed}


def _program_arguments(root: Path, env_file: Path, project: str) -> list[str]:
    return [
        sys.executable,
        str(root / "scripts/macos/agent-paper-loop-service.py"),
        "run",
        "--repo-root",
        str(root),
        "--env-file",
        str(env_file),
        "--project-name",
        project,
    ]


def _engine_arguments(root: Path, env_file: Path) -> list[str]:
    return ["--repo-root", str(root), "--env-file", str(env_file)]


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


def _write_service_plist(root: Path, private_directory: Path, env_file: Path, project: str) -> Path:
    path = private_directory / "service.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": _program_arguments(root, env_file, project),
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


def _start(root: Path, env_file: Path, project: str) -> int:
    engine = root / ".venv" / "bin" / "nexus-engine-agent-paper-loop"
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        state, last_exit_code = _launchctl_inspection(launchctl, root, env_file, project)
    except RuntimeError as error:
        _emit({"error_code": str(error)})
        return 1
    if last_exit_code == 75:
        _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
        return 1
    if state != "stopped":
        _emit({"label": LABEL, "status": state})
        return 0
    if not engine.is_file() or not os.access(engine, os.X_OK):
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        private_directory = _private_directory(root)
    except (OSError, ValueError):
        _emit({"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"})
        return 1
    environment = _runtime_environment()
    preflight = subprocess.run(
        [str(engine), "--service-preflight", *_engine_arguments(root, env_file)],
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
        plist_path = _write_service_plist(root, private_directory, env_file, project)
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
    last_exit_code = None
    inspection_failed = False
    for attempt in range(2):
        try:
            _, last_exit_code = _launchctl_inspection(launchctl, root, env_file, project)
        except RuntimeError as error:
            if str(error) == "AGENT_PAPER_SERVICE_OWNER_MISMATCH":
                _emit({"error_code": str(error)})
                return 1
            inspection_failed = True
            break
        if last_exit_code == 75:
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
            return 1
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


def _run(root: Path, env_file: Path) -> int:
    engine = root / ".venv" / "bin" / "nexus-engine-agent-paper-loop"
    if not engine.is_file() or not os.access(engine, os.X_OK):
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    environment = _runtime_environment()
    preflight = subprocess.run(
        [str(engine), "--service-preflight", *_engine_arguments(root, env_file)],
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
    os.execve(str(engine), [str(engine), *_engine_arguments(root, env_file)], environment)
    return 1


def _launchctl_inspection(
    launchctl: str, root: Path, env_file: Path, project: str, *, allow_legacy: bool = False
) -> tuple[str, int | None]:
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
    arguments = re.search(
        r"^\s*arguments = \{\n(.*?)^\s*\}", inspected.stdout, re.MULTILINE | re.DOTALL
    )
    working_directory = re.search(r"^\s*working directory = (.+)$", inspected.stdout, re.MULTILINE)
    registered = [line.strip() for line in arguments.group(1).splitlines()] if arguments else []
    expected = _program_arguments(root, env_file, project)
    legacy = allow_legacy and len(registered) == 3 and registered[1:] == expected[1:3]
    if (
        not working_directory
        or working_directory.group(1).strip() != str(root)
        or (registered != expected and not legacy)
    ):
        raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH")
    match = re.search(r"^\s*last exit code = (\d+)(?::|$)", inspected.stdout, re.MULTILINE)
    last_exit_code = int(match.group(1)) if match is not None else None
    state_match = re.search(r"^\s*state = ([^\n]+)$", inspected.stdout, re.MULTILINE)
    raw_state = state_match.group(1).strip() if state_match else None
    has_pid = re.search(r"^\s*pid = [1-9][0-9]*\s*$", inspected.stdout, re.MULTILINE)
    if raw_state == "running" or has_pid:
        state = "running"
    elif raw_state in {"inactive", "not running", "exited"}:
        state = "inactive"
    else:
        raise RuntimeError("AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE")
    return state, last_exit_code


def _status(root: Path, env_file: Path, project: str) -> int:
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        status, last_exit_code = _launchctl_inspection(
            launchctl, root, env_file, project, allow_legacy=True
        )
    except RuntimeError as error:
        _emit({"error_code": str(error)})
        return 1
    if last_exit_code == 75:
        _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
        return 1
    _emit({"label": LABEL, "status": status})
    return 0


def _stop(root: Path, env_file: Path, project: str, *, acknowledge: bool = False) -> int:
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    domain_label = f"gui/{os.getuid()}/{LABEL}"
    try:
        state, last_exit_code = _launchctl_inspection(
            launchctl, root, env_file, project, allow_legacy=not acknowledge
        )
    except RuntimeError as error:
        _emit({"error_code": str(error)})
        return 1
    if acknowledge and (state != "inactive" or last_exit_code != 75):
        _emit({"error_code": "AGENT_PAPER_SERVICE_ACKNOWLEDGEMENT_INVALID"})
        return 1
    if last_exit_code == 75 and not acknowledge:
        _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
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
                state, last_exit_code = _launchctl_inspection(
                    launchctl, root, env_file, project, allow_legacy=True
                )
            except RuntimeError as error:
                code = (
                    str(error)
                    if str(error) == "AGENT_PAPER_SERVICE_OWNER_MISMATCH"
                    else ("AGENT_PAPER_SERVICE_STOP_UNCONFIRMED")
                )
                _emit({"error_code": code})
                return 1
            if state != "running":
                break
            time.sleep(0.2)
        if state == "running" or last_exit_code == 75:
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
    if not acknowledge:
        (root / ".secrets/nexus-agent-paper/service.plist").unlink(missing_ok=True)
    _emit({"label": LABEL, "status": "stopped"})
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "status", "stop", "run"))
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--acknowledge-unconfirmed", action="store_true")
    args = parser.parse_args(argv)
    if args.acknowledge_unconfirmed and args.action != "stop":
        _emit({"error_code": "AGENT_PAPER_SERVICE_ACTION_INVALID"})
        return 2
    try:
        root, env_file = _validate_config(args.repo_root, args.env_file, args.project_name)
    except ValueError as error:
        _emit({"error_code": str(error)})
        return 1
    except OSError:
        _emit({"error_code": "LOCAL_ENV_UNSAFE"})
        return 1
    if args.action == "start":
        return _start(root, env_file, args.project_name)
    if args.action == "run":
        return _run(root, env_file)
    if args.action == "status":
        return _status(root, env_file, args.project_name)
    return _stop(root, env_file, args.project_name, acknowledge=args.acknowledge_unconfirmed)


if __name__ == "__main__":
    raise SystemExit(main())
