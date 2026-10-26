#!/usr/bin/env python3
"""Manage one transient host-side Agent paper loop without copying credentials."""

from __future__ import annotations

import argparse
import errno
import fcntl
import importlib.util
import json
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

LABEL = "com.nexus.agent-paper-loop"
_RELEASE_REVISION = re.compile(r"[0-9a-f]{40}")


def _recovery_module():
    source = Path(__file__).with_name("agent-paper-recovery.py")
    spec = importlib.util.spec_from_file_location("nexus_recovery_control", source)
    if spec is None or spec.loader is None:
        raise RuntimeError("AGENT_PAPER_RECOVERY_RUNTIME_MISSING")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _has_recovery(root: Path) -> bool:
    path = root / ".secrets/nexus-agent-paper/recovery.json"
    return path.exists() or path.is_symlink()


@contextmanager
def _service_control_lock() -> Iterator[None]:
    """Serialize owner inspection and launchd mutation across all local checkouts."""
    directory = Path.home() / "Library" / "Application Support" / "NEXUS" / "runtime"
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = directory.stat()
        if (
            directory.is_symlink()
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise OSError
        descriptor = os.open(
            directory / "agent-paper-service-control.lock",
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
        )
    except OSError:
        raise RuntimeError("AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE") from None
    locked = False
    try:
        lock_info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_uid != os.getuid()
            or stat.S_IMODE(lock_info.st_mode) != 0o600
            or lock_info.st_nlink != 1
        ):
            raise RuntimeError("AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise RuntimeError("AGENT_PAPER_SERVICE_CONTROL_BUSY") from None
            raise RuntimeError("AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE") from None
        locked = True
        yield
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    }
    return {name: value for name, value in os.environ.items() if name in allowed}


def _runtime_executable() -> str:
    return str(Path(sys.executable).resolve())


def _service_working_directory() -> str:
    return str(Path(_runtime_executable()).parent)


def _checkout_revision(root: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    revision = completed.stdout.strip()
    return revision if completed.returncode == 0 and _RELEASE_REVISION.fullmatch(revision) else None


def _engine_environment(root: Path) -> dict[str, str]:
    environment = {
        **_runtime_environment(),
        "__PYVENV_LAUNCHER__": str(root / ".venv/bin/python3"),
    }
    release_revision = _checkout_revision(root)
    if release_revision is not None:
        environment["NEXUS_RELEASE_REVISION"] = release_revision
    return environment


def _program_arguments(root: Path, env_file: Path, project: str) -> list[str]:
    return [
        _runtime_executable(),
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
    return directory


def _private_log_directory() -> Path:
    directory = Path.home() / "Library/Logs/nexus-agent-paper"
    if directory.is_symlink():
        raise ValueError("AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE")
    directory.mkdir(exist_ok=True, mode=0o700)
    info = directory.stat()
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


def _write_service_plist(
    root: Path,
    private_directory: Path,
    log_directory: Path,
    env_file: Path,
    project: str,
) -> Path:
    path = private_directory / "service.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": _program_arguments(root, env_file, project),
        "WorkingDirectory": _service_working_directory(),
        "StandardOutPath": str(log_directory / "service.stdout.log"),
        "StandardErrorPath": str(log_directory / "service.stderr.log"),
        "RunAtLoad": False,
        "KeepAlive": False,
    }
    release_revision = _checkout_revision(root)
    if release_revision is not None:
        payload["EnvironmentVariables"] = {"NEXUS_RELEASE_REVISION": release_revision}
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
    if state == "running":
        _emit({"label": LABEL, "status": state})
        return 0
    if state == "inactive" and last_exit_code not in {None, 0}:
        _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
        return 1
    if state not in {"stopped", "inactive"}:
        _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
        return 1
    if not engine.is_file() or not os.access(engine, os.X_OK):
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    try:
        private_directory = _private_directory(root)
        log_directory = _private_log_directory()
    except (OSError, ValueError):
        _emit({"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"})
        return 1
    environment = _engine_environment(root)
    preflight = subprocess.run(
        [
            _runtime_executable(),
            str(engine),
            "--service-startup-preflight",
            *_engine_arguments(root, env_file),
        ],
        cwd=root,
        env=environment,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if preflight.returncode != 0:
        _emit({"error_code": "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED"})
        return 1
    domain = f"gui/{os.getuid()}"
    if state == "inactive":
        started = subprocess.run(
            [launchctl, "kickstart", f"{domain}/{LABEL}"],
            cwd=root,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            state, last_exit_code = _launchctl_inspection(launchctl, root, env_file, project)
        except RuntimeError as error:
            code = (
                str(error)
                if str(error) == "AGENT_PAPER_SERVICE_OWNER_MISMATCH"
                else "AGENT_PAPER_SERVICE_START_FAILED"
            )
            _emit({"error_code": code})
            return 1
        if started.returncode != 0 or state != "running" or last_exit_code == 75:
            _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
            return 1
        _emit({"label": LABEL, "status": "started"})
        return 0
    try:
        plist_path = _write_service_plist(
            root,
            private_directory,
            log_directory,
            env_file,
            project,
        )
    except (OSError, ValueError):
        _emit({"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"})
        return 1
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
    for attempt in range(2):
        try:
            state, last_exit_code = _launchctl_inspection(launchctl, root, env_file, project)
        except RuntimeError as error:
            code = (
                str(error)
                if str(error) == "AGENT_PAPER_SERVICE_OWNER_MISMATCH"
                else "AGENT_PAPER_SERVICE_START_FAILED"
            )
            _emit({"error_code": code})
            return 1
        if last_exit_code == 75:
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
            return 1
        if started.returncode != 0 or last_exit_code == 78:
            # Only fresh exact-owner inspection with positive inactivity permits cleanup.
            # Never erase an unknown/live registration or its last-exit evidence.
            if state == "inactive":
                removed = subprocess.run(
                    [launchctl, "bootout", f"{domain}/{LABEL}"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if removed.returncode == 0:
                    plist_path.unlink(missing_ok=True)
            code = (
                "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED"
                if state == "inactive" and last_exit_code == 78
                else "AGENT_PAPER_SERVICE_START_FAILED"
            )
            _emit({"error_code": code})
            return 1
        if attempt == 0:
            time.sleep(0.25)
    if state != "running":
        _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
        return 1
    _emit({"label": LABEL, "status": "started"})
    return 0


def _run(root: Path, env_file: Path) -> int:
    engine = root / ".venv" / "bin" / "nexus-engine-agent-paper-loop"
    if not engine.is_file() or not os.access(engine, os.X_OK):
        _emit({"error_code": "AGENT_PAPER_SERVICE_RUNTIME_MISSING"})
        return 1
    environment = _engine_environment(root)
    preflight = subprocess.run(
        [
            _runtime_executable(),
            str(engine),
            "--service-startup-preflight",
            *_engine_arguments(root, env_file),
        ],
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
    runtime = _runtime_executable()
    recovery_arguments = (
        _recovery_module().activation_arguments(sys.modules[__name__], root, env_file)
        if _has_recovery(root)
        else []
    )
    os.execve(
        runtime,
        [runtime, str(engine), *_engine_arguments(root, env_file), *recovery_arguments],
        environment,
    )
    return 1


def _launchctl_inspection(
    launchctl: str,
    root: Path,
    env_file: Path,
    project: str,
    *,
    allow_legacy: bool = False,
    label: str = LABEL,
    expected_arguments: list[str] | None = None,
) -> tuple[str, int | None]:
    inspected = subprocess.run(
        [launchctl, "print", f"gui/{os.getuid()}/{label}"],
        check=False,
        capture_output=True,
    )
    if inspected.returncode == 113:
        return "stopped", None
    if inspected.returncode != 0:
        raise RuntimeError("AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE")
    try:
        output = inspected.stdout.decode("utf-8")
    except UnicodeDecodeError:
        raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH") from None
    arguments = list(re.finditer(r"^(\t*)arguments = \{\n", output, re.MULTILINE))
    working_directories = re.findall(r"^\t*working directory = ([^\n]*)$", output, re.MULTILINE)
    if len(arguments) != 1 or len(working_directories) != 1 or "\r" in output:
        raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH")
    indent = arguments[0].group(1)
    argument_prefix = indent + "\t"
    registered = []
    for line in output[arguments[0].end() :].split("\n"):
        if line == indent + "}":
            break
        if not line.startswith(argument_prefix):
            raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH")
        registered.append(line[len(argument_prefix) :])
    else:
        raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH")
    expected = expected_arguments or _program_arguments(root, env_file, project)
    current = working_directories[0] == _service_working_directory() and registered == expected
    legacy_arguments = len(registered) == 3 and registered[1:] == expected[1:3]
    legacy = (
        allow_legacy
        and working_directories[0] == str(root)
        and (registered == expected or legacy_arguments)
    )
    if not current and not legacy:
        raise RuntimeError("AGENT_PAPER_SERVICE_OWNER_MISMATCH")
    match = re.search(r"^\s*last exit code = (\d+)(?::|$)", output, re.MULTILINE)
    last_exit_code = int(match.group(1)) if match is not None else None
    state_match = re.search(r"^\s*state = ([^\n]+)$", output, re.MULTILINE)
    raw_state = state_match.group(1).strip() if state_match else None
    has_pid = re.search(r"^\s*pid = [1-9][0-9]*\s*$", output, re.MULTILINE)
    if raw_state == "running" or has_pid:
        state = "running"
    elif raw_state in {"inactive", "not running", "exited"}:
        state = "inactive"
    else:
        raise RuntimeError("AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE")
    return state, last_exit_code


def _status(root: Path, env_file: Path, project: str) -> int:
    result: dict[str, object] = {}
    if _has_recovery(root):
        try:
            result["recovery"] = _recovery_module().summary(root)
        except (OSError, ValueError, KeyError, TypeError):
            result["recovery"] = {"desired": "unknown", "reason": "STATE_UNAVAILABLE"}
            result["error_code"] = "AGENT_PAPER_RECOVERY_STATE_INVALID"
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        result["error_code"] = "AGENT_PAPER_SERVICE_RUNTIME_MISSING"
    else:
        try:
            status, last_exit_code = _launchctl_inspection(
                launchctl, root, env_file, project, allow_legacy=True
            )
            result.update(label=LABEL, status=status)
            if last_exit_code == 75:
                result["error_code"] = "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"
        except RuntimeError as error:
            result["error_code"] = str(error)
    _emit(result)
    return 1 if "error_code" in result else 0


def _stop(root: Path, env_file: Path, project: str, *, acknowledge: bool = False) -> int:
    if _has_recovery(root):
        _recovery_module().disable(root)
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
    if acknowledge and (state != "inactive" or last_exit_code == 0):
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
            if state != "running" and not (state == "inactive" and last_exit_code is None):
                break
            time.sleep(0.2)
        if state == "running" or last_exit_code == 75:
            _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
            return 1
    if not acknowledge and state != "stopped" and (state != "inactive" or last_exit_code != 0):
        _emit({"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"})
        return 1
    if not acknowledge and _has_recovery(root):
        _recovery_module().record_stop(
            root, observed_clean_exit=state == "inactive" and last_exit_code == 0
        )
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
    parser.add_argument(
        "action",
        choices=(
            "start",
            "status",
            "stop",
            "run",
            "enable-recovery",
            "disable-recovery",
            "maintenance",
            "recover",
        ),
    )
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
    if args.action in {"enable-recovery", "disable-recovery", "maintenance", "recover"}:
        try:
            with _service_control_lock():
                recovery = _recovery_module()
                if args.action == "enable-recovery":
                    return recovery.enable(sys.modules[__name__], root, env_file, args.project_name)
                if args.action == "recover":
                    return recovery.recover(
                        sys.modules[__name__], root, env_file, args.project_name
                    )
                if args.action == "maintenance":
                    recovery.maintenance(sys.modules[__name__], root, env_file, args.project_name)
                else:
                    recovery.disable(root)
                _emit({"status": "disabled", "label": recovery.LABEL})
                return 0
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            code = str(error)
            if re.fullmatch(r"AGENT_PAPER_[A-Z_]+", code) is None:
                code = "AGENT_PAPER_RECOVERY_INSPECTION_FAILED"
            _emit({"error_code": code})
            return 1
    if args.action == "start":
        try:
            with _service_control_lock():
                if _has_recovery(root):
                    before, _ = _launchctl_inspection(
                        "launchctl", root, env_file, args.project_name
                    )
                    _recovery_module().enable(
                        sys.modules[__name__], root, env_file, args.project_name, report=False
                    )
                    after, last_exit = _launchctl_inspection(
                        "launchctl", root, env_file, args.project_name
                    )
                    if after != "running" or last_exit == 75:
                        _emit({"error_code": "AGENT_PAPER_SERVICE_START_FAILED"})
                        return 1
                    _emit(
                        {"label": LABEL, "status": "running" if before == "running" else "started"}
                    )
                    return 0
                return _start(root, env_file, args.project_name)
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            code = str(error)
            _emit(
                {
                    "error_code": code
                    if re.fullmatch(r"AGENT_PAPER_[A-Z_]+", code)
                    else "AGENT_PAPER_RECOVERY_INSPECTION_FAILED"
                }
            )
            return 1
    if args.action == "run":
        try:
            return _run(root, env_file)
        except (OSError, ValueError, RuntimeError):
            _emit({"error_code": "AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED"})
            return 1
    if args.action == "status":
        return _status(root, env_file, args.project_name)
    try:
        with _service_control_lock():
            return _stop(
                root,
                env_file,
                args.project_name,
                acknowledge=args.acknowledge_unconfirmed,
            )
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        code = str(error)
        _emit(
            {
                "error_code": code
                if re.fullmatch(r"AGENT_PAPER_[A-Z_]+", code)
                else "AGENT_PAPER_RECOVERY_STATE_INVALID"
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
