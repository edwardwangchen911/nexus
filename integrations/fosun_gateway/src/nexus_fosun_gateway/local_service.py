"""Host-managed, explicit lifecycle for the local read-only Gateway."""

import argparse
import fcntl
import json
import os
import plistlib
import re
import shutil
import ssl
import stat
import subprocess
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx

from nexus_fosun_gateway.local_runtime import (
    _private_existing_file,
    checkout_release,
    load_local_gateway_settings,
)
from nexus_fosun_gateway.security.hmac import read_readiness_marker

LABEL = "com.nexus.fosun-local-gateway"


@contextmanager
def _service_control_lock(root: Path) -> Iterator[None]:
    directory = root / ".secrets/nexus-fosun-local"
    try:
        for path in (root / ".secrets", directory):
            info = path.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o700
            ):
                raise ValueError("LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE")
        descriptor = os.open(
            directory / "service-control.lock",
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
        )
    except OSError:
        raise ValueError("LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE") from None
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise ValueError("LOCAL_GATEWAY_SERVICE_CONTROL_BUSY") from None
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def build_service_plist(root: Path, runtime: Path, revision: str) -> dict[str, object]:
    if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("LOCAL_GATEWAY_RELEASE_INVALID")
    log_dir = root / ".secrets/nexus-fosun-local"
    return {
        "Label": LABEL,
        "ProgramArguments": [str(runtime), "run", "--repo-root", str(root)],
        "WorkingDirectory": str(root),
        "StandardOutPath": str(log_dir / "service.stdout.log"),
        "StandardErrorPath": str(log_dir / "service.stderr.log"),
        "RunAtLoad": False,
        "KeepAlive": False,
        "EnvironmentVariables": {"NEXUS_RELEASE_REVISION": revision},
    }


def inspect_launchctl_output(
    output: str,
    root: Path,
    runtime: Path,
    *,
    expected_revision: str | None = None,
) -> tuple[str, int | None, int | None]:
    if "\r" in output:
        raise ValueError("LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH")
    working = re.findall(r"^\s*working directory = ([^\n]+)$", output, re.MULTILINE)
    starts = list(re.finditer(r"^(\t*)arguments = \{\n", output, re.MULTILINE))
    if len(working) != 1 or working[0] != str(root) or len(starts) != 1:
        raise ValueError("LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH")
    indent = starts[0].group(1)
    arguments: list[str] = []
    for line in output[starts[0].end() :].splitlines():
        if line == indent + "}":
            break
        if not line.startswith(indent + "\t"):
            raise ValueError("LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH")
        arguments.append(line[len(indent) + 1 :])
    else:
        raise ValueError("LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH")
    if arguments != [str(runtime), "run", "--repo-root", str(root)]:
        raise ValueError("LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH")
    if expected_revision is not None:
        releases = re.findall(
            r"^\t+NEXUS_RELEASE_REVISION => ([0-9a-f]{40})$",
            output,
            re.MULTILINE,
        )
        if releases != [expected_revision]:
            raise ValueError("LOCAL_GATEWAY_SERVICE_RELEASE_MISMATCH")
    states = re.findall(r"^\t?state = ([^\n]+)$", output, re.MULTILINE)
    pids = re.findall(r"^\t?pid = ([1-9][0-9]*)$", output, re.MULTILINE)
    exits = re.findall(r"^\t?last exit code = ([0-9]+)(?::[^\n]*)?$", output, re.MULTILINE)
    if len(states) != 1 or len(pids) > 1 or len(exits) > 1:
        raise ValueError("LOCAL_GATEWAY_SERVICE_STATUS_UNAVAILABLE")
    if states[0].strip() == "running" and len(pids) == 1:
        state = "running"
        pid: int | None = int(pids[0])
    elif states[0].strip() in {"inactive", "not running", "exited"} and not pids:
        state = "inactive"
        pid = None
    else:
        raise ValueError("LOCAL_GATEWAY_SERVICE_STATUS_UNAVAILABLE")
    return state, pid, int(exits[0]) if exits else None


def _client_ssl_context(local: Path, role: str) -> ssl.SSLContext:
    if role not in {"broker", "market"}:
        raise ValueError("LOCAL_GATEWAY_SERVICE_ROLE_INVALID")
    certificate = local / f"{role}-client.pem"
    key = local / f"{role}-client.key"
    authority = local / "server-ca.pem"
    for path in (certificate, key, authority):
        _private_existing_file(path)
    context = ssl.create_default_context(cafile=str(authority))
    context.load_cert_chain(certfile=str(certificate), keyfile=str(key))
    return context


def local_health_ready(
    root: Path,
    *,
    expected_pid: int | None = None,
    get: Callable[[int, str], int] | None = None,
) -> bool:
    if expected_pid is not None:
        marker_path = root / ".secrets/nexus-fosun-local/replay.sqlite3.ready"
        try:
            marker = read_readiness_marker(marker_path)
        except RuntimeError:
            return False
        if (
            marker is None
            or marker[0] != "ready"
            or marker[1] != expected_pid
            or marker[2] is None
            or time.time() >= marker[2]
        ):
            return False
    if get is None:
        load_local_gateway_settings(root)
        local = root / ".secrets/nexus-fosun-local"

        def get(port: int, role: str) -> int:
            try:
                with httpx.Client(
                    verify=_client_ssl_context(local, role),
                    follow_redirects=False,
                    trust_env=False,
                    timeout=2.0,
                ) as client:
                    return client.get(f"https://127.0.0.1:{port}/health/ready").status_code
            except (OSError, httpx.HTTPError, ValueError):
                return 0

    return all(status == 200 for status in (get(8081, "broker"), get(8082, "market")))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-fosun-gateway-service")
    parser.add_argument("action", choices=("start", "status", "stop"))
    parser.add_argument("--repo-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    root = arguments.repo_root.resolve()
    runtime = root / ".venv/bin/nexus-fosun-gateway-local"
    if arguments.action == "status":
        return _service_action(arguments.action, root, runtime)
    try:
        with _service_control_lock(root):
            return _service_action(arguments.action, root, runtime)
    except ValueError as error:
        code = str(error)
        if code not in {
            "LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE",
            "LOCAL_GATEWAY_SERVICE_CONTROL_BUSY",
        }:
            code = "LOCAL_GATEWAY_SERVICE_CONTROL_UNSAFE"
        print(json.dumps({"error_code": code}))
        return 1


def _service_action(action: str, root: Path, runtime: Path) -> int:
    launchctl = shutil.which("launchctl")
    if launchctl is None:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_RUNTIME_MISSING"}))
        return 1
    domain_label = f"gui/{os.getuid()}/{LABEL}"
    inspected = subprocess.run(
        [launchctl, "print", domain_label],
        check=False,
        capture_output=True,
    )
    if inspected.returncode == 113:
        state, pid, last_exit = "stopped", None, None
    elif inspected.returncode != 0:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_STATUS_UNAVAILABLE"}))
        return 1
    else:
        try:
            revision = _checkout_revision(root) if action != "stop" else None
            state, pid, last_exit = inspect_launchctl_output(
                inspected.stdout.decode("utf-8"),
                root,
                runtime,
                expected_revision=revision,
            )
        except (UnicodeError, ValueError) as error:
            code = str(error)
            if code not in {
                "LOCAL_GATEWAY_SERVICE_OWNER_MISMATCH",
                "LOCAL_GATEWAY_SERVICE_RELEASE_MISMATCH",
            }:
                code = "LOCAL_GATEWAY_SERVICE_STATUS_UNAVAILABLE"
            print(json.dumps({"error_code": code}))
            return 1
    if action == "status":
        if state == "running":
            try:
                ready = local_health_ready(root, expected_pid=pid)
            except ValueError:
                ready = False
            print(json.dumps({"status": "ready" if ready else "degraded"}))
            return 0 if ready else 1
        print(json.dumps({"status": state}))
        return 0
    if action == "start":
        if state == "running":
            ready = local_health_ready(root, expected_pid=pid)
            print(json.dumps({"status": "running" if ready else "degraded"}))
            return 0 if ready else 1
        if state == "inactive":
            if last_exit != 0:
                print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_PREVIOUS_EXIT_UNKNOWN"}))
                return 1
            removed = subprocess.run(
                [launchctl, "bootout", domain_label],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if removed.returncode != 0:
                print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_BOOTOUT_FAILED"}))
                return 1
        return _start(root, runtime, launchctl, domain_label)
    if state == "stopped":
        print(json.dumps({"status": "stopped"}))
        return 0
    if state == "running":
        interrupted = subprocess.run(
            [launchctl, "kill", "SIGINT", domain_label],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if interrupted.returncode != 0:
            print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_STOP_FAILED"}))
            return 1
        for _ in range(50):
            inspected = subprocess.run(
                [launchctl, "print", domain_label], check=False, capture_output=True
            )
            if inspected.returncode != 0:
                break
            try:
                state, _, last_exit = inspect_launchctl_output(
                    inspected.stdout.decode("utf-8"),
                    root,
                    runtime,
                    expected_revision=revision,
                )
            except (UnicodeError, ValueError):
                break
            if state == "inactive" and last_exit is not None:
                break
            time.sleep(0.2)
        if state != "inactive" or last_exit is None:
            print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_STOP_UNCONFIRMED"}))
            return 1
    removed = subprocess.run(
        [launchctl, "bootout", domain_label],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if removed.returncode != 0:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_BOOTOUT_FAILED"}))
        return 1
    inspected = subprocess.run([launchctl, "print", domain_label], check=False, capture_output=True)
    if inspected.returncode != 113:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_STOP_UNCONFIRMED"}))
        return 1
    (root / ".secrets/nexus-fosun-local/service.plist").unlink(missing_ok=True)
    print(json.dumps({"status": "stopped"}))
    return 0


def _checkout_revision(root: Path) -> str:
    revision = checkout_release(root)
    if revision is None:
        raise ValueError("LOCAL_GATEWAY_RELEASE_INVALID")
    return revision


def _start(root: Path, runtime: Path, launchctl: str, domain_label: str) -> int:
    if not runtime.is_file() or not os.access(runtime, os.X_OK):
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_RUNTIME_MISSING"}))
        return 1
    try:
        revision = _checkout_revision(root)
        directory = root / ".secrets/nexus-fosun-local"
        info = directory.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise ValueError
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
                    raise ValueError
            finally:
                os.close(descriptor)
        checked = subprocess.run(
            [str(runtime), "check", "--repo-root", str(root)],
            check=False,
            capture_output=True,
            timeout=10,
        )
        check_result = json.loads(checked.stdout) if checked.returncode == 0 else None
        if not isinstance(check_result, dict) or check_result.get("status") != "ready":
            raise ValueError
        path = directory / "service.plist"
        temporary: Path | None = None
        try:
            with NamedTemporaryFile(dir=directory, prefix="service.plist.", delete=False) as file:
                temporary = Path(file.name)
                plistlib.dump(build_service_plist(root, runtime, revision), file)
                file.flush()
                os.fsync(file.fileno())
            temporary.chmod(0o600)
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    except (OSError, ValueError, json.JSONDecodeError, subprocess.TimeoutExpired):
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_PREFLIGHT_FAILED"}))
        return 1
    bootstrapped = subprocess.run(
        [launchctl, "bootstrap", f"gui/{os.getuid()}", str(path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if bootstrapped.returncode != 0:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_BOOTSTRAP_FAILED"}))
        return 1
    started = subprocess.run(
        [launchctl, "kickstart", domain_label],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if started.returncode != 0:
        print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_START_FAILED"}))
        return 1
    for _ in range(70):
        inspected = subprocess.run(
            [launchctl, "print", domain_label], check=False, capture_output=True
        )
        if inspected.returncode != 0:
            break
        try:
            state, pid, last_exit = inspect_launchctl_output(
                inspected.stdout.decode("utf-8"),
                root,
                runtime,
                expected_revision=revision,
            )
        except (UnicodeError, ValueError):
            break
        if state == "inactive" and last_exit is None:
            time.sleep(1)
            continue
        if state != "running" or last_exit not in (None, 0):
            break
        try:
            if local_health_ready(root, expected_pid=pid):
                print(json.dumps({"status": "started"}))
                return 0
        except ValueError:
            break
        time.sleep(1)
    print(json.dumps({"error_code": "LOCAL_GATEWAY_SERVICE_START_UNCONFIRMED"}))
    return 1
