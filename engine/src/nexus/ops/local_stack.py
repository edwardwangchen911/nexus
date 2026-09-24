"""Redacted, exact command authority for the fixture local stack."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path

from dotenv import dotenv_values


class LocalStackConfigurationError(ValueError):
    """The requested local stack could not be identified safely."""


class LocalStackAction(StrEnum):
    DOCTOR = "doctor"
    UP = "up"
    STATUS = "status"
    DOWN = "down"


@dataclass(frozen=True)
class LocalStackCommand:
    argv: tuple[str, ...]
    timeout_seconds: int


@dataclass(frozen=True)
class LocalStackStepResult:
    action: LocalStackAction
    ordinal: int
    exit_code: int
    result_code: str
    fact: str = "compose"
    details: tuple[tuple[str, object], ...] = ()


_HOST_STATUSES = frozenset({"started", "running", "inactive", "stopped"})
_HOST_ERROR_CODES = frozenset(
    {
        "AGENT_PAPER_SERVICE_ACKNOWLEDGEMENT_INVALID",
        "AGENT_PAPER_SERVICE_ACTION_INVALID",
        "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED",
        "AGENT_PAPER_SERVICE_BOOTSTRAP_FAILED",
        "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE",
        "AGENT_PAPER_SERVICE_OWNER_MISMATCH",
        "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED",
        "AGENT_PAPER_SERVICE_RUNTIME_MISSING",
        "AGENT_PAPER_SERVICE_START_FAILED",
        "AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE",
        "AGENT_PAPER_SERVICE_STOP_FAILED",
        "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED",
        "LOCAL_ENV_REQUIRED",
        "LOCAL_ENV_UNSAFE",
        "PROJECT_NAME_INVALID",
        "REPOSITORY_ROOT_INVALID",
    }
)
_SAFE_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}")
_SAFE_TEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:+-]{0,127}")
_RELEASE_REVISION = re.compile(r"[0-9a-f]{40}")
_PINNED_ENGINE_BASE_ID = "sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a"
_PINNED_ENGINE_BASE_IMAGE = f"python:3.12-slim@{_PINNED_ENGINE_BASE_ID}"
_LOCAL_ENGINE_BASE_IMAGE = "nexus-local-python-base:3.12"


def _verified_local_engine_base_image(
    *, inspector: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run
) -> str | None:
    """Use the cached base only when its image ID equals the reviewed digest."""
    try:
        completed = inspector(
            ("docker", "image", "inspect", _LOCAL_ENGINE_BASE_IMAGE, "--format", "{{.Id}}"),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    if completed.stdout.strip() != _PINNED_ENGINE_BASE_ID:
        raise LocalStackConfigurationError("LOCAL_STACK_BASE_IMAGE_MISMATCH")
    return _LOCAL_ENGINE_BASE_IMAGE


def _checkout_revision(repo_root: Path) -> str | None:
    completed = subprocess.run(
        ("git", "-C", str(repo_root), "rev-parse", "--verify", "HEAD"),
        check=False,
        capture_output=True,
        text=True,
    )
    revision = completed.stdout.strip()
    return revision if completed.returncode == 0 and _RELEASE_REVISION.fullmatch(revision) else None


def build_local_stack_commands(
    action: LocalStackAction,
    *,
    repo_root: Path,
    env_file: Path,
    project_name: str,
) -> tuple[LocalStackCommand, ...]:
    """Build only the reviewed app-profile command sequence."""
    root = repo_root.resolve()
    compose_file = root / "infra/compose.yaml"
    if not root.is_dir() or not compose_file.is_file():
        raise LocalStackConfigurationError("REPOSITORY_ROOT_INVALID")
    if re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project_name) is None:
        raise LocalStackConfigurationError("PROJECT_NAME_INVALID")
    if action is not LocalStackAction.DOCTOR:
        resolved_env = _validate_local_env(root, env_file)
    else:
        resolved_env = env_file.resolve()
    compose = (
        "docker",
        "compose",
        "-p",
        project_name,
        "--env-file",
        str(resolved_env),
        "-f",
        str(compose_file),
        "--profile",
        "app",
    )
    if action is LocalStackAction.UP:
        return (
            LocalStackCommand((*compose, "config", "--quiet"), 30),
            LocalStackCommand((*compose, "up", "-d", "postgres", "minio"), 120),
            LocalStackCommand((*compose, "build", "object-store-init"), 300),
            LocalStackCommand((*compose, "build", "api"), 300),
            LocalStackCommand((*compose, "build", "worker"), 300),
            LocalStackCommand(
                (*compose, "run", "--rm", "object-store-init"),
                120,
            ),
            LocalStackCommand(
                (
                    *compose,
                    "run",
                    "--rm",
                    "--no-deps",
                    "api",
                    "alembic",
                    "-c",
                    "engine/alembic.ini",
                    "upgrade",
                    "head",
                ),
                180,
            ),
            LocalStackCommand(
                (*compose, "up", "-d", "--no-build", "api", "worker"),
                180,
            ),
        )
    if action is LocalStackAction.STATUS:
        return (LocalStackCommand((*compose, "ps", "-a"), 30),)
    if action is LocalStackAction.DOWN:
        return (
            LocalStackCommand((*compose, "stop", "api", "worker"), 60),
            LocalStackCommand((*compose, "down"), 120),
        )
    return (
        LocalStackCommand(("docker", "version"), 30),
        LocalStackCommand(("docker", "compose", "version"), 30),
        LocalStackCommand(("uv", "--version"), 30),
        LocalStackCommand(("node", "--version"), 30),
        LocalStackCommand(("npm", "--version"), 30),
    )


def run_local_stack(
    action: LocalStackAction,
    *,
    repo_root: Path,
    env_file: Path,
    project_name: str,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    base_image: str = _PINNED_ENGINE_BASE_IMAGE,
) -> tuple[LocalStackStepResult, ...]:
    """Run the exact Compose and host-service lifecycle, emitting safe facts only."""
    root = repo_root.resolve()
    resolved_env = (
        env_file.resolve()
        if action is LocalStackAction.DOCTOR
        else _validate_local_env(root, env_file)
    )
    agent_paper_enabled = (
        _agent_paper_enabled(resolved_env) if action is LocalStackAction.UP else False
    )
    commands = build_local_stack_commands(
        action,
        repo_root=root,
        env_file=resolved_env,
        project_name=project_name,
    )
    command_environment = dict(os.environ)
    if action is LocalStackAction.UP:
        if base_image not in {_PINNED_ENGINE_BASE_IMAGE, _LOCAL_ENGINE_BASE_IMAGE}:
            raise LocalStackConfigurationError("LOCAL_STACK_BASE_IMAGE_INVALID")
        command_environment["NEXUS_ENGINE_BASE_IMAGE"] = base_image
    release_revision = _checkout_revision(root)
    if release_revision is not None:
        command_environment["NEXUS_RELEASE_REVISION"] = release_revision
    results: list[LocalStackStepResult] = []

    def run_command(command: LocalStackCommand, *, fact: str = "compose") -> tuple[int, str]:
        ordinal = len(results) + 1
        try:
            completed = runner(
                command.argv,
                cwd=root,
                timeout=command.timeout_seconds,
                check=False,
                text=True,
                capture_output=True,
                env=command_environment,
            )
        except subprocess.TimeoutExpired:
            results.append(
                LocalStackStepResult(
                    action=action,
                    ordinal=ordinal,
                    exit_code=-1,
                    result_code="LOCAL_STACK_STEP_TIMEOUT",
                    fact=fact,
                )
            )
            return -1, ""
        except OSError:
            results.append(
                LocalStackStepResult(
                    action=action,
                    ordinal=ordinal,
                    exit_code=-1,
                    result_code="LOCAL_STACK_STEP_UNAVAILABLE",
                    fact=fact,
                )
            )
            return -1, ""
        result_code = (
            "LOCAL_STACK_STEP_OK" if completed.returncode == 0 else "LOCAL_STACK_STEP_FAILED"
        )
        results.append(
            LocalStackStepResult(
                action=action,
                ordinal=ordinal,
                exit_code=completed.returncode,
                result_code=result_code,
                fact=fact,
            )
        )
        return completed.returncode, completed.stdout

    def run_host(action_name: str) -> bool:
        command = _host_service_command(
            action_name,
            repo_root=root,
            env_file=resolved_env,
            project_name=project_name,
        )
        exit_code, stdout = run_command(command, fact="host_service")
        if exit_code == -1:
            return False
        results[-1] = _parse_host_service_result(results[-1], stdout)
        status = dict(results[-1].details).get("status")
        if action_name == "start":
            accepted = status in {"started", "running"}
        elif action_name == "stop":
            accepted = status == "stopped"
        else:
            accepted = status in _HOST_STATUSES
        if exit_code != 0 or not accepted:
            if results[-1].exit_code == 0:
                results[-1] = replace(results[-1], exit_code=1)
            return False
        return True

    if action is LocalStackAction.UP and agent_paper_enabled:
        if not run_host("status"):
            return tuple(results)
        if dict(results[-1].details).get("status") in {"running", "started"}:
            if release_revision is None:
                results.append(
                    LocalStackStepResult(
                        action=action,
                        ordinal=len(results) + 1,
                        exit_code=1,
                        result_code="LOCAL_STACK_RELEASE_UNVERIFIED",
                        fact="release",
                    )
                )
                return tuple(results)
            diagnostic = _service_status_command(repo_root=root, env_file=resolved_env)
            exit_code, stdout = run_command(diagnostic, fact="heartbeat")
            if exit_code == -1:
                return tuple(results)
            heartbeat, _ = _parse_service_status_results(
                action=action,
                first_ordinal=results[-1].ordinal,
                stdout=stdout,
                expected_release_revision=release_revision,
            )
            results[-1] = heartbeat
            if heartbeat.exit_code != 0:
                return tuple(results)

    if action is LocalStackAction.DOWN:
        if not run_host("stop"):
            return tuple(results)
        for command in commands:
            exit_code, _ = run_command(command)
            if exit_code != 0:
                break
        return tuple(results)

    for command in commands:
        exit_code, _ = run_command(command)
        if exit_code != 0 and action is not LocalStackAction.STATUS:
            return tuple(results)

    if action is LocalStackAction.UP:
        if agent_paper_enabled:
            if not run_host("start") or not run_host("status"):
                return tuple(results)
            if dict(results[-1].details).get("status") != "running":
                results[-1] = replace(results[-1], exit_code=1)
                return tuple(results)
            for attempt in range(2):
                diagnostic = _service_status_command(repo_root=root, env_file=resolved_env)
                exit_code, stdout = run_command(diagnostic, fact="heartbeat")
                if exit_code == -1:
                    return tuple(results)
                heartbeat, preflight = _parse_service_status_results(
                    action=action,
                    first_ordinal=results[-1].ordinal,
                    stdout=stdout,
                    expected_release_revision=release_revision,
                )
                results[-1] = heartbeat
                results.append(preflight)
                if (
                    attempt == 1
                    or heartbeat.result_code
                    not in {"AGENT_PAPER_HEARTBEAT_STALE", "AGENT_PAPER_HEARTBEAT_MISSING"}
                    or preflight.exit_code != 0
                ):
                    break
                results.pop()
                results.pop()
                if not run_host("status"):
                    return tuple(results)
                if dict(results[-1].details).get("status") != "running":
                    results[-1] = replace(results[-1], exit_code=1)
                    return tuple(results)
        return tuple(results)

    if action is LocalStackAction.STATUS:
        run_host("status")
        diagnostic = _service_status_command(repo_root=root, env_file=resolved_env)
        try:
            completed = runner(
                diagnostic.argv,
                cwd=root,
                timeout=diagnostic.timeout_seconds,
                check=False,
                text=True,
                capture_output=True,
                env=command_environment,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            result_code = (
                "LOCAL_STACK_STEP_TIMEOUT"
                if isinstance(error, subprocess.TimeoutExpired)
                else "LOCAL_STACK_STEP_UNAVAILABLE"
            )
            for fact in ("heartbeat", "preflight"):
                results.append(
                    LocalStackStepResult(
                        action=action,
                        ordinal=len(results) + 1,
                        exit_code=-1,
                        result_code=result_code,
                        fact=fact,
                    )
                )
        else:
            results.extend(
                _parse_service_status_results(
                    action=action,
                    first_ordinal=len(results) + 1,
                    stdout=completed.stdout,
                    expected_release_revision=release_revision,
                )
            )
        if results[0].exit_code == 0:
            compose_prefix = commands[0].argv[:-2]
            for service in ("api", "worker"):
                release_command = LocalStackCommand(
                    (*compose_prefix, "exec", "-T", service, "printenv", "NEXUS_RELEASE_REVISION"),
                    30,
                )
                exit_code, stdout = run_command(release_command, fact=f"{service}_release")
                if exit_code == 0:
                    reported = stdout.strip()
                    details: tuple[tuple[str, object], ...] = ()
                    if release_revision is not None:
                        details = (("expected_release_revision", release_revision),)
                    if _RELEASE_REVISION.fullmatch(reported) is not None:
                        details += (("reported_release_revision", reported),)
                    matched = release_revision is not None and reported == release_revision
                    results[-1] = replace(
                        results[-1],
                        exit_code=0 if matched else 1,
                        result_code=(
                            "LOCAL_STACK_RELEASE_MATCHED"
                            if matched
                            else "LOCAL_STACK_RELEASE_MISMATCH"
                            if _RELEASE_REVISION.fullmatch(reported) is not None
                            else "LOCAL_STACK_RELEASE_UNVERIFIED"
                        ),
                        details=details,
                    )
    return tuple(results)


def main(argv: list[str] | None = None) -> int:
    """Parse one action and emit safe result metadata only."""
    parser = argparse.ArgumentParser(prog="nexus-local-stack")
    parser.add_argument("action", choices=tuple(action.value for action in LocalStackAction))
    parser.add_argument("--repo-root", type=Path, default=_discover_repo_root())
    parser.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    parser.add_argument("--project-name", default="nexus-local")
    arguments = parser.parse_args(argv)
    repo_root = arguments.repo_root.resolve()
    env_file = arguments.env_file
    if not env_file.is_absolute():
        env_file = repo_root / env_file
    try:
        action = LocalStackAction(arguments.action)
        base_image = _PINNED_ENGINE_BASE_IMAGE
        if action is LocalStackAction.UP:
            base_image = _verified_local_engine_base_image() or _PINNED_ENGINE_BASE_IMAGE
        results = run_local_stack(
            action,
            repo_root=repo_root,
            env_file=env_file,
            project_name=arguments.project_name,
            base_image=base_image,
        )
    except LocalStackConfigurationError:
        sys.stderr.write("local-stack configuration rejected\n")
        return 2
    for result in results:
        safe_details = " ".join(f"{name}={value}" for name, value in result.details)
        suffix = f" {safe_details}" if safe_details else ""
        if result.fact == "compose":
            sys.stdout.write(
                f"{result.action.value} step={result.ordinal} "
                f"code={result.result_code} exit={result.exit_code}\n"
            )
        else:
            sys.stdout.write(
                f"{result.action.value} fact={result.fact} step={result.ordinal} "
                f"code={result.result_code} exit={result.exit_code}{suffix}\n"
            )
    return 0 if all(result.exit_code == 0 for result in results) else 1


def _discover_repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _validate_local_env(repo_root: Path, env_file: Path) -> Path:
    if env_file.is_symlink() or not env_file.is_file():
        error_code = "LOCAL_ENV_UNSAFE" if env_file.is_symlink() else "LOCAL_ENV_REQUIRED"
        raise LocalStackConfigurationError(error_code)
    resolved = env_file.resolve()
    try:
        relative = resolved.relative_to(repo_root)
    except ValueError:
        raise LocalStackConfigurationError("LOCAL_ENV_UNSAFE") from None
    tracked = subprocess.run(
        ("git", "-C", str(repo_root), "ls-files", "--error-unmatch", "--", str(relative)),
        check=False,
        capture_output=True,
        text=True,
    )
    ignored = subprocess.run(
        ("git", "-C", str(repo_root), "check-ignore", "-q", "--", str(relative)),
        check=False,
        capture_output=True,
        text=True,
    )
    if tracked.returncode == 0 or ignored.returncode != 0:
        raise LocalStackConfigurationError("LOCAL_ENV_UNSAFE")
    return resolved


def _agent_paper_enabled(env_file: Path) -> bool:
    value = dotenv_values(env_file, interpolate=False).get(
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false"
    )
    normalized = value.strip().lower() if isinstance(value, str) else "false"
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"", "0", "false", "no", "off"}:
        return False
    raise LocalStackConfigurationError("AGENT_PAPER_CYCLE_FLAG_INVALID")


def _host_service_command(
    action: str, *, repo_root: Path, env_file: Path, project_name: str
) -> LocalStackCommand:
    return LocalStackCommand(
        (
            sys.executable,
            str(repo_root / "scripts/macos/agent-paper-loop-service.py"),
            action,
            "--repo-root",
            str(repo_root),
            "--env-file",
            str(env_file),
            "--project-name",
            project_name,
        ),
        30 if action == "status" else 60,
    )


def _service_status_command(*, repo_root: Path, env_file: Path) -> LocalStackCommand:
    return LocalStackCommand(
        (
            str(repo_root / ".venv/bin/nexus-engine-agent-paper-loop"),
            "--service-status",
            "--repo-root",
            str(repo_root),
            "--env-file",
            str(env_file),
        ),
        30,
    )


def _json_object(stdout: str) -> dict[str, object] | None:
    try:
        payload = json.loads(stdout)
    except (json.JSONDecodeError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_host_service_result(result: LocalStackStepResult, stdout: str) -> LocalStackStepResult:
    payload = _json_object(stdout)
    if payload is not None and payload.get("status") in _HOST_STATUSES:
        status = str(payload["status"])
        return LocalStackStepResult(
            action=result.action,
            ordinal=result.ordinal,
            exit_code=result.exit_code,
            result_code=f"AGENT_PAPER_SERVICE_{status.upper()}",
            fact="host_service",
            details=(("status", status),),
        )
    error_code = payload.get("error_code") if payload is not None else None
    if isinstance(error_code, str) and error_code in _HOST_ERROR_CODES:
        return LocalStackStepResult(
            action=result.action,
            ordinal=result.ordinal,
            exit_code=result.exit_code,
            result_code=error_code,
            fact="host_service",
        )
    return LocalStackStepResult(
        action=result.action,
        ordinal=result.ordinal,
        exit_code=result.exit_code if result.exit_code != 0 else 1,
        result_code="AGENT_PAPER_SERVICE_OUTPUT_INVALID",
        fact="host_service",
    )


def _safe_text(value: object) -> str | None:
    return value if isinstance(value, str) and _SAFE_TEXT.fullmatch(value) else None


def _safe_codes(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and _SAFE_CODE.fullmatch(item) for item in value
    ):
        return None
    return tuple(value)


def _parse_service_status_results(
    *,
    action: LocalStackAction,
    first_ordinal: int,
    stdout: str,
    expected_release_revision: str | None,
) -> tuple[LocalStackStepResult, LocalStackStepResult]:
    payload = _json_object(stdout)
    heartbeat = payload.get("heartbeat") if payload is not None else None
    preflight = payload.get("preflight") if payload is not None else None
    heartbeat_details: tuple[tuple[str, object], ...] = ()
    heartbeat_code = "AGENT_PAPER_HEARTBEAT_OUTPUT_INVALID"
    if isinstance(heartbeat, dict) and heartbeat.get("status") in {
        "fresh",
        "stale",
        "incompatible",
        "missing",
        "unavailable",
    }:
        details: list[tuple[str, object]] = [("status", heartbeat["status"])]
        valid = True
        for name in ("seen_at", "started_at", "process_version", "migration_head"):
            if name in heartbeat:
                safe = _safe_text(heartbeat[name])
                if safe is None:
                    valid = False
                    break
                details.append((name, safe))
        if valid:
            heartbeat_details = tuple(details)
            heartbeat_code = f"AGENT_PAPER_HEARTBEAT_{str(heartbeat['status']).upper()}"
            if heartbeat["status"] == "fresh" and expected_release_revision is not None:
                reported = _safe_text(heartbeat.get("release_revision"))
                if reported is None or _RELEASE_REVISION.fullmatch(reported) is None:
                    heartbeat_code = "AGENT_PAPER_HEARTBEAT_RELEASE_UNVERIFIED"
                    heartbeat_details += (("expected_release_revision", expected_release_revision),)
                elif reported != expected_release_revision:
                    heartbeat_code = "AGENT_PAPER_HEARTBEAT_RELEASE_MISMATCH"
                    heartbeat_details += (
                        ("expected_release_revision", expected_release_revision),
                        ("reported_release_revision", reported),
                    )

    preflight_details: tuple[tuple[str, object], ...] = ()
    preflight_code = "AGENT_PAPER_PREFLIGHT_OUTPUT_INVALID"
    if isinstance(preflight, dict) and preflight.get("status") in {"ready", "blocked"}:
        blocking = _safe_codes(preflight.get("blocking_codes"))
        optional = _safe_codes(preflight.get("optional_codes"))
        locked = preflight.get("execution_locked")
        external = preflight.get("external_requests")
        if (
            blocking is not None
            and optional is not None
            and locked is True
            and type(external) is int
            and external == 0
        ):
            preflight_details = (
                ("status", preflight["status"]),
                ("execution_locked", locked),
                ("external_requests", external),
                ("blocking_codes", blocking),
                ("optional_codes", optional),
            )
            preflight_code = f"AGENT_PAPER_PREFLIGHT_{str(preflight['status']).upper()}"

    return (
        LocalStackStepResult(
            action=action,
            ordinal=first_ordinal,
            exit_code=(
                0 if heartbeat_details and heartbeat_code == "AGENT_PAPER_HEARTBEAT_FRESH" else 1
            ),
            result_code=heartbeat_code,
            fact="heartbeat",
            details=heartbeat_details,
        ),
        LocalStackStepResult(
            action=action,
            ordinal=first_ordinal + 1,
            exit_code=(
                0
                if preflight_details
                and dict(preflight_details)["status"] == "ready"
                and dict(preflight_details)["external_requests"] == 0
                else 1
            ),
            result_code=preflight_code,
            fact="preflight",
            details=preflight_details,
        ),
    )
