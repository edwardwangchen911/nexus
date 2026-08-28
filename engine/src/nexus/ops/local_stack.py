"""Redacted, exact command authority for the fixture local stack."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


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
            LocalStackCommand((*compose, "run", "--rm", "object-store-init"), 120),
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
            LocalStackCommand((*compose, "up", "-d", "api", "worker"), 180),
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
) -> tuple[LocalStackStepResult, ...]:
    """Run reviewed commands and discard all subprocess output."""
    commands = build_local_stack_commands(
        action,
        repo_root=repo_root,
        env_file=env_file,
        project_name=project_name,
    )
    results: list[LocalStackStepResult] = []
    for ordinal, command in enumerate(commands, start=1):
        try:
            completed = runner(
                command.argv,
                cwd=repo_root.resolve(),
                timeout=command.timeout_seconds,
                check=False,
                text=True,
                capture_output=True,
            )
        except subprocess.TimeoutExpired:
            results.append(
                LocalStackStepResult(
                    action=action,
                    ordinal=ordinal,
                    exit_code=-1,
                    result_code="LOCAL_STACK_STEP_TIMEOUT",
                )
            )
            break
        result_code = (
            "LOCAL_STACK_STEP_OK"
            if completed.returncode == 0
            else "LOCAL_STACK_STEP_FAILED"
        )
        results.append(
            LocalStackStepResult(
                action=action,
                ordinal=ordinal,
                exit_code=completed.returncode,
                result_code=result_code,
            )
        )
        if completed.returncode != 0:
            break
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
        results = run_local_stack(
            LocalStackAction(arguments.action),
            repo_root=repo_root,
            env_file=env_file,
            project_name=arguments.project_name,
        )
    except LocalStackConfigurationError:
        sys.stderr.write("local-stack configuration rejected\n")
        return 2
    for result in results:
        sys.stdout.write(
            f"{result.action.value} step={result.ordinal} "
            f"code={result.result_code} exit={result.exit_code}\n"
        )
    return 0 if all(result.result_code == "LOCAL_STACK_STEP_OK" for result in results) else 1


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
