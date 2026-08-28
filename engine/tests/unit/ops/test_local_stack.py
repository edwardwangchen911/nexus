"""Behavior contracts for the redacted local-stack controller."""

import subprocess
from pathlib import Path

import pytest
from nexus.ops.local_stack import (
    LocalStackAction,
    LocalStackConfigurationError,
    LocalStackStepResult,
    build_local_stack_commands,
    main,
    run_local_stack,
)


def test_up_uses_exact_app_services_and_never_broker_profile(repo_root: Path) -> None:
    """A regression that adds broker services to ordinary local up must fail."""
    commands = build_local_stack_commands(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
    )

    rendered = [command.argv for command in commands]
    assert all("broker" not in argument for argv in rendered for argument in argv)
    assert rendered[-1][-4:] == ("up", "-d", "api", "worker")


def test_missing_env_file_fails_before_any_command_is_built(repo_root: Path) -> None:
    """A missing ignored configuration must not silently select process env."""
    with pytest.raises(LocalStackConfigurationError, match="LOCAL_ENV_REQUIRED"):
        build_local_stack_commands(
            LocalStackAction.UP,
            repo_root=repo_root,
            env_file=repo_root / "infra/missing.env",
            project_name="nexus-phase3-local",
        )


@pytest.mark.parametrize("unsafe_kind", ("tracked", "symlink", "outside"))
def test_up_rejects_env_files_outside_the_ignored_local_boundary(
    repo_root: Path,
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    """A regression that admits a tracked, linked, or outside env could expose secrets."""
    if unsafe_kind == "tracked":
        env_file = repo_root / "infra/tracked.conf"
        env_file.write_text("NEXUS_ENV=test\n", encoding="utf-8")
        subprocess.run(
            ("git", "-C", str(repo_root), "add", "infra/tracked.conf"),
            check=True,
        )
    elif unsafe_kind == "symlink":
        env_file = repo_root / "infra/.env.link"
        env_file.symlink_to(repo_root / "infra/.env.local")
    else:
        env_file = tmp_path / ".env.outside"
        env_file.write_text("NEXUS_ENV=test\n", encoding="utf-8")

    with pytest.raises(LocalStackConfigurationError, match="LOCAL_ENV_UNSAFE"):
        build_local_stack_commands(
            LocalStackAction.UP,
            repo_root=repo_root,
            env_file=env_file,
            project_name="nexus-phase3-local",
        )


def test_status_and_down_target_only_the_exact_compose_project(repo_root: Path) -> None:
    """A broad stop/down command must not be constructible by this controller."""
    status = build_local_stack_commands(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
    )
    down = build_local_stack_commands(
        LocalStackAction.DOWN,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
    )

    assert [command.argv[-2:] for command in status] == [("ps", "-a")]
    assert down[0].argv[-3:] == ("stop", "api", "worker")
    assert down[1].argv[-1:] == ("down",)
    assert all("broker" not in argument for command in down for argument in command.argv)


def test_runner_returns_only_safe_step_metadata(repo_root: Path) -> None:
    """Captured subprocess output must never cross the local-stack API boundary."""
    captured_argv: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        captured_argv.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="fixture-secret-provider-body",
            stderr="fixture-private-key",
        )

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert captured_argv[0][-2:] == ("ps", "-a")
    assert [result.result_code for result in results] == ["LOCAL_STACK_STEP_OK"]
    assert "fixture-secret" not in repr(results)


def test_doctor_needs_no_env_and_checks_exact_local_dependencies(repo_root: Path) -> None:
    """Removing a required runtime probe must make doctor observably incomplete."""
    commands = build_local_stack_commands(
        LocalStackAction.DOCTOR,
        repo_root=repo_root,
        env_file=repo_root / "infra/does-not-exist.env",
        project_name="nexus-phase3-local",
    )

    assert [command.argv for command in commands] == [
        ("docker", "version"),
        ("docker", "compose", "version"),
        ("uv", "--version"),
        ("node", "--version"),
        ("npm", "--version"),
    ]


@pytest.mark.parametrize(
    "project_name",
    ("", "UPPER", "../nexus", "nexus space", "nexus."),
)
def test_project_name_rejects_ambiguous_compose_scope(
    repo_root: Path,
    project_name: str,
) -> None:
    """An ambiguous project name could target resources outside the task scope."""
    with pytest.raises(LocalStackConfigurationError, match="PROJECT_NAME_INVALID"):
        build_local_stack_commands(
            LocalStackAction.STATUS,
            repo_root=repo_root,
            env_file=repo_root / "infra/.env.local",
            project_name=project_name,
        )


def test_runner_stops_after_first_nonzero_exit(repo_root: Path) -> None:
    """Continuing after a failed preflight could start a partially configured stack."""
    calls = 0

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 17, "sensitive", "sensitive")

    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert calls == 1
    assert results == (
        LocalStackStepResult(
            action=LocalStackAction.UP,
            ordinal=1,
            exit_code=17,
            result_code="LOCAL_STACK_STEP_FAILED",
        ),
    )


def test_runner_converts_timeout_to_safe_failure(repo_root: Path) -> None:
    """A timeout must not leak the command output or escape without a safe result."""
    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(argv, 30, output="sensitive-output", stderr="sensitive")

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert results[0].exit_code == -1
    assert results[0].result_code == "LOCAL_STACK_STEP_TIMEOUT"
    assert "sensitive" not in repr(results)


def test_main_prints_only_safe_codes(
    repo_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI output must remain safe even when underlying tooling had sensitive output."""
    def fixture_run(*_: object, **__: object) -> tuple[LocalStackStepResult, ...]:
        return (
            LocalStackStepResult(
                action=LocalStackAction.STATUS,
                ordinal=1,
                exit_code=0,
                result_code="LOCAL_STACK_STEP_OK",
            ),
        )

    monkeypatch.setattr("nexus.ops.local_stack.run_local_stack", fixture_run)

    exit_code = main(
        [
            "status",
            "--repo-root",
            str(repo_root),
            "--env-file",
            str(repo_root / "infra/.env.local"),
            "--project-name",
            "nexus-phase3-local",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == "status step=1 code=LOCAL_STACK_STEP_OK exit=0\n"
    assert captured.err == ""
