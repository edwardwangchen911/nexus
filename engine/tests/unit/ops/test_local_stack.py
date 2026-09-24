"""Behavior contracts for the redacted local-stack controller."""

import json
import subprocess
from pathlib import Path

import pytest
from nexus.ops.local_stack import (
    LocalStackAction,
    LocalStackConfigurationError,
    LocalStackStepResult,
    _verified_local_engine_base_image,
    build_local_stack_commands,
    main,
    run_local_stack,
)


def test_local_engine_base_requires_exact_pinned_image_id() -> None:
    image_id = "sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a"

    def matching(argv, **_):
        assert argv[:3] == ("docker", "image", "inspect")
        return subprocess.CompletedProcess(argv, 0, image_id + "\n", "")

    assert _verified_local_engine_base_image(inspector=matching) == "nexus-local-python-base:3.12"

    def missing(argv, **_):
        return subprocess.CompletedProcess(argv, 1, "", "")

    assert _verified_local_engine_base_image(inspector=missing) is None

    def mismatched(argv, **_):
        return subprocess.CompletedProcess(argv, 0, "sha256:" + "0" * 64, "")

    with pytest.raises(LocalStackConfigurationError, match="LOCAL_STACK_BASE_IMAGE_MISMATCH"):
        _verified_local_engine_base_image(inspector=mismatched)


def test_up_passes_selected_base_image_to_compose(repo_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("NEXUS_ENGINE_BASE_IMAGE", "unreviewed-image:latest")
    observed = []

    def runner(argv, **kwargs):
        observed.append(kwargs["env"]["NEXUS_ENGINE_BASE_IMAGE"])
        return subprocess.CompletedProcess(argv, 0, "", "")

    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
        base_image="nexus-local-python-base:3.12",
    )

    assert len(results) == 8
    assert observed == ["nexus-local-python-base:3.12"] * 8


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
    assert [argv[-2:] for argv in rendered[2:5]] == [
        ("build", "object-store-init"),
        ("build", "api"),
        ("build", "worker"),
    ]
    assert [command.timeout_seconds for command in commands[2:5]] == [300, 300, 300]
    assert rendered[5][-3:] == ("run", "--rm", "object-store-init")
    assert "--no-build" not in rendered[5]
    assert "--no-build" not in rendered[6]
    assert rendered[-1][-5:] == ("up", "-d", "--no-build", "api", "worker")


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
    assert [result.fact for result in results] == [
        "compose",
        "host_service",
        "heartbeat",
        "preflight",
        "api_release",
        "worker_release",
    ]
    assert "fixture-secret" not in repr(results)


def test_runner_injects_the_exact_checkout_revision(repo_root: Path) -> None:
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=NEXUS Test",
            "-c",
            "user.email=nexus-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "test fixture",
        ),
        check=True,
    )
    observed_revisions: list[str] = []

    def runner(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        observed_revisions.append(str(environment["NEXUS_RELEASE_REVISION"]))
        if "agent-paper-loop-service.py" in " ".join(argv):
            stdout = '{"status":"running"}'
        elif "--service-status" in argv:
            stdout = json.dumps(
                {
                    "heartbeat": {
                        "status": "fresh",
                        "release_revision": environment["NEXUS_RELEASE_REVISION"],
                    },
                    "preflight": {
                        "status": "ready",
                        "execution_locked": True,
                        "external_requests": 0,
                        "blocking_codes": [],
                        "optional_codes": [],
                    },
                }
            )
        elif "printenv" in argv:
            stdout = str(environment["NEXUS_RELEASE_REVISION"])
        else:
            stdout = ""
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    expected = subprocess.run(
        ("git", "-C", str(repo_root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert all(result.exit_code == 0 for result in results)
    assert observed_revisions and set(observed_revisions) == {expected}


def test_up_starts_host_service_after_compose_only_when_enabled(repo_root: Path) -> None:
    """The host service must never get ahead of the app dependencies it consumes."""
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if "agent-paper-loop-service.py" in " ".join(argv):
            status = "started" if "start" in argv else "stopped"
            return subprocess.CompletedProcess(argv, 0, json.dumps({"status": status}), "private")
        return subprocess.CompletedProcess(argv, 0, "private", "private")

    env_file = repo_root / "infra/.env.local"
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n", encoding="utf-8")
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=env_file,
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert calls[0][2] == "status"
    assert [call[-1] for call in calls[1:9]] == [
        "--quiet",
        "minio",
        "object-store-init",
        "api",
        "worker",
        "object-store-init",
        "head",
        "worker",
    ]
    assert calls[9][1:3] == (
        str(repo_root / "scripts/macos/agent-paper-loop-service.py"),
        "start",
    )
    assert [result.fact for result in results] == ["host_service"] + ["compose"] * 8 + [
        "host_service"
    ]

    calls.clear()
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=false\n", encoding="utf-8")
    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=env_file,
        project_name="nexus-phase3-local",
        runner=runner,
    )
    assert len(calls) == 8
    assert [result.fact for result in results] == ["compose"] * 8


def test_up_rejects_running_host_from_another_release_before_compose(repo_root: Path) -> None:
    """Updating containers must not leave an older active scheduler beside them."""
    env_file = repo_root / "infra/.env.local"
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n", encoding="utf-8")
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "")
        if "--service-status" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps({"heartbeat": {"status": "fresh", "release_revision": "a" * 40}}),
                "",
            )
        pytest.fail("Compose must not run with an older active host")

    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=env_file,
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert len(calls) == 2
    assert [result.fact for result in results] == ["host_service", "heartbeat"]
    assert results[-1].result_code == "AGENT_PAPER_HEARTBEAT_RELEASE_MISMATCH"
    assert results[-1].exit_code == 1


def test_up_repairs_dependencies_when_running_host_release_matches(repo_root: Path) -> None:
    """A blocked preflight must not prevent rebuilding dependencies for the same host."""
    env_file = repo_root / "infra/.env.local"
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n", encoding="utf-8")
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    expected = subprocess.run(
        ("git", "-C", str(repo_root), "rev-parse", "HEAD"),
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "")
        if "--service-status" in argv:
            return subprocess.CompletedProcess(
                argv,
                1,
                json.dumps(
                    {
                        "heartbeat": {"status": "fresh", "release_revision": expected},
                        "preflight": {"status": "blocked"},
                    }
                ),
                "",
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=env_file,
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert results[1].result_code == "AGENT_PAPER_HEARTBEAT_FRESH"
    assert [result.fact for result in results[2:10]] == ["compose"] * 8
    assert calls[-1][2] == "start"


def test_up_rejects_invalid_agent_paper_flag_before_compose(repo_root: Path) -> None:
    """A malformed opt-in value must not be interpreted as either enabled or disabled."""
    env_file = repo_root / "infra/.env.local"
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=sometimes\n", encoding="utf-8")

    with pytest.raises(LocalStackConfigurationError, match="AGENT_PAPER_CYCLE_FLAG_INVALID"):
        run_local_stack(
            LocalStackAction.UP,
            repo_root=repo_root,
            env_file=env_file,
            project_name="nexus-phase3-local",
            runner=lambda *_args, **_kwargs: pytest.fail("invalid config must precede Compose"),
        )


def test_status_reports_four_independent_facts_after_compose_failure(repo_root: Path) -> None:
    """A failed Compose probe must not hide host, heartbeat, or preflight evidence."""
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        if argv[-2:] == ("ps", "-a"):
            return subprocess.CompletedProcess(argv, 19, "private-compose", "private")
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "private")
        payload = {
            "heartbeat": {
                "status": "fresh",
                "seen_at": "2026-09-16T10:00:00+00:00",
                "started_at": "2026-09-16T09:00:00+00:00",
                "process_version": "agent-paper-loop-v1",
                "migration_head": "0059_agent_paper_intraday_cycles",
            },
            "preflight": {
                "status": "blocked",
                "execution_locked": True,
                "external_requests": 0,
                "blocking_codes": ["AGENT_PAPER_CYCLE_DISABLED"],
                "optional_codes": [],
            },
            "execution_locked": True,
        }
        return subprocess.CompletedProcess(argv, 1, json.dumps(payload), "private")

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert [result.fact for result in results] == [
        "compose",
        "host_service",
        "heartbeat",
        "preflight",
    ]
    assert results[0].result_code == "LOCAL_STACK_STEP_FAILED"
    assert results[1].details == (("status", "running"),)
    assert results[2].details[0] == ("status", "fresh")
    assert results[3].details == (
        ("status", "blocked"),
        ("execution_locked", True),
        ("external_requests", 0),
        ("blocking_codes", ("AGENT_PAPER_CYCLE_DISABLED",)),
        ("optional_codes", ()),
    )
    assert calls[0][-2:] == ("ps", "-a")
    assert calls[1][1:3] == (
        str(repo_root / "scripts/macos/agent-paper-loop-service.py"),
        "status",
    )
    assert "--service-status" in calls[2]
    assert "private" not in repr(results)


@pytest.mark.parametrize("heartbeat_status", ("stale", "incompatible"))
def test_status_preserves_nonfresh_heartbeat_classification(
    repo_root: Path, heartbeat_status: str
) -> None:
    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        if argv[-2:] == ("ps", "-a"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "")
        return subprocess.CompletedProcess(
            argv,
            1,
            json.dumps(
                {
                    "heartbeat": {"status": heartbeat_status},
                    "preflight": {
                        "status": "ready",
                        "execution_locked": True,
                        "external_requests": 0,
                        "blocking_codes": [],
                        "optional_codes": [],
                    },
                }
            ),
            "",
        )

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    heartbeat = results[2]
    assert heartbeat.result_code == f"AGENT_PAPER_HEARTBEAT_{heartbeat_status.upper()}"
    assert heartbeat.details == (("status", heartbeat_status),)
    assert heartbeat.exit_code == 1


def test_status_rejects_fresh_heartbeat_from_an_older_release(repo_root: Path) -> None:
    """A live old loop must not make the current checkout look ready for acceptance."""
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    expected = subprocess.run(
        ("git", "-C", str(repo_root), "rev-parse", "HEAD"),
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    reported = "a" * 40 if expected != "a" * 40 else "b" * 40

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        if argv[-2:] == ("ps", "-a"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "heartbeat": {
                        "status": "fresh",
                        "seen_at": "2026-09-23T10:00:00+00:00",
                        "started_at": "2026-09-23T09:00:00+00:00",
                        "process_version": f"agent-paper-loop-v2+{reported}",
                        "release_revision": reported,
                        "migration_head": "0065_portfolio_management",
                    },
                    "preflight": {
                        "status": "ready",
                        "execution_locked": True,
                        "external_requests": 0,
                        "blocking_codes": [],
                        "optional_codes": [],
                    },
                }
            ),
            "",
        )

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    heartbeat = results[2]
    assert heartbeat.result_code == "AGENT_PAPER_HEARTBEAT_RELEASE_MISMATCH"
    assert heartbeat.exit_code == 1
    assert ("expected_release_revision", expected) in heartbeat.details
    assert ("reported_release_revision", reported) in heartbeat.details
    assert results[3].result_code == "AGENT_PAPER_PREFLIGHT_READY"


def test_status_rejects_old_api_while_worker_and_host_match(repo_root: Path) -> None:
    """A healthy heartbeat cannot conceal an API container from an older checkout."""
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    expected = subprocess.run(
        ("git", "-C", str(repo_root), "rev-parse", "HEAD"),
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    old = "a" * 40 if expected != "a" * 40 else "b" * 40

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        if "agent-paper-loop-service.py" in " ".join(argv):
            stdout = '{"status":"running"}'
        elif "--service-status" in argv:
            stdout = json.dumps(
                {
                    "heartbeat": {"status": "fresh", "release_revision": expected},
                    "preflight": {
                        "status": "ready",
                        "execution_locked": True,
                        "external_requests": 0,
                        "blocking_codes": [],
                        "optional_codes": [],
                    },
                }
            )
        elif "printenv" in argv:
            stdout = old if "api" in argv else expected
        else:
            stdout = ""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert results[2].result_code == "AGENT_PAPER_HEARTBEAT_FRESH"
    assert results[4].result_code == "LOCAL_STACK_RELEASE_MISMATCH"
    assert results[4].exit_code == 1
    assert ("reported_release_revision", old) in results[4].details
    assert results[5].result_code == "LOCAL_STACK_RELEASE_MATCHED"


def test_status_continues_when_compose_executable_is_unavailable(repo_root: Path) -> None:
    """An unavailable Compose executable is one failed fact, not a status-wide exception."""
    calls = 0

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileNotFoundError("private-path-canary")
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"stopped"}', "")
        return subprocess.CompletedProcess(
            argv,
            1,
            json.dumps(
                {
                    "heartbeat": {"status": "missing"},
                    "preflight": {
                        "status": "blocked",
                        "execution_locked": True,
                        "external_requests": 0,
                        "blocking_codes": ["AGENT_PAPER_CYCLE_DISABLED"],
                        "optional_codes": [],
                    },
                }
            ),
            "",
        )

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert [result.fact for result in results] == [
        "compose",
        "host_service",
        "heartbeat",
        "preflight",
    ]
    assert results[0].result_code == "LOCAL_STACK_STEP_UNAVAILABLE"
    assert "private-path-canary" not in repr(results)


@pytest.mark.parametrize("attempt", (1, 2))
def test_down_aborts_before_compose_when_host_stop_is_unconfirmed(
    repo_root: Path, attempt: int
) -> None:
    """Repeated ordinary down must preserve dependencies needed to inspect uncertain work."""
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            1,
            '{"error_code":"AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"}',
            "private",
        )

    for _ in range(attempt):
        results = run_local_stack(
            LocalStackAction.DOWN,
            repo_root=repo_root,
            env_file=repo_root / "infra/.env.local",
            project_name="nexus-phase3-local",
            runner=runner,
        )
        assert [result.fact for result in results] == ["host_service"]
        assert results[0].result_code == "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"

    assert all("agent-paper-loop-service.py" in " ".join(call) for call in calls)


@pytest.mark.parametrize(
    "host_output",
    (
        '{"status":"running"}',
        '{"status":"started"}',
        '{"error_code":"AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"}',
        '{"error_code":"AGENT_PAPER_SERVICE_STOP_FAILED"}',
        "not-json",
    ),
)
def test_down_requires_explicit_stopped_status_before_compose(
    repo_root: Path, host_output: str
) -> None:
    """Even exit zero cannot authorize dependency shutdown without explicit stopped proof."""
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, host_output, "private")

    results = run_local_stack(
        LocalStackAction.DOWN,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert len(calls) == 1
    assert [result.fact for result in results] == ["host_service"]


@pytest.mark.parametrize(
    "host_output",
    (
        '{"status":"inactive"}',
        '{"status":"stopped"}',
        '{"error_code":"AGENT_PAPER_SERVICE_START_FAILED"}',
    ),
)
def test_enabled_up_accepts_only_started_or_running_host_status(
    repo_root: Path, host_output: str
) -> None:
    """An exit-zero host response must still prove that start reached a live state."""
    env_file = repo_root / "infra/.env.local"
    env_file.write_text("NEXUS_AGENT_PAPER_CYCLE_ENABLED=true\n", encoding="utf-8")
    subprocess.run(
        (
            "git",
            "-C",
            str(repo_root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        stdout = (
            '{"status":"stopped"}'
            if "agent-paper-loop-service.py" in " ".join(argv) and "status" in argv
            else host_output
            if "agent-paper-loop-service.py" in " ".join(argv)
            else ""
        )
        return subprocess.CompletedProcess(argv, 0, stdout, "private")

    results = run_local_stack(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=env_file,
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert len(results) == 10
    assert results[-1].fact == "host_service"
    assert results[-1].exit_code != 0


def test_down_stops_host_before_exact_compose_project(repo_root: Path) -> None:
    """Confirmed host stop is the only path that permits dependency shutdown."""
    calls: list[tuple[str, ...]] = []

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        stdout = '{"status":"stopped"}' if "agent-paper-loop-service.py" in " ".join(argv) else ""
        return subprocess.CompletedProcess(argv, 0, stdout, "private")

    results = run_local_stack(
        LocalStackAction.DOWN,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert calls[0][1:3] == (
        str(repo_root / "scripts/macos/agent-paper-loop-service.py"),
        "stop",
    )
    assert calls[1][-3:] == ("stop", "api", "worker")
    assert calls[2][-1] == "down"
    assert [result.fact for result in results] == ["host_service", "compose", "compose"]


@pytest.mark.parametrize(
    ("locked", "external"),
    ((False, 0), (True, False), (True, 1), (True, -1)),
)
def test_status_rejects_non_fail_closed_preflight_shapes(
    repo_root: Path, locked: bool, external: object
) -> None:
    """Only locked, exact-zero external-request evidence may be accepted as preflight."""

    def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        if argv[-2:] == ("ps", "-a"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "agent-paper-loop-service.py" in " ".join(argv):
            return subprocess.CompletedProcess(argv, 0, '{"status":"running"}', "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "heartbeat": {"status": "fresh"},
                    "preflight": {
                        "status": "ready",
                        "execution_locked": locked,
                        "external_requests": external,
                        "blocking_codes": [],
                        "optional_codes": [],
                    },
                }
            ),
            "",
        )

    results = run_local_stack(
        LocalStackAction.STATUS,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
        runner=runner,
    )

    assert [result.fact for result in results] == [
        "compose",
        "host_service",
        "heartbeat",
        "preflight",
        "api_release",
        "worker_release",
    ]
    assert results[3].result_code == "AGENT_PAPER_PREFLIGHT_OUTPUT_INVALID"
    assert results[3].details == ()
    assert results[3].exit_code == 1


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
