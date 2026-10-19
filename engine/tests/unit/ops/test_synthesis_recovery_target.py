"""Isolation routes the trusted inspector without weakening producer ownership."""

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import manual_paper


def test_recovery_owner_arguments_are_paired_and_synthesis_only():
    ids = ["--cycle-id", str(uuid4()), "--run-id", str(uuid4())]
    args = manual_paper.parser().parse_args(
        [
            "check-synthesis-acceptance",
            *ids,
            "--recovery-producer-repo-root",
            "/owner",
            "--recovery-producer-env-file",
            "infra/.env.local",
        ]
    )
    assert args.recovery_producer_repo_root == Path("/owner")
    assert args.recovery_producer_env_file == Path("infra/.env.local")
    with pytest.raises(SystemExit):
        manual_paper.parser().parse_args(
            ["read", "--request-id", str(uuid4()), "--recovery-producer-repo-root", "/owner"]
        )


def test_trusted_inspector_receives_original_owner_paths(tmp_path, monkeypatch):
    from nexus.paper import manual_preparation_close as close

    candidate, owner = tmp_path / "candidate", tmp_path / "owner"
    commands = []
    monkeypatch.setattr(close, "require_preparation_inspector_source", lambda *_a: None)
    monkeypatch.setattr(
        close.subprocess,
        "run",
        lambda command, **kw: (
            commands.append(command)
            or SimpleNamespace(returncode=0, stdout='{"status":"inactive"}')
        ),
    )
    calls = []
    monkeypatch.setattr(close, "require_research_processes_quiescent", lambda: calls.append(True))
    close.require_preparation_producers_stopped(
        owner, owner / "infra/.env.local", inspector_repo_root=candidate
    )
    assert commands[0][1] == str(candidate / "scripts/macos/agent-paper-loop-service.py")
    assert commands[0][commands[0].index("--repo-root") + 1] == str(owner)
    assert commands[0][commands[0].index("--env-file") + 1] == str(owner / "infra/.env.local")
    assert calls == [True]


def _recovery_fixture(
    tmp_path, monkeypatch, *, target_name="nexus_recovery_case", source_name="nexus"
):
    from nexus.ops import synthesis_recovery_target as recovery
    from nexus.settings import EngineSettings

    owner = tmp_path / "owner"
    owner.mkdir()
    env = owner / "source.env"
    env.write_text(f'DATABASE_URL="postgresql+asyncpg://test:test@127.0.0.1:5432/{source_name}"\n')
    env.chmod(0o600)
    monkeypatch.setattr(recovery, "_validate_local_env", lambda root, path: path.resolve())
    args = SimpleNamespace(recovery_producer_repo_root=owner, recovery_producer_env_file=env)
    settings = EngineSettings(
        DATABASE_URL=f"postgresql+asyncpg://test:test@localhost:5432/{target_name}"
    )
    return recovery, args, settings, env


@pytest.mark.asyncio
async def test_isolation_requires_actual_database_and_rechecks_config(tmp_path, monkeypatch):
    recovery, args, settings, env = _recovery_fixture(tmp_path, monkeypatch)
    owner = recovery.load_recovery_producer(settings, args)

    class Session:
        async def scalar(self, statement):
            assert str(statement) == "SELECT current_database()"
            return "nexus_recovery_case"

    await owner.verify(Session())
    env.write_text(env.read_text() + "# changed during maintenance\n")
    with pytest.raises(ValueError):
        await owner.verify(Session())


@pytest.mark.parametrize(
    "target,source",
    [
        ("nexus", "source"),
        ("nexus_recovery_case", "nexus_recovery_case"),
        ("nexus_recovery_", "source"),
    ],
)
def test_recovery_target_rejects_formal_or_same_database(tmp_path, monkeypatch, target, source):
    recovery, args, settings, _ = _recovery_fixture(
        tmp_path, monkeypatch, target_name=target, source_name=source
    )
    with pytest.raises(ValueError):
        recovery.load_recovery_producer(settings, args)


@pytest.mark.parametrize(
    "query",
    ["host=remote", "hostaddr=remote", "port=1", "dsn=remote", "database=other", "dbname=other"],
)
def test_recovery_target_rejects_endpoint_overrides(tmp_path, monkeypatch, query):
    recovery, args, settings, _ = _recovery_fixture(tmp_path, monkeypatch)
    settings = settings.model_copy(update={"database_url": settings.database_url + "?" + query})
    with pytest.raises(ValueError):
        recovery.load_recovery_producer(settings, args)


@pytest.mark.asyncio
async def test_recovery_target_rejects_different_actual_database(tmp_path, monkeypatch):
    recovery, args, settings, _ = _recovery_fixture(tmp_path, monkeypatch)
    owner = recovery.load_recovery_producer(settings, args)

    class Session:
        async def scalar(self, statement):
            return "nexus"

    with pytest.raises(ValueError):
        await owner.verify(Session())


def test_recovery_pair_and_default(tmp_path, monkeypatch):
    recovery, args, settings, _ = _recovery_fixture(tmp_path, monkeypatch)
    args.recovery_producer_env_file = None
    with pytest.raises(ValueError):
        recovery.load_recovery_producer(settings, args)
    args.recovery_producer_repo_root = None
    assert recovery.load_recovery_producer(settings, args) is None


@pytest.mark.parametrize(
    "state,exit_code", [("running", 0), ("stopped", 0), ("unknown", 0), ("inactive", 1)]
)
def test_alternate_inspector_keeps_host_rejections(tmp_path, monkeypatch, state, exit_code):
    from nexus.paper import manual_preparation_close as close

    monkeypatch.setattr(close, "require_preparation_inspector_source", lambda *_a: None)
    monkeypatch.setattr(
        close.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=exit_code, stdout=__import__("json").dumps({"status": state})
        ),
    )
    with pytest.raises(close.PreparationCloseDiagnosticError):
        close.require_preparation_producers_stopped(
            tmp_path, tmp_path / "owner.env", inspector_repo_root=tmp_path / "candidate"
        )


def test_alternate_inspector_keeps_research_process_gate(tmp_path, monkeypatch):
    from nexus.paper import manual_preparation_close as close

    monkeypatch.setattr(close, "require_preparation_inspector_source", lambda *_a: None)
    monkeypatch.setattr(
        close.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(returncode=0, stdout='{"status":"inactive"}'),
    )

    def busy():
        raise ValueError("busy")

    monkeypatch.setattr(close, "require_research_processes_quiescent", busy)
    with pytest.raises(close.PreparationCloseDiagnosticError):
        close.require_preparation_producers_stopped(
            tmp_path, tmp_path / "owner.env", inspector_repo_root=tmp_path / "candidate"
        )


@pytest.mark.parametrize("mode", [0o644, 0o666])
def test_source_configuration_must_remain_private(tmp_path, monkeypatch, mode):
    recovery, args, settings, env = _recovery_fixture(tmp_path, monkeypatch)
    env.chmod(mode)
    with pytest.raises(ValueError):
        recovery.load_recovery_producer(settings, args)


def test_inspector_loading_failure_has_safe_diagnostic(tmp_path, monkeypatch):
    from nexus.paper import manual_preparation_close as close

    monkeypatch.setattr(
        close, "require_preparation_inspector_source", lambda *_a: tmp_path / "script"
    )

    def fail(*_a):
        raise OSError("private path must not be exposed")

    monkeypatch.setattr(close.runpy, "run_path", fail)
    with pytest.raises(close.PreparationCloseDiagnosticError) as caught:
        close.preparation_service_control_lock(tmp_path, "pinned")
    assert caught.value.stage == "service_control_source"
    assert caught.value.code == "INSPECTION_FAILED"


def test_inspector_rejects_changed_source_at_pinned_revision(tmp_path):
    import subprocess

    from nexus.paper import manual_preparation_close as close

    script = tmp_path / "scripts/macos/agent-paper-loop-service.py"
    script.parent.mkdir(parents=True)
    script.write_text("# trusted inspector\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    revision = subprocess.check_output(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], text=True
    ).strip()
    assert close.require_preparation_inspector_source(tmp_path, revision) == script
    script.write_text("# changed inspector\n")
    with pytest.raises(close.PreparationCloseDiagnosticError) as caught:
        close.require_preparation_inspector_source(tmp_path, revision)
    assert caught.value.code == "SOURCE_IDENTITY_INVALID"
