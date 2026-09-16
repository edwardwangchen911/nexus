"""Safety contracts for the fixture-only backup/restore drill."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn

import pytest
from nexus.ops.backup_restore import (
    VERIFICATION_TABLES,
    DrillTargets,
    LocalDockerDrillOperations,
    execute_drill,
    main,
    minio_container_command,
    pg_dump_command,
    pg_restore_command,
    postgres_container_command,
    resolve_single_migration_head,
    verify_manifest_object_binding,
)

_RUN_ID = "0123456789abcdef0123456789abcdef"


def test_targets_are_random_run_scoped_and_never_broad() -> None:
    targets = DrillTargets.from_run_id(_RUN_ID)

    assert targets.source_database == f"nexus_drill_source_{_RUN_ID}"
    assert targets.restore_database == f"nexus_drill_restore_{_RUN_ID}"
    assert targets.source_prefix == f"drills/{_RUN_ID}/source/"
    assert targets.backup_prefix == f"drills/{_RUN_ID}/backup/"
    assert targets.restore_prefix == f"drills/{_RUN_ID}/restore/"
    assert targets.dump_path == f"/tmp/nexus_drill_{_RUN_ID}.dump"
    assert targets.postgres_container == f"nexus-drill-postgres-{_RUN_ID}"
    assert targets.minio_container == f"nexus-drill-minio-{_RUN_ID}"

    for rejected in ("", "0" * 31, "0" * 33, "ABCDEF" * 5 + "AB", "../escape"):
        with pytest.raises(ValueError, match="run ID"):
            DrillTargets.from_run_id(rejected)


def test_pg_commands_are_exact_and_never_contain_credentials() -> None:
    targets = DrillTargets.from_run_id(_RUN_ID)

    dump = pg_dump_command(targets, database_user="nexus_drill")
    restore = pg_restore_command(targets, database_user="nexus_drill")

    assert dump == (
        "docker",
        "exec",
        targets.postgres_container,
        "pg_dump",
        "--username",
        "nexus_drill",
        "--dbname",
        targets.source_database,
        "--file",
        targets.dump_path,
        "--format=custom",
        "--no-owner",
        "--no-privileges",
    )
    assert restore == (
        "docker",
        "exec",
        targets.postgres_container,
        "pg_restore",
        "--username",
        "nexus_drill",
        "--dbname",
        targets.restore_database,
        "--exit-on-error",
        "--single-transaction",
        targets.dump_path,
    )
    assert "password" not in " ".join((*dump, *restore)).lower()


def test_disposable_containers_use_dynamic_loopback_ports_and_tmpfs_only() -> None:
    targets = DrillTargets.from_run_id(_RUN_ID)
    postgres = postgres_container_command(targets)
    minio = minio_container_command(targets)

    assert "127.0.0.1::5432" in postgres
    assert "127.0.0.1::9000" in minio
    assert "/var/lib/postgresql/data:rw,noexec,nosuid,size=1g" in postgres
    assert "/data:rw,noexec,nosuid,size=256m" in minio
    assert "--tmpfs" in postgres and "--tmpfs" in minio
    assert "--volume" not in postgres and "--volume" not in minio
    assert "-v" not in postgres and "-v" not in minio
    ownership_label = f"nexus.drill.run_id={_RUN_ID}"
    assert ownership_label in postgres
    assert ownership_label in minio


@pytest.mark.asyncio
async def test_start_rejects_a_preexisting_container_before_any_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = LocalDockerDrillOperations()
    targets = DrillTargets.from_run_id(_RUN_ID)
    commands: list[tuple[str, ...]] = []

    async def _existing(command: tuple[str, ...], **_kwargs: object):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "existing-id\n", "")

    monkeypatch.setattr(operations, "_run", _existing)

    with pytest.raises(RuntimeError, match="already exists"):
        await operations.start(targets)

    assert commands == [
        (
            "docker",
            "inspect",
            "--format",
            "{{.Id}}",
            targets.postgres_container,
        )
    ]
    assert all("run" not in command for command in commands)


@pytest.mark.asyncio
async def test_cleanup_refuses_to_remove_a_container_without_its_run_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = LocalDockerDrillOperations()
    targets = DrillTargets.from_run_id(_RUN_ID)
    commands: list[tuple[str, ...]] = []

    async def _foreign(command: tuple[str, ...], **_kwargs: object):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "another-run-id\n", "")

    monkeypatch.setattr(operations, "_run", _foreign)

    with pytest.raises(RuntimeError, match="ownership"):
        await operations._remove_owned_container(  # noqa: SLF001
            targets.postgres_container,
            targets.run_id,
        )

    assert len(commands) == 1
    assert "rm" not in commands[0]


@pytest.mark.asyncio
async def test_every_subprocess_has_a_bounded_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = LocalDockerDrillOperations()
    captured: dict[str, object] = {}

    def _completed(command: tuple[str, ...], **kwargs: object):
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("nexus.ops.backup_restore.subprocess.run", _completed)

    await operations._run(("docker", "version"))  # noqa: SLF001

    assert captured["timeout"] == 30


def test_verification_set_covers_authority_roots_and_balanced_ledger() -> None:
    assert VERIFICATION_TABLES == (
        "paper_accounts",
        "agent_version_evaluations",
        "agent_version_cycle_pairs",
        "agent_version_evaluation_decisions",
        "paper_ledger_transactions",
        "paper_ledger_entries",
        "paper_performance_snapshots",
        "paper_performance_attributions",
        "portfolio_snapshots",
        "audit_events",
        "audit_chain_manifests",
    )


def test_drill_resolves_the_configured_alembic_head_instead_of_a_stale_revision(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "migrations"
    versions = migrations / "versions"
    versions.mkdir(parents=True)
    (versions / "0999_test_head.py").write_text(
        "revision = '0999_test_head'\ndown_revision = None\n",
        encoding="utf-8",
    )
    config = tmp_path / "alembic.ini"
    config.write_text(
        f"[alembic]\nscript_location = {migrations}\n",
        encoding="utf-8",
    )

    assert resolve_single_migration_head(config) == "0999_test_head"


def test_manifest_database_record_must_bind_to_the_restored_object() -> None:
    relative = "audit/2026-08-27/manifest.json"
    manifest = (
        "f" * 64,
        "a" * 64,
        245,
        f"s3://nexus-drill-{_RUN_ID}/drills/{_RUN_ID}/source/{relative}",
    )

    verify_manifest_object_binding(
        manifest,
        bucket=f"nexus-drill-{_RUN_ID}",
        source_prefix=f"drills/{_RUN_ID}/source/",
        object_hashes={relative: (245, "a" * 64)},
    )

    with pytest.raises(RuntimeError, match="manifest object binding"):
        verify_manifest_object_binding(
            manifest,
            bucket=f"nexus-drill-{_RUN_ID}",
            source_prefix=f"drills/{_RUN_ID}/source/",
            object_hashes={relative: (244, "a" * 64)},
        )

    with pytest.raises(RuntimeError, match="manifest object binding"):
        verify_manifest_object_binding(
            (*manifest[:3], "s3://another-bucket/escaped"),
            bucket=f"nexus-drill-{_RUN_ID}",
            source_prefix=f"drills/{_RUN_ID}/source/",
            object_hashes={relative: (245, "a" * 64)},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_phase",
    ("start", "create", "seed", "backup", "restore", "verify"),
)
async def test_every_failure_path_runs_exact_cleanup(failure_phase: str) -> None:
    operations = _FailingOperations(failure_phase)
    targets = DrillTargets.from_run_id(_RUN_ID)

    with pytest.raises(RuntimeError, match=f"injected {failure_phase} failure"):
        await execute_drill(operations, targets)

    assert operations.cleanup_calls == [targets]
    assert operations.calls[-1] == "cleanup"


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_hide_the_primary_drill_failure() -> None:
    operations = _FailingOperations("backup", cleanup_failure=True)
    targets = DrillTargets.from_run_id(_RUN_ID)

    with pytest.raises(BaseExceptionGroup) as captured:
        await execute_drill(operations, targets)

    messages = [str(error) for error in captured.value.exceptions]
    assert messages == ["injected backup failure", "injected cleanup failure"]


@pytest.mark.asyncio
async def test_local_drill_refuses_production_before_any_container_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEXUS_ENV", "production")
    operations = LocalDockerDrillOperations()

    with pytest.raises(RuntimeError, match="forbidden in production"):
        await operations.start(DrillTargets.from_run_id(_RUN_ID))


def test_cli_failure_is_generic_and_never_prints_exception_detail(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def _fail() -> dict[str, object]:
        raise RuntimeError("private-database-and-object-detail")

    monkeypatch.setattr("nexus.ops.backup_restore.run_local_drill", _fail)

    assert main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "backup/restore drill failed\n"
    assert "private-database-and-object-detail" not in output.err


@dataclass
class _FailingOperations:
    failure_phase: str
    cleanup_failure: bool = False
    calls: list[str] = field(default_factory=list)
    cleanup_calls: list[DrillTargets] = field(default_factory=list)

    async def start(self, _targets: DrillTargets) -> None:
        self._record("start")

    async def create(self, _targets: DrillTargets) -> None:
        self._record("create")

    async def seed(self, _targets: DrillTargets) -> None:
        self._record("seed")

    async def backup(self, _targets: DrillTargets) -> None:
        self._record("backup")

    async def restore(self, _targets: DrillTargets) -> None:
        self._record("restore")

    async def verify(self, _targets: DrillTargets) -> dict[str, object]:
        self._record("verify")
        return {"status": "verified"}

    async def cleanup(self, targets: DrillTargets) -> None:
        self.calls.append("cleanup")
        self.cleanup_calls.append(targets)
        if self.cleanup_failure:
            raise RuntimeError("injected cleanup failure")

    def _record(self, phase: str) -> None:
        self.calls.append(phase)
        if phase == self.failure_phase:
            self._raise(phase)

    @staticmethod
    def _raise(phase: str) -> NoReturn:
        raise RuntimeError(f"injected {phase} failure")
