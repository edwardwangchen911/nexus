"""Synthetic regression cases; real recovery copies never enter tests."""

import asyncio
import hashlib
import json
from asyncio import CancelledError
from threading import Event
from types import SimpleNamespace

import pytest
from nexus.ops.local_recovery import (
    LocalRecovery,
    RecoveryError,
    collect_bindings,
    read_backup,
    validate_target,
    verify_object,
    write_private,
)


@pytest.mark.parametrize(
    "target,relations,schemas",
    [
        ("postgresql://u:p@remote.example:5555/nexus_recovery_abc", 0, []),
        ("postgresql://u:p@localhost:5432/another_database", 0, []),
        ("postgresql://u:p@127.0.0.1:5555/nexus_recovery_abc", 1, []),
        ("postgresql://u:p@127.0.0.1:5555/nexus_recovery_abc", 0, ["foreign"]),
    ],
)
def test_restore_refuses_shared_remote_or_nonempty_target(target, relations, schemas):
    with pytest.raises(RecoveryError):
        validate_target("postgresql://u:p@127.0.0.1:5432/source", target, relations, schemas)


def test_backup_corruption_is_rejected_before_any_restore(tmp_path):
    dump = b"synthetic custom archive"
    (tmp_path / "database.dump").write_bytes(dump)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema": "0033_cash_flow_business_date",
                "bytes": len(dump),
                "dump_sha256": hashlib.sha256(dump).hexdigest(),
                "before": {
                    table: {"count": 0, "sha256": hashlib.sha256(b"").hexdigest()}
                    for table in (
                        "provider_capabilities",
                        "portfolio_snapshots",
                        "price_series_manifests",
                        "price_bar_records",
                    )
                },
            }
        )
    )
    assert read_backup(tmp_path)["bytes"] == len(dump)
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    del manifest["before"]["price_bar_records"]
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RecoveryError, match="MANIFEST_INVALID"):
        read_backup(tmp_path)
    (tmp_path / "database.dump").write_bytes(b"corrupt")
    with pytest.raises(RecoveryError, match="BACKUP_DIGEST"):
        read_backup(tmp_path)


def test_all_nested_references_need_a_consistent_database_binding():
    uri = "s3://synthetic/provider/market/raw/sample?versionId=version1"
    binding = {"uri": uri, "sha256": hashlib.sha256(b"abc").hexdigest(), "byte_size": 3}
    refs = [
        {"payload": {"citation": uri}},
        {"location": uri, "content_hash": binding["sha256"]},
        binding,
    ]
    assert set(collect_bindings(refs)) == {uri}
    with pytest.raises(RecoveryError, match="UNBOUND"):
        collect_bindings(refs + [{"location": "s3://synthetic/missing?versionId=v"}])
    with pytest.raises(RecoveryError, match="CONFLICT"):
        collect_bindings(refs + [{**binding, "byte_size": 4}])
    with pytest.raises(RecoveryError, match="CONFLICT"):
        collect_bindings(refs + [{"location": uri, "content_hash": "0" * 64}])


@pytest.mark.parametrize("payload,version", [(b"abd", "v1"), (b"abc", "v2"), (b"abcd", "v1")])
def test_object_version_hash_and_length_must_all_match(payload, version):
    with pytest.raises(RecoveryError, match="OBJECT_MISMATCH"):
        verify_object(payload, version, "v1", hashlib.sha256(b"abc").hexdigest(), 3)


def test_private_output_cannot_overwrite_or_follow_a_symlink(tmp_path):
    target = tmp_path / "existing"
    target.write_text("keep")
    with pytest.raises(FileExistsError):
        write_private(target, b"replace")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(FileExistsError):
        write_private(link, b"replace")
    assert target.read_text() == "keep"
    new = tmp_path / "new"
    write_private(new, b"private")
    assert new.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_cleanup_refuses_foreign_container_even_with_same_name(tmp_path, monkeypatch):
    recovery = LocalRecovery(tmp_path, tmp_path)
    recovery.owns_target = True
    commands = []

    async def command(*args, **kwargs):
        commands.append(args)
        return b"container-id" if args[0] == "ps" else b"another-owner"

    monkeypatch.setattr(recovery, "command", command)
    with pytest.raises(RecoveryError, match="OWNERSHIP"):
        await recovery.cleanup()
    assert not any(cmd[0] == "rm" for cmd in commands)


@pytest.mark.asyncio
async def test_remote_docker_is_rejected_before_any_resource_operation(tmp_path, monkeypatch):
    recovery = LocalRecovery(tmp_path, tmp_path)
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote.example:2376")

    async def unexpected(*args, **kwargs):
        pytest.fail("remote target must be refused before Docker is called")

    monkeypatch.setattr(recovery, "command", unexpected)
    with pytest.raises(RecoveryError, match="LOCAL_DOCKER"):
        await recovery.start()


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("synthetic private credential and account data"),
        BaseExceptionGroup(
            "interrupted cleanup",
            [CancelledError(), RuntimeError("synthetic private credential and account data")],
        ),
    ],
)
def test_cli_does_not_print_secret_bearing_exception(tmp_path, monkeypatch, capsys, failure):
    from nexus.ops import local_recovery

    async def failed(self):
        raise failure

    monkeypatch.setattr(LocalRecovery, "execute", failed)
    monkeypatch.setattr(
        "sys.argv", ["recovery", "--backup", str(tmp_path), "--repo-root", str(tmp_path)]
    )
    assert local_recovery.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "failed", "error": "LOCAL_RECOVERY_FAILED"}


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_first_restore_failure_stops_objects_and_still_cleans_owned_target(
    tmp_path, monkeypatch, cleanup_fails
):
    from nexus.ops import local_recovery

    tmp_path.chmod(0o700)
    write_private(tmp_path / "database.dump", b"synthetic")
    write_private(tmp_path / "manifest.json", b"{}")
    monkeypatch.setattr(local_recovery, "read_backup", lambda _: {"schema": "synthetic"})
    recovery = LocalRecovery(tmp_path, tmp_path)
    events = []

    async def start():
        events.append("start")
        return "unused synthetic target"

    async def restore(*args):
        events.append("restore")
        raise RecoveryError("RECOVERY_SYNTHETIC_RESTORE_FAILED")

    async def objects(*args):
        pytest.fail("first restore failure must prevent object reads")

    async def cleanup():
        events.append("cleanup")
        if cleanup_fails:
            raise RecoveryError("RECOVERY_SYNTHETIC_CLEANUP_FAILED")

    monkeypatch.setattr(recovery, "start", start)
    monkeypatch.setattr(recovery, "restore_database", restore)
    monkeypatch.setattr(recovery, "recover_objects", objects)
    monkeypatch.setattr(recovery, "cleanup", cleanup)
    with pytest.raises(BaseExceptionGroup if cleanup_fails else RecoveryError) as error:
        await recovery.execute()
    if cleanup_fails:
        assert len(error.value.exceptions) == 2
        assert "RESTORE_FAILED" in str(error.value.exceptions[0])
        assert "CLEANUP_FAILED" in str(error.value.exceptions[1])
    assert events == ["start", "restore", "cleanup"]
    report = json.loads((recovery.output / "result.json").read_bytes())
    assert report["status"] == ("cleanup_failed" if cleanup_fails else "failed")
    assert not report.get("object_bindings_verified", False)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_twice", [False, True])
async def test_cancelled_create_finishes_before_owner_cleanup(tmp_path, monkeypatch, cancel_twice):
    from nexus.ops.local_backup import LocalBackup

    backup = LocalBackup(tmp_path / "backup", tmp_path)
    loop = asyncio.get_running_loop()
    create_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_create = Event()
    events = []
    created = False

    def run(command, **kwargs):
        nonlocal created
        action = command[1]
        output = b""
        if action == "create":
            events.append("create_started")
            loop.call_soon_threadsafe(create_started.set)
            assert release_create.wait(timeout=3)
            created = True
            events.append("create_finished")
        elif action == "ps":
            output = b"synthetic-owned-container" if created else b""
        elif action == "inspect":
            output = backup.run_id.encode()
        elif action == "rm":
            created = False
            events.append("removed")
        return SimpleNamespace(returncode=0, stdout=output)

    async def local_docker():
        pass

    original_cleanup = backup.cleanup

    async def cleanup():
        events.append("cleanup_started")
        cleanup_started.set()
        await original_cleanup()

    monkeypatch.setattr("nexus.ops.local_recovery.subprocess.run", run)
    monkeypatch.setattr(backup, "local_docker", local_docker)
    monkeypatch.setattr(backup, "cleanup", cleanup)
    operation = asyncio.create_task(backup.execute())
    try:
        await asyncio.wait_for(create_started.wait(), timeout=2)
        operation.cancel()
        await asyncio.sleep(0)
        if cancel_twice:
            operation.cancel()
            await asyncio.sleep(0)
        assert not cleanup_started.is_set()
    finally:
        release_create.set()
        with pytest.raises(CancelledError):
            await asyncio.wait_for(operation, timeout=2)
    assert events == ["create_started", "create_finished", "cleanup_started", "removed"]
    assert created is False
    assert not (backup.backup / "complete.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_twice", [False, True])
async def test_restore_cancel_waits_for_issued_transaction_before_cleanup(
    tmp_path, monkeypatch, cancel_twice
):
    from nexus.ops import local_recovery

    tmp_path.chmod(0o700)
    write_private(tmp_path / "database.dump", b"synthetic")
    write_private(tmp_path / "manifest.json", b"{}")
    monkeypatch.setattr(
        local_recovery,
        "read_backup",
        lambda _: {
            "schema": "0107_platform_price_renewal",
            "format": "nexus_local_backup_v1",
            "source_port": 55439,
        },
    )
    recovery = LocalRecovery(tmp_path, tmp_path)
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = Event()
    events = []

    def run(args, **kwargs):
        if "psql" in args:
            events.append("transaction_started")
            loop.call_soon_threadsafe(started.set)
            assert release.wait(timeout=5)
            events.append("transaction_finished")
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    async def start():
        return "synthetic target"

    async def restore(*args):
        await recovery.restore_archive("0107_platform_price_renewal", complete_backup=True)

    async def cleanup():
        assert events == ["transaction_started", "transaction_finished"]
        events.append("cleanup")

    async def forbidden(*args):
        pytest.fail("cancelled database restoration must not start object recovery")

    monkeypatch.setattr(local_recovery.subprocess, "run", run)
    monkeypatch.setattr(recovery, "start", start)
    monkeypatch.setattr(recovery, "restore_database", restore)
    monkeypatch.setattr(recovery, "cleanup", cleanup)
    monkeypatch.setattr(recovery, "recover_local_objects", forbidden)
    task = asyncio.create_task(recovery.execute())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel("restore-stop")
        await asyncio.sleep(0)
        if cancel_twice:
            task.cancel("second-cancel")
            await asyncio.sleep(0)
        assert events == ["transaction_started"]
    finally:
        release.set()
        with pytest.raises(CancelledError) as cancelled:
            await asyncio.wait_for(task, timeout=5)
    assert cancelled.value.args == ("restore-stop",)
    assert events == ["transaction_started", "transaction_finished", "cleanup"]
    result = json.loads((recovery.output / "result.json").read_text())
    assert result["status"] == "failed" and result["target_residue"] == 0
    assert not result.get("database_restored", False)
