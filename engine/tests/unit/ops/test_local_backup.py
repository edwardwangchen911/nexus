"""Only synthetic backup files and sources enter these tests."""

import hashlib
import io
import json
from asyncio import CancelledError
from types import SimpleNamespace

import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.local_backup import LocalBackup
from nexus.ops.local_recovery import (
    LocalRecovery,
    RecoveryError,
    collect_bindings,
    read_backup,
    write_private,
)

CONTENT = b"synthetic immutable evidence"
URI = "s3://synthetic/provider/market/item?versionId=v1"
ROW = {"uri": URI, "sha256": hashlib.sha256(CONTENT).hexdigest(), "byte_size": len(CONTENT)}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "cancel", "cleanup", "cancel_cleanup"])
async def test_complete_marker_requires_objects_and_successful_cleanup(
    tmp_path, monkeypatch, failure
):
    backup = LocalBackup(tmp_path / "new-backup", tmp_path)
    events = []

    async def start():
        events.append("start")
        return "synthetic target"

    async def capture():
        write_private(backup.backup / "database.dump", b"synthetic dump")
        return {"schema": "synthetic", "before": {}}, [ROW]

    async def objects(rows):
        events.append("objects")
        if failure == "missing":
            raise RecoveryError("RECOVERY_OBJECT_MISMATCH")
        if failure in {"cancel", "cancel_cleanup"}:
            raise CancelledError()
        (backup.backup / "objects").mkdir(mode=0o700)
        write_private(backup.backup / "objects" / "000000.bin", CONTENT)
        write_private(
            backup.backup / "object-bindings.json",
            json.dumps([{**ROW, "file": "000000.bin"}]).encode(),
        )
        return 1

    async def cleanup():
        events.append("cleanup")
        if failure in {"cleanup", "cancel_cleanup"}:
            raise RecoveryError("RECOVERY_CLEANUP_INCOMPLETE")

    monkeypatch.setattr(backup, "start", start)
    monkeypatch.setattr(backup, "capture_database", capture)
    monkeypatch.setattr(backup, "recover_objects", objects)
    monkeypatch.setattr(backup, "cleanup", cleanup)
    if failure:
        with pytest.raises((RecoveryError, CancelledError, BaseExceptionGroup)) as error:
            await backup.execute()
        if failure == "cancel_cleanup":
            assert len(error.value.exceptions) == 2
            assert isinstance(error.value.exceptions[0], CancelledError)
        assert not (backup.backup / "complete.json").exists()
    else:
        assert (await backup.execute())["status"] == "complete"
        assert (backup.backup / "complete.json").exists()
    assert events[-1] == "cleanup"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "version", "hash"])
async def test_source_object_mismatch_prevents_complete_backup(tmp_path, monkeypatch, failure):
    from nexus.ops import local_backup, local_recovery

    environment = {
        "OBJECT_STORE_ENDPOINT": "http://localhost:59999",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "synthetic",
        "OBJECT_STORE_ACCESS_KEY_ID": "synthetic",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic",
    }
    monkeypatch.setattr(local_backup, "load_local_environment", lambda *a: environment)
    calls = []

    def get_object(**request):
        calls.append(request)
        if failure == "missing":
            raise RuntimeError("synthetic missing source object")
        return {
            "VersionId": "v2" if failure == "version" else "v1",
            "Body": io.BytesIO(b"wrong" if failure == "hash" else CONTENT),
        }

    monkeypatch.setattr(
        local_recovery,
        "S3ProviderEvidenceStore",
        lambda settings: S3ProviderEvidenceStore(
            settings, client=SimpleNamespace(get_object=get_object)
        ),
    )
    backup = LocalBackup(tmp_path / "bundle", tmp_path)

    async def start():
        return "synthetic target"

    async def capture():
        return {"schema": "synthetic"}, [ROW]

    async def cleanup():
        calls.append("cleanup")

    monkeypatch.setattr(backup, "start", start)
    monkeypatch.setattr(backup, "capture_database", capture)
    monkeypatch.setattr(backup, "cleanup", cleanup)
    with pytest.raises(RuntimeError):
        await backup.execute()
    assert len(calls) == 2 and calls[-1] == "cleanup"
    assert not (backup.backup / "complete.json").exists()


@pytest.mark.asyncio
async def test_existing_backup_cannot_be_overwritten(tmp_path):
    write_private(tmp_path / "keep", b"synthetic existing complete backup")
    with pytest.raises(FileExistsError):
        await LocalBackup(tmp_path, tmp_path).execute()
    assert (tmp_path / "keep").read_bytes() == b"synthetic existing complete backup"


@pytest.mark.parametrize("missing", ["complete.json", "objects_sha256", "format", "object_count"])
def test_incomplete_bundle_never_falls_back_to_source_store(tmp_path, missing):
    write_private(tmp_path / "object-bindings.json", b"[]")
    manifest = {
        "format": "nexus_local_backup_v1",
        "object_count": 0,
        "objects_sha256": hashlib.sha256(b"[]").hexdigest(),
        "source_port": 55433,
    }
    if missing != "complete.json":
        manifest.pop(missing)
    encoded = json.dumps(manifest).encode()
    write_private(tmp_path / "manifest.json", encoded)
    if missing != "complete.json":
        write_private(
            tmp_path / "complete.json",
            json.dumps({"manifest_sha256": hashlib.sha256(encoded).hexdigest()}).encode(),
        )
    with pytest.raises(RecoveryError):
        read_backup(tmp_path)


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("synthetic secret"),
        BaseExceptionGroup("synthetic", [CancelledError(), RuntimeError("synthetic secret")]),
    ],
)
def test_backup_cli_never_prints_underlying_failure(tmp_path, monkeypatch, capsys, failure):
    from nexus.ops import local_backup

    async def fail(self):
        raise failure

    monkeypatch.setattr(LocalBackup, "execute", fail)
    monkeypatch.setattr(
        "sys.argv", ["backup", "--destination", str(tmp_path), "--repo-root", str(tmp_path)]
    )
    assert local_backup.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "failed", "error": "LOCAL_BACKUP_FAILED"}


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", [None, "missing", "version", "hash", "omitted", "path"])
async def test_offline_restore_uses_exact_db_reference_set_without_source_access(
    tmp_path,
    monkeypatch,
    corruption,
):
    recovery = LocalRecovery(tmp_path, tmp_path)
    recovery.output = tmp_path / "restored"
    recovery.output.mkdir(mode=0o700)
    (tmp_path / "objects").mkdir(mode=0o700)
    payload = b"corrupt" if corruption == "hash" else CONTENT
    if corruption != "missing":
        write_private(tmp_path / "objects" / "000000.bin", payload)
    binding = {**ROW, "file": "000000.bin"}
    if corruption == "version":
        binding["uri"] = URI.replace("v1", "v2")
    if corruption == "path":
        binding["file"] = "../outside"
    write_private(
        tmp_path / "object-bindings.json",
        json.dumps([] if corruption == "omitted" else [binding]).encode(),
    )

    def forbidden(*args, **kwargs):
        pytest.fail("offline recovery must not construct or read the source object store")

    monkeypatch.setattr("nexus.ops.local_recovery.S3ProviderEvidenceStore", forbidden)
    if corruption:
        with pytest.raises((RecoveryError, FileNotFoundError)):
            await recovery.recover_local_objects([ROW])
    else:
        assert await recovery.recover_local_objects([ROW]) == 1
        assert (recovery.output / "objects" / "000000.bin").read_bytes() == CONTENT
        assert collect_bindings([ROW]) == {URI: (ROW["sha256"], len(CONTENT))}
