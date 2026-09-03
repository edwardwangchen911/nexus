"""Real pg_dump/restore, entirely synthetic facts and a frozen S3 transport stub."""

import hashlib
import io
import json
from types import SimpleNamespace

import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops import local_backup, local_recovery
from nexus.ops.local_backup import LocalBackup
from nexus.ops.local_recovery import LocalRecovery
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.asyncio
async def test_consistent_bundle_restores_without_source_credentials_or_object_client(
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    content = b"synthetic versioned evidence"
    uri = "s3://synthetic/provider/market/test?versionId=v1"
    binding = {"uri": uri, "sha256": hashlib.sha256(content).hexdigest(), "byte_size": len(content)}
    source = create_async_engine(migrated_database_url)
    try:
        async with source.begin() as connection:
            await connection.execute(text("CREATE TABLE synthetic_backup_refs (payload jsonb)"))
            await connection.execute(
                text("INSERT INTO synthetic_backup_refs VALUES (CAST(:p AS jsonb))"),
                {"p": json.dumps(binding)},
            )
        environment = {
            "DATABASE_URL": migrated_database_url,
            "OBJECT_STORE_ENDPOINT": "http://localhost:59999",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "synthetic",
            "OBJECT_STORE_ACCESS_KEY_ID": "synthetic",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic",
        }
        monkeypatch.setattr(local_backup, "load_local_environment", lambda *a: environment)
        requests = []

        def get_object(**request):
            requests.append(request)
            return {"Body": io.BytesIO(content), "VersionId": "v1"}

        monkeypatch.setattr(
            local_recovery,
            "S3ProviderEvidenceStore",
            lambda settings: S3ProviderEvidenceStore(
                settings, client=SimpleNamespace(get_object=get_object)
            ),
        )
        backup = LocalBackup(tmp_path / "bundle", tmp_path)
        original_command = backup.command

        async def race_after_export(*args, **kwargs):
            if "pg_dump" in args:
                async with source.begin() as connection:
                    await connection.execute(
                        text("INSERT INTO synthetic_backup_refs VALUES (CAST(:p AS jsonb))"),
                        {"p": json.dumps({"later": True})},
                    )
            return await original_command(*args, **kwargs)

        monkeypatch.setattr(backup, "command", race_after_export)
        assert (await backup.execute())["status"] == "complete"
        assert requests == [
            {"Bucket": "synthetic", "Key": "provider/market/test", "VersionId": "v1"}
        ]

        def forbidden(*args, **kwargs):
            pytest.fail("offline recovery must not read source credentials or construct source S3")

        monkeypatch.setattr(local_recovery, "load_local_environment", forbidden)
        monkeypatch.setattr(local_recovery, "S3ProviderEvidenceStore", forbidden)
        recovery = LocalRecovery(backup.backup, tmp_path / "no-source-configuration")
        original_restore = recovery.restore_database

        async def verify_rows(*args):
            rows = await original_restore(*args)
            assert {"payload": binding} in rows
            assert {"payload": {"later": True}} not in rows
            return rows

        monkeypatch.setattr(recovery, "restore_database", verify_rows)
        result = await recovery.execute()
        assert result["status"] == "verified"
        assert result["source_object_store_required"] is False
        assert result["object_count"] == 1 and result["target_residue"] == 0
        assert (recovery.output / "objects" / "000000.bin").read_bytes() == content
        assert json.loads((recovery.output / "object-bindings.json").read_bytes())[0]["uri"] == uri
    finally:
        await source.dispose()
