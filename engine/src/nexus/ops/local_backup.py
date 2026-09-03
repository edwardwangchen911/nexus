"""Create one private, self-contained local database and referenced-object backup."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import (
    _LOOPBACK,
    _REQUIRED_TABLES,
    LocalRecovery,
    RecoveryError,
    collect_bindings,
    database_rows,
    private_file,
    write_private,
)


class LocalBackup(LocalRecovery):
    """Reuse owned local Docker tools and version-verified object copying."""

    async def capture_database(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        source = make_url(self.environment["DATABASE_URL"])
        if source.host not in _LOOPBACK or source.drivername != "postgresql+asyncpg":
            raise RecoveryError("BACKUP_LOCAL_SOURCE_REQUIRED")
        engine = create_async_engine(source, isolation_level="REPEATABLE READ")
        try:
            async with engine.connect() as connection, connection.begin():
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                snapshot = await connection.scalar(text("SELECT pg_export_snapshot()"))
                exported_at = datetime.now(UTC).isoformat()
                schema = list(
                    await connection.scalars(text("SELECT version_num FROM alembic_version"))
                )
                if len(schema) != 1 or not isinstance(snapshot, str):
                    raise RecoveryError("BACKUP_SOURCE_SCHEMA_INVALID")
                before = {}
                for table in sorted(_REQUIRED_TABLES):
                    records = list(
                        await connection.scalars(
                            text(f'SELECT row_to_json(t)::text FROM public."{table}" t ORDER BY id')
                        )
                    )
                    before[table] = {
                        "count": len(records),
                        "sha256": hashlib.sha256("\n".join(records).encode()).hexdigest(),
                    }
                rows = await database_rows(connection)
                collect_bindings(rows)  # Refuse unbound references before producing a dump.
                # The exporting read-only transaction stays open until pg_dump finishes.
                # Credentials are inherited environment values, never CLI arguments.
                pg_environment = {
                    "PGHOST": "host.docker.internal",
                    "PGPORT": str(source.port or 5432),
                    "PGUSER": source.username or "",
                    "PGDATABASE": source.database or "",
                    "PGPASSWORD": source.password or "",
                    "PGOPTIONS": "-c default_transaction_read_only=on",
                }
                await self.command(
                    "exec",
                    *(arg for key in pg_environment for arg in ("-e", key)),
                    self.container,
                    "pg_dump",
                    "--format=custom",
                    "--no-owner",
                    "--no-privileges",
                    f"--snapshot={snapshot}",
                    "--file=/tmp/backup.dump",
                    environment=pg_environment,
                )
                size = int(
                    (
                        await self.command(
                            "exec", self.container, "stat", "-c", "%s", "/tmp/backup.dump"
                        )
                    ).strip()
                )
                if not 0 < size <= 512 * 1024 * 1024:
                    raise RecoveryError("BACKUP_DUMP_BUDGET_EXCEEDED")
                await self.command("exec", self.container, "chmod", "600", "/tmp/backup.dump")
                source_digest = (
                    (await self.command("exec", self.container, "sha256sum", "/tmp/backup.dump"))
                    .decode()
                    .split()[0]
                )
                await self.command(
                    "cp", f"{self.container}:/tmp/backup.dump", str(self.backup / "database.dump")
                )
                (self.backup / "database.dump").chmod(0o600)
                content = private_file(self.backup / "database.dump")
                if len(content) != size or hashlib.sha256(content).hexdigest() != source_digest:
                    raise RecoveryError("BACKUP_DUMP_SIZE_MISMATCH")
                return {
                    "schema": schema[0],
                    "before": before,
                    "source_port": source.port or 5432,
                    "snapshot_exported_at": exported_at,
                    "dump_sha256": hashlib.sha256(content).hexdigest(),
                    "bytes": size,
                }, rows
        finally:
            await engine.dispose()

    async def execute(self) -> dict[str, Any]:
        # Never reuse, delete, or overwrite any existing backup directory.
        self.backup.mkdir(mode=0o700)
        self.output = self.backup
        self.environment = load_local_environment(self.repo_root, {})
        primary_error: BaseException | None = None
        try:
            await self.start()
            manifest, rows = await self.capture_database()
            count = await self.recover_objects(rows)
            # Revalidate the complete mapping against the SAME database snapshot.
            mapping_bytes = private_file(self.backup / "object-bindings.json")
            mapping = json.loads(mapping_bytes)
            expected = collect_bindings(rows)
            if (
                len(mapping) != len(expected)
                or count != len(expected)
                or {item["uri"]: (item["sha256"], item["byte_size"]) for item in mapping}
                != expected
            ):
                raise RecoveryError("BACKUP_OBJECT_SET_MISMATCH")
            manifest.update(
                {
                    "format": "nexus_local_backup_v1",
                    "object_count": count,
                    "objects_sha256": hashlib.sha256(mapping_bytes).hexdigest(),
                }
            )
            manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
            write_private(self.backup / "manifest.json", manifest_bytes)
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                await self.cleanup()
            except BaseException as cleanup_error:
                if primary_error is not None:
                    raise BaseExceptionGroup(
                        "backup and cleanup failed", [primary_error, cleanup_error]
                    ) from None
                raise
        # This is the sole completion marker: cancellation or failed cleanup cannot reach it.
        write_private(
            self.backup / "complete.json",
            json.dumps(
                {
                    "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                    "completed_at": datetime.now(UTC).isoformat(),
                }
            ).encode(),
        )
        return {
            "status": "complete",
            "run_id": self.run_id,
            "object_count": count,
            "target_residue": 0,
            "unreferenced_objects_included": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--repo-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = asyncio.run(LocalBackup(args.destination, args.repo_root).execute())
    except (Exception, KeyboardInterrupt, BaseExceptionGroup):
        print(json.dumps({"status": "failed", "error": "LOCAL_BACKUP_FAILED"}), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
