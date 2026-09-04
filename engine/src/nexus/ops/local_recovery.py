"""Restore a protected business backup into a disposable, isolated local database.

This is an operator recovery operation, never a fixture generator. Original
object URIs remain immutable; verified files and their bindings stay private.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from nexus.market.object_store import S3ProviderEvidenceStore, _version_id_from_frozen_uri
from nexus.ops.backup_restore import _POSTGRES_IMAGE
from nexus.ops.fosun_local import load_local_environment
from nexus.settings import EngineSettings

_LOOPBACK = {"localhost", "127.0.0.1", "::1"}
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_URI = re.compile(r"s3://[^\s\"'<>`)\]}]+")
_MAX_OBJECT_BYTES = 64 * 1024 * 1024
_MAX_TOTAL_BYTES = 512 * 1024 * 1024
_MAX_OBJECTS = 10000
_LABEL = "nexus.recovery.run_id"
_REQUIRED_TABLES = {
    "provider_capabilities",
    "portfolio_snapshots",
    "price_series_manifests",
    "price_bar_records",
}


class RecoveryError(RuntimeError):
    """Only fixed, non-sensitive error codes may leave the CLI."""


def write_private(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


def private_file(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise RecoveryError("RECOVERY_PRIVATE_BACKUP_REQUIRED")
    return path.read_bytes()


def read_backup(directory: Path) -> dict[str, Any]:
    manifest = json.loads((directory / "manifest.json").read_bytes())
    if not isinstance(manifest, dict):
        raise RecoveryError("RECOVERY_BACKUP_MANIFEST_INVALID")
    if "format" not in manifest and any(
        (directory / name).exists() for name in ("complete.json", "object-bindings.json")
    ):
        raise RecoveryError("RECOVERY_BACKUP_FORMAT_INVALID")
    if "format" in manifest:
        if manifest["format"] != "nexus_local_backup_v1":
            raise RecoveryError("RECOVERY_BACKUP_FORMAT_INVALID")
        marker = json.loads(private_file(directory / "complete.json"))
        mapping_bytes = private_file(directory / "object-bindings.json")
        mapping = json.loads(mapping_bytes)
        if (
            not isinstance(marker, dict)
            or marker.get("manifest_sha256")
            != hashlib.sha256(private_file(directory / "manifest.json")).hexdigest()
            or hashlib.sha256(mapping_bytes).hexdigest() != manifest.get("objects_sha256")
            or not isinstance(mapping, list)
            or type(manifest.get("object_count")) is not int
            or len(mapping) != manifest["object_count"]
            or not 0 <= len(mapping) <= _MAX_OBJECTS
            or type(manifest.get("source_port")) is not int
            or not 0 < manifest["source_port"] < 65536
        ):
            raise RecoveryError("RECOVERY_BACKUP_INCOMPLETE")
    with (directory / "database.dump").open("rb") as archive:
        digest = hashlib.file_digest(archive, "sha256").hexdigest()
    if digest != manifest.get("dump_sha256") or (
        directory / "database.dump"
    ).stat().st_size != manifest.get("bytes"):
        raise RecoveryError("RECOVERY_BACKUP_DIGEST_MISMATCH")
    before = manifest.get("before")
    if (
        not isinstance(manifest.get("schema"), str)
        or not isinstance(before, dict)
        or not _REQUIRED_TABLES.issubset(before)
        or any(
            not _IDENTIFIER.fullmatch(table)
            or not isinstance(summary, dict)
            or not isinstance(summary.get("count"), int)
            or summary["count"] < 0
            or not _DIGEST.fullmatch(str(summary.get("sha256")))
            for table, summary in before.items()
        )
    ):
        raise RecoveryError("RECOVERY_BACKUP_MANIFEST_INVALID")
    return manifest


async def database_rows(connection: AsyncConnection) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tables = await connection.scalars(
        text("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
    )
    for table in tables:
        if not _IDENTIFIER.fullmatch(table):
            raise RecoveryError("RECOVERY_TABLE_NAME_INVALID")
        rows.extend(
            json.loads(row)
            for row in await connection.scalars(
                text(f'SELECT row_to_json(t)::text FROM public."{table}" t')
            )
        )
    return rows


def validate_target(
    source_url: str, target_url: str, relations: int, extra_schemas: list[str]
) -> None:
    source, target = make_url(source_url), make_url(target_url)
    if source.host not in _LOOPBACK or target.host not in _LOOPBACK:
        raise RecoveryError("RECOVERY_LOCAL_TARGET_REQUIRED")
    # A different database on the source server is still not an isolated target.
    if (source.port or 5432) == (target.port or 5432):
        raise RecoveryError("RECOVERY_SOURCE_TARGET_COLLISION")
    if not (target.database or "").startswith("nexus_recovery_"):
        raise RecoveryError("RECOVERY_GENERATED_TARGET_REQUIRED")
    if relations or extra_schemas:
        raise RecoveryError("RECOVERY_EMPTY_TARGET_REQUIRED")


def collect_bindings(rows: list[dict[str, Any]]) -> dict[str, tuple[str, int]]:
    """Check every stored S3 reference against explicit hash/length bindings."""
    references: set[str] = set()
    bindings: dict[str, tuple[str, int]] = {}
    hashes: dict[str, str] = {}

    def visit(value: Any) -> None:
        if isinstance(value, str):
            references.update(match.rstrip(".,;") for match in _URI.findall(value))
        elif isinstance(value, list):
            for child in value:
                visit(child)
        elif isinstance(value, dict):
            for uri_field, hash_field, size_field in (
                ("uri", "sha256", "byte_size"),
                ("location", "content_hash", "byte_length"),
                ("object_uri", "object_sha256", "byte_size"),
            ):
                uri, digest, size = (value.get(k) for k in (uri_field, hash_field, size_field))
                if isinstance(uri, str) and uri.startswith("s3://") and digest is not None:
                    if (
                        not isinstance(digest, str)
                        or not _DIGEST.fullmatch(digest)
                        or (
                            size is not None
                            and (type(size) is not int or not 0 < size <= _MAX_OBJECT_BYTES)
                        )
                    ):
                        raise RecoveryError("RECOVERY_OBJECT_BINDING_INVALID")
                    if uri in hashes and hashes[uri] != digest:
                        raise RecoveryError("RECOVERY_OBJECT_BINDING_CONFLICT")
                    hashes[uri] = digest
                    # Derived citations inherit length from the original artifact.
                    if size is None:
                        continue
                    binding = (digest, size)
                    if uri in bindings and bindings[uri] != binding:
                        raise RecoveryError("RECOVERY_OBJECT_BINDING_CONFLICT")
                    bindings[uri] = binding
            for child in value.values():
                visit(child)

    visit(rows)
    if references != set(bindings):
        raise RecoveryError("RECOVERY_OBJECT_REFERENCE_UNBOUND")
    if (
        len(bindings) > _MAX_OBJECTS
        or sum(size for _, size in bindings.values()) > _MAX_TOTAL_BYTES
    ):
        raise RecoveryError("RECOVERY_OBJECT_BUDGET_EXCEEDED")
    return bindings


def _backup_object_version(uri: str, *, bucket: str, storage_key: str, digest: str) -> str | None:
    """Audit manifests use immutable content addresses; provider objects use versions."""
    match = re.fullmatch(
        r"audit/manifests/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})\.ndjson\.gz", storage_key
    )
    if match is not None:
        if uri != f"s3://{bucket}/{storage_key}" or match.group(2) != digest:
            raise RecoveryError("RECOVERY_OBJECT_BINDING_INVALID")
        try:
            date.fromisoformat(match.group(1))
        except ValueError:
            raise RecoveryError("RECOVERY_OBJECT_BINDING_INVALID") from None
        return None
    return _version_id_from_frozen_uri(uri, bucket=bucket, storage_key=storage_key)


def verify_object(
    content: bytes, actual_version: str | None, expected_version: str | None, digest: str, size: int
) -> None:
    if (
        actual_version != expected_version
        or len(content) != size
        or hashlib.sha256(content).hexdigest() != digest
    ):
        raise RecoveryError("RECOVERY_OBJECT_MISMATCH")


def _read_audit_manifest(store: S3ProviderEvidenceStore, bucket: str, key: str, size: int) -> bytes:
    response = store._get_object(Bucket=bucket, Key=key)
    body: Any = response.get("Body")
    if body is None or not callable(getattr(body, "read", None)):
        raise RecoveryError("RECOVERY_OBJECT_MISMATCH")
    try:
        content = body.read(size + 1)
    finally:
        if callable(getattr(body, "close", None)):
            body.close()
    if not isinstance(content, bytes) or not content.startswith(b"\x1f\x8b"):
        raise RecoveryError("RECOVERY_OBJECT_MISMATCH")
    return content


class LocalRecovery:
    def __init__(self, backup: Path, repo_root: Path) -> None:
        self.backup = backup.resolve()
        self.repo_root = repo_root
        self.environment: dict[str, str] = {}
        self.run_id = uuid4().hex
        self.container = f"nexus-recovery-postgres-{self.run_id}"
        self.database = f"nexus_recovery_{self.run_id}"
        self.docker: tuple[str, ...] = ("docker",)
        self.owns_target = False
        self.output: Path | None = None

    async def command(
        self,
        *arguments: str,
        input_bytes: bytes | None = None,
        environment: dict[str, str] | None = None,
    ) -> bytes:
        pending_command = asyncio.create_task(
            asyncio.to_thread(
                subprocess.run,
                (*self.docker, *arguments),
                input=input_bytes,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env={**os.environ, **(environment or {})},
                timeout=120,
                check=False,
            )
        )
        try:
            result = await asyncio.shield(pending_command)
        except asyncio.CancelledError:
            # Cancelling an await cannot stop this thread's subprocess. Let the
            # issued command settle before the caller performs owner cleanup.
            while not pending_command.done():
                try:
                    await asyncio.shield(pending_command)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not pending_command.cancelled():
                pending_command.exception()  # Consume failure; cancellation still propagates.
            raise
        if result.returncode:
            raise RecoveryError("RECOVERY_DOCKER_COMMAND_FAILED")
        return result.stdout

    async def local_docker(self) -> None:
        endpoint = os.environ.get("DOCKER_HOST")
        if not endpoint:
            context = (await self.command("context", "show")).decode().strip()
            endpoint = (
                (
                    await self.command(
                        "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"
                    )
                )
                .decode()
                .strip()
            )
        if not endpoint.startswith("unix:///"):
            raise RecoveryError("RECOVERY_LOCAL_DOCKER_REQUIRED")
        self.docker = ("docker", "--host", endpoint)

    async def target_ids(self) -> bytes:
        return (await self.command("ps", "-aq", "--filter", f"name=^/{self.container}$")).strip()

    async def cleanup(self) -> None:
        if not self.owns_target or not await self.target_ids():
            return
        label = (
            (
                await self.command(
                    "inspect",
                    "--format",
                    f'{{{{ index .Config.Labels "{_LABEL}" }}}}',
                    self.container,
                )
            )
            .decode()
            .strip()
        )
        if not secrets.compare_digest(label, self.run_id):
            raise RecoveryError("RECOVERY_CLEANUP_OWNERSHIP_MISMATCH")
        await self.command("rm", "--force", "--volumes", self.container)
        if await self.target_ids():
            raise RecoveryError("RECOVERY_CLEANUP_INCOMPLETE")

    async def start(self) -> str:
        await self.local_docker()
        if await self.target_ids():
            raise RecoveryError("RECOVERY_TARGET_EXISTS")
        await self.command("image", "inspect", _POSTGRES_IMAGE)
        password = secrets.token_urlsafe(32)
        self.owns_target = True
        await self.command(
            "create",
            "--name",
            self.container,
            "--label",
            f"{_LABEL}={self.run_id}",
            "--log-driver=none",
            "-e",
            "POSTGRES_USER=nexus_recovery",
            "-e",
            f"POSTGRES_DB={self.database}",
            "-e",
            "POSTGRES_PASSWORD",
            "--tmpfs",
            "/var/lib/postgresql/data:rw,noexec,nosuid,size=1g",
            "-p",
            "127.0.0.1::5432",
            _POSTGRES_IMAGE,
            environment={"POSTGRES_PASSWORD": password},
        )
        await self.command("start", self.container)
        # This readiness observation never replays restore or any business operation.
        await self.command(
            "exec",
            self.container,
            "sh",
            "-c",
            "i=0; until pg_isready -q -h 127.0.0.1 -U nexus_recovery; do "
            'i=$((i+1)); [ "$i" -lt 30 ] || exit 1; sleep 1; done',
        )
        address = (await self.command("port", self.container, "5432/tcp")).decode().strip()
        if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", address):
            raise RecoveryError("RECOVERY_LOOPBACK_PORT_REQUIRED")
        return URL.create(
            "postgresql+asyncpg",
            username="nexus_recovery",
            password=password,
            host="127.0.0.1",
            port=int(address.split(":")[1]),
            database=self.database,
        ).render_as_string(hide_password=False)

    async def restore_database(
        self, manifest: dict[str, Any], target_url: str
    ) -> list[dict[str, Any]]:
        validate_target(self.environment["DATABASE_URL"], target_url, 0, [])
        engine = create_async_engine(target_url)
        try:
            async with engine.connect() as connection:
                relations = await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'"
                    )
                )
                schemas = list(
                    await connection.scalars(
                        text(
                            "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' "
                            "AND nspname NOT IN ('information_schema', 'public')"
                        )
                    )
                )
                validate_target(
                    self.environment["DATABASE_URL"], target_url, int(relations or 0), schemas
                )
            await self.command(
                "cp", str(self.backup / "database.dump"), f"{self.container}:/tmp/recovery.dump"
            )
            digest = (
                (await self.command("exec", self.container, "sha256sum", "/tmp/recovery.dump"))
                .decode()
                .split()[0]
            )
            if digest != manifest["dump_sha256"]:
                raise RecoveryError("RECOVERY_BACKUP_DIGEST_MISMATCH")
            await self.command(
                "exec",
                self.container,
                "pg_restore",
                "-U",
                "nexus_recovery",
                "-d",
                self.database,
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                "--single-transaction",
                "/tmp/recovery.dump",
            )
            async with engine.connect() as connection:
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                if list(
                    await connection.scalars(text("SELECT version_num FROM alembic_version"))
                ) != [manifest["schema"]]:
                    raise RecoveryError("RECOVERY_SCHEMA_MISMATCH")
                for table, expected in manifest["before"].items():
                    records = list(
                        await connection.scalars(
                            text(f'SELECT row_to_json(t)::text FROM public."{table}" t ORDER BY id')
                        )
                    )
                    if (
                        len(records) != expected["count"]
                        or hashlib.sha256("\n".join(records).encode()).hexdigest()
                        != expected["sha256"]
                    ):
                        raise RecoveryError("RECOVERY_TABLE_DIGEST_MISMATCH")
                return await database_rows(connection)
        finally:
            await engine.dispose()

    async def recover_objects(self, rows: list[dict[str, Any]]) -> int:
        assert self.output is not None
        settings = EngineSettings.model_validate(self.environment)
        if urlsplit(settings.object_store_endpoint or "").hostname not in _LOOPBACK:
            raise RecoveryError("RECOVERY_LOCAL_OBJECT_STORE_REQUIRED")
        bindings = collect_bindings(rows)
        objects = self.output / "objects"
        objects.mkdir(mode=0o700)
        store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        mapping = []
        for index, (uri, (digest, size)) in enumerate(sorted(bindings.items())):
            storage_key = urlsplit(uri).path.removeprefix("/")
            prefix = settings.object_store_prefix + "/"
            version = _backup_object_version(
                uri,
                bucket=settings.object_store_bucket or "",
                storage_key=storage_key,
                digest=digest,
            )
            if version is None:
                content = await asyncio.to_thread(
                    _read_audit_manifest, store, settings.object_store_bucket or "",
                    storage_key, size,
                )
            else:
                if not storage_key.startswith(prefix):
                    raise RecoveryError("RECOVERY_OBJECT_PREFIX_MISMATCH")
                # Reuse the bounded, no-retry, exact-version provider reader.
                content = await asyncio.to_thread(
                    store._read_bounded, storage_key.removeprefix(prefix), version_id=version
                )
            verify_object(content, version, version, digest, size)
            filename = f"{index:06d}.bin"
            write_private(objects / filename, content)
            mapping.append({"uri": uri, "sha256": digest, "byte_size": size, "file": filename})
        write_private(self.output / "object-bindings.json", json.dumps(mapping).encode())
        # Offline readback: no second source fetch, no rewriting historical URIs.
        for binding in json.loads((self.output / "object-bindings.json").read_bytes()):
            content = (objects / binding["file"]).read_bytes()
            verify_object(content, "local", "local", binding["sha256"], binding["byte_size"])
        return len(mapping)

    async def recover_local_objects(self, rows: list[dict[str, Any]]) -> int:
        """Read only the bundle; no source configuration or client is needed."""
        assert self.output is not None
        expected = collect_bindings(rows)
        mapping = json.loads(private_file(self.backup / "object-bindings.json"))
        if not isinstance(mapping, list) or len(mapping) != len(expected):
            raise RecoveryError("RECOVERY_OBJECT_SET_MISMATCH")
        source = self.backup / "objects"
        if source.is_symlink() or not source.is_dir() or source.stat().st_mode & 0o077:
            raise RecoveryError("RECOVERY_PRIVATE_BACKUP_REQUIRED")
        objects = self.output / "objects"
        objects.mkdir(mode=0o700)
        seen: set[str] = set()
        for binding in mapping:
            if not isinstance(binding, dict) or set(binding) != {
                "uri",
                "sha256",
                "byte_size",
                "file",
            }:
                raise RecoveryError("RECOVERY_OBJECT_BINDING_INVALID")
            uri, filename = binding["uri"], binding["file"]
            if (
                not isinstance(uri, str)
                or uri in seen
                or uri not in expected
                or (binding["sha256"], binding["byte_size"]) != expected[uri]
                or not isinstance(filename, str)
                or not re.fullmatch(r"[0-9]{6}\.bin", filename)
            ):
                raise RecoveryError("RECOVERY_OBJECT_SET_MISMATCH")
            parsed = urlsplit(uri)
            digest, size = expected[uri]
            version = _backup_object_version(
                uri, bucket=parsed.netloc, storage_key=parsed.path.removeprefix("/"), digest=digest
            )
            if (source / filename).stat().st_size != size:
                raise RecoveryError("RECOVERY_OBJECT_MISMATCH")
            content = private_file(source / filename)
            verify_object(content, version, version, digest, size)
            write_private(objects / filename, content)
            verify_object(private_file(objects / filename), version, version, digest, size)
            seen.add(uri)
        if seen != set(expected):
            raise RecoveryError("RECOVERY_OBJECT_SET_MISMATCH")
        write_private(self.output / "object-bindings.json", json.dumps(mapping).encode())
        return len(mapping)

    async def execute(self) -> dict[str, Any]:
        started = monotonic()
        manifest = read_backup(self.backup)
        offline = manifest.get("format") == "nexus_local_backup_v1"
        self.environment = (
            {"DATABASE_URL": f"postgresql://127.0.0.1:{manifest['source_port']}/source"}
            if offline
            else load_local_environment(self.repo_root, {})
        )
        # Existing backup and every new recovery artifact must remain private.
        for path in (self.backup, self.backup / "database.dump", self.backup / "manifest.json"):
            if path.stat().st_mode & 0o077:
                raise RecoveryError("RECOVERY_PRIVATE_BACKUP_REQUIRED")
        self.output = self.backup / f"recovery-{self.run_id}"
        self.output.mkdir(mode=0o700)
        result: dict[str, Any] = {"status": "failed", "run_id": self.run_id, "stage": "start"}
        primary_error: BaseException | None = None
        try:
            target_url = await self.start()
            result["stage"] = "database"
            rows = await self.restore_database(manifest, target_url)
            result["database_restored"] = True
            result["backup_schema_verified"] = True
            result["backup_table_digests_verified"] = True
            result["stage"] = "objects"
            result["object_count"] = await (
                self.recover_local_objects(rows) if offline else self.recover_objects(rows)
            )
            result["source_object_store_required"] = not offline
            result["object_bindings_verified"] = True
            result["status"] = "verified"
            result["stage"] = "complete"
        except BaseException as error:
            primary_error = error
            result["status"] = "failed"
            if isinstance(error, RecoveryError):
                result["error_code"] = str(error)
            raise
        finally:
            try:
                await self.cleanup()
                result["target_residue"] = 0
            except BaseException as cleanup_error:
                result["status"] = "cleanup_failed"
                if primary_error is not None:
                    raise BaseExceptionGroup(
                        "recovery and cleanup failed", [primary_error, cleanup_error]
                    ) from None
                raise
            finally:
                result.update(
                    {
                        "backup_schema": manifest["schema"],
                        "backup_file_modified_at": datetime.fromtimestamp(
                            (self.backup / "database.dump").stat().st_mtime, UTC
                        ).isoformat(),
                        "verified_at": datetime.now(UTC).isoformat(),
                        "elapsed_seconds": round(monotonic() - started, 3),
                        "service_failover_tested": False,
                        "unreferenced_objects_included": False,
                    }
                )
                write_private(self.output / "result.json", json.dumps(result).encode())
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(LocalRecovery(args.backup, args.repo_root).execute())
    except (Exception, KeyboardInterrupt, BaseExceptionGroup):
        print(json.dumps({"status": "failed", "error": "LOCAL_RECOVERY_FAILED"}), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
