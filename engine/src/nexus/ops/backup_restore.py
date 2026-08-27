"""Fixture-only PostgreSQL and object-store backup/restore drill."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.audit.manifest import AuditManifestObjectStore, publish_audit_chain_manifest
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.paper import (
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.portfolio import (
    BrokerSyncRun,
    CashSnapshot,
    PortfolioSnapshotRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings, Environment

_RUN_ID = re.compile(r"^[0-9a-f]{32}$")

VERIFICATION_TABLES = (
    "paper_accounts",
    "paper_ledger_transactions",
    "paper_ledger_entries",
    "paper_performance_snapshots",
    "paper_performance_attributions",
    "portfolio_snapshots",
    "audit_events",
    "audit_chain_manifests",
)

_PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
_ACCOUNT_ID = UUID("877caec6-5092-5945-a268-544f5e78a40c")
_PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")
_POSTGRES_IMAGE = (
    "postgres:17@sha256:e38411452a464af89e5adadb8d223bf53b898d47d6ef918b2d58c08707350449"
)
_MINIO_IMAGE = (
    "minio/minio:RELEASE.2025-07-23T15-54-02Z@"
    "sha256:d249d1fb6966de4d8ad26c04754b545205ff15a62e4fd19ebd0f26fa5baacbc0"
)
_DATABASE_USER = "nexus_drill"
_MAX_OBJECTS = 100
_MAX_OBJECT_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class DrillTargets:
    """All destructive targets derived from one unambiguous random run ID."""

    run_id: str
    source_database: str
    restore_database: str
    source_prefix: str
    backup_prefix: str
    restore_prefix: str
    dump_path: str
    postgres_container: str
    minio_container: str

    @classmethod
    def from_run_id(cls, run_id: str) -> DrillTargets:
        if _RUN_ID.fullmatch(run_id) is None:
            raise ValueError("drill run ID must be exactly 32 lowercase hexadecimal characters")
        return cls(
            run_id=run_id,
            source_database=f"nexus_drill_source_{run_id}",
            restore_database=f"nexus_drill_restore_{run_id}",
            source_prefix=f"drills/{run_id}/source/",
            backup_prefix=f"drills/{run_id}/backup/",
            restore_prefix=f"drills/{run_id}/restore/",
            dump_path=f"/tmp/nexus_drill_{run_id}.dump",
            postgres_container=f"nexus-drill-postgres-{run_id}",
            minio_container=f"nexus-drill-minio-{run_id}",
        )


class DrillOperations(Protocol):
    async def start(self, targets: DrillTargets) -> None: ...

    async def create(self, targets: DrillTargets) -> None: ...

    async def seed(self, targets: DrillTargets) -> None: ...

    async def backup(self, targets: DrillTargets) -> None: ...

    async def restore(self, targets: DrillTargets) -> None: ...

    async def verify(self, targets: DrillTargets) -> dict[str, object]: ...

    async def cleanup(self, targets: DrillTargets) -> None: ...


async def execute_drill(
    operations: DrillOperations,
    targets: DrillTargets,
) -> dict[str, object]:
    """Execute every phase and always attempt exact run-scoped cleanup."""
    result: dict[str, object] | None = None
    primary_error: BaseException | None = None
    try:
        await operations.start(targets)
        await operations.create(targets)
        await operations.seed(targets)
        await operations.backup(targets)
        await operations.restore(targets)
        result = await operations.verify(targets)
    except BaseException as error:
        primary_error = error
    try:
        await operations.cleanup(targets)
    except BaseException as cleanup_error:
        if primary_error is not None:
            raise BaseExceptionGroup(
                "backup/restore drill and cleanup both failed",
                [primary_error, cleanup_error],
            )
        raise
    if primary_error is not None:
        raise primary_error.with_traceback(primary_error.__traceback__)
    if result is None:
        raise RuntimeError("backup/restore drill returned no result")
    return result


def pg_dump_command(
    targets: DrillTargets,
    *,
    database_user: str,
) -> tuple[str, ...]:
    _database_user(database_user)
    return (
        "docker",
        "exec",
        targets.postgres_container,
        "pg_dump",
        "--username",
        database_user,
        "--dbname",
        targets.source_database,
        "--file",
        targets.dump_path,
        "--format=custom",
        "--no-owner",
        "--no-privileges",
    )


def pg_restore_command(
    targets: DrillTargets,
    *,
    database_user: str,
) -> tuple[str, ...]:
    _database_user(database_user)
    return (
        "docker",
        "exec",
        targets.postgres_container,
        "pg_restore",
        "--username",
        database_user,
        "--dbname",
        targets.restore_database,
        "--exit-on-error",
        "--single-transaction",
        targets.dump_path,
    )


def postgres_container_command(targets: DrillTargets) -> tuple[str, ...]:
    """Build an isolated PostgreSQL container command without persistent volumes."""
    return (
        "docker",
        "run",
        "--rm",
        "-d",
        "--name",
        targets.postgres_container,
        "-e",
        "POSTGRES_DB",
        "-e",
        "POSTGRES_USER",
        "-e",
        "POSTGRES_PASSWORD",
        "--tmpfs",
        "/var/lib/postgresql/data:rw,noexec,nosuid,size=1g",
        "-p",
        "127.0.0.1::5432",
        _POSTGRES_IMAGE,
    )


def minio_container_command(targets: DrillTargets) -> tuple[str, ...]:
    """Build an isolated object-store command without persistent volumes."""
    return (
        "docker",
        "run",
        "--rm",
        "-d",
        "--name",
        targets.minio_container,
        "-e",
        "MINIO_ROOT_USER",
        "-e",
        "MINIO_ROOT_PASSWORD",
        "--tmpfs",
        "/data:rw,noexec,nosuid,size=256m",
        "-p",
        "127.0.0.1::9000",
        _MINIO_IMAGE,
        "server",
        "/data",
    )


def _database_user(value: str) -> str:
    if re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value) is None:
        raise ValueError("drill database user is invalid")
    return value


def verify_manifest_object_binding(
    manifest: object,
    *,
    bucket: str,
    source_prefix: str,
    object_hashes: dict[str, tuple[int, str]],
) -> None:
    """Require the database manifest record to identify one captured object exactly."""
    if not isinstance(manifest, tuple) or len(manifest) != 4:
        raise RuntimeError("drill manifest object binding is invalid")
    _, object_sha256, byte_size, object_uri = manifest
    uri_prefix = f"s3://{bucket}/{source_prefix}"
    if (
        not isinstance(object_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", object_sha256) is None
        or not isinstance(byte_size, int)
        or byte_size <= 0
        or not isinstance(object_uri, str)
        or not object_uri.startswith(uri_prefix)
    ):
        raise RuntimeError("drill manifest object binding is invalid")
    relative = object_uri.removeprefix(uri_prefix)
    if not relative or object_hashes.get(relative) != (byte_size, object_sha256):
        raise RuntimeError("drill manifest object binding is invalid")


class _DrillManifestStore(AuditManifestObjectStore):
    def __init__(self, operations: LocalDockerDrillOperations, prefix: str) -> None:
        self._operations = operations
        self._prefix = prefix

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        full_key = f"{self._prefix}{key}"
        await self._operations.put_source_object(full_key, content)
        return f"s3://{self._operations.bucket}/{full_key}"


class LocalDockerDrillOperations:
    """Own disposable local containers and reject every pre-existing data target."""

    def __init__(self, *, alembic_config: Path | None = None) -> None:
        self._alembic_config = (alembic_config or Path("engine/alembic.ini")).resolve()
        self._database_password = secrets.token_urlsafe(32)
        self._minio_access_key = f"nexusdrill{secrets.token_hex(8)}"
        self._minio_secret_key = secrets.token_urlsafe(32)
        self._admin_url: str | None = None
        self._source_url: str | None = None
        self._restore_url: str | None = None
        self._s3: Any = None
        self._bucket = ""
        self._fixture_at: datetime | None = None
        self._backup_wall_started_at: datetime | None = None
        self._total_started = perf_counter()
        self._backup_seconds = 0.0
        self._restore_started = 0.0
        self._restore_seconds = 0.0
        self._verify_seconds = 0.0
        self._dump_bytes = 0
        self._object_hashes: dict[str, tuple[int, str]] = {}
        self._result: dict[str, object] | None = None

    @property
    def bucket(self) -> str:
        if not self._bucket:
            raise RuntimeError("drill object-store bucket is not initialized")
        return self._bucket

    async def start(self, targets: DrillTargets) -> None:
        if os.environ.get("NEXUS_ENV", "development") == Environment.PRODUCTION.value:
            raise RuntimeError("backup/restore drill is forbidden in production")
        if not self._alembic_config.is_file():
            raise RuntimeError("drill Alembic configuration is unavailable")
        await self._run(
            postgres_container_command(targets),
            environment={
                "POSTGRES_DB": "postgres",
                "POSTGRES_USER": _DATABASE_USER,
                "POSTGRES_PASSWORD": self._database_password,
            },
        )
        await self._run(
            minio_container_command(targets),
            environment={
                "MINIO_ROOT_USER": self._minio_access_key,
                "MINIO_ROOT_PASSWORD": self._minio_secret_key,
            },
        )
        await self._wait_for_postgres(targets)
        postgres_port = await self._published_port(targets.postgres_container, "5432/tcp")
        minio_port = await self._published_port(targets.minio_container, "9000/tcp")
        self._admin_url = self._database_url(postgres_port, "postgres")
        self._source_url = self._database_url(postgres_port, targets.source_database)
        self._restore_url = self._database_url(postgres_port, targets.restore_database)
        self._bucket = f"nexus-drill-{targets.run_id}"
        self._s3 = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{minio_port}",
            region_name="us-east-1",
            aws_access_key_id=self._minio_access_key,
            aws_secret_access_key=self._minio_secret_key,
            config=Config(
                connect_timeout=1,
                read_timeout=2,
                retries={"max_attempts": 0},
                s3={"addressing_style": "path"},
            ),
        )
        await self._wait_for_minio()
        await asyncio.to_thread(self._s3.create_bucket, Bucket=self._bucket)

    async def create(self, targets: DrillTargets) -> None:
        admin_url = self._required_url(self._admin_url)
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                for database in (targets.source_database, targets.restore_database):
                    await connection.execute(text(f'CREATE DATABASE "{database}"'))
        finally:
            await engine.dispose()
        await self._migrate(self._required_url(self._source_url))

    async def seed(self, targets: DrillTargets) -> None:
        source_url = self._required_url(self._source_url)
        settings = EngineSettings.model_validate(
            {"NEXUS_ENV": "test", "DATABASE_URL": source_url}
        )
        factory = session_factory(settings)
        fixture_at = datetime.now(UTC)
        sync_run_id = new_uuid7()
        snapshot_id = new_uuid7()
        performance_id = new_uuid7()
        try:
            async with factory() as session, session.begin():
                session.add(
                    BrokerSyncRun(
                        id=sync_run_id,
                        account_id=_ACCOUNT_ID,
                        portfolio_id=_PORTFOLIO_ID,
                        trigger_kind="MANUAL",
                        actor_id="system:backup-restore-drill",
                        route="/ops/backup-restore-drill",
                        idempotency_key=f"drill-{targets.run_id}",
                        request_hash="a" * 64,
                        status="READY",
                        accepted_at=fixture_at,
                        completed_at=fixture_at,
                        attempt_count=1,
                        failure_reason=None,
                        cash_flow_quality="PASS",
                    )
                )
                await session.flush()
                session.add(
                    PortfolioSnapshotRecord(
                        id=snapshot_id,
                        portfolio_id=_PORTFOLIO_ID,
                        sync_run_id=sync_run_id,
                        stale_from_snapshot_id=None,
                        status="READY",
                        as_of=fixture_at,
                        position_hash_version="drill_v1",
                        position_hash="b" * 64,
                        order_hash_version="drill_v1",
                        order_hash="c" * 64,
                        source_request_ids=[f"drill-{targets.run_id}"],
                        source_observations=[
                            {
                                "source": "fixture",
                                "as_of": fixture_at.isoformat(),
                                "quality": "PASS",
                            }
                        ],
                    )
                )
                performance = PaperPerformanceSnapshotRecord(
                    id=performance_id,
                    account_id=_PAPER_ACCOUNT_ID,
                    as_of=fixture_at,
                    nav=Decimal("10000"),
                    peak_nav=Decimal("10000"),
                    cash=Decimal("10000"),
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    fees=Decimal("0"),
                    drawdown_ratio=Decimal("0"),
                    content_hash="d" * 64,
                )
                session.add(performance)
                await session.flush()
                session.add(
                    CashSnapshot(
                        id=new_uuid7(),
                        portfolio_snapshot_id=snapshot_id,
                        currency="USD",
                        cash_balance=Decimal("10000"),
                        available_cash=Decimal("10000"),
                        provider_as_of=fixture_at,
                    )
                )
                session.add(
                    PaperPerformanceAttributionRecord(
                        id=new_uuid7(),
                        performance_snapshot_id=performance_id,
                        account_id=_PAPER_ACCOUNT_ID,
                        dimension="cash",
                        dimension_key="USD",
                        market_value=Decimal("10000"),
                        realized_pnl=Decimal("0"),
                        unrealized_pnl=Decimal("0"),
                        fees=Decimal("0"),
                        slippage=Decimal("0"),
                        completeness="FULL",
                        content_hash="e" * 64,
                    )
                )
                await append_audit_event(
                    session,
                    actor_id="system:backup-restore-drill",
                    action="ops.backup_restore.fixture_seeded",
                    schema_version="backup_restore_drill_v1",
                    payload={"run_id": targets.run_id, "status": "fixture"},
                    occurred_at=fixture_at,
                )
            async with factory() as session, session.begin():
                await publish_audit_chain_manifest(
                    session,
                    session_date=fixture_at.date(),
                    object_store=_DrillManifestStore(self, targets.source_prefix),
                )
        finally:
            await cast(Any, factory.kw["bind"]).dispose()
        self._fixture_at = fixture_at

    async def backup(self, targets: DrillTargets) -> None:
        started = perf_counter()
        self._backup_wall_started_at = datetime.now(UTC)
        await self._run(pg_dump_command(targets, database_user=_DATABASE_USER))
        size = await self._run(
            (
                "docker",
                "exec",
                targets.postgres_container,
                "stat",
                "--format=%s",
                targets.dump_path,
            )
        )
        try:
            self._dump_bytes = int(size.stdout.strip())
        except ValueError as error:
            raise RuntimeError("drill dump byte size is invalid") from error
        if self._dump_bytes <= 0:
            raise RuntimeError("drill dump is empty")
        source = await self._read_prefix(targets.source_prefix)
        if not source:
            raise RuntimeError("drill source prefix is empty")
        for relative, content in source.items():
            await self._copy_and_verify(
                f"{targets.source_prefix}{relative}",
                f"{targets.backup_prefix}{relative}",
                content,
            )
            self._object_hashes[relative] = (
                len(content),
                hashlib.sha256(content).hexdigest(),
            )
        self._backup_seconds = perf_counter() - started

    async def restore(self, targets: DrillTargets) -> None:
        self._restore_started = perf_counter()
        await self._run(pg_restore_command(targets, database_user=_DATABASE_USER))
        backup = await self._read_prefix(targets.backup_prefix)
        if set(backup) != set(self._object_hashes):
            raise RuntimeError("drill backup object set is incomplete")
        for relative, content in backup.items():
            await self._copy_and_verify(
                f"{targets.backup_prefix}{relative}",
                f"{targets.restore_prefix}{relative}",
                content,
            )
        self._restore_seconds = perf_counter() - self._restore_started

    async def verify(self, targets: DrillTargets) -> dict[str, object]:
        started = perf_counter()
        source = await self._database_snapshot(self._required_url(self._source_url))
        restored = await self._database_snapshot(self._required_url(self._restore_url))
        if source != restored:
            raise RuntimeError("restored database authority roots do not match source")
        if source["migration_head"] != "0021_manual_job_retry_allowance":
            raise RuntimeError("restored database migration head is incompatible")
        ledger = source["ledger"]
        if not isinstance(ledger, tuple) or ledger[0] != ledger[1] or ledger[0] <= 0:
            raise RuntimeError("restored paper ledger is not balanced")
        restored_objects = await self._read_prefix(targets.restore_prefix)
        if set(restored_objects) != set(self._object_hashes):
            raise RuntimeError("restored object set does not match source")
        object_bytes = 0
        for relative, content in restored_objects.items():
            expected = self._object_hashes[relative]
            actual = (len(content), hashlib.sha256(content).hexdigest())
            if actual != expected:
                raise RuntimeError("restored object content does not match source")
            object_bytes += len(content)
        verify_manifest_object_binding(
            source["manifest"],
            bucket=self.bucket,
            source_prefix=targets.source_prefix,
            object_hashes=self._object_hashes,
        )
        self._verify_seconds = perf_counter() - started
        fixture_at = self._required_time(self._fixture_at)
        backup_started = self._required_time(self._backup_wall_started_at)
        result: dict[str, object] = {
            "status": "verified",
            "run_id": targets.run_id,
            "scope": "fixture_only",
            "database_dump_bytes": self._dump_bytes,
            "object_count": len(restored_objects),
            "object_bytes": object_bytes,
            "measured_fixture_rpo_seconds": max(
                0.0, (backup_started - fixture_at).total_seconds()
            ),
            "measured_fixture_rto_seconds": perf_counter() - self._restore_started,
            "durations_seconds": {
                "backup": self._backup_seconds,
                "restore": self._restore_seconds,
                "verify": self._verify_seconds,
                "total_before_cleanup": perf_counter() - self._total_started,
            },
            "verification": {
                "migration_head": source["migration_head"],
                "table_counts_match": True,
                "ledger_balanced": True,
                "latest_performance_root_match": True,
                "latest_ready_root_match": True,
                "audit_manifest_root_match": True,
                "object_hashes_match": True,
            },
        }
        self._result = result
        return result

    async def cleanup(self, targets: DrillTargets) -> None:
        cleanup_errors: list[Exception] = []
        if self._s3 is not None and self._bucket:
            try:
                for prefix in (
                    targets.source_prefix,
                    targets.backup_prefix,
                    targets.restore_prefix,
                ):
                    await self._delete_prefix(prefix)
                await asyncio.to_thread(self._s3.delete_bucket, Bucket=self._bucket)
            except Exception as error:
                cleanup_errors.append(error)
        if self._admin_url is not None:
            try:
                await self._drop_databases(targets)
            except Exception as error:
                cleanup_errors.append(error)
        for container in (targets.minio_container, targets.postgres_container):
            try:
                await self._stop_container(container)
            except Exception as error:
                cleanup_errors.append(error)
        if self._result is not None:
            self._result["cleanup"] = {
                "database_residue": 0,
                "object_residue": 0,
                "container_residue": 0,
            }
        if cleanup_errors:
            raise RuntimeError("backup/restore drill cleanup failed") from cleanup_errors[0]

    async def put_source_object(self, key: str, content: bytes) -> None:
        if not key.startswith("drills/") or not content or len(content) > _MAX_OBJECT_BYTES:
            raise ValueError("drill source object is invalid")
        digest = hashlib.sha256(content).hexdigest()
        await asyncio.to_thread(
            self._s3.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=content,
            Metadata={"sha256": digest},
        )
        existing = await self._read_object(key)
        if not secrets.compare_digest(existing, content):
            raise RuntimeError("drill source object verification failed")

    async def _migrate(self, database_url: str) -> None:
        await self._run(
            (
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(self._alembic_config),
                "upgrade",
                "head",
            ),
            environment={"DATABASE_URL": database_url},
        )

    async def _wait_for_postgres(self, targets: DrillTargets) -> None:
        for _ in range(80):
            result = await self._run(
                (
                    "docker",
                    "exec",
                    targets.postgres_container,
                    "pg_isready",
                    "--username",
                    _DATABASE_USER,
                    "--dbname",
                    "postgres",
                ),
                check=False,
            )
            if result.returncode == 0:
                return
            await asyncio.sleep(0.25)
        raise RuntimeError("drill PostgreSQL did not become ready")

    async def _wait_for_minio(self) -> None:
        for _ in range(80):
            try:
                await asyncio.to_thread(self._s3.list_buckets)
                return
            except Exception:
                await asyncio.sleep(0.25)
        raise RuntimeError("drill object store did not become ready")

    async def _published_port(self, container: str, target: str) -> int:
        result = await self._run(("docker", "port", container, target))
        value = result.stdout.strip().rsplit(":", 1)[-1]
        if not value.isdigit() or not 1 <= int(value) <= 65535:
            raise RuntimeError("drill published port is invalid")
        return int(value)

    def _database_url(self, port: int, database: str) -> str:
        return URL.create(
            "postgresql+asyncpg",
            username=_DATABASE_USER,
            password=self._database_password,
            host="127.0.0.1",
            port=port,
            database=database,
        ).render_as_string(hide_password=False)

    async def _copy_and_verify(self, source: str, target: str, expected: bytes) -> None:
        await asyncio.to_thread(
            self._s3.copy_object,
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": source},
            Key=target,
            MetadataDirective="COPY",
        )
        actual = await self._read_object(target)
        if not secrets.compare_digest(actual, expected):
            raise RuntimeError("drill copied object verification failed")

    async def _read_prefix(self, prefix: str) -> dict[str, bytes]:
        response = await asyncio.to_thread(
            self._s3.list_objects_v2,
            Bucket=self.bucket,
            Prefix=prefix,
            MaxKeys=_MAX_OBJECTS + 1,
        )
        if bool(response.get("IsTruncated")):
            raise RuntimeError("drill object prefix exceeds its bound")
        contents = response.get("Contents", [])
        if not isinstance(contents, list) or len(contents) > _MAX_OBJECTS:
            raise RuntimeError("drill object listing is invalid")
        result: dict[str, bytes] = {}
        for item in contents:
            if not isinstance(item, dict) or not isinstance(item.get("Key"), str):
                raise RuntimeError("drill object key is invalid")
            key = item["Key"]
            if not key.startswith(prefix) or key == prefix:
                raise RuntimeError("drill object escaped its prefix")
            relative = key.removeprefix(prefix)
            if not relative or relative in result:
                raise RuntimeError("drill object relative key is invalid")
            result[relative] = await self._read_object(key)
        return result

    async def _read_object(self, key: str) -> bytes:
        response = await asyncio.to_thread(
            self._s3.get_object,
            Bucket=self.bucket,
            Key=key,
        )
        body = response.get("Body")
        if body is None or not callable(getattr(body, "read", None)):
            raise RuntimeError("drill object body is unavailable")
        try:
            content = await asyncio.to_thread(body.read, _MAX_OBJECT_BYTES + 1)
        finally:
            if callable(getattr(body, "close", None)):
                body.close()
        if not isinstance(content, bytes) or not content or len(content) > _MAX_OBJECT_BYTES:
            raise RuntimeError("drill object content is invalid")
        return content

    async def _database_snapshot(self, database_url: str) -> dict[str, object]:
        engine = create_async_engine(database_url)
        sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sessions() as session:
                head = await session.scalar(text("SELECT version_num FROM alembic_version"))
                counts: dict[str, int] = {}
                for table in VERIFICATION_TABLES:
                    counts[table] = int(
                        await session.scalar(text(f'SELECT count(*) FROM "{table}"')) or 0
                    )
                ledger = (
                    Decimal(
                        await session.scalar(
                            text("SELECT COALESCE(sum(debit_amount), 0) FROM paper_ledger_entries")
                        )
                        or 0
                    ),
                    Decimal(
                        await session.scalar(
                            text(
                                "SELECT COALESCE(sum(credit_amount), 0) "
                                "FROM paper_ledger_entries"
                            )
                        )
                        or 0
                    ),
                )
                performance = (
                    await session.execute(
                        text(
                            "SELECT account_id::text, as_of, nav, peak_nav, cash, "
                            "realized_pnl, unrealized_pnl, fees, drawdown_ratio, content_hash "
                            "FROM paper_performance_snapshots ORDER BY as_of DESC LIMIT 1"
                        )
                    )
                ).one()
                ready = (
                    await session.execute(
                        text(
                            "SELECT portfolio_id::text, status, as_of, position_hash, order_hash "
                            "FROM portfolio_snapshots WHERE status = 'READY' "
                            "ORDER BY as_of DESC LIMIT 1"
                        )
                    )
                ).one()
                manifest = (
                    await session.execute(
                        text(
                            "SELECT head_hash, object_sha256, byte_size, object_uri "
                            "FROM audit_chain_manifests ORDER BY session DESC LIMIT 1"
                        )
                    )
                ).one()
            return {
                "migration_head": head,
                "counts": counts,
                "ledger": ledger,
                "performance": tuple(performance),
                "ready": tuple(ready),
                "manifest": tuple(manifest),
            }
        finally:
            await engine.dispose()

    async def _delete_prefix(self, prefix: str) -> None:
        objects = await self._read_prefix(prefix)
        if objects:
            await asyncio.to_thread(
                self._s3.delete_objects,
                Bucket=self.bucket,
                Delete={
                    "Objects": [{"Key": f"{prefix}{relative}"} for relative in objects],
                    "Quiet": True,
                },
            )
        if await self._read_prefix(prefix):
            raise RuntimeError("drill object prefix cleanup is incomplete")

    async def _drop_databases(self, targets: DrillTargets) -> None:
        engine = create_async_engine(
            self._required_url(self._admin_url), isolation_level="AUTOCOMMIT"
        )
        try:
            async with engine.connect() as connection:
                for database in (targets.source_database, targets.restore_database):
                    await connection.execute(
                        text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
                    )
                residue = await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_database "
                        "WHERE datname IN (:source, :restore)"
                    ),
                    {
                        "source": targets.source_database,
                        "restore": targets.restore_database,
                    },
                )
            if residue != 0:
                raise RuntimeError("drill database cleanup is incomplete")
        finally:
            await engine.dispose()

    async def _stop_container(self, container: str) -> None:
        await self._run(("docker", "stop", "--time", "5", container), check=False)
        for _ in range(50):
            result = await self._run(("docker", "inspect", container), check=False)
            if result.returncode != 0:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError("drill container cleanup is incomplete")

    async def _run(
        self,
        command: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        child_environment = os.environ.copy()
        if environment:
            child_environment.update(environment)
        return await asyncio.to_thread(
            subprocess.run,
            command,
            check=check,
            capture_output=True,
            text=True,
            env=child_environment,
        )

    @staticmethod
    def _required_url(value: str | None) -> str:
        if value is None:
            raise RuntimeError("drill database URL is unavailable")
        return value

    @staticmethod
    def _required_time(value: datetime | None) -> datetime:
        if value is None:
            raise RuntimeError("drill measurement time is unavailable")
        return value


async def run_local_drill() -> dict[str, object]:
    targets = DrillTargets.from_run_id(uuid4().hex)
    return await execute_drill(LocalDockerDrillOperations(), targets)


def main() -> int:
    """Run one local drill and emit only sanitized fixture measurements."""
    try:
        result = asyncio.run(run_local_drill())
    except Exception:
        print("backup/restore drill failed", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0
