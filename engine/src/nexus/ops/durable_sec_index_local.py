"""Normal local SEC index collection/import/readback; no research or notification dispatch."""

import asyncio
from datetime import datetime
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import text

from nexus.ops.research_identity_local import SecDirectoryInBucket, collect_identity_sources
from nexus.ops.sec_index_local import _read, _store
from nexus.persistence.session import session_factory
from nexus.research.durable_sec_index_storage import persist_durable_index_archive
from nexus.research.sec_exhibit_storage import ExhibitStore, _read_identity
from nexus.settings import EngineSettings


async def import_durable_index(
    settings: EngineSettings,
    *,
    identity_snapshot_id: UUID,
    archive_dir: Path,
    store: ExhibitStore | None = None,
) -> dict[str, object]:
    archive_store = _store(settings, store)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            if not isinstance(now, datetime):
                raise ValueError("SEC_INDEX_CLOCK_UNAVAILABLE")
            identity = await _read_identity(
                session, archive_store, snapshot_id=identity_snapshot_id, as_of=now
            )
            symbol = identity.identity.symbol
            directory = (
                _read(archive_dir / "directory.json", limit=8 * 1024 * 1024),
                _read(archive_dir / "directory-receipt.json", limit=65536),
            )
            submissions = (
                _read(archive_dir / f"{symbol}-submissions.json", limit=8 * 1024 * 1024),
                _read(archive_dir / f"{symbol}-submissions-receipt.json", limit=65536),
            )
            # Timestamp local custody after all archive bytes are read and identity
            # originals verified; older network receipt dates never backdate import.
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            assert isinstance(now, datetime)
            snapshot, created = await persist_durable_index_archive(
                session,
                archive_store,
                identity_snapshot_id=identity_snapshot_id,
                directory=directory,
                submissions=submissions,
                now=now,
            )
            return {
                "status": "created" if created else "existing",
                "snapshot_id": str(snapshot.id),
                "available_at": snapshot.available_at.isoformat(),
                "execution_locked": True,
                "model_requests": 0,
                "provider_requests": 0,
            }
    finally:
        await factory.kw["bind"].dispose()


async def collect_durable_index(
    settings: EngineSettings,
    *,
    identity_snapshot_id: UUID,
    archive_dir: Path,
    contact: str,
    store: ExhibitStore | None = None,
    client: httpx.AsyncClient | None = None,
    directory_in_bucket: SecDirectoryInBucket | None = None,
) -> dict[str, object]:
    archive_store = _store(settings, store)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            assert isinstance(now, datetime)
            identity = await _read_identity(
                session, archive_store, snapshot_id=identity_snapshot_id, as_of=now
            )
        # One deadline for the public HTTP chain, as in the original collector.
        # Import below retains its own transaction; this is not a command deadline.
        async with asyncio.timeout(60):
            collected = await collect_identity_sources(
                symbols=(identity.identity.symbol,),
                contact=contact,
                output_dir=archive_dir,
                client=client,
                directory_in_bucket=directory_in_bucket,
                expected_directory_binding=(identity.identity.cik, identity.identity.exchange)
                if directory_in_bucket is not None
                else None,
            )
        result = await import_durable_index(
            settings,
            identity_snapshot_id=identity_snapshot_id,
            archive_dir=archive_dir,
            store=archive_store,
        )
        return {**result, "provider_requests": collected["requests"]}
    finally:
        await factory.kw["bind"].dispose()
