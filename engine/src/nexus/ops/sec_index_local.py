"""Normal local SEC index collection/import/readback; no research or notification dispatch."""

import argparse
import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from sqlalchemy import func, select, text

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.research_identity_local import collect_identity_sources
from nexus.persistence.session import session_factory
from nexus.research.sec_exhibit_storage import ExhibitStore, _read_identity
from nexus.research.sec_index_storage import (
    compare_index_captures,
    persist_index_archive,
    read_index_capture,
)
from nexus.settings import EngineSettings


def _store(settings: EngineSettings, store: ExhibitStore | None) -> ExhibitStore:
    _validate_local_database(settings.database_url)
    if store is not None:
        return store
    if urlsplit(settings.object_store_endpoint or "").hostname not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise ValueError("SEC_INDEX_LOCAL_STORE_REQUIRED")
    return S3ProviderEvidenceStore(settings)


def _read(path: Path, *, limit: int) -> bytes:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > limit:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    raw = path.read_bytes()
    if len(raw) > limit:
        raise ValueError("SEC_INDEX_ARCHIVE_INVALID")
    return raw


async def import_index(
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
            now = await session.scalar(select(func.clock_timestamp()))
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
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            snapshot, created = await persist_index_archive(
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


async def collect_index(
    settings: EngineSettings,
    *,
    identity_snapshot_id: UUID,
    archive_dir: Path,
    contact: str,
    store: ExhibitStore | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, object]:
    archive_store = _store(settings, store)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            identity = await _read_identity(
                session, archive_store, snapshot_id=identity_snapshot_id, as_of=now
            )
        collected = await collect_identity_sources(
            symbols=(identity.identity.symbol,),
            contact=contact,
            output_dir=archive_dir,
            client=client,
        )
        result = await import_index(
            settings,
            identity_snapshot_id=identity_snapshot_id,
            archive_dir=archive_dir,
            store=archive_store,
        )
        return {**result, "provider_requests": collected["requests"]}
    finally:
        await factory.kw["bind"].dispose()


async def inspect_index(
    settings: EngineSettings,
    *,
    snapshot_id: UUID,
    baseline_id: UUID | None = None,
    baseline_as_of: datetime | None = None,
    store: ExhibitStore | None = None,
) -> dict[str, object]:
    archive_store = _store(settings, store)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            if baseline_id is not None:
                if baseline_as_of is None:
                    raise ValueError("SEC_INDEX_BASELINE_REQUIRED")
                return await compare_index_captures(
                    session,
                    archive_store,
                    baseline_id=baseline_id,
                    current_id=snapshot_id,
                    baseline_as_of=baseline_as_of,
                    now=now,
                )
            row, index = await read_index_capture(
                session, archive_store, snapshot_id=snapshot_id, as_of=now
            )
            return {
                "status": "verified",
                "snapshot_id": str(row.id),
                "entries": len(index.entries),
                "source_fresh": now - index.received_at <= timedelta(minutes=5),
                "source_received_at": index.received_at.isoformat(),
                "available_at": row.available_at.isoformat(),
                "history_complete": index.history_complete,
                "execution_locked": True,
                "model_requests": 0,
                "provider_requests": 0,
            }
    finally:
        await factory.kw["bind"].dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("collect", "import"):
        command = commands.add_parser(action)
        command.add_argument("--identity-snapshot-id", type=UUID, required=True)
        command.add_argument("--archive-dir", type=Path, required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--snapshot-id", type=UUID, required=True)
    inspect.add_argument("--baseline-id", type=UUID)
    inspect.add_argument("--baseline-as-of", type=datetime.fromisoformat)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        settings = EngineSettings.model_validate(environment)
        if args.action == "inspect":
            operation = inspect_index(
                settings,
                snapshot_id=args.snapshot_id,
                baseline_id=args.baseline_id,
                baseline_as_of=args.baseline_as_of,
            )
        else:
            inputs = {
                "identity_snapshot_id": args.identity_snapshot_id,
                "archive_dir": args.archive_dir,
            }
            operation = (
                collect_index(settings, **inputs, contact=environment.get("SEC_CONTACT", ""))
                if args.action == "collect"
                else import_index(settings, **inputs)
            )
        print(json.dumps(asyncio.run(operation)))
    except Exception as error:
        code = str(error)
        if not isinstance(error, ValueError) or not re.fullmatch(r"SEC_INDEX_[A-Z_]+", code):
            code = "SEC_INDEX_UNAVAILABLE"
        print(json.dumps({"status": "unavailable", "error_code": code, "execution_locked": True}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
