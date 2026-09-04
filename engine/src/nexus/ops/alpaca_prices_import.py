"""Import a completed private split-price archive as locked current research context."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.research.alpaca_prices_archive import ValidatedPriceArchive, load_price_collection
from nexus.research.current_price_trends_storage import (
    persist_current_price_trends,
    read_current_price_trends,
)
from nexus.settings import EngineSettings


async def import_trends(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    collection_dir: Path,
    store: S3ProviderEvidenceStore | None = None,
) -> dict[str, object]:
    """No collection/model/price authority; caller receives only verified stored evidence."""
    database = make_url(settings.database_url) if settings.database_url else None
    endpoint = urlsplit(settings.object_store_endpoint or "")
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        database is None
        or database.drivername != "postgresql+asyncpg"
        or database.host not in local
        or database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("CURRENT_TRENDS_LOCAL_STORAGE_REQUIRED")
    archive = load_price_collection(collection_dir, instrument_id=instrument_id)
    object_store = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    assert settings.database_url is not None
    engine = create_async_engine(
        settings.database_url, connect_args={"timeout": 10, "command_timeout": 15}
    )
    try:
        async with asyncio.timeout(120):
            async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
                return await _import_records(session, archive, instrument_id, object_store)
    finally:
        await asyncio.wait_for(engine.dispose(), timeout=10)


async def _import_records(
    session: AsyncSession,
    archive: ValidatedPriceArchive,
    instrument_id: UUID,
    object_store: S3ProviderEvidenceStore,
) -> dict[str, object]:
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
        {"key": SCHEMA_ADMISSION_LOCK_KEY},
    )
    head = await session.scalar(
        text("SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END FROM public.alembic_version")
    )
    if head != CURRENT_MIGRATION_HEAD:
        raise ValueError("CURRENT_TRENDS_SCHEMA_NOT_READY")
    evidence = await persist_current_price_trends(session, archive, store=object_store)
    documents = await read_current_price_trends(
        session, instrument_id=instrument_id, as_of=datetime.now(UTC), evidence_ids=(evidence.id,)
    )
    document = documents.get(evidence.id)
    if document is None:
        raise ValueError("CURRENT_TRENDS_IMPORT_READBACK_FAILED")
    return {
        "status": "stored_context",
        "evidence_id": str(evidence.id),
        "content_hash": document.content_hash,
        "available_at": document.available_at.isoformat().replace("+00:00", "Z"),
        "last_observed_on": document.last_observed_on.isoformat()
        if document.last_observed_on
        else None,
        "research_created": False,
        "historical_backtest_eligible": False,
        "market_price_eligible": False,
        "total_return_eligible": False,
        "execution_locked": True,
        "model_called": False,
    }


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("CURRENT_TRENDS_IMPORT_REQUEST_INVALID")


def main() -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--instrument-id", type=UUID, required=True)
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    started_import = False
    try:
        args = parser.parse_args()
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("CURRENT_TRENDS_IMPORT_OUTPUT_EXISTS")
        environment = load_local_environment(args.repo_root, os.environ)
        settings = EngineSettings.model_validate(environment)
        started_import = True
        result = asyncio.run(
            import_trends(
                settings, instrument_id=args.instrument_id, collection_dir=args.collection_dir
            )
        )
        # DB may already be committed if this local result-file write fails; replay is idempotent.
        write_private(args.output, json.dumps(result, sort_keys=True).encode())
        print(
            json.dumps(
                {"status": "stored_context", "model_called": False, "research_created": False}
            )
        )
        return 0
    except KeyboardInterrupt:
        code = "CURRENT_TRENDS_IMPORT_INTERRUPTED"
    except Exception:
        code = "CURRENT_TRENDS_IMPORT_FAILED"
    if started_import:
        code = "CURRENT_TRENDS_IMPORT_OUTCOME_UNKNOWN"
    print(
        json.dumps({"status": "unknown" if started_import else "rejected", "code": code}),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
