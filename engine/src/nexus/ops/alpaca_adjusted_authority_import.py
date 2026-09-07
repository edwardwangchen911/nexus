"""Import one verified Alpaca all-adjusted archive as diagnostic price authority."""

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.market.ingestion import ingest_price_page
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.providers.market.alpaca import AlpacaMarketDataProvider, AlpacaRawResponse
from nexus.providers.market.alpaca_http import load_exact_json
from nexus.research.alpaca_prices_archive import ValidatedPriceArchive, load_price_collection
from nexus.settings import EngineSettings

_PAGE = re.compile(r"^page-\d{3}\.json$")


class _ArchivedTransport:
    def __init__(self, archive: ValidatedPriceArchive) -> None:
        pages = tuple(item for item in archive.originals if _PAGE.fullmatch(item.name))
        if len(pages) != 1:
            raise ValueError("ADJUSTED_AUTHORITY_SINGLE_PAGE_REQUIRED")
        payload = load_exact_json(pages[0].content)
        if not isinstance(payload, Mapping) or payload.get("next_page_token") is not None:
            raise ValueError("ADJUSTED_AUTHORITY_ARCHIVE_INVALID")
        self._archive = archive
        self._raw = pages[0]
        self._payload = payload

    async def fetch_bars(self, request, cursor):  # type: ignore[no-untyped-def]
        if request != self._archive.request or cursor is not None:
            raise ValueError("ADJUSTED_AUTHORITY_REQUEST_MISMATCH")
        return AlpacaRawResponse(
            payload=self._payload,
            raw_payload=self._raw.content,
            request_id=None,
        )


async def import_adjusted_authority(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    collection_dir: Path,
    store: S3ProviderEvidenceStore | None = None,
) -> dict[str, object]:
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
        raise ValueError("ADJUSTED_AUTHORITY_LOCAL_STORAGE_REQUIRED")
    archive = load_price_collection(collection_dir, instrument_id=instrument_id)
    if archive.request.adjustment != "all" or archive.request.interval != "1Day":
        raise ValueError("ADJUSTED_AUTHORITY_ALL_DAILY_REQUIRED")
    transport = _ArchivedTransport(archive)
    provider = AlpacaMarketDataProvider(
        transport,
        feed="sip",
        delivery_mode="delayed",
        now=lambda: archive.received_at,
    )
    page = await provider.fetch_bars(archive.request)
    if not page.items:
        raise ValueError("ADJUSTED_AUTHORITY_EMPTY")
    object_store = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    assert settings.database_url is not None
    engine = create_async_engine(
        settings.database_url, connect_args={"timeout": 10, "command_timeout": 15}
    )
    try:
        async with asyncio.timeout(120):
            async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
                await session.execute(
                    text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                    {"key": SCHEMA_ADMISSION_LOCK_KEY},
                )
                head = await session.scalar(
                    text(
                        "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END "
                        "FROM public.alembic_version"
                    )
                )
                if head != CURRENT_MIGRATION_HEAD:
                    raise ValueError("ADJUSTED_AUTHORITY_SCHEMA_NOT_READY")
                result = await ingest_price_page(session, object_store, page, commit=False)
                if result.price_authority is None or result.price_evidence_ref is None:
                    raise ValueError("ADJUSTED_AUTHORITY_NOT_FINALIZED")
                return {
                    "status": "stored_authority",
                    "manifest_id": str(result.manifest.id),
                    "authority_id": str(result.price_authority.id),
                    "content_hash": result.price_authority.content_hash,
                    "available_at": result.price_authority.available_at.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "bar_count": result.price_authority.bar_count,
                    "adjustment": result.manifest.adjustment,
                    "execution_locked": True,
                    "model_called": False,
                    "provider_called": False,
                }
    finally:
        await asyncio.wait_for(engine.dispose(), timeout=10)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("ADJUSTED_AUTHORITY_IMPORT_REQUEST_INVALID")


def main() -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--instrument-id", type=UUID, required=True)
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    started = False
    try:
        args = parser.parse_args()
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("ADJUSTED_AUTHORITY_IMPORT_OUTPUT_EXISTS")
        settings = EngineSettings.model_validate(
            load_local_environment(args.repo_root, os.environ)
        )
        started = True
        result = asyncio.run(
            import_adjusted_authority(
                settings,
                instrument_id=args.instrument_id,
                collection_dir=args.collection_dir,
            )
        )
        write_private(args.output, json.dumps(result, sort_keys=True).encode())
        print(
            json.dumps(
                {
                    "status": "stored_authority",
                    "provider_called": False,
                    "model_called": False,
                    "execution_locked": True,
                }
            )
        )
        return 0
    except KeyboardInterrupt:
        code = "ADJUSTED_AUTHORITY_IMPORT_INTERRUPTED"
    except Exception:
        code = "ADJUSTED_AUTHORITY_IMPORT_FAILED"
    if started:
        code = "ADJUSTED_AUTHORITY_IMPORT_OUTCOME_UNKNOWN"
    print(
        json.dumps({"status": "unknown" if started else "rejected", "code": code}),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
