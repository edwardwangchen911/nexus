"""Explicit local import of already archived evidence; never collect, prepare or run models."""

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.session import session_factory
from nexus.research.sec_exhibit_storage import ExhibitStore, persist_exhibit_archive
from nexus.research.sec_exhibits import MAX_RAW_BYTES
from nexus.settings import EngineSettings


async def import_sec_exhibit(
    settings: EngineSettings,
    *,
    identity_snapshot_id: UUID,
    archives: Sequence[tuple[Path, Path]],
    store: ExhibitStore | None = None,
) -> dict[str, object]:
    if not settings.database_url or make_url(settings.database_url).host not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise ValueError("SEC_EXHIBIT_LOCAL_DATABASE_REQUIRED")
    if store is None:
        if urlsplit(settings.object_store_endpoint or "").hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise ValueError("SEC_EXHIBIT_LOCAL_STORE_REQUIRED")
        store = S3ProviderEvidenceStore(settings)
    if len(archives) != 3:
        raise ValueError("SEC_EXHIBIT_ARCHIVES_REQUIRED")
    contents = []
    for raw, receipt in archives:
        if raw.stat().st_size > MAX_RAW_BYTES or receipt.stat().st_size > 65536:
            raise ValueError("SEC_EXHIBIT_ORIGINAL_TOO_LARGE")
        contents.append((raw.read_bytes(), receipt.read_bytes()))
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            result = await persist_exhibit_archive(
                session,
                store,
                identity_snapshot_id=identity_snapshot_id,
                archives=contents,
                now=now,
            )
            return {
                **result,
                "provider_requests": 0,
                "model_requests": 0,
                "research_completed": False,
                "execution_locked": True,
            }
    finally:
        await factory.kw["bind"].dispose()
