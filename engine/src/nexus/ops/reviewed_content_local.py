"""Read-only verification of one already adopted filing through normal configuration."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from nexus.monitoring.filing_adopted_content import build_adopted_archive
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.persistence.migrations import SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def inspect_reviewed_content(
    settings: EngineSettings, *, adoption_id: UUID
) -> dict[str, object]:
    if settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_ACCOUNT_UNCONFIGURED")
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as db, db.begin():
            await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await db.execute(text("SET LOCAL statement_timeout='10s'"))
            await db.execute(text("SET LOCAL lock_timeout='2s'"))
            await db.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            schema = await db.scalar(text("SELECT version_num FROM alembic_version"))
            if schema not in {
                "0134_sec_foundation",
                "0135_routine_budget",
                "0136_sec_adopted_content",
            }:
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            now = await db.scalar(text("SELECT clock_timestamp()"))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            document = await build_adopted_archive(
                db, adoption_id=adoption_id, account_id=settings.agent_paper_account_id, now=now
            )
            return {
                "status": "reviewed_content_verified_readonly",
                "content_schema": document.schema_version,
                "adoption_snapshot_id": str(adoption_id),
                "documents": len(document.documents),
                "document_characters": {d.filename: len(d.text) for d in document.documents},
                "dependency_proof": document.dependency_proof,
                "research_authority_granted": False,
                "model_requests": 0,
                "provider_requests": 0,
                "job_created": False,
                "execution_locked": True,
            }
    finally:
        await factory.kw["bind"].dispose()
