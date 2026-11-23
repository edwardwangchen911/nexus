"""Export exact unqualified review objects from one configured scope, without SQL writes."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from sqlalchemy import select, text

from nexus.domain.time import require_utc
from nexus.monitoring.filing_material import rebuild_material
from nexus.monitoring.filing_originals import read_originals
from nexus.monitoring.filing_review import ERROR
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_recovery import write_private
from nexus.ops.monitor_content_local import _private_directory
from nexus.persistence.migrations import SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.sec_monitor import SecMonitorRule
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def prepare_review_material(
    settings: EngineSettings, *, snapshot_id: UUID, output_dir: Path
) -> dict[str, object]:
    if settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_ACCOUNT_UNCONFIGURED")
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as db, db.begin():
            await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await db.execute(text("SET LOCAL statement_timeout='5s'"))
            await db.execute(text("SET LOCAL lock_timeout='2s'"))
            await db.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            schema = await db.scalar(text("SELECT version_num FROM alembic_version"))
            # This export only reads the unchanged archive/scope contracts. It
            # cannot enter any admission or dispatch path on either schema.
            if schema not in {
                "0134_sec_foundation",
                "0135_routine_budget",
                "0136_sec_adopted_content",
                "0137_linear_json_canonical",
                "0138_sec_accepted_cutoff",
                "0139_sec_audit_lookup",
            }:
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            now = await db.scalar(text("SELECT clock_timestamp()"))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            now = require_utc(now)
            document = await read_originals(db, snapshot_id=snapshot_id, as_of=now)
            if (
                await db.scalar(
                    select(SecMonitorRule.id).where(
                        SecMonitorRule.account_id == settings.agent_paper_account_id,
                        SecMonitorRule.instrument_id == document.instrument_id,
                        SecMonitorRule.issuer_cik == document.issuer_cik,
                    )
                )
                is None
            ):
                raise ValueError("SEC_DURABLE_SCOPE_INVALID")
            material, pairs = await rebuild_material(
                db, snapshot_id=snapshot_id, prepared_at=now, schema=schema
            )
        canonical = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
        if len(canonical) > 32 * 1024 * 1024:
            raise ValueError(ERROR)
        digest = sha256(canonical).hexdigest()
        _private_directory(output_dir)
        for name, (raw, receipt) in pairs.items():
            write_private(output_dir / name, raw)
            write_private(output_dir / (name + ".receipt.json"), receipt)
            if (output_dir / name).read_bytes() != raw or (
                output_dir / (name + ".receipt.json")
            ).read_bytes() != receipt:
                raise ValueError(ERROR)
        write_private(output_dir / "material.json", canonical)
        write_private(
            output_dir / "complete.json",
            json.dumps(
                {
                    "schema_version": "sec_content_review_export_v1",
                    "material_sha256": digest,
                    "original_snapshot_id": str(snapshot_id),
                    "original_pairs": len(pairs),
                    "research_qualified": False,
                },
                sort_keys=True,
            ).encode(),
        )
        return {
            "status": "prepared_unqualified",
            "snapshot_id": str(snapshot_id),
            "material_sha256": digest,
            "original_pairs": len(pairs),
            "research_qualified": False,
            "authority_status": "unverified",
            "model_requests": 0,
            "provider_requests": 0,
            "job_created": False,
            "execution_locked": True,
        }
    finally:
        await factory.kw["bind"].dispose()
