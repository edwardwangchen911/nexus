"""Actual migration/ownership guard, with normally collected and persisted source material."""

import json
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from helpers.research_snapshots import seed_filing
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.research.current_price_trends_storage import persist_current_price_trends
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from .test_current_price_trends import INSTRUMENT, setup_trends


def test_empty_migration_roundtrip_retains_immutable_published_purpose(
    temporary_database_url, monkeypatch
):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0042_fmp_profiles")
    command.upgrade(config, "0043_current_price_trends")
    command.downgrade(config, "0042_fmp_profiles")
    command.upgrade(config, "0043_current_price_trends")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem",
    ["missing_received", "null_size", "missing_raw_text", "missing_name", "duplicate_raw"],
)
async def test_guard_rejects_incomplete_original_after_normal_source_import(
    problem,
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    _, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    async with factory.begin() as session:
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        evidence = await persist_current_price_trends(session, archive, store=store)
        row = await session.scalar(
            select(DataSnapshot).where(DataSnapshot.source == "alpaca_current_trends")
        )
        payload = json.loads(json.dumps(row.payload))
        if problem == "missing_received":
            del payload["originals"][0]["received_at"]
        elif problem == "null_size":
            payload["originals"][0]["byte_length"] = None
        elif problem == "missing_raw_text":
            del payload["originals"][0]["raw_text"]
        elif problem == "missing_name":
            del payload["originals"][0]["name"]
        else:
            payload["originals"][1] = payload["originals"][0]
        # Corrupt only source storage in this isolated DB. The snapshot ownership guard stays on.
        await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE data_snapshots SET payload=CAST(:payload AS jsonb) WHERE id=:id"),
            {"payload": json.dumps(payload), "id": row.id},
        )
        await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))
        session.expire(row)
        parent = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert str(evidence.id) not in parent.payload["evidence_ids"]
        with pytest.raises(IntegrityError, match="not owned by the instrument"):
            async with session.begin_nested():
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        data_snapshot_id=parent.id,
                        instrument_id=INSTRUMENT,
                        evidence_ref_id=evidence.id,
                        authority_kind="research",
                        selected_at=None,
                        source=evidence.source,
                        provider_object_id=evidence.provider_object_id,
                        revision=evidence.revision,
                        available_at=evidence.available_at,
                        location=evidence.location,
                        content_hash=evidence.content_hash,
                    )
                )
                await session.flush()
