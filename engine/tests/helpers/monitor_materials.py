"""Normal installed plans, sealed batch and exact local original import for mixed tests."""

import argparse
from datetime import timedelta
from uuid import UUID

from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from integration.test_monitor_filing_content_pg import archive
from nexus.monitoring.batches import scan_stock_batch
from nexus.ops.research_monitor import run
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy import select


async def prepared_materials(
    database_url, monkeypatch, root, *, sec_only=False, independent=False, exhibit_text=""
):
    factory, settings, store, captured, pointer = await installed(
        database_url,
        monkeypatch,
        root,
        kinds=("disclosure",) if sec_only else ("price", "disclosure"),
        independent=independent,
    )
    monkeypatch.setattr(
        "nexus.monitoring.filing_archive.configured_exhibit_store", lambda session: store
    )

    async def read_evidence(key, *, uri):
        assert uri == "s3://synthetic/" + key
        return store.objects[key]

    if not independent:
        monkeypatch.setattr(store, "read_evidence", read_evidence, raising=False)
    async with factory() as session, session.begin():
        observed = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    captured["clock"][0] = AT + timedelta(seconds=9)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session:
        current = await session.scalar(
            select(DataSnapshot)
            .where(DataSnapshot.source == "sec_disclosure_index")
            .order_by(DataSnapshot.created_at.desc())
        )
    entries = [
        row
        for row in current.payload["index"]["entries"]
        if row["accession_number"].endswith(("000002", "000003"))
    ]
    for number, entry in enumerate(entries):
        await run(
            settings,
            argparse.Namespace(
                action="import-content",
                index_snapshot_id=current.id,
                accession=entry["accession_number"],
                archive_dir=archive(
                    root / f"original-{number}",
                    entry,
                    extra=number == 1,
                    exhibit_text=exhibit_text if number == 0 else "",
                ),
            ),
        )
    prepared = await run(
        settings, argparse.Namespace(action="prepare-batch", batch_id=UUID(observed["batch_id"]))
    )
    assert prepared["status"] == "prepared"
    return (
        factory,
        settings,
        store,
        captured,
        pointer,
        UUID(observed["batch_id"]),
        UUID(prepared["snapshot_id"]),
    )
