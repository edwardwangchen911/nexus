"""Synthetic originals through normal import, reader, SQL and discovery transactions."""

import json
import shutil
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from integration.test_monitor_disclosure_refresh_pg import settings_for_import, setup
from nexus.domain.ids import new_uuid7
from nexus.monitoring import durable_sec_storage as durable
from nexus.monitoring.durable_sec_claims import audit_fact
from nexus.notifications.durable_sec_gate import notice_claim_condition
from nexus.ops.durable_sec_index_local import import_durable_index
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecNotificationClaim,
)
from nexus.research.durable_sec_index_storage import read_durable_index_capture
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_bad_historical_row_preserves_new_event_normal_chain(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    archive = tmp_path / "durable-originals"
    shutil.copytree(tmp_path / "baseline", archive)
    try:
        async with factory() as session, session.begin():
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
        async with factory() as session:
            received = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        assert received > rule.effective_at
        path = archive / "AAPL-submissions.json"
        document = json.loads(path.read_bytes())
        recent = document["filings"]["recent"]
        recent["acceptanceDateTime"][0] = (received + timedelta(days=1)).isoformat()
        for key, value in {
            "accessionNumber": "0000123456-26-000099",
            "form": "8-K",
            "acceptanceDateTime": received.isoformat(),
            "filingDate": received.date().isoformat(),
            "primaryDocument": "independent-new.htm",
        }.items():
            recent[key].append(value)
        path.write_text(json.dumps(document))
        for name in ("directory", "AAPL-submissions"):
            raw = (archive / f"{name}.json").read_bytes()
            receipt_path = archive / f"{name}-receipt.json"
            receipt = json.loads(receipt_path.read_bytes())
            receipt.update(
                received_at=received.isoformat(),
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
            )
            receipt_path.write_text(json.dumps(receipt))
        settings = settings_for_import(factory)
        # The historical strict contract is unchanged; it cannot silently become tolerant.
        with pytest.raises(ValueError):
            await import_index(
                settings,
                identity_snapshot_id=captured["identity"].snapshot_id,
                archive_dir=archive,
                store=store,
            )
        first = await import_durable_index(
            settings,
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=archive,
            store=store,
        )
        again = await import_durable_index(
            settings,
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=archive,
            store=store,
        )
        assert again["snapshot_id"] == first["snapshot_id"] and again["status"] == "existing"
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            row, capture = await read_durable_index_capture(
                session, store, snapshot_id=UUID(first["snapshot_id"]), as_of=now
            )
            assert len(capture.issues) == 1 and len(capture.index.entries) == 1
            assert capture.index.history_complete is False
            check = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=row.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            )
            assert (check.result, check.coverage) == ("true", "partial")
            assert len(check.observation["row_issues"]) == 1
            check_id = check.id
        async with factory() as session, session.begin():
            check2 = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=row.id,
                scheduled_at=check.scheduled_at,
            )
            assert check2.id == check_id
            assert await session.scalar(select(func.count()).select_from(SecDiscoveryEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 1
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
        # Constraint/delivery subcase uses the real imported metadata capture,
        # not a fabricated snapshot. A conflict must occupy X without rewriting it.
        changed = tmp_path / "changed-metadata"
        shutil.copytree(archive, changed)
        async with factory() as session:
            later = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            event = await session.scalar(select(SecDiscoveryEvent))
            original_entry = dict(event.entry)
        changed_path = changed / "AAPL-submissions.json"
        changed_document = json.loads(changed_path.read_bytes())
        changed_recent = changed_document["filings"]["recent"]
        changed_recent["form"][-1] = "10-K"
        changed_recent["acceptanceDateTime"][-1] = (
            rule.effective_at - timedelta(seconds=1)
        ).isoformat()
        changed_raw = json.dumps(changed_document).encode()
        changed_path.write_bytes(changed_raw)
        changed_receipt_path = changed / "AAPL-submissions-receipt.json"
        changed_receipt = json.loads(changed_receipt_path.read_bytes())
        changed_receipt.update(
            received_at=later.isoformat(),
            sha256=sha256(changed_raw).hexdigest(),
            byte_length=len(changed_raw),
        )
        changed_receipt_path.write_text(json.dumps(changed_receipt))
        revised = await import_durable_index(
            settings,
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=changed,
            store=store,
        )
        async with factory() as session, session.begin():
            at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            revised_row, revised_capture = await read_durable_index_capture(
                session, store, snapshot_id=UUID(revised["snapshot_id"]), as_of=at
            )
            fact = next(
                v for v in revised_capture.metadata_entries if v.accession_number.endswith("000099")
            )
            assert fact.form == "10-K" and fact.accepted_at < rule.effective_at
            revision = SecDiscoveryEventRevision(
                id=new_uuid7(),
                created_at=at,
                event_id=event.id,
                capture_id=revised_row.id,
                entry=fact.model_dump(mode="json"),
                metadata_hash=durable._metadata(fact),
                observed_at=at,
            )
            session.add(revision)
            await audit_fact(session, revision)
        async with factory() as session:
            saved = await session.get(SecDiscoveryEvent, event.id)
            assert saved.entry == original_entry
            assert (
                await session.scalar(select(OutboxEvent.id).where(notice_claim_condition())) is None
            )
            assert await session.scalar(select(OutboxEvent.status)) == "queued"
        # Shared source corruption fails the normal importer, without creating another event.
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(ValueError):
            await import_durable_index(
                settings,
                identity_snapshot_id=captured["identity"].snapshot_id,
                archive_dir=archive,
                store=store,
            )
        assert not calls
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
