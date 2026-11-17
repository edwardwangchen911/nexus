"""Synthetic originals through normal import, reader, SQL and discovery transactions."""

import json
import shutil
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from integration.test_monitor_disclosure_refresh_pg import settings_for_import, setup
from nexus.monitoring import durable_sec_host as host
from nexus.monitoring import durable_sec_storage as durable
from nexus.monitoring.batches import scan_stock_batch
from nexus.ops.durable_sec_index_local import import_durable_index
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent, SecNotificationClaim
from nexus.research.durable_sec_index_storage import read_durable_index_capture
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_expired_research_anchor_keeps_durable_host_discovery_and_no_recollection(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    monkeypatch.setattr(host, "configured_exhibit_store", lambda session: store)
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
            assert plan.valid_until < now
            claim = await host.claim_refresh(
                session,
                rule=rule,
                now=now,
                scheduled=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
                repo_root=tmp_path,
                contact="test@example.invalid",
            )
            assert claim is None, "fresh durable source must not recollect"
            check, notices = await host.scan_registered(session, rule, now)
            assert notices == 1
            assert (check.result, check.coverage) == ("true", "partial")
            assert len(check.observation["row_issues"]) == 1
            check_id = check.id
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            check2, notices = await host.scan_registered(session, rule, now)
            assert notices == 0
            assert check2.id == check_id
            assert await session.scalar(select(func.count()).select_from(SecDiscoveryEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 1
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
        async with factory() as session, session.begin():
            await durable.change_state(
                session, account_id=pointer.account_id, rule_id=rule.id, state="paused"
            )
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            claim = await host.claim_refresh(
                session,
                rule=rule,
                now=now,
                scheduled=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
                repo_root=tmp_path,
                contact="test@example.invalid",
            )
            assert claim["status"] == "paused"
            assert await host.scan_registered(session, rule, now) == (None, 0)
            with pytest.raises(ValueError, match="MONITOR_PLAN_UNAVAILABLE"):
                await scan_stock_batch(
                    session,
                    account_id=pointer.account_id,
                    instrument_id=pointer.instrument_id,
                    now=now,
                )
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
