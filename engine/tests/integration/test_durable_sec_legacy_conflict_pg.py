import asyncio
import json
import shutil
from datetime import timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import httpx
import pytest
from integration.test_monitor_disclosure_refresh_pg import settings_for_import, setup
from nexus.monitoring import durable_sec_storage as durable
from nexus.monitoring.batches import scan_stock_batch
from nexus.notifications.durable_sec_gate import DurableSecPublisher, notice_claim_condition
from nexus.ops.durable_sec_index_local import import_durable_index
from nexus.outbox.dispatcher import _claim, _complete, _reclaim_expired
from nexus.outbox.models import OutboxDeliveryIdentity, OutboxMessage
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["queued", "claimed", "sending"])
async def test_normal_legacy_only_key_conflict_blocks_original_notice(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    preclaimed = mode != "queued"
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    try:
        async with factory() as session, session.begin():
            await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            trigger = await session.scalar(
                select(MonitorTrigger)
                .where(MonitorTrigger.kind == "disclosure")
                .order_by(MonitorTrigger.id)
            )
            check = await session.get(MonitorCheck, trigger.check_id)
            original_entry = check.observation["entry"]
            original = await session.get(OutboxEvent, trigger.outbox_id)
            original_payload = dict(original.payload)
            plan = await session.get(MonitorPlan, check.plan_id)
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
        async with factory() as session, session.begin():
            received = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            claimed = None
            if preclaimed:
                claimed = await _claim(
                    session,
                    worker_id="old-sec-worker",
                    now=received,
                    lease_seconds=60,
                    event_keys=None,
                    created_at_or_after=None,
                    claim_condition=(OutboxEvent.id == original.id),
                )
                assert claimed is not None
        before_attempts = 1 if preclaimed else 0
        archive = tmp_path / "conflicting-index"
        shutil.copytree(tmp_path / "baseline", archive)
        path = archive / "AAPL-submissions.json"
        doc = json.loads(path.read_bytes())
        recent = doc["filings"]["recent"]
        accession = original_entry["accession_number"]
        if accession in recent["accessionNumber"]:
            index = recent["accessionNumber"].index(accession)
            recent["form"][index] = "10-K"
        else:
            for key, value in {
                "accessionNumber": accession,
                "form": "10-K",
                "acceptanceDateTime": original_entry["accepted_at"],
                "filingDate": original_entry["filed_at"][:10],
                "primaryDocument": original_entry["primary_document"],
            }.items():
                recent[key].append(value)
        for key, value in {
            "accessionNumber": "0000123456-26-000099",
            "form": "8-K",
            "acceptanceDateTime": received.isoformat(),
            "filingDate": received.date().isoformat(),
            "primaryDocument": "good-new.htm",
        }.items():
            recent[key].append(value)
        path.write_text(json.dumps(doc))
        for name in ("directory", "AAPL-submissions"):
            raw = (archive / f"{name}.json").read_bytes()
            rp = archive / f"{name}-receipt.json"
            receipt = json.loads(rp.read_bytes())
            receipt.update(
                received_at=received.isoformat(),
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
            )
            rp.write_text(json.dumps(receipt))
        imported = await import_durable_index(
            settings_for_import(factory),
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=archive,
            store=store,
        )
        # A correct source conflict without its exact audit cannot commit.
        from nexus.persistence.models.data import DataSnapshot
        from nexus.research.durable_sec_index import SecMetadataFact
        from sqlalchemy.exc import DBAPIError

        async with factory() as session:
            capture = await session.get(DataSnapshot, UUID(imported["snapshot_id"]))
            fact = next(
                v for v in capture.payload["metadata_entries"] if v["accession_number"] == accession
            )
        forged = dict(fact, primary_document="not-in-the-source.htm")
        with pytest.raises(DBAPIError, match="SEC_KEY_CONFLICT_INVALID"):
            async with factory() as session, session.begin():
                at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                session.add(
                    SecDisclosureKeyConflict(
                        id=uuid4(),
                        account_id=pointer.account_id,
                        instrument_id=pointer.instrument_id,
                        event_key=trigger.event_key,
                        rule_id=rule.id,
                        capture_id=capture.id,
                        original_event_id=None,
                        legacy_trigger_id=trigger.id,
                        entry=forged,
                        metadata_hash=durable._metadata(SecMetadataFact.model_validate(forged)),
                        observed_at=at,
                        created_at=at,
                    )
                )
                await session.flush()
        with pytest.raises(DBAPIError, match="SEC_KEY_CONFLICT_AUDIT_INVALID"):
            async with factory() as session, session.begin():
                at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                session.add(
                    SecDisclosureKeyConflict(
                        id=uuid4(),
                        account_id=pointer.account_id,
                        instrument_id=pointer.instrument_id,
                        event_key=trigger.event_key,
                        rule_id=rule.id,
                        capture_id=capture.id,
                        original_event_id=None,
                        legacy_trigger_id=trigger.id,
                        entry=fact,
                        metadata_hash=durable._metadata(SecMetadataFact.model_validate(fact)),
                        observed_at=at,
                        created_at=at,
                    )
                )
                await session.flush()
        conflict_started = asyncio.Future()

        async def record_current():
            async with factory() as session, session.begin():
                pid = await session.scalar(text("SELECT pg_backend_pid()"))
                if not conflict_started.done():
                    conflict_started.set_result(pid)
                at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                discovery = await durable.record_discovery(
                    session,
                    account_id=pointer.account_id,
                    rule_id=rule.id,
                    capture_id=UUID(imported["snapshot_id"]),
                    scheduled_at=at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0),
                )
                assert (discovery.result, discovery.coverage) == ("true", "partial")

        if mode == "sending":
            from nexus.notifications.bark import BarkPublisher

            entered, release = asyncio.Event(), asyncio.Event()
            sends = []

            async def transport(request):
                sends.append(request.method)
                entered.set()
                await release.wait()
                return httpx.Response(200, json={"code": 200})

            async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http_client:
                bark = BarkPublisher(
                    client=http_client,
                    server_url="http://127.0.0.1:8080",
                    device_key="test-key",
                    encryption_key=b"0123456789abcdef",
                )
                message = OutboxMessage(
                    claimed.event_id,
                    claimed.event_type,
                    claimed.aggregate_id,
                    claimed.schema_version,
                    claimed.payload,
                    OutboxDeliveryIdentity(
                        claimed.attempt_no,
                        claimed.worker_id,
                        claimed.lease_token,
                        claimed.expires_at,
                    ),
                )
                sending = asyncio.create_task(DurableSecPublisher(factory, bark)(message))
                await asyncio.wait_for(entered.wait(), 10)
                recording = asyncio.create_task(record_current())
                try:
                    pid = await asyncio.wait_for(conflict_started, 10)

                    async def wait_for_lock():
                        while True:
                            async with factory() as session:
                                waiting = await session.scalar(
                                    text(
                                        "SELECT wait_event_type FROM pg_stat_activity "
                                        "WHERE pid=:pid"
                                    ),
                                    {"pid": pid},
                                )
                            if waiting == "Lock":
                                return
                            await asyncio.sleep(0.02)

                    await asyncio.wait_for(wait_for_lock(), 10)
                    assert not recording.done(), (
                        "conflict publication must wait for the original send"
                    )
                finally:
                    release.set()
                    await sending
                    await recording
                assert sends == ["POST"]
            async with factory() as session, session.begin():
                at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                await _complete(session, claim=claimed, now=at)
        else:
            await record_current()
        async with factory() as session:
            permitted = await session.scalar(
                select(OutboxEvent.id).where(
                    OutboxEvent.id == original.id, notice_claim_condition()
                )
            )
            assert permitted is None, "conflicted legacy stable key must not be claimed"
            saved = await session.get(OutboxEvent, original.id)
            assert (
                saved.status
                == ("published" if mode == "sending" else "delivering" if preclaimed else "queued")
                and saved.attempt_count == before_attempts
                and saved.payload == original_payload
            )
        async with factory() as session:
            conflicts = tuple(await session.scalars(select(SecDisclosureKeyConflict)))
            assert len(conflicts) == 1 and conflicts[0].legacy_trigger_id == trigger.id
            assert (
                await session.scalar(
                    select(SecDiscoveryEvent.id).where(
                        SecDiscoveryEvent.event_key == trigger.event_key
                    )
                )
                is None
            )
            assert (
                await session.scalar(
                    select(OutboxEvent.id).where(
                        OutboxEvent.event_type == "research.sec.discovered",
                        notice_claim_condition(),
                    )
                )
                is not None
            )
            assert (
                len(tuple(await session.scalars(select(OutboxDeliveryAttempt)))) == before_attempts
            )
        if mode == "queued":
            from nexus.outbox.dispatcher import dispatch_outbox_once
            from nexus.outbox.models import PublisherSpec
            from nexus.outbox.registry import PublisherRegistry

            attempted = []

            async def forbidden_queued(message):
                attempted.append(message.event_id)

            assert not await dispatch_outbox_once(
                factory,
                registry=PublisherRegistry(
                    (
                        PublisherSpec(
                            original.event_type, original.schema_version, True, forbidden_queued
                        ),
                    )
                ),
                worker_id="queued-conflict",
                clock=lambda: received,
                claim_condition=(OutboxEvent.id == original.id) & notice_claim_condition(),
            )
            assert attempted == []
        if claimed is not None:
            posts = []

            async def forbidden(message):
                posts.append(message)

            message = OutboxMessage(
                claimed.event_id,
                claimed.event_type,
                claimed.aggregate_id,
                claimed.schema_version,
                claimed.payload,
                OutboxDeliveryIdentity(
                    claimed.attempt_no, claimed.worker_id, claimed.lease_token, claimed.expires_at
                ),
            )
            with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_NOT_ACTIVE"):
                await DurableSecPublisher(factory, forbidden)(message)
            assert posts == []
        # Invalid shared original cannot manufacture a second conflict.
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(ValueError):
            await import_durable_index(
                settings_for_import(factory),
                identity_snapshot_id=captured["identity"].snapshot_id,
                archive_dir=archive,
                store=store,
            )
        assert not calls
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_old_delivery_identity_cannot_use_reclaimed_attempt(migrated_database_url):
    from nexus.persistence.session import session_factory
    from nexus.settings import EngineSettings

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            row = OutboxEvent(
                id=uuid4(),
                event_type="test.sec.lease",
                schema_version="v1",
                aggregate_id=uuid4(),
                payload={},
                status="queued",
                attempt_count=0,
                max_attempts=2,
                available_at=now - timedelta(minutes=3),
                created_at=now - timedelta(minutes=3),
            )
            session.add(row)
        async with factory() as session, session.begin():
            old = await _claim(
                session,
                worker_id="old",
                now=now - timedelta(minutes=2),
                lease_seconds=60,
                event_keys=(("test.sec.lease", "v1"),),
                created_at_or_after=None,
            )
        async with factory() as session, session.begin():
            assert await _reclaim_expired(session, now=now) == 1
            current = await _claim(
                session,
                worker_id="new",
                now=now,
                lease_seconds=60,
                event_keys=(("test.sec.lease", "v1"),),
                created_at_or_after=None,
            )

        def message(claim):
            return OutboxMessage(
                claim.event_id,
                claim.event_type,
                claim.aggregate_id,
                claim.schema_version,
                claim.payload,
                OutboxDeliveryIdentity(
                    claim.attempt_no, claim.worker_id, claim.lease_token, claim.expires_at
                ),
            )

        sent = []

        async def publisher(msg):
            sent.append(msg.event_id)

        guarded = DurableSecPublisher(factory, publisher)
        with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_NOT_ACTIVE"):
            await guarded(message(old))
        assert sent == []
        await guarded(message(current))
        assert sent == [row.id]
        async with factory() as session, session.begin():
            await _complete(session, claim=current, now=now)
        async with factory() as session:
            saved = await session.get(OutboxEvent, row.id)
            assert saved.status == "published" and saved.attempt_count == 2
            attempts = tuple(
                await session.scalars(
                    select(OutboxDeliveryAttempt).order_by(OutboxDeliveryAttempt.attempt_no)
                )
            )
            assert [a.outcome for a in attempts] == ["lost", "published"]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("pause_first", [True, False])
async def test_pause_send_order_preserves_original_cleanup(
    migrated_database_url, monkeypatch, tmp_path, pause_first
):
    from nexus.notifications.bark import BarkPublisher

    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    try:
        async with factory() as session, session.begin():
            await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            claim = await _claim(
                session,
                worker_id="pause-fence",
                now=now,
                lease_seconds=60,
                event_keys=None,
                created_at_or_after=None,
                claim_condition=notice_claim_condition(),
            )
        assert claim is not None
        message = OutboxMessage(
            claim.event_id,
            claim.event_type,
            claim.aggregate_id,
            claim.schema_version,
            claim.payload,
            OutboxDeliveryIdentity(
                claim.attempt_no, claim.worker_id, claim.lease_token, claim.expires_at
            ),
        )
        started = asyncio.Future()

        async def pause():
            async with factory() as session, session.begin():
                pid = await session.scalar(text("SELECT pg_backend_pid()"))
                started.set_result(pid)
                await durable.change_state(
                    session, account_id=pointer.account_id, rule_id=rule.id, state="paused"
                )

        entered, release = asyncio.Event(), asyncio.Event()
        posts = []

        async def transport(request):
            posts.append(request.method)
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"code": 200})

        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http_client:
            publisher = DurableSecPublisher(
                factory,
                BarkPublisher(
                    client=http_client,
                    server_url="http://127.0.0.1:8080",
                    device_key="test-key",
                    encryption_key=b"0123456789abcdef",
                ),
            )
            if pause_first:
                await pause()
                with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_NOT_ACTIVE"):
                    await publisher(message)
                assert posts == []
            else:
                sending = asyncio.create_task(publisher(message))
                await asyncio.wait_for(entered.wait(), 10)
                pausing = asyncio.create_task(pause())
                try:
                    pid = await asyncio.wait_for(started, 10)

                    async def wait_locked():
                        while True:
                            async with factory() as session:
                                waiting = await session.scalar(
                                    text(
                                        "SELECT wait_event_type FROM pg_stat_activity "
                                        "WHERE pid=:pid"
                                    ),
                                    {"pid": pid},
                                )
                            if waiting == "Lock":
                                return
                            await asyncio.sleep(0.02)

                    await asyncio.wait_for(wait_locked(), 10)
                    assert not pausing.done()
                finally:
                    release.set()
                    await sending
                    await pausing
                async with factory() as session, session.begin():
                    now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                    await _complete(session, claim=claim, now=now)
                assert posts == ["POST"]
        async with factory() as session:
            event = await session.get(OutboxEvent, claim.event_id)
            assert event.status == ("delivering" if pause_first else "published")
            assert event.attempt_count == 1
        assert not calls
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


def test_empty_conflict_migration_roundtrip(migrated_database_url):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    command.downgrade(config, "0128_sec_durable_baseline")
    command.upgrade(config, "head")
