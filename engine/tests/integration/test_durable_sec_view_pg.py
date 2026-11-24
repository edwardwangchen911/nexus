"""Synthetic originals through normal import, reader, SQL and discovery transactions."""

import json
import shutil
from datetime import timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import httpx
import pytest
from integration.test_monitor_disclosure_refresh_pg import settings_for_import, setup
from nexus.api.durable_sec import read_durable_sec_view
from nexus.api.routes.research import _research_wire
from nexus.api.schemas.agents import ResearchRunEnvelope, ResearchRunResponse
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.audit.service import append_audit_event
from nexus.monitoring import durable_sec_host as host
from nexus.monitoring import durable_sec_storage as durable
from nexus.monitoring.batches import scan_stock_batch
from nexus.notifications.worker import load_bark_worker_settings, run_bark_worker
from nexus.ops.durable_sec_index_local import import_durable_index
from nexus.ops.monitor_disclosure_refresh import CLAIM, FEED, RESULT, SCHEMA
from nexus.ops.sec_index_local import import_index
from nexus.outbox.failure import read_delivery_failure
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent, SecNotificationClaim
from nexus.research.durable_sec_index_storage import read_durable_index_capture
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError


async def _collection_claim(session, rule, identity_id, now):
    return await append_audit_event(
        session,
        actor_id="research-monitor",
        action=CLAIM,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "operation_id": str(uuid4()),
            "account_id": str(rule.account_id),
            "instrument_id": str(rule.instrument_id),
            "rule_id": str(rule.id),
            "durable_sec": True,
            "identity_snapshot_id": str(identity_id),
            "issuer_cik": rule.issuer_cik,
            "feed": FEED,
            "listed_history": False,
            "scheduled_at": now.replace(
                minute=now.minute // 5 * 5, second=0, microsecond=0
            ).isoformat(),
            "archive_dir": "/private/fixture-only",
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("server_response", ["accepted", "read_timeout", "ambiguous", "http_error"])
async def test_durable_sec_api_projection_outlives_anchor_without_research_authority(
    migrated_database_url, monkeypatch, tmp_path, server_response
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
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            assert await read_durable_sec_view(session, run, now=now) is None
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
            assert check.observation["evaluation_version"] == "accepted_cutoff_v1"
            assert check.observation["accepted_complete_through_at"] is None
            assert "coverage_proof" not in check.observation
            assert len(check.observation["row_issues"]) == 1
            check_id = check.id
            for result, coverage, observation in (
                ("unknown", "partial", {}),
                (
                    "unknown",
                    "complete",
                    {
                        "evaluation_version": "accepted_cutoff_v1",
                        "accepted_complete_through_at": None,
                    },
                ),
                (
                    "unknown",
                    "partial",
                    {
                        "evaluation_version": "accepted_cutoff_v1",
                        "accepted_complete_through_at": received.isoformat(),
                    },
                ),
            ):
                with pytest.raises(
                    IntegrityError, match="ck_sec_discovery_checks_accepted_cutoff_v1"
                ):
                    async with session.begin_nested():
                        await session.execute(
                            text(
                                "INSERT INTO sec_discovery_checks "
                                "(id, rule_id, capture_id, scheduled_at, checked_at, "
                                "result, coverage, observation, created_at) "
                                "SELECT gen_random_uuid(), rule_id, capture_id, "
                                "scheduled_at - interval '5 minutes', checked_at, "
                                ":result, :coverage, CAST(:observation AS jsonb), created_at "
                                "FROM sec_discovery_checks WHERE id=:check_id"
                            ),
                            {
                                "result": result,
                                "coverage": coverage,
                                "observation": json.dumps(observation),
                                "check_id": check_id,
                            },
                        )
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            check2, notices = await host.scan_registered(session, rule, now)
            assert notices == 0
            assert check2.id == check_id
            assert await session.scalar(select(func.count()).select_from(SecDiscoveryEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 1
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
            collection_claim = await _collection_claim(
                session, rule, capture.index.identity_snapshot_id, now
            )
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action=RESULT,
                schema_version=SCHEMA,
                occurred_at=now,
                payload={
                    **collection_claim.payload,
                    "status": "refreshed",
                    "snapshot_id": str(row.id),
                    "snapshot_hash": row.revision,
                    "provider_requests": 2,
                },
            )
        async with factory() as session, session.begin():
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            view = await read_durable_sec_view(
                session, run, now=now, include_notice=True, include_collection=True
            )
            assert view is not None and view.state == "active"
            assert view.latest_collection is not None
            assert view.latest_collection.status == "archived"
            assert view.latest_check.result == "true" and view.latest_check.coverage == "partial"
            assert view.latest_check.evaluation_version == "accepted_cutoff_v1"
            assert view.latest_check.source_received_at == received
            assert view.latest_check.accepted_complete_through_at is None
            assert view.event_count == 1 and len(view.events) == 1
            assert view.events[0].accepted_at < view.events[0].first_seen_at
            assert view.execution_locked is True
            queued = view.events[0].notice
            assert queued is not None and queued.status == "queued" and queued.attempt_count == 0
            assert queued.server_result == "unconfirmed" and queued.device_result == "unknown"
            before_attempt = now

        sent = []

        def respond(request):
            assert request.url.path == "/push"
            sent.append(request.url.path)
            if server_response == "read_timeout":
                raise httpx.ReadTimeout("private response text must not appear in the view")
            if server_response == "ambiguous":
                return httpx.Response(200, json={})
            if server_response == "http_error":
                return httpx.Response(500, json={"code": 500})
            return httpx.Response(200, json={"code": 200})

        bark = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": settings.database_url,
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": rule.effective_at.isoformat(),
            }
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as transport:
            assert await run_bark_worker(bark, run_forever=False, client=transport) == 1
            assert await run_bark_worker(bark, run_forever=False, client=transport) == 0
        assert sent == ["/push"]
        async with factory() as session, session.begin():
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            view = await read_durable_sec_view(
                session, run, now=now, include_notice=True, include_research=True
            )
            notice = view.events[0].notice
            assert notice is not None and notice.outbox_id == queued.outbox_id
            assert notice.attempt_count == 1 and notice.device_result == "unknown"
            assert before_attempt <= notice.attempted_at <= notice.finished_at <= now
            if server_response == "accepted":
                assert notice.status == "published" and notice.server_result == "accepted"
                assert notice.failure_reason is None
            else:
                assert notice.status == "dead_letter"
                assert notice.server_result == (
                    "http_error" if server_response == "http_error" else "unconfirmed"
                )
                assert (
                    notice.failure_reason
                    == {
                        "read_timeout": "READ_TIMEOUT",
                        "ambiguous": "PROVIDER_REJECTED",
                        "http_error": "HTTP_REJECTED",
                    }[server_response]
                )
                wrong_time = await read_delivery_failure(
                    session,
                    event_id=notice.outbox_id,
                    attempt_no=1,
                    as_of=now,
                    finished_at=notice.finished_at + timedelta(seconds=1),
                )
                assert wrong_time == {"stage": "unknown", "reason": "UNKNOWN"}
            earlier = await read_durable_sec_view(
                session, run, now=before_attempt, include_notice=True
            )
            assert earlier.event_count == 1 and earlier.events[0].notice == queued
            attempt = await session.scalar(select(OutboxDeliveryAttempt))
            pending_at = max(attempt.started_at, attempt.created_at)
            assert pending_at < attempt.finished_at
            inflight = await read_durable_sec_view(
                session, run, now=pending_at, include_notice=True
            )
            assert inflight.events[0].notice.status == "delivering"
            assert inflight.events[0].notice.finished_at is None
            assert inflight.events[0].notice.server_result == "unconfirmed"

            envelope = ResearchRunEnvelope.model_construct(
                data=ResearchRunResponse.model_construct(durable_disclosure=view),
                meta=SuccessMetaResponse(request_id=UUID(int=1), as_of=now),
            )
            baseline_wire = json.loads(_research_wire(envelope, "1").body)
            opted_in_wire = json.loads(_research_wire(envelope, "1", include_sec_notices=True).body)
            research_wire = json.loads(
                _research_wire(envelope, "1", include_sec_research=True).body
            )["data"]["durable_disclosure"]["events"][0]
            assert "notice" not in baseline_wire["data"]["durable_disclosure"]["events"][0]
            assert "research_status" not in opted_in_wire["data"]["durable_disclosure"]["events"][0]
            assert research_wire["research_status"] == "no_receipt"
            assert "notice" not in research_wire
            assert opted_in_wire["data"]["durable_disclosure"]["events"][0]["notice"] == (
                notice.model_dump(mode="json")
            )
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
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_durable_sec_view(session, run, now=now)
            assert view.state == "paused" and view.event_count == 1
            assert view.latest_check.result == "true"
            assert await host.scan_registered(session, rule, now) == (None, 0)
            with pytest.raises(ValueError, match="MONITOR_PLAN_UNAVAILABLE"):
                await scan_stock_batch(
                    session,
                    account_id=pointer.account_id,
                    instrument_id=pointer.instrument_id,
                    now=now,
                )
        async with factory() as session, session.begin():
            await durable.change_state(
                session, account_id=pointer.account_id, rule_id=rule.id, state="revoked"
            )
        async with factory() as session:
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_durable_sec_view(session, run, now=now)
            assert view.state == "revoked" and view.event_count == 1
            assert view.latest_check.interval_start == rule.effective_at
            assert view.latest_check.interval_end <= view.latest_check.checked_at
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
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
