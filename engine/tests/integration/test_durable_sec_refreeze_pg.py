"""Actual-current normal ordinary research binds a new SEC event without another notice."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_conflicting_x_keeps_y_and_merges_z_append_only(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers import disclosure_observations as reader
    from integration import test_candidate_multistock_path as candidate
    from integration import test_monitor_batches_pg as batches
    from integration import test_monitor_disclosure_refresh_pg as refresh_fixture
    from integration import test_monitor_filing_content_pg as filing_fixture
    from nexus.monitoring import durable_sec_research as bridge
    from nexus.monitoring import durable_sec_storage as durable
    from nexus.ops.research_monitor import run
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.monitoring import MonitorPlan
    from nexus.persistence.models.runtime import Job, OutboxEvent
    from nexus.persistence.models.sec_monitor import SecNotificationClaim
    from nexus.persistence.models.sec_research import SecEventResearchBinding
    from nexus.research.durable_sec_index_storage import persist_durable_index_archive
    from sqlalchemy.ext.asyncio import AsyncSession

    original_scalar = AsyncSession.scalar
    now = datetime.now(UTC)
    at = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if at >= now:
        at -= timedelta(days=1)
    while at.weekday() > 4:
        at -= timedelta(days=1)
    for module in (candidate, batches, refresh_fixture, filing_fixture, reader):
        monkeypatch.setattr(module, "AT", at)
    factory, settings, store, captured, pointer = await batches.installed(
        migrated_database_url, monkeypatch, tmp_path, kinds=("disclosure",), independent=True
    )
    monkeypatch.setattr(AsyncSession, "scalar", original_scalar)
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    monkeypatch.setattr(bridge, "configured_exhibit_store", lambda session: store)
    monkeypatch.setattr(
        "nexus.monitoring.filing_archive.configured_exhibit_store", lambda session: store
    )
    try:
        async with factory() as session, session.begin():
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
            assert (
                await session.scalar(
                    text("SELECT single_monitor_root_request(:run)"), {"run": plan.source_run_id}
                )
                is None
            )
            assert (
                await session.get(AgentResearchRunRecord, plan.source_run_id)
            ).actor_id != "local-single-monitor-sol-v1"
        accepted = datetime.now(UTC)
        captured["clock"][0] = accepted
        directory = (tmp_path / "baseline/directory.json").read_bytes()
        document = json.loads((tmp_path / "baseline/AAPL-submissions.json").read_bytes())
        accession = "0000123456-26-000099"
        document["filings"]["recent"] = {
            "accessionNumber": [accession, "0000123456-26-000100"],
            "form": ["8-K", "8-K"],
            "acceptanceDateTime": [accepted.isoformat(), accepted.isoformat()],
            "filingDate": [accepted.date().isoformat(), accepted.date().isoformat()],
            "primaryDocument": ["new.htm", "second.htm"],
        }
        raw = json.dumps(document).encode()
        received = datetime.now(UTC)

        def receipt(raw, url):
            return json.dumps(
                {
                    "source_url": url,
                    "received_at": received.isoformat(),
                    "sha256": sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                    "http_status": 200,
                }
            ).encode()

        captured["clock"][0] = datetime.now(UTC)
        async with factory() as session, session.begin():
            capture, _ = await persist_durable_index_archive(
                session,
                store,
                identity_snapshot_id=rule.identity_snapshot_id,
                directory=(
                    directory,
                    receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
                ),
                submissions=(
                    raw,
                    receipt(raw, f"https://data.sec.gov/submissions/CIK{rule.issuer_cik}.json"),
                ),
                now=datetime.now(UTC),
            )
        async with factory() as session, session.begin():
            now = datetime.now(UTC)
            check = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=capture.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            )
            assert check.result == "true", check.reason
        async with factory() as session:
            original_notices = await session.scalar(select(func.count()).select_from(OutboxEvent))
            original_claims = await session.scalar(
                select(func.count()).select_from(SecNotificationClaim)
            )
            original_runs = await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            )
            original_jobs = await session.scalar(select(func.count()).select_from(Job))
        captured["clock"][0] = datetime.now(UTC)
        async with factory() as session, session.begin():
            batch = await bridge.prepare_durable_sec_research_batch(
                session, account_id=pointer.account_id, rule_id=rule.id
            )
            assert batch is not None
            assert batch.outbox_id is None and batch.routing_version == bridge.ROUTING
            batch_id = batch.id
        async with factory() as session:
            bindings = tuple(
                await session.scalars(
                    select(SecEventResearchBinding).where(
                        SecEventResearchBinding.batch_id == batch_id
                    )
                )
            )
            events = [
                await session.get(bridge.SecDiscoveryEvent, binding.event_id)
                for binding in bindings
            ]
        for n, event in enumerate(events):
            monkeypatch.setattr(filing_fixture, "AT", datetime.now(UTC) - timedelta(seconds=8))
            archive = filing_fixture.archive(tmp_path / f"filing-{n}", event.entry)
            captured["clock"][0] = datetime.now(UTC)
            await run(
                settings,
                argparse.Namespace(
                    action="import-content",
                    index_snapshot_id=capture.id,
                    accession=event.entry["accession_number"],
                    archive_dir=archive,
                ),
            )
        captured["clock"][0] = datetime.now(UTC)
        material = await run(
            settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id)
        )
        assert material["status"] == "prepared", material
        from uuid import UUID

        from nexus.persistence.models.data import DataSnapshot
        from nexus.persistence.models.monitoring import MonitorTrigger

        async with factory() as session:
            original_material = await session.get(DataSnapshot, UUID(material["snapshot_id"]))
            original_payload = json.dumps(original_material.payload, sort_keys=True)
            old_material_id = original_material.id
            original_triggers = {
                t.id: (t.status, t.check_id, t.batch_id, t.research_run_id)
                for t in await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            }
        # The producer's same-bucket once remains intact: wait for the actual next bucket.
        import asyncio

        boundary = datetime.now(UTC).replace(
            minute=datetime.now(UTC).minute // 5 * 5, second=0, microsecond=0
        ) + timedelta(minutes=5)
        await asyncio.sleep(max(0, (boundary - datetime.now(UTC)).total_seconds()) + 0.05)
        document["filings"]["recent"]["primaryDocument"][0] = "changed.htm"
        now = datetime.now(UTC)
        for key, value in {
            "accessionNumber": "0000123456-26-000101",
            "form": "8-K",
            "acceptanceDateTime": now.isoformat(),
            "filingDate": now.date().isoformat(),
            "primaryDocument": "third.htm",
        }.items():
            document["filings"]["recent"][key].append(value)
        raw = json.dumps(document).encode()
        received = datetime.now(UTC)
        captured["clock"][0] = received
        async with factory() as session, session.begin():
            current, _ = await persist_durable_index_archive(
                session,
                store,
                identity_snapshot_id=rule.identity_snapshot_id,
                directory=(
                    directory,
                    receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
                ),
                submissions=(
                    raw,
                    receipt(raw, f"https://data.sec.gov/submissions/CIK{rule.issuer_cik}.json"),
                ),
                now=datetime.now(UTC),
            )
        async with factory() as session, session.begin():
            now = datetime.now(UTC)
            discovered = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=current.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            )
            assert discovered.result == "true"
        async with factory() as session, session.begin():
            replacement = await bridge.prepare_durable_sec_research_batch(
                session, account_id=pointer.account_id, rule_id=rule.id
            )
            assert replacement is not None and replacement.id != batch_id
            replacement_id = replacement.id
        async with factory() as session:
            z_event = await session.scalar(
                select(bridge.SecDiscoveryEvent).where(
                    bridge.SecDiscoveryEvent.event_key == "sec:0000123456:0000123456-26-000101"
                )
            )
        monkeypatch.setattr(filing_fixture, "AT", datetime.now(UTC) - timedelta(seconds=8))
        z_archive = filing_fixture.archive(tmp_path / "filing-z", z_event.entry)
        captured["clock"][0] = datetime.now(UTC)
        await run(
            settings,
            argparse.Namespace(
                action="import-content",
                index_snapshot_id=current.id,
                accession=z_event.entry["accession_number"],
                archive_dir=z_archive,
            ),
        )
        captured["clock"][0] = datetime.now(UTC)
        new_material = await run(
            settings, argparse.Namespace(action="prepare-batch", batch_id=replacement_id)
        )
        assert new_material["status"] == "prepared", new_material
        assert new_material["snapshot_id"] != str(old_material_id)
        from nexus.monitoring.batch_inputs import read_batch_inputs

        async with factory() as session:
            old_row = await session.get(DataSnapshot, old_material_id)
            historical = await read_batch_inputs(
                session, snapshot_id=old_material_id, as_of=old_row.available_at
            )
            assert historical is not None
        from sqlalchemy.exc import DBAPIError

        with pytest.raises(DBAPIError, match="SEC_RESEARCH_BATCH_SUPERSEDED"):
            async with factory() as session, session.begin():
                await session.execute(
                    text("SELECT public.sec_research_require_current_batch(:batch)"),
                    {"batch": batch_id},
                )
        async with factory() as session, session.begin():
            await session.execute(
                text("SELECT public.sec_research_require_current_batch(:batch)"),
                {"batch": replacement_id},
            )
            repeated = await bridge.prepare_durable_sec_research_batch(
                session, account_id=pointer.account_id, rule_id=rule.id
            )
            assert repeated.id == replacement_id
        from helpers.paper_batch import CALENDAR_ID
        from nexus.ops.agent_paper_loop import AgentPaperLoopSnapshot
        from nexus.ops.monitor_dispatch import inspect_monitor_queue

        now = datetime.now(UTC)
        async with factory() as session:
            source_run = await session.get(AgentResearchRunRecord, plan.source_run_id)
            from nexus.monitoring.mixed_readback import unadmitted_stock_count

            assert (
                await unadmitted_stock_count(
                    session, account_id=pointer.account_id, instrument_id=pointer.instrument_id
                )
                == 2
            )
        queue_settings = settings.model_copy(
            update={
                "agent_paper_cycle_enabled": True,
                "agent_paper_formal_model_config_id": source_run.model_config_id,
            }
        )
        queued = await inspect_monitor_queue(
            queue_settings,
            account_id=pointer.account_id,
            snapshot=AgentPaperLoopSnapshot(
                now=now,
                calendar_authority_id=CALENDAR_ID,
                market_open_at=at,
                decision_window_start_at=at,
                decision_window_end_at=at + timedelta(hours=7),
                next_market_open_at=at + timedelta(days=1),
                next_decision_window_start_at=at + timedelta(days=1),
            ),
        )
        assert queued.monitor_batch_id == replacement_id
        async with factory() as session:
            rebound = tuple(
                await session.scalars(
                    select(SecEventResearchBinding).where(
                        SecEventResearchBinding.batch_id == replacement_id
                    )
                )
            )
            actual = {
                (await session.get(bridge.SecDiscoveryEvent, b.event_id)).event_key for b in rebound
            }
            assert actual == {
                "sec:0000123456:0000123456-26-000100",
                "sec:0000123456:0000123456-26-000101",
            }
            assert (
                json.dumps(
                    (await session.get(DataSnapshot, old_material_id)).payload, sort_keys=True
                )
                == original_payload
            )
            assert {
                t.id: (t.status, t.check_id, t.batch_id, t.research_run_id)
                for t in await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            } == original_triggers
            assert (
                await session.scalar(select(func.count()).select_from(SecNotificationClaim))
                == original_claims + 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(OutboxEvent))
                == original_notices + 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord))
                == original_runs
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == original_jobs
    finally:
        await factory.kw["bind"].dispose()
