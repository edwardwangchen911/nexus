"""Valid source and exact audits cannot attach unrelated custody or forge a batch hash."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import event as sa_event
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_normal_batch_hash_and_unrelated_binding_reject_at_commit(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers import disclosure_observations as reader
    from integration import test_candidate_multistock_path as candidate
    from integration import test_monitor_batches_pg as batches
    from integration import test_monitor_disclosure_refresh_pg as refresh_fixture
    from integration import test_monitor_filing_content_pg as filing_fixture
    from nexus.monitoring import durable_sec_research as bridge
    from nexus.monitoring import durable_sec_storage as durable
    from nexus.monitoring.batches import scan_stock_batch
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
    from nexus.persistence.models.sec_research import SecEventResearchBinding
    from nexus.research.durable_sec_index_storage import persist_durable_index_archive
    from sqlalchemy.exc import DBAPIError
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
            legacy_report = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            assert legacy_report["new_triggers"] > 0
            unrelated = await session.scalar(
                select(MonitorTrigger)
                .where(MonitorTrigger.durable_binding_id.is_(None))
                .order_by(MonitorTrigger.id)
            )
            unrelated_check = await session.get(MonitorCheck, unrelated.check_id)
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
            "accessionNumber": [accession],
            "form": ["8-K"],
            "acceptanceDateTime": [accepted.isoformat()],
            "filingDate": [accepted.date().isoformat()],
            "primaryDocument": ["new.htm"],
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
        # Corrupt exactly the new batch hash, keeping the real source, members and audits.
        returned = False

        def wrong_hash(sync_session, flush_context, instances):
            for row in sync_session.new:
                if isinstance(row, bridge.MonitorObservationBatch):
                    row.observation_hash = "0" * 64

        with pytest.raises(DBAPIError, match="SEC_RESEARCH_BATCH_HASH_INVALID"):
            async with factory() as session, session.begin():
                sa_event.listen(session.sync_session, "before_flush", wrong_hash)
                rejected = await bridge.prepare_durable_sec_research_batch(
                    session, account_id=pointer.account_id, rule_id=rule.id
                )
                assert rejected is not None
                returned = True
        assert returned, "hash failure must be real COMMIT after the normal producer body"
        async with factory() as session, session.begin():
            batch_a = await bridge.prepare_durable_sec_research_batch(
                session, account_id=pointer.account_id, rule_id=rule.id
            )
            assert batch_a is not None
        async with factory() as session:
            original_binding = await session.scalar(
                select(SecEventResearchBinding).where(
                    SecEventResearchBinding.batch_id == batch_a.id
                )
            )
            await session.execute(
                text("SELECT public.sec_research_validate_batch(:id)"), {"id": batch_a.id}
            )
        # Do not fabricate a second current-bucket discovery or override SQL clock.
        next_bucket = check.scheduled_at + timedelta(minutes=5)
        while datetime.now(UTC) < next_bucket:
            await asyncio.sleep(min(10, (next_bucket - datetime.now(UTC)).total_seconds()))
        accepted_y = datetime.now(UTC)
        entries = document["filings"]["recent"]
        for key, value in {
            "accessionNumber": "0000123456-26-000100",
            "form": "8-K",
            "acceptanceDateTime": accepted_y.isoformat(),
            "filingDate": accepted_y.date().isoformat(),
            "primaryDocument": "second.htm",
        }.items():
            entries[key].append(value)
        raw = json.dumps(document).encode()
        received = datetime.now(UTC)
        captured["clock"][0] = received
        async with factory() as session, session.begin():
            capture_y, _ = await persist_durable_index_archive(
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
            discovery_y = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=capture_y.id,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            )
            assert discovery_y.result == "true"
        async with factory() as session:
            event_y = await session.scalar(
                select(bridge.SecDiscoveryEvent).where(
                    bridge.SecDiscoveryEvent.event_key.endswith("000100")
                )
            )
            assert event_y is not None and event_y.id != original_binding.event_id
        flushed = False
        with pytest.raises(DBAPIError, match="SEC_RESEARCH_BINDING_REVERSE_INVALID"):
            async with factory() as session, session.begin():
                bound_at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                bad = SecEventResearchBinding(
                    id=uuid4(),
                    created_at=bound_at,
                    event_id=event_y.id,
                    account_id=pointer.account_id,
                    instrument_id=pointer.instrument_id,
                    rule_id=rule.id,
                    discovery_check_id=discovery_y.id,
                    metadata_hash=event_y.metadata_hash,
                    condition_hash=plan.condition_hash,
                    plan_id=plan.id,
                    source_run_id=plan.source_run_id,
                    source_output_id=plan.source_output_id,
                    source_hash=plan.source_hash,
                    capture_id=capture_y.id,
                    capture_hash=capture_y.revision,
                    check_id=unrelated_check.id,
                    trigger_id=unrelated.id,
                    batch_id=batch_a.id,
                    bound_at=bound_at,
                )
                session.add(bad)
                await session.flush([bad])
                await bridge._audit(session, bad)
                flushed = True
        assert flushed, "valid source and exact audit must reach COMMIT before reverse rejection"
        async with factory() as session:
            await session.execute(
                text("SELECT public.sec_research_validate_batch(:id)"), {"id": batch_a.id}
            )
            assert (
                await session.scalar(select(func.count()).select_from(SecEventResearchBinding)) == 1
            )
            assert (
                await session.scalar(
                    select(SecEventResearchBinding.id).where(
                        SecEventResearchBinding.event_id == event_y.id
                    )
                )
                is None
            )
    finally:
        await factory.kw["bind"].dispose()
