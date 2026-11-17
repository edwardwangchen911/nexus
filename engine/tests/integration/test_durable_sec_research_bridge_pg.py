"""Actual-current normal ordinary research binds a new SEC event without another notice."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_ordinary_current_source_reaches_research_only_material_batch(
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
    from nexus.persistence.models.monitoring import MonitorIncrement, MonitorPlan
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
            binding = await session.scalar(
                select(SecEventResearchBinding).where(SecEventResearchBinding.batch_id == batch_id)
            )
            event = await session.get(bridge.SecDiscoveryEvent, binding.event_id)
        monkeypatch.setattr(filing_fixture, "AT", datetime.now(UTC) - timedelta(seconds=8))
        archive = filing_fixture.archive(tmp_path / "filing", event.entry)
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
        async with factory() as session:
            assert (
                await session.scalar(select(func.count()).select_from(OutboxEvent))
                == original_notices
            )
            assert (
                await session.scalar(select(func.count()).select_from(SecNotificationClaim))
                == original_claims
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord))
                == original_runs
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == original_jobs
            assert not tuple(await session.scalars(select(MonitorIncrement)))
    finally:
        await factory.kw["bind"].dispose()
