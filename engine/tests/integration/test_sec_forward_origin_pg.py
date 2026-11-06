"""Normal archived captures advance one origin without replaying the incomplete interval."""

import asyncio
import json
import os
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest
from helpers.current_identity import CIK
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.disclosure_forward import SOURCE, ForwardObservation, establish_origin
from nexus.monitoring.disclosure_observations import read_disclosure_observation
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import func, select


async def listed(
    settings,
    store,
    factory,
    captured,
    root,
    name,
    *,
    late=False,
    late_txt=False,
    recent_added=0,
    late_form=None,
):
    target = root / name
    target.mkdir()
    first = json.loads((root / "baseline/AAPL-submissions.json").read_text())
    history = {k: [v[0]] for k, v in first["filings"]["recent"].items()}
    history.update(
        accessionNumber=[f"{CIK}-20-000001"],
        acceptanceDateTime=["2020-01-05T19:00:00Z"],
        filingDate=["2020-01-05"],
        primaryDocument=["old.txt"],
    )
    if late:
        for k in history:
            history[k].append(history[k][0])
        history["accessionNumber"][-1] = f"{CIK}-20-000009"
        history["primaryDocument"][-1] = "late.txt" if late_txt else "late.htm"
        if late_form is not None:
            history["form"][-1] = late_form
        first["filings"]["files"][0]["filingCount"] = 2
    recent = first["filings"]["recent"]
    for number in range(2, recent_added + 2):
        for key in recent:
            recent[key].append(recent[key][0])
        recent["accessionNumber"][-1] = f"{CIK}-26-{number:06d}"
        recent["primaryDocument"][-1] = f"new{number}.htm"
    shard = first["filings"]["files"][0]["name"]
    now = captured["clock"][0]
    await _set_test_insert_clock(factory, now)
    directory = json.loads((root / "baseline/directory.json").read_text())
    for stem, doc, url in (
        ("directory", directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
        ("AAPL-submissions", first, f"https://data.sec.gov/submissions/CIK{CIK}.json"),
        ("AAPL-submissions-close", first, f"https://data.sec.gov/submissions/CIK{CIK}.json"),
        ("history-0", history, f"https://data.sec.gov/submissions/{shard}"),
    ):
        raw = json.dumps(doc).encode()
        (target / (stem + ".json")).write_bytes(raw)
        (target / (stem + "-receipt.json")).write_text(
            json.dumps(
                dict(
                    source_url=url,
                    received_at=now.isoformat(),
                    sha256=sha256(raw).hexdigest(),
                    byte_length=len(raw),
                    http_status=200,
                )
            )
        )
    (target / "history-manifest.json").write_text(
        json.dumps(dict(schema="sec_listed_history_archive_v2", files=[shard]))
    )
    return UUID(
        (
            await import_index(
                settings,
                identity_snapshot_id=captured["identity"].snapshot_id,
                archive_dir=target,
                store=store,
                listed_history=True,
            )
        )["snapshot_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("late_txt", [False, True])
async def test_normal_forward_origin_atomic_readback_and_event_dedup(
    migrated_database_url, monkeypatch, tmp_path, late_txt
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        added=0,
        kinds=("disclosure",),
        listed_history=True,
    )
    factory.kw["info"] = {"sec_exhibit_store": store}
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_forward.configured_exhibit_store", lambda s: store
    )
    async with factory() as s:
        plan = await s.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
        baseline = await s.get(DataSnapshot, plan.baseline_snapshot_id)
        frozen = json.dumps(baseline.payload, sort_keys=True), baseline.revision
    try:
        captured["clock"][0] = AT + timedelta(minutes=1)
        first = await listed(settings, store, factory, captured, tmp_path, "listed-first")

        async def publish():
            async with factory() as s, s.begin():
                return (
                    await establish_origin(
                        s, plan_id=plan.id, capture_id=first, now=captured["clock"][0]
                    )
                ).id

        with pytest.raises(RuntimeError, match="synthetic rollback"):
            async with factory() as s, s.begin():
                await establish_origin(
                    s, plan_id=plan.id, capture_id=first, now=captured["clock"][0]
                )
                raise RuntimeError("synthetic rollback")
        async with factory() as s:
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == SOURCE)
                )
                == 0
            )
        origins = await asyncio.gather(publish(), publish())
        assert origins[0] == origins[1]
        async with factory() as s, s.begin():
            origin = await s.get(DataSnapshot, origins[0])
            cutoff = origin.available_at
            initial = await read_disclosure_observation(s, plan=plan, now=cutoff)
            assert isinstance(initial, ForwardObservation)
            assert initial.result == "unknown" and initial.old_history_gap == "unknown"
            assert initial.reason == "SEC_INDEX_FORWARD_BASELINE_ESTABLISHED" and not initial.added
            report = await scan_stock_batch(
                s, account_id=plan.account_id, instrument_id=plan.instrument_id, now=cutoff
            )
            assert report["new_triggers"] == 0 and report["notifications"] == 0
        captured["clock"][0] = AT + timedelta(minutes=2)
        second = await listed(settings, store, factory, captured, tmp_path, "listed-second")
        async with factory() as s, s.begin():
            view = await read_disclosure_observation(s, plan=plan, now=captured["clock"][0])
            assert view.result == "false" and view.comparison_snapshot_id == first
            assert view.old_history_gap == "unknown" and view.current_snapshot_id == second
            await scan_stock_batch(
                s,
                account_id=plan.account_id,
                instrument_id=plan.instrument_id,
                now=captured["clock"][0],
            )
            from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
            from nexus.persistence.models.agents import AgentResearchRunRecord

            run = await s.get(AgentResearchRunRecord, plan.source_run_id)
            page = await read_disclosure_monitoring_view(s, run, now=captured["clock"][0])
            assert page.latest_check.result == "false"
            assert page.latest_check.comparison_from == cutoff
            assert page.latest_check.prior_history_unknown is True

            assert (
                await establish_origin(
                    s, plan_id=plan.id, capture_id=second, now=captured["clock"][0]
                )
            ).id == origins[0]
            before = await read_disclosure_observation(
                s, plan=plan, now=cutoff - timedelta(microseconds=1)
            )
            assert not isinstance(before, ForwardObservation)
        browser_root = os.environ.get("NEXUS_TEST_FORWARD_BROWSER_DIR")
        if browser_root and not late_txt:
            import httpx
            from helpers.monitoring_readview_browser import stage_facts, verify_monitoring_browser
            from helpers.paper_api import app, headers

            now = captured["clock"][0]
            monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
            application = app(migrated_database_url, now=now)
            path = f"/internal/v1/research-runs/{plan.source_run_id}"
            async with factory() as s:
                protected = await stage_facts(s)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application), base_url="http://testserver"
            ) as client:
                response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.text
            saved = response.json()["data"]["disclosure_monitoring"]
            read_at = datetime.fromisoformat(saved["observed_at"].replace("Z", "+00:00"))
            assert read_at >= page.observed_at
            async with factory() as s:
                expected = await read_disclosure_monitoring_view(s, run, now=read_at)
            assert saved == expected.model_dump(mode="json")
            destination = Path(browser_root).resolve()
            destination.mkdir(parents=True, exist_ok=False)
            (destination / "ready.json").write_text(
                json.dumps(
                    dict(
                        run_id=str(plan.source_run_id),
                        instrument_id=str(plan.instrument_id),
                        response=response.json(),
                    )
                )
            )
            await verify_monitoring_browser(
                application,
                directory=destination,
                spec_path="tests/e2e/sec-forward-readview.spec.ts",
            )
            async with factory() as s:
                assert await stage_facts(s) == protected
        captured["clock"][0] = AT + timedelta(minutes=3)
        late_id = await listed(
            settings,
            store,
            factory,
            captured,
            tmp_path,
            "listed-late",
            late=True,
            late_txt=late_txt,
        )
        async with factory() as s, s.begin():
            view = await read_disclosure_observation(s, plan=plan, now=captured["clock"][0])
            assert view.result == "true" and len(view.added) == 1
            assert view.added[0].accession_number == f"{CIK}-20-000009"
            report = await scan_stock_batch(
                s,
                account_id=plan.account_id,
                instrument_id=plan.instrument_id,
                now=captured["clock"][0],
            )
            assert report["new_triggers"] == 1 and report["notifications"] == 1
            batch_id = UUID(report["batch_id"])
            late_entry = view.added[0]

        async with factory() as s, s.begin():
            report = await scan_stock_batch(
                s,
                account_id=plan.account_id,
                instrument_id=plan.instrument_id,
                now=captured["clock"][0],
            )
            assert report["new_triggers"] == 0
            assert await s.scalar(select(func.count()).select_from(MonitorTrigger)) == 1
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.event_type == "research.monitor.batch_triggered")
                )
                == 1
            )
            old = await s.get(DataSnapshot, plan.baseline_snapshot_id)
            assert (json.dumps(old.payload, sort_keys=True), old.revision) == frozen
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == SOURCE)
                )
                == 1
            )
        # The v2 event traverses the same content custody and prepared-input consumer.
        import argparse

        from integration.test_monitor_filing_content_pg import archive
        from nexus.ops.research_monitor import run as normal_entry

        monkeypatch.setattr(
            "nexus.monitoring.filing_archive.configured_exhibit_store", lambda s: store
        )

        async def read_evidence(key, *, uri):
            assert uri == "s3://synthetic/" + key
            return store.objects[key]

        monkeypatch.setattr(store, "read_evidence", read_evidence, raising=False)
        if late_txt:
            blocked = await normal_entry(
                settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id)
            )
            assert (
                blocked["status"] == "blocked"
                and blocked["reason"] == "SEC_MONITOR_DOCUMENT_FORMAT_UNSUPPORTED"
            )
            assert blocked["provider_requests"] == blocked["model_requests"] == 0
        else:
            imported = await normal_entry(
                settings,
                argparse.Namespace(
                    action="import-content",
                    index_snapshot_id=late_id,
                    accession=late_entry.accession_number,
                    archive_dir=archive(tmp_path / "body", late_entry.model_dump()),
                ),
            )
            assert imported["status"] == "created"
            prepared = await normal_entry(
                settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id)
            )
            assert prepared["status"] == "prepared", prepared
            assert prepared["model_requests"] == prepared["provider_requests"] == 0
        # An old query remains byte-for-byte tied to its original observation time.
        async with factory() as s:
            again = await read_disclosure_observation(s, plan=plan, now=cutoff)
            assert again == initial
            stale = await read_disclosure_observation(s, plan=plan, now=AT + timedelta(minutes=20))
            assert stale.result == "unknown" and stale.reason == "SEC_INDEX_STALE"
    finally:
        await factory.kw["bind"].dispose()
