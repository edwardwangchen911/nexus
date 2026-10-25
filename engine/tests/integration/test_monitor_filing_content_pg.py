"""Exact filing originals enter through normal CLI; index-only never reserves a job."""

import argparse
import asyncio
import json
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from nexus.monitoring.batches import scan_stock_batch
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorObservationBatch
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select


def archive(root, entry, *, extra=False):
    root.mkdir(parents=True)
    accession = entry["accession_number"]
    base = f"https://www.sec.gov/Archives/edgar/data/123456/{accession.replace('-', '')}/"
    documents = {
        entry["primary_document"]: (
            entry["form"],
            b"<p>Item 2.02. Quarterly results are furnished; fiscal definitions unchanged.</p>",
        ),
        "ex99.htm": (
            "EX-99.1",
            b"<p>Unaudited revenue USD 100 million; quarter, not year to date.</p>",
        ),
    }
    if extra:
        documents["ex992.htm"] = (
            "EX-99.2",
            b"<p>Additional GAAP reconciliation for the same fiscal quarter.</p>",
        )
    rows = "".join(
        f'<tr><td>{number}</td><td>Document</td><td><a href="{base}{name}">{name}</a></td>'
        f"<td>{kind}</td><td>{len(body)}</td></tr>"
        for number, (name, (kind, body)) in enumerate(documents.items(), 1)
    )
    inventory = (
        '<table summary="Document Format Files"><tr><th>Seq</th><th>Description</th>'
        "<th>Document</th><th>Type</th><th>Size</th></tr>" + rows + "</table>"
    ).encode()
    bodies = {
        **{key: body for key, (_, body) in documents.items()},
        accession + "-index.html": inventory,
    }
    for name, body in bodies.items():
        (root / name).write_bytes(body)
        (root / (name + ".receipt.json")).write_text(
            json.dumps(
                dict(
                    source_url=base + name,
                    received_at=(AT + timedelta(seconds=8)).isoformat(),
                    sha256=sha256(body).hexdigest(),
                    byte_length=len(body),
                    http_status=200,
                )
            )
        )
    return root


@pytest.mark.asyncio
async def test_normal_content_entry_binds_all_exact_events_and_preserves_old_cutoff(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(
        "nexus.monitoring.filing_archive.configured_exhibit_store", lambda session: store
    )

    async def read_evidence(key, *, uri):
        assert uri == "s3://synthetic/" + key
        return store.objects[key]

    monkeypatch.setattr(store, "read_evidence", read_evidence, raising=False)
    async with factory() as session, session.begin():
        scanned = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    args = argparse.Namespace(action="prepare-batch", batch_id=UUID(scanned["batch_id"]))
    initial = await run(settings, args)
    assert initial["status"] == "blocked" and initial["reason"] == "MONITOR_CONTENT_MISSING"
    assert initial["missing_content"] == 2 and initial["model_requests"] == 0
    assert initial["missing_event_ids"] == [
        "sec:0000123456:0000123456-26-000002",
        "sec:0000123456:0000123456-26-000003",
    ]
    async with factory() as session:
        batch = await session.get(MonitorObservationBatch, args.batch_id)
        original = list(batch.trigger_ids)
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
        counts = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        )
    captured["clock"][0] = AT + timedelta(seconds=9)
    await _set_test_insert_clock(factory, captured["clock"][0])
    original_put = store.put_if_absent
    delayed = [False]
    raw_delayed = [False]

    async def put_with_later_custody(*, key, content):
        result = await original_put(key=key, content=content)
        if key.startswith("raw/sec/monitor-filing/") and not raw_delayed[0]:
            raw_delayed[0] = True
            captured["clock"][0] = AT + timedelta(seconds=10)
        if key.startswith("evidence/monitor-filing/") and not delayed[0]:
            delayed[0] = True
            captured["clock"][0] = AT + timedelta(seconds=11)
        return result

    monkeypatch.setattr(store, "put_if_absent", put_with_later_custody)
    for number, entry in enumerate(entries):
        imported = await run(
            settings,
            argparse.Namespace(
                action="import-content",
                index_snapshot_id=current.id,
                accession=entry["accession_number"],
                archive_dir=archive(tmp_path / f"body-{number}", entry, extra=number == 1),
            ),
        )
        assert (
            imported["status"] == "created"
            and imported["model_requests"] == imported["provider_requests"] == 0
        )
        if number == 0:
            from nexus.monitoring.filing_archive import read_filing_content
            from nexus.persistence.models.data import DataArtifact

            async with factory() as session:
                first_archived = await session.scalar(
                    select(func.min(DataArtifact.available_at)).where(
                        DataArtifact.schema_version == "sec_monitor_original_v1"
                    )
                )
                assert first_archived >= AT + timedelta(seconds=10)
                assert (
                    await read_filing_content(
                        session,
                        snapshot_id=UUID(imported["snapshot_id"]),
                        as_of=AT + timedelta(seconds=9),
                    )
                    is None
                )
        repeated = await run(
            settings,
            argparse.Namespace(
                action="import-content",
                index_snapshot_id=current.id,
                accession=entry["accession_number"],
                archive_dir=tmp_path / f"body-{number}",
            ),
        )
        assert (
            repeated["snapshot_id"] == imported["snapshot_id"] and repeated["status"] == "existing"
        )
    ready = await run(settings, args)
    assert (
        ready["status"] == "prepared"
        and ready["trigger_count"] == 3
        and ready["content_count"] == 2
    )
    assert ready["job_created"] is False and ready["execution_locked"] is True
    repeated = await asyncio.gather(run(settings, args), run(settings, args))
    assert all(result["snapshot_id"] == ready["snapshot_id"] for result in repeated)
    from nexus.persistence.models.runtime import AuditEvent

    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "research.monitor.batch_inputs_prepared")
            )
            == 1
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "nexus_monitor_batch_inputs")
            )
            == 1
        )
    from nexus.monitoring.batch_inputs import read_batch_inputs

    async with factory() as session:
        frozen = await read_batch_inputs(
            session, snapshot_id=UUID(ready["snapshot_id"]), as_of=captured["clock"][0]
        )
        assert frozen is not None and len(frozen.parents) == 2 and len(frozen.members) == 3
        assert len(frozen.contents[1].archive.documents) == 3
        assert (
            await read_batch_inputs(
                session, snapshot_id=UUID(ready["snapshot_id"]), as_of=AT + timedelta(seconds=7)
            )
            is None
        )
        assert (await session.get(MonitorObservationBatch, batch.id)).trigger_ids == original
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
                ]
            )
            == counts
        )
    # A material package is not a research snapshot or an execution capability.
    from decimal import Decimal

    from nexus.agents.models import ResearchRequest
    from nexus.agents.persistence import admit_research_run
    from nexus.jobs.models import JobType

    async with factory() as session, session.begin():
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        with pytest.raises(ValueError, match="point-in-time data authority is invalid"):
            async with session.begin_nested():
                await admit_research_run(
                    session,
                    actor_id="material-package-negative",
                    route="research/admit",
                    idempotency_key="material-is-not-a-run",
                    request_hash="b" * 64,
                    request=ResearchRequest(
                        instrument_id=pointer.instrument_id,
                        data_snapshot_id=UUID(ready["snapshot_id"]),
                        decision_time=captured["clock"][0],
                    ),
                    prompt_version_id=parent.prompt_version_id,
                    model_config_id=parent.model_config_id,
                    daily_limit_usd=Decimal("10"),
                    now=captured["clock"][0],
                    job_type=JobType.AGENT_PAPER_RESEARCH,
                    formal_stage_capacity=True,
                )
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
                ]
            )
            == counts
        )
    # A missing object is a material gap, never an uncaught provider response.
    from botocore.exceptions import ClientError

    normal_read_evidence = store.read_evidence

    async def unavailable(key, *, uri):
        raise ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "private object details"}}, "GetObject"
        )

    monkeypatch.setattr(store, "read_evidence", unavailable)
    async with factory() as session:
        assert (
            await read_batch_inputs(
                session, snapshot_id=UUID(ready["snapshot_id"]), as_of=captured["clock"][0]
            )
            is None
        )
    monkeypatch.setattr(store, "read_evidence", normal_read_evidence)
    # Original bytes must be present on every read, not only when imported.
    key = next(key for key in store.objects if key.startswith("evidence/monitor-filing/"))
    saved = store.objects[key]
    store.objects[key] = b"corrupt canonical"
    async with factory() as session:
        assert (
            await read_batch_inputs(
                session, snapshot_id=UUID(ready["snapshot_id"]), as_of=captured["clock"][0]
            )
            is None
        )
    store.objects[key] = saved
    # Later conflicting originals are new facts; they cannot rewrite the old cutoff.
    captured["clock"][0] = AT + timedelta(seconds=12)
    await _set_test_insert_clock(factory, captured["clock"][0])
    conflict_dir = archive(tmp_path / "conflicting", entries[0])
    filename = entries[0]["primary_document"]
    body = (conflict_dir / filename).read_bytes().replace(b"unchanged", b"different")
    (conflict_dir / filename).write_bytes(body)
    receipt_path = conflict_dir / (filename + ".receipt.json")
    receipt_json = json.loads(receipt_path.read_text())
    receipt_json["sha256"] = sha256(body).hexdigest()
    receipt_path.write_text(json.dumps(receipt_json))
    await run(
        settings,
        argparse.Namespace(
            action="import-content",
            index_snapshot_id=current.id,
            accession=entries[0]["accession_number"],
            archive_dir=conflict_dir,
        ),
    )
    conflicted = await run(settings, args)
    assert conflicted["status"] == "blocked" and conflicted["reason"] == "MONITOR_CONTENT_CONFLICT"
    async with factory() as session:
        assert (
            await read_batch_inputs(
                session, snapshot_id=UUID(ready["snapshot_id"]), as_of=captured["clock"][0]
            )
            == frozen
        )
    # A later plan must not invalidate the original material package at its cutoff.
    from integration.test_monitor_kinds_pg import clone
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorPlan

    captured["clock"][0] = AT + timedelta(seconds=13)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session, session.begin():
        prior_plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "price"))
        prior_output = await session.get(AgentResearchOutputRecord, prior_plan.source_output_id)
        prior_run = await session.get(AgentResearchRunRecord, prior_plan.source_run_id)
        later_run = clone(
            prior_run,
            idempotency_key="later-plan-material-cutoff",
            decision_time=AT + timedelta(seconds=8),
            status="failed",
        )
        session.add(later_run)
        await session.flush()
        later_output = clone(prior_output, run_id=later_run.id)
        session.add(later_output)
        await session.flush()
        session.add(
            clone(
                prior_plan,
                source_output_id=later_output.id,
                source_run_id=later_run.id,
                predecessor_id=prior_plan.id,
                generation=prior_plan.generation + 1,
                effective_at=captured["clock"][0],
            )
        )
    async with factory() as session:
        assert (
            await read_batch_inputs(
                session, snapshot_id=UUID(ready["snapshot_id"]), as_of=captured["clock"][0]
            )
            == frozen
        )
    rejected = await run(settings, args)
    assert (
        rejected["status"] == "blocked" and rejected["reason"] == "MONITOR_AWAITING_RECONCILIATION"
    )
    await factory.kw["bind"].dispose()
