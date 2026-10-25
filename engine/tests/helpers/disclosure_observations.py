"""Normal v10 research plus synthetic SEC archives for reader-only integration tests."""

import json
from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

from helpers.current_identity import MemoryStore
from helpers.disclosure_baseline import import_baseline_capture, import_baseline_identity
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from nexus.monitoring.disclosure_contracts import DisclosureMonitoringProposal
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select


async def reader_source(database_url, monkeypatch, root, *, monitoring_lifetime_days=None):
    store, captured = MemoryStore(), {}
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_context.configured_exhibit_store", lambda s: store
    )

    async def prepare(settings, sessions, authority, manifest, clock_at):
        historical = AT - timedelta(seconds=2)
        clock_at[0] = historical
        await _set_test_insert_clock(sessions, historical)
        settings = settings.model_copy(update={"object_store_endpoint": "http://127.0.0.1:9000"})
        identity = await import_baseline_identity(
            settings,
            store,
            root / "identity",
            exchange="NASDAQ",
            received=historical - timedelta(seconds=1),
        )
        await import_baseline_capture(
            settings,
            store,
            root / "baseline",
            identity=identity,
            exchange="NASDAQ",
            received=historical - timedelta(seconds=1),
        )
        captured.update(identity=identity, clock=clock_at, account_id=authority.account_id)
        clock_at[0] = AT + timedelta(seconds=1)
        await _set_test_insert_clock(sessions, AT - timedelta(seconds=1))

    await normal_cycle(
        database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        disclosure=True,
        before_research=prepare,
        monitoring_lifetime_days=monitoring_lifetime_days,
    )
    settings = EngineSettings(
        database_url=database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    factory = session_factory(settings)
    async with factory() as session:
        outputs = await session.scalars(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
        output = next(row for row in outputs if row.intent.get("disclosure_proposal") is not None)
        run = await session.get(AgentResearchRunRecord, output.run_id)
        proposal = DisclosureMonitoringProposal.model_validate(output.intent["disclosure_proposal"])
        # Installation is the root writer's slice; reader uses a typed plan over
        # the real native Worker output, without pretending this is an installed plan.
        plan = MonitorPlan(
            id=uuid4(),
            account_id=captured["account_id"],
            instrument_id=run.instrument_id,
            kind="disclosure",
            generation=1,
            source_run_id=run.id,
            source_output_id=output.id,
            data_snapshot_id=run.data_snapshot_id,
            source_hash=output.content_hash,
            condition=proposal.condition.model_dump(mode="json"),
            condition_hash=proposal.condition.semantic_hash,
            baseline_snapshot_id=proposal.baseline_snapshot_id,
            baseline_as_of=proposal.baseline_as_of,
            effective_at=AT + timedelta(seconds=4),
            review_due_at=proposal.review_due_at,
            valid_until=proposal.valid_until,
        )
    return factory, settings, store, captured, plan


async def append_capture(
    settings,
    store,
    captured,
    factory,
    root,
    name,
    *,
    added=1,
    partial=False,
    conflict=False,
    received=None,
    imported_at=None,
):
    received = received or AT + timedelta(seconds=2)
    captured["clock"][0] = imported_at or AT + timedelta(seconds=5)
    await _set_test_insert_clock(factory, captured["clock"][0])
    target = root / name
    target.mkdir()
    for stem in ("directory", "AAPL-submissions"):
        document = json.loads((root / "baseline" / f"{stem}.json").read_bytes())
        proof = json.loads((root / "baseline" / f"{stem}-receipt.json").read_bytes())
        if stem.endswith("submissions"):
            recent = document["filings"]["recent"]
            if conflict:
                recent["form"][0] = "8-K/A"
            for number in range(2, added + 2):
                for key in recent:
                    recent[key].append(recent[key][0])
                recent["accessionNumber"][-1] = f"0000123456-26-{number:06d}"
                recent["primaryDocument"][-1] = f"new{number}.htm"
            if partial:
                document["filings"]["files"] = [{"name": "older.json"}]
        raw = json.dumps(document, separators=(",", ":")).encode()
        proof.update(
            received_at=received.isoformat(), sha256=sha256(raw).hexdigest(), byte_length=len(raw)
        )
        (target / f"{stem}.json").write_bytes(raw)
        (target / f"{stem}-receipt.json").write_text(json.dumps(proof))
    return await import_index(
        settings,
        identity_snapshot_id=captured["identity"].snapshot_id,
        archive_dir=target,
        store=store,
    )
