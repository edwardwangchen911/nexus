"""Normal v10 research plus synthetic SEC archives for reader-only integration tests."""

import json
from datetime import timedelta
from hashlib import sha256
from uuid import UUID, uuid4

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


async def reader_source(
    database_url, monkeypatch, root, *, monitoring_lifetime_days=None, independent=False
):
    class VersionedMemoryStore(MemoryStore):
        async def put_if_absent(self, *, key, content):
            uri = await super().put_if_absent(key=key, content=content)
            return uri + "?versionId=" + sha256(content).hexdigest()

        async def read_raw(self, key, *, uri):
            content = self.objects[key]
            assert uri == "s3://synthetic/" + key + "?versionId=" + sha256(content).hexdigest()
            return content

        read_evidence = read_raw

    store, captured = (VersionedMemoryStore() if independent else MemoryStore()), {}
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
            sibling_symbols=("AVGO", "NVDA") if independent else (),
            exchange="NASDAQ",
            received=historical - timedelta(seconds=1),
        )
        if independent:
            sessions.kw["info"] = {"sec_exhibit_store": store}
            import shutil
            from types import SimpleNamespace

            from sqlalchemy import literal

            # Freeze explicit insertion expressions as well as server defaults.
            for module in ("current_identity_storage", "observation_identity_storage"):
                monkeypatch.setattr(
                    f"nexus.research.{module}.func",
                    SimpleNamespace(clock_timestamp=lambda: literal(clock_at[0])),
                )
            from nexus.research.current_identity_storage import import_identity_binding

            identities = {"AAPL": identity}
            for symbol in ("AVGO", "NVDA"):
                identities[symbol] = await import_baseline_identity(
                    settings,
                    store,
                    root / f"identity-{symbol}",
                    symbol=symbol,
                    exchange="NASDAQ",
                    received=historical - timedelta(seconds=1),
                )
            for symbol, proof in identities.items():
                target = root / f"research-identity-{symbol}"
                target.mkdir(mode=0o700)
                receipt = target / "result.json"
                receipt.write_text(
                    json.dumps(
                        dict(
                            status="archived",
                            requests=2,
                            authority_registered=False,
                            execution_locked=True,
                        )
                    )
                )
                receipt.chmod(0o600)
                for name in (
                    "directory.json",
                    "directory-receipt.json",
                    "AAPL-submissions.json",
                    "AAPL-submissions-receipt.json",
                ):
                    destination = target / name.replace("AAPL", symbol)
                    shutil.copyfile(root / "baseline" / name, destination)
                    if name.endswith("-receipt.json"):
                        from nexus.research.current_identity_archive import RECEIPT_SCHEMA

                        document = json.loads(destination.read_text())
                        document["schema"] = RECEIPT_SCHEMA
                        destination.write_text(json.dumps(document))
                    destination.chmod(0o600)
                async with sessions.begin() as session:
                    await import_identity_binding(
                        session,
                        store,
                        identity_snapshot_id=proof.snapshot_id,
                        collection_dir=target,
                    )
            monkeypatch.setattr(
                "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
            )
        if independent:
            from nexus.monitoring.disclosure_context import _exact_baseline
            from nexus.persistence.models.data import DataSnapshot

            async with sessions() as session:
                baseline_row = await session.scalar(
                    select(DataSnapshot).where(DataSnapshot.source == "sec_disclosure_index")
                )
                await _exact_baseline(
                    session,
                    snapshot_id=baseline_row.id,
                    instrument_id=UUID(baseline_row.payload["index"]["instrument_id"]),
                    as_of=clock_at[0],
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
        independent=independent,
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
        outputs = tuple(outputs)
        if independent:
            from nexus.persistence.models.data import DataSnapshot

            snapshots = tuple(
                await session.scalars(
                    select(DataSnapshot).where(
                        DataSnapshot.source == "nexus_independent_monitor_research"
                    )
                )
            )
            assert any(row.intent.get("disclosure_proposal") is not None for row in outputs), {
                "outcome_codes": [row.intent.get("quality") for row in outputs],
                "contexts": [row.payload["independent_monitor_context"] for row in snapshots],
            }
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
