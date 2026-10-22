"""Normal formal trigger consumes current identity at the actual decision cutoff."""

from datetime import timedelta

import pytest
from helpers.current_identity import archives
from integration.test_candidate_multistock_path import AT
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.instrument_identity_local import revise_listing
from nexus.ops.research_identity_binding_local import register_identity
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.research import sec_exhibit_storage
from nexus.research.current_identity_storage import SOURCE, read_identity_bindings
from sqlalchemy import func, literal, select
from unit.market.test_object_store import FakeS3Client


async def seed_current_identity(settings, manifest, root, clock, monkeypatch):
    """Only public-source and object-store transports are synthetic."""
    import json
    from hashlib import sha256

    monkeypatch.setattr(func, "clock_timestamp", lambda: literal(clock[0]))
    monkeypatch.setenv("SEC_CONTACT", "synthetic@example.invalid")
    from nexus.ops import agent_paper_identity

    async def no_source_request(**kwargs):
        raise AssertionError("pre-registered current identities must not request the source")

    monkeypatch.setattr(agent_paper_identity, "collect_identity_sources", no_source_request)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    monkeypatch.setattr(sec_exhibit_storage, "S3ProviderEvidenceStore", lambda actual: store)
    for candidate in manifest:
        symbol = candidate.symbol
        received = clock[0] - timedelta(minutes=2)
        fm, sc = archives(root / symbol, symbol=symbol, received=received.isoformat())
        base = await revise_listing(
            settings, symbol=symbol, fmp_archive=fm, sec_archive=sc, store=store
        )
        folder = root / symbol / "current"
        folder.mkdir(mode=0o700)
        docs = {
            "directory": (
                {
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [[123456, "Synthetic", symbol, "NYSE"]],
                },
                "https://www.sec.gov/files/company_tickers_exchange.json",
            ),
            f"{symbol}-submissions": (
                {"cik": "123456", "tickers": [symbol], "exchanges": ["NYSE"]},
                "https://data.sec.gov/submissions/CIK0000123456.json",
            ),
        }
        for name, (body, url) in docs.items():
            raw = json.dumps(body).encode()
            receipt = json.dumps(
                {
                    "schema": "sec_identity_source_receipt_v1",
                    "source_url": url,
                    "http_status": 200,
                    "received_at": received.isoformat(),
                    "sha256": sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                }
            )
            for name, content in (
                (name + ".json", raw),
                (name + "-receipt.json", receipt.encode()),
            ):
                path = folder / name
                path.write_bytes(content)
                path.chmod(0o600)
        result = folder / "result.json"
        result.write_text(
            json.dumps(
                {
                    "status": "archived",
                    "requests": 2,
                    "authority_registered": False,
                    "execution_locked": True,
                }
            )
        )
        result.chmod(0o600)
        await register_identity(
            settings, identity_snapshot_id=base.snapshot_id, collection_dir=folder, store=store
        )
    return store


async def assert_cycle_identity(session, cycle, runs, original_manifest, store):
    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.agents.paper_context import read_agent_paper_research_context
    from nexus.agents.persistence import select_authoritative_snapshot

    session.info["sec_exhibit_store"] = store
    assert cycle.decision_time == AT + timedelta(seconds=10)
    old_ids = {item.research_snapshot_id for item in original_manifest}
    for run in runs:
        final = await session.get(DataSnapshot, run.data_snapshot_id)
        assert final.observed_at == cycle.decision_time
        assert final.id not in old_ids
        base = await select_authoritative_snapshot(
            session, instrument_id=run.instrument_id, as_of=AT
        )
        assert base.id not in old_ids and base.observed_at == AT
        refs = tuple(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == final.id,
                    AgentDataSnapshotEvidenceRecord.source == SOURCE,
                )
            )
        )
        assert len(refs) == 1
        base_refs = tuple(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                    AgentDataSnapshotEvidenceRecord.source == SOURCE,
                )
            )
        )
        assert refs == base_refs
        frozen = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
        assert {item.id for item in frozen if item.source == SOURCE} == set(refs)
        assert run.decision_time == cycle.decision_time
        candidate = next(
            item
            for item in cycle.candidate_manifest
            if item["instrument_id"] == str(run.instrument_id)
        )
        assert candidate["research_snapshot_id"] == str(base.id)
        bindings = await read_identity_bindings(
            session,
            instrument_id=run.instrument_id,
            as_of=cycle.decision_time,
            decision_session=AT.date(),
            evidence_ids=refs,
        )
        assert set(bindings) == set(refs)
        assert all(item.decision_session == AT.date() for item in bindings.values())
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=final.id,
            instrument_id=run.instrument_id,
            as_of=cycle.decision_time,
        )
        assert context is not None and context[0].decision_time == cycle.decision_time
        assert context[0].cycle_id == cycle.id


@pytest.mark.asyncio
async def test_normal_cycle_consumes_current_identity(migrated_database_url, monkeypatch, tmp_path):
    from integration.test_formal_host_trigger import (
        test_host_preparation_and_formal_trigger_freeze_exact_batch,
    )

    await test_host_preparation_and_formal_trigger_freeze_exact_batch(
        migrated_database_url, monkeypatch, 3, current_identity=True, tmp_path=tmp_path
    )
