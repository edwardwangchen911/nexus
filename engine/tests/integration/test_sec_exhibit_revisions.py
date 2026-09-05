"""New revisions must not duplicate a filing or alter an older frozen snapshot."""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from helpers.current_identity import CIK, MemoryStore, archives
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.instrument_identity_local import import_identity
from nexus.ops.openai_local import prepare_database
from nexus.ops.sec_exhibit_local import import_sec_exhibit
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select
from unit.research.test_sec_exhibits import NOW, archive, exhibit_archives


class Store(MemoryStore):
    async def put_if_absent(self, *, key, content):
        uri = await super().put_if_absent(key=key, content=content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}" if key.startswith("evidence/") else uri

    async def read_evidence(self, key, *, uri):
        assert uri == f"memory://sha256/{key.rsplit('/', 1)[-1]}"
        return self.objects[key]


async def setup_archive(url, tmp_path):
    settings = EngineSettings(database_url=url, object_store_endpoint="http://127.0.0.1:9000")
    await prepare_database(settings)
    store = Store()
    fm, sc = archives(tmp_path / "identity", received=NOW.isoformat())
    identity = await import_identity(
        settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
    )
    contents = exhibit_archives()
    metadata = json.loads(contents[0][0])
    metadata["cik"] = int(CIK)
    contents[0] = archive(
        json.dumps(metadata).encode(), f"https://data.sec.gov/submissions/CIK{CIK}.json"
    )
    for index in (1, 2):
        receipt = json.loads(contents[index][1])
        contents[index] = archive(
            contents[index][0], receipt["source_url"].replace("/data/1/", "/data/123456/")
        )
    paths = write_archives(tmp_path / "originals", contents)
    return settings, store, identity, contents, paths


def write_archives(directory, contents):
    directory.mkdir()
    paths = []
    for index, (body, receipt) in enumerate(contents):
        raw, info = directory / f"{index}.raw", directory / f"{index}.json"
        raw.write_bytes(body)
        info.write_bytes(receipt)
        paths.append((raw, info))
    return paths


async def frozen_sec_refs(session, snapshot_id):
    return list(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot_id,
                AgentDataSnapshotEvidenceRecord.source == "sec",
            )
        )
    )


@pytest.mark.asyncio
async def test_normal_prepare_selects_one_latest_revision_and_preserves_old_snapshot(
    migrated_database_url, tmp_path
):
    settings, store, identity, contents, paths = await setup_archive(
        migrated_database_url, tmp_path
    )
    first = await import_sec_exhibit(
        settings, identity_snapshot_id=identity.snapshot_id, archives=paths, store=store
    )
    factory = session_factory(settings)
    factory.configure(info={"sec_exhibit_store": store})
    try:
        old_cutoff = datetime.now(UTC)
        async with factory() as session, session.begin():
            old = await prepare_research_snapshot(session, identity.instrument_id, as_of=old_cutoff)
            assert old is not None
            assert await frozen_sec_refs(session, old.id) == [UUID(first["evidence_ref_id"])]
            old_payload = dict(old.payload)
        # Same accession/receipt time, a distinct valid revision. No new accounting semantics.
        changed = list(contents)
        parent_receipt = json.loads(changed[1][1])
        changed[1] = archive(
            changed[1][0] + b"<p>A separately preserved revision.</p>",
            parent_receipt["source_url"],
        )
        second = await import_sec_exhibit(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archives=write_archives(tmp_path / "revision", changed),
            store=store,
        )
        assert second["snapshot_id"] != first["snapshot_id"]
        async with factory() as session, session.begin():
            new = await prepare_research_snapshot(
                session, identity.instrument_id, as_of=datetime.now(UTC)
            )
            assert new is not None
            assert await frozen_sec_refs(session, new.id) == [UUID(second["evidence_ref_id"])]
            replay = await prepare_research_snapshot(
                session, identity.instrument_id, as_of=old_cutoff
            )
            assert replay is not None and replay.id == old.id and replay.payload == old_payload
            assert await frozen_sec_refs(session, old.id) == [UUID(first["evidence_ref_id"])]
        # A different filing must survive the same-accession revision filter.
        other_contents = []
        for body, receipt in contents:
            url = json.loads(receipt)["source_url"].replace(
                "000000004226000001", "000000004226000002"
            )
            other_contents.append(
                archive(body.replace(b"0000000042-26-000001", b"0000000042-26-000002"), url)
            )
        other = await import_sec_exhibit(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archives=write_archives(tmp_path / "other-filing", other_contents),
            store=store,
        )
        async with factory() as session, session.begin():
            combined = await prepare_research_snapshot(
                session, identity.instrument_id, as_of=datetime.now(UTC)
            )
            assert set(await frozen_sec_refs(session, combined.id)) == {
                UUID(second["evidence_ref_id"]),
                UUID(other["evidence_ref_id"]),
            }
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_optional_projection_reuses_originals_and_reads_both_revisions(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.persistence.models.data import DataArtifact, DataSnapshot
    from nexus.persistence.models.research import FinancialFactRecord
    from nexus.research.sec_disclosures import read_disclosures
    from sqlalchemy import func
    from unit.research.test_sec_exhibit_quarter_metrics import exhibit_html

    settings, store, identity, contents, _ = await setup_archive(migrated_database_url, tmp_path)
    child_receipt = json.loads(contents[2][1])
    contents[2] = archive(exhibit_html(), child_receipt["source_url"])
    paths = write_archives(tmp_path / "metrics-originals", contents)
    first = await import_sec_exhibit(
        settings, identity_snapshot_id=identity.snapshot_id, archives=paths, store=store
    )
    factory = session_factory(settings)
    factory.configure(info={"sec_exhibit_store": store})
    try:
        async with factory() as session:
            originals = [
                dict(row)
                for row in (await session.execute(select(DataArtifact.__table__))).mappings()
            ]
            old = await session.get(DataSnapshot, UUID(first["snapshot_id"]))
            old_payload = dict(old.payload)
            assert "quarter_metrics" not in old_payload
        second = await import_sec_exhibit(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archives=paths,
            store=store,
            include_quarter_metrics=True,
        )
        assert second["snapshot_id"] != first["snapshot_id"]
        ids = tuple(UUID(item["evidence_ref_id"]) for item in (first, second))
        async with factory() as session, session.begin():
            assert [
                dict(row)
                for row in (await session.execute(select(DataArtifact.__table__))).mappings()
            ] == originals
            reports = await read_disclosures(
                session,
                instrument_id=identity.instrument_id,
                evidence_ids=ids,
                as_of=datetime.now(UTC),
            )
            assert set(reports) == set(ids)
            assert reports[ids[0]].quarter_metrics is None
            assert (
                reports[ids[1]].quarter_metrics.raw_content_hash
                == reports[ids[1]].exhibit.raw_content_hash
            )
            assert (
                await session.get(DataSnapshot, UUID(first["snapshot_id"]))
            ).payload == old_payload
            prepared = await prepare_research_snapshot(
                session, identity.instrument_id, as_of=datetime.now(UTC)
            )
            assert await frozen_sec_refs(session, prepared.id) == [ids[1]]
            assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
            from decimal import Decimal

            from nexus.agents.codex_sdk import CodexCommittee
            from nexus.agents.committee import load_prompt_assets
            from nexus.agents.database_tools import database_read_only_tools
            from nexus.agents.models import ResearchRequest
            from nexus.agents.persistence import admit_research_run
            from nexus.domain.evidence import EvidenceRef
            from nexus.persistence.models.research import ResearchEvidenceRef
            from nexus.research.sec_exhibit_storage import verify_exhibit_originals
            from unit.agents.test_codex_sdk import _sdk

            report = reports[ids[1]]
            damaged_metrics = report.quarter_metrics.model_copy(
                update={"raw_content_hash": "0" * 64}
            )
            with pytest.raises(ValueError, match="SEC_EXHIBIT_FROZEN_ORIGINAL_MISMATCH"):
                await verify_exhibit_originals(
                    session,
                    store,
                    report.model_copy(update={"quarter_metrics": damaged_metrics}),
                    as_of=datetime.now(UTC),
                )
            request = ResearchRequest(
                instrument_id=identity.instrument_id,
                data_snapshot_id=prepared.id,
                decision_time=datetime.now(UTC),
            )
            admitted = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key="quarter-wire",
                request_hash="0" * 64,
                request=request,
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=datetime.now(UTC),
            )
            tools = await database_read_only_tools(
                session, admitted.run, allow_fixture_evidence=False
            )
            ref = await session.get(ResearchEvidenceRef, ids[1])
            evidence = (
                EvidenceRef(
                    source=ref.source,
                    object_id=ref.provider_object_id,
                    available_at=ref.available_at,
                    location=ref.location,
                    content_hash=ref.content_hash,
                ),
            )
            prompts = load_prompt_assets()
            _, thread, _, _ = _sdk(monkeypatch)
            await CodexCommittee(prompts=prompts, tools=tools).analyze(
                "orchestrator", request, evidence, prompts["orchestrator"]
            )
            wire = json.loads(thread.run.call_args.args[0])
            sent = wire["frozen_inputs"]["filing_disclosures"]["items"]
            assert len(sent) == 1
            assert sent[0]["disclosure"]["quarter_metrics"] == report.quarter_metrics.model_dump(
                mode="json"
            )
            assert thread.run.await_count == 1
        objects = dict(store.objects)
        again = await import_sec_exhibit(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archives=paths,
            store=store,
            include_quarter_metrics=True,
        )
        assert again["status"] == "existing" and again["snapshot_id"] == second["snapshot_id"]
        assert store.objects == objects
    finally:
        await factory.kw["bind"].dispose()
