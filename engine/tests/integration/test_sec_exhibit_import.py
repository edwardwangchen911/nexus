"""Normal identity/archive import and strict readback, with no research admission."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.current_identity import CIK, MemoryStore, archives
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.instrument_identity_local import import_identity
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.research.sec_disclosures import read_disclosures
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.research.test_sec_exhibits import archive, exhibit_archives


@pytest.mark.asyncio
async def test_normal_exhibit_import_readback_verifies_originals_and_old_cutoff(
    migrated_database_url,
    tmp_path,
):
    from nexus.ops.sec_exhibit_local import import_sec_exhibit

    settings = EngineSettings(
        database_url=migrated_database_url, object_store_endpoint="http://127.0.0.1:9000"
    )

    class Store(MemoryStore):
        async def put_if_absent(self, *, key, content):
            uri = await super().put_if_absent(key=key, content=content)
            return (
                f"memory://sha256/{key.rsplit('/', 1)[-1]}" if key.startswith("evidence/") else uri
            )

        async def read_evidence(self, key, *, uri):
            assert uri == f"memory://sha256/{key.rsplit('/', 1)[-1]}"
            return self.objects[key]

    store = Store()
    fmp, sec = archives(tmp_path / "identity")
    identity = await import_identity(
        settings,
        symbol="AAPL",
        fmp_archive=fmp,
        sec_archive=sec,
        store=store,
    )
    source = exhibit_archives()
    metadata = json.loads(source[0][0])
    metadata["cik"] = int(CIK)
    source[0] = archive(
        json.dumps(metadata).encode(), f"https://data.sec.gov/submissions/CIK{CIK}.json"
    )
    for index in (1, 2):
        receipt = json.loads(source[index][1])
        source[index] = archive(
            source[index][0], receipt["source_url"].replace("/data/1/", "/data/123456/")
        )
    paths = []
    for index, (raw, receipt) in enumerate(source):
        body, info = tmp_path / f"{index}.raw", tmp_path / f"{index}.json"
        body.write_bytes(raw)
        info.write_bytes(receipt)
        paths.append((body, info))
    cutoff = datetime.now(UTC)
    result = await import_sec_exhibit(
        settings,
        identity_snapshot_id=identity.snapshot_id,
        archives=paths,
        store=store,
    )
    assert result["status"] == "created"
    factory = session_factory(settings)
    factory.configure(info={"sec_exhibit_store": store})
    try:
        async with factory() as session, session.begin():
            evidence_id = UUID(result["evidence_ref_id"])
            assert not await read_disclosures(
                session,
                instrument_id=identity.instrument_id,
                evidence_ids=(evidence_id,),
                as_of=cutoff,
            )
            current = datetime.now(UTC)
            reports = await read_disclosures(
                session,
                instrument_id=identity.instrument_id,
                evidence_ids=(evidence_id,),
                as_of=current,
            )
            report = reports[evidence_id]
            assert "GAAP operating cash flow" in report.exhibit.excerpt
            assert "operating cash flow" not in report.excerpt
            snapshot = await prepare_research_snapshot(
                session, identity.instrument_id, as_of=current
            )
            assert snapshot is not None
            for model in (FinancialFactRecord, AgentResearchRunRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
        objects = dict(store.objects)
        again = await import_sec_exhibit(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archives=paths,
            store=store,
        )
        assert (
            again["status"] == "existing" and again["evidence_ref_id"] == result["evidence_ref_id"]
        )
        assert objects == store.objects
        # A valid canonical JSON cannot conceal a damaged raw exhibit.
        digest = sha256(source[2][0]).hexdigest()
        key = next(key for key in store.objects if digest in key)
        store.objects[key] = b"damaged"
        async with factory() as session:
            assert not await read_disclosures(
                session,
                instrument_id=identity.instrument_id,
                evidence_ids=(evidence_id,),
                as_of=datetime.now(UTC),
            )
            assert (await session.get(DataSnapshot, UUID(result["snapshot_id"]))) is not None
    finally:
        await factory.kw["bind"].dispose()
