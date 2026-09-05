"""Normal archive to tools/SDK/API; only source storage and SDK transport are synthetic."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from botocore.exceptions import ClientError
from helpers.current_identity import CIK, MemoryStore, archives
from helpers.portfolio_api import app, headers
from nexus.agents.codex_sdk import CodexCommittee
from nexus.agents.committee import load_prompt_assets
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.domain.evidence import EvidenceRef
from nexus.ops.instrument_identity_local import import_identity
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.sec_disclosures import read_disclosures
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _sdk
from unit.research.test_sec_exhibits import NOW, archive, exhibit_archives


@pytest.mark.asyncio
@pytest.mark.parametrize("listing_revision", [False, True])
async def test_normal_exhibit_import_readback_verifies_originals_and_old_cutoff(
    migrated_database_url,
    tmp_path,
    monkeypatch,
    listing_revision,
):
    from nexus.ops.sec_exhibit_local import import_sec_exhibit

    settings = EngineSettings(
        database_url=migrated_database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    await prepare_database(settings)

    class Store(MemoryStore):
        missing_key = None

        async def read_raw(self, key, *, uri):
            if key == self.missing_key:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return await super().read_raw(key, uri=uri)

        async def put_if_absent(self, *, key, content):
            uri = await super().put_if_absent(key=key, content=content)
            return (
                f"memory://sha256/{key.rsplit('/', 1)[-1]}" if key.startswith("evidence/") else uri
            )

        async def read_evidence(self, key, *, uri):
            assert uri == f"memory://sha256/{key.rsplit('/', 1)[-1]}"
            return self.objects[key]

    store = Store()
    fmp, sec = archives(tmp_path / "identity", received=NOW.isoformat())
    importer = import_identity
    if listing_revision:
        from integration.test_current_listing_revision import seed
        from nexus.ops.instrument_identity_local import revise_listing

        identity_factory = session_factory(settings)
        try:
            await seed(identity_factory)
        finally:
            await identity_factory.kw["bind"].dispose()
        importer = revise_listing
    identity = await importer(
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
            inputs = await read_research_inputs(
                session,
                snapshot_id=snapshot.id,
                instrument_id=identity.instrument_id,
                as_of=current,
            )
            assert inputs.filings[0].exhibit.excerpt == report.exhibit.excerpt
            assert inputs.filings[0].exhibit.parent_raw_content_hash == report.raw_content_hash
            for model in (FinancialFactRecord, AgentResearchRunRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            request = ResearchRequest(
                instrument_id=identity.instrument_id,
                data_snapshot_id=snapshot.id,
                decision_time=current,
            )
            admitted = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key="exhibit-wire",
                request_hash="0" * 64,
                request=request,
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=current,
            )
            run_id = admitted.run.id
            tools = await database_read_only_tools(
                session, admitted.run, allow_fixture_evidence=False
            )
            ref = await session.get(ResearchEvidenceRef, evidence_id)
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
            committee = CodexCommittee(prompts=prompts, tools=tools)
            await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
            wire = json.loads(thread.run.call_args.args[0])
            sent = wire["frozen_inputs"]["filing_disclosures"]["items"][0]["disclosure"]
            assert sent["exhibit"]["excerpt"] == report.exhibit.excerpt
            assert sent["raw_content_hash"] == report.raw_content_hash
            assert thread.run.await_count == 1
        application = app(migrated_database_url, now=datetime.now(UTC))
        application.state.session_factory.configure(info={"sec_exhibit_store": store})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application),
            base_url="http://testserver",
        ) as client:
            path = f"/internal/v1/research-runs/{run_id}"
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            filing = response.json()["data"]["inputs"]["filings"][0]
            assert filing["exhibit"]["excerpt"] == report.exhibit.excerpt
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
        store.objects = objects
        store.missing_key = key
        async with factory() as session:
            assert not await read_disclosures(
                session, instrument_id=identity.instrument_id, evidence_ids=(evidence_id,),
                as_of=datetime.now(UTC),
            )
    finally:
        await factory.kw["bind"].dispose()
