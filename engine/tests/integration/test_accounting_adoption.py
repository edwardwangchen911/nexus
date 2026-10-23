"""Full operator adoption, future freeze and immutable legacy boundary on real PostgreSQL."""

import json
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.current_identity import MemoryStore
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.accounting_review import build_material, validate_submission
from nexus.research.accounting_review_storage import receive_submission
from nexus.research.sec_amendment_storage import persist_amendment
from nexus.research.sec_amendments import SecAmendmentDisclosure
from nexus.research.sec_disclosures import disclosure_excerpt
from nexus.settings import EngineSettings
from sqlalchemy import func, select

from engine.tests.integration.test_sec_local import INSTRUMENT, seed_instrument
from engine.tests.unit.research.test_accounting_adoption import designation
from engine.tests.unit.research.test_accounting_review_intake import manifest, submission

RAW = b"<html><body><p>This amendment adds a limited disclosure for investors.</p></body></html>"


class Store(MemoryStore):
    async def put_if_absent(self, *, key, content):
        uri = await super().put_if_absent(key=key, content=content)
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}" if key.startswith("evidence/") else uri

    async def read_evidence(self, key, *, uri):
        return self.objects[key]


def document():
    when = datetime(2026, 9, 1, 20, tzinfo=UTC)
    excerpt, truncated = disclosure_excerpt(RAW)
    return SecAmendmentDisclosure(
        instrument_id=INSTRUMENT,
        form="8-K/A",
        issuer_cik="0000320193",
        accession_number="0000320193-26-000002",
        source_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/doc.htm",
        filed_at=when,
        accepted_at=when,
        received_at=when,
        raw_byte_length=len(RAW),
        http_status=200,
        raw_content_hash=sha256(RAW).hexdigest(),
        excerpt=excerpt,
        truncated=truncated,
    )


async def seed_case(session, tmp_path):
    store = Store()
    session.info["sec_exhibit_store"] = store
    doc = document()
    await persist_amendment(session, store, doc, raw=RAW)
    amended = await session.scalar(select(Filing))
    when = datetime(2026, 9, 3, tzinfo=UTC)
    ref = ResearchEvidenceRef(
        source="sec",
        provider_object_id="0000320193-25-000079",
        revision="original",
        available_at=when,
        created_at=when,
        content_hash="a" * 64,
        byte_length=10,
        location="memory://sha256/" + "a" * 64,
    )
    session.add(ref)
    await session.flush()
    filing = Filing(
        instrument_id=INSTRUMENT,
        evidence_ref_id=ref.id,
        source="sec",
        provider_object_id=ref.provider_object_id,
        revision=ref.revision,
        accession_number=ref.provider_object_id,
        form="10-K",
        filed_at=when,
        accepted_at=when,
        created_at=when,
    )
    session.add(filing)
    await session.flush()
    fact = FinancialFactRecord(
        id=UUID(int=1),
        instrument_id=INSTRUMENT,
        filing_id=filing.id,
        evidence_ref_id=ref.id,
        accession_number=ref.provider_object_id,
        revision=ref.revision,
        form="10-K",
        canonical_fact="diluted_eps",
        raw_taxonomy="us-gaap",
        raw_tag="EarningsPerShareDiluted",
        period_kind="duration",
        period_start=date(2024, 9, 29),
        period_end=date(2025, 9, 27),
        unit="USD/shares",
        value=Decimal("7.460000000000"),
        filed_at=when,
        accepted_at=when,
        created_at=when,
    )
    session.add(fact)
    await session.flush()
    raw = (
        b'<html xmlns:x="http://www.xbrl.org/2003/instance" xmlns:ix="http://www.xbrl'
        b'.org/2013/inlineXBRL"><x:context id="c"><x:entity><x:identifier scheme="htt'
        b'p://www.sec.gov/CIK">0000320193</x:identifier></x:entity><x:period><x:start'
        b"Date>2024-09-29</x:startDate><x:endDate>2025-09-27</x:endDate></x:period></"
        b'x:context><x:unit id="u"><x:divide><x:unitNumerator><x:measure>iso4217:USD<'
        b"/x:measure></x:unitNumerator><x:unitDenominator><x:measure>xbrli:shares</x:"
        b'measure></x:unitDenominator></x:divide></x:unit><ix:nonFraction id="eps" na'
        b'me="us-gaap:EarningsPerShareDiluted" contextRef="c" unitRef="u">7.46</ix:no'
        b'nFraction><ix:nonNumeric name="dei:EntityCentralIndexKey">0000320193</ix:no'
        b'nNumeric><ix:nonNumeric name="dei:DocumentType">10-K</ix:nonNumeric></html>'
    )
    data = manifest(tmp_path)
    data["instrument_id"] = str(INSTRUMENT)
    (tmp_path / "eps.xml").write_bytes(raw)
    (tmp_path / "source.txt").write_bytes(RAW)
    data["sources"][0].update(
        sha256=sha256(RAW).hexdigest(), byte_length=len(RAW), source_url=doc.source_url
    )
    data["sources"].append(
        {
            **data["sources"][0],
            "relative_path": "eps.xml",
            "source_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl.htm",
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
        }
    )
    data["amendments"][0].update(
        evidence_ref_id=str(amended.evidence_ref_id),
        accession_number=doc.accession_number,
        source_sha256=sha256(RAW).hexdigest(),
    )
    context = ET.fromstring(raw).find("{http://www.xbrl.org/2003/instance}context")
    ch = sha256(ET.tostring(context, encoding="unicode").encode()).hexdigest()
    binding = {
        k: str(getattr(fact, k))
        for k in (
            "instrument_id",
            "canonical_fact",
            "raw_taxonomy",
            "raw_tag",
            "period_kind",
            "unit",
            "accession_number",
            "revision",
            "evidence_ref_id",
        )
    }
    binding.update(
        period_start="2024-09-29",
        period_end="2025-09-27",
        value="7.460000000000",
        fact_created_at=when.isoformat(),
        evidence_content_hash=ref.content_hash,
        evidence_revision=ref.revision,
        evidence_available_at=when.isoformat(),
        xbrl_nodes_json=json.dumps(
            [
                {
                    "attributes": {"id": "eps", "contextRef": "c", "unitRef": "u"},
                    "context_xml_sha256": ch,
                }
            ]
        ),
    )
    data["facts"][0].update(
        binding=binding, original_source_sha256=sha256(raw).hexdigest(), context_sha256=ch
    )
    now = datetime.now(UTC)
    material = build_material(data, source_root=tmp_path, now=now)
    opinion_data = submission(material)
    opinion_data["reviewed_at"] = now.isoformat()
    opinion_data["findings"][0].update(amendment_evidence_ref_id=str(amended.evidence_ref_id))
    opinion_data["findings"][0]["citations"][0].update(
        source_sha256=sha256(RAW).hexdigest(), quote="limited disclosure"
    )
    opinion = validate_submission(opinion_data, package=material, source_root=tmp_path, now=now)
    receipt = await receive_submission(session, material, opinion, source_root=tmp_path)
    return material, opinion, receipt, ref, amended, store


@pytest.mark.asyncio
async def test_designation_adoption_freeze_and_late_evidence_never_changes_old_snapshot(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.research.accounting_adoption_storage import (
        adopt_review,
        read_adoptions,
        register_designation,
    )

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    from nexus.ops.openai_local import prepare_database

    await prepare_database(settings)
    await seed_instrument(settings)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            material, opinion, receipt, base, amended, store = await seed_case(session, tmp_path)
            before = await session.scalar(select(func.clock_timestamp()))
            old = await prepare_research_snapshot(session, INSTRUMENT, as_of=before)
            old_payload = dict(old.payload)
            with pytest.raises(ValueError, match="DESIGNATION"):
                await adopt_review(
                    session, opinion.submission_id, UUID(int=20), source_root=tmp_path
                )
            authorization = await register_designation(
                session, opinion.submission_id, designation(material, opinion)
            )
            ref = await adopt_review(
                session, opinion.submission_id, UUID(int=20), source_root=tmp_path
            )
            repeated = await adopt_review(
                session, opinion.submission_id, UUID(int=20), source_root=tmp_path
            )
            assert ref.id == repeated.id
            assert ref.available_at >= authorization.available_at > before
            assert receipt.payload["status"] == "received_unverified"
            assert not await read_adoptions(session, instrument_id=INSTRUMENT, as_of=before)
            now = await session.scalar(select(func.clock_timestamp()))
            found = await read_adoptions(session, instrument_id=INSTRUMENT, as_of=now)
            assert set(found) == {ref.id}
            new = await prepare_research_snapshot(session, INSTRUMENT, as_of=now)
            frozen = set(
                await session.scalars(
                    select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                        AgentDataSnapshotEvidenceRecord.data_snapshot_id == new.id
                    )
                )
            )
            assert {ref.id, base.id, amended.evidence_ref_id} <= frozen
            assert old.payload == old_payload and str(ref.id) not in old.payload["evidence_ids"]
            assert str(ref.id) in new.payload["evidence_ids"]
            assert new.id != old.id
            assert not await read_adoptions(
                session, instrument_id=INSTRUMENT, as_of=now, evidence_ids=()
            )
            # Neither a different issuer nor a forged frozen reference is usable.
            with pytest.raises(ValueError, match="ACCOUNTING_ADOPTION_INVALID"):
                await read_adoptions(
                    session, instrument_id=UUID(int=999), as_of=now, evidence_ids=(ref.id,)
                )
            conflict = designation(material, opinion)
            conflict["report_text"] = "A different report cannot reuse the designation identity."
            with pytest.raises(ValueError, match="DESIGNATION_CONFLICT"):
                await register_designation(session, opinion.submission_id, conflict)
        import httpx
        from helpers.portfolio_api import app, headers
        from nexus.agents.database_tools import database_read_only_tools
        from nexus.persistence.models.agents import AgentResearchRunRecord

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=now)),
            base_url="http://testserver",
        ) as client:
            for label, snapshot in (("old", old), ("new", new)):
                body = json.dumps(
                    {
                        "instrument_id": str(INSTRUMENT),
                        "data_snapshot_id": str(snapshot.id),
                        "decision_time": snapshot.observed_at.isoformat(),
                    }
                ).encode()
                path = "/internal/v1/research-runs"
                response = await client.post(
                    path,
                    content=body,
                    headers={**headers("POST", path, body), "Idempotency-Key": f"review-{label}"},
                )
                assert response.status_code == 202, response.text
                async with factory() as session:
                    session.info["sec_exhibit_store"] = store
                    run = await session.get(
                        AgentResearchRunRecord, UUID(response.json()["data"]["run_id"])
                    )
                    tools = await database_read_only_tools(
                        session, run, allow_fixture_evidence=True
                    )
                    facts = await tools.execute(
                        "get_financial_facts",
                        {
                            "instrument_id": INSTRUMENT,
                            "data_snapshot_id": snapshot.id,
                            "as_of": run.decision_time,
                            "fields": ["diluted_eps"],
                        },
                    )
                    assert facts["status"] == "ok"
                    if label == "old":
                        assert "accounting_review_contract" not in facts
                        assert facts["items"][0]["amendment_review_status"] == "unverified"
                    else:
                        assert facts["accounting_review_contract"] == "as_disclosed_eps_v1"
                        assert (
                            facts["items"][0]["amendment_review_status"]
                            == "as_disclosed_not_replaced"
                        )
                        assert facts["items"][0]["accounting_review_evidence_ref_ids"] == [
                            str(ref.id)
                        ]
        import asyncio
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="cannot remove ownership support"):
            await asyncio.to_thread(command.downgrade, config, "0085_manual_host_exits")
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_operator_designate_adopt_and_inspect(
    migrated_database_url, tmp_path, monkeypatch
):
    import argparse

    from nexus.ops.accounting_review_local import database_action

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await seed_instrument(settings)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        material, opinion, _, _, _, store = await seed_case(session, tmp_path)
    monkeypatch.setattr("nexus.ops.openai_local.local_settings", lambda *a, **k: settings)
    monkeypatch.setattr("nexus.ops.local_stack._validate_local_env", lambda *a: None)
    factory.configure(info={"engine_settings": settings, "sec_exhibit_store": store})
    monkeypatch.setattr("nexus.persistence.session.session_factory", lambda settings: factory)
    path = tmp_path / "designation.json"
    path.write_text(json.dumps(designation(material, opinion)))
    args = argparse.Namespace(
        action="designate",
        repo_root=tmp_path,
        submission_id=opinion.submission_id,
        designation=path,
        designation_id=UUID(int=20),
        sources=tmp_path,
        operator_confirmed=False,
        material=None,
        submission=None,
    )
    with pytest.raises(ValueError, match="OPERATOR"):
        await database_action(args)
    args.operator_confirmed = True
    registered = await database_action(args)
    assert registered["status"] == "owner_designation_registered"
    args.action = "adopt"
    adopted = await database_action(args)
    assert adopted["status"] == "adopted_as_disclosed_scope"
    assert adopted["model_calls"] == adopted["provider_calls"] == 0
    again = await database_action(args)
    assert again == adopted
    args.action = "inspect-adoption"
    assert await database_action(args) == adopted
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_adoption_verified_after_cutoff_cannot_enter_that_cutoff(
    migrated_database_url, tmp_path, monkeypatch
):
    import asyncio

    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.research import accounting_source_proof
    from nexus.research.accounting_adoption_storage import (
        adopt_review,
        read_adoptions,
        register_designation,
    )

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await seed_instrument(settings)
    factory = session_factory(settings)
    entered, release = asyncio.Event(), asyncio.Event()
    verify = accounting_source_proof.verify_adoption_sources

    async def paused_verification(*args, **kwargs):
        entered.set()
        await release.wait()
        await verify(*args, **kwargs)

    monkeypatch.setattr(accounting_source_proof, "verify_adoption_sources", paused_verification)
    task = None
    try:
        async with factory() as session, session.begin():
            material, opinion, _, _, _, store = await seed_case(session, tmp_path)
            await register_designation(
                session, opinion.submission_id, designation(material, opinion)
            )

        async def adopting():
            async with factory() as session, session.begin():
                session.info["sec_exhibit_store"] = store
                return await adopt_review(
                    session, opinion.submission_id, UUID(int=20), source_root=tmp_path
                )

        task = asyncio.create_task(adopting())
        await asyncio.wait_for(entered.wait(), timeout=30)
        async with factory() as session, session.begin():
            cutoff = await session.scalar(select(func.clock_timestamp()))
            old = await prepare_research_snapshot(session, INSTRUMENT, as_of=cutoff)
            old_payload = dict(old.payload)
        release.set()
        ref = await task
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            assert not await read_adoptions(session, instrument_id=INSTRUMENT, as_of=cutoff)
            after = await session.scalar(select(func.clock_timestamp()))
            assert set(await read_adoptions(session, instrument_id=INSTRUMENT, as_of=after)) == {
                ref.id
            }
            earlier = await prepare_research_snapshot(session, INSTRUMENT, as_of=cutoff)
            later = await prepare_research_snapshot(session, INSTRUMENT, as_of=after)
            assert str(ref.id) not in earlier.payload["evidence_ids"]
            assert str(ref.id) in later.payload["evidence_ids"]
            assert old.payload == old_payload
            repeated = await adopt_review(
                session, opinion.submission_id, UUID(int=20), source_root=tmp_path
            )
            assert repeated.id == ref.id
            assert repeated.available_at == repeated.created_at == ref.available_at > cutoff
    finally:
        release.set()
        if task is not None and not task.done():
            await task
        await factory.kw["bind"].dispose()


def test_accounting_ownership_migration_roundtrip_without_frozen_reviews(
    temporary_database_url, monkeypatch
):
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    from engine.tests.integration.test_research_identity_migration import definition

    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0085_manual_host_exits")
    before = asyncio.run(definition(temporary_database_url))
    command.upgrade(config, "head")
    command.downgrade(config, "0085_manual_host_exits")
    assert asyncio.run(definition(temporary_database_url)) == before


@pytest.mark.asyncio
async def test_new_snapshot_retains_unknown_and_later_favorable_adoptions(
    migrated_database_url, tmp_path
):
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.research.accounting_adoption import fact_review_fields
    from nexus.research.accounting_adoption_storage import (
        adopt_review,
        read_adoptions,
        register_designation,
    )

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await seed_instrument(settings)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            material, original, _, _, amendment, _ = await seed_case(session, tmp_path)
            ids = set()
            for number, conclusion in (
                (30, "as_disclosed_not_replaced"),
                (31, "unknown"),
                (32, "as_disclosed_not_replaced"),
            ):
                data = original.model_dump(mode="json")
                data["submission_id"] = str(UUID(int=number))
                data["findings"][0]["conclusion"] = conclusion
                opinion = validate_submission(
                    data, package=material, source_root=tmp_path, now=datetime.now(UTC)
                )
                await receive_submission(session, material, opinion, source_root=tmp_path)
                authorization = designation(material, opinion)
                authorization["designation_id"] = str(UUID(int=number + 100))
                await register_designation(session, opinion.submission_id, authorization)
                ref = await adopt_review(
                    session, opinion.submission_id, UUID(int=number + 100), source_root=tmp_path
                )
                ids.add(ref.id)
            now = await session.scalar(select(func.clock_timestamp()))
            snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=now)
            assert {str(item) for item in ids} <= set(snapshot.payload["evidence_ids"])
            reviews = await read_adoptions(
                session, instrument_id=INSTRUMENT, as_of=now, evidence_ids=tuple(ids)
            )
            fields = fact_review_fields(UUID(int=1), (amendment.evidence_ref_id,), reviews)
            assert fields["amendment_review_status"] == "conflict"
            assert set(fields["accounting_review_evidence_ref_ids"]) == {str(item) for item in ids}
    finally:
        await factory.kw["bind"].dispose()
