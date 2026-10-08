"""Synthetic normal identity import → frozen research → tool; no model/provider calls."""

import asyncio
import importlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.current_identity import archives
from helpers.research_snapshots import seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.openai_local import prepare_database
from nexus.ops.research_identity_binding_local import register_identity
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from unit.market.test_object_store import FakeS3Client


async def seed(factory, symbol="AAPL"):
    identifier = uuid4()
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=identifier,
                asset_type="common_stock",
                exchange="XNAS",
                currency="USD",
                risk_bucket="EQUITY",
                tick_size=None,
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=identifier,
                symbol=symbol,
                alias_type="ticker",
                valid_from=datetime.now(UTC).date() - timedelta(days=3),
                valid_to=None,
            )
        )
    return identifier


def source_archive(path):
    path.mkdir(mode=0o700)
    # Synthetic receipt evidence is comfortably before admission; do not test
    # contract correctness against host/container submillisecond clock skew.
    received = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    documents = {
        "directory": (
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[123456, "Synthetic", "AAPL", "NYSE"]],
            },
            "https://www.sec.gov/files/company_tickers_exchange.json",
        ),
        "AAPL-submissions": (
            {"cik": "123456", "tickers": ["AAPL"], "exchanges": ["NYSE"]},
            "https://data.sec.gov/submissions/CIK0000123456.json",
        ),
    }
    for name, (document, url) in documents.items():
        raw = json.dumps(document).encode()
        for filename, content in (
            (name + ".json", raw),
            (
                name + "-receipt.json",
                json.dumps(
                    {
                        "schema": "sec_identity_source_receipt_v1",
                        "source_url": url,
                        "http_status": 200,
                        "received_at": received,
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                    }
                ).encode(),
            ),
        ):
            saved = path / filename
            saved.write_bytes(content)
            saved.chmod(0o600)
    saved = path / "result.json"
    saved.write_text(
        json.dumps(
            {
                "status": "archived",
                "requests": 2,
                "authority_registered": False,
                "execution_locked": True,
            }
        )
    )
    saved.chmod(0o600)


@pytest.mark.asyncio
async def test_exact_new_frozen_identity_and_old_run_unchanged(migrated_database_url, tmp_path):
    module = importlib.import_module("nexus.research.current_identity_storage")
    settings = EngineSettings(
        database_url=migrated_database_url, agent_runtime_mode=AgentRuntimeMode.CODEX_SDK
    ).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(settings, client=client)
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        await prepare_database(settings)
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            old = await prepare_research_snapshot(session, instrument, as_of=datetime.now(UTC))
            old_id, old_payload, old_revision = old.id, dict(old.payload), old.revision
        source_archive(tmp_path / "current")
        registered = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        assert registered["status"] == "registered"
        assert registered["execution_locked"] is True
        async with factory() as session, session.begin():
            ref = await module.import_identity_binding(
                session,
                store,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "current",
            )
            replay = await module.import_identity_binding(
                session,
                store,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "current",
            )
            assert replay.id == ref.id
            assert registered["evidence_id"] == str(ref.id)
            ref_id = ref.id
        decision = datetime.now(UTC)
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            frozen = await prepare_research_snapshot(session, instrument, as_of=decision)
            assert str(ref_id) in frozen.payload["evidence_ids"]
            old = await session.get(DataSnapshot, old_id)
            assert (old.payload, old.revision) == (old_payload, old_revision)
            assert str(ref_id) not in old.payload["evidence_ids"]
            for snapshot, expected in ((old, 0), (frozen, 1)):
                cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
                admission = await admit_research_run(
                    session,
                    actor_id="test",
                    route="/internal/v1/research-runs",
                    idempotency_key=f"identity-{expected}",
                    request_hash=str(expected) * 64,
                    request=ResearchRequest(
                        instrument_id=instrument, data_snapshot_id=snapshot.id, decision_time=cutoff
                    ),
                    prompt_version_id=None,
                    model_config_id=None,
                    daily_limit_usd=Decimal("3"),
                    now=cutoff,
                )
                tools = await database_read_only_tools(
                    session, admission.run, allow_fixture_evidence=True
                )
                result = await tools.execute(
                    "get_security_snapshot",
                    {
                        "instrument_id": str(instrument),
                        "data_snapshot_id": str(snapshot.id),
                        "as_of": cutoff.isoformat(),
                    },
                )
                bindings = [
                    row["research_identity_binding"]
                    for row in result["items"]
                    if "research_identity_binding" in row
                ]
                assert len(bindings) == expected
                if bindings:
                    assert bindings[0]["issuer_cik"] == "0000123456"
                    assert bindings[0]["execution_locked"] is True
            assert await load_frozen_evidence(session, admission.run, allow_fixture_evidence=False)
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=cutoff,
                evidence_ids=(uuid4(),),
            )
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=datetime.fromisoformat(old.payload["as_of"]),
                evidence_ids=(ref_id,),
            )
            assert not await module.read_identity_bindings(
                session,
                instrument_id=instrument,
                as_of=decision + timedelta(days=1),
                evidence_ids=(ref_id,),
            )
            row = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == frozen.id,
                    AgentDataSnapshotEvidenceRecord.evidence_ref_id == ref_id,
                )
            )
            copied = {column.name: getattr(row, column.name) for column in row.__table__.columns}
            foreign = await seed(factory, "OTHER")
            for override in (
                {"instrument_id": foreign},
                {"data_snapshot_id": old.id},
                {"instrument_id": foreign, "authority_kind": "price", "selected_at": decision},
            ):
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        session.add(AgentDataSnapshotEvidenceRecord(**(copied | override)))
                        await session.flush()
        # Exact frozen readback cannot silently fall back after source corruption.
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(RuntimeError, match="cannot remove ownership support"):
            await asyncio.to_thread(command.downgrade, config, "0065_portfolio_management")
        key = next(k for k in client.objects if "/sec_research_identity_source_v1/" in k)
        client.objects[key] += b" "
        async with factory() as session:
            session.info["sec_exhibit_store"] = store
            assert (
                await module.read_identity_bindings(
                    session, instrument_id=instrument, as_of=decision, evidence_ids=(ref_id,)
                )
                == {}
            )
            assert not await load_frozen_evidence(
                session, admission.run, allow_fixture_evidence=False
            )
            tools = await database_read_only_tools(
                session, admission.run, allow_fixture_evidence=False
            )
            rejected = await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": str(instrument),
                    "data_snapshot_id": str(frozen.id),
                    "as_of": decision.isoformat(),
                },
            )
            assert rejected["status"] == "unavailable"
            assert rejected["reason"] == "invalid_frozen_evidence_authority"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["identity_only", "capacity", "ambiguous"])
async def test_identity_remains_supplemental_and_bounded(migrated_database_url, tmp_path, case):
    from nexus.persistence.models.research import Filing, ResearchEvidenceRef
    from nexus.research.current_identity_storage import (
        import_identity_binding,
        read_identity_bindings,
    )

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        base = await ops.revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        source_archive(tmp_path / "current")
        result = await register_identity(
            settings,
            identity_snapshot_id=base.snapshot_id,
            collection_dir=tmp_path / "current",
            store=store,
        )
        async with factory() as session, session.begin():
            session.info["sec_exhibit_store"] = store
            if case != "identity_only":
                await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            if case == "capacity":
                stamp = datetime.now(UTC) - timedelta(days=1)
                for index in range(97):
                    digest = sha256(str(index).encode()).hexdigest()
                    ref = ResearchEvidenceRef(
                        source="sec",
                        provider_object_id=f"capacity:{index}",
                        revision="v1",
                        available_at=stamp,
                        created_at=stamp,
                        content_hash=digest,
                        location="memory://sha256/" + digest,
                        byte_length=10,
                    )
                    session.add(ref)
                    await session.flush()
                    session.add(
                        Filing(
                            instrument_id=instrument,
                            evidence_ref_id=ref.id,
                            source="sec",
                            provider_object_id=ref.provider_object_id,
                            revision=ref.revision,
                            accession_number=f"0000000001-26-{index + 2:06d}",
                            form="10-Q",
                            filed_at=stamp,
                            accepted_at=stamp,
                            created_at=stamp,
                        )
                    )
                await session.flush()
            if case == "ambiguous":
                source_archive(tmp_path / "another")
                second = await import_identity_binding(
                    session,
                    store,
                    identity_snapshot_id=base.snapshot_id,
                    collection_dir=tmp_path / "another",
                )
                assert str(second.id) != result["evidence_id"]
            cutoff = datetime.now(UTC)
            identities = await read_identity_bindings(
                session, instrument_id=instrument, as_of=cutoff
            )
            assert bool(identities) is (case != "ambiguous")
            frozen = await prepare_research_snapshot(session, instrument, as_of=cutoff)
            if case == "identity_only":
                assert frozen is None
            elif case == "capacity":
                assert len(frozen.payload["evidence_ids"]) == 98
                assert result["evidence_id"] in frozen.payload["evidence_ids"]
            else:
                assert result["evidence_id"] not in frozen.payload["evidence_ids"]
                assert str(second.id) not in frozen.payload["evidence_ids"]
                # Exact frozen identity remains readable; no latest-version fallback.
                assert await read_identity_bindings(
                    session,
                    instrument_id=instrument,
                    as_of=cutoff,
                    evidence_ids=(second.id,),
                )
    finally:
        await factory.kw["bind"].dispose()
