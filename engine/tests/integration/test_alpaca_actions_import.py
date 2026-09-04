"""Normal collector and import CLI, real PG and S3 adapter; synthetic network bytes."""

import asyncio
import json
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from uuid import UUID

import httpx
import pytest
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops import alpaca_actions_local as collector
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataArtifact, DataSnapshot, PriceBarSetAuthority
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.providers.corporate_actions.alpaca_http import CorporateActionsRequest
from nexus.research.alpaca_actions import SOURCE
from nexus.research.alpaca_actions_storage import read_action_observations
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client


async def collect_synthetic_archive(directory, monkeypatch):
    raw = (
        b'{"corporate_actions":{"cash_dividends":[{"id":"synthetic-import-event",'
        b'"symbol":"TEST","ex_date":"2026-08-15","rate":0.123456789012345678901}]},'
        b'"next_page_token":null}'
    )
    real_client = httpx.AsyncClient

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW + timedelta(minutes=1)

    def response(request):
        assert request.url.host == "data.alpaca.markets"
        assert request.url.path == "/v1/corporate-actions"
        assert request.url.params["symbols"] == "TEST"
        return httpx.Response(200, content=raw)

    directory.mkdir(mode=0o700)
    with monkeypatch.context() as scoped:
        scoped.setattr(collector, "datetime", Clock)
        scoped.setattr(
            collector.httpx,
            "AsyncClient",
            lambda **kwargs: real_client(
                **kwargs,
                transport=httpx.MockTransport(response),
            ),
        )
        result = await collector.collect_actions(
            CorporateActionsRequest("TEST", date(2026, 8, 1), date(2026, 8, 31)),
            key_id=SecretStr("synthetic-key"),
            secret=SecretStr("synthetic-secret"),
            output_dir=directory,
            max_pages=1,
        )
    assert result["status"] == "collected"
    assert (directory / "page-001.json").read_bytes() == raw
    return raw


@pytest.mark.asyncio
async def test_normal_cli_import_freezes_actual_archived_bytes_and_replays_evidence(
    migrated_database_url,
    tmp_path,
    monkeypatch,
    capsys,
):
    from nexus.ops import alpaca_actions_import as entry

    tmp_path.chmod(0o700)
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        await seed_filing(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        old_hash = old.revision
        old_id = old.id
    directory = tmp_path / "collection"
    raw = await collect_synthetic_archive(directory, monkeypatch)
    infra = tmp_path / "infra"
    infra.mkdir(mode=0o700)
    config = {
        "DATABASE_URL": migrated_database_url,
        "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "synthetic-actions",
        "OBJECT_STORE_PREFIX": "synthetic-actions",
        "OBJECT_STORE_ACCESS_KEY_ID": "synthetic-access",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic-secret",
    }
    configuration = infra / ".env.local"
    configuration.write_text("\n".join(f"{key}={value}" for key, value in config.items()))
    configuration.chmod(0o600)
    client = FakeS3Client()

    def store(configured, **kwargs):
        return S3ProviderEvidenceStore(configured, client=client, **kwargs)

    monkeypatch.setattr(entry, "S3ProviderEvidenceStore", store)
    references = []
    for name in ("result.json", "replay.json"):
        output = tmp_path / name
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "alpaca-actions-import",
                "--repo-root",
                str(tmp_path),
                "--instrument-id",
                str(INSTRUMENT),
                "--collection-dir",
                str(directory),
                "--output",
                str(output),
            ],
        )
        assert await asyncio.to_thread(entry.main) == 0
        captured = capsys.readouterr()
        assert json.loads(captured.out) == {
            "status": "verified",
            "frozen_readback_verified": True,
            "model_called": False,
        }
        assert captured.err == ""
        assert output.stat().st_mode & 0o777 == 0o600
        references.append(json.loads(output.read_text()))
    assert references[0]["evidence_id"] == references[1]["evidence_id"]
    assert len(client.objects) == 4
    assert raw in client.objects.values()
    result = references[0]
    decision = datetime.fromisoformat(result["as_of"])
    async with factory() as session, session.begin():
        evidence_id = UUID(result["evidence_id"])
        snapshot_id = UUID(result["research_snapshot_id"])
        frozen = await session.get(DataSnapshot, snapshot_id)
        assert str(evidence_id) in frozen.payload["evidence_ids"]
        old = await session.get(DataSnapshot, old_id)
        assert old.revision == old_hash and str(evidence_id) not in old.payload["evidence_ids"]
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 4
        assert (
            await session.scalar(
                select(func.count())
                .select_from(ResearchEvidenceRef)
                .where(
                    ResearchEvidenceRef.source == SOURCE,
                )
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(CorporateActionRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0
        documents = await read_action_observations(
            session,
            instrument_id=INSTRUMENT,
            as_of=decision,
            evidence_ids=(evidence_id,),
        )
        document = documents[evidence_id]
        assert document.pages[0].raw_content_hash == sha256(raw).hexdigest()
        request = ResearchRequest(
            instrument_id=INSTRUMENT,
            data_snapshot_id=snapshot_id,
            decision_time=decision,
        )
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="import-tool",
            request_hash="c" * 64,
            request=request,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=datetime.now(UTC),
        )
        tools = await database_read_only_tools(session, admitted.run, allow_fixture_evidence=False)
        view = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(snapshot_id),
                "as_of": decision.isoformat(),
            },
        )
        item = next(row for row in view["items"] if row["evidence_ref_id"] == str(evidence_id))
        action = item["corporate_action_observation"]
        assert action["events"][0]["rate"] == "0.123456789012345678901"
        assert action["events"][0]["currency"] is None
        assert action["source_completeness"] == "unknown"
        assert action["execution_locked"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["tampered_archive", "canonical_readback", "wrong_instrument"])
async def test_import_failure_leaves_no_partial_database_evidence(
    migrated_database_url,
    tmp_path,
    monkeypatch,
    invalid,
):
    from nexus.ops.alpaca_actions_import import import_collection

    settings = EngineSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:9000",
        object_store_region="us-east-1",
        object_store_bucket="synthetic-actions",
        object_store_prefix="synthetic-actions",
        object_store_access_key_id=SecretStr("synthetic"),
        object_store_secret_access_key=SecretStr("synthetic"),
    )
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        await seed_filing(session)
    directory = tmp_path / "collection"
    await collect_synthetic_archive(directory, monkeypatch)
    if invalid == "tampered_archive":
        (directory / "page-001.json").write_bytes(b"tampered original")

    class CorruptReadback(FakeS3Client):
        def get_object(self, **kwargs):
            result = super().get_object(**kwargs)
            if "evidence/" in kwargs["Key"]:
                result["Body"] = BytesIO(b"wrong canonical bytes")
            return result

    client = CorruptReadback() if invalid == "canonical_readback" else FakeS3Client()
    with pytest.raises(ValueError):
        await import_collection(
            settings,
            instrument_id=UUID(int=99) if invalid == "wrong_instrument" else INSTRUMENT,
            collection_dir=directory,
            store=S3ProviderEvidenceStore(settings, client=client),
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        assert (
            await session.scalar(
                select(func.count())
                .select_from(ResearchEvidenceRef)
                .where(
                    ResearchEvidenceRef.source == SOURCE,
                )
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(CorporateActionRecord)) == 0
    # Objects are immutable and intentionally not deleted on an uncertain DB outcome.
    assert len(client.objects) == (4 if invalid == "canonical_readback" else 0)
