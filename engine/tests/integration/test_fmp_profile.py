"""Only HTTP/S3 transport is synthetic; profile import uses real PG and canonical reader."""

import importlib
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fmp_profile_local import collect_profile
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client

INSTRUMENT = UUID(int=17)


async def setup_profile(url, directory):
    settings = EngineSettings(database_url=url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic-profile",
            "object_store_prefix": "synthetic-profile",
            "object_store_access_key_id": SecretStr("synthetic-access"),
            "object_store_secret_access_key": SecretStr("synthetic-secret"),
        }
    )
    factory = session_factory(settings)
    async with factory.begin() as session:
        session.add(
            Instrument(
                id=INSTRUMENT,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=None,
                risk_bucket="EQUITY",
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
    raw = b'[{"symbol":"AAPL","companyName":"Synthetic","industry":"Vendor Industry","price":1.25}]'
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=raw),
        )
    ) as client:
        await collect_profile(
            symbol="AAPL", key=SecretStr("synthetic-secret"), output_dir=directory, client=client
        )
    client = FakeS3Client()
    return settings, factory, S3ProviderEvidenceStore(settings, client=client), client, raw


@pytest.mark.asyncio
async def test_normal_profile_import_replays_exact_evidence_without_financial_authority(
    migrated_database_url,
    tmp_path,
):
    entry = importlib.import_module("nexus.ops.fmp_profile_local")
    directory = tmp_path / "profile"
    settings, factory, store, client, raw = await setup_profile(migrated_database_url, directory)
    before = datetime.now(UTC)
    first = await entry.import_profile(
        settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
    )
    second = await entry.import_profile(
        settings, instrument_id=INSTRUMENT, symbol="AAPL", collection_dir=directory, store=store
    )
    assert first["status"] == "stored_context" and first["evidence_id"] == second["evidence_id"]
    assert first["execution_locked"] and not first["model_called"]
    assert len(client.objects) == 3 and raw in client.objects.values()
    reader = importlib.import_module("nexus.research.fmp_profile_storage").read_company_profiles
    evidence_id = UUID(first["evidence_id"])
    async with factory() as session:
        docs = await reader(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC), evidence_ids=(evidence_id,)
        )
        assert docs[evidence_id].industry == "Vendor Industry"
        assert not docs[evidence_id].financial_fact_eligible
        assert not docs[evidence_id].sector_risk_eligible
        assert (
            await reader(
                session,
                instrument_id=UUID(int=999),
                as_of=datetime.now(UTC),
                evidence_ids=(evidence_id,),
            )
            == {}
        )
        assert (
            await reader(
                session,
                instrument_id=INSTRUMENT,
                as_of=docs[evidence_id].received_at - timedelta(microseconds=1),
            )
            == {}
        )
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 3
        snapshot = await session.scalar(select(DataSnapshot))
        assert snapshot.created_at >= before and snapshot.source == "fmp_company_profile"
