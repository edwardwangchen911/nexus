"""Synthetic HTTP/S3 only; real archive, policies, persistence and exact reader."""

import importlib
import json
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.alpaca_prices_local import _resolve_identity, collect_prices
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.diagnostics import DiagnosticPolicy
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.alpaca_prices_archive import collection_window, load_price_collection
from nexus.research.current_price_trends import (
    PURPOSE_POLICY_ID,
    PURPOSE_POLICY_KEY,
    PURPOSE_POLICY_PAYLOAD,
    PURPOSE_POLICY_VERSION,
)
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client
from unit.research.test_current_price_trends import source_bars

INSTRUMENT = UUID("00000000-0000-4000-8000-000000000001")


async def setup_trends(url, directory, monkeypatch):
    directory.mkdir(mode=0o700)
    settings = EngineSettings(database_url=url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic-trends",
            "object_store_prefix": "synthetic-trends",
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
        # Until 0043 seeds this exact published purpose, this is only policy source fixture.
        if await session.get(DiagnosticPolicy, PURPOSE_POLICY_ID) is None:
            session.add(
                DiagnosticPolicy(
                    id=PURPOSE_POLICY_ID,
                    key=PURPOSE_POLICY_KEY,
                    version=PURPOSE_POLICY_VERSION,
                    schema_version="1",
                    payload=PURPOSE_POLICY_PAYLOAD,
                )
            )
    bars, sessions = source_bars()
    raw = json.dumps(
        {
            "symbol": "AAPL",
            "bars": [
                {
                    "t": bar.observed_at.isoformat(),
                    "o": str(bar.open),
                    "h": str(bar.high),
                    "l": str(bar.low),
                    "c": str(bar.close),
                    "v": str(bar.volume),
                }
                for bar in bars
            ],
            "next_page_token": None,
        }
    ).encode()
    request, calendar = collection_window(
        INSTRUMENT, sessions[0], sessions[-1], "split", now=datetime.now(UTC)
    )
    symbol = await _resolve_identity(url, request, request.end.date())
    original_client = httpx.AsyncClient
    with monkeypatch.context() as patch:
        patch.setattr(
            httpx,
            "AsyncClient",
            lambda **kwargs: original_client(
                **kwargs, transport=httpx.MockTransport(lambda _: httpx.Response(200, content=raw))
            ),
        )
        result = await collect_prices(
            request,
            symbol=symbol,
            calendar=calendar,
            resolved_at=datetime.now(UTC),
            key_id=SecretStr("synthetic-key"),
            secret=SecretStr("synthetic-secret"),
            output_dir=directory,
            max_pages=1,
        )
    assert result["status"] == "collected"
    client = FakeS3Client()
    archive = load_price_collection(directory, instrument_id=INSTRUMENT)
    return settings, factory, S3ProviderEvidenceStore(settings, client=client), client, archive


@pytest.mark.asyncio
async def test_normal_archive_persists_and_exactly_reads_without_price_authority(
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    name = "nexus.research.current_price_trends_storage"
    assert importlib.util.find_spec(name) is not None, "normal trend storage consumer missing"
    storage = importlib.import_module(name)
    _, factory, store, client, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    before = datetime.now(UTC)
    async with factory.begin() as session:
        evidence = await storage.persist_current_price_trends(session, archive, store=store)
        evidence_id = evidence.id
    async with factory.begin() as session:
        repeated = await storage.persist_current_price_trends(session, archive, store=store)
        assert repeated.id == evidence_id
    async with factory() as session:
        documents = await storage.read_current_price_trends(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC), evidence_ids=(evidence_id,)
        )
        document = documents[evidence_id]
        assert document.features["sma_200"] == "100.500000000000"
        assert document.observed_sessions == document.contiguous_tail_sessions == 200
        assert not document.market_price_eligible and not document.historical_backtest_eligible
        assert not document.total_return_eligible and document.execution_locked
        assert document.materialized_at >= before
        assert len(client.objects) == 4
        for original in archive.originals:
            assert original.content in client.objects.values()
        assert (
            await storage.read_current_price_trends(
                session,
                instrument_id=UUID(int=99),
                as_of=datetime.now(UTC),
                evidence_ids=(evidence_id,),
            )
            == {}
        )
        assert (
            await storage.read_current_price_trends(
                session,
                instrument_id=INSTRUMENT,
                as_of=document.available_at - timedelta(microseconds=1),
                evidence_ids=(evidence_id,),
            )
            == {}
        )
        assert (
            await storage.read_current_price_trends(
                session,
                instrument_id=INSTRUMENT,
                as_of=datetime.now(UTC),
                evidence_ids=(UUID(int=99),),
            )
            == {}
        )
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 4
