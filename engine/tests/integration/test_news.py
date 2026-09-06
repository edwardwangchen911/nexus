"""News collection, immutable storage and the authenticated route's read side."""

import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.local_recovery import write_private
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.research.news import import_news, read_news, validate_archive
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client

ID = UUID(int=17)


def archive(directory, *, symbol="EXAMPLE", empty=False):
    now = datetime.now(UTC) - timedelta(seconds=1)
    article = dict(
        id=17,
        headline="Synthetic announcement",
        summary="Plain <b>text</b>",
        author="Example",
        created_at=(now - timedelta(hours=2)).isoformat(),
        updated_at=(now - timedelta(hours=1)).isoformat(),
        url="https://example.com/story",
        symbols=[symbol],
        source="synthetic",
    )
    raw = json.dumps(dict(news=[] if empty else [article], next_page_token="more")).encode()
    receipt = dict(
        schema="nexus_news_receipt_v1",
        source="alpaca",
        symbol=symbol,
        start=(now - timedelta(days=1)).isoformat(),
        end=(now - timedelta(minutes=15)).isoformat(),
        received_at=now.isoformat(),
        raw_sha256=sha256(raw).hexdigest(),
        raw_byte_length=len(raw),
        has_more=True,
    )
    directory.mkdir(mode=0o700)
    write_private(directory / "raw.json", raw)
    write_private(directory / "receipt.json", json.dumps(receipt).encode())
    return raw, receipt


async def setup(url):
    settings = EngineSettings.for_test().model_copy(
        update={
            "database_url": url,
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-news",
            "object_store_region": "us-east-1",
            "object_store_access_key_id": SecretStr("synthetic-access"),
            "object_store_secret_access_key": SecretStr("synthetic-secret"),
        }
    )
    factory = session_factory(settings)
    async with factory.begin() as s:
        s.add(
            Instrument(
                id=ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                risk_bucket="EQUITY",
            )
        )
        await s.flush()
        s.add(
            InstrumentAlias(
                instrument_id=ID,
                symbol="EXAMPLE",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )
    client = FakeS3Client()
    return settings, factory, client, S3ProviderEvidenceStore(settings, client=client)


@pytest.mark.asyncio
async def test_import_read_api_idempotent_and_corruption(migrated_database_url, tmp_path):
    settings, factory, client, store = await setup(migrated_database_url)
    raw, receipt = archive(tmp_path / "news")
    async with factory.begin() as s:
        first = await import_news(s, store, instrument_id=ID, directory=tmp_path / "news")
    async with factory.begin() as s:
        assert first == await import_news(s, store, instrument_id=ID, directory=tmp_path / "news")
        assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        assert await s.scalar(select(func.count()).select_from(DataArtifact)) == 2
        assert (
            await read_news(
                s,
                store,
                instrument_id=ID,
                now=datetime.fromisoformat(receipt["received_at"]) - timedelta(seconds=1),
            )
        ).status == "unavailable"
    app = create_engine_app(settings)
    app.state.news_object_store = store
    app.dependency_overrides[require_service_actor] = lambda: VerifiedServiceActor(
        actor_id="test", request_id=new_uuid7(), key_id="test"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        response = await http.get(f"/internal/v1/instruments/{ID}/news")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "saved" and data["execution_locked"] is True
        assert data["articles"][0]["summary"] == "Plain <b>text</b>"
        assert data["received_at"] != data["articles"][0]["updated_at"]
        assert data["has_more"] is True and data["content_hash"] == sha256(raw).hexdigest()
        for key in client.objects:
            if client.objects[key] == raw:
                client.objects[key] = b"{}"
                break
        assert (await http.get(f"/internal/v1/instruments/{ID}/news")).json()["data"][
            "status"
        ] == "invalid"
    async with factory() as s:
        assert (
            await read_news(s, store, instrument_id=UUID(int=18), now=datetime.now(UTC))
        ).status == "unavailable"


@pytest.mark.asyncio
async def test_empty_and_wrong_identity(migrated_database_url, tmp_path):
    _, factory, _, store = await setup(migrated_database_url)
    archive(tmp_path / "empty", empty=True)
    async with factory.begin() as s:
        await import_news(s, store, instrument_id=ID, directory=tmp_path / "empty")
    async with factory() as s:
        assert (
            await read_news(s, store, instrument_id=ID, now=datetime.now(UTC))
        ).status == "empty"
    archive(tmp_path / "wrong", symbol="OTHER")
    async with factory.begin() as s:
        with pytest.raises(ValueError, match="NEWS_IDENTITY_INVALID"):
            await import_news(s, store, instrument_id=ID, directory=tmp_path / "wrong")
        assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1


def test_receipt_rejects_future_and_hash_mismatch(tmp_path):
    raw, receipt = archive(tmp_path / "news")
    receipt["raw_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        validate_archive(raw, json.dumps(receipt).encode(), now=datetime.now(UTC))
    receipt["raw_sha256"] = sha256(raw).hexdigest()
    with pytest.raises(ValueError):
        validate_archive(
            raw, json.dumps(receipt).encode(), now=datetime.now(UTC) - timedelta(days=2)
        )
