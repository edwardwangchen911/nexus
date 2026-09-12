from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.market.fosun_instrument_discovery import FosunInstrumentDiscoveryDocument
from nexus.market.fosun_instrument_discovery_storage import (
    persist_instrument_discovery,
    read_pending_instrument_discoveries,
    save_discovery_artifact,
)
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.fosun_security_classification import (
    RAW_SCHEMA,
    FosunClassificationLevels,
    FosunFundAssetType,
    FosunRawQuoteBinding,
)
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client

NOW = datetime.now(UTC) - timedelta(minutes=10)
RAW = b'{"synthetic":"unresolved-quote"}'
RAW_HASH = sha256(RAW).hexdigest()


def _settings(url: str) -> EngineSettings:
    return EngineSettings(database_url=url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic",
            "object_store_prefix": "discovery",
            "object_store_access_key_id": SecretStr("synthetic"),
            "object_store_secret_access_key": SecretStr("synthetic"),
        }
    )


def _document(raw_uri: str, **changes: object) -> FosunInstrumentDiscoveryDocument:
    values: dict[str, object] = {
        "provider_code": "usNEW",
        "provider_name": "New Company",
        "market_scope": "US",
        "currency": "USD",
        "provider_product_type": 6,
        "levels": FosunClassificationLevels(first=1, second=101, third=10105),
        "equity_profile_fields_complete": True,
        "net_asset_profile_fields_complete": False,
        "fund_asset_type": FosunFundAssetType(chs="", cht="", eng=""),
        "routing_family": "company_equity_like",
        "recommended_identity_route": "current_held_common_stock",
        "holding_observed_at": NOW - timedelta(seconds=1),
        "received_at": NOW,
        "available_at": NOW,
        "raw_quote": FosunRawQuoteBinding(
            artifact_sha256=RAW_HASH,
            artifact_byte_length=len(RAW),
            artifact_uri=raw_uri,
            gateway_request_id=UUID("00000000-0000-7000-8000-000000000032"),
            request_body_sha256="b" * 64,
        ),
    }
    values.update(changes)
    return FosunInstrumentDiscoveryDocument.model_validate(values)


async def _seed_raw(factory, store: S3ProviderEvidenceStore) -> str:
    raw_uri = await store.archive_raw(f"raw/fosun/quote/{RAW_HASH}.json", RAW)
    async with factory.begin() as session:
        session.add(
            DataArtifact(
                uri=raw_uri,
                sha256=RAW_HASH,
                byte_size=len(RAW),
                mime_type="application/json",
                schema_version=RAW_SCHEMA,
                source="fosun",
                observed_at=NOW,
                available_at=NOW,
                received_at=NOW,
            )
        )
    return raw_uri


@pytest.mark.asyncio
async def test_persist_replay_pending_and_resolution_read(
    migrated_database_url: str,
) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_uri = await _seed_raw(factory, store)
    document = _document(raw_uri)

    async with factory.begin() as session:
        location = await save_discovery_artifact(session, store, document)
        first = await persist_instrument_discovery(session, document, location=location)
        replay = await persist_instrument_discovery(session, document, location=location)
        assert replay.id == first.id
        snapshot_id = first.id

    async with factory() as session:
        assert (
            await read_pending_instrument_discoveries(
                session, as_of=NOW - timedelta(microseconds=1)
            )
            == {}
        )
        pending = await read_pending_instrument_discoveries(
            session, as_of=datetime.now(UTC)
        )
        assert pending == {snapshot_id: document}
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 2
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 0

    instrument_id = UUID("00000000-0000-7000-8000-000000000033")
    async with factory.begin() as session:
        session.add_all(
            [
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=None,
                    risk_bucket="EQUITY",
                ),
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol="NEW",
                    alias_type="ticker",
                    valid_from=NOW.date(),
                    valid_to=None,
                ),
            ]
        )
    async with factory() as session:
        assert (
            await read_pending_instrument_discoveries(
                session, as_of=datetime.now(UTC)
            )
            == {}
        )
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_reader_rejects_tampered_raw_binding(migrated_database_url: str) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_uri = await _seed_raw(factory, store)
    document = _document(raw_uri)
    async with factory.begin() as session:
        location = await save_discovery_artifact(session, store, document)
        snapshot = await persist_instrument_discovery(session, document, location=location)
        snapshot.payload = {
            **snapshot.payload,
            "raw_quote": {
                **snapshot.payload["raw_quote"],
                "artifact_uri": "s3://wrong",
            },
        }
    async with factory() as session:
        assert (
            await read_pending_instrument_discoveries(
                session, as_of=datetime.now(UTC)
            )
            == {}
        )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_same_clock_conflict_returns_no_pending_identity(
    migrated_database_url: str,
) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_uri = await _seed_raw(factory, store)
    first = _document(raw_uri)
    second = _document(raw_uri, provider_name="Conflicting Name")
    async with factory.begin() as session:
        for document in (first, second):
            location = await save_discovery_artifact(session, store, document)
            await persist_instrument_discovery(session, document, location=location)
    async with factory() as session:
        assert (
            await read_pending_instrument_discoveries(
                session, as_of=datetime.now(UTC)
            )
            == {}
        )
    await factory.kw["bind"].dispose()
