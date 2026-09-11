from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.fosun_security_classification import (
    RAW_SCHEMA,
    FosunClassificationLevels,
    FosunFundAssetType,
    FosunRawQuoteBinding,
    FosunSecurityClassificationDocument,
)
from nexus.research.fosun_security_classification_storage import (
    persist_security_classification,
    read_security_classifications,
    save_classification_artifact,
)
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client

NOW = datetime(2026, 1, 2, tzinfo=UTC)
INSTRUMENT = UUID("00000000-0000-7000-8000-000000000011")
RAW = b'{"synthetic":"quote"}'
RAW_HASH = sha256(RAW).hexdigest()


def _settings(url: str) -> EngineSettings:
    return EngineSettings(database_url=url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic",
            "object_store_prefix": "classification",
            "object_store_access_key_id": SecretStr("synthetic"),
            "object_store_secret_access_key": SecretStr("synthetic"),
        }
    )


def _document(raw_uri: str, *, received_at: datetime = NOW) -> FosunSecurityClassificationDocument:
    return FosunSecurityClassificationDocument(
        instrument_id=INSTRUMENT,
        provider_code="usAAPL",
        provider_name="Apple",
        levels=FosunClassificationLevels(first=1, second=101, third=10105),
        equity_profile_fields_complete=True,
        net_asset_profile_fields_complete=False,
        fund_asset_type=FosunFundAssetType(chs="", cht="", eng="Stock"),
        routing_family="company_equity_like",
        received_at=received_at,
        available_at=received_at,
        raw_quote=FosunRawQuoteBinding(
            artifact_sha256=RAW_HASH,
            artifact_byte_length=len(RAW),
            artifact_uri=raw_uri,
            gateway_request_id=UUID("00000000-0000-7000-8000-000000000012"),
            request_body_sha256="b" * 64,
        ),
    )


@pytest.mark.asyncio
async def test_persist_replay_and_point_in_time_read(migrated_database_url: str) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
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
    document = _document(raw_uri)
    async with factory.begin() as session:
        location = await save_classification_artifact(session, store, document)
        first = await persist_security_classification(session, document, location=location)
        again = await persist_security_classification(session, document, location=location)
        assert again.id == first.id
        evidence_id = first.id
    async with factory() as session:
        assert (
            await read_security_classifications(
                session,
                instrument_id=INSTRUMENT,
                as_of=NOW - timedelta(microseconds=1),
            )
            == {}
        )
        result = await read_security_classifications(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
        )
        assert result == {evidence_id: document}
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 2
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_reader_rejects_tampered_raw_binding(migrated_database_url: str) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_uri = await store.archive_raw(f"raw/fosun/quote/{RAW_HASH}.json", RAW)
    async with factory.begin() as session:
        session.add(
            DataArtifact(
                uri=raw_uri, sha256=RAW_HASH, byte_size=len(RAW), mime_type="application/json",
                schema_version=RAW_SCHEMA, source="fosun", observed_at=NOW,
                available_at=NOW, received_at=NOW,
            )
        )
    document = _document(raw_uri)
    async with factory.begin() as session:
        location = await save_classification_artifact(session, store, document)
        evidence = await persist_security_classification(session, document, location=location)
        snapshot = await session.scalar(
            select(DataSnapshot).where(DataSnapshot.revision == evidence.revision)
        )
        assert snapshot is not None
        snapshot.payload = {
            **snapshot.payload,
            "raw_quote": {
                **snapshot.payload["raw_quote"],
                "artifact_uri": "s3://wrong",
            },
        }
    async with factory() as session:
        assert await read_security_classifications(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
        ) == {}
    await factory.kw["bind"].dispose()
