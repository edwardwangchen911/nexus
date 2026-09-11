import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local_refresh import _persist_quote_classifications
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.providers.market.fosun_gateway import (
    FosunQuoteItem,
    FosunSecurityClassification,
    RawGatewayResponse,
)
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

NOW = datetime.now(UTC) - timedelta(minutes=10)
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
async def test_research_snapshot_freezes_classification_with_sec_base(
    migrated_database_url: str,
) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_uri = await store.archive_raw(f"raw/fosun/quote/{RAW_HASH}.json", RAW)
    sec_id = UUID("00000000-0000-7000-8000-000000000021")
    async with factory.begin() as session:
        session.add_all(
            [
                Instrument(
                    id=INSTRUMENT,
                    asset_type="common_stock",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=None,
                    risk_bucket="EQUITY",
                ),
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
                ),
                ResearchEvidenceRef(
                    id=sec_id,
                    source="sec",
                    provider_object_id="0000000001-25-000001",
                    revision="v1",
                    available_at=NOW,
                    location=f"memory://sha256/{'c' * 64}",
                    content_hash="c" * 64,
                    byte_length=10,
                ),
            ]
        )
        await session.flush()
        session.add(
            Filing(
                instrument_id=INSTRUMENT,
                evidence_ref_id=sec_id,
                source="sec",
                provider_object_id="0000000001-25-000001",
                accession_number="0000000001-25-000001",
                form="10-Q",
                filed_at=NOW,
                accepted_at=NOW,
                revision="v1",
            )
        )
        document = _document(raw_uri)
        location = await save_classification_artifact(session, store, document)
        classification = await persist_security_classification(
            session, document, location=location
        )
        classification_id = classification.id
    async with factory.begin() as session:
        decision_time = datetime.now(UTC)
        snapshot = await prepare_research_snapshot(
            session, INSTRUMENT, as_of=decision_time
        )
        assert snapshot is not None
        assert str(classification_id) in snapshot.payload["evidence_ids"]
        frozen = set(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id
                )
            )
        )
        assert frozen == {sec_id, classification_id}
        refs = {
            item.id: item
            for item in await session.scalars(
                select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(frozen))
            )
        }
        manifest = [
            {
                "id": str(refs[UUID(item)].id),
                "source": refs[UUID(item)].source,
                "provider_object_id": refs[UUID(item)].provider_object_id,
                "revision": refs[UUID(item)].revision,
                "available_at": refs[UUID(item)].available_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "location": refs[UUID(item)].location,
                "content_hash": refs[UUID(item)].content_hash,
            }
            for item in snapshot.payload["evidence_ids"]
        ]
        evidence_hash = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        prompt = AgentPromptVersionRecord(
            role="orchestrator",
            version="classification-test-v1",
            content_hash=hashlib.sha256(b"test").hexdigest(),
            content="test",
        )
        model = AgentModelConfigRecord(
            model_id="fixture-model",
            version="classification-test-v1",
            config_hash="d" * 64,
            max_input_tokens=100,
            max_tool_calls=1,
            max_output_tokens=100,
            input_usd_per_million=Decimal("0"),
            output_usd_per_million=Decimal("0"),
            pricing_available_at=NOW,
            pricing_expires_at=decision_time + timedelta(days=1),
        )
        session.add_all((prompt, model))
        await session.flush()
        run = AgentResearchRunRecord(
            instrument_id=INSTRUMENT,
            data_snapshot_id=snapshot.id,
            prompt_version_id=prompt.id,
            model_config_id=model.id,
            decision_time=decision_time,
            actor_id="classification-test",
            idempotency_key="classification-test",
            status="prepared",
            retry_count=0,
            schema_hash="1" * 64,
            model_hash="2" * 64,
            prompt_hash="3" * 64,
            prompt_manifest={},
            evidence_hash=evidence_hash,
            evidence_manifest=manifest,
            tool_hash="4" * 64,
            input_hash="5" * 64,
            reserved_cost_usd=Decimal("0"),
            settled_cost_usd=None,
            pricing_version="classification-test-v1",
            input_tokens=None,
            output_tokens=None,
        )
        session.add(run)
        await session.flush()
        tools = await database_read_only_tools(
            session, run, allow_fixture_evidence=False
        )
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(snapshot.id),
                "as_of": decision_time,
            },
        )
        item = next(
            row for row in result["items"] if row["evidence_ref_id"] == str(classification_id)
        )
        assert item["provider_security_classification"] == document.tool_payload()
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


@pytest.mark.asyncio
async def test_full_refresh_producer_uses_already_read_quote(
    migrated_database_url: str,
) -> None:
    settings = _settings(migrated_database_url)
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    raw_response = RawGatewayResponse(
        operation="fosun_get_quotes",
        gateway_request_id=UUID("00000000-0000-7000-8000-000000000012"),
        request_body_sha256="b" * 64,
        raw_payload=RAW,
        sent_at=NOW - timedelta(seconds=1),
        received_at=NOW,
    )
    raw_uri = await store.archive_raw(f"raw/fosun/quote/{RAW_HASH}.json", RAW)
    async with factory.begin() as session:
        artifact = DataArtifact(
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
        session.add(artifact)
        await session.flush()
        await _persist_quote_classifications(
            session,
            store,
            quote_items=(
                FosunQuoteItem(
                    kind="quote",
                    instrument_code="usAAPL",
                    price="100",
                    as_of=NOW,
                    provider_name="Apple",
                    provider_classification=FosunSecurityClassification(
                        level_1=1,
                        level_2=101,
                        level_3=10105,
                        equity_profile_fields_complete=True,
                        net_asset_profile_fields_complete=False,
                    ),
                ),
            ),
            instrument_ids={"usAAPL": INSTRUMENT},
            quote_raw=raw_response,
            quote_artifact=artifact,
        )
    async with factory() as session:
        records = await read_security_classifications(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
        )
        assert len(records) == 1
        assert next(iter(records.values())).routing_family == "company_equity_like"
    await factory.kw["bind"].dispose()
