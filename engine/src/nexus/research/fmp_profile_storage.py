"""Receipt-bound FMP background using existing immutable evidence identities."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.fmp_profile import RAW_SCHEMA, RECEIPT_SCHEMA, SCHEMA, SOURCE, FmpCompanyProfile


async def require_profile_identity(session: AsyncSession, document: FmpCompanyProfile) -> None:
    instrument = await session.get(Instrument, document.instrument_id)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.symbol == document.symbol,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= document.received_at.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= document.received_at.date(),
                ),
                InstrumentAlias.created_at <= document.received_at,
            )
        )
    )
    if (
        instrument is None
        or instrument.created_at > document.received_at
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or instrument.asset_type not in {"equity", "common_stock", "equity_etf"}
        or len(aliases) != 1
        or aliases[0].instrument_id != document.instrument_id
    ):
        raise ValueError("FMP_PROFILE_IDENTITY_INVALID")


async def save_profile_artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    content: bytes,
    schema: str,
    received_at: datetime,
) -> str:
    digest = sha256(content).hexdigest()
    canonical = schema == SCHEMA
    key = f"{'evidence' if canonical else 'raw'}/fmp/profile/{schema}/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    saved = (
        await store.read_evidence(key, uri=uri) if canonical else await store.read_raw(key, uri=uri)
    )
    if saved != content:
        raise ValueError("FMP_PROFILE_OBJECT_READBACK_FAILED")
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source="fmp",
            schema_version=schema,
            received_at=received_at,
            observed_at=received_at,
            available_at=received_at,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or (
        artifact.uri != uri
        or artifact.source != "fmp"
        or artifact.schema_version != schema
        or artifact.byte_size != len(content)
        or artifact.received_at > received_at
        or artifact.available_at > received_at
        or artifact.observed_at > received_at
    ):
        raise ValueError("FMP_PROFILE_ARTIFACT_CONFLICT")
    return uri


async def _artifacts_valid(
    session: AsyncSession,
    document: FmpCompanyProfile,
    *,
    as_of: datetime,
    location: str,
) -> bool:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    expected = (
        (document.raw_content_hash, document.raw_byte_length, RAW_SCHEMA),
        (document.receipt_content_hash, document.receipt_byte_length, RECEIPT_SCHEMA),
        (digest, len(content), SCHEMA),
    )
    rows = {
        row.sha256: row
        for row in await session.scalars(
            select(DataArtifact).where(
                DataArtifact.sha256.in_([item[0] for item in expected]),
            )
        )
    }
    return all(
        (row := rows.get(checksum)) is not None
        and row.source == "fmp"
        and row.schema_version == schema
        and row.byte_size == size
        and row.created_at <= as_of
        and max(row.observed_at, row.received_at, row.available_at)
        <= min(document.available_at, as_of)
        and (checksum != digest or row.uri == location)
        for checksum, size, schema in expected
    )


async def persist_company_profile(
    session: AsyncSession,
    document: FmpCompanyProfile,
    *,
    location: str,
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    now = datetime.now(UTC)
    await require_profile_identity(session, document)
    if document.available_at > now or not await _artifacts_valid(
        session,
        document,
        as_of=now,
        location=location,
    ):
        raise ValueError("FMP_PROFILE_ARTIFACT_INVALID")
    identity = dict(source=SOURCE, provider_object_id=document.provider_object_id, revision=digest)
    async with session.begin_nested():
        await session.execute(
            insert(ResearchEvidenceRef)
            .values(
                **identity,
                available_at=document.available_at,
                location=location,
                content_hash=digest,
                byte_length=len(content),
                created_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        )
        evidence = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.provider_object_id == document.provider_object_id,
                ResearchEvidenceRef.revision == digest,
            )
        )
        if evidence is None or (
            evidence.content_hash,
            evidence.byte_length,
            evidence.available_at,
            evidence.location,
        ) != (digest, len(content), document.available_at, location):
            raise ValueError("FMP_PROFILE_EVIDENCE_CONFLICT")
        await session.execute(
            insert(DataSnapshot)
            .values(
                **identity,
                observed_at=document.received_at,
                received_at=document.received_at,
                available_at=document.available_at,
                schema_version=SCHEMA,
                payload=document.model_dump(mode="json"),
                created_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
        )
        snapshot = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == document.provider_object_id,
                DataSnapshot.revision == digest,
            )
        )
        if snapshot is None or (
            snapshot.schema_version,
            snapshot.observed_at,
            snapshot.received_at,
            snapshot.available_at,
            snapshot.payload,
        ) != (
            SCHEMA,
            document.received_at,
            document.received_at,
            document.available_at,
            document.model_dump(mode="json"),
        ):
            raise ValueError("FMP_PROFILE_SNAPSHOT_CONFLICT")
    return evidence


async def read_company_profiles(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, FmpCompanyProfile]:
    as_of = require_utc(as_of)
    query = (
        select(DataSnapshot, ResearchEvidenceRef)
        .join(
            ResearchEvidenceRef,
            (
                (ResearchEvidenceRef.source == DataSnapshot.source)
                & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
                & (ResearchEvidenceRef.revision == DataSnapshot.revision)
            ),
        )
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            DataSnapshot.payload["instrument_id"].astext == str(instrument_id),
            DataSnapshot.created_at <= as_of,
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            ResearchEvidenceRef.available_at <= as_of,
        )
        .order_by(DataSnapshot.available_at.desc(), DataSnapshot.id)
        .limit(1)
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    result = {}
    with session.no_autoflush:
        for snapshot, evidence in await session.execute(query):
            try:
                document = FmpCompanyProfile.model_validate(snapshot.payload)
                content = document.canonical_bytes()
                if (
                    document.instrument_id == instrument_id
                    and document.provider_object_id
                    == snapshot.provider_object_id
                    == evidence.provider_object_id
                    and sha256(content).hexdigest()
                    == snapshot.revision
                    == evidence.revision
                    == evidence.content_hash
                    and len(content) == evidence.byte_length
                    and document.available_at
                    == snapshot.observed_at
                    == snapshot.received_at
                    == snapshot.available_at
                    == evidence.available_at
                    and await _artifacts_valid(
                        session, document, as_of=as_of, location=evidence.location
                    )
                ):
                    result[evidence.id] = document
            except (ValueError, TypeError):
                continue
    return result
