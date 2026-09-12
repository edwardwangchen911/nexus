"""Immutable persistence and current-pending reads for Fosun discovery evidence."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.fosun_instrument_discovery import (
    SCHEMA,
    SOURCE,
    FosunInstrumentDiscoveryDocument,
)
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.research.fosun_security_classification import RAW_SCHEMA


async def save_discovery_artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    document: FosunInstrumentDiscoveryDocument,
) -> str:
    """Archive a canonical discovery document and verify its database identity."""
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    key = f"evidence/fosun/instrument-discovery/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    if await store.read_evidence(key, uri=uri) != content:
        raise ValueError("FOSUN_DISCOVERY_OBJECT_READBACK_FAILED")
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source=SOURCE,
            schema_version=SCHEMA,
            observed_at=document.received_at,
            received_at=document.received_at,
            available_at=document.available_at,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or (
        artifact.uri,
        artifact.byte_size,
        artifact.mime_type,
        artifact.source,
        artifact.schema_version,
        artifact.observed_at,
        artifact.received_at,
        artifact.available_at,
    ) != (
        uri,
        len(content),
        "application/json",
        SOURCE,
        SCHEMA,
        document.received_at,
        document.received_at,
        document.available_at,
    ):
        raise ValueError("FOSUN_DISCOVERY_ARTIFACT_CONFLICT")
    return uri


async def _artifacts_valid(
    session: AsyncSession,
    document: FosunInstrumentDiscoveryDocument,
    *,
    as_of: datetime,
    location: str | None = None,
) -> bool:
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    artifacts = {
        item.sha256: item
        for item in await session.scalars(
            select(DataArtifact).where(
                DataArtifact.sha256.in_((digest, document.raw_quote.artifact_sha256))
            )
        )
    }
    canonical = artifacts.get(digest)
    raw = artifacts.get(document.raw_quote.artifact_sha256)
    return bool(
        canonical is not None
        and (location is None or canonical.uri == location)
        and canonical.source == SOURCE
        and canonical.schema_version == SCHEMA
        and canonical.byte_size == len(content)
        and canonical.mime_type == "application/json"
        and raw is not None
        and raw.uri == document.raw_quote.artifact_uri
        and raw.source == "fosun"
        and raw.schema_version == RAW_SCHEMA
        and raw.byte_size == document.raw_quote.artifact_byte_length
        and canonical.created_at <= as_of
        and raw.created_at <= as_of
        and max(canonical.observed_at, canonical.received_at, canonical.available_at)
        <= min(document.available_at, as_of)
        and max(raw.observed_at, raw.received_at, raw.available_at)
        <= min(document.available_at, as_of)
    )


async def persist_instrument_discovery(
    session: AsyncSession,
    document: FosunInstrumentDiscoveryDocument,
    *,
    location: str,
) -> DataSnapshot:
    """Insert or exactly reuse one provider-scoped discovery snapshot."""
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    now = datetime.now(UTC)
    if document.available_at > now or not await _artifacts_valid(
        session, document, location=location, as_of=now
    ):
        raise ValueError("FOSUN_DISCOVERY_ARTIFACT_INVALID")
    identity = {
        "source": SOURCE,
        "provider_object_id": document.provider_object_id,
        "revision": digest,
    }
    async with session.begin_nested():
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
            raise ValueError("FOSUN_DISCOVERY_SNAPSHOT_CONFLICT")
    return snapshot


async def _has_current_identity(
    session: AsyncSession,
    document: FosunInstrumentDiscoveryDocument,
    *,
    as_of: datetime,
) -> bool:
    symbol = document.provider_code[2:]
    identities = tuple(
        await session.scalars(
            select(Instrument.id)
            .join(InstrumentAlias, InstrumentAlias.instrument_id == Instrument.id)
            .where(
                InstrumentAlias.symbol == symbol,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= as_of.date()),
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
                Instrument.currency == "USD",
            )
            .limit(2)
        )
    )
    if len(identities) > 1:
        raise ValueError("FOSUN_DISCOVERY_ALIAS_AMBIGUOUS")
    return bool(identities)


async def read_pending_instrument_discoveries(
    session: AsyncSession,
    *,
    as_of: datetime,
) -> dict[UUID, FosunInstrumentDiscoveryDocument]:
    """Return latest valid unresolved discoveries, grouped by provider code."""
    as_of = require_utc(as_of)
    query = (
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            DataSnapshot.created_at <= as_of,
            DataSnapshot.observed_at <= as_of,
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
        )
        .order_by(
            DataSnapshot.provider_object_id,
            DataSnapshot.available_at.desc(),
            DataSnapshot.received_at.desc(),
            DataSnapshot.id,
        )
    )
    grouped: dict[str, list[tuple[DataSnapshot, FosunInstrumentDiscoveryDocument]]] = {}
    with session.no_autoflush:
        for snapshot in await session.scalars(query):
            try:
                document = FosunInstrumentDiscoveryDocument.model_validate(snapshot.payload)
                digest = sha256(document.canonical_bytes()).hexdigest()
                if (
                    document.provider_object_id == snapshot.provider_object_id
                    and digest == snapshot.revision
                    and document.received_at
                    == snapshot.observed_at
                    == snapshot.received_at
                    == snapshot.available_at
                    and await _artifacts_valid(session, document, as_of=as_of)
                ):
                    grouped.setdefault(document.provider_code, []).append((snapshot, document))
            except (TypeError, ValueError):
                continue

    pending: dict[UUID, FosunInstrumentDiscoveryDocument] = {}
    for rows in grouped.values():
        newest = max((item[1].available_at, item[1].received_at) for item in rows)
        latest = [
            item for item in rows if (item[1].available_at, item[1].received_at) == newest
        ]
        if len({item[1].canonical_bytes() for item in latest}) != 1:
            continue
        snapshot, document = min(latest, key=lambda item: str(item[0].id))
        if not await _has_current_identity(session, document, as_of=as_of):
            pending[snapshot.id] = document
    return pending
