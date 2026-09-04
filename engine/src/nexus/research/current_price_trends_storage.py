"""Receipt-bound supplementary trends; never writes the market-price authority tables."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.calendar_policy import load_xnys_calendar_adoption
from nexus.domain.time import require_utc
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.diagnostics import DiagnosticPolicy
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.alpaca_prices_archive import ValidatedPriceArchive, validate_price_originals
from nexus.research.current_price_trends import (
    PURPOSE_POLICY_ID,
    SCHEMA,
    SOURCE,
    CurrentPriceTrend,
    build_current_price_trend,
    purpose_policy_input,
)


def _object_id(archive: ValidatedPriceArchive) -> str:
    return f"collection:{archive.request.instrument_id}:{archive.manifest_content_hash}"


def _original_schema(name: str) -> str:
    if name == "manifest.json":
        return "alpaca_trends_manifest_v1"
    return "alpaca_trends_receipt_v1" if ".receipt." in name else "alpaca_trends_raw_v1"


async def _identity(session: AsyncSession, archive: ValidatedPriceArchive) -> None:
    import json

    resolved_at = require_utc(
        datetime.fromisoformat(json.loads(archive.originals[-1].content)["identity_resolved_at"])
    )
    instrument = await session.get(Instrument, archive.request.instrument_id)
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= archive.identity_asof,
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= archive.identity_asof,
                ),
                InstrumentAlias.created_at <= resolved_at,
                or_(
                    InstrumentAlias.instrument_id == archive.request.instrument_id,
                    InstrumentAlias.symbol == archive.symbol,
                ),
            )
        )
    )
    if (
        instrument is None
        or instrument.created_at > resolved_at
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or instrument.currency != "USD"
        or instrument.asset_type not in {"common_stock", "equity", "equity_etf"}
        or len(aliases) != 1
        or aliases[0].instrument_id != archive.request.instrument_id
        or aliases[0].symbol != archive.symbol
    ):
        raise ValueError("CURRENT_TRENDS_IDENTITY_INVALID")


async def _build(
    session: AsyncSession,
    archive: ValidatedPriceArchive,
    materialized_at: datetime,
) -> CurrentPriceTrend:
    await _identity(session, archive)
    calendar = await load_xnys_calendar_adoption(session, materialized_at)
    purpose = purpose_policy_input(
        await session.get(DiagnosticPolicy, PURPOSE_POLICY_ID), materialized_at
    )
    return await build_current_price_trend(
        archive, calendar=calendar, purpose_policy=purpose, materialized_at=materialized_at
    )


async def _artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    content: bytes,
    schema: str,
    received_at: datetime,
) -> str:
    digest = sha256(content).hexdigest()
    canonical = schema == SCHEMA
    key = f"{'evidence' if canonical else 'raw'}/alpaca-current-trends/sha256/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    saved = await (store.read_evidence(key, uri=uri) if canonical else store.read_raw(key, uri=uri))
    if saved != content:
        raise ValueError("CURRENT_TRENDS_OBJECT_READBACK_FAILED")
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            schema_version=schema,
            source=SOURCE,
            observed_at=received_at,
            received_at=received_at,
            available_at=received_at,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    row = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if row is None or (
        row.uri,
        row.source,
        row.schema_version,
        row.byte_size,
        row.observed_at,
        row.received_at,
        row.available_at,
    ) != (uri, SOURCE, schema, len(content), received_at, received_at, received_at):
        raise ValueError("CURRENT_TRENDS_ARTIFACT_CONFLICT")
    return uri


async def _artifacts_valid(
    session: AsyncSession,
    document: CurrentPriceTrend,
    as_of: datetime,
    location: str,
) -> bool:
    content = document.canonical_bytes()
    expected = [(sha256(content).hexdigest(), len(content), SCHEMA, document.materialized_at)]
    expected.extend(
        (item.content_hash, item.byte_length, _original_schema(item.name), item.received_at)
        for item in document.originals
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
        (row := rows.get(digest)) is not None
        and row.created_at <= as_of
        and (
            row.source,
            row.schema_version,
            row.byte_size,
            row.observed_at,
            row.received_at,
            row.available_at,
        )
        == (SOURCE, schema, size, received, received, received)
        and received <= min(as_of, document.available_at)
        and (schema != SCHEMA or row.uri == location)
        for digest, size, schema, received in expected
    )


async def _read_one(
    session: AsyncSession,
    snapshot: DataSnapshot,
    evidence: ResearchEvidenceRef,
    *,
    instrument_id: UUID,
    as_of: datetime,
) -> CurrentPriceTrend:
    document = CurrentPriceTrend.model_validate(snapshot.payload)
    if document.instrument_id != instrument_id or document.available_at > as_of:
        raise ValueError("CURRENT_TRENDS_BINDING_INVALID")
    originals = {item.name: item.raw_text.encode("utf-8") for item in document.originals}
    if len(originals) != len(document.originals):
        raise ValueError("CURRENT_TRENDS_ORIGINAL_INVALID")
    archive = validate_price_originals(
        originals, instrument_id=instrument_id, now=document.materialized_at
    )
    rebuilt = await _build(session, archive, document.materialized_at)
    content = document.canonical_bytes()
    if (
        content != rebuilt.canonical_bytes()
        or not (snapshot.source == evidence.source == SOURCE and snapshot.schema_version == SCHEMA)
        or not (snapshot.provider_object_id == evidence.provider_object_id == _object_id(archive))
        or not (
            snapshot.revision
            == evidence.revision
            == evidence.content_hash
            == sha256(content).hexdigest()
        )
        or evidence.byte_length != len(content)
        or not (snapshot.observed_at == snapshot.received_at == document.received_at)
        or not (snapshot.available_at == evidence.available_at == document.available_at)
        or max(snapshot.created_at, evidence.created_at) > as_of
        or not await _artifacts_valid(session, document, as_of, evidence.location)
    ):
        raise ValueError("CURRENT_TRENDS_BINDING_INVALID")
    return document


async def read_current_price_trends(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, CurrentPriceTrend]:
    """Select at most one visible supplement; an exact id never falls back to latest."""
    cutoff = require_utc(as_of)
    if evidence_ids is not None and len(evidence_ids) != 1:
        return {}
    query = (
        select(DataSnapshot, ResearchEvidenceRef)
        .join(
            ResearchEvidenceRef,
            (ResearchEvidenceRef.source == DataSnapshot.source)
            & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
            & (ResearchEvidenceRef.revision == DataSnapshot.revision),
        )
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            DataSnapshot.payload["instrument_id"].astext == str(instrument_id),
            DataSnapshot.created_at <= cutoff,
            DataSnapshot.received_at <= cutoff,
            DataSnapshot.available_at <= cutoff,
            ResearchEvidenceRef.created_at <= cutoff,
            ResearchEvidenceRef.available_at <= cutoff,
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
                result[evidence.id] = await _read_one(
                    session, snapshot, evidence, instrument_id=instrument_id, as_of=cutoff
                )
            except (ValueError, TypeError, KeyError, OverflowError):
                continue
    return result


async def persist_current_price_trends(
    session: AsyncSession,
    archive: ValidatedPriceArchive,
    *,
    store: S3ProviderEvidenceStore,
) -> ResearchEvidenceRef:
    """Caller commits; reimports retain the first actual materialization and exact refs."""
    now = datetime.now(UTC)
    archive = validate_price_originals(
        {item.name: item.content for item in archive.originals},
        instrument_id=archive.request.instrument_id,
        now=now,
    )
    provider_object_id = _object_id(archive)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
        {"identity": f"{SOURCE}:{provider_object_id}"},
    )
    existing = tuple(
        await session.scalars(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.provider_object_id == provider_object_id,
            )
        )
    )
    if existing:
        if len(existing) != 1:
            raise ValueError("CURRENT_TRENDS_EXISTING_INVALID")
        documents = await read_current_price_trends(
            session,
            instrument_id=archive.request.instrument_id,
            as_of=datetime.now(UTC),
            evidence_ids=(existing[0].id,),
        )
        if existing[0].id not in documents:
            raise ValueError("CURRENT_TRENDS_EXISTING_INVALID")
        return existing[0]
    orphan = await session.scalar(
        select(DataSnapshot.id).where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == provider_object_id,
        )
    )
    if orphan is not None:
        raise ValueError("CURRENT_TRENDS_EXISTING_INVALID")
    document = await _build(session, archive, datetime.now(UTC))
    for item in archive.originals:
        await _artifact(
            session,
            store,
            content=item.content,
            schema=_original_schema(item.name),
            received_at=item.received_at,
        )
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    location = await _artifact(
        session, store, content=content, schema=SCHEMA, received_at=document.materialized_at
    )
    identity = dict(source=SOURCE, provider_object_id=provider_object_id, revision=digest)
    evidence = ResearchEvidenceRef(
        **identity,
        available_at=document.available_at,
        location=location,
        content_hash=digest,
        byte_length=len(content),
        created_at=func.clock_timestamp(),
    )
    snapshot = DataSnapshot(
        **identity,
        observed_at=document.received_at,
        received_at=document.received_at,
        available_at=document.available_at,
        schema_version=SCHEMA,
        payload=document.model_dump(mode="json"),
        created_at=func.clock_timestamp(),
    )
    session.add_all((evidence, snapshot))
    await session.flush()
    await _read_one(
        session,
        snapshot,
        evidence,
        instrument_id=archive.request.instrument_id,
        as_of=datetime.now(UTC),
    )
    return evidence
