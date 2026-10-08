"""Append-only current research identity references, scoped by frozen decision day."""

import json
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.current_identity_storage import (
    ArtifactBinding,
    CurrentIdentityProof,
    CustomMemberIdentityProof,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.current_identity_archive import (
    POLICY_HASH,
    load_identity_source_archive,
    validate_identity_source_originals,
)
from nexus.research.sec_exhibit_storage import (
    ExhibitStore,
    _read_identity,
    configured_exhibit_store,
)

SOURCE = "research_current_identity"
CAPACITY_REASON = "research_identity_evidence_capacity_exhausted"
SCHEMA: Literal["research_identity_binding_v1"] = "research_identity_binding_v1"


class ResearchIdentityBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    schema_version: Literal["research_identity_binding_v1"] = SCHEMA
    policy_id: Literal["research_current_identity_session_v1"] = (
        "research_current_identity_session_v1"
    )
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_id: UUID
    alias_id: UUID
    symbol: str
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    exchange: str
    decision_session: date
    available_at: AwareDatetime
    identity_snapshot_id: UUID
    identity_source: str
    identity_revision: str
    identity_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    identity_artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    source_artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    source_received_at: tuple[AwareDatetime, AwareDatetime]
    purpose: Literal["research_issuer_security_binding_only"] = (
        "research_issuer_security_binding_only"
    )
    historical_backtest_eligible: Literal[False] = False
    price_authority_eligible: Literal[False] = False
    risk_authority_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def _binding(artifact: DataArtifact) -> ArtifactBinding:
    return ArtifactBinding(
        uri=artifact.uri,
        sha256=artifact.sha256,
        byte_size=artifact.byte_size,
        source=artifact.source,
        schema_version=artifact.schema_version,
    )


def _payload_hash(snapshot: DataSnapshot) -> str:
    return sha256(
        json.dumps(snapshot.payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def _clock(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("RESEARCH_IDENTITY_CLOCK_INVALID")
    return now


async def _base(
    session: AsyncSession, store: ExhibitStore, identifier: UUID, as_of: datetime
) -> tuple[DataSnapshot, CurrentIdentityProof]:
    snapshot = await session.get(DataSnapshot, identifier)
    if (
        snapshot is None
        or max(
            snapshot.created_at, snapshot.available_at, snapshot.received_at, snapshot.observed_at
        )
        > as_of
    ):
        raise ValueError("RESEARCH_IDENTITY_BASE_INVALID")
    proof = await _read_identity(session, store, snapshot_id=identifier, as_of=as_of)
    if proof.checked_at > as_of:
        raise ValueError("RESEARCH_IDENTITY_BASE_INVALID")
    bindings = proof.artifacts + (
        proof.taxonomy_artifacts if isinstance(proof, CustomMemberIdentityProof) else ()
    )
    artifacts = {
        artifact.sha256: artifact
        for artifact in await session.scalars(
            select(DataArtifact).where(
                DataArtifact.sha256.in_(binding.sha256 for binding in bindings)
            )
        )
    }
    for binding in bindings:
        artifact = artifacts.get(binding.sha256)
        if (
            artifact is None
            or _binding(artifact) != binding
            or max(
                artifact.created_at,
                artifact.available_at,
                artifact.received_at,
                artifact.observed_at,
            )
            > as_of
        ):
            raise ValueError("RESEARCH_IDENTITY_ARTIFACT_INVALID")
    return snapshot, proof


async def _read_artifact(
    session: AsyncSession,
    store: ExhibitStore,
    binding: ArtifactBinding,
    as_of: datetime,
    *,
    canonical: bool = False,
) -> bytes:
    record = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == binding.sha256))
    if (
        record is None
        or _binding(record) != binding
        or max(record.created_at, record.available_at, record.received_at, record.observed_at)
        > as_of
    ):
        raise ValueError("RESEARCH_IDENTITY_ARTIFACT_INVALID")
    prefix = "evidence/" if canonical else "raw/"
    path = urlsplit(binding.uri).path
    if "/" + prefix not in path:
        raise ValueError("RESEARCH_IDENTITY_ARTIFACT_INVALID")
    key = prefix + path.split("/" + prefix, 1)[1]
    raw = await (
        store.read_evidence(key, uri=binding.uri)
        if canonical
        else store.read_raw(key, uri=binding.uri)
    )
    if len(raw) != binding.byte_size or sha256(raw).hexdigest() != binding.sha256:
        raise ValueError("RESEARCH_IDENTITY_ARTIFACT_INVALID")
    return raw


async def _save(
    session: AsyncSession, store: ExhibitStore, content: bytes, schema: str, received: datetime
) -> ArtifactBinding:
    digest = sha256(content).hexdigest()
    source = SOURCE if schema == SCHEMA else "sec"
    # Existing identical raw bytes retain their original immutable receipt metadata.
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is not None:
        if artifact.source != source:
            raise ValueError("RESEARCH_IDENTITY_ARTIFACT_CONFLICT")
        binding = _binding(artifact)
        saved = await _read_artifact(
            session,
            store,
            binding,
            await _clock(session),
            canonical=schema == SCHEMA,
        )
        if saved != content:
            raise ValueError("RESEARCH_IDENTITY_ARTIFACT_CONFLICT")
        return binding
    key = f"{'evidence' if schema == SCHEMA else 'raw'}/research-identity/{schema}/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source=source,
            schema_version=schema,
            received_at=received,
            observed_at=received,
            available_at=received,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or artifact.source != source:
        raise ValueError("RESEARCH_IDENTITY_ARTIFACT_CONFLICT")
    binding = _binding(artifact)
    if (
        await _read_artifact(
            session,
            store,
            binding,
            await _clock(session),
            canonical=schema == SCHEMA,
        )
        != content
    ):
        raise ValueError("RESEARCH_IDENTITY_ARTIFACT_CONFLICT")
    return binding


async def import_identity_binding(
    session: AsyncSession, store: ExhibitStore, *, identity_snapshot_id: UUID, collection_dir: Path
) -> ResearchEvidenceRef:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("RESEARCH_IDENTITY_CLOCK_INVALID")
    base, proof = await _base(session, store, identity_snapshot_id, now)
    sources = load_identity_source_archive(
        collection_dir,
        symbol=proof.identity.symbol,
        cik=proof.identity.cik,
        exchange=proof.identity.exchange,
        decision_time=now,
    )
    saved = [
        await _save(session, store, original.content, original.schema_version, original.received_at)
        for original in sources.originals
    ]
    document = ResearchIdentityBinding(
        policy_hash=POLICY_HASH,
        instrument_id=proof.instrument_id,
        alias_id=proof.alias_id,
        symbol=sources.symbol,
        issuer_cik=sources.cik,
        exchange=sources.exchange,
        decision_session=sources.decision_session,
        available_at=max(sources.available_at, base.available_at),
        identity_snapshot_id=base.id,
        identity_source=base.source,
        identity_revision=base.revision,
        identity_payload_hash=_payload_hash(base),
        identity_artifacts=proof.artifacts,
        source_artifacts=(saved[0], saved[1], saved[2], saved[3]),
        source_received_at=(sources.originals[0].received_at, sources.originals[2].received_at),
    )
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    canonical = await _save(session, store, content, SCHEMA, document.available_at)
    identity = dict(source=SOURCE, provider_object_id=str(proof.instrument_id), revision=digest)
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            **identity,
            available_at=document.available_at,
            location=canonical.uri,
            content_hash=digest,
            byte_length=len(content),
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    await session.execute(
        insert(DataSnapshot)
        .values(
            **identity,
            schema_version=SCHEMA,
            observed_at=document.available_at,
            received_at=document.available_at,
            available_at=document.available_at,
            payload=document.model_dump(mode="json"),
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == str(proof.instrument_id),
            ResearchEvidenceRef.revision == digest,
        )
    )
    if evidence is None or (
        evidence.location,
        evidence.available_at,
        evidence.byte_length,
        evidence.content_hash,
    ) != (canonical.uri, document.available_at, len(content), digest):
        raise ValueError("RESEARCH_IDENTITY_REFERENCE_CONFLICT")
    verified = await read_identity_bindings(
        session,
        instrument_id=proof.instrument_id,
        as_of=await _clock(session),
        evidence_ids=(evidence.id,),
        store=store,
    )
    if verified.get(evidence.id) != document:
        raise ValueError("RESEARCH_IDENTITY_REFERENCE_CONFLICT")
    return evidence


async def read_identity_bindings(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
    decision_session: date | None = None,
    store: ExhibitStore | None = None,
) -> dict[UUID, ResearchIdentityBinding]:
    cutoff = require_utc(as_of)
    day = cutoff.astimezone(ZoneInfo("America/New_York")).date()
    if decision_session not in (None, day):
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
            DataSnapshot.provider_object_id == str(instrument_id),
            DataSnapshot.payload["decision_session"].astext == day.isoformat(),
            DataSnapshot.created_at <= cutoff,
            ResearchEvidenceRef.created_at <= cutoff,
        )
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    rows = list(await session.execute(query))
    if len(rows) != 1:
        return {}
    snapshot, evidence = rows[0]
    try:
        document = ResearchIdentityBinding.model_validate(snapshot.payload)
        store = store or configured_exhibit_store(session)
        base, proof = await _base(session, store, document.identity_snapshot_id, cutoff)
        if (
            document.policy_hash != POLICY_HASH
            or document.instrument_id != instrument_id
            or document.alias_id != proof.alias_id
            or document.symbol != proof.identity.symbol
            or document.issuer_cik != proof.identity.cik
            or document.exchange != proof.identity.exchange
            or (
                document.identity_source,
                document.identity_revision,
                document.identity_payload_hash,
                document.identity_artifacts,
            )
            != (base.source, base.revision, _payload_hash(base), proof.artifacts)
            or document.available_at != max(base.available_at, *document.source_received_at)
            or document.available_at > cutoff
        ):
            raise ValueError
        raw = [
            await _read_artifact(session, store, item, cutoff) for item in document.source_artifacts
        ]
        sources = validate_identity_source_originals(
            contents=(raw[0], raw[1], raw[2], raw[3]),
            symbol=document.symbol,
            cik=document.issuer_cik,
            exchange=document.exchange,
            decision_time=cutoff,
            decision_session=day,
        )
        if (
            sources.originals[0].received_at,
            sources.originals[2].received_at,
        ) != document.source_received_at:
            raise ValueError
        content = document.canonical_bytes()
        digest = sha256(content).hexdigest()
        canonical = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if (
            canonical is None
            or canonical.uri != evidence.location
            or canonical.source != SOURCE
            or canonical.schema_version != SCHEMA
            or await _read_artifact(session, store, _binding(canonical), cutoff, canonical=True)
            != content
            or snapshot.revision != digest
            or evidence.revision != digest
            or evidence.content_hash != digest
            or evidence.byte_length != len(content)
            or (
                snapshot.observed_at,
                snapshot.received_at,
                snapshot.available_at,
                evidence.available_at,
            )
            != (document.available_at,) * 4
        ):
            raise ValueError
        return {evidence.id: document}
    except Exception:
        return {}
