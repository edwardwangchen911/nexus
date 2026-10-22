"""Append-only historical observations derived from strictly verified stored originals.

The new purpose never upgrades the old current-identity policy or grants execution.
"""

import json
from datetime import date, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy import Select, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.current_identity_storage import ArtifactBinding
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.current_identity_archive import POLICY_HASH as CURRENT_POLICY_HASH
from nexus.research.current_identity_archive import RECEIPT_SCHEMA
from nexus.research.current_identity_storage import (
    SOURCE as CURRENT_SOURCE,
)
from nexus.research.current_identity_storage import (
    ResearchIdentityBinding,
    _base,
    _binding,
    _clock,
    _payload_hash,
    _read_artifact,
)
from nexus.research.identity_sources import (
    directory_binding,
    parse_identity_source,
    require_submissions_binding,
)
from nexus.research.observation_identity import POLICY_HASH, validate_observation_source_originals
from nexus.research.sec_exhibit_storage import ExhibitStore, configured_exhibit_store

SOURCE = "research_observation_identity"
SCHEMA: Literal["research_observation_identity_v1"] = "research_observation_identity_v1"


class ResearchObservationIdentityBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    schema_version: Literal["research_observation_identity_v1"] = SCHEMA
    policy_id: Literal["research_observation_identity_v1"] = SCHEMA
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_id: UUID
    alias_id: UUID
    alias_valid_from: date
    alias_valid_to: date | None
    symbol: str
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    exchange: str
    available_at: AwareDatetime
    original_evidence_id: UUID
    identity_snapshot_id: UUID
    identity_source: str
    identity_revision: str
    identity_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    identity_artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    source_artifacts: tuple[ArtifactBinding, ArtifactBinding, ArtifactBinding, ArtifactBinding]
    source_received_at: tuple[AwareDatetime, AwareDatetime]
    source_effective_at: None = None
    remote_latest_status: Literal["unverified"] = "unverified"
    purpose: Literal["research_historical_observation_only"] = (
        "research_historical_observation_only"
    )
    historical_backtest_eligible: Literal[False] = False
    price_authority_eligible: Literal[False] = False
    risk_authority_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def _rows(
    instrument_id: UUID, source: str, cutoff: datetime
) -> Select[tuple[DataSnapshot, ResearchEvidenceRef]]:
    return (
        select(DataSnapshot, ResearchEvidenceRef)
        .outerjoin(
            ResearchEvidenceRef,
            (ResearchEvidenceRef.source == DataSnapshot.source)
            & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
            & (ResearchEvidenceRef.revision == DataSnapshot.revision),
        )
        .where(
            DataSnapshot.source == source,
            DataSnapshot.provider_object_id == str(instrument_id),
            DataSnapshot.created_at <= cutoff,
        )
    )


async def _verify_document(
    session: AsyncSession,
    store: ExhibitStore,
    snapshot: DataSnapshot,
    evidence: ResearchEvidenceRef,
    document: ResearchIdentityBinding | ResearchObservationIdentityBinding,
    cutoff: datetime,
) -> None:
    if evidence is None:
        raise ValueError("RESEARCH_OBSERVATION_ORIGINAL_MISSING")
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    canonical = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if (
        canonical is None
        or canonical.source != snapshot.source
        or canonical.schema_version != document.schema_version
        or snapshot.schema_version != document.schema_version
        or canonical.uri != evidence.location
        or evidence.source != snapshot.source
        or evidence.provider_object_id != str(document.instrument_id)
        or snapshot.provider_object_id != str(document.instrument_id)
        or snapshot.revision != digest
        or evidence.revision != digest
        or evidence.content_hash != digest
        or evidence.byte_length != len(content)
        or max(snapshot.created_at, evidence.created_at, document.available_at) > cutoff
        or (
            snapshot.available_at,
            snapshot.observed_at,
            snapshot.received_at,
            evidence.available_at,
            canonical.available_at,
            canonical.received_at,
            canonical.observed_at,
        )
        != (document.available_at,) * 7
        or await _read_artifact(session, store, _binding(canonical), cutoff, canonical=True)
        != content
    ):
        raise ValueError("RESEARCH_OBSERVATION_DOCUMENT_INVALID")
    base, proof = await _base(session, store, document.identity_snapshot_id, cutoff)
    if (
        (
            document.instrument_id,
            document.alias_id,
            document.symbol,
            document.issuer_cik,
            document.exchange,
        )
        != (
            proof.instrument_id,
            proof.alias_id,
            proof.identity.symbol,
            proof.identity.cik,
            proof.identity.exchange,
        )
        or (
            document.identity_source,
            document.identity_revision,
            document.identity_payload_hash,
            document.identity_artifacts,
        )
        != (base.source, base.revision, _payload_hash(base), proof.artifacts)
        or document.available_at != max(base.available_at, *document.source_received_at)
    ):
        raise ValueError("RESEARCH_OBSERVATION_BASE_INVALID")
    raw = [await _read_artifact(session, store, item, cutoff) for item in document.source_artifacts]
    sources = validate_observation_source_originals(
        contents=(raw[0], raw[1], raw[2], raw[3]),
        symbol=document.symbol,
        cik=document.issuer_cik,
        exchange=document.exchange,
        as_of=cutoff,
    )
    if (
        sources.originals[0].received_at,
        sources.originals[2].received_at,
    ) != document.source_received_at:
        raise ValueError("RESEARCH_OBSERVATION_RECEIPT_INVALID")


async def _check_known_sources(
    session: AsyncSession,
    store: ExhibitStore,
    document: ResearchIdentityBinding,
    cutoff: datetime,
) -> None:
    """Known new receipts can contradict an older wrapper before a replacement exists."""
    receipts = await session.scalars(
        select(DataArtifact).where(
            DataArtifact.source == "sec",
            DataArtifact.schema_version == RECEIPT_SCHEMA,
            DataArtifact.created_at <= cutoff,
        )
    )
    directory_url = "https://www.sec.gov/files/company_tickers_exchange.json"
    submissions_url = f"https://data.sec.gov/submissions/CIK{document.issuer_cik}.json"
    for artifact in receipts:
        receipt_raw = await _read_artifact(session, store, _binding(artifact), cutoff)
        receipt = parse_identity_source(receipt_raw)
        url = receipt.get("source_url")
        if url not in {directory_url, submissions_url}:
            continue
        received = require_utc(datetime.fromisoformat(receipt["received_at"]))
        original = await session.scalar(
            select(DataArtifact).where(DataArtifact.sha256 == receipt["sha256"])
        )
        if original is None or original.source != "sec" or received > cutoff:
            raise ValueError("RESEARCH_OBSERVATION_SOURCE_INVALID")
        raw = await _read_artifact(session, store, _binding(original), cutoff)
        if (
            type(receipt.get("byte_length")) is not int
            or type(receipt.get("http_status")) is not int
            or receipt
            != {
                "schema": RECEIPT_SCHEMA,
                "source_url": url,
                "http_status": 200,
                "received_at": receipt["received_at"],
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
            }
        ):
            raise ValueError("RESEARCH_OBSERVATION_SOURCE_INVALID")
        parsed = parse_identity_source(raw)
        if url == directory_url:
            if directory_binding(parsed, document.symbol) != (
                document.issuer_cik,
                document.exchange,
            ):
                raise ValueError("RESEARCH_OBSERVATION_MAPPING_CONFLICT")
        else:
            require_submissions_binding(
                parsed, document.symbol, document.issuer_cik, document.exchange
            )


async def _known_originals(
    session: AsyncSession,
    store: ExhibitStore,
    instrument_id: UUID,
    cutoff: datetime,
) -> tuple[
    list[tuple[DataSnapshot, ResearchEvidenceRef, ResearchIdentityBinding]], InstrumentAlias
]:
    rows = list(await session.execute(_rows(instrument_id, CURRENT_SOURCE, cutoff)))
    if not rows:
        raise ValueError("RESEARCH_OBSERVATION_ORIGINAL_MISSING")
    verified = []
    for snapshot, evidence in rows:
        document = ResearchIdentityBinding.model_validate(snapshot.payload)
        if document.policy_hash != CURRENT_POLICY_HASH:
            raise ValueError("RESEARCH_OBSERVATION_ORIGINAL_INVALID")
        await _verify_document(session, store, snapshot, evidence, document, cutoff)
        verified.append((snapshot, evidence, document))
    mappings = {
        (d.instrument_id, d.alias_id, d.symbol, d.issuer_cik, d.exchange) for _, _, d in verified
    }
    if len(mappings) != 1:
        raise ValueError("RESEARCH_OBSERVATION_MAPPING_CONFLICT")
    document = verified[0][2]
    await _check_known_sources(session, store, document, cutoff)
    instrument = await session.get(Instrument, instrument_id)
    aliases = list(
        await session.scalars(
            select(InstrumentAlias).where(
                or_(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.symbol == document.symbol,
                ),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.created_at <= cutoff,
                InstrumentAlias.valid_from <= cutoff.date(),
                or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= cutoff.date()),
            )
        )
    )
    if (
        instrument is None
        or instrument.created_at > cutoff
        or len(aliases) != 1
        or aliases[0].id != document.alias_id
        or aliases[0].instrument_id != instrument_id
        or aliases[0].symbol != document.symbol
    ):
        raise ValueError("RESEARCH_OBSERVATION_MAPPING_CONFLICT")
    # Inspect every locally known relevant strict proof, even when the caller pins
    # an older observation. A contradictory later proof may never be skipped.
    bases = list(
        await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.source.in_(
                    ("current_instrument_identity", "current_listing_revision")
                ),
                DataSnapshot.created_at <= cutoff,
                or_(
                    DataSnapshot.payload["instrument_id"].astext == str(instrument_id),
                    DataSnapshot.provider_object_id == "identity:" + document.symbol,
                ),
            )
        )
    )
    for candidate in bases:
        _, proof = await _base(session, store, candidate.id, cutoff)
        if (
            proof.instrument_id,
            proof.alias_id,
            proof.identity.symbol,
            proof.identity.cik,
            proof.identity.exchange,
        ) != next(iter(mappings)):
            raise ValueError("RESEARCH_OBSERVATION_MAPPING_CONFLICT")
    return verified, aliases[0]


async def register_observation_identity_binding(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    store: ExhibitStore | None = None,
    identity_evidence_id: UUID | None = None,
) -> ResearchEvidenceRef:
    """Derive a new purpose from existing originals; never collect or backdate."""
    store = store or configured_exhibit_store(session)
    cutoff = await _clock(session)
    originals, alias = await _known_originals(session, store, instrument_id, cutoff)
    eligible = [row for row in originals if identity_evidence_id in (None, row[1].id)]
    if not eligible:
        raise ValueError("RESEARCH_OBSERVATION_ORIGINAL_MISSING")
    _, original, old = max(eligible, key=lambda row: (row[0].created_at, str(row[1].id)))
    fields = old.model_dump(
        exclude={"schema_version", "policy_id", "policy_hash", "decision_session", "purpose"}
    )
    document = ResearchObservationIdentityBinding(
        **fields,
        policy_hash=POLICY_HASH,
        original_evidence_id=original.id,
        alias_valid_from=alias.valid_from,
        alias_valid_to=alias.valid_to,
    )
    content = document.canonical_bytes()
    digest = sha256(content).hexdigest()
    uri = await store.put_if_absent(
        key=f"evidence/research-observation-identity/{digest}.json", content=content
    )
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source=SOURCE,
            schema_version=SCHEMA,
            received_at=document.available_at,
            observed_at=document.available_at,
            available_at=document.available_at,
            created_at=func.clock_timestamp(),
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    identity = dict(source=SOURCE, provider_object_id=str(instrument_id), revision=digest)
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            **identity,
            available_at=document.available_at,
            location=uri,
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
            ResearchEvidenceRef.provider_object_id == str(instrument_id),
            ResearchEvidenceRef.revision == digest,
        )
    )
    if (
        evidence is None
        or (
            await read_observation_identity_bindings(
                session,
                instrument_id=instrument_id,
                as_of=await _clock(session),
                evidence_ids=(evidence.id,),
                store=store,
            )
        ).get(evidence.id)
        != document
    ):
        raise ValueError("RESEARCH_OBSERVATION_REFERENCE_INVALID")
    return evidence


async def read_observation_identity_bindings(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
    store: ExhibitStore | None = None,
) -> dict[UUID, ResearchObservationIdentityBinding]:
    try:
        cutoff = require_utc(as_of)
        if cutoff > await _clock(session):
            return {}
        store = store or configured_exhibit_store(session)
        originals, alias = await _known_originals(session, store, instrument_id, cutoff)
        original_docs = {ref.id: doc for _, ref, doc in originals}
        rows = list(await session.execute(_rows(instrument_id, SOURCE, cutoff)))
        verified = {}
        for snapshot, evidence in rows:
            document = ResearchObservationIdentityBinding.model_validate(snapshot.payload)
            if (
                document.policy_hash != POLICY_HASH
                or document.original_evidence_id not in original_docs
                or document.alias_valid_from != alias.valid_from
                or document.alias_valid_to != alias.valid_to
            ):
                raise ValueError("RESEARCH_OBSERVATION_MAPPING_CONFLICT")
            old = original_docs[document.original_evidence_id]
            shared = set(ResearchIdentityBinding.model_fields) - {
                "schema_version",
                "policy_id",
                "policy_hash",
                "decision_session",
                "purpose",
            }
            if any(getattr(document, key) != getattr(old, key) for key in shared):
                raise ValueError("RESEARCH_OBSERVATION_ORIGINAL_INVALID")
            await _verify_document(session, store, snapshot, evidence, document, cutoff)
            verified[evidence.id] = document
        if evidence_ids is not None:
            return (
                {key: verified[key] for key in evidence_ids}
                if all(key in verified for key in evidence_ids)
                else {}
            )
        if not rows:
            return {}
        # Equal mappings were checked above. New receipt versions retain their
        # original content hashes; this ordering is not a remote freshness claim.
        latest = max(rows, key=lambda row: (row[0].created_at, str(row[1].id)))[1].id
        return {latest: verified[latest]}
    except Exception:
        return {}
