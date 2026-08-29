"""Database-finalized authority for eligible normalized price Bar sets."""

from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.research import ResearchEvidenceRef

SOURCE_POLICY_ID = UUID("3c60888d-e92d-5e04-913c-7d427004cfef")
HASH_VERSION = "price_bar_set_sha256_v1"
POLICY_VERSION = "agent_price_authority_v1"
SOURCE_POLICY_V2_ID = UUID("f780fa7f-9135-5004-ab21-d665ae7be24b")
HASH_VERSION_V2 = "price_bar_set_sha256_v2"
POLICY_VERSION_V2 = "agent_price_authority_v2"


@dataclass(frozen=True)
class PriceBarSetPreparation:
    """Canonical Bar-set fields prepared exclusively by PostgreSQL."""

    content_hash: str
    byte_length: int
    bar_count: int
    first_session: date
    last_session: date
    available_at: datetime


async def prepare_price_bar_set(
    session: AsyncSession, manifest_id: UUID
) -> PriceBarSetPreparation | None:
    """Ask PostgreSQL whether a manifest can become price authority evidence."""
    source = await session.scalar(
        select(PriceSeriesManifest.source).where(PriceSeriesManifest.id == manifest_id)
    )
    function = (
        "nexus_prepare_price_bar_set_v2"
        if source == "fosun"
        else "nexus_prepare_price_bar_set"
    )
    row = (
        await session.execute(
            text(f"SELECT * FROM {function}(:manifest_id)"),
            {"manifest_id": manifest_id},
        )
    ).mappings().one_or_none()
    return None if row is None else PriceBarSetPreparation(**dict(row))


async def finalize_price_bar_set(
    session: AsyncSession, manifest: PriceSeriesManifest
) -> tuple[PriceBarSetAuthority, ResearchEvidenceRef] | None:
    """Persist the database-prepared authority and its owned evidence in this transaction."""
    prepared = await prepare_price_bar_set(session, manifest.id)
    if prepared is None:
        return None

    if manifest.source == "fosun":
        source_policy_id = SOURCE_POLICY_V2_ID
        hash_version = HASH_VERSION_V2
        policy_version = POLICY_VERSION_V2
    else:
        source_policy_id = SOURCE_POLICY_ID
        hash_version = HASH_VERSION
        policy_version = POLICY_VERSION

    provider_object_id = f"price-bar-set:{manifest.id}"
    evidence = ResearchEvidenceRef(
        source=manifest.source,
        provider_object_id=provider_object_id,
        revision=prepared.content_hash,
        available_at=prepared.available_at,
        location=f"db://price-bar-set/{manifest.id}@sha256:{prepared.content_hash}",
        content_hash=prepared.content_hash,
        byte_length=prepared.byte_length,
    )
    session.add(evidence)
    await session.flush()

    authority = PriceBarSetAuthority(
        price_series_manifest_id=manifest.id,
        evidence_ref_id=evidence.id,
        source_policy_id=source_policy_id,
        source=manifest.source,
        provider_object_id=provider_object_id,
        revision=prepared.content_hash,
        hash_version=hash_version,
        policy_version=policy_version,
        content_hash=prepared.content_hash,
        byte_length=prepared.byte_length,
        bar_count=prepared.bar_count,
        first_session=prepared.first_session,
        last_session=prepared.last_session,
        available_at=prepared.available_at,
    )
    session.add(authority)
    await session.flush()
    return authority, evidence
