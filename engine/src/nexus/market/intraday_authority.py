"""Database-finalized authority for display-only normalized 1Min Bar sets."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import IntradayBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.quality.source_policy import INTRADAY_SOURCE_POLICY_ID

HASH_VERSION = "intraday_bar_set_sha256_v1"
POLICY_VERSION = "intraday_monitor_authority_v1"
INTRADAY_SOURCE_POLICY_V2_ID = UUID("9bd1eb52-5486-5f40-a3d8-06bd56f80b8d")
HASH_VERSION_V2 = "intraday_bar_set_sha256_v2"
POLICY_VERSION_V2 = "intraday_monitor_authority_v2"


@dataclass(frozen=True)
class IntradayBarSetPreparation:
    content_hash: str
    byte_length: int
    bar_count: int
    first_observed_at: datetime
    last_observed_at: datetime
    available_at: datetime


async def prepare_intraday_bar_set(
    session: AsyncSession, manifest_id: UUID
) -> IntradayBarSetPreparation | None:
    source = await session.scalar(
        select(PriceSeriesManifest.source).where(PriceSeriesManifest.id == manifest_id)
    )
    function = (
        "nexus_prepare_intraday_bar_set_v2"
        if source == "fosun"
        else "nexus_prepare_intraday_bar_set"
    )
    row = (
        (
            await session.execute(
                text(f"SELECT * FROM {function}(:manifest_id)"),
                {"manifest_id": manifest_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else IntradayBarSetPreparation(**dict(row))


async def finalize_intraday_bar_set(
    session: AsyncSession, manifest: PriceSeriesManifest
) -> tuple[IntradayBarSetAuthority, ResearchEvidenceRef] | None:
    """Persist only PostgreSQL-recomputed, source-owned 1Min authority."""
    prepared = await prepare_intraday_bar_set(session, manifest.id)
    if prepared is None:
        return None
    if manifest.source == "fosun":
        source_policy_id = INTRADAY_SOURCE_POLICY_V2_ID
        hash_version = HASH_VERSION_V2
        policy_version = POLICY_VERSION_V2
    else:
        source_policy_id = INTRADAY_SOURCE_POLICY_ID
        hash_version = HASH_VERSION
        policy_version = POLICY_VERSION
    provider_object_id = f"intraday-bar-set:{manifest.id}"
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
    authority = IntradayBarSetAuthority(
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
        first_observed_at=prepared.first_observed_at,
        last_observed_at=prepared.last_observed_at,
        available_at=prepared.available_at,
    )
    session.add(authority)
    await session.flush()
    return authority, evidence
