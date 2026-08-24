"""Point-in-time selection of one database-resident price authority."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, case, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.data import DataArtifact, DataSnapshot, PriceSeriesManifest
from nexus.persistence.models.research import ResearchEvidenceRef


async def freeze_price_authority(
    session: AsyncSession,
    *,
    snapshot: DataSnapshot,
    instrument_id: UUID,
    decision_time: datetime,
) -> None:
    """Attach one policy-eligible daily price manifest to a snapshot, if available."""
    existing = await session.scalar(
        select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
            AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
            AgentDataSnapshotEvidenceRecord.instrument_id == instrument_id,
            AgentDataSnapshotEvidenceRecord.authority_kind == "price",
        )
    )
    if existing is not None:
        return
    selected_at = snapshot.available_at
    if snapshot.received_at > decision_time or selected_at > decision_time:
        return
    authority = (
        await session.execute(
            select(PriceSeriesManifest, DataArtifact, ResearchEvidenceRef)
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .join(
                ResearchEvidenceRef,
                ResearchEvidenceRef.id == PriceSeriesManifest.evidence_ref_id,
            )
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.interval == "1Day",
                or_(
                    and_(
                        PriceSeriesManifest.source == "alpaca",
                        PriceSeriesManifest.feed == "sip",
                        PriceSeriesManifest.delivery_mode == "delayed",
                    ),
                    and_(
                        PriceSeriesManifest.source == "fixture",
                        PriceSeriesManifest.delivery_mode == "fixture",
                    ),
                ),
                PriceSeriesManifest.end_at <= selected_at,
                DataArtifact.observed_at == PriceSeriesManifest.start_at,
                DataArtifact.available_at <= selected_at,
                DataArtifact.received_at <= selected_at,
                ResearchEvidenceRef.source == PriceSeriesManifest.source,
                ResearchEvidenceRef.source == DataArtifact.source,
                ResearchEvidenceRef.content_hash == DataArtifact.sha256,
                ResearchEvidenceRef.location == DataArtifact.uri,
            )
            .order_by(
                case((PriceSeriesManifest.source == "alpaca", 0), else_=1),
                DataArtifact.available_at.desc(),
                DataArtifact.received_at.desc(),
                PriceSeriesManifest.id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    if authority is None:
        return
    _manifest, _artifact, evidence = authority
    await session.execute(
        pg_insert(AgentDataSnapshotEvidenceRecord)
        .values(
            data_snapshot_id=snapshot.id,
            instrument_id=instrument_id,
            evidence_ref_id=evidence.id,
            authority_kind="price",
            selected_at=selected_at,
            source=evidence.source,
            provider_object_id=evidence.provider_object_id,
            revision=evidence.revision,
            available_at=evidence.available_at,
            location=evidence.location,
            content_hash=evidence.content_hash,
        )
        .on_conflict_do_nothing()
    )
