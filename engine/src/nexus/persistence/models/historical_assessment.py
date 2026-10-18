"""Immutable sources for isolated, non-executing historical assessments."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CHAR, CheckConstraint, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.persistence.base import Base, UTCDateTime


class HistoricalAssessmentSourceRecord(Base):
    __tablename__ = "historical_assessment_sources"
    __table_args__ = (
        UniqueConstraint("cycle_id"),
        UniqueConstraint("archive_hash"),
        CheckConstraint("cycle_id <> source_cycle_id", name="cycles_distinct"),
        CheckConstraint(
            "archive_hash ~ '^[0-9a-f]{64}$' AND content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        CheckConstraint(
            "evidence_at <= observed_started_at AND observed_started_at <= observed_finished_at "
            "AND observed_finished_at <= imported_at",
            name="times_ordered",
        ),
        CheckConstraint(
            "jsonb_typeof(payload) = 'object' "
            "AND jsonb_typeof(payload->'archive') IS NOT DISTINCT FROM 'object' "
            "AND jsonb_typeof(payload->'projection') IS NOT DISTINCT FROM 'object' "
            "AND jsonb_typeof(payload->'projection'->'research') IS NOT DISTINCT FROM 'array'",
            name="payload_shape",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    source_cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    archive_hash: Mapped[str] = mapped_column(CHAR(64))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    content_hash: Mapped[str] = mapped_column(CHAR(64))
    evidence_at: Mapped[datetime] = mapped_column(UTCDateTime())
    observed_started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    observed_finished_at: Mapped[datetime] = mapped_column(UTCDateTime())
    imported_at: Mapped[datetime] = mapped_column(UTCDateTime())
