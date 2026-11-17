"""Append-only supersession of unsubmitted SEC research batches."""

from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt


class SecResearchBatchSupersession(ImmutableCreatedAt, Base):
    __tablename__ = "sec_research_batch_supersessions"
    __table_args__ = (
        UniqueConstraint("old_batch_id"),
        CheckConstraint("old_batch_id<>new_batch_id", name="distinct_batches"),
        CheckConstraint("reason='current_evidence_refreeze'", name="reason"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    old_batch_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_batches.id"))
    new_batch_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_batches.id"))
    reason: Mapped[str] = mapped_column(
        String(40), default="current_evidence_refreeze", server_default="current_evidence_refreeze"
    )
