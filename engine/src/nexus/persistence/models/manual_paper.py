"""Immutable identity and append-only receipts for explicit manual Paper requests."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class ManualPaperRequest(ImmutableCreatedAt, Base):
    __tablename__ = "manual_paper_requests"
    __table_args__ = (
        UniqueConstraint("account_id", "key_hash", name="uq_manual_paper_request_key"),
        CheckConstraint("mode IN ('research_only','research_and_paper')", name="mode_valid"),
        CheckConstraint(
            "key_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$' "
            "AND batch_hash ~ '^[0-9a-f]{64}$' AND model_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_valid",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    candidate_batch_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_candidate_batches.id"))
    model_config_id: Mapped[UUID] = mapped_column(ForeignKey("agent_model_configs.id"))
    mode: Mapped[str] = mapped_column(String(32))
    key_hash: Mapped[str] = mapped_column(String(64))
    request_hash: Mapped[str] = mapped_column(String(64))
    batch_hash: Mapped[str] = mapped_column(String(64))
    model_hash: Mapped[str] = mapped_column(String(64))
    requested_at: Mapped[datetime] = mapped_column(UTCDateTime())


class ManualPaperRequestEvent(ImmutableCreatedAt, Base):
    __tablename__ = "manual_paper_request_events"
    __table_args__ = (
        UniqueConstraint("request_id", "sequence", name="uq_manual_paper_event_sequence"),
        Index(
            "uq_manual_paper_active_cycle",
            "cycle_id",
            unique=True,
            postgresql_where=text("status='active'"),
        ),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        CheckConstraint(
            "status IN ('queued','preparing','active','completed','unavailable','unknown')",
            name="status_valid",
        ),
        CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_valid"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    request_id: Mapped[UUID] = mapped_column(ForeignKey("manual_paper_requests.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24))
    code: Mapped[str] = mapped_column(String(64))
    cycle_id: Mapped[UUID | None] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    event_hash: Mapped[str] = mapped_column(String(64))
