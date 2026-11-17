"""Append-only stable-key conflicts cover legacy and durable SEC notices alike."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class SecDisclosureKeyConflict(ImmutableCreatedAt, Base):
    __tablename__ = "sec_disclosure_key_conflicts"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "event_key", "metadata_hash"),
        CheckConstraint("num_nonnulls(original_event_id,legacy_trigger_id)=1", name="original"),
        CheckConstraint("metadata_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    event_key: Mapped[str] = mapped_column(String(128))
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("sec_monitor_rules.id"))
    capture_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    original_event_id: Mapped[UUID | None] = mapped_column(ForeignKey("sec_discovery_events.id"))
    legacy_trigger_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_monitor_triggers.id")
    )
    entry: Mapped[dict[str, object]] = mapped_column(JSONB)
    metadata_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
