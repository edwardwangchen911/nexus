"""Immutable consumption proof for independent foundation SEC daily inputs."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class SecDurableDailyEventReceipt(ImmutableCreatedAt, Base):
    __tablename__ = "sec_durable_daily_event_receipts"
    __table_args__ = (UniqueConstraint("event_id", name="uq_sec_durable_daily_event"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    event_id: Mapped[UUID] = mapped_column(ForeignKey("sec_discovery_events.id"))
    cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    output_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_outputs.id"))
    snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    context_hash: Mapped[str] = mapped_column(String(64))
    proposal_hash: Mapped[str] = mapped_column(String(64))
    review_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
