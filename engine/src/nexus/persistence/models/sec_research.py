"""Immutable plan-specific research custody; notice identity stays in sec_monitor."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class SecEventResearchBinding(ImmutableCreatedAt, Base):
    __tablename__ = "sec_event_research_bindings"
    __table_args__ = (
        UniqueConstraint("event_id", "plan_id"),
        UniqueConstraint("check_id"),
        UniqueConstraint("trigger_id"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    event_id: Mapped[UUID] = mapped_column(ForeignKey("sec_discovery_events.id"))
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("sec_monitor_rules.id"))
    discovery_check_id: Mapped[UUID] = mapped_column(ForeignKey("sec_discovery_checks.id"))
    metadata_hash: Mapped[str] = mapped_column(String(64))
    condition_hash: Mapped[str] = mapped_column(String(64))
    plan_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_plans.id"))
    source_run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    source_output_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_outputs.id"))
    source_hash: Mapped[str] = mapped_column(String(64))
    capture_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    capture_hash: Mapped[str] = mapped_column(String(64))
    check_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_monitor_checks.id", deferrable=True, initially="DEFERRED")
    )
    trigger_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_monitor_triggers.id", deferrable=True, initially="DEFERRED")
    )
    batch_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_monitor_batches.id", deferrable=True, initially="DEFERRED")
    )
    bound_at: Mapped[datetime] = mapped_column(UTCDateTime())


class SecEventConsumptionReceipt(ImmutableCreatedAt, Base):
    __tablename__ = "sec_event_consumption_receipts"
    __table_args__ = (
        CheckConstraint("num_nonnulls(mixed_receipt_id,daily_receipt_id)=1", name="exact_proof"),
        CheckConstraint("status='addressed'", name="addressed_only"),
        UniqueConstraint("mixed_receipt_id"),
        UniqueConstraint("daily_receipt_id"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    binding_id: Mapped[UUID] = mapped_column(ForeignKey("sec_event_research_bindings.id"))
    event_id: Mapped[UUID] = mapped_column(ForeignKey("sec_discovery_events.id"))
    status: Mapped[str] = mapped_column(String(16), default="addressed", server_default="addressed")
    mixed_receipt_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_monitor_mixed_event_receipts.id")
    )
    daily_receipt_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_monitor_daily_event_receipts.id")
    )
