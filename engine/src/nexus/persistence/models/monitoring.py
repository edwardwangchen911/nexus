"""Research monitoring lineage, immutable checks, and durable trigger latches."""

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, Date, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class MonitorPlan(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_plans"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "generation"),
        UniqueConstraint("account_id", "source_output_id", name="uq_monitor_account_output"),
        CheckConstraint("generation > 0", name="generation_positive"),
        CheckConstraint(
            "effective_at < review_due_at AND review_due_at <= valid_until", name="time_order"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    generation: Mapped[int] = mapped_column(Integer)
    source_run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    source_output_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_outputs.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    predecessor_id: Mapped[UUID | None] = mapped_column(ForeignKey("research_monitor_plans.id"))
    condition_hash: Mapped[str] = mapped_column(String(64))
    condition: Mapped[dict[str, object]] = mapped_column(JSONB)
    source_hash: Mapped[str] = mapped_column(String(64))
    effective_at: Mapped[datetime] = mapped_column(UTCDateTime())
    review_due_at: Mapped[datetime] = mapped_column(UTCDateTime())
    valid_until: Mapped[datetime] = mapped_column(UTCDateTime())


class MonitorEpisode(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_episodes"
    __table_args__ = (CheckConstraint("sequence >= 0", name="sequence_nonnegative"),)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"), primary_key=True)
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    condition_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, default=0)
    latched: Mapped[bool] = mapped_column(Boolean, default=False)
    observed_false: Mapped[bool] = mapped_column(Boolean, default=False)
    checked_at: Mapped[datetime] = mapped_column(UTCDateTime())


class MonitorCheck(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_checks"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "scheduled_at"),
        CheckConstraint("result IN ('true','false','unknown')", name="result_allowed"),
        CheckConstraint("scheduled_at <= checked_at", name="time_order"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    plan_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_plans.id"))
    scheduled_at: Mapped[datetime] = mapped_column(UTCDateTime())
    checked_at: Mapped[datetime] = mapped_column(UTCDateTime())
    result: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str | None] = mapped_column(String(64))
    observation: Mapped[dict[str, object] | None] = mapped_column(JSONB)


class MonitorTrigger(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_triggers"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "condition_hash", "episode"),
        UniqueConstraint("check_id"),
        CheckConstraint("episode > 0", name="episode_positive"),
        CheckConstraint(
            "status IN ('pending','blocked','admitted','completed','failed','unknown')",
            name="status_allowed",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    condition_hash: Mapped[str] = mapped_column(String(64))
    episode: Mapped[int] = mapped_column(Integer)
    check_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_checks.id"))
    cause: Mapped[str] = mapped_column(String(24))
    status: Mapped[str] = mapped_column(String(16), default="pending")
    reason: Mapped[str | None] = mapped_column(String(64))
    research_run_id: Mapped[UUID | None] = mapped_column(ForeignKey("agent_research_runs.id"))
    outbox_id: Mapped[UUID] = mapped_column(ForeignKey("outbox_events.id"), unique=True)


class MonitorIncrement(ImmutableCreatedAt, Base):
    """One immutable, non-executing research admission and its finite call hold."""

    __tablename__ = "research_monitor_increments"
    __table_args__ = (
        CheckConstraint("reserved_sdk_calls = 3", name="three_stage_calls"),
        CheckConstraint("daily_sdk_call_limit BETWEEN 3 AND 90", name="bounded_call_limit"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    trigger_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_monitor_triggers.id"), unique=True
    )
    plan_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_plans.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"), unique=True)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id"), unique=True)
    snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"), unique=True)
    research_as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    session_date: Mapped[date] = mapped_column(Date())
    reserved_sdk_calls: Mapped[int] = mapped_column(Integer)
    daily_sdk_call_limit: Mapped[int] = mapped_column(Integer)
    context_hash: Mapped[str] = mapped_column(String(64))
    manifest_hash: Mapped[str] = mapped_column(String(64))
