"""Research monitoring lineage, immutable checks, and durable trigger latches."""

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class MonitorPlan(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_plans"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "kind", "generation"),
        UniqueConstraint(
            "id", "account_id", "instrument_id", "kind", name="uq_monitor_plan_scope_kind"
        ),
        CheckConstraint("kind IN ('price','disclosure')", name="kind_allowed"),
        CheckConstraint(
            "(kind='price' AND baseline_snapshot_id IS NULL AND baseline_as_of IS NULL) OR "
            "(kind='disclosure' AND baseline_snapshot_id IS NOT NULL "
            "AND baseline_as_of IS NOT NULL "
            "AND baseline_as_of<=effective_at)",
            name="baseline_kind",
        ),
        UniqueConstraint(
            "account_id", "source_output_id", "kind", name="uq_monitor_account_output"
        ),
        ForeignKeyConstraint(
            ["predecessor_id", "account_id", "instrument_id", "kind"],
            [
                "research_monitor_plans.id",
                "research_monitor_plans.account_id",
                "research_monitor_plans.instrument_id",
                "research_monitor_plans.kind",
            ],
            name="fk_monitor_plan_predecessor_kind",
        ),
        CheckConstraint("generation > 0", name="generation_positive"),
        CheckConstraint(
            "effective_at < review_due_at AND review_due_at <= valid_until", name="time_order"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    kind: Mapped[str] = mapped_column(String(16), default="price", server_default="price")
    baseline_snapshot_id: Mapped[UUID | None] = mapped_column(ForeignKey("data_snapshots.id"))
    baseline_as_of: Mapped[datetime | None] = mapped_column(UTCDateTime())
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
        UniqueConstraint("account_id", "instrument_id", "kind", "scheduled_at", "event_key"),
        UniqueConstraint(
            "id",
            "account_id",
            "instrument_id",
            "kind",
            "event_key",
            name="uq_monitor_check_scope_kind",
        ),
        ForeignKeyConstraint(
            ["plan_id", "account_id", "instrument_id", "kind"],
            [
                "research_monitor_plans.id",
                "research_monitor_plans.account_id",
                "research_monitor_plans.instrument_id",
                "research_monitor_plans.kind",
            ],
            name="fk_monitor_check_plan_kind",
        ),
        CheckConstraint(
            "(kind='price' AND event_key='') OR (kind='disclosure' AND length(event_key)>0)",
            name="kind_event",
        ),
        CheckConstraint("result IN ('true','false','unknown')", name="result_allowed"),
        CheckConstraint("scheduled_at <= checked_at", name="time_order"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    plan_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_plans.id"))
    kind: Mapped[str] = mapped_column(String(16), default="price", server_default="price")
    event_key: Mapped[str] = mapped_column(String(128), default="", server_default="")
    scheduled_at: Mapped[datetime] = mapped_column(UTCDateTime())
    checked_at: Mapped[datetime] = mapped_column(UTCDateTime())
    result: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str | None] = mapped_column(String(64))
    observation: Mapped[dict[str, object] | None] = mapped_column(JSONB)


class MonitorTrigger(ImmutableCreatedAt, Base):
    __tablename__ = "research_monitor_triggers"
    __table_args__ = (
        Index(
            "uq_monitor_price_episode",
            "account_id",
            "instrument_id",
            "condition_hash",
            "episode",
            unique=True,
            postgresql_where=text("kind='price'"),
        ),
        Index(
            "uq_monitor_disclosure_event",
            "account_id",
            "instrument_id",
            "event_key",
            unique=True,
            postgresql_where=text("kind='disclosure'"),
        ),
        ForeignKeyConstraint(
            ["check_id", "account_id", "instrument_id", "kind", "event_key"],
            [
                "research_monitor_checks.id",
                "research_monitor_checks.account_id",
                "research_monitor_checks.instrument_id",
                "research_monitor_checks.kind",
                "research_monitor_checks.event_key",
            ],
            name="fk_monitor_trigger_check_kind",
        ),
        UniqueConstraint("check_id"),
        CheckConstraint(
            "(kind='price' AND event_key='' AND episode IS NOT NULL "
            "AND episode>0 AND cause IN ('initial_match','crossed')) OR "
            "(kind='disclosure' AND event_key ~ '^sec:[0-9]{10}:[0-9]{10}-[0-9]{2}-[0-9]{6}$' "
            "AND episode IS NULL AND cause='disclosure_added')",
            name="kind_episode",
        ),
        CheckConstraint(
            "status IN ('pending','blocked','admitted','completed','failed','unknown')",
            name="status_allowed",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    condition_hash: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(16), default="price", server_default="price")
    event_key: Mapped[str] = mapped_column(String(128), default="", server_default="")
    episode: Mapped[int | None] = mapped_column(Integer)
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


class MonitorCoverageReceipt(ImmutableCreatedAt, Base):
    """One append-only account observation of a terminal research result."""

    __tablename__ = "research_monitor_coverage"
    __table_args__ = (
        UniqueConstraint("account_id", "run_id", "kind"),
        CheckConstraint("kind IN ('price','disclosure')", name="kind_allowed"),
        ForeignKeyConstraint(
            ["prior_plan_id", "account_id", "instrument_id", "kind"],
            [
                "research_monitor_plans.id",
                "research_monitor_plans.account_id",
                "research_monitor_plans.instrument_id",
                "research_monitor_plans.kind",
            ],
            name="fk_monitor_coverage_prior_kind",
        ),
        ForeignKeyConstraint(
            ["active_plan_id", "account_id", "instrument_id", "kind"],
            [
                "research_monitor_plans.id",
                "research_monitor_plans.account_id",
                "research_monitor_plans.instrument_id",
                "research_monitor_plans.kind",
            ],
            name="fk_monitor_coverage_active_kind",
        ),
        CheckConstraint(
            "status IN ('installed','retained','gap','unknown','superseded','unverified')",
            name="status_allowed",
        ),
        CheckConstraint(
            "quality_status IS NULL OR quality_status IN ('complete','partial')",
            name="quality_allowed",
        ),
        CheckConstraint("observed_at >= research_as_of", name="time_order"),
        CheckConstraint(
            "status NOT IN ('installed','retained') OR active_plan_id IS NOT NULL",
            name="active_plan_required",
        ),
        CheckConstraint(
            "status NOT IN ('gap','superseded','unverified') OR active_plan_id IS NULL",
            name="gap_has_no_active_plan",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    output_id: Mapped[UUID | None] = mapped_column(ForeignKey("agent_research_outputs.id"))
    kind: Mapped[str] = mapped_column(String(16), default="price", server_default="price")
    prior_plan_id: Mapped[UUID | None] = mapped_column(ForeignKey("research_monitor_plans.id"))
    active_plan_id: Mapped[UUID | None] = mapped_column(ForeignKey("research_monitor_plans.id"))
    research_as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    status: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str] = mapped_column(String(64))
    quality_status: Mapped[str | None] = mapped_column(String(16))


class MonitorDailyTriggerReceipt(ImmutableCreatedAt, Base):
    """An original daily result addressed this exact trigger; no trigger is rewritten."""

    __tablename__ = "research_monitor_daily_trigger_receipts"
    __table_args__ = (
        UniqueConstraint("trigger_id"),
        CheckConstraint("context_hash ~ '^[0-9a-f]{64}$'", name="context_hash_canonical"),
        CheckConstraint("review_bundle_hash ~ '^[0-9a-f]{64}$'", name="review_hash_canonical"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    trigger_id: Mapped[UUID] = mapped_column(ForeignKey("research_monitor_triggers.id"))
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    output_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_outputs.id"))
    snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    context_hash: Mapped[str] = mapped_column(String(64))
    review_bundle_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
