"""Append-only durable SEC discovery, notice identity and session admission facts."""

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class SecMonitorRule(ImmutableCreatedAt, Base):
    __tablename__ = "sec_monitor_rules"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id"),
        CheckConstraint("issuer_cik ~ '^[0-9]{10}$'", name="issuer"),
        CheckConstraint("policy_version='durable_sec_v1'", name="policy"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    issuer_cik: Mapped[str] = mapped_column(String(10))
    identity_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    anchor_plan_id: Mapped[UUID | None] = mapped_column(ForeignKey("research_monitor_plans.id"))
    effective_at: Mapped[datetime] = mapped_column(UTCDateTime())
    forms: Mapped[list[str]] = mapped_column(JSONB)
    policy_version: Mapped[str] = mapped_column(String(32), default="durable_sec_v1")


class SecMonitorRuleState(ImmutableCreatedAt, Base):
    __tablename__ = "sec_monitor_rule_states"
    __table_args__ = (
        UniqueConstraint("rule_id", "sequence"),
        CheckConstraint("state IN ('active','paused','revoked')", name="state"),
        CheckConstraint("sequence>=0", name="sequence"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("sec_monitor_rules.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(16))
    effective_at: Mapped[datetime] = mapped_column(UTCDateTime())


class SecDiscoveryCheck(ImmutableCreatedAt, Base):
    __tablename__ = "sec_discovery_checks"
    __table_args__ = (
        UniqueConstraint("rule_id", "scheduled_at"),
        CheckConstraint("result IN ('true','false','unknown')", name="result"),
        CheckConstraint("coverage IN ('complete','partial','unknown')", name="coverage"),
        CheckConstraint("scheduled_at<=checked_at", name="time_order"),
        CheckConstraint(
            "(result='unknown' AND reason IS NOT NULL) OR (result<>'unknown' AND reason IS NULL)",
            name="reason",
        ),
        CheckConstraint("result<>'false' OR coverage='complete'", name="negative_coverage"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("sec_monitor_rules.id"))
    scheduled_at: Mapped[datetime] = mapped_column(UTCDateTime())
    checked_at: Mapped[datetime] = mapped_column(UTCDateTime())
    capture_id: Mapped[UUID | None] = mapped_column(ForeignKey("data_snapshots.id"))
    result: Mapped[str] = mapped_column(String(16))
    coverage: Mapped[str] = mapped_column(String(16))
    observation: Mapped[dict[str, object]] = mapped_column(JSONB)
    reason: Mapped[str | None] = mapped_column(String(100))


class SecDiscoveryEvent(ImmutableCreatedAt, Base):
    __tablename__ = "sec_discovery_events"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "event_key"),
        CheckConstraint("metadata_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    event_key: Mapped[str] = mapped_column(String(128))
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("sec_monitor_rules.id"))
    capture_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    entry: Mapped[dict[str, object]] = mapped_column(JSONB)
    metadata_hash: Mapped[str] = mapped_column(String(64))
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime())
    observation_receipt_id: Mapped[UUID | None] = mapped_column(ForeignKey("audit_events.id"))


class SecDiscoveryEventRevision(ImmutableCreatedAt, Base):
    __tablename__ = "sec_discovery_event_revisions"
    __table_args__ = (
        UniqueConstraint("event_id", "metadata_hash"),
        CheckConstraint("metadata_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    event_id: Mapped[UUID] = mapped_column(ForeignKey("sec_discovery_events.id"))
    capture_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    entry: Mapped[dict[str, object]] = mapped_column(JSONB)
    metadata_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class SecNotificationClaim(ImmutableCreatedAt, Base):
    __tablename__ = "sec_notification_claims"
    __table_args__ = (
        UniqueConstraint("account_id", "instrument_id", "event_key"),
        UniqueConstraint("legacy_trigger_id"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    event_key: Mapped[str] = mapped_column(String(128))
    outbox_id: Mapped[UUID] = mapped_column(
        ForeignKey("outbox_events.id", deferrable=True, initially="DEFERRED")
    )
    legacy_trigger_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_monitor_triggers.id", deferrable=True, initially="DEFERRED")
    )


class SecAutoIncrementDayClaim(ImmutableCreatedAt, Base):
    __tablename__ = "sec_auto_increment_day_claims"
    __table_args__ = (UniqueConstraint("increment_id"), UniqueConstraint("run_id"))
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"), primary_key=True)
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    session_date: Mapped[date] = mapped_column(primary_key=True)
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    increment_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_monitor_increments.id", deferrable=True, initially="DEFERRED")
    )
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
