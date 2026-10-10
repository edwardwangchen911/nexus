"""Immutable economic identity and planned-slot links, without activation authority."""

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class EconomicEvaluationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "economic_evaluations"
    __table_args__ = (
        UniqueConstraint("protocol_hash"),
        UniqueConstraint("binding_event_id"),
        ForeignKeyConstraint(
            ["incumbent_account_id", "incumbent_portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
        ),
        ForeignKeyConstraint(
            ["challenger_account_id", "challenger_portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
        ),
        ForeignKeyConstraint(
            ["incumbent_opening_id", "incumbent_account_id"],
            ["paper_ledger_transactions.id", "paper_ledger_transactions.account_id"],
        ),
        ForeignKeyConstraint(
            ["challenger_opening_id", "challenger_account_id"],
            ["paper_ledger_transactions.id", "paper_ledger_transactions.account_id"],
        ),
        CheckConstraint(
            "incumbent_account_id <> challenger_account_id "
            "AND incumbent_portfolio_id <> challenger_portfolio_id",
            name="lanes_distinct",
        ),
        CheckConstraint(
            "protocol_hash ~ '^[0-9a-f]{64}$' AND binding_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    protocol_event_id: Mapped[UUID] = mapped_column(ForeignKey("audit_events.id"))
    binding_event_id: Mapped[UUID] = mapped_column(ForeignKey("audit_events.id"))
    protocol_hash: Mapped[str] = mapped_column(String(64))
    binding_hash: Mapped[str] = mapped_column(String(64))
    incumbent_account_id: Mapped[UUID]
    incumbent_portfolio_id: Mapped[UUID]
    incumbent_opening_id: Mapped[UUID]
    challenger_account_id: Mapped[UUID]
    challenger_portfolio_id: Mapped[UUID]
    challenger_opening_id: Mapped[UUID]


class EconomicPairSlotRecord(ImmutableCreatedAt, Base):
    __tablename__ = "economic_pair_slots"
    __table_args__ = (
        UniqueConstraint("evaluation_id", "ordinal", name="uq_economic_pair_slot_ordinal"),
        UniqueConstraint("evaluation_id", "decision_at", name="uq_economic_pair_slot_time"),
        CheckConstraint("ordinal BETWEEN 1 AND 24", name="ordinal_bounded"),
        CheckConstraint(
            "cutoff_at = decision_at AND market_close_at > decision_at", name="times_ordered"
        ),
        CheckConstraint("execution_locked", name="execution_locked"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    evaluation_id: Mapped[UUID] = mapped_column(ForeignKey("economic_evaluations.id"))
    ordinal: Mapped[int] = mapped_column(Integer)
    session: Mapped[date] = mapped_column(Date)
    decision_at: Mapped[datetime] = mapped_column(UTCDateTime())
    cutoff_at: Mapped[datetime] = mapped_column(UTCDateTime())
    market_close_at: Mapped[datetime] = mapped_column(UTCDateTime())
    execution_locked: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))


class EconomicLaneCycleRecord(ImmutableCreatedAt, Base):
    __tablename__ = "economic_lane_cycles"
    __table_args__ = (
        UniqueConstraint("pair_slot_id", "lane"),
        UniqueConstraint("cycle_id"),
        ForeignKeyConstraint(
            ["account_id", "portfolio_id"], ["paper_accounts.id", "paper_accounts.portfolio_id"]
        ),
        CheckConstraint("lane IN ('incumbent','challenger')", name="lane_allowed"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    pair_slot_id: Mapped[UUID] = mapped_column(ForeignKey("economic_pair_slots.id"))
    lane: Mapped[str] = mapped_column(String(16))
    account_id: Mapped[UUID]
    portfolio_id: Mapped[UUID]
    cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))


class EconomicCommonWindowRecord(ImmutableCreatedAt, Base):
    __tablename__ = "economic_common_windows"
    __table_args__ = (
        UniqueConstraint("pair_slot_id", "window_seq"),
        CheckConstraint("window_seq >= 1", name="sequence_positive"),
        CheckConstraint("status IN ('frozen','missed')", name="status_allowed"),
        CheckConstraint(
            "window_end = window_start + interval '5 minutes' AND eligible_at < window_start",
            name="bounds_exact",
        ),
        CheckConstraint(
            "status <> 'frozen' OR frozen_at < window_start", name="frozen_before_start"
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    pair_slot_id: Mapped[UUID] = mapped_column(ForeignKey("economic_pair_slots.id"))
    window_seq: Mapped[int]
    status: Mapped[str] = mapped_column(String(16))
    eligible_at: Mapped[datetime] = mapped_column(UTCDateTime())
    window_start: Mapped[datetime] = mapped_column(UTCDateTime())
    window_end: Mapped[datetime] = mapped_column(UTCDateTime())
    frozen_at: Mapped[datetime] = mapped_column(UTCDateTime())
    qualification: Mapped[dict[str, object]] = mapped_column(JSONB)
    content_hash: Mapped[str] = mapped_column(String(64))


class EconomicWindowOrderRecord(ImmutableCreatedAt, Base):
    __tablename__ = "economic_window_orders"
    __table_args__ = (
        UniqueConstraint("window_id", "order_id"),
        ForeignKeyConstraint(
            ["order_id", "account_id"], ["paper_orders.id", "paper_orders.account_id"]
        ),
        CheckConstraint("lane IN ('incumbent','challenger')", name="lane_allowed"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    window_id: Mapped[UUID] = mapped_column(ForeignKey("economic_common_windows.id"))
    lane: Mapped[str] = mapped_column(String(16))
    order_id: Mapped[UUID]
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
