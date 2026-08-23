"""Immutable database records for advisory policy and diagnostic projections."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class DiagnosticPolicy(ImmutableCreatedAt, Base):
    __tablename__ = "diagnostic_policies"
    __table_args__ = (
        UniqueConstraint("key", "version", name="uq_diagnostic_policies_key_version"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class PortfolioDiagnosticReportRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_diagnostic_reports"
    __table_args__ = (
        CheckConstraint(
            "optimization_need IN ("
            "'BLOCKED', 'RESEARCH_REQUIRED', 'OPTIMIZATION_CANDIDATE', 'NO_ACTION')",
            name="optimization_need_allowed",
        ),
        UniqueConstraint(
            "portfolio_snapshot_id",
            "policy_id",
            name="uq_diag_reports_snapshot_policy",
        ),
        ForeignKeyConstraint(
            ["portfolio_snapshot_id", "portfolio_id"],
            ["portfolio_snapshots.id", "portfolio_snapshots.portfolio_id"],
            name="fk_diag_reports_snapshot_same_portfolio",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_id: Mapped[UUID] = mapped_column(ForeignKey("portfolios.id"))
    portfolio_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_snapshots.id"))
    trading_day: Mapped[date | None] = mapped_column(Date)
    policy_id: Mapped[UUID] = mapped_column(ForeignKey("diagnostic_policies.id"))
    policy_version: Mapped[str] = mapped_column(String(100))
    optimization_need: Mapped[str] = mapped_column(String(40))


class PortfolioDiagnosticItemRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_diagnostic_items"
    __table_args__ = (
        CheckConstraint("severity IN ('info', 'warning', 'critical')", name="severity_allowed"),
        CheckConstraint(
            "actionability IN ('monitor', 'research', 'reduce_only', 'blocked')",
            name="actionability_allowed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    report_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_diagnostic_reports.id"))
    diagnostic_code: Mapped[str] = mapped_column(String(200))
    metric_value: Mapped[Decimal | None]
    unit: Mapped[str] = mapped_column(String(100))
    reference_limit: Mapped[Decimal | None]
    policy_scope: Mapped[str] = mapped_column(String(200))
    severity: Mapped[str] = mapped_column(String(20))
    actionability: Mapped[str] = mapped_column(String(20))
    unknown_reason: Mapped[str | None] = mapped_column(String(200))


class PortfolioDiagnosticEvidenceRefRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_diagnostic_evidence_refs"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    item_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_diagnostic_items.id"))
    source: Mapped[str] = mapped_column(String(100))
    object_id: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    location: Mapped[str] = mapped_column(String(1000))
    content_hash: Mapped[str] = mapped_column(String(64))
