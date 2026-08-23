# ruff: noqa: E501
"""Immutable persistence records for deterministic optimization proposals."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class OptimizationPolicyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "optimization_policies"
    __table_args__ = (
        UniqueConstraint("key", "version", name="uq_optimization_policies_key_version"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class CostPolicyRecord(ImmutableCreatedAt, Base):
    """Versioned, evidenced cost inputs; rows are immutable once published."""

    __tablename__ = "cost_policies"
    __table_args__ = (UniqueConstraint("key", "version", name="uq_cost_policies_key_version"),)
    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(32))
    parameters: Mapped[dict[str, object]] = mapped_column(JSONB)
    evidence_refs: Mapped[list[str]] = mapped_column(JSONB)
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())


class OptimizationAdmissionAuthorityRecord(ImmutableCreatedAt, Base):
    """Frozen upstream proof required before admitting a proposal-construction job."""

    __tablename__ = "optimization_admission_authorities"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_snapshot_id",
            "data_snapshot_id",
            "cost_policy_id",
            name="uq_optimization_admission_authorities_binding",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_snapshots.id"))
    diagnostic_report_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_diagnostic_reports.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    source_policy_id: Mapped[UUID] = mapped_column(ForeignKey("source_policies.id"))
    optimizer_policy_id: Mapped[UUID] = mapped_column(ForeignKey("optimization_policies.id"))
    cost_policy_id: Mapped[UUID] = mapped_column(ForeignKey("cost_policies.id"))
    diagnostic_input_hash: Mapped[str] = mapped_column(String(64))
    valuation_time: Mapped[datetime] = mapped_column(UTCDateTime())
    baseline_ready: Mapped[bool] = mapped_column(Boolean)
    diagnostic_complete: Mapped[bool] = mapped_column(Boolean)
    source_conflict: Mapped[bool] = mapped_column(Boolean)
    partial: Mapped[bool] = mapped_column(Boolean)
    requires_fx: Mapped[bool] = mapped_column(Boolean)
    cost_complete: Mapped[bool] = mapped_column(Boolean)
    authority_stale: Mapped[bool] = mapped_column(Boolean, default=False)


class OptimizationProposalGroupRecord(ImmutableCreatedAt, Base):
    __tablename__ = "optimization_proposal_groups"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft','compared','shadowing','validated','rejected','expired')",
            name="status_allowed",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    baseline_portfolio_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("portfolio_snapshots.id")
    )
    status: Mapped[str] = mapped_column(String(20), default="draft")
    version: Mapped[int] = mapped_column(Integer, default=0)
    policy_id: Mapped[UUID] = mapped_column(ForeignKey("optimization_policies.id"))
    diagnostic_report_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("portfolio_diagnostic_reports.id")
    )
    data_snapshot_id: Mapped[UUID | None] = mapped_column(ForeignKey("data_snapshots.id"))
    source_policy_id: Mapped[UUID | None] = mapped_column(ForeignKey("source_policies.id"))
    cost_policy_id: Mapped[UUID | None] = mapped_column(ForeignKey("cost_policies.id"))
    diagnostic_input_hash: Mapped[str | None] = mapped_column(String(64))
    valuation_time: Mapped[datetime | None] = mapped_column(UTCDateTime())
    baseline_ready: Mapped[bool] = mapped_column(Boolean, default=False)
    diagnostic_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    source_conflict: Mapped[bool] = mapped_column(Boolean, default=False)
    partial: Mapped[bool] = mapped_column(Boolean, default=False)
    requires_fx: Mapped[bool] = mapped_column(Boolean, default=False)
    cost_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    authority_stale: Mapped[bool] = mapped_column(Boolean, default=False)


class OptimizationProposalRecord(ImmutableCreatedAt, Base):
    __tablename__ = "optimization_proposals"
    __table_args__ = (
        UniqueConstraint(
            "proposal_group_id", "variant", name="uq_optimization_proposals_group_variant"
        ),
        CheckConstraint(
            "variant IN ('hold_current','minimum_change','strategy_target')", name="variant_allowed"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    proposal_group_id: Mapped[UUID] = mapped_column(ForeignKey("optimization_proposal_groups.id"))
    variant: Mapped[str] = mapped_column(String(30))
    baseline_portfolio_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("portfolio_snapshots.id")
    )
    analysis_only: Mapped[bool]
    strategy_evidence_complete: Mapped[bool]


class ProposalPositionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "proposal_positions"
    __table_args__ = (
        UniqueConstraint(
            "proposal_id", "instrument_id", name="uq_proposal_positions_proposal_instrument"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    proposal_id: Mapped[UUID] = mapped_column(ForeignKey("optimization_proposals.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    current_weight: Mapped[Decimal]
    target_weight: Mapped[Decimal]
    action: Mapped[str] = mapped_column(String(20))
    reason_code: Mapped[str] = mapped_column(String(100))


class ProposalComparisonRecord(ImmutableCreatedAt, Base):
    __tablename__ = "proposal_comparisons"
    __table_args__ = (
        UniqueConstraint("proposal_id", name="uq_proposal_comparisons_proposal"),
        CheckConstraint("tax_impact = 'unknown'", name="tax_impact_unknown"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    proposal_id: Mapped[UUID] = mapped_column(ForeignKey("optimization_proposals.id"))
    metrics: Mapped[dict[str, object]] = mapped_column(JSONB)
    tax_impact: Mapped[str] = mapped_column(String(20), default="unknown")
    requires_fx: Mapped[bool] = mapped_column(Boolean, default=False)
    non_executable: Mapped[bool] = mapped_column(Boolean, default=True)
    cost_complete: Mapped[bool] = mapped_column(Boolean, default=False)
