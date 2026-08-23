# ruff: noqa: E501
"""Immutable persistence records for deterministic optimization proposals."""

from decimal import Decimal
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt


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
