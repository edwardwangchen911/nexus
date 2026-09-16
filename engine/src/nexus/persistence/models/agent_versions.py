"""Append-only authority for paired Agent-version paper evaluation."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime

_HASH_CHECK = " ~ '^[0-9a-f]{64}$'"


class AgentVersionEvaluationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_version_evaluations"
    __table_args__ = (
        UniqueConstraint("incumbent_account_id", name="uq_agent_version_evaluations_incumbent"),
        UniqueConstraint("challenger_account_id", name="uq_agent_version_evaluations_challenger"),
        UniqueConstraint("authority_hash", name="uq_agent_version_evaluations_authority"),
        CheckConstraint("incumbent_account_id <> challenger_account_id", name="distinct_accounts"),
        CheckConstraint("policy_version = 'agent_version_evaluation_v1'", name="policy_version"),
        CheckConstraint("cardinality(train_slots) = 4", name="train_slots"),
        CheckConstraint("cardinality(oos_slots) = 20", name="oos_slots"),
        CheckConstraint(
            f"incumbent_prompt_hash{_HASH_CHECK} AND incumbent_model_hash{_HASH_CHECK} "
            f"AND challenger_prompt_hash{_HASH_CHECK} AND challenger_model_hash{_HASH_CHECK} "
            f"AND policy_hash{_HASH_CHECK} AND authority_hash{_HASH_CHECK}",
            name="hashes",
        ),
        CheckConstraint(
            "incumbent_prompt_hash <> challenger_prompt_hash "
            "OR incumbent_model_hash <> challenger_model_hash",
            name="distinct_versions",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    incumbent_account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    challenger_account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    incumbent_version_source_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_research_runs.id")
    )
    challenger_prompt_version_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_prompt_versions.id")
    )
    challenger_model_config_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_model_configs.id")
    )
    incumbent_prompt_hash: Mapped[str] = mapped_column(String(64))
    incumbent_model_hash: Mapped[str] = mapped_column(String(64))
    challenger_prompt_hash: Mapped[str] = mapped_column(String(64))
    challenger_model_hash: Mapped[str] = mapped_column(String(64))
    train_slots: Mapped[list[datetime]] = mapped_column(ARRAY(DateTime(timezone=True)))
    oos_slots: Mapped[list[datetime]] = mapped_column(ARRAY(DateTime(timezone=True)))
    policy_version: Mapped[str] = mapped_column(String(64))
    policy_hash: Mapped[str] = mapped_column(String(64))
    authority_hash: Mapped[str] = mapped_column(String(64))


class AgentVersionCyclePairRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_version_cycle_pairs"
    __table_args__ = (
        UniqueConstraint(
            "evaluation_id", "decision_slot_at", name="uq_agent_version_cycle_pairs_slot"
        ),
        UniqueConstraint("incumbent_cycle_id", name="uq_agent_version_cycle_pairs_inc_cycle"),
        UniqueConstraint("challenger_cycle_id", name="uq_agent_version_cycle_pairs_chal_cycle"),
        UniqueConstraint("incumbent_run_id", name="uq_agent_version_cycle_pairs_inc_run"),
        UniqueConstraint("challenger_run_id", name="uq_agent_version_cycle_pairs_chal_run"),
        UniqueConstraint("pair_hash", name="uq_agent_version_cycle_pairs_hash"),
        CheckConstraint("phase IN ('train','oos')", name="phase"),
        CheckConstraint("incumbent_cycle_id <> challenger_cycle_id", name="distinct_cycles"),
        CheckConstraint("incumbent_run_id <> challenger_run_id", name="distinct_runs"),
        CheckConstraint(
            f"market_evidence_hash{_HASH_CHECK} AND "
            f"starting_projection_hash{_HASH_CHECK} AND pair_hash{_HASH_CHECK}",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    evaluation_id: Mapped[UUID] = mapped_column(ForeignKey("agent_version_evaluations.id"))
    phase: Mapped[str] = mapped_column(String(16))
    decision_slot_at: Mapped[datetime] = mapped_column(UTCDateTime())
    incumbent_cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    challenger_cycle_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    incumbent_run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    challenger_run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    market_evidence_hash: Mapped[str] = mapped_column(String(64))
    starting_projection_hash: Mapped[str] = mapped_column(String(64))
    pair_hash: Mapped[str] = mapped_column(String(64))


class AgentVersionEvaluationDecisionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_version_evaluation_decisions"
    __table_args__ = (
        UniqueConstraint("evaluation_id", name="uq_agent_version_decisions_evaluation"),
        UniqueConstraint("decision_hash", name="uq_agent_version_decisions_hash"),
        CheckConstraint("status IN ('PASS','FAIL','UNKNOWN')", name="status"),
        CheckConstraint("btrim(reason_code) <> ''", name="reason_code"),
        CheckConstraint(
            f"metric_hash{_HASH_CHECK} AND decision_hash{_HASH_CHECK}", name="hashes"
        ),
        CheckConstraint("jsonb_typeof(metric_report) = 'object'", name="metric_report"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    evaluation_id: Mapped[UUID] = mapped_column(ForeignKey("agent_version_evaluations.id"))
    status: Mapped[str] = mapped_column(String(16))
    metric_report: Mapped[dict[str, object]] = mapped_column(JSONB)
    metric_hash: Mapped[str] = mapped_column(String(64))
    selected_version_source_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_research_runs.id")
    )
    reason_code: Mapped[str] = mapped_column(String(64))
    decision_hash: Mapped[str] = mapped_column(String(64))
