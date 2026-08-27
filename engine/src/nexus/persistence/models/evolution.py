"""Persistence mappings for bounded strategy evolution and internal-paper registry facts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import DECIMAL_NUMERIC, Base, ImmutableCreatedAt, UTCDateTime


class StrategyRNDRunRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_rnd_runs"
    __table_args__ = (
        CheckConstraint("champion_generation >= 1", name="generation"),
        CheckConstraint(
            "status IN ('prepared','running','completed','failed','abstained','rejected')",
            name="status",
        ),
        CheckConstraint(
            "reserved_cost_usd >= 0 AND (settled_cost_usd IS NULL OR "
            "settled_cost_usd BETWEEN 0 AND reserved_cost_usd)",
            name="costs",
        ),
        CheckConstraint(
            "tool_call_count BETWEEN 0 AND 4 AND input_tokens >= 0 AND output_tokens >= 0",
            name="usage",
        ),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="request_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    prompt_version_id: Mapped[UUID] = mapped_column(ForeignKey("agent_prompt_versions.id"))
    model_config_id: Mapped[UUID] = mapped_column(ForeignKey("agent_model_configs.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    champion_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    status: Mapped[str] = mapped_column(String(24))
    reserved_cost_usd: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    settled_cost_usd: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    tool_call_count: Mapped[int] = mapped_column(Integer)
    input_tokens: Mapped[int] = mapped_column(Integer)
    output_tokens: Mapped[int] = mapped_column(Integer)
    request_hash: Mapped[str] = mapped_column(String(64))


class StrategyRNDOutputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_rnd_outputs"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_strategy_rnd_outputs_run"),
        CheckConstraint("schema_version = 'strategy_experiment_proposal_v1'", name="schema"),
        CheckConstraint(
            "validation_status IN ('VALID','REJECTED','ABSTAINED')",
            name="validation_status",
        ),
        CheckConstraint("proposal_hash ~ '^[0-9a-f]{64}$'", name="proposal_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_rnd_runs.id"))
    schema_version: Mapped[str] = mapped_column(String(64))
    proposal: Mapped[dict[str, object]] = mapped_column(JSONB)
    proposal_hash: Mapped[str] = mapped_column(String(64))
    validation_status: Mapped[str] = mapped_column(String(16))
    reason_code: Mapped[str | None] = mapped_column(String(64))


class StrategyEvolutionPolicyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_evolution_policies"
    __table_args__ = (
        UniqueConstraint("policy_kind", "version", name="uq_strategy_evolution_policies_identity"),
        CheckConstraint(
            "policy_kind IN ('candidate_budget','evolution_search_space')", name="kind"
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    policy_kind: Mapped[str] = mapped_column(String(64))
    version: Mapped[str] = mapped_column(String(100))
    document: Mapped[dict[str, object]] = mapped_column(JSONB)
    content_hash: Mapped[str] = mapped_column(String(64))
    published_at: Mapped[datetime] = mapped_column(UTCDateTime())


class StrategyPromotionPolicyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_promotion_policies"
    __table_args__ = (
        UniqueConstraint("version", name="uq_strategy_promotion_policies_version"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    version: Mapped[str] = mapped_column(String(100))
    document: Mapped[dict[str, object]] = mapped_column(JSONB)
    content_hash: Mapped[str] = mapped_column(String(64))
    published_at: Mapped[datetime] = mapped_column(UTCDateTime())


class StrategyResearchLineageRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_research_lineages"
    __table_args__ = (
        UniqueConstraint("lineage_key", name="uq_strategy_research_lineages_key"),
        CheckConstraint("cumulative_oos_trial_count >= 0", name="trial_count"),
        CheckConstraint("status IN ('active','paused','retired')", name="status"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    lineage_key: Mapped[str] = mapped_column(String(100))
    evolution_policy_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_evolution_policies.id"))
    cumulative_oos_trial_count: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16))


class StrategyResearchLineageProjectionRecord(Base):
    __tablename__ = "strategy_research_lineage_projections"
    __table_args__ = (
        CheckConstraint(
            "cumulative_oos_trial_count >= 0 AND version >= 1", name="counters"
        ),
    )

    lineage_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_research_lineages.id", ondelete="CASCADE"), primary_key=True
    )
    cumulative_oos_trial_count: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), server_default=func.now(), onupdate=func.now()
    )


class StrategyExperimentFamilyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_experiment_families"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_strategy_experiment_families_idempotency"),
        CheckConstraint("champion_generation >= 1", name="generation"),
        CheckConstraint("candidate_count BETWEEN 0 AND 24", name="candidate_count"),
        CheckConstraint(
            "admission_status IN ('PROPOSED','REJECTED','ADMITTED')",
            name="admission_status",
        ),
        CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND protocol_hash ~ '^[0-9a-f]{64}$' "
            "AND code_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    research_lineage_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_research_lineages.id"))
    parent_strategy_definition_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_definitions.id")
    )
    rnd_output_id: Mapped[UUID | None] = mapped_column(ForeignKey("strategy_rnd_outputs.id"))
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    admission_status: Mapped[str] = mapped_column(String(32))
    candidate_count: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(100))
    authority_hash: Mapped[str] = mapped_column(String(64))
    protocol_hash: Mapped[str] = mapped_column(String(64))
    code_hash: Mapped[str] = mapped_column(String(64))
    baseline_backtest_run_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("backtest_runs.id")
    )


class StrategyExperimentRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_experiments"
    __table_args__ = (
        UniqueConstraint(
            "family_id", "candidate_ordinal", name="uq_strategy_experiments_family_ordinal"
        ),
        UniqueConstraint(
            "family_id",
            "candidate_strategy_definition_id",
            name="uq_strategy_experiments_family_definition",
        ),
        UniqueConstraint(
            "family_id", "expression_hash", name="uq_strategy_experiments_family_expression"
        ),
        CheckConstraint("candidate_ordinal BETWEEN 1 AND 24", name="candidate_ordinal"),
        CheckConstraint(
            "status IN ('ADMITTED','BACKTEST_RUNNING','BACKTEST_FAILED','BACKTEST_PASSED',"
            "'SHADOW_RUNNING','SHADOW_FAILED','EXPIRED','VETOED','PROMOTION_ELIGIBLE',"
            "'PROMOTED','STALE_CHAMPION_BASELINE')",
            name="status",
        ),
        CheckConstraint(
            "candidate_hash ~ '^[0-9a-f]{64}$' AND expression_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    family_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiment_families.id"))
    candidate_strategy_definition_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_definitions.id")
    )
    candidate_ordinal: Mapped[int] = mapped_column(Integer)
    candidate_delta: Mapped[dict[str, object]] = mapped_column(JSONB)
    candidate_hash: Mapped[str] = mapped_column(String(64))
    expression_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(40))


class StrategyExperimentEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_experiment_events"
    __table_args__ = (
        UniqueConstraint(
            "family_id", "sequence", name="uq_strategy_experiment_events_family_sequence"
        ),
        UniqueConstraint("idempotency_key", name="uq_strategy_experiment_events_idempotency"),
        UniqueConstraint("event_hash", name="uq_strategy_experiment_events_hash"),
        CheckConstraint("sequence >= 1", name="sequence"),
        CheckConstraint("schema_version = 'strategy_experiment_event_v1'", name="schema"),
        CheckConstraint(
            "actor_kind IN ('service','agent','user','scheduler','recovery')", name="actor"
        ),
        CheckConstraint(
            "previous_hash ~ '^[0-9a-f]{64}$' AND event_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    family_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiment_families.id"))
    experiment_id: Mapped[UUID | None] = mapped_column(ForeignKey("strategy_experiments.id"))
    schema_version: Mapped[str] = mapped_column(
        String(64), server_default="strategy_experiment_event_v1"
    )
    sequence: Mapped[int] = mapped_column(BigInteger)
    event_type: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(100))
    actor_kind: Mapped[str] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    previous_hash: Mapped[str] = mapped_column(String(64))
    event_hash: Mapped[str] = mapped_column(String(64))


class StrategyExperimentAttemptRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_experiment_attempts"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id",
            "attempt_kind",
            "attempt_number",
            name="uq_strategy_experiment_attempts_number",
        ),
        CheckConstraint("attempt_number >= 1", name="number"),
        CheckConstraint("attempt_kind IN ('backtest','shadow','promotion')", name="kind"),
        CheckConstraint("status IN ('running','completed','failed')", name="status"),
        CheckConstraint(
            "output_hash IS NULL OR output_hash ~ '^[0-9a-f]{64}$'", name="output_hash"
        ),
        CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    attempt_kind: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16))
    input_hash: Mapped[str] = mapped_column(String(64))
    output_hash: Mapped[str | None] = mapped_column(String(64))
    reason_code: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class StrategyCandidateValidationResultRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_candidate_validation_results"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id",
            name="uq_strategy_candidate_validation_results_experiment",
        ),
        UniqueConstraint("attempt_id", name="uq_strategy_candidate_validation_results_attempt"),
        UniqueConstraint("result_hash", name="uq_strategy_candidate_validation_results_hash"),
        CheckConstraint("validation_cost >= 0 AND valid_window_count >= 0", name="metrics"),
        CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND "
            "protocol_hash ~ '^[0-9a-f]{64}$' AND code_hash ~ '^[0-9a-f]{64}$' "
            "AND result_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    attempt_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiment_attempts.id"))
    validation_sharpe: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    validation_max_drawdown: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    validation_cost: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    valid_window_count: Mapped[int] = mapped_column(Integer)
    authority_complete: Mapped[bool] = mapped_column(Boolean)
    authority_hash: Mapped[str] = mapped_column(String(64))
    protocol_hash: Mapped[str] = mapped_column(String(64))
    code_hash: Mapped[str] = mapped_column(String(64))
    result_hash: Mapped[str] = mapped_column(String(64))
    completed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class StrategyExperimentStateProjectionRecord(Base):
    __tablename__ = "strategy_experiment_state_projections"
    __table_args__ = (
        CheckConstraint(
            "event_sequence >= 0 AND cumulative_oos_trial_count >= 0 AND version >= 1",
            name="counters",
        ),
        CheckConstraint(
            "current_state IN ('PROPOSED','REJECTED','ADMITTED','BACKTEST_RUNNING',"
            "'BACKTEST_FAILED','BACKTEST_PASSED','SHADOW_RUNNING','SHADOW_FAILED','EXPIRED',"
            "'VETOED','PROMOTION_ELIGIBLE','PROMOTED','STALE_CHAMPION_BASELINE')",
            name="state",
        ),
    )

    family_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_experiment_families.id", ondelete="CASCADE"), primary_key=True
    )
    research_lineage_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_research_lineages.id"))
    current_state: Mapped[str] = mapped_column(String(40))
    event_sequence: Mapped[int] = mapped_column(BigInteger)
    cumulative_oos_trial_count: Mapped[int] = mapped_column(BigInteger, server_default="0")
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), server_default=func.now(), onupdate=func.now()
    )


Index(
    "uq_strategy_experiment_state_projections_active_lineage",
    StrategyExperimentStateProjectionRecord.research_lineage_id,
    unique=True,
    postgresql_where=text(
        "current_state NOT IN ('REJECTED','BACKTEST_FAILED','SHADOW_FAILED','EXPIRED',"
        "'VETOED','PROMOTED','STALE_CHAMPION_BASELINE')"
    ),
)


class StrategyExperimentCandidateProjectionRecord(Base):
    __tablename__ = "strategy_experiment_candidate_projections"
    __table_args__ = (
        CheckConstraint("event_sequence >= 1 AND version >= 1", name="counters"),
        CheckConstraint(
            "current_state IN ('ADMITTED','VALIDATION_FAILED','VALIDATION_PASSED',"
            "'OOS_NOT_SELECTED','OOS_SELECTED','OOS_RESERVED','BACKTEST_FAILED',"
            "'BACKTEST_PASSED','SHADOW_RUNNING','SHADOW_FAILED','EXPIRED','VETOED',"
            "'PROMOTION_ELIGIBLE','PROMOTED','STALE_CHAMPION_BASELINE')",
            name="state",
        ),
    )

    experiment_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_experiments.id", ondelete="CASCADE"), primary_key=True
    )
    family_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_experiment_families.id", ondelete="CASCADE")
    )
    current_state: Mapped[str] = mapped_column(String(40))
    event_sequence: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), server_default=func.now(), onupdate=func.now()
    )


Index(
    "ix_strategy_experiment_candidate_projections_family_state",
    StrategyExperimentCandidateProjectionRecord.family_id,
    StrategyExperimentCandidateProjectionRecord.current_state,
)


class StrategyOOSExposureRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_oos_exposures"
    __table_args__ = (
        UniqueConstraint(
            "research_lineage_id",
            "expression_hash",
            name="uq_strategy_oos_exposures_lineage_expression",
        ),
        UniqueConstraint(
            "research_lineage_id", "ordinal", name="uq_strategy_oos_exposures_lineage_ordinal"
        ),
        UniqueConstraint("exposure_hash", name="uq_strategy_oos_exposures_hash"),
        CheckConstraint("ordinal >= 1", name="ordinal"),
        CheckConstraint(
            "expression_hash ~ '^[0-9a-f]{64}$' AND exposure_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    research_lineage_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_research_lineages.id")
    )
    expression_hash: Mapped[str] = mapped_column(String(64))
    ordinal: Mapped[int] = mapped_column(BigInteger)
    first_experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    exposed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    exposure_hash: Mapped[str] = mapped_column(String(64))


class StrategyExperimentOOSExposureLinkRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_experiment_oos_exposure_links"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id", name="uq_strategy_experiment_oos_exposure_links_experiment"
        ),
        UniqueConstraint("link_hash", name="uq_strategy_experiment_oos_exposure_links_hash"),
        CheckConstraint("link_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    exposure_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_oos_exposures.id"))
    link_hash: Mapped[str] = mapped_column(String(64))


class StrategyChampionComparisonRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_champion_comparisons"
    __table_args__ = (
        UniqueConstraint("experiment_id", name="uq_strategy_champion_comparisons_experiment"),
        UniqueConstraint("comparison_hash", name="uq_strategy_champion_comparisons_hash"),
        CheckConstraint(
            "champion_generation >= 1 AND cumulative_oos_trial_count >= 1", name="counters"
        ),
        CheckConstraint(
            "champion_backtest_run_id <> challenger_backtest_run_id", name="distinct_runs"
        ),
        CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND protocol_hash ~ '^[0-9a-f]{64}$' "
            "AND cost_policy_hash ~ '^[0-9a-f]{64}$' "
            "AND comparison_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    champion_backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    challenger_backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    cumulative_oos_trial_count: Mapped[int] = mapped_column(BigInteger)
    metric_report: Mapped[dict[str, object]] = mapped_column(JSONB)
    authority_hash: Mapped[str] = mapped_column(String(64))
    protocol_hash: Mapped[str] = mapped_column(String(64))
    cost_policy_hash: Mapped[str] = mapped_column(String(64))
    comparison_hash: Mapped[str] = mapped_column(String(64))


class StrategyOOSEvidenceLinkRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_oos_evidence_links"
    __table_args__ = (
        UniqueConstraint("experiment_id", name="uq_strategy_oos_evidence_links_experiment"),
        UniqueConstraint("comparison_id", name="uq_strategy_oos_evidence_links_comparison"),
        UniqueConstraint("evidence_hash", name="uq_strategy_oos_evidence_links_hash"),
        CheckConstraint(
            "champion_backtest_run_id <> challenger_backtest_run_id", name="distinct_runs"
        ),
        CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND evidence_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    exposure_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_oos_exposures.id"))
    comparison_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_champion_comparisons.id"))
    champion_backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    challenger_backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    authority_hash: Mapped[str] = mapped_column(String(64))
    evidence_hash: Mapped[str] = mapped_column(String(64))


class StrategyPromotionDecisionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_promotion_decisions"
    __table_args__ = (
        UniqueConstraint(
            "comparison_id",
            "stage",
            name="uq_strategy_promotion_decisions_comparison_stage",
        ),
        UniqueConstraint("decision_hash", name="uq_strategy_promotion_decisions_hash"),
        CheckConstraint("stage IN ('historical','shadow','handover')", name="stage"),
        CheckConstraint("status IN ('PASS','FAIL','UNKNOWN')", name="status"),
        CheckConstraint("decision_hash ~ '^[0-9a-f]{64}$'", name="decision_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    comparison_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_champion_comparisons.id"))
    promotion_policy_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_promotion_policies.id"))
    stage: Mapped[str] = mapped_column(String(24))
    status: Mapped[str] = mapped_column(String(16))
    gate_report: Mapped[dict[str, object]] = mapped_column(JSONB)
    reason_code: Mapped[str] = mapped_column(String(64))
    decision_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowGroupRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_groups"
    __table_args__ = (
        UniqueConstraint("experiment_id", name="uq_strategy_shadow_groups_experiment"),
        UniqueConstraint("idempotency_key", name="uq_strategy_shadow_groups_idempotency"),
        CheckConstraint("champion_generation >= 1", name="generation"),
        CheckConstraint("evaluation_sessions = 63 AND grace_sessions = 5", name="session_budgets"),
        CheckConstraint("initial_nav > 0", name="initial_nav"),
        CheckConstraint(
            "champion_strategy_definition_id <> challenger_strategy_definition_id",
            name="distinct_definitions",
        ),
        CheckConstraint("status IN ('running','failed','finalized','stale')", name="status"),
        CheckConstraint("authority_hash ~ '^[0-9a-f]{64}$'", name="authority_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    experiment_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_experiments.id"))
    historical_promotion_decision_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_promotion_decisions.id")
    )
    champion_strategy_definition_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_definitions.id")
    )
    challenger_strategy_definition_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_definitions.id")
    )
    baseline_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("paper_performance_snapshots.id"))
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(32))
    evaluation_sessions: Mapped[int] = mapped_column(Integer)
    grace_sessions: Mapped[int] = mapped_column(Integer)
    initial_nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    idempotency_key: Mapped[str] = mapped_column(String(100))
    authority_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowLegRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_legs"
    __table_args__ = (
        UniqueConstraint("group_id", "role", name="uq_strategy_shadow_legs_group_role"),
        CheckConstraint("role IN ('champion','challenger')", name="role"),
        CheckConstraint("initial_state_hash ~ '^[0-9a-f]{64}$'", name="initial_state_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    group_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_groups.id"))
    role: Mapped[str] = mapped_column(String(16))
    strategy_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    initial_state_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowAdvanceAttemptRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_advance_attempts"
    __table_args__ = (
        UniqueConstraint(
            "group_id",
            "session",
            "attempt_number",
            name="uq_strategy_shadow_advance_attempts_number",
        ),
        CheckConstraint("attempt_number >= 1", name="number"),
        CheckConstraint("status IN ('applied','failed','duplicate')", name="status"),
        CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    group_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_groups.id"))
    session: Mapped[date] = mapped_column(Date)
    attempt_number: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    reason_code: Mapped[str | None] = mapped_column(String(64))
    input_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class StrategyShadowSessionAuthorityRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_session_authorities"
    __table_args__ = (
        UniqueConstraint(
            "group_id",
            "session",
            name="uq_strategy_shadow_session_authorities_group_session",
        ),
        UniqueConstraint(
            "group_id",
            "session_ordinal",
            name="uq_strategy_shadow_session_authorities_group_ordinal",
        ),
        CheckConstraint("session_ordinal BETWEEN 1 AND 68", name="ordinal"),
        CheckConstraint("authority_hash ~ '^[0-9a-f]{64}$'", name="authority_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    group_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_groups.id"))
    session: Mapped[date] = mapped_column(Date)
    session_ordinal: Mapped[int] = mapped_column(Integer)
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    source_policy_id: Mapped[UUID] = mapped_column(ForeignKey("source_policies.id"))
    cost_policy_id: Mapped[UUID] = mapped_column(ForeignKey("cost_policies.id"))
    authority_hash: Mapped[str] = mapped_column(String(64))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())


class StrategyShadowSessionPriceInputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_session_price_inputs"
    __table_args__ = (
        UniqueConstraint(
            "session_authority_id",
            "instrument_id",
            name="uq_strategy_shadow_session_price_inputs_instrument",
        ),
        CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    session_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_shadow_session_authorities.id")
    )
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    raw_price_authority_id: Mapped[UUID] = mapped_column(ForeignKey("price_bar_set_authorities.id"))
    adjusted_price_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_bar_set_authorities.id")
    )
    input_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowLegStateRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_leg_states"
    __table_args__ = (
        UniqueConstraint("leg_id", "session_ordinal", name="uq_strategy_shadow_leg_states_ordinal"),
        UniqueConstraint(
            "leg_id",
            "session_authority_id",
            name="uq_strategy_shadow_leg_states_authority",
        ),
        CheckConstraint(
            "session_ordinal BETWEEN 0 AND 68 AND cash >= 0 AND nav >= 0", name="values"
        ),
        CheckConstraint(
            "state_hash ~ '^[0-9a-f]{64}$' AND transition_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    leg_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_legs.id"))
    session_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("strategy_shadow_session_authorities.id")
    )
    session_ordinal: Mapped[int] = mapped_column(Integer)
    cash: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    positions: Mapped[dict[str, object]] = mapped_column(JSONB)
    hard_risk_violation: Mapped[bool] = mapped_column(Boolean)
    state_hash: Mapped[str] = mapped_column(String(64))
    transition_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowOrderRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_orders"
    __table_args__ = (
        UniqueConstraint("order_hash", name="uq_strategy_shadow_orders_hash"),
        CheckConstraint("side IN ('BUY','SELL')", name="side"),
        CheckConstraint("quantity > 0", name="quantity"),
        CheckConstraint(
            "status IN ('FILLED','PARTIALLY_FILLED','REJECTED','CANCELLED','EXPIRED')",
            name="status",
        ),
        CheckConstraint("order_hash ~ '^[0-9a-f]{64}$'", name="order_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    leg_state_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_leg_states.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    side: Mapped[str] = mapped_column(String(4))
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    status: Mapped[str] = mapped_column(String(24))
    reason_code: Mapped[str | None] = mapped_column(String(64))
    order_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowFillRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_fills"
    __table_args__ = (
        UniqueConstraint("fill_hash", name="uq_strategy_shadow_fills_hash"),
        CheckConstraint("quantity > 0 AND price > 0 AND fees >= 0", name="values"),
        CheckConstraint("fill_hash ~ '^[0-9a-f]{64}$'", name="fill_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_orders.id"))
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    fees: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    filled_at: Mapped[datetime] = mapped_column(UTCDateTime())
    fill_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowCheckpointRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "group_id",
            "checkpoint_ordinal",
            name="uq_strategy_shadow_checkpoints_ordinal",
        ),
        CheckConstraint("checkpoint_ordinal IN (5,21,63)", name="ordinal"),
        CheckConstraint("authority_hash ~ '^[0-9a-f]{64}$'", name="authority_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    group_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_groups.id"))
    checkpoint_ordinal: Mapped[int] = mapped_column(Integer)
    session: Mapped[date] = mapped_column(Date)
    authority_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowCheckpointMetricRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_checkpoint_metrics"
    __table_args__ = (
        UniqueConstraint(
            "checkpoint_id",
            "leg_id",
            name="uq_strategy_shadow_checkpoint_metrics_leg",
        ),
        CheckConstraint("metric_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    checkpoint_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_checkpoints.id"))
    leg_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_legs.id"))
    metric_report: Mapped[dict[str, object]] = mapped_column(JSONB)
    metric_hash: Mapped[str] = mapped_column(String(64))


class StrategyShadowFinalizationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_shadow_finalizations"
    __table_args__ = (
        UniqueConstraint("group_id", name="uq_strategy_shadow_finalizations_group"),
        UniqueConstraint("finalization_hash", name="uq_strategy_shadow_finalizations_hash"),
        CheckConstraint("status IN ('PASS','FAIL','UNKNOWN','STALE')", name="status"),
        CheckConstraint(
            "final_authority_hash ~ '^[0-9a-f]{64}$' AND finalization_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    group_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_shadow_groups.id"))
    promotion_policy_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_promotion_policies.id"))
    status: Mapped[str] = mapped_column(String(16))
    gate_report: Mapped[dict[str, object]] = mapped_column(JSONB)
    reason_code: Mapped[str] = mapped_column(String(64))
    final_authority_hash: Mapped[str] = mapped_column(String(64))
    finalization_hash: Mapped[str] = mapped_column(String(64))


class StrategyRegistryEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_registry_events"
    __table_args__ = (
        UniqueConstraint(
            "channel", "sequence", name="uq_strategy_registry_events_channel_sequence"
        ),
        UniqueConstraint("idempotency_key", name="uq_strategy_registry_events_idempotency"),
        UniqueConstraint("event_hash", name="uq_strategy_registry_events_hash"),
        CheckConstraint("channel = 'internal_paper'", name="channel"),
        CheckConstraint("sequence >= 1", name="sequence"),
        CheckConstraint("schema_version = 'strategy_registry_event_v1'", name="schema"),
        CheckConstraint(
            "event_type IN ('GENESIS','PROMOTE','ROLLBACK','PAUSE','RESUME','VETO')",
            name="event_type",
        ),
        CheckConstraint(
            "actor_kind IN ('migration','service','user','scheduler','recovery')",
            name="actor",
        ),
        CheckConstraint("champion_generation >= 1", name="generation"),
        CheckConstraint(
            "previous_hash ~ '^[0-9a-f]{64}$' AND event_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    channel: Mapped[str] = mapped_column(String(32))
    schema_version: Mapped[str] = mapped_column(String(64))
    sequence: Mapped[int] = mapped_column(BigInteger)
    event_type: Mapped[str] = mapped_column(String(32))
    idempotency_key: Mapped[str] = mapped_column(String(100))
    actor_kind: Mapped[str] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    from_definition_id: Mapped[UUID | None] = mapped_column(ForeignKey("strategy_definitions.id"))
    to_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    promotion_decision_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("strategy_promotion_decisions.id")
    )
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    effective_session: Mapped[date | None] = mapped_column(Date)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    previous_hash: Mapped[str] = mapped_column(String(64))
    event_hash: Mapped[str] = mapped_column(String(64))


class StrategyRegistryActivationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_registry_activations"
    __table_args__ = (
        UniqueConstraint("registry_event_id", name="uq_strategy_registry_activations_event"),
        UniqueConstraint("activation_hash", name="uq_strategy_registry_activations_hash"),
        CheckConstraint("champion_generation >= 2", name="generation"),
        CheckConstraint("from_definition_id <> to_definition_id", name="distinct_definitions"),
        CheckConstraint("activation_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    registry_event_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_registry_events.id"))
    decision_session: Mapped[date] = mapped_column(Date)
    activated_at: Mapped[datetime] = mapped_column(UTCDateTime())
    from_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    to_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    champion_generation: Mapped[int] = mapped_column(BigInteger)
    activation_hash: Mapped[str] = mapped_column(String(64))


class StrategyRegistryHeadRecord(Base):
    __tablename__ = "strategy_registry_heads"
    __table_args__ = (
        CheckConstraint("channel = 'internal_paper'", name="channel"),
        CheckConstraint(
            "active_generation >= 1 AND event_sequence >= 1 AND version >= 1",
            name="counters",
        ),
        CheckConstraint(
            "(pending_event_id IS NULL) = (pending_effective_session IS NULL)",
            name="pending_binding",
        ),
    )

    channel: Mapped[str] = mapped_column(String(32), primary_key=True)
    active_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    active_generation: Mapped[int] = mapped_column(BigInteger)
    last_event_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_registry_events.id"))
    event_sequence: Mapped[int] = mapped_column(BigInteger)
    auto_promotion_paused: Mapped[bool] = mapped_column(Boolean)
    pending_event_id: Mapped[UUID | None] = mapped_column(ForeignKey("strategy_registry_events.id"))
    pending_effective_session: Mapped[date | None] = mapped_column(Date)
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), server_default=func.now(), onupdate=func.now()
    )
