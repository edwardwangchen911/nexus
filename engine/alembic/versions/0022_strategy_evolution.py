"""Add bounded strategy-evolution authorities.

Revision ID: 0022_strategy_evolution
Revises: 0021_manual_job_retry_allowance
Create Date: 2026-08-27
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0022_strategy_evolution"
down_revision: str | Sequence[str] | None = "0021_manual_job_retry_allowance"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_BASELINE_DEFINITION_ID = "019d0000-0000-7000-8000-000000002200"
_GENESIS_EVENT_ID = "019d0000-0000-7000-8000-000000002201"
_CANDIDATE_POLICY_ID = "019d0000-0000-7000-8000-000000002202"
_SEARCH_POLICY_ID = "019d0000-0000-7000-8000-000000002203"
_PROMOTION_POLICY_ID = "019d0000-0000-7000-8000-000000002204"
_BASELINE_EXPRESSION_HASH = "f643694ad6d31f0dd504061fbd06068a93f9fb679aba9ba5b75337e2e169d13d"

_BASELINE_DOCUMENT: dict[str, Any] = {
    "version": "baseline_v1",
    "universe": {
        "key": "us_liquid",
        "snapshot_selection": "latest_published_effective",
        "eligibility_status": "PASS",
        "asset_types": ["stock", "etf"],
        "etf_sector_exposure_required": True,
    },
    "root": {
        "op": "all",
        "args": [
            {"op": "gte", "feature": "adv20_usd", "value": "50000000"},
            {"op": "gte", "feature": "raw_close", "value": "5"},
            {"op": "gt", "feature": "adjusted_close", "right_feature": "sma200"},
            {"op": "gt", "feature": "return_126", "value": "0"},
        ],
    },
    "ranking": {
        "feature": "momentum_126_skip5",
        "direction": "desc",
        "select_count": 6,
        "exit_buffer_rank": 20,
        "exit_after_consecutive_evaluations": 2,
    },
    "rebalance": {
        "frequency": "weekly",
        "decision_session": "last_exchange_session_of_iso_week",
        "fill_timing": "next_exchange_session_open",
    },
    "holding_period": {
        "minimum_sessions": 0,
        "maximum_sessions": 10000,
        "exit_evaluation": "scheduled_and_event_driven",
    },
    "portfolio": {
        "max_positions": 6,
        "max_etfs": 2,
        "max_stocks_per_sector": 2,
        "sector_taxonomy": "NEXUS_SECTOR_V1",
        "max_sector_weight": "0.30",
        "min_cash_weight": "0.25",
    },
    "exits": {
        "trend_rule": {
            "op": "gt",
            "feature": "adjusted_close",
            "right_feature": "sma200",
        },
        "ranking_buffer_enabled": True,
        "hard_risk_exit": "forward_only",
    },
    "parameters": [
        {
            "name": "min_adv20_usd",
            "policy_ref": "$.root.args[0].value",
            "value": "50000000",
            "minimum": "10000000",
            "maximum": "200000000",
        },
        {
            "name": "min_close",
            "policy_ref": "$.root.args[1].value",
            "value": "5",
            "minimum": "1",
            "maximum": "100",
        },
        {
            "name": "target_weight",
            "policy_ref": "$.target_weight.weight",
            "value": "0.125",
            "minimum": "0.05",
            "maximum": "0.20",
        },
    ],
    "target_weight": {"method": "fixed_equal_weight", "weight": "0.125"},
}

_CANDIDATE_POLICY = {
    "version": "candidate_budget_policy_v1",
    "max_axes": 3,
    "max_candidates": 24,
    "max_oos_candidates": 3,
}
_SEARCH_POLICY = {
    "version": "evolution_search_space_v1",
    "axes": [
        {
            "axis": "min_adv20_usd",
            "value_kind": "decimal_string",
            "allowed_values": ["25000000", "50000000", "75000000", "100000000"],
        },
        {
            "axis": "min_close",
            "value_kind": "decimal_string",
            "allowed_values": ["3", "5", "10"],
        },
        {
            "axis": "target_weight",
            "value_kind": "decimal_string",
            "allowed_values": ["0.075", "0.10", "0.125"],
        },
        {
            "axis": "ranking_feature",
            "value_kind": "feature_name",
            "allowed_values": ["momentum_126_skip5", "return_126"],
        },
        {
            "axis": "positive_return_filter_enabled",
            "value_kind": "boolean",
            "allowed_values": [False, True],
        },
    ],
}
_PROMOTION_POLICY = {
    "version": "promotion_policy_v1",
    "minimum_oos_windows": 8,
    "minimum_deflated_sharpe_confidence": "0.95",
    "minimum_sharpe_improvement": "0.15",
    "minimum_annual_return_improvement": "0.01",
    "maximum_drawdown_degradation": "0.02",
    "cost_stress_multiplier": "2",
}

_TERMINAL_FAMILY_STATES = (
    "REJECTED",
    "BACKTEST_FAILED",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
)

_AUTHORITY_TABLES = (
    "strategy_rnd_runs",
    "strategy_rnd_outputs",
    "strategy_evolution_policies",
    "strategy_promotion_policies",
    "strategy_research_lineages",
    "strategy_experiment_families",
    "strategy_experiments",
    "strategy_experiment_events",
    "strategy_experiment_attempts",
    "strategy_champion_comparisons",
    "strategy_promotion_decisions",
    "strategy_shadow_groups",
    "strategy_shadow_legs",
    "strategy_shadow_advance_attempts",
    "strategy_shadow_session_authorities",
    "strategy_shadow_session_price_inputs",
    "strategy_shadow_leg_states",
    "strategy_shadow_orders",
    "strategy_shadow_fills",
    "strategy_shadow_checkpoints",
    "strategy_shadow_checkpoint_metrics",
    "strategy_shadow_finalizations",
    "strategy_registry_events",
)

_EVOLUTION_TABLES_DROP_ORDER = (
    "strategy_registry_heads",
    "strategy_registry_events",
    "strategy_shadow_finalizations",
    "strategy_shadow_checkpoint_metrics",
    "strategy_shadow_checkpoints",
    "strategy_shadow_fills",
    "strategy_shadow_orders",
    "strategy_shadow_leg_states",
    "strategy_shadow_session_price_inputs",
    "strategy_shadow_session_authorities",
    "strategy_shadow_advance_attempts",
    "strategy_shadow_legs",
    "strategy_shadow_groups",
    "strategy_promotion_decisions",
    "strategy_champion_comparisons",
    "strategy_experiment_state_projections",
    "strategy_experiment_attempts",
    "strategy_experiment_events",
    "strategy_experiments",
    "strategy_experiment_families",
    "strategy_research_lineages",
    "strategy_promotion_policies",
    "strategy_evolution_policies",
    "strategy_rnd_outputs",
    "strategy_rnd_runs",
)


def _uuid(name: str, *, nullable: bool = False) -> sa.Column[Any]:
    return sa.Column(name, sa.UUID(), nullable=nullable)


def _text(name: str, length: int, *, nullable: bool = False) -> sa.Column[Any]:
    return sa.Column(name, sa.String(length=length), nullable=nullable)


def _json(name: str, *, nullable: bool = False) -> sa.Column[Any]:
    return sa.Column(name, postgresql.JSONB(astext_type=sa.Text()), nullable=nullable)


def _utc(name: str, *, nullable: bool = False) -> sa.Column[Any]:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def _created_at() -> sa.Column[Any]:
    return sa.Column(
        "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def _hash_check(*columns: str, name: str = "hashes_canonical") -> sa.CheckConstraint:
    predicate = " AND ".join(f"{column} ~ '^[0-9a-f]{{64}}$'" for column in columns)
    return sa.CheckConstraint(predicate, name=op.f(name))


def _json_literal(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha256(value: object) -> str:
    return hashlib.sha256(_json_literal(value).encode()).hexdigest()


def _pk() -> sa.PrimaryKeyConstraint:
    return sa.PrimaryKeyConstraint("id")


def _fk(column: str, table: str, *, ondelete: str | None = None) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint([column], [f"{table}.id"], ondelete=ondelete)


def upgrade() -> None:
    """Create bounded evolution facts, seed genesis, and bind Forward decisions."""
    _create_agent_and_policy_tables()
    _create_experiment_tables()
    _create_comparison_and_shadow_tables()
    _create_registry_tables()
    _seed_policies_and_registry()
    _bind_forward_decisions()
    _extend_strategy_rnd_prompt_role()
    _install_database_guards()


def downgrade() -> None:
    """Remove Phase 2 only when no non-seed facts can be lost."""
    _refuse_downgrade_with_phase2_facts()
    _drop_database_guards()
    _restore_strategy_rnd_prompt_role()
    _unbind_forward_decisions()
    for table_name in _EVOLUTION_TABLES_DROP_ORDER:
        op.drop_table(table_name)


def _create_agent_and_policy_tables() -> None:
    op.create_table(
        "strategy_rnd_runs",
        _uuid("id"),
        _uuid("prompt_version_id"),
        _uuid("model_config_id"),
        _uuid("data_snapshot_id"),
        _uuid("champion_definition_id"),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        _utc("as_of"),
        _text("status", 24),
        sa.Column("reserved_cost_usd", sa.Numeric(38, 12), nullable=False),
        sa.Column("settled_cost_usd", sa.Numeric(38, 12), nullable=True),
        sa.Column("tool_call_count", sa.Integer(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        _text("request_hash", 64),
        _created_at(),
        _pk(),
        _fk("prompt_version_id", "agent_prompt_versions"),
        _fk("model_config_id", "agent_model_configs"),
        _fk("data_snapshot_id", "data_snapshots"),
        _fk("champion_definition_id", "strategy_definitions"),
        sa.CheckConstraint(
            "champion_generation >= 1", name=op.f("ck_strategy_rnd_runs_generation")
        ),
        sa.CheckConstraint(
            "status IN ('prepared','running','completed','failed','abstained','rejected')",
            name=op.f("ck_strategy_rnd_runs_status"),
        ),
        sa.CheckConstraint(
            "reserved_cost_usd >= 0 AND (settled_cost_usd IS NULL OR "
            "settled_cost_usd BETWEEN 0 AND reserved_cost_usd)",
            name=op.f("ck_strategy_rnd_runs_costs"),
        ),
        sa.CheckConstraint(
            "tool_call_count BETWEEN 0 AND 4 AND input_tokens >= 0 AND output_tokens >= 0",
            name=op.f("ck_strategy_rnd_runs_usage"),
        ),
        _hash_check("request_hash", name="ck_strategy_rnd_runs_request_hash"),
    )
    op.create_table(
        "strategy_rnd_outputs",
        _uuid("id"),
        _uuid("run_id"),
        _text("schema_version", 64),
        _json("proposal"),
        _text("proposal_hash", 64),
        _text("validation_status", 16),
        _text("reason_code", 64, nullable=True),
        _created_at(),
        _pk(),
        _fk("run_id", "strategy_rnd_runs"),
        sa.UniqueConstraint("run_id", name="uq_strategy_rnd_outputs_run"),
        sa.CheckConstraint(
            "schema_version = 'strategy_experiment_proposal_v1'",
            name=op.f("ck_strategy_rnd_outputs_schema"),
        ),
        sa.CheckConstraint(
            "validation_status IN ('VALID','REJECTED','ABSTAINED')",
            name=op.f("ck_strategy_rnd_outputs_validation_status"),
        ),
        _hash_check("proposal_hash", name="ck_strategy_rnd_outputs_proposal_hash"),
    )
    op.create_table(
        "strategy_evolution_policies",
        _uuid("id"),
        _text("policy_kind", 64),
        _text("version", 100),
        _json("document"),
        _text("content_hash", 64),
        _utc("published_at"),
        _created_at(),
        _pk(),
        sa.UniqueConstraint(
            "policy_kind", "version", name="uq_strategy_evolution_policies_identity"
        ),
        sa.CheckConstraint(
            "policy_kind IN ('candidate_budget','evolution_search_space')",
            name=op.f("ck_strategy_evolution_policies_kind"),
        ),
        _hash_check("content_hash", name="ck_strategy_evolution_policies_content_hash"),
    )
    op.create_table(
        "strategy_promotion_policies",
        _uuid("id"),
        _text("version", 100),
        _json("document"),
        _text("content_hash", 64),
        _utc("published_at"),
        _created_at(),
        _pk(),
        sa.UniqueConstraint("version", name="uq_strategy_promotion_policies_version"),
        _hash_check("content_hash", name="ck_strategy_promotion_policies_content_hash"),
    )


def _create_experiment_tables() -> None:
    op.create_table(
        "strategy_research_lineages",
        _uuid("id"),
        _text("lineage_key", 100),
        _uuid("evolution_policy_id"),
        sa.Column("cumulative_oos_trial_count", sa.BigInteger(), nullable=False),
        _text("status", 16),
        _created_at(),
        _pk(),
        _fk("evolution_policy_id", "strategy_evolution_policies"),
        sa.UniqueConstraint("lineage_key", name="uq_strategy_research_lineages_key"),
        sa.CheckConstraint(
            "cumulative_oos_trial_count >= 0",
            name=op.f("ck_strategy_research_lineages_trial_count"),
        ),
        sa.CheckConstraint(
            "status IN ('active','paused','retired')",
            name=op.f("ck_strategy_research_lineages_status"),
        ),
    )
    op.create_table(
        "strategy_experiment_families",
        _uuid("id"),
        _uuid("research_lineage_id"),
        _uuid("parent_strategy_definition_id"),
        _uuid("rnd_output_id", nullable=True),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        _text("admission_status", 32),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        _text("idempotency_key", 100),
        _text("authority_hash", 64),
        _text("protocol_hash", 64),
        _text("code_hash", 64),
        _created_at(),
        _pk(),
        _fk("research_lineage_id", "strategy_research_lineages"),
        _fk("parent_strategy_definition_id", "strategy_definitions"),
        _fk("rnd_output_id", "strategy_rnd_outputs"),
        sa.UniqueConstraint("idempotency_key", name="uq_strategy_experiment_families_idempotency"),
        sa.CheckConstraint(
            "champion_generation >= 1", name=op.f("ck_strategy_experiment_families_generation")
        ),
        sa.CheckConstraint(
            "candidate_count BETWEEN 0 AND 24",
            name=op.f("ck_strategy_experiment_families_candidate_count"),
        ),
        sa.CheckConstraint(
            "admission_status IN ('PROPOSED','REJECTED','ADMITTED')",
            name=op.f("ck_strategy_experiment_families_admission_status"),
        ),
        _hash_check(
            "authority_hash",
            "protocol_hash",
            "code_hash",
            name="ck_strategy_experiment_families_hashes",
        ),
    )
    op.create_table(
        "strategy_experiments",
        _uuid("id"),
        _uuid("family_id"),
        _uuid("candidate_strategy_definition_id"),
        sa.Column("candidate_ordinal", sa.Integer(), nullable=False),
        _json("candidate_delta"),
        _text("candidate_hash", 64),
        _text("expression_hash", 64),
        _text("status", 40),
        _created_at(),
        _pk(),
        _fk("family_id", "strategy_experiment_families"),
        _fk("candidate_strategy_definition_id", "strategy_definitions"),
        sa.UniqueConstraint(
            "family_id", "candidate_ordinal", name="uq_strategy_experiments_family_ordinal"
        ),
        sa.UniqueConstraint(
            "family_id",
            "candidate_strategy_definition_id",
            name="uq_strategy_experiments_family_definition",
        ),
        sa.UniqueConstraint(
            "family_id", "expression_hash", name="uq_strategy_experiments_family_expression"
        ),
        sa.CheckConstraint(
            "candidate_ordinal BETWEEN 1 AND 24",
            name=op.f("ck_strategy_experiments_candidate_ordinal"),
        ),
        sa.CheckConstraint(
            "status IN ('ADMITTED','BACKTEST_RUNNING','BACKTEST_FAILED','BACKTEST_PASSED',"
            "'SHADOW_RUNNING','SHADOW_FAILED','EXPIRED','VETOED','PROMOTION_ELIGIBLE',"
            "'PROMOTED','STALE_CHAMPION_BASELINE')",
            name=op.f("ck_strategy_experiments_status"),
        ),
        _hash_check("candidate_hash", "expression_hash", name="ck_strategy_experiments_hashes"),
    )
    op.create_table(
        "strategy_experiment_events",
        _uuid("id"),
        _uuid("family_id"),
        sa.Column(
            "schema_version",
            sa.String(length=64),
            server_default="strategy_experiment_event_v1",
            nullable=False,
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        _text("event_type", 64),
        _text("idempotency_key", 100),
        _text("actor_kind", 32),
        _utc("occurred_at"),
        _json("payload"),
        _text("previous_hash", 64),
        _text("event_hash", 64),
        _created_at(),
        _pk(),
        _fk("family_id", "strategy_experiment_families"),
        sa.UniqueConstraint(
            "family_id", "sequence", name="uq_strategy_experiment_events_family_sequence"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_strategy_experiment_events_idempotency"),
        sa.UniqueConstraint("event_hash", name="uq_strategy_experiment_events_hash"),
        sa.CheckConstraint("sequence >= 1", name=op.f("ck_strategy_experiment_events_sequence")),
        sa.CheckConstraint(
            "schema_version = 'strategy_experiment_event_v1'",
            name=op.f("ck_strategy_experiment_events_schema"),
        ),
        sa.CheckConstraint(
            "actor_kind IN ('service','agent','user','scheduler','recovery')",
            name=op.f("ck_strategy_experiment_events_actor"),
        ),
        _hash_check("previous_hash", "event_hash", name="ck_strategy_experiment_events_hashes"),
    )
    op.create_table(
        "strategy_experiment_attempts",
        _uuid("id"),
        _uuid("experiment_id"),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        _text("attempt_kind", 32),
        _text("status", 16),
        _text("input_hash", 64),
        _text("output_hash", 64, nullable=True),
        _text("reason_code", 64, nullable=True),
        _utc("started_at"),
        _utc("completed_at", nullable=True),
        _created_at(),
        _pk(),
        _fk("experiment_id", "strategy_experiments"),
        sa.UniqueConstraint(
            "experiment_id",
            "attempt_kind",
            "attempt_number",
            name="uq_strategy_experiment_attempts_number",
        ),
        sa.CheckConstraint(
            "attempt_number >= 1", name=op.f("ck_strategy_experiment_attempts_number")
        ),
        sa.CheckConstraint(
            "attempt_kind IN ('backtest','shadow','promotion')",
            name=op.f("ck_strategy_experiment_attempts_kind"),
        ),
        sa.CheckConstraint(
            "status IN ('running','completed','failed')",
            name=op.f("ck_strategy_experiment_attempts_status"),
        ),
        sa.CheckConstraint(
            "output_hash IS NULL OR output_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_experiment_attempts_output_hash"),
        ),
        _hash_check("input_hash", name="ck_strategy_experiment_attempts_input_hash"),
    )
    op.create_table(
        "strategy_experiment_state_projections",
        _uuid("family_id"),
        _uuid("research_lineage_id"),
        _text("current_state", 40),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column(
            "cumulative_oos_trial_count", sa.BigInteger(), server_default="0", nullable=False
        ),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("family_id"),
        _fk("family_id", "strategy_experiment_families", ondelete="CASCADE"),
        _fk("research_lineage_id", "strategy_research_lineages"),
        sa.CheckConstraint(
            "event_sequence >= 0 AND cumulative_oos_trial_count >= 0 AND version >= 1",
            name=op.f("ck_strategy_experiment_state_projections_counters"),
        ),
        sa.CheckConstraint(
            "current_state IN ('PROPOSED','REJECTED','ADMITTED','BACKTEST_RUNNING',"
            "'BACKTEST_FAILED','BACKTEST_PASSED','SHADOW_RUNNING','SHADOW_FAILED','EXPIRED',"
            "'VETOED','PROMOTION_ELIGIBLE','PROMOTED','STALE_CHAMPION_BASELINE')",
            name=op.f("ck_strategy_experiment_state_projections_state"),
        ),
    )
    terminal_sql = ",".join(f"'{state}'" for state in _TERMINAL_FAMILY_STATES)
    op.create_index(
        "uq_strategy_experiment_state_projections_active_lineage",
        "strategy_experiment_state_projections",
        ["research_lineage_id"],
        unique=True,
        postgresql_where=sa.text(f"current_state NOT IN ({terminal_sql})"),
    )


def _create_comparison_and_shadow_tables() -> None:
    op.create_table(
        "strategy_champion_comparisons",
        _uuid("id"),
        _uuid("experiment_id"),
        _uuid("champion_backtest_run_id"),
        _uuid("challenger_backtest_run_id"),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        sa.Column("cumulative_oos_trial_count", sa.BigInteger(), nullable=False),
        _json("metric_report"),
        _text("authority_hash", 64),
        _text("protocol_hash", 64),
        _text("cost_policy_hash", 64),
        _text("comparison_hash", 64),
        _created_at(),
        _pk(),
        _fk("experiment_id", "strategy_experiments"),
        _fk("champion_backtest_run_id", "backtest_runs"),
        _fk("challenger_backtest_run_id", "backtest_runs"),
        sa.UniqueConstraint("experiment_id", name="uq_strategy_champion_comparisons_experiment"),
        sa.UniqueConstraint("comparison_hash", name="uq_strategy_champion_comparisons_hash"),
        sa.CheckConstraint(
            "champion_generation >= 1 AND cumulative_oos_trial_count >= 1",
            name=op.f("ck_strategy_champion_comparisons_counters"),
        ),
        sa.CheckConstraint(
            "champion_backtest_run_id <> challenger_backtest_run_id",
            name=op.f("ck_strategy_champion_comparisons_distinct_runs"),
        ),
        _hash_check(
            "authority_hash",
            "protocol_hash",
            "cost_policy_hash",
            "comparison_hash",
            name="ck_strategy_champion_comparisons_hashes",
        ),
    )
    op.create_table(
        "strategy_promotion_decisions",
        _uuid("id"),
        _uuid("comparison_id"),
        _uuid("promotion_policy_id"),
        _text("stage", 24),
        _text("status", 16),
        _json("gate_report"),
        _text("reason_code", 64),
        _text("decision_hash", 64),
        _created_at(),
        _pk(),
        _fk("comparison_id", "strategy_champion_comparisons"),
        _fk("promotion_policy_id", "strategy_promotion_policies"),
        sa.UniqueConstraint(
            "comparison_id", "stage", name="uq_strategy_promotion_decisions_comparison_stage"
        ),
        sa.UniqueConstraint("decision_hash", name="uq_strategy_promotion_decisions_hash"),
        sa.CheckConstraint(
            "stage IN ('historical','shadow','handover')",
            name=op.f("ck_strategy_promotion_decisions_stage"),
        ),
        sa.CheckConstraint(
            "status IN ('PASS','FAIL','UNKNOWN')",
            name=op.f("ck_strategy_promotion_decisions_status"),
        ),
        _hash_check("decision_hash", name="ck_strategy_promotion_decisions_decision_hash"),
    )
    op.create_table(
        "strategy_shadow_groups",
        _uuid("id"),
        _uuid("experiment_id"),
        _uuid("historical_promotion_decision_id"),
        _uuid("champion_strategy_definition_id"),
        _uuid("challenger_strategy_definition_id"),
        _uuid("baseline_snapshot_id"),
        _uuid("calendar_authority_id"),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        _text("status", 32),
        sa.Column("evaluation_sessions", sa.Integer(), nullable=False),
        sa.Column("grace_sessions", sa.Integer(), nullable=False),
        sa.Column("initial_nav", sa.Numeric(38, 12), nullable=False),
        _text("idempotency_key", 100),
        _text("authority_hash", 64),
        _created_at(),
        _pk(),
        _fk("experiment_id", "strategy_experiments"),
        _fk("historical_promotion_decision_id", "strategy_promotion_decisions"),
        _fk("champion_strategy_definition_id", "strategy_definitions"),
        _fk("challenger_strategy_definition_id", "strategy_definitions"),
        _fk("baseline_snapshot_id", "paper_performance_snapshots"),
        _fk("calendar_authority_id", "exchange_calendar_authorities"),
        sa.UniqueConstraint("experiment_id", name="uq_strategy_shadow_groups_experiment"),
        sa.UniqueConstraint("idempotency_key", name="uq_strategy_shadow_groups_idempotency"),
        sa.CheckConstraint(
            "champion_generation >= 1", name=op.f("ck_strategy_shadow_groups_generation")
        ),
        sa.CheckConstraint(
            "evaluation_sessions = 63 AND grace_sessions = 5",
            name=op.f("ck_strategy_shadow_groups_session_budgets"),
        ),
        sa.CheckConstraint("initial_nav > 0", name=op.f("ck_strategy_shadow_groups_initial_nav")),
        sa.CheckConstraint(
            "champion_strategy_definition_id <> challenger_strategy_definition_id",
            name=op.f("ck_strategy_shadow_groups_distinct_definitions"),
        ),
        sa.CheckConstraint(
            "status IN ('running','failed','finalized','stale')",
            name=op.f("ck_strategy_shadow_groups_status"),
        ),
        _hash_check("authority_hash", name="ck_strategy_shadow_groups_authority_hash"),
    )
    op.create_table(
        "strategy_shadow_legs",
        _uuid("id"),
        _uuid("group_id"),
        _text("role", 16),
        _uuid("strategy_definition_id"),
        _text("initial_state_hash", 64),
        _created_at(),
        _pk(),
        _fk("group_id", "strategy_shadow_groups"),
        _fk("strategy_definition_id", "strategy_definitions"),
        sa.UniqueConstraint("group_id", "role", name="uq_strategy_shadow_legs_group_role"),
        sa.CheckConstraint(
            "role IN ('champion','challenger')", name=op.f("ck_strategy_shadow_legs_role")
        ),
        _hash_check("initial_state_hash", name="ck_strategy_shadow_legs_initial_state_hash"),
    )
    op.create_table(
        "strategy_shadow_advance_attempts",
        _uuid("id"),
        _uuid("group_id"),
        sa.Column("session", sa.Date(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        _text("status", 16),
        _text("reason_code", 64, nullable=True),
        _text("input_hash", 64),
        _utc("observed_at"),
        _created_at(),
        _pk(),
        _fk("group_id", "strategy_shadow_groups"),
        sa.UniqueConstraint(
            "group_id",
            "session",
            "attempt_number",
            name="uq_strategy_shadow_advance_attempts_number",
        ),
        sa.CheckConstraint(
            "attempt_number >= 1", name=op.f("ck_strategy_shadow_advance_attempts_number")
        ),
        sa.CheckConstraint(
            "status IN ('applied','failed','duplicate')",
            name=op.f("ck_strategy_shadow_advance_attempts_status"),
        ),
        _hash_check("input_hash", name="ck_strategy_shadow_advance_attempts_input_hash"),
    )
    op.create_table(
        "strategy_shadow_session_authorities",
        _uuid("id"),
        _uuid("group_id"),
        sa.Column("session", sa.Date(), nullable=False),
        sa.Column("session_ordinal", sa.Integer(), nullable=False),
        _uuid("calendar_authority_id"),
        _uuid("source_policy_id"),
        _uuid("cost_policy_id"),
        _text("authority_hash", 64),
        _utc("available_at"),
        _created_at(),
        _pk(),
        _fk("group_id", "strategy_shadow_groups"),
        _fk("calendar_authority_id", "exchange_calendar_authorities"),
        _fk("source_policy_id", "source_policies"),
        _fk("cost_policy_id", "cost_policies"),
        sa.UniqueConstraint(
            "group_id", "session", name="uq_strategy_shadow_session_authorities_group_session"
        ),
        sa.UniqueConstraint(
            "group_id",
            "session_ordinal",
            name="uq_strategy_shadow_session_authorities_group_ordinal",
        ),
        sa.CheckConstraint(
            "session_ordinal BETWEEN 1 AND 68",
            name=op.f("ck_strategy_shadow_session_authorities_ordinal"),
        ),
        _hash_check("authority_hash", name="ck_strategy_shadow_session_authorities_authority_hash"),
    )
    op.create_table(
        "strategy_shadow_session_price_inputs",
        _uuid("id"),
        _uuid("session_authority_id"),
        _uuid("instrument_id"),
        _uuid("raw_price_authority_id"),
        _uuid("adjusted_price_authority_id"),
        _text("input_hash", 64),
        _created_at(),
        _pk(),
        _fk("session_authority_id", "strategy_shadow_session_authorities"),
        _fk("instrument_id", "instruments"),
        _fk("raw_price_authority_id", "price_bar_set_authorities"),
        _fk("adjusted_price_authority_id", "price_bar_set_authorities"),
        sa.UniqueConstraint(
            "session_authority_id",
            "instrument_id",
            name="uq_strategy_shadow_session_price_inputs_instrument",
        ),
        _hash_check("input_hash", name="ck_strategy_shadow_session_price_inputs_hash"),
    )
    op.create_table(
        "strategy_shadow_leg_states",
        _uuid("id"),
        _uuid("leg_id"),
        _uuid("session_authority_id"),
        sa.Column("session_ordinal", sa.Integer(), nullable=False),
        sa.Column("cash", sa.Numeric(38, 12), nullable=False),
        sa.Column("nav", sa.Numeric(38, 12), nullable=False),
        _json("positions"),
        sa.Column("hard_risk_violation", sa.Boolean(), nullable=False),
        _text("state_hash", 64),
        _text("transition_hash", 64),
        _created_at(),
        _pk(),
        _fk("leg_id", "strategy_shadow_legs"),
        _fk("session_authority_id", "strategy_shadow_session_authorities"),
        sa.UniqueConstraint(
            "leg_id", "session_ordinal", name="uq_strategy_shadow_leg_states_ordinal"
        ),
        sa.UniqueConstraint(
            "leg_id", "session_authority_id", name="uq_strategy_shadow_leg_states_authority"
        ),
        sa.CheckConstraint(
            "session_ordinal BETWEEN 0 AND 68 AND cash >= 0 AND nav >= 0",
            name=op.f("ck_strategy_shadow_leg_states_values"),
        ),
        _hash_check("state_hash", "transition_hash", name="ck_strategy_shadow_leg_states_hashes"),
    )
    _create_shadow_execution_tables()


def _create_shadow_execution_tables() -> None:
    op.create_table(
        "strategy_shadow_orders",
        _uuid("id"),
        _uuid("leg_state_id"),
        _uuid("instrument_id"),
        _text("side", 4),
        sa.Column("quantity", sa.Numeric(38, 12), nullable=False),
        _text("status", 24),
        _text("reason_code", 64, nullable=True),
        _text("order_hash", 64),
        _created_at(),
        _pk(),
        _fk("leg_state_id", "strategy_shadow_leg_states"),
        _fk("instrument_id", "instruments"),
        sa.UniqueConstraint("order_hash", name="uq_strategy_shadow_orders_hash"),
        sa.CheckConstraint("side IN ('BUY','SELL')", name=op.f("ck_strategy_shadow_orders_side")),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_strategy_shadow_orders_quantity")),
        sa.CheckConstraint(
            "status IN ('FILLED','PARTIALLY_FILLED','REJECTED','CANCELLED','EXPIRED')",
            name=op.f("ck_strategy_shadow_orders_status"),
        ),
        _hash_check("order_hash", name="ck_strategy_shadow_orders_order_hash"),
    )
    op.create_table(
        "strategy_shadow_fills",
        _uuid("id"),
        _uuid("order_id"),
        sa.Column("quantity", sa.Numeric(38, 12), nullable=False),
        sa.Column("price", sa.Numeric(38, 12), nullable=False),
        sa.Column("fees", sa.Numeric(38, 12), nullable=False),
        _utc("filled_at"),
        _text("fill_hash", 64),
        _created_at(),
        _pk(),
        _fk("order_id", "strategy_shadow_orders"),
        sa.UniqueConstraint("fill_hash", name="uq_strategy_shadow_fills_hash"),
        sa.CheckConstraint(
            "quantity > 0 AND price > 0 AND fees >= 0",
            name=op.f("ck_strategy_shadow_fills_values"),
        ),
        _hash_check("fill_hash", name="ck_strategy_shadow_fills_fill_hash"),
    )
    op.create_table(
        "strategy_shadow_checkpoints",
        _uuid("id"),
        _uuid("group_id"),
        sa.Column("checkpoint_ordinal", sa.Integer(), nullable=False),
        sa.Column("session", sa.Date(), nullable=False),
        _text("authority_hash", 64),
        _created_at(),
        _pk(),
        _fk("group_id", "strategy_shadow_groups"),
        sa.UniqueConstraint(
            "group_id", "checkpoint_ordinal", name="uq_strategy_shadow_checkpoints_ordinal"
        ),
        sa.CheckConstraint(
            "checkpoint_ordinal IN (5,21,63)",
            name=op.f("ck_strategy_shadow_checkpoints_ordinal"),
        ),
        _hash_check("authority_hash", name="ck_strategy_shadow_checkpoints_authority_hash"),
    )
    op.create_table(
        "strategy_shadow_checkpoint_metrics",
        _uuid("id"),
        _uuid("checkpoint_id"),
        _uuid("leg_id"),
        _json("metric_report"),
        _text("metric_hash", 64),
        _created_at(),
        _pk(),
        _fk("checkpoint_id", "strategy_shadow_checkpoints"),
        _fk("leg_id", "strategy_shadow_legs"),
        sa.UniqueConstraint(
            "checkpoint_id", "leg_id", name="uq_strategy_shadow_checkpoint_metrics_leg"
        ),
        _hash_check("metric_hash", name="ck_strategy_shadow_checkpoint_metrics_hash"),
    )
    op.create_table(
        "strategy_shadow_finalizations",
        _uuid("id"),
        _uuid("group_id"),
        _uuid("promotion_policy_id"),
        _text("status", 16),
        _json("gate_report"),
        _text("reason_code", 64),
        _text("final_authority_hash", 64),
        _text("finalization_hash", 64),
        _created_at(),
        _pk(),
        _fk("group_id", "strategy_shadow_groups"),
        _fk("promotion_policy_id", "strategy_promotion_policies"),
        sa.UniqueConstraint("group_id", name="uq_strategy_shadow_finalizations_group"),
        sa.UniqueConstraint("finalization_hash", name="uq_strategy_shadow_finalizations_hash"),
        sa.CheckConstraint(
            "status IN ('PASS','FAIL','UNKNOWN','STALE')",
            name=op.f("ck_strategy_shadow_finalizations_status"),
        ),
        _hash_check(
            "final_authority_hash",
            "finalization_hash",
            name="ck_strategy_shadow_finalizations_hashes",
        ),
    )


def _create_registry_tables() -> None:
    op.create_table(
        "strategy_registry_events",
        _uuid("id"),
        _text("channel", 32),
        _text("schema_version", 64),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        _text("event_type", 32),
        _text("idempotency_key", 100),
        _text("actor_kind", 32),
        _utc("occurred_at"),
        _uuid("from_definition_id", nullable=True),
        _uuid("to_definition_id"),
        _uuid("promotion_decision_id", nullable=True),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        sa.Column("effective_session", sa.Date(), nullable=True),
        _json("payload"),
        _text("previous_hash", 64),
        _text("event_hash", 64),
        _created_at(),
        _pk(),
        _fk("from_definition_id", "strategy_definitions"),
        _fk("to_definition_id", "strategy_definitions"),
        _fk("promotion_decision_id", "strategy_promotion_decisions"),
        sa.UniqueConstraint(
            "channel", "sequence", name="uq_strategy_registry_events_channel_sequence"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_strategy_registry_events_idempotency"),
        sa.UniqueConstraint("event_hash", name="uq_strategy_registry_events_hash"),
        sa.CheckConstraint(
            "channel = 'internal_paper'", name=op.f("ck_strategy_registry_events_channel")
        ),
        sa.CheckConstraint("sequence >= 1", name=op.f("ck_strategy_registry_events_sequence")),
        sa.CheckConstraint(
            "schema_version = 'strategy_registry_event_v1'",
            name=op.f("ck_strategy_registry_events_schema"),
        ),
        sa.CheckConstraint(
            "event_type IN ('GENESIS','PROMOTE','ROLLBACK','PAUSE','RESUME','VETO')",
            name=op.f("ck_strategy_registry_events_event_type"),
        ),
        sa.CheckConstraint(
            "actor_kind IN ('migration','service','user','scheduler','recovery')",
            name=op.f("ck_strategy_registry_events_actor"),
        ),
        sa.CheckConstraint(
            "champion_generation >= 1", name=op.f("ck_strategy_registry_events_generation")
        ),
        _hash_check("previous_hash", "event_hash", name="ck_strategy_registry_events_hashes"),
    )
    op.create_table(
        "strategy_registry_heads",
        _text("channel", 32),
        _uuid("active_definition_id"),
        sa.Column("active_generation", sa.BigInteger(), nullable=False),
        _uuid("last_event_id"),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("auto_promotion_paused", sa.Boolean(), nullable=False),
        _uuid("pending_event_id", nullable=True),
        sa.Column("pending_effective_session", sa.Date(), nullable=True),
        sa.Column("version", sa.BigInteger(), nullable=False),
        _utc("updated_at"),
        sa.PrimaryKeyConstraint("channel"),
        _fk("active_definition_id", "strategy_definitions"),
        _fk("last_event_id", "strategy_registry_events"),
        _fk("pending_event_id", "strategy_registry_events"),
        sa.CheckConstraint(
            "channel = 'internal_paper'", name=op.f("ck_strategy_registry_heads_channel")
        ),
        sa.CheckConstraint(
            "active_generation >= 1 AND event_sequence >= 1 AND version >= 1",
            name=op.f("ck_strategy_registry_heads_counters"),
        ),
        sa.CheckConstraint(
            "(pending_event_id IS NULL) = (pending_effective_session IS NULL)",
            name=op.f("ck_strategy_registry_heads_pending_binding"),
        ),
    )


def _seed_policies_and_registry() -> None:
    connection = op.get_bind()
    baseline = (
        connection.execute(
            sa.text(
                "SELECT id,strategy_version,schema_version,expression_hash,document,lifecycle "
                "FROM strategy_definitions WHERE strategy_version='baseline_v1'"
            )
        )
        .mappings()
        .first()
    )
    if baseline is None:
        connection.execute(
            sa.text(
                "INSERT INTO strategy_definitions "
                "(id,strategy_version,schema_version,expression_hash,document,lifecycle,published_at) "
                "VALUES (:id,'baseline_v1','strategy_dsl_v1',:expression_hash,"
                "CAST(:document AS jsonb),'champion','2026-08-27T00:00:00Z')"
            ),
            {
                "id": _BASELINE_DEFINITION_ID,
                "expression_hash": _BASELINE_EXPRESSION_HASH,
                "document": _json_literal(_BASELINE_DOCUMENT),
            },
        )
        baseline_id = _BASELINE_DEFINITION_ID
    else:
        expected = {
            "strategy_version": "baseline_v1",
            "schema_version": "strategy_dsl_v1",
            "expression_hash": _BASELINE_EXPRESSION_HASH,
            "document": _BASELINE_DOCUMENT,
            "lifecycle": "champion",
        }
        observed = {key: baseline[key] for key in expected}
        if observed != expected:
            raise RuntimeError(
                "baseline_v1 definition does not match the released compiler authority"
            )
        baseline_id = str(baseline["id"])

    policies = (
        (
            _CANDIDATE_POLICY_ID,
            "candidate_budget",
            "candidate_budget_policy_v1",
            _CANDIDATE_POLICY,
        ),
        (
            _SEARCH_POLICY_ID,
            "evolution_search_space",
            "evolution_search_space_v1",
            _SEARCH_POLICY,
        ),
    )
    for policy_id, kind, version, document in policies:
        connection.execute(
            sa.text(
                "INSERT INTO strategy_evolution_policies "
                "(id,policy_kind,version,document,content_hash,published_at) "
                "VALUES (:id,:kind,:version,CAST(:document AS jsonb),:content_hash,"
                "'2026-08-27T00:00:00Z')"
            ),
            {
                "id": policy_id,
                "kind": kind,
                "version": version,
                "document": _json_literal(document),
                "content_hash": _sha256(document),
            },
        )
    connection.execute(
        sa.text(
            "INSERT INTO strategy_promotion_policies "
            "(id,version,document,content_hash,published_at) "
            "VALUES (:id,'promotion_policy_v1',CAST(:document AS jsonb),:content_hash,"
            "'2026-08-27T00:00:00Z')"
        ),
        {
            "id": _PROMOTION_POLICY_ID,
            "document": _json_literal(_PROMOTION_POLICY),
            "content_hash": _sha256(_PROMOTION_POLICY),
        },
    )

    legacy_decision_count = int(
        connection.execute(sa.text("SELECT count(*) FROM strategy_decisions")).scalar_one()
    )
    genesis_payload = {
        "schema_version": "strategy_registry_genesis_payload_v1",
        "reason_code": "PHASE2_BASELINE_GENESIS",
        "legacy_decision_count": legacy_decision_count,
        "strategy_version": "baseline_v1",
        "expression_hash": _BASELINE_EXPRESSION_HASH,
    }
    genesis_identity = {
        "channel": "internal_paper",
        "schema_version": "strategy_registry_event_v1",
        "sequence": 1,
        "event_type": "GENESIS",
        "idempotency_key": "phase2-registry-genesis-v1",
        "actor_kind": "migration",
        "occurred_at": "2026-08-27T00:00:00Z",
        "from_definition_id": None,
        "to_definition_id": baseline_id,
        "champion_generation": 1,
        "effective_session": None,
        "payload": genesis_payload,
        "previous_hash": "0" * 64,
    }
    genesis_hash = _sha256(genesis_identity)
    connection.execute(
        sa.text(
            "INSERT INTO strategy_registry_events "
            "(id,channel,schema_version,sequence,event_type,idempotency_key,actor_kind,"
            "occurred_at,from_definition_id,to_definition_id,promotion_decision_id,"
            "champion_generation,effective_session,payload,previous_hash,event_hash) "
            "VALUES (:id,'internal_paper','strategy_registry_event_v1',1,'GENESIS',"
            "'phase2-registry-genesis-v1','migration','2026-08-27T00:00:00Z',NULL,"
            ":definition_id,NULL,1,NULL,CAST(:payload AS jsonb),:previous_hash,:event_hash)"
        ),
        {
            "id": _GENESIS_EVENT_ID,
            "definition_id": baseline_id,
            "payload": _json_literal(genesis_payload),
            "previous_hash": "0" * 64,
            "event_hash": genesis_hash,
        },
    )
    connection.execute(
        sa.text(
            "INSERT INTO strategy_registry_heads "
            "(channel,active_definition_id,active_generation,last_event_id,event_sequence,"
            "auto_promotion_paused,pending_event_id,pending_effective_session,version,updated_at) "
            "VALUES ('internal_paper',:definition_id,1,:event_id,1,false,NULL,NULL,1,"
            "'2026-08-27T00:00:00Z')"
        ),
        {"definition_id": baseline_id, "event_id": _GENESIS_EVENT_ID},
    )


def _bind_forward_decisions() -> None:
    op.execute(
        "CREATE FUNCTION nexus_phase2_genesis_definition_id() RETURNS uuid LANGUAGE sql "
        "STABLE AS $$ SELECT active_definition_id FROM strategy_registry_heads "
        "WHERE channel='internal_paper' $$"
    )
    op.add_column(
        "strategy_decisions",
        sa.Column(
            "strategy_definition_id",
            sa.UUID(),
            server_default=sa.text("nexus_phase2_genesis_definition_id()"),
            nullable=False,
        ),
    )
    op.add_column(
        "strategy_decisions",
        sa.Column(
            "registry_event_id",
            sa.UUID(),
            server_default=sa.text(f"'{_GENESIS_EVENT_ID}'::uuid"),
            nullable=False,
        ),
    )
    op.add_column(
        "strategy_decisions",
        sa.Column("champion_generation", sa.BigInteger(), server_default="1", nullable=False),
    )
    op.alter_column("strategy_decisions", "strategy_definition_id", server_default=None)
    op.alter_column("strategy_decisions", "registry_event_id", server_default=None)
    op.alter_column("strategy_decisions", "champion_generation", server_default=None)
    op.execute("DROP FUNCTION nexus_phase2_genesis_definition_id()")
    op.create_foreign_key(
        "fk_strategy_decisions_strategy_definition",
        "strategy_decisions",
        "strategy_definitions",
        ["strategy_definition_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_strategy_decisions_registry_event",
        "strategy_decisions",
        "strategy_registry_events",
        ["registry_event_id"],
        ["id"],
    )
    op.drop_constraint(
        "uq_strategy_decisions_account_version_session", "strategy_decisions", type_="unique"
    )
    op.create_unique_constraint(
        "uq_strategy_decisions_account_session",
        "strategy_decisions",
        ["account_id", "decision_session"],
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_strategy_fixed"), "strategy_decisions", type_="check"
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        "btrim(strategy_id) <> '' AND btrim(strategy_version) <> '' AND champion_generation >= 1",
    )


def _extend_strategy_rnd_prompt_role() -> None:
    op.drop_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"), "agent_prompt_versions", type_="check"
    )
    op.create_check_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"),
        "agent_prompt_versions",
        "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
        "'evidence_audit','portfolio','strategy_rnd')",
    )


def _install_database_guards() -> None:
    op.execute(
        "CREATE FUNCTION nexus_prevent_strategy_evolution_change() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "RAISE EXCEPTION 'strategy evolution authority is append-only' USING ERRCODE='23514'; "
        "END; $$"
    )
    for table_name in _AUTHORITY_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable BEFORE UPDATE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_strategy_evolution_change()"
        )

    op.execute(
        """
        CREATE FUNCTION nexus_strategy_experiment_event_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          prior_sequence bigint;
          prior_hash varchar(64);
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('strategy-experiment-event:' || NEW.family_id::text, 0)
          );
          SELECT sequence,event_hash INTO prior_sequence,prior_hash
          FROM strategy_experiment_events
          WHERE family_id=NEW.family_id
          ORDER BY sequence DESC LIMIT 1;
          IF prior_sequence IS NULL THEN
            IF NEW.sequence <> 1 OR NEW.previous_hash <> repeat('0',64) THEN
              RAISE EXCEPTION 'strategy experiment event chain is invalid'
                USING ERRCODE='23514';
            END IF;
          ELSIF NEW.sequence <> prior_sequence + 1 OR NEW.previous_hash <> prior_hash THEN
            RAISE EXCEPTION 'strategy experiment event chain is invalid'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_experiment_events_chain BEFORE INSERT "
        "ON strategy_experiment_events FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_experiment_event_chain_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_registry_event_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          prior strategy_registry_events%ROWTYPE;
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('strategy-registry-event:' || NEW.channel, 0)
          );
          SELECT * INTO prior FROM strategy_registry_events
          WHERE channel=NEW.channel ORDER BY sequence DESC LIMIT 1;
          IF prior.id IS NULL THEN
            IF NEW.event_type <> 'GENESIS' OR NEW.sequence <> 1
              OR NEW.champion_generation <> 1 OR NEW.previous_hash <> repeat('0',64) THEN
              RAISE EXCEPTION 'strategy registry genesis is invalid' USING ERRCODE='23514';
            END IF;
          ELSE
            IF NEW.event_type='GENESIS' OR NEW.sequence <> prior.sequence + 1
              OR NEW.previous_hash <> prior.event_hash THEN
              RAISE EXCEPTION 'strategy registry event chain is invalid' USING ERRCODE='23514';
            END IF;
            IF NEW.event_type IN ('PROMOTE','ROLLBACK') THEN
              IF NEW.champion_generation <> prior.champion_generation + 1 THEN
                RAISE EXCEPTION 'strategy registry generation is invalid' USING ERRCODE='23514';
              END IF;
            ELSIF NEW.champion_generation <> prior.champion_generation THEN
              RAISE EXCEPTION 'strategy registry control changed generation' USING ERRCODE='23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_registry_events_chain BEFORE INSERT "
        "ON strategy_registry_events FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_registry_event_chain_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_experiment_projection_monotonic()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF NEW.family_id <> OLD.family_id
            OR NEW.research_lineage_id <> OLD.research_lineage_id
            OR NEW.version <> OLD.version + 1
            OR NEW.event_sequence < OLD.event_sequence
            OR NEW.cumulative_oos_trial_count < OLD.cumulative_oos_trial_count THEN
            RAISE EXCEPTION 'strategy experiment projection regressed' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_experiment_state_projections_monotonic BEFORE UPDATE "
        "ON strategy_experiment_state_projections FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_experiment_projection_monotonic()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_registry_head_monotonic()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF NEW.channel <> OLD.channel
            OR NEW.version <> OLD.version + 1
            OR NEW.event_sequence < OLD.event_sequence
            OR NEW.event_sequence > OLD.event_sequence + 1
            OR NEW.active_generation < OLD.active_generation
            OR NEW.active_generation > OLD.active_generation + 1 THEN
            RAISE EXCEPTION 'strategy registry head regressed' USING ERRCODE='23514';
          END IF;
          IF NEW.active_generation = OLD.active_generation + 1
            AND NEW.active_definition_id = OLD.active_definition_id
            AND NEW.pending_event_id IS NULL THEN
            RAISE EXCEPTION 'strategy registry generation lacks a new champion'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_registry_heads_monotonic BEFORE UPDATE "
        "ON strategy_registry_heads FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_registry_head_monotonic()"
    )


def _drop_database_guards() -> None:
    op.execute("DROP TRIGGER trg_strategy_registry_heads_monotonic ON strategy_registry_heads")
    op.execute("DROP FUNCTION nexus_strategy_registry_head_monotonic()")
    op.execute(
        "DROP TRIGGER trg_strategy_experiment_state_projections_monotonic "
        "ON strategy_experiment_state_projections"
    )
    op.execute("DROP FUNCTION nexus_strategy_experiment_projection_monotonic()")
    op.execute("DROP TRIGGER trg_strategy_registry_events_chain ON strategy_registry_events")
    op.execute("DROP FUNCTION nexus_strategy_registry_event_chain_valid()")
    op.execute("DROP TRIGGER trg_strategy_experiment_events_chain ON strategy_experiment_events")
    op.execute("DROP FUNCTION nexus_strategy_experiment_event_chain_valid()")
    for table_name in reversed(_AUTHORITY_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute("DROP FUNCTION nexus_prevent_strategy_evolution_change()")


def _refuse_downgrade_with_phase2_facts() -> None:
    op.execute(
        """
        DO $$
        DECLARE
          fact_count bigint;
          expected_legacy_decisions bigint;
        BEGIN
          SELECT COALESCE((payload->>'legacy_decision_count')::bigint, -1)
          INTO expected_legacy_decisions
          FROM strategy_registry_events
          WHERE id='019d0000-0000-7000-8000-000000002201';

          SELECT
            (SELECT count(*) FROM strategy_rnd_runs)
            + (SELECT count(*) FROM strategy_rnd_outputs)
            + GREATEST((SELECT count(*) FROM strategy_evolution_policies) - 2, 0)
            + GREATEST((SELECT count(*) FROM strategy_promotion_policies) - 1, 0)
            + (SELECT count(*) FROM strategy_research_lineages)
            + (SELECT count(*) FROM strategy_experiment_families)
            + (SELECT count(*) FROM strategy_experiments)
            + (SELECT count(*) FROM strategy_experiment_events)
            + (SELECT count(*) FROM strategy_experiment_attempts)
            + (SELECT count(*) FROM strategy_experiment_state_projections)
            + (SELECT count(*) FROM strategy_champion_comparisons)
            + (SELECT count(*) FROM strategy_promotion_decisions)
            + (SELECT count(*) FROM strategy_shadow_groups)
            + (SELECT count(*) FROM strategy_shadow_legs)
            + (SELECT count(*) FROM strategy_shadow_advance_attempts)
            + (SELECT count(*) FROM strategy_shadow_session_authorities)
            + (SELECT count(*) FROM strategy_shadow_session_price_inputs)
            + (SELECT count(*) FROM strategy_shadow_leg_states)
            + (SELECT count(*) FROM strategy_shadow_orders)
            + (SELECT count(*) FROM strategy_shadow_fills)
            + (SELECT count(*) FROM strategy_shadow_checkpoints)
            + (SELECT count(*) FROM strategy_shadow_checkpoint_metrics)
            + (SELECT count(*) FROM strategy_shadow_finalizations)
            + GREATEST((SELECT count(*) FROM strategy_registry_events) - 1, 0)
            + GREATEST((SELECT count(*) FROM strategy_registry_heads) - 1, 0)
            + (SELECT count(*) FROM agent_prompt_versions WHERE role='strategy_rnd')
            + GREATEST((SELECT count(*) FROM strategy_decisions) - expected_legacy_decisions, 0)
          INTO fact_count;

          IF expected_legacy_decisions < 0 OR fact_count > 0 OR EXISTS (
            SELECT 1 FROM strategy_decisions
            WHERE registry_event_id <> '019d0000-0000-7000-8000-000000002201'
               OR champion_generation <> 1
          ) THEN
            RAISE EXCEPTION 'phase 2 facts prevent downgrade' USING ERRCODE='23514';
          END IF;
        END;
        $$
        """
    )


def _restore_strategy_rnd_prompt_role() -> None:
    op.drop_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"), "agent_prompt_versions", type_="check"
    )
    op.create_check_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"),
        "agent_prompt_versions",
        "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
        "'evidence_audit','portfolio')",
    )


def _unbind_forward_decisions() -> None:
    op.drop_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_strategy_fixed"),
        "strategy_decisions",
        "strategy_id = 'baseline' AND strategy_version = 'baseline_v1'",
    )
    op.drop_constraint(
        "uq_strategy_decisions_account_session", "strategy_decisions", type_="unique"
    )
    op.create_unique_constraint(
        "uq_strategy_decisions_account_version_session",
        "strategy_decisions",
        ["account_id", "strategy_version", "decision_session"],
    )
    op.drop_constraint(
        "fk_strategy_decisions_registry_event", "strategy_decisions", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_strategy_decisions_strategy_definition", "strategy_decisions", type_="foreignkey"
    )
    op.drop_column("strategy_decisions", "champion_generation")
    op.drop_column("strategy_decisions", "registry_event_id")
    op.drop_column("strategy_decisions", "strategy_definition_id")
