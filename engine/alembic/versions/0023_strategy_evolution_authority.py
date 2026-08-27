"""Add replayable candidate, OOS, and registry activation authorities.

Revision ID: 0023_evolution_authority
Revises: 0022_strategy_evolution
Create Date: 2026-08-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0023_evolution_authority"
down_revision: str | Sequence[str] | None = "0022_strategy_evolution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_AUTHORITY_TABLES = (
    "strategy_candidate_validation_results",
    "strategy_oos_exposures",
    "strategy_experiment_oos_exposure_links",
    "strategy_oos_evidence_links",
    "strategy_registry_activations",
)
_CANDIDATE_STATES = (
    "ADMITTED",
    "VALIDATION_FAILED",
    "VALIDATION_PASSED",
    "OOS_NOT_SELECTED",
    "OOS_SELECTED",
    "OOS_RESERVED",
    "BACKTEST_FAILED",
    "BACKTEST_PASSED",
    "SHADOW_RUNNING",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTION_ELIGIBLE",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
)


def upgrade() -> None:
    _refuse_ambiguous_upgrade()
    op.add_column(
        "strategy_experiment_events",
        sa.Column("experiment_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_strategy_experiment_events_experiment",
        "strategy_experiment_events",
        "strategy_experiments",
        ["experiment_id"],
        ["id"],
    )
    op.create_index(
        "ix_strategy_experiment_events_experiment_sequence",
        "strategy_experiment_events",
        ["experiment_id", "sequence"],
        unique=False,
    )
    op.add_column(
        "strategy_experiment_families",
        sa.Column("baseline_backtest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_strategy_experiment_families_baseline_backtest_run",
        "strategy_experiment_families",
        "backtest_runs",
        ["baseline_backtest_run_id"],
        ["id"],
    )
    _create_projection_tables()
    _create_validation_and_oos_tables()
    _create_registry_activation_table()
    _backfill_projections()
    _install_guards()


def downgrade() -> None:
    _refuse_downgrade_with_authority_facts()
    _drop_guards()
    op.drop_table("strategy_registry_activations")
    op.drop_table("strategy_oos_evidence_links")
    op.drop_table("strategy_experiment_oos_exposure_links")
    op.drop_table("strategy_oos_exposures")
    op.drop_table("strategy_candidate_validation_results")
    op.drop_table("strategy_experiment_candidate_projections")
    op.drop_table("strategy_research_lineage_projections")
    op.drop_constraint(
        "fk_strategy_experiment_families_baseline_backtest_run",
        "strategy_experiment_families",
        type_="foreignkey",
    )
    op.drop_column("strategy_experiment_families", "baseline_backtest_run_id")
    op.drop_index(
        "ix_strategy_experiment_events_experiment_sequence",
        table_name="strategy_experiment_events",
    )
    op.drop_constraint(
        "fk_strategy_experiment_events_experiment",
        "strategy_experiment_events",
        type_="foreignkey",
    )
    op.drop_column("strategy_experiment_events", "experiment_id")


def _created_at() -> sa.Column[object]:
    return sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


def _create_projection_tables() -> None:
    op.create_table(
        "strategy_research_lineage_projections",
        sa.Column("lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cumulative_oos_trial_count", sa.BigInteger(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("lineage_id"),
        sa.ForeignKeyConstraint(
            ["lineage_id"],
            ["strategy_research_lineages.id"],
            ondelete="CASCADE",
            name="fk_strategy_research_lineage_projections_lineage",
        ),
        sa.CheckConstraint(
            "cumulative_oos_trial_count >= 0 AND version >= 1",
            name=op.f("ck_strategy_research_lineage_projections_counters"),
        ),
    )
    states = ",".join(f"'{state}'" for state in _CANDIDATE_STATES)
    op.create_table(
        "strategy_experiment_candidate_projections",
        sa.Column("experiment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("family_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("current_state", sa.String(length=40), nullable=False),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("experiment_id"),
        sa.ForeignKeyConstraint(
            ["experiment_id"],
            ["strategy_experiments.id"],
            ondelete="CASCADE",
            name="fk_strategy_experiment_candidate_projections_experiment",
        ),
        sa.ForeignKeyConstraint(
            ["family_id"],
            ["strategy_experiment_families.id"],
            ondelete="CASCADE",
            name="fk_strategy_experiment_candidate_projections_family",
        ),
        sa.CheckConstraint(
            "event_sequence >= 1 AND version >= 1",
            name=op.f("ck_strategy_experiment_candidate_projections_counters"),
        ),
        sa.CheckConstraint(
            f"current_state IN ({states})",
            name=op.f("ck_strategy_experiment_candidate_projections_state"),
        ),
    )
    op.create_index(
        "ix_strategy_experiment_candidate_projections_family_state",
        "strategy_experiment_candidate_projections",
        ["family_id", "current_state"],
        unique=False,
    )


def _create_validation_and_oos_tables() -> None:
    op.create_table(
        "strategy_candidate_validation_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("experiment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("validation_sharpe", sa.Numeric(38, 12), nullable=False),
        sa.Column("validation_max_drawdown", sa.Numeric(38, 12), nullable=False),
        sa.Column("validation_cost", sa.Numeric(38, 12), nullable=False),
        sa.Column("valid_window_count", sa.Integer(), nullable=False),
        sa.Column("authority_complete", sa.Boolean(), nullable=False),
        sa.Column("authority_hash", sa.String(length=64), nullable=False),
        sa.Column("protocol_hash", sa.String(length=64), nullable=False),
        sa.Column("code_hash", sa.String(length=64), nullable=False),
        sa.Column("result_hash", sa.String(length=64), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["experiment_id"],
            ["strategy_experiments.id"],
            name="fk_strategy_candidate_validation_results_experiment",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["strategy_experiment_attempts.id"],
            name="fk_strategy_candidate_validation_results_attempt",
        ),
        sa.UniqueConstraint(
            "experiment_id", name="uq_strategy_candidate_validation_results_experiment"
        ),
        sa.UniqueConstraint(
            "attempt_id", name="uq_strategy_candidate_validation_results_attempt"
        ),
        sa.UniqueConstraint(
            "result_hash", name="uq_strategy_candidate_validation_results_hash"
        ),
        sa.CheckConstraint(
            "validation_cost >= 0 AND valid_window_count >= 0",
            name=op.f("ck_strategy_candidate_validation_results_metrics"),
        ),
        sa.CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND "
            "protocol_hash ~ '^[0-9a-f]{64}$' AND "
            "code_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_candidate_validation_results_hashes"),
        ),
    )
    op.create_table(
        "strategy_oos_exposures",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("research_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("expression_hash", sa.String(length=64), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.Column("first_experiment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("exposed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exposure_hash", sa.String(length=64), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["research_lineage_id"],
            ["strategy_research_lineages.id"],
            name="fk_strategy_oos_exposures_lineage",
        ),
        sa.ForeignKeyConstraint(
            ["first_experiment_id"],
            ["strategy_experiments.id"],
            name="fk_strategy_oos_exposures_first_experiment",
        ),
        sa.UniqueConstraint(
            "research_lineage_id",
            "expression_hash",
            name="uq_strategy_oos_exposures_lineage_expression",
        ),
        sa.UniqueConstraint(
            "research_lineage_id",
            "ordinal",
            name="uq_strategy_oos_exposures_lineage_ordinal",
        ),
        sa.UniqueConstraint("exposure_hash", name="uq_strategy_oos_exposures_hash"),
        sa.CheckConstraint(
            "ordinal >= 1", name=op.f("ck_strategy_oos_exposures_ordinal")
        ),
        sa.CheckConstraint(
            "expression_hash ~ '^[0-9a-f]{64}$' AND "
            "exposure_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_oos_exposures_hashes"),
        ),
    )
    op.create_table(
        "strategy_experiment_oos_exposure_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("experiment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("exposure_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("link_hash", sa.String(length=64), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["experiment_id"],
            ["strategy_experiments.id"],
            name="fk_strategy_experiment_oos_exposure_links_experiment",
        ),
        sa.ForeignKeyConstraint(
            ["exposure_id"],
            ["strategy_oos_exposures.id"],
            name="fk_strategy_experiment_oos_exposure_links_exposure",
        ),
        sa.UniqueConstraint(
            "experiment_id", name="uq_strategy_experiment_oos_exposure_links_experiment"
        ),
        sa.UniqueConstraint(
            "link_hash", name="uq_strategy_experiment_oos_exposure_links_hash"
        ),
        sa.CheckConstraint(
            "link_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_experiment_oos_exposure_links_hash"),
        ),
    )
    op.create_table(
        "strategy_oos_evidence_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("experiment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("exposure_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("comparison_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("champion_backtest_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("challenger_backtest_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("authority_hash", sa.String(length=64), nullable=False),
        sa.Column("evidence_hash", sa.String(length=64), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["experiment_id"],
            ["strategy_experiments.id"],
            name="fk_strategy_oos_evidence_links_experiment",
        ),
        sa.ForeignKeyConstraint(
            ["exposure_id"],
            ["strategy_oos_exposures.id"],
            name="fk_strategy_oos_evidence_links_exposure",
        ),
        sa.ForeignKeyConstraint(
            ["comparison_id"],
            ["strategy_champion_comparisons.id"],
            name="fk_strategy_oos_evidence_links_comparison",
        ),
        sa.ForeignKeyConstraint(
            ["champion_backtest_run_id"],
            ["backtest_runs.id"],
            name="fk_strategy_oos_evidence_links_champion_run",
        ),
        sa.ForeignKeyConstraint(
            ["challenger_backtest_run_id"],
            ["backtest_runs.id"],
            name="fk_strategy_oos_evidence_links_challenger_run",
        ),
        sa.UniqueConstraint("experiment_id", name="uq_strategy_oos_evidence_links_experiment"),
        sa.UniqueConstraint("comparison_id", name="uq_strategy_oos_evidence_links_comparison"),
        sa.UniqueConstraint("evidence_hash", name="uq_strategy_oos_evidence_links_hash"),
        sa.CheckConstraint(
            "champion_backtest_run_id <> challenger_backtest_run_id",
            name=op.f("ck_strategy_oos_evidence_links_distinct_runs"),
        ),
        sa.CheckConstraint(
            "authority_hash ~ '^[0-9a-f]{64}$' AND "
            "evidence_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_oos_evidence_links_hashes"),
        ),
    )


def _create_registry_activation_table() -> None:
    op.create_table(
        "strategy_registry_activations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("registry_event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("decision_session", sa.Date(), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("from_definition_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("to_definition_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("champion_generation", sa.BigInteger(), nullable=False),
        sa.Column("activation_hash", sa.String(length=64), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["registry_event_id"],
            ["strategy_registry_events.id"],
            name="fk_strategy_registry_activations_event",
        ),
        sa.ForeignKeyConstraint(
            ["from_definition_id"],
            ["strategy_definitions.id"],
            name="fk_strategy_registry_activations_from_definition",
        ),
        sa.ForeignKeyConstraint(
            ["to_definition_id"],
            ["strategy_definitions.id"],
            name="fk_strategy_registry_activations_to_definition",
        ),
        sa.UniqueConstraint(
            "registry_event_id", name="uq_strategy_registry_activations_event"
        ),
        sa.UniqueConstraint(
            "activation_hash", name="uq_strategy_registry_activations_hash"
        ),
        sa.CheckConstraint(
            "champion_generation >= 2",
            name=op.f("ck_strategy_registry_activations_generation"),
        ),
        sa.CheckConstraint(
            "from_definition_id <> to_definition_id",
            name=op.f("ck_strategy_registry_activations_distinct_definitions"),
        ),
        sa.CheckConstraint(
            "activation_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_strategy_registry_activations_hash"),
        ),
    )


def _refuse_ambiguous_upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM strategy_experiment_state_projections
            WHERE current_state <> 'ADMITTED'
          ) OR EXISTS (SELECT 1 FROM strategy_champion_comparisons)
            OR EXISTS (SELECT 1 FROM strategy_shadow_groups)
            OR EXISTS (SELECT 1 FROM strategy_promotion_decisions)
          THEN
            RAISE EXCEPTION 'phase 2 candidate facts require an explicit 0023 backfill'
              USING ERRCODE='23514';
          END IF;
          IF EXISTS (
            SELECT 1 FROM strategy_registry_heads
            WHERE active_generation <> 1 OR event_sequence <> 1
              OR pending_event_id IS NOT NULL OR auto_promotion_paused
          ) OR (SELECT count(*) FROM strategy_registry_events) <> 1
          THEN
            RAISE EXCEPTION 'strategy registry activations cannot be inferred'
              USING ERRCODE='23514';
          END IF;
        END;
        $$
        """
    )


def _backfill_projections() -> None:
    op.execute(
        """
        INSERT INTO strategy_research_lineage_projections
          (lineage_id,cumulative_oos_trial_count,version,updated_at)
        SELECT id,cumulative_oos_trial_count,1,created_at
        FROM strategy_research_lineages
        """
    )
    op.execute(
        """
        INSERT INTO strategy_experiment_candidate_projections
          (experiment_id,family_id,current_state,event_sequence,version,updated_at)
        SELECT e.id,e.family_id,'ADMITTED',p.event_sequence,1,p.updated_at
        FROM strategy_experiments e
        JOIN strategy_experiment_state_projections p ON p.family_id=e.family_id
        """
    )


def _install_guards() -> None:
    for table_name in _NEW_AUTHORITY_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable BEFORE UPDATE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_strategy_evolution_change()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_lineage_projection_monotonic()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.lineage_id <> OLD.lineage_id
            OR NEW.version <> OLD.version + 1
            OR NEW.cumulative_oos_trial_count < OLD.cumulative_oos_trial_count THEN
            RAISE EXCEPTION 'strategy lineage projection regressed' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_research_lineage_projections_monotonic "
        "BEFORE UPDATE ON strategy_research_lineage_projections FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_lineage_projection_monotonic()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_candidate_projection_valid()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          bound_event strategy_experiment_events%ROWTYPE;
        BEGIN
          IF TG_OP='DELETE' THEN
            RAISE EXCEPTION 'strategy candidate projection cannot be deleted'
              USING ERRCODE='23514';
          END IF;
          IF TG_OP='UPDATE' AND (
            NEW.experiment_id <> OLD.experiment_id
            OR NEW.family_id <> OLD.family_id
            OR NEW.version <> OLD.version + 1
            OR NEW.event_sequence <= OLD.event_sequence
          ) THEN
            RAISE EXCEPTION 'strategy candidate projection regressed' USING ERRCODE='23514';
          END IF;
          SELECT * INTO bound_event FROM strategy_experiment_events
          WHERE family_id=NEW.family_id AND sequence=NEW.event_sequence;
          IF bound_event.id IS NULL
            OR bound_event.event_type <> NEW.current_state
            OR NOT (
              bound_event.experiment_id=NEW.experiment_id
              OR (TG_OP='INSERT' AND NEW.current_state='ADMITTED'
                  AND bound_event.experiment_id IS NULL)
            ) THEN
            RAISE EXCEPTION 'strategy candidate projection lacks its event'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_experiment_candidate_projections_valid "
        "BEFORE INSERT OR UPDATE OR DELETE ON "
        "strategy_experiment_candidate_projections FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_candidate_projection_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_family_projection_valid()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          expected_state text;
          lineage_trials bigint;
          bound_event_id uuid;
        BEGIN
          SELECT CASE
            WHEN bool_or(current_state='PROMOTED') THEN 'PROMOTED'
            WHEN bool_or(current_state='PROMOTION_ELIGIBLE') THEN 'PROMOTION_ELIGIBLE'
            WHEN bool_or(current_state='SHADOW_RUNNING') THEN 'SHADOW_RUNNING'
            WHEN bool_or(current_state='BACKTEST_PASSED') THEN 'BACKTEST_PASSED'
            WHEN bool_and(current_state='ADMITTED') THEN 'ADMITTED'
            WHEN bool_or(current_state IN (
              'ADMITTED','VALIDATION_PASSED','OOS_SELECTED','OOS_RESERVED'
            )) THEN 'BACKTEST_RUNNING'
            WHEN bool_or(current_state='STALE_CHAMPION_BASELINE')
              THEN 'STALE_CHAMPION_BASELINE'
            WHEN bool_or(current_state='SHADOW_FAILED') THEN 'SHADOW_FAILED'
            WHEN bool_or(current_state='EXPIRED') THEN 'EXPIRED'
            WHEN bool_or(current_state='VETOED') THEN 'VETOED'
            ELSE 'BACKTEST_FAILED'
          END INTO expected_state
          FROM strategy_experiment_candidate_projections
          WHERE family_id=NEW.family_id;
          SELECT cumulative_oos_trial_count INTO lineage_trials
          FROM strategy_research_lineage_projections
          WHERE lineage_id=NEW.research_lineage_id;
          SELECT id INTO bound_event_id FROM strategy_experiment_events
          WHERE family_id=NEW.family_id AND sequence=NEW.event_sequence;
          IF expected_state IS NULL OR lineage_trials IS NULL OR bound_event_id IS NULL
            OR NEW.current_state <> expected_state
            OR NEW.cumulative_oos_trial_count > lineage_trials
            OR (
              expected_state NOT IN (
                'BACKTEST_FAILED','SHADOW_FAILED','EXPIRED','VETOED','PROMOTED',
                'STALE_CHAMPION_BASELINE'
              )
              AND NEW.cumulative_oos_trial_count <> lineage_trials
            ) THEN
            RAISE EXCEPTION 'strategy family projection lacks replay authority'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_experiment_state_projections_valid "
        "BEFORE INSERT OR UPDATE ON strategy_experiment_state_projections FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_family_projection_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_family_projection_deferred_valid()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          target_family_id uuid;
          expected_state text;
          lineage_trials bigint;
          latest_sequence bigint;
          stored strategy_experiment_state_projections%ROWTYPE;
        BEGIN
          target_family_id := NEW.family_id;
          SELECT CASE
            WHEN bool_or(current_state='PROMOTED') THEN 'PROMOTED'
            WHEN bool_or(current_state='PROMOTION_ELIGIBLE') THEN 'PROMOTION_ELIGIBLE'
            WHEN bool_or(current_state='SHADOW_RUNNING') THEN 'SHADOW_RUNNING'
            WHEN bool_or(current_state='BACKTEST_PASSED') THEN 'BACKTEST_PASSED'
            WHEN bool_and(current_state='ADMITTED') THEN 'ADMITTED'
            WHEN bool_or(current_state IN (
              'ADMITTED','VALIDATION_PASSED','OOS_SELECTED','OOS_RESERVED'
            )) THEN 'BACKTEST_RUNNING'
            WHEN bool_or(current_state='STALE_CHAMPION_BASELINE')
              THEN 'STALE_CHAMPION_BASELINE'
            WHEN bool_or(current_state='SHADOW_FAILED') THEN 'SHADOW_FAILED'
            WHEN bool_or(current_state='EXPIRED') THEN 'EXPIRED'
            WHEN bool_or(current_state='VETOED') THEN 'VETOED'
            ELSE 'BACKTEST_FAILED'
          END INTO expected_state
          FROM strategy_experiment_candidate_projections
          WHERE family_id=target_family_id;
          SELECT * INTO stored FROM strategy_experiment_state_projections
          WHERE family_id=target_family_id;
          SELECT max(sequence) INTO latest_sequence FROM strategy_experiment_events
          WHERE family_id=target_family_id;
          SELECT cumulative_oos_trial_count INTO lineage_trials
          FROM strategy_research_lineage_projections
          WHERE lineage_id=stored.research_lineage_id;
          IF stored.family_id IS NULL OR expected_state IS NULL
            OR latest_sequence IS NULL OR lineage_trials IS NULL
            OR stored.current_state <> expected_state
            OR stored.event_sequence <> latest_sequence
            OR stored.cumulative_oos_trial_count > lineage_trials
            OR (
              expected_state NOT IN (
                'BACKTEST_FAILED','SHADOW_FAILED','EXPIRED','VETOED','PROMOTED',
                'STALE_CHAMPION_BASELINE'
              )
              AND stored.cumulative_oos_trial_count <> lineage_trials
            ) THEN
            RAISE EXCEPTION 'strategy family projection is not transactionally complete'
              USING ERRCODE='23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER "
        "trg_strategy_experiment_candidate_projections_family_complete "
        "AFTER INSERT OR UPDATE ON strategy_experiment_candidate_projections "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_family_projection_deferred_valid()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_strategy_experiment_events_family_complete "
        "AFTER INSERT ON strategy_experiment_events "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_family_projection_deferred_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_strategy_registry_activation_valid()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          transition strategy_registry_events%ROWTYPE;
          head strategy_registry_heads%ROWTYPE;
        BEGIN
          SELECT * INTO transition FROM strategy_registry_events
          WHERE id=NEW.registry_event_id;
          SELECT * INTO head FROM strategy_registry_heads
          WHERE channel='internal_paper';
          IF transition.id IS NULL OR transition.event_type NOT IN ('PROMOTE','ROLLBACK')
            OR transition.effective_session IS NULL
            OR NEW.decision_session < transition.effective_session
            OR NEW.from_definition_id <> transition.from_definition_id
            OR NEW.to_definition_id <> transition.to_definition_id
            OR NEW.champion_generation <> transition.champion_generation
            OR head.pending_event_id <> transition.id THEN
            RAISE EXCEPTION 'strategy registry activation authority is invalid'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_registry_activations_valid BEFORE INSERT "
        "ON strategy_registry_activations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_strategy_registry_activation_valid()"
    )
    _install_registry_head_replay_guard()


def _install_registry_head_replay_guard() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION nexus_strategy_registry_head_monotonic()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          latest_event_id uuid;
          latest_sequence bigint;
          event_count bigint;
          activation_count bigint;
          expected_active_definition_id uuid;
          expected_active_generation bigint;
          expected_auto_promotion_paused boolean := false;
          expected_pending_event_id uuid;
          expected_pending_effective_session date;
          unactivated_transition_count bigint;
          pending_transition strategy_registry_events%ROWTYPE;
        BEGIN
          IF NEW.channel <> OLD.channel
            OR NEW.version <> OLD.version + 1
            OR NEW.event_sequence < OLD.event_sequence
            OR NEW.event_sequence > OLD.event_sequence + 1
            OR NEW.active_generation < OLD.active_generation
            OR NEW.active_generation > OLD.active_generation + 1 THEN
            RAISE EXCEPTION 'strategy registry head regressed' USING ERRCODE='23514';
          END IF;

          SELECT count(*),max(sequence) INTO event_count,latest_sequence
          FROM strategy_registry_events WHERE channel='internal_paper';
          SELECT id INTO latest_event_id FROM strategy_registry_events
          WHERE channel='internal_paper' ORDER BY sequence DESC LIMIT 1;
          SELECT count(*) INTO activation_count FROM strategy_registry_activations;

          SELECT a.to_definition_id,a.champion_generation
          INTO expected_active_definition_id,expected_active_generation
          FROM strategy_registry_activations a
          JOIN strategy_registry_events e ON e.id=a.registry_event_id
          ORDER BY e.sequence DESC LIMIT 1;
          IF expected_active_definition_id IS NULL THEN
            SELECT to_definition_id,champion_generation
            INTO expected_active_definition_id,expected_active_generation
            FROM strategy_registry_events
            WHERE channel='internal_paper' AND sequence=1 AND event_type='GENESIS';
          END IF;

          SELECT event_type='PAUSE' INTO expected_auto_promotion_paused
          FROM strategy_registry_events
          WHERE channel='internal_paper' AND event_type IN ('PAUSE','RESUME')
          ORDER BY sequence DESC LIMIT 1;
          expected_auto_promotion_paused := COALESCE(
            expected_auto_promotion_paused,false
          );

          SELECT count(*) INTO unactivated_transition_count
          FROM strategy_registry_events e
          LEFT JOIN strategy_registry_activations a ON a.registry_event_id=e.id
          WHERE e.channel='internal_paper'
            AND e.event_type IN ('PROMOTE','ROLLBACK') AND a.id IS NULL;
          IF unactivated_transition_count = 1 THEN
            SELECT e.* INTO pending_transition
            FROM strategy_registry_events e
            LEFT JOIN strategy_registry_activations a ON a.registry_event_id=e.id
            WHERE e.channel='internal_paper'
              AND e.event_type IN ('PROMOTE','ROLLBACK') AND a.id IS NULL;
            expected_pending_event_id := pending_transition.id;
            expected_pending_effective_session := pending_transition.effective_session;
            IF pending_transition.from_definition_id <> expected_active_definition_id
              OR pending_transition.to_definition_id = expected_active_definition_id
              OR pending_transition.champion_generation <> expected_active_generation + 1
              OR pending_transition.effective_session IS NULL THEN
              RAISE EXCEPTION 'strategy registry pending transition is invalid'
                USING ERRCODE='23514';
            END IF;
          ELSIF unactivated_transition_count > 1 THEN
            RAISE EXCEPTION 'strategy registry has multiple pending transitions'
              USING ERRCODE='23514';
          END IF;

          IF event_count < 1 OR latest_event_id IS NULL
            OR expected_active_definition_id IS NULL
            OR expected_active_generation IS NULL
            OR NEW.last_event_id IS DISTINCT FROM latest_event_id
            OR NEW.event_sequence IS DISTINCT FROM latest_sequence
            OR NEW.active_definition_id IS DISTINCT FROM expected_active_definition_id
            OR NEW.active_generation IS DISTINCT FROM expected_active_generation
            OR NEW.auto_promotion_paused IS DISTINCT FROM expected_auto_promotion_paused
            OR NEW.pending_event_id IS DISTINCT FROM expected_pending_event_id
            OR NEW.pending_effective_session
              IS DISTINCT FROM expected_pending_effective_session
            OR NEW.version IS DISTINCT FROM event_count + activation_count THEN
            RAISE EXCEPTION 'strategy registry head lacks immutable replay authority'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def _drop_guards() -> None:
    _restore_registry_head_monotonic_guard()
    op.execute(
        "DROP TRIGGER trg_strategy_experiment_events_family_complete "
        "ON strategy_experiment_events"
    )
    op.execute(
        "DROP TRIGGER trg_strategy_experiment_candidate_projections_family_complete "
        "ON strategy_experiment_candidate_projections"
    )
    op.execute("DROP FUNCTION nexus_strategy_family_projection_deferred_valid()")
    op.execute(
        "DROP TRIGGER trg_strategy_experiment_state_projections_valid "
        "ON strategy_experiment_state_projections"
    )
    op.execute("DROP FUNCTION nexus_strategy_family_projection_valid()")
    op.execute(
        "DROP TRIGGER trg_strategy_registry_activations_valid "
        "ON strategy_registry_activations"
    )
    op.execute("DROP FUNCTION nexus_strategy_registry_activation_valid()")
    op.execute(
        "DROP TRIGGER trg_strategy_experiment_candidate_projections_valid "
        "ON strategy_experiment_candidate_projections"
    )
    op.execute("DROP FUNCTION nexus_strategy_candidate_projection_valid()")
    op.execute(
        "DROP TRIGGER trg_strategy_research_lineage_projections_monotonic "
        "ON strategy_research_lineage_projections"
    )
    op.execute("DROP FUNCTION nexus_strategy_lineage_projection_monotonic()")
    for table_name in reversed(_NEW_AUTHORITY_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")


def _restore_registry_head_monotonic_guard() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION nexus_strategy_registry_head_monotonic()
        RETURNS trigger LANGUAGE plpgsql AS $$
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


def _refuse_downgrade_with_authority_facts() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM strategy_candidate_validation_results)
            OR EXISTS (SELECT 1 FROM strategy_oos_exposures)
            OR EXISTS (SELECT 1 FROM strategy_experiment_oos_exposure_links)
            OR EXISTS (SELECT 1 FROM strategy_oos_evidence_links)
            OR EXISTS (SELECT 1 FROM strategy_registry_activations)
            OR EXISTS (
              SELECT 1 FROM strategy_experiment_candidate_projections
              WHERE current_state <> 'ADMITTED' OR version <> 1
            )
            OR EXISTS (
              SELECT 1 FROM strategy_research_lineage_projections p
              JOIN strategy_research_lineages l ON l.id=p.lineage_id
              WHERE p.cumulative_oos_trial_count <> l.cumulative_oos_trial_count
                OR p.version <> 1
            )
            OR EXISTS (
              SELECT 1 FROM strategy_experiment_families
              WHERE baseline_backtest_run_id IS NOT NULL
            )
          THEN
            RAISE EXCEPTION 'strategy evolution authority facts prevent downgrade'
              USING ERRCODE='23514';
          END IF;
        END;
        $$
        """
    )
