"""Add bounded portfolio-management persistence and fail-closed database guards.

Revision ID: 0065_portfolio_management
Revises: 0064_prompt_release_channel
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0065_portfolio_management"
down_revision: str | Sequence[str] | None = "0064_prompt_release_channel"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = (
    "agent_paper_candidate_batches",
    "paper_initial_target_approvals",
    "paper_target_versions",
    "agent_portfolio_synthesis_runs",
    "agent_portfolio_synthesis_outputs",
    "agent_portfolio_synthesis_budget_reservations",
)
_LINKS = (
    ("agent_paper_cycles", "candidate_batch_id", "fk_agent_paper_cycles_candidate_batch"),
    (
        "strategy_decisions",
        "portfolio_synthesis_output_id",
        "fk_strategy_decisions_portfolio_synthesis",
    ),
    ("paper_order_batches", "target_version_id", "fk_paper_order_batches_target_version"),
)
_NEW_SIZING = "(candidate_batch_id IS NULL AND sizing_policy_version = 'agent_paper_sizing_v1') OR (candidate_batch_id IS NOT NULL AND sizing_policy_version = 'portfolio_allocation_v1' AND sizing_policy_hash = 'd7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7')"
_NEW_ORIGIN = "(decision_origin = 'registered_strategy' AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 3 AND champion_generation >= 1 AND execution_source_policy_id IS NULL AND quality_report_id IS NOT NULL AND reason = 'WEEKLY_REBALANCE' AND execution_session > decision_session) OR (decision_origin = 'agent_research' AND strategy_id = 'agent_research' AND ((strategy_version = 'agent_paper_decision_v1' AND portfolio_synthesis_output_id IS NULL) OR (strategy_version = 'agent_portfolio_proposal_v1' AND portfolio_synthesis_output_id IS NOT NULL AND strategy_parameters_hash = 'd7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7')) AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 0 AND quality_report_id IS NULL AND reason = 'AGENT_RESEARCH' AND ((source_policy_id = '2573d63a-fb6d-53d3-8d44-207766a78322'::uuid AND execution_source_policy_id = 'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid AND execution_session > decision_session) OR (source_policy_id = '4d24df4a-b510-541b-8a6a-fe7db5afaac4'::uuid AND execution_source_policy_id = 'cf525f07-4e55-5f84-a839-60d845819ec5'::uuid AND execution_session = decision_session)))"
_OLD_ORIGIN = _NEW_ORIGIN.replace(
    "((strategy_version = 'agent_paper_decision_v1' AND portfolio_synthesis_output_id IS NULL) OR "
    "(strategy_version = 'agent_portfolio_proposal_v1' AND portfolio_synthesis_output_id IS NOT NULL "
    "AND strategy_parameters_hash = 'd7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7'))",
    "strategy_version = 'agent_paper_decision_v1'",
)


def _create_tables() -> None:
    op.create_table(
        "agent_paper_candidate_batches",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("account_id", sa.UUID(), nullable=False),
        sa.Column("universe_snapshot_id", sa.UUID(), nullable=False),
        sa.Column("candidate_instrument_ids", sa.ARRAY(sa.UUID()), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("batch_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "batch_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_agent_paper_candidate_batches_hash_canonical"),
        ),
        sa.CheckConstraint(
            "source = 'explicit_research_scope'",
            name=op.f("ck_agent_paper_candidate_batches_source_fixed"),
        ),
        sa.CheckConstraint(
            "array_ndims(candidate_instrument_ids) = 1 AND cardinality(candidate_instrument_ids) BETWEEN 1 AND 5 AND array_position(candidate_instrument_ids, NULL) IS NULL",
            name=op.f("ck_agent_paper_candidate_batches_candidates_bounded"),
        ),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name=op.f("fk_agent_paper_candidate_batches_account_id_paper_accounts"),
        ),
        sa.ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name=op.f("fk_agent_paper_candidate_batches_universe_snapshot_id_universe_snapshots"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_paper_candidate_batches")),
        sa.UniqueConstraint("batch_hash", name="uq_agent_paper_candidate_batches_hash"),
        sa.UniqueConstraint(
            "id", "account_id", "universe_snapshot_id", name="uq_candidate_batches_scope"
        ),
    )
    op.create_table(
        "paper_initial_target_approvals",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("account_id", sa.UUID(), nullable=False),
        sa.Column("portfolio_id", sa.UUID(), nullable=False),
        sa.Column("projection_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("projection_hash", sa.String(length=64), nullable=False),
        sa.Column("allocation_policy_version", sa.String(length=50), nullable=False),
        sa.Column("allocation_policy_hash", sa.String(length=64), nullable=False),
        sa.Column("approved_by", sa.String(length=100), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approval_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "allocation_policy_version = 'portfolio_allocation_v1' AND allocation_policy_hash = 'd7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7'",
            name=op.f("ck_paper_initial_target_approvals_policy_fixed"),
        ),
        sa.CheckConstraint(
            "btrim(approved_by) <> ''",
            name=op.f("ck_paper_initial_target_approvals_approver_nonempty"),
        ),
        sa.CheckConstraint(
            "projection_hash ~ '^[0-9a-f]{64}$' AND allocation_policy_hash ~ '^[0-9a-f]{64}$' AND approval_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_paper_initial_target_approvals_hashes_canonical"),
        ),
        sa.CheckConstraint(
            "approved_at >= projection_as_of",
            name=op.f("ck_paper_initial_target_approvals_times_ordered"),
        ),
        sa.ForeignKeyConstraint(
            ["account_id", "portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
            name="fk_paper_initial_target_approvals_account",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_paper_initial_target_approvals")),
        sa.UniqueConstraint("account_id", name="uq_paper_initial_target_approvals_account"),
    )
    op.create_table(
        "paper_target_versions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("account_id", sa.UUID(), nullable=False),
        sa.Column("portfolio_id", sa.UUID(), nullable=False),
        sa.Column("version_seq", sa.BigInteger(), nullable=False),
        sa.Column("strategy_decision_id", sa.UUID(), nullable=False),
        sa.Column("previous_target_version_id", sa.UUID(), nullable=True),
        sa.Column("target_content_hash", sa.String(length=64), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("activation_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "target_content_hash ~ '^[0-9a-f]{64}$' AND activation_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_paper_target_versions_hashes_canonical"),
        ),
        sa.CheckConstraint(
            "previous_target_version_id IS NULL OR previous_target_version_id <> id",
            name=op.f("ck_paper_target_versions_previous_not_self"),
        ),
        sa.CheckConstraint(
            "version_seq >= 1", name=op.f("ck_paper_target_versions_sequence_positive")
        ),
        sa.ForeignKeyConstraint(
            ["account_id", "portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
            name="fk_paper_target_versions_account",
        ),
        sa.ForeignKeyConstraint(
            ["previous_target_version_id", "account_id"],
            ["paper_target_versions.id", "paper_target_versions.account_id"],
            name="fk_paper_target_versions_previous",
        ),
        sa.ForeignKeyConstraint(
            ["strategy_decision_id", "account_id"],
            ["strategy_decisions.id", "strategy_decisions.account_id"],
            name="fk_paper_target_versions_decision",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_paper_target_versions")),
        sa.UniqueConstraint(
            "account_id", "version_seq", name="uq_paper_target_versions_account_seq"
        ),
        sa.UniqueConstraint("id", "account_id", name="uq_paper_target_versions_id_account"),
        sa.UniqueConstraint("previous_target_version_id", name="uq_paper_target_versions_previous"),
        sa.UniqueConstraint("strategy_decision_id", name="uq_paper_target_versions_decision"),
    )
    op.create_table(
        "agent_portfolio_synthesis_runs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("cycle_id", sa.UUID(), nullable=False),
        sa.Column("candidate_batch_id", sa.UUID(), nullable=False),
        sa.Column("research_output_ids", sa.ARRAY(sa.UUID()), nullable=False),
        sa.Column("active_target_version_id", sa.UUID(), nullable=True),
        sa.Column("projection_hash", sa.String(length=64), nullable=False),
        sa.Column("prompt_version_id", sa.UUID(), nullable=False),
        sa.Column("model_config_id", sa.UUID(), nullable=False),
        sa.Column("prompt_hash", sa.String(length=64), nullable=False),
        sa.Column("model_hash", sa.String(length=64), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("frozen_input", postgresql.JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="prepared", nullable=False),
        sa.Column("pricing_version", sa.String(length=100), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("settled_cost_usd", sa.Numeric(precision=38, scale=12), nullable=True),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(idempotency_key) <> '' AND btrim(pricing_version) <> ''",
            name=op.f("ck_agent_portfolio_synthesis_runs_identity_nonempty"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(frozen_input) = 'object'",
            name=op.f("ck_agent_portfolio_synthesis_runs_frozen_input_object"),
        ),
        sa.CheckConstraint(
            "projection_hash ~ '^[0-9a-f]{64}$' AND prompt_hash ~ '^[0-9a-f]{64}$' AND model_hash ~ '^[0-9a-f]{64}$' AND input_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_agent_portfolio_synthesis_runs_hashes_canonical"),
        ),
        sa.CheckConstraint(
            "status IN ('prepared','running','completed','abstained','failed')",
            name=op.f("ck_agent_portfolio_synthesis_runs_status_allowed"),
        ),
        sa.CheckConstraint(
            "cardinality(research_output_ids) BETWEEN 1 AND 5 AND array_ndims(research_output_ids) = 1 AND array_position(research_output_ids, NULL) IS NULL",
            name=op.f("ck_agent_portfolio_synthesis_runs_outputs_bounded"),
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= decision_time",
            name=op.f("ck_agent_portfolio_synthesis_runs_times_ordered"),
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name=op.f("ck_agent_portfolio_synthesis_runs_input_nonnegative"),
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name=op.f("ck_agent_portfolio_synthesis_runs_output_nonnegative"),
        ),
        sa.CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd >= 0",
            name=op.f("ck_agent_portfolio_synthesis_runs_cost_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["active_target_version_id"],
            ["paper_target_versions.id"],
            name=op.f(
                "fk_agent_portfolio_synthesis_runs_active_target_version_id_paper_target_versions"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_batch_id"],
            ["agent_paper_candidate_batches.id"],
            name=op.f(
                "fk_agent_portfolio_synthesis_runs_candidate_batch_id_agent_paper_candidate_batches"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["cycle_id"],
            ["agent_paper_cycles.id"],
            name=op.f("fk_agent_portfolio_synthesis_runs_cycle_id_agent_paper_cycles"),
        ),
        sa.ForeignKeyConstraint(
            ["model_config_id"],
            ["agent_model_configs.id"],
            name=op.f("fk_agent_portfolio_synthesis_runs_model_config_id_agent_model_configs"),
        ),
        sa.ForeignKeyConstraint(
            ["prompt_version_id"],
            ["agent_prompt_versions.id"],
            name=op.f("fk_agent_portfolio_synthesis_runs_prompt_version_id_agent_prompt_versions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_portfolio_synthesis_runs")),
        sa.UniqueConstraint("cycle_id", name="uq_portfolio_synthesis_runs_cycle"),
        sa.UniqueConstraint("idempotency_key", name="uq_portfolio_synthesis_runs_idempotency"),
    )
    op.create_table(
        "agent_portfolio_synthesis_outputs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("intent", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("binding_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND intent IS NOT NULL AND jsonb_typeof(intent) = 'object') OR (status IN ('abstained','failed') AND intent IS NULL)",
            name=op.f("ck_agent_portfolio_synthesis_outputs_intent_bound"),
        ),
        sa.CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$' AND binding_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_agent_portfolio_synthesis_outputs_hashes_canonical"),
        ),
        sa.CheckConstraint(
            "status IN ('completed','abstained','failed')",
            name=op.f("ck_agent_portfolio_synthesis_outputs_status_allowed"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["agent_portfolio_synthesis_runs.id"],
            name=op.f("fk_agent_portfolio_synthesis_outputs_run_id_agent_portfolio_synthesis_runs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_portfolio_synthesis_outputs")),
        sa.UniqueConstraint("run_id", name="uq_portfolio_synthesis_outputs_run"),
    )
    op.create_table(
        "agent_portfolio_synthesis_budget_reservations",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("pricing_version", sa.String(length=100), nullable=False),
        sa.Column("reserved_usd", sa.Numeric(precision=38, scale=12), nullable=False),
        sa.Column("settled_usd", sa.Numeric(precision=38, scale=12), nullable=True),
        sa.Column("state", sa.String(length=16), server_default="reserved", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(state = 'reserved' AND settled_usd IS NULL) OR (state = 'settled' AND settled_usd IS NOT NULL) OR (state = 'released' AND settled_usd IS NOT NULL AND settled_usd = 0)",
            name=op.f("ck_synthesis_budget_settlement_bound"),
        ),
        sa.CheckConstraint(
            "state IN ('reserved','settled','released')",
            name=op.f("ck_agent_portfolio_synthesis_budget_reservations_state_allowed"),
        ),
        sa.CheckConstraint(
            "reserved_usd >= 0",
            name=op.f("ck_synthesis_budget_reserved_nonnegative"),
        ),
        sa.CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd",
            name=op.f("ck_synthesis_budget_settled_bounded"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["agent_portfolio_synthesis_runs.id"],
            name=op.f(
                "fk_agent_portfolio_synthesis_budget_reservations_run_id_agent_portfolio_synthesis_runs"
            ),
        ),
        sa.PrimaryKeyConstraint(
            "id", name=op.f("pk_agent_portfolio_synthesis_budget_reservations")
        ),
        sa.UniqueConstraint("run_id", name="uq_portfolio_synthesis_budget_run"),
    )


def _replace_check(table: str, name: str, expression: str) -> None:
    op.drop_constraint(op.f(f"ck_{table}_{name}"), table, type_="check")
    op.create_check_constraint(op.f(f"ck_{table}_{name}"), table, expression)


def upgrade() -> None:
    _create_tables()
    for table, column, _ in _LINKS:
        op.add_column(table, sa.Column(column, sa.UUID(), nullable=True))
    op.create_foreign_key(
        _LINKS[0][2],
        "agent_paper_cycles",
        "agent_paper_candidate_batches",
        ["candidate_batch_id", "account_id", "universe_snapshot_id"],
        ["id", "account_id", "universe_snapshot_id"],
    )
    op.create_foreign_key(
        _LINKS[1][2],
        "strategy_decisions",
        "agent_portfolio_synthesis_outputs",
        ["portfolio_synthesis_output_id"],
        ["id"],
    )
    op.create_foreign_key(
        _LINKS[2][2],
        "paper_order_batches",
        "paper_target_versions",
        ["target_version_id", "account_id"],
        ["id", "account_id"],
    )
    _replace_check("agent_paper_cycles", "sizing_policy_fixed", _NEW_SIZING)
    _replace_check("strategy_decisions", "origin_authority", _NEW_ORIGIN)
    _create_guards()


def _create_guards() -> None:
    for table in _TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        mutable = table in (
            "agent_portfolio_synthesis_runs",
            "agent_portfolio_synthesis_budget_reservations",
        )
        events = "DELETE" if mutable else "UPDATE OR DELETE"
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE {events} ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_agent_immutable_change()"
        )
    op.execute("""
        CREATE FUNCTION nexus_portfolio_candidates_valid() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF NEW.candidate_instrument_ids IS DISTINCT FROM
             (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(NEW.candidate_instrument_ids) x)
          THEN
            RAISE EXCEPTION 'candidate IDs must be canonical unique UUIDs' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END; $$
    """)
    op.execute(
        "CREATE TRIGGER trg_portfolio_candidates_valid BEFORE INSERT ON agent_paper_candidate_batches "
        "FOR EACH ROW EXECUTE FUNCTION nexus_portfolio_candidates_valid()"
    )
    op.execute("""
        CREATE FUNCTION nexus_portfolio_synthesis_transition() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF OLD.status IN ('completed','abstained','failed')
             OR (to_jsonb(NEW) - ARRAY['status','input_tokens','output_tokens',
                   'settled_cost_usd','completed_at'])
                IS DISTINCT FROM
                (to_jsonb(OLD) - ARRAY['status','input_tokens','output_tokens',
                   'settled_cost_usd','completed_at'])
             OR NOT (
                 (OLD.status='prepared' AND NEW.status IN ('running','failed')) OR
                 (OLD.status='running' AND NEW.status IN ('completed','abstained','failed'))
             )
          THEN
            RAISE EXCEPTION 'invalid synthesis transition or frozen authority mutation'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END; $$
    """)
    op.execute(
        "CREATE TRIGGER trg_portfolio_synthesis_transition BEFORE UPDATE ON agent_portfolio_synthesis_runs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_portfolio_synthesis_transition()"
    )
    op.execute("""
        CREATE FUNCTION nexus_portfolio_synthesis_budget_transition() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF OLD.state <> 'reserved' OR NEW.state NOT IN ('settled','released')
             OR (to_jsonb(NEW) - ARRAY['state','settled_usd'])
                IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','settled_usd'])
          THEN
            RAISE EXCEPTION 'invalid synthesis budget transition or frozen authority mutation'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END; $$
    """)
    op.execute(
        "CREATE TRIGGER trg_portfolio_synthesis_budget_transition BEFORE UPDATE "
        "ON agent_portfolio_synthesis_budget_reservations "
        "FOR EACH ROW EXECUTE FUNCTION nexus_portfolio_synthesis_budget_transition()"
    )
    op.execute("""
        CREATE FUNCTION nexus_portfolio_synthesis_scope_valid() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE cycle public.agent_paper_cycles%ROWTYPE;
        BEGIN
          SELECT * INTO cycle FROM public.agent_paper_cycles WHERE id=NEW.cycle_id FOR SHARE;
          IF cycle.id IS NULL OR cycle.candidate_batch_id IS DISTINCT FROM NEW.candidate_batch_id
             OR cycle.decision_time IS DISTINCT FROM NEW.decision_time
             OR (NEW.active_target_version_id IS NOT NULL AND NOT EXISTS (
                 SELECT 1 FROM public.paper_target_versions target
                 WHERE target.id=NEW.active_target_version_id AND target.account_id=cycle.account_id
             ))
          THEN
            RAISE EXCEPTION 'synthesis cycle authority mismatch' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END; $$
    """)
    op.execute(
        "CREATE TRIGGER trg_portfolio_synthesis_scope_valid BEFORE INSERT ON agent_portfolio_synthesis_runs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_portfolio_synthesis_scope_valid()"
    )
    op.execute("""
        CREATE FUNCTION nexus_portfolio_proposal_synthesis_valid() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF NEW.portfolio_synthesis_output_id IS NOT NULL AND (
             NEW.decision_origin <> 'agent_research'
             OR NEW.strategy_version <> 'agent_portfolio_proposal_v1'
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_portfolio_synthesis_outputs output
               JOIN public.agent_portfolio_synthesis_runs run ON run.id=output.run_id
               JOIN public.agent_paper_cycles cycle ON cycle.id=run.cycle_id
               WHERE output.id=NEW.portfolio_synthesis_output_id
                 AND output.status='completed' AND run.status='completed'
                 AND run.cycle_id=NEW.agent_paper_cycle_id
                 AND cycle.account_id=NEW.account_id AND cycle.portfolio_id=NEW.portfolio_id
                 AND cycle.candidate_batch_id=run.candidate_batch_id
                 AND run.projection_hash=NEW.portfolio_projection_hash
             ))
          THEN
            RAISE EXCEPTION 'portfolio proposal requires completed bound synthesis'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END; $$
    """)
    op.execute(
        "CREATE TRIGGER trg_portfolio_proposal_synthesis_valid BEFORE INSERT ON strategy_decisions "
        "FOR EACH ROW EXECUTE FUNCTION nexus_portfolio_proposal_synthesis_valid()"
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE "
        + ", ".join(_TABLES + tuple(x[0] for x in _LINKS))
        + " IN ACCESS EXCLUSIVE MODE"
    )
    existence = " OR ".join(f"EXISTS (SELECT 1 FROM {table})" for table in _TABLES)
    existence += " OR " + " OR ".join(
        f"EXISTS (SELECT 1 FROM {table} WHERE {column} IS NOT NULL)" for table, column, _ in _LINKS
    )
    op.execute(
        "DO $$ BEGIN IF "
        + existence
        + " THEN RAISE EXCEPTION 'portfolio management facts prevent downgrade' "
        "USING ERRCODE='P3D01'; END IF; END $$"
    )
    op.execute("DROP TRIGGER trg_portfolio_proposal_synthesis_valid ON strategy_decisions")
    op.execute("DROP FUNCTION nexus_portfolio_proposal_synthesis_valid()")
    _replace_check(
        "agent_paper_cycles",
        "sizing_policy_fixed",
        "sizing_policy_version = 'agent_paper_sizing_v1'",
    )
    _replace_check("strategy_decisions", "origin_authority", _OLD_ORIGIN)
    for table, column, constraint in reversed(_LINKS):
        op.drop_constraint(constraint, table, type_="foreignkey")
        op.drop_column(table, column)
    for table in reversed(_TABLES):
        op.drop_table(table)
    for function in (
        "nexus_portfolio_candidates_valid",
        "nexus_portfolio_synthesis_transition",
        "nexus_portfolio_synthesis_budget_transition",
        "nexus_portfolio_synthesis_scope_valid",
    ):
        op.execute(f"DROP FUNCTION {function}()")
