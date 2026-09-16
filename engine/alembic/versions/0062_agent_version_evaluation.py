"""Add isolated incumbent/challenger Agent-version evaluation authority.

Revision ID: 0062_agent_version_evaluation
Revises: 0061_agent_paper_job_isolation
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0062_agent_version_evaluation"
down_revision: str | Sequence[str] | None = "0061_agent_paper_job_isolation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOCK_KEY = "nexus:agent-version-evaluation:v1"
_TABLES = (
    "agent_version_evaluations",
    "agent_version_cycle_pairs",
    "agent_version_evaluation_decisions",
)


def _create_tables() -> None:
    op.create_table(
        "agent_version_evaluations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("incumbent_account_id", sa.Uuid(), nullable=False),
        sa.Column("challenger_account_id", sa.Uuid(), nullable=False),
        sa.Column("instrument_id", sa.Uuid(), nullable=False),
        sa.Column("calendar_authority_id", sa.Uuid(), nullable=False),
        sa.Column("incumbent_version_source_run_id", sa.Uuid(), nullable=False),
        sa.Column("challenger_prompt_version_id", sa.Uuid(), nullable=False),
        sa.Column("challenger_model_config_id", sa.Uuid(), nullable=False),
        sa.Column("incumbent_prompt_hash", sa.String(64), nullable=False),
        sa.Column("incumbent_model_hash", sa.String(64), nullable=False),
        sa.Column("challenger_prompt_hash", sa.String(64), nullable=False),
        sa.Column("challenger_model_hash", sa.String(64), nullable=False),
        sa.Column(
            "train_slots", postgresql.ARRAY(sa.DateTime(timezone=True)), nullable=False
        ),
        sa.Column(
            "oos_slots", postgresql.ARRAY(sa.DateTime(timezone=True)), nullable=False
        ),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("policy_hash", sa.String(64), nullable=False),
        sa.Column("authority_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["incumbent_account_id"], ["paper_accounts.id"]),
        sa.ForeignKeyConstraint(["challenger_account_id"], ["paper_accounts.id"]),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.ForeignKeyConstraint(
            ["calendar_authority_id"], ["exchange_calendar_authorities.id"]
        ),
        sa.ForeignKeyConstraint(
            ["incumbent_version_source_run_id"], ["agent_research_runs.id"]
        ),
        sa.ForeignKeyConstraint(
            ["challenger_prompt_version_id"], ["agent_prompt_versions.id"]
        ),
        sa.ForeignKeyConstraint(
            ["challenger_model_config_id"], ["agent_model_configs.id"]
        ),
        sa.UniqueConstraint(
            "incumbent_account_id", name="uq_agent_version_evaluations_incumbent"
        ),
        sa.UniqueConstraint(
            "challenger_account_id", name="uq_agent_version_evaluations_challenger"
        ),
        sa.UniqueConstraint("authority_hash", name="uq_agent_version_evaluations_authority"),
        sa.CheckConstraint(
            "incumbent_account_id <> challenger_account_id",
            name="distinct_accounts",
        ),
        sa.CheckConstraint(
            "policy_version = 'agent_version_evaluation_v1'",
            name="policy_version",
        ),
        sa.CheckConstraint(
            "cardinality(train_slots) = 4",
            name="train_slots",
        ),
        sa.CheckConstraint(
            "cardinality(oos_slots) = 20",
            name="oos_slots",
        ),
        sa.CheckConstraint(
            "incumbent_prompt_hash ~ '^[0-9a-f]{64}$' "
            "AND incumbent_model_hash ~ '^[0-9a-f]{64}$' "
            "AND challenger_prompt_hash ~ '^[0-9a-f]{64}$' "
            "AND challenger_model_hash ~ '^[0-9a-f]{64}$' "
            "AND policy_hash ~ '^[0-9a-f]{64}$' "
            "AND authority_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
        sa.CheckConstraint(
            "incumbent_prompt_hash <> challenger_prompt_hash "
            "OR incumbent_model_hash <> challenger_model_hash",
            name="distinct_versions",
        ),
    )
    op.create_table(
        "agent_version_cycle_pairs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("evaluation_id", sa.Uuid(), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("decision_slot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("incumbent_cycle_id", sa.Uuid(), nullable=False),
        sa.Column("challenger_cycle_id", sa.Uuid(), nullable=False),
        sa.Column("incumbent_run_id", sa.Uuid(), nullable=False),
        sa.Column("challenger_run_id", sa.Uuid(), nullable=False),
        sa.Column("market_evidence_hash", sa.String(64), nullable=False),
        sa.Column("starting_projection_hash", sa.String(64), nullable=False),
        sa.Column("pair_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["evaluation_id"], ["agent_version_evaluations.id"]),
        sa.ForeignKeyConstraint(["incumbent_cycle_id"], ["agent_paper_cycles.id"]),
        sa.ForeignKeyConstraint(["challenger_cycle_id"], ["agent_paper_cycles.id"]),
        sa.ForeignKeyConstraint(["incumbent_run_id"], ["agent_research_runs.id"]),
        sa.ForeignKeyConstraint(["challenger_run_id"], ["agent_research_runs.id"]),
        sa.UniqueConstraint(
            "evaluation_id",
            "decision_slot_at",
            name="uq_agent_version_cycle_pairs_slot",
        ),
        sa.UniqueConstraint(
            "incumbent_cycle_id", name="uq_agent_version_cycle_pairs_inc_cycle"
        ),
        sa.UniqueConstraint(
            "challenger_cycle_id", name="uq_agent_version_cycle_pairs_chal_cycle"
        ),
        sa.UniqueConstraint("incumbent_run_id", name="uq_agent_version_cycle_pairs_inc_run"),
        sa.UniqueConstraint("challenger_run_id", name="uq_agent_version_cycle_pairs_chal_run"),
        sa.UniqueConstraint("pair_hash", name="uq_agent_version_cycle_pairs_hash"),
        sa.CheckConstraint("phase IN ('train','oos')", name="phase"),
        sa.CheckConstraint(
            "incumbent_cycle_id <> challenger_cycle_id",
            name="distinct_cycles",
        ),
        sa.CheckConstraint(
            "incumbent_run_id <> challenger_run_id",
            name="distinct_runs",
        ),
        sa.CheckConstraint(
            "market_evidence_hash ~ '^[0-9a-f]{64}$' "
            "AND starting_projection_hash ~ '^[0-9a-f]{64}$' "
            "AND pair_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )
    op.create_table(
        "agent_version_evaluation_decisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("evaluation_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("metric_report", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metric_hash", sa.String(64), nullable=False),
        sa.Column("selected_version_source_run_id", sa.Uuid(), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        sa.Column("decision_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["evaluation_id"], ["agent_version_evaluations.id"]),
        sa.ForeignKeyConstraint(
            ["selected_version_source_run_id"], ["agent_research_runs.id"]
        ),
        sa.UniqueConstraint(
            "evaluation_id", name="uq_agent_version_decisions_evaluation"
        ),
        sa.UniqueConstraint("decision_hash", name="uq_agent_version_decisions_hash"),
        sa.CheckConstraint(
            "status IN ('PASS','FAIL','UNKNOWN')",
            name="status",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metric_report) = 'object'",
            name="metric_report",
        ),
        sa.CheckConstraint(
            "metric_hash ~ '^[0-9a-f]{64}$' AND decision_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
        sa.CheckConstraint(
            "btrim(reason_code) <> ''", name="reason_code"
        ),
    )


def _install_evaluation_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_evaluation_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          incumbent_capability varchar(50);
          challenger_capability varchar(50);
          incumbent_capital numeric(38,12);
          challenger_capital numeric(38,12);
          incumbent_provider varchar(50);
          challenger_provider varchar(50);
          incumbent_instrument uuid;
          incumbent_prompt varchar(64);
          incumbent_model varchar(64);
          challenger_model varchar(64);
          registered_slot_count integer;
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:v1', 0)
          );
          IF EXISTS (
            SELECT 1 FROM public.agent_version_evaluations evaluation
            LEFT JOIN public.agent_version_evaluation_decisions decision
              ON decision.evaluation_id=evaluation.id
            WHERE decision.id IS NULL
          ) THEN
            RAISE EXCEPTION 'another Agent version evaluation is open' USING ERRCODE='23514';
          END IF;
          SELECT paper.capability,paper.initial_capital,account.provider
          INTO incumbent_capability,incumbent_capital,incumbent_provider
          FROM public.paper_accounts paper JOIN public.accounts account
            ON account.id=paper.account_id
          WHERE paper.id=NEW.incumbent_account_id AND account.status='ACTIVE';
          SELECT paper.capability,paper.initial_capital,account.provider
          INTO challenger_capability,challenger_capital,challenger_provider
          FROM public.paper_accounts paper JOIN public.accounts account
            ON account.id=paper.account_id
          WHERE paper.id=NEW.challenger_account_id AND account.status='ACTIVE';
          IF incumbent_capability IS DISTINCT FROM 'simulation_write_internal'
             OR challenger_capability IS DISTINCT FROM 'simulation_write_internal'
             OR incumbent_provider IS DISTINCT FROM 'internal_paper_evaluation'
             OR challenger_provider IS DISTINCT FROM 'internal_paper_evaluation'
             OR incumbent_capital IS DISTINCT FROM challenger_capital
             OR EXISTS (
               SELECT 1 FROM public.paper_accounts paper
               JOIN public.account_external_id_envelopes envelope
                 ON envelope.account_id=paper.account_id
               WHERE paper.id IN (NEW.incumbent_account_id,NEW.challenger_account_id)
             )
          THEN
            RAISE EXCEPTION 'Agent version evaluation accounts are invalid'
              USING ERRCODE='23514';
          END IF;
          SELECT instrument_id,prompt_hash,model_hash
          INTO incumbent_instrument,incumbent_prompt,incumbent_model
          FROM public.agent_research_runs
          WHERE id=NEW.incumbent_version_source_run_id;
          SELECT config_hash INTO challenger_model FROM public.agent_model_configs
          WHERE id=NEW.challenger_model_config_id;
          IF incumbent_instrument IS DISTINCT FROM NEW.instrument_id
             OR incumbent_prompt IS DISTINCT FROM NEW.incumbent_prompt_hash
             OR incumbent_model IS DISTINCT FROM NEW.incumbent_model_hash
             OR challenger_model IS DISTINCT FROM NEW.challenger_model_hash
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_prompt_versions
               WHERE id=NEW.challenger_prompt_version_id AND role='orchestrator'
             )
             OR EXISTS (
               SELECT 1 FROM unnest(NEW.train_slots || NEW.oos_slots) slot
               WHERE NOT EXISTS (
                 SELECT 1 FROM public.instrument_aliases alias
                 WHERE alias.instrument_id=NEW.instrument_id AND alias.symbol='AAPL'
                 AND alias.valid_from <= (slot AT TIME ZONE 'America/New_York')::date
                   AND (alias.valid_to IS NULL OR alias.valid_to >=
                        (slot AT TIME ZONE 'America/New_York')::date)
               )
             )
             OR NOT EXISTS (
               SELECT 1 FROM public.exchange_calendar_authorities calendar
               WHERE calendar.id=NEW.calendar_authority_id
                 AND calendar.exchange='XNYS'
                 AND calendar.completeness_status='complete'
             )
          THEN
            RAISE EXCEPTION 'Agent version evaluation authority is invalid'
              USING ERRCODE='23514';
          END IF;
          SELECT count(DISTINCT slot)
          INTO registered_slot_count
          FROM unnest(NEW.train_slots || NEW.oos_slots) slot
          JOIN public.exchange_calendar_sessions session_row
            ON session_row.calendar_authority_id=NEW.calendar_authority_id
           AND session_row.session=(slot AT TIME ZONE 'America/New_York')::date
           AND slot >= session_row.market_open_at
           AND slot <= session_row.market_close_at;
          IF (SELECT count(DISTINCT slot) FROM unnest(NEW.train_slots) slot) <> 4
             OR (SELECT count(DISTINCT slot) FROM unnest(NEW.oos_slots) slot) <> 20
             OR registered_slot_count <> 24
             OR (SELECT count(DISTINCT (slot AT TIME ZONE 'America/New_York')::date)
                 FROM unnest(NEW.train_slots) slot) < 2
             OR (SELECT count(DISTINCT (slot AT TIME ZONE 'America/New_York')::date)
                 FROM unnest(NEW.oos_slots) slot) < 10
             OR (SELECT max(slot) FROM unnest(NEW.train_slots) slot) >=
                (SELECT min(slot) FROM unnest(NEW.oos_slots) slot)
             OR EXISTS (
               SELECT 1 FROM unnest(NEW.oos_slots) slot
               GROUP BY (slot AT TIME ZONE 'America/New_York')::date HAVING count(*) > 2
             )
          THEN
            RAISE EXCEPTION 'Agent version evaluation window is invalid'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_evaluations_valid BEFORE INSERT ON "
        "agent_version_evaluations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_evaluation_valid()"
    )


def _install_pair_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_pair_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          evaluation public.agent_version_evaluations%ROWTYPE;
          incumbent_cycle public.agent_paper_cycles%ROWTYPE;
          challenger_cycle public.agent_paper_cycles%ROWTYPE;
          incumbent_run public.agent_research_runs%ROWTYPE;
          challenger_run public.agent_research_runs%ROWTYPE;
          incumbent_decision public.strategy_decisions%ROWTYPE;
          challenger_decision public.strategy_decisions%ROWTYPE;
          expected_starting_projection_hash varchar(64);
        BEGIN
          SELECT * INTO evaluation FROM public.agent_version_evaluations
          WHERE id=NEW.evaluation_id FOR SHARE;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:' || NEW.evaluation_id::text, 0)
          );
          SELECT * INTO incumbent_cycle FROM public.agent_paper_cycles
          WHERE id=NEW.incumbent_cycle_id FOR SHARE;
          SELECT * INTO challenger_cycle FROM public.agent_paper_cycles
          WHERE id=NEW.challenger_cycle_id FOR SHARE;
          SELECT * INTO incumbent_run FROM public.agent_research_runs
          WHERE id=NEW.incumbent_run_id FOR SHARE;
          SELECT * INTO challenger_run FROM public.agent_research_runs
          WHERE id=NEW.challenger_run_id FOR SHARE;
          SELECT * INTO incumbent_decision FROM public.strategy_decisions
          WHERE agent_paper_cycle_id=NEW.incumbent_cycle_id FOR SHARE;
          SELECT * INTO challenger_decision FROM public.strategy_decisions
          WHERE agent_paper_cycle_id=NEW.challenger_cycle_id FOR SHARE;
          expected_starting_projection_hash := pg_catalog.encode(
            public.digest(
              pg_catalog.convert_to(
                'agent_version_starting_projection_pair_v1|' ||
                incumbent_decision.portfolio_projection_hash || '|' ||
                challenger_decision.portfolio_projection_hash,
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          );
          IF evaluation.id IS NULL OR incumbent_cycle.id IS NULL OR challenger_cycle.id IS NULL
             OR incumbent_run.id IS NULL OR challenger_run.id IS NULL
             OR incumbent_decision.id IS NULL OR challenger_decision.id IS NULL
             OR EXISTS (
               SELECT 1 FROM public.agent_version_evaluation_decisions decision
               WHERE decision.evaluation_id=NEW.evaluation_id
             )
             OR incumbent_cycle.account_id <> evaluation.incumbent_account_id
             OR challenger_cycle.account_id <> evaluation.challenger_account_id
             OR incumbent_cycle.decision_slot_at <> NEW.decision_slot_at
             OR challenger_cycle.decision_slot_at <> NEW.decision_slot_at
             OR NOT incumbent_cycle.execution_locked OR NOT challenger_cycle.execution_locked
             OR incumbent_cycle.decision_price_policy_id <>
                challenger_cycle.decision_price_policy_id
             OR incumbent_cycle.execution_price_policy_id <>
                challenger_cycle.execution_price_policy_id
             OR incumbent_cycle.sizing_policy_hash <> challenger_cycle.sizing_policy_hash
             OR incumbent_cycle.universe_snapshot_id <> challenger_cycle.universe_snapshot_id
             OR incumbent_cycle.candidate_hash <> challenger_cycle.candidate_hash
             OR incumbent_cycle.candidate_manifest <> challenger_cycle.candidate_manifest
             OR incumbent_run.actor_id <> 'local-agent-version-incumbent'
             OR challenger_run.actor_id <> 'local-agent-version-challenger'
             OR incumbent_run.instrument_id <> evaluation.instrument_id
             OR challenger_run.instrument_id <> evaluation.instrument_id
             OR incumbent_run.prompt_hash <> evaluation.incumbent_prompt_hash
             OR incumbent_run.model_hash <> evaluation.incumbent_model_hash
             OR challenger_run.prompt_hash <> evaluation.challenger_prompt_hash
             OR challenger_run.model_hash <> evaluation.challenger_model_hash
             OR challenger_run.prompt_version_id <> evaluation.challenger_prompt_version_id
             OR challenger_run.model_config_id <> evaluation.challenger_model_config_id
             OR incumbent_run.data_snapshot_id <> challenger_run.data_snapshot_id
             OR incumbent_run.evidence_hash <> challenger_run.evidence_hash
             OR NEW.market_evidence_hash <> incumbent_run.evidence_hash
             OR incumbent_decision.account_id <> evaluation.incumbent_account_id
             OR challenger_decision.account_id <> evaluation.challenger_account_id
             OR incumbent_decision.calendar_authority_id <> evaluation.calendar_authority_id
             OR challenger_decision.calendar_authority_id <> evaluation.calendar_authority_id
             OR incumbent_decision.source_policy_id <> challenger_decision.source_policy_id
             OR incumbent_decision.execution_source_policy_id IS DISTINCT FROM
                challenger_decision.execution_source_policy_id
             OR NEW.starting_projection_hash IS DISTINCT FROM
                expected_starting_projection_hash
             OR (NEW.phase='train' AND NOT NEW.decision_slot_at=ANY(evaluation.train_slots))
             OR (NEW.phase='oos' AND NOT NEW.decision_slot_at=ANY(evaluation.oos_slots))
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_paper_cycle_events event
               WHERE event.cycle_id=incumbent_cycle.id
                 AND event.fact_ids->'research_run_ids' ? incumbent_run.id::text
             )
             OR NOT EXISTS (
               SELECT 1 FROM public.agent_paper_cycle_events event
               WHERE event.cycle_id=challenger_cycle.id
                 AND event.fact_ids->'research_run_ids' ? challenger_run.id::text
             )
             OR EXISTS (
               SELECT 1 FROM public.broker_execution_intents intent
               JOIN public.strategy_decisions decision
                 ON decision.id=intent.strategy_decision_id
               JOIN public.broker_account_bindings binding
                 ON binding.id=intent.account_binding_id
               WHERE decision.account_id IN (
                 evaluation.incumbent_account_id,evaluation.challenger_account_id
               )
             )
          THEN
            RAISE EXCEPTION 'Agent version cycle pair is invalid' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_cycle_pairs_valid BEFORE INSERT ON "
        "agent_version_cycle_pairs FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_pair_valid()"
    )


def _install_decision_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_decision_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          incumbent_run uuid;
          earliest_challenger_run uuid;
          oos_count integer;
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:' || NEW.evaluation_id::text, 0)
          );
          SELECT incumbent_version_source_run_id INTO incumbent_run
          FROM public.agent_version_evaluations WHERE id=NEW.evaluation_id FOR SHARE;
          SELECT count(*),(
            SELECT pair.challenger_run_id FROM public.agent_version_cycle_pairs pair
            WHERE pair.evaluation_id=NEW.evaluation_id AND pair.phase='oos'
            ORDER BY pair.decision_slot_at,pair.id LIMIT 1
          ) INTO oos_count,earliest_challenger_run
          FROM public.agent_version_cycle_pairs counted
          WHERE counted.evaluation_id=NEW.evaluation_id AND counted.phase='oos';
          IF incumbent_run IS NULL
             OR (NEW.status='PASS' AND (
               oos_count <> 20 OR NEW.selected_version_source_run_id IS DISTINCT FROM
               earliest_challenger_run
             ))
             OR (NEW.status IN ('FAIL','UNKNOWN') AND
               NEW.selected_version_source_run_id IS DISTINCT FROM incumbent_run)
          THEN
            RAISE EXCEPTION 'Agent version evaluation decision is invalid'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_evaluation_decisions_valid BEFORE INSERT ON "
        "agent_version_evaluation_decisions FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_decision_valid()"
    )


def _install_external_isolation_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_external_account_denied()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:v1', 0)
          );
          IF EXISTS (
            SELECT 1 FROM public.agent_version_evaluations evaluation
            JOIN public.paper_accounts paper
              ON paper.id IN (evaluation.incumbent_account_id,evaluation.challenger_account_id)
            WHERE paper.account_id=NEW.account_id
          ) THEN
            RAISE EXCEPTION 'Agent version evaluation account cannot have an external id'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_external_account_denied "
        "BEFORE INSERT ON account_external_id_envelopes FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_external_account_denied()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_broker_intent_denied()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:v1', 0)
          );
          IF EXISTS (
            SELECT 1 FROM public.strategy_decisions decision
            JOIN public.agent_version_evaluations evaluation
              ON decision.account_id IN (
                evaluation.incumbent_account_id,evaluation.challenger_account_id
              )
            WHERE decision.id=NEW.strategy_decision_id
          ) THEN
            RAISE EXCEPTION 'Agent version evaluation account cannot create broker intent'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_broker_intent_denied "
        "BEFORE INSERT ON broker_execution_intents FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_broker_intent_denied()"
    )


def _install_cycle_scope_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_agent_version_cycle_scope_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          evaluation public.agent_version_evaluations%ROWTYPE;
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-version-evaluation:v1', 0)
          );
          SELECT * INTO evaluation FROM public.agent_version_evaluations
          WHERE NEW.account_id IN (incumbent_account_id,challenger_account_id);
          IF evaluation.id IS NULL THEN
            RETURN NEW;
          END IF;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
              'nexus:agent-version-evaluation:' || evaluation.id::text,
              0
            )
          );
          IF EXISTS (
               SELECT 1 FROM public.agent_version_evaluation_decisions decision
               WHERE decision.evaluation_id=evaluation.id
             )
             OR NOT (
               NEW.decision_slot_at=ANY(evaluation.train_slots)
               OR NEW.decision_slot_at=ANY(evaluation.oos_slots)
             )
             OR NEW.decision_session IS DISTINCT FROM
                (NEW.decision_slot_at AT TIME ZONE 'America/New_York')::date
             OR NOT EXISTS (
               SELECT 1 FROM public.exchange_calendar_sessions session_row
               WHERE session_row.calendar_authority_id=evaluation.calendar_authority_id
                 AND session_row.session=NEW.decision_session
                 AND NEW.decision_slot_at >= session_row.market_open_at
                 AND NEW.decision_slot_at <= session_row.market_close_at
             )
          THEN
            RAISE EXCEPTION 'Agent version evaluation cycle is outside frozen scope'
              USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_version_cycle_scope_valid "
        "BEFORE INSERT ON agent_paper_cycles FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_version_cycle_scope_valid()"
    )


def _install_immutability() -> None:
    for table in _TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_reject_paper_fact_mutation()"
        )


def upgrade() -> None:
    _create_tables()
    _install_evaluation_guard()
    _install_pair_guard()
    _install_decision_guard()
    _install_external_isolation_guards()
    _install_cycle_scope_guard()
    _install_immutability()


def downgrade() -> None:
    op.execute(
        f"SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('{_LOCK_KEY}', 0))"
    )
    op.execute(
        "LOCK TABLE public.agent_version_evaluation_decisions, "
        "public.agent_version_cycle_pairs, public.agent_version_evaluations "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.agent_version_evaluations)
             OR EXISTS (SELECT 1 FROM public.agent_version_cycle_pairs)
             OR EXISTS (SELECT 1 FROM public.agent_version_evaluation_decisions)
          THEN
            RAISE EXCEPTION 'agent version evaluation facts prevent downgrade'
              USING ERRCODE='P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        "DROP TRIGGER trg_agent_version_broker_intent_denied ON broker_execution_intents"
    )
    op.execute("DROP FUNCTION nexus_agent_version_broker_intent_denied()")
    op.execute(
        "DROP TRIGGER trg_agent_version_external_account_denied "
        "ON account_external_id_envelopes"
    )
    op.execute("DROP FUNCTION nexus_agent_version_external_account_denied()")
    op.execute("DROP TRIGGER trg_agent_version_cycle_scope_valid ON agent_paper_cycles")
    op.execute("DROP FUNCTION nexus_agent_version_cycle_scope_valid()")
    for table in reversed(_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.execute(
        "DROP TRIGGER trg_agent_version_evaluation_decisions_valid "
        "ON agent_version_evaluation_decisions"
    )
    op.execute("DROP FUNCTION nexus_agent_version_decision_valid()")
    op.execute(
        "DROP TRIGGER trg_agent_version_cycle_pairs_valid ON agent_version_cycle_pairs"
    )
    op.execute("DROP FUNCTION nexus_agent_version_pair_valid()")
    op.execute(
        "DROP TRIGGER trg_agent_version_evaluations_valid ON agent_version_evaluations"
    )
    op.execute("DROP FUNCTION nexus_agent_version_evaluation_valid()")
    op.drop_table("agent_version_evaluation_decisions")
    op.drop_table("agent_version_cycle_pairs")
    op.drop_table("agent_version_evaluations")
