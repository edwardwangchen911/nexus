"""Add immutable Agent paper-cycle and decision authority.

Revision ID: 0056_agent_paper_cycle
Revises: 0055_schwab_intraday_job_v3
"""

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0056_agent_paper_cycle"
down_revision: str | Sequence[str] | None = "0055_schwab_intraday_job_v3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PAPER_DECISION_SOURCE_POLICY_ID = "2573d63a-fb6d-53d3-8d44-207766a78322"
PAPER_EXECUTION_SOURCE_POLICY_ID = "e99532cc-e24c-52a8-8165-c8076325fbd2"
_LOCK_KEY = "nexus:agent-paper-cycle:v1"

_DECISION_POLICY_DOCUMENT: dict[str, object] = {
    "selected_source": "alpaca",
    "fallback": "disabled",
    "alpaca": {
        "feed": "sip",
        "delivery_mode": "delayed",
        "interval": "1Day",
        "adjustment": "all",
        "market_scope": "US",
    },
    "purposes": [
        "paper_decision_valuation",
        "paper_position_sizing",
        "paper_risk",
        "paper_nonzero_position_performance",
    ],
    "availability_rule": "published_and_available_at_or_before_decision_time",
}
_EXECUTION_POLICY_DOCUMENT: dict[str, object] = {
    "selected_source": "alpaca",
    "fallback": "disabled",
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "interval": "1Min",
        "adjustment": "raw",
        "market_scope": "US",
        "coverage": "iex_exchange_subset",
    },
    "purpose": "paper_fill_only",
    "window": {
        "continuous_trading_minutes": 5,
        "row_count": 5,
        "single_instrument": True,
        "single_manifest_authority": True,
    },
    "availability_rule": "observed_received_and_available_at_or_before_evaluated_at",
    "raw_artifact_required": True,
    "hash_binding": ["raw_artifact", "manifest", "authority"],
    "reject": [
        "missing_minute",
        "duplicate_minute",
        "nonpositive_price",
        "negative_volume",
        "outside_window",
        "source_mismatch",
        "feed_mismatch",
    ],
    "quote": {
        "complete_same_source_two_sided_slippage_bps": 10,
        "missing_slippage_bps": 25,
        "one_sided_is_complete": False,
    },
}


def _canonical_payload(document: dict[str, object]) -> dict[str, object]:
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return {**document, "content_hash": hashlib.sha256(canonical.encode()).hexdigest()}


PAPER_DECISION_SOURCE_POLICY_PAYLOAD = _canonical_payload(_DECISION_POLICY_DOCUMENT)
PAPER_EXECUTION_SOURCE_POLICY_PAYLOAD = _canonical_payload(_EXECUTION_POLICY_DOCUMENT)


def _seed_policies() -> None:
    statement = text(
        "INSERT INTO source_policies (id,key,version,schema_version,payload) "
        "VALUES (CAST(:id AS uuid),:key,:version,'1',CAST(:payload AS jsonb))"
    )
    for policy_id, key, version, payload in (
        (
            PAPER_DECISION_SOURCE_POLICY_ID,
            "paper_decision_price",
            "paper_decision_price_policy_v1",
            PAPER_DECISION_SOURCE_POLICY_PAYLOAD,
        ),
        (
            PAPER_EXECUTION_SOURCE_POLICY_ID,
            "paper_execution_price",
            "paper_execution_price_policy_v1",
            PAPER_EXECUTION_SOURCE_POLICY_PAYLOAD,
        ),
    ):
        op.execute(
            statement.bindparams(
                id=policy_id,
                key=key,
                version=version,
                payload=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            )
        )


def _create_cycle_tables() -> None:
    op.create_table(
        "agent_paper_cycles",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("account_id", sa.UUID(), nullable=False),
        sa.Column("portfolio_id", sa.UUID(), nullable=False),
        sa.Column("decision_session", sa.Date(), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_session", sa.Date(), nullable=False),
        sa.Column("universe_snapshot_id", sa.UUID(), nullable=False),
        sa.Column("quality_report_id", sa.UUID(), nullable=False),
        sa.Column("decision_price_policy_id", sa.UUID(), nullable=False),
        sa.Column("execution_price_policy_id", sa.UUID(), nullable=False),
        sa.Column("candidate_manifest", postgresql.JSONB(), nullable=False),
        sa.Column("candidate_hash", sa.String(64), nullable=False),
        sa.Column("sizing_policy_version", sa.String(50), nullable=False),
        sa.Column("sizing_policy_hash", sa.String(64), nullable=False),
        sa.Column("execution_locked", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name="pk_agent_paper_cycles"),
        sa.UniqueConstraint(
            "account_id", "decision_session", name="uq_agent_paper_cycles_account_session"
        ),
        sa.ForeignKeyConstraint(
            ["account_id", "portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
            name="fk_agent_paper_cycles_paper_account",
        ),
        sa.ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name="fk_agent_paper_cycles_universe",
        ),
        sa.ForeignKeyConstraint(
            ["quality_report_id"],
            ["quality_reports.id"],
            name="fk_agent_paper_cycles_quality",
        ),
        sa.ForeignKeyConstraint(
            ["decision_price_policy_id"],
            ["source_policies.id"],
            name="fk_agent_paper_cycles_decision_policy",
        ),
        sa.ForeignKeyConstraint(
            ["execution_price_policy_id"],
            ["source_policies.id"],
            name="fk_agent_paper_cycles_execution_policy",
        ),
        sa.CheckConstraint("execution_session > decision_session", name="sessions_ordered"),
        sa.CheckConstraint(
            "decision_price_policy_id = '2573d63a-fb6d-53d3-8d44-207766a78322'::uuid "
            "AND execution_price_policy_id = 'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid",
            name="policies_fixed",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(candidate_manifest) = 'array' "
            "AND jsonb_array_length(candidate_manifest) BETWEEN 0 AND 5",
            name="candidate_manifest_bounded",
        ),
        sa.CheckConstraint(
            "candidate_hash ~ '^[0-9a-f]{64}$' AND sizing_policy_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint(
            "sizing_policy_version = 'agent_paper_sizing_v1'",
            name="sizing_policy_fixed",
        ),
        sa.CheckConstraint("execution_locked", name="execution_locked"),
    )
    op.create_table(
        "agent_paper_cycle_events",
        sa.Column("cycle_id", sa.UUID(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("from_status", sa.String(30), nullable=True),
        sa.Column("to_status", sa.String(30), nullable=False),
        sa.Column("result_code", sa.String(100), nullable=False),
        sa.Column("fact_ids", postgresql.JSONB(), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("cycle_id", "sequence", name="pk_agent_paper_cycle_events"),
        sa.ForeignKeyConstraint(
            ["cycle_id"],
            ["agent_paper_cycles.id"],
            name="fk_agent_paper_cycle_events_cycle",
        ),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "(from_status IS NULL OR from_status IN "
            "('prepared','researching','decision_recorded','risk_evaluated','awaiting_market',"
            "'partially_filled')) AND to_status IN "
            "('prepared','researching','decision_recorded','risk_evaluated','awaiting_market',"
            "'partially_filled','completed','unavailable','failed')",
            name="statuses_allowed",
        ),
        sa.CheckConstraint("btrim(result_code) <> ''", name="result_code_nonempty"),
        sa.CheckConstraint(
            "jsonb_typeof(fact_ids) = 'object'",
            name="fact_ids_object",
        ),
        sa.CheckConstraint(
            "event_hash ~ '^[0-9a-f]{64}$'",
            name="event_hash_canonical",
        ),
    )


def _extend_decisions() -> None:
    op.add_column(
        "strategy_decisions",
        sa.Column(
            "decision_origin",
            sa.String(30),
            server_default="registered_strategy",
            nullable=False,
        ),
    )
    op.add_column(
        "strategy_decisions", sa.Column("execution_source_policy_id", sa.UUID(), nullable=True)
    )
    op.create_foreign_key(
        "fk_strategy_decisions_execution_source_policy",
        "strategy_decisions",
        "source_policies",
        ["execution_source_policy_id"],
        ["id"],
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_reason_weekly"), "strategy_decisions", type_="check"
    )
    for column_name in ("strategy_definition_id", "registry_event_id", "champion_generation"):
        op.alter_column("strategy_decisions", column_name, nullable=True)
    op.create_check_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        "btrim(strategy_id) <> '' AND btrim(strategy_version) <> ''",
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_origin_allowed"),
        "strategy_decisions",
        "decision_origin IN ('registered_strategy','agent_research')",
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_reason_allowed"),
        "strategy_decisions",
        "reason IN ('WEEKLY_REBALANCE','AGENT_RESEARCH')",
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_origin_authority"),
        "strategy_decisions",
        "(decision_origin = 'registered_strategy' "
        "AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 3 "
        "AND champion_generation >= 1 AND execution_source_policy_id IS NULL "
        "AND reason = 'WEEKLY_REBALANCE') OR "
        "(decision_origin = 'agent_research' "
        "AND strategy_id = 'agent_research' "
        "AND strategy_version = 'agent_paper_decision_v1' "
        "AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 0 "
        "AND source_policy_id = '2573d63a-fb6d-53d3-8d44-207766a78322'::uuid "
        "AND execution_source_policy_id = 'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid "
        "AND reason = 'AGENT_RESEARCH')",
    )


def _create_output_bindings() -> None:
    op.create_table(
        "strategy_decision_agent_outputs",
        sa.Column("strategy_decision_id", sa.UUID(), nullable=False),
        sa.Column("agent_research_output_id", sa.UUID(), nullable=False),
        sa.Column("intent_hash", sa.String(64), nullable=False),
        sa.Column("sizing_action", sa.String(16), nullable=False),
        sa.Column("binding_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint(
            "strategy_decision_id",
            "agent_research_output_id",
            name="pk_strategy_decision_agent_outputs",
        ),
        sa.UniqueConstraint(
            "agent_research_output_id", name="uq_strategy_decision_agent_outputs_output"
        ),
        sa.ForeignKeyConstraint(
            ["strategy_decision_id"],
            ["strategy_decisions.id"],
            name="fk_strategy_decision_agent_outputs_decision",
        ),
        sa.ForeignKeyConstraint(
            ["agent_research_output_id"],
            ["agent_research_outputs.id"],
            name="fk_strategy_decision_agent_outputs_output",
        ),
        sa.CheckConstraint(
            "intent_hash ~ '^[0-9a-f]{64}$' AND binding_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint(
            "sizing_action IN ('increase','reduce','maintain','none')",
            name="sizing_action_allowed",
        ),
    )


def _install_guards() -> None:
    for table_name in (
        "agent_paper_cycles",
        "agent_paper_cycle_events",
        "strategy_decision_agent_outputs",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable BEFORE UPDATE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_reject_paper_fact_mutation()"
        )

    op.execute(
        """
        CREATE FUNCTION nexus_agent_paper_cycle_event_chain_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          account_ref uuid;
          prior_sequence integer;
          prior_status varchar(30);
          transition_valid boolean;
        BEGIN
          SELECT account_id INTO account_ref FROM public.agent_paper_cycles WHERE id=NEW.cycle_id;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('nexus:agent-paper-cycle:' || account_ref::text, 0)
          );
          SELECT sequence,to_status INTO prior_sequence,prior_status
          FROM public.agent_paper_cycle_events WHERE cycle_id=NEW.cycle_id
          ORDER BY sequence DESC LIMIT 1;
          IF prior_sequence IS NULL THEN
            transition_valid := NEW.sequence=1 AND NEW.from_status IS NULL
              AND NEW.to_status='prepared';
          ELSE
            transition_valid := NEW.sequence=prior_sequence+1
              AND NEW.from_status=prior_status
              AND (
                (prior_status='prepared' AND NEW.to_status IN ('researching','unavailable','failed'))
                OR (prior_status='researching' AND NEW.to_status IN
                    ('decision_recorded','unavailable','failed'))
                OR (prior_status='decision_recorded' AND NEW.to_status IN ('risk_evaluated','failed'))
                OR (prior_status='risk_evaluated' AND NEW.to_status IN
                    ('awaiting_market','completed','failed'))
                OR (prior_status='awaiting_market' AND NEW.to_status IN
                    ('partially_filled','completed','failed'))
                OR (prior_status='partially_filled' AND NEW.to_status IN
                    ('partially_filled','completed','failed'))
              );
          END IF;
          IF NOT COALESCE(transition_valid,FALSE) THEN
            RAISE EXCEPTION 'agent paper cycle event chain is invalid' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_paper_cycle_events_chain BEFORE INSERT "
        "ON agent_paper_cycle_events FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_paper_cycle_event_chain_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_agent_output_binding_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          decision_origin_ref varchar(30);
          output_status_ref varchar(16);
          run_version_ref varchar(100);
          instrument_ref uuid;
          binding_count integer;
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
              'nexus:agent-paper-decision:' || NEW.strategy_decision_id::text, 0
            )
          );
          SELECT decision_origin INTO decision_origin_ref FROM public.strategy_decisions
          WHERE id=NEW.strategy_decision_id FOR SHARE;
          SELECT output.status,run.pricing_version,run.instrument_id
          INTO output_status_ref,run_version_ref,instrument_ref
          FROM public.agent_research_outputs output
          JOIN public.agent_research_runs run ON run.id=output.run_id
          WHERE output.id=NEW.agent_research_output_id FOR SHARE OF output,run;
          SELECT count(*) INTO binding_count FROM public.strategy_decision_agent_outputs
          WHERE strategy_decision_id=NEW.strategy_decision_id;
          IF decision_origin_ref IS DISTINCT FROM 'agent_research'
             OR output_status_ref IS DISTINCT FROM 'completed'
             OR run_version_ref IS DISTINCT FROM 'codex-sdk-chatgpt-v4-actionable'
             OR binding_count >= 5
             OR EXISTS (
               SELECT 1 FROM public.strategy_decision_agent_outputs binding
               JOIN public.agent_research_outputs existing_output
                 ON existing_output.id=binding.agent_research_output_id
               JOIN public.agent_research_runs existing_run ON existing_run.id=existing_output.run_id
               WHERE binding.strategy_decision_id=NEW.strategy_decision_id
                 AND existing_run.instrument_id=instrument_ref
             )
          THEN
            RAISE EXCEPTION 'agent decision output binding is invalid' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_decision_agent_outputs_valid BEFORE INSERT "
        "ON strategy_decision_agent_outputs FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_output_binding_valid()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_agent_decision_batch_bindings_complete()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          origin_ref varchar(30);
          binding_count integer;
        BEGIN
          SELECT decision_origin INTO origin_ref FROM public.strategy_decisions
          WHERE id=NEW.strategy_decision_id;
          IF origin_ref='agent_research' THEN
            SELECT count(*) INTO binding_count FROM public.strategy_decision_agent_outputs
            WHERE strategy_decision_id=NEW.strategy_decision_id;
            IF binding_count NOT BETWEEN 1 AND 5 THEN
              RAISE EXCEPTION 'agent decision requires one to five completed v4 outputs'
                USING ERRCODE='23514';
            END IF;
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_order_batches_agent_bindings "
        "AFTER INSERT ON paper_order_batches DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_agent_decision_batch_bindings_complete()"
    )


def upgrade() -> None:
    _seed_policies()
    _create_cycle_tables()
    _extend_decisions()
    _create_output_bindings()
    _install_guards()


def downgrade() -> None:
    op.execute(
        f"SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('{_LOCK_KEY}', 0))"
    )
    op.execute(
        "LOCK TABLE public.agent_paper_cycle_events, public.agent_paper_cycles, "
        "public.strategy_decision_agent_outputs, public.paper_order_batches, "
        "public.strategy_decisions, public.source_policies IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.agent_paper_cycles)
             OR EXISTS (SELECT 1 FROM public.agent_paper_cycle_events)
             OR EXISTS (SELECT 1 FROM public.strategy_decision_agent_outputs)
             OR EXISTS (
               SELECT 1 FROM public.strategy_decisions WHERE decision_origin='agent_research'
             )
             OR EXISTS (
               SELECT 1 FROM public.paper_order_batches batch
               JOIN public.strategy_decisions decision ON decision.id=batch.strategy_decision_id
               WHERE decision.decision_origin='agent_research'
             )
          THEN
            RAISE EXCEPTION 'agent paper facts prevent downgrade' USING ERRCODE='23514';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER trg_paper_order_batches_agent_bindings ON paper_order_batches")
    op.execute("DROP FUNCTION nexus_agent_decision_batch_bindings_complete()")
    op.execute(
        "DROP TRIGGER trg_strategy_decision_agent_outputs_valid ON strategy_decision_agent_outputs"
    )
    op.execute("DROP FUNCTION nexus_agent_output_binding_valid()")
    op.execute("DROP TRIGGER trg_agent_paper_cycle_events_chain ON agent_paper_cycle_events")
    op.execute("DROP FUNCTION nexus_agent_paper_cycle_event_chain_valid()")
    for table_name in (
        "strategy_decision_agent_outputs",
        "agent_paper_cycle_events",
        "agent_paper_cycles",
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.drop_table("strategy_decision_agent_outputs")
    op.drop_constraint(
        op.f("ck_strategy_decisions_origin_authority"), "strategy_decisions", type_="check"
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_reason_allowed"), "strategy_decisions", type_="check"
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_origin_allowed"), "strategy_decisions", type_="check"
    )
    op.drop_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        type_="check",
    )
    op.drop_constraint(
        "fk_strategy_decisions_execution_source_policy", "strategy_decisions", type_="foreignkey"
    )
    for column_name in ("strategy_definition_id", "registry_event_id", "champion_generation"):
        op.alter_column("strategy_decisions", column_name, nullable=False)
    op.create_check_constraint(
        op.f("ck_strategy_decisions_strategy_content_addressed"),
        "strategy_decisions",
        "btrim(strategy_id) <> '' AND btrim(strategy_version) <> '' AND champion_generation >= 1",
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_reason_weekly"),
        "strategy_decisions",
        "reason = 'WEEKLY_REBALANCE'",
    )
    op.drop_column("strategy_decisions", "execution_source_policy_id")
    op.drop_column("strategy_decisions", "decision_origin")
    op.drop_table("agent_paper_cycle_events")
    op.drop_table("agent_paper_cycles")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    for policy_id, key, version, payload in (
        (
            PAPER_DECISION_SOURCE_POLICY_ID,
            "paper_decision_price",
            "paper_decision_price_policy_v1",
            PAPER_DECISION_SOURCE_POLICY_PAYLOAD,
        ),
        (
            PAPER_EXECUTION_SOURCE_POLICY_ID,
            "paper_execution_price",
            "paper_execution_price_policy_v1",
            PAPER_EXECUTION_SOURCE_POLICY_PAYLOAD,
        ),
    ):
        deleted = op.get_bind().execute(
            text(
                "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) AND key=:key "
                "AND version=:version AND schema_version='1' AND payload=CAST(:payload AS jsonb)"
            ),
            {
                "id": policy_id,
                "key": key,
                "version": version,
                "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
        )
        if deleted is not None and deleted.rowcount != 1:
            raise RuntimeError("agent paper source policy downgrade mismatch")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
