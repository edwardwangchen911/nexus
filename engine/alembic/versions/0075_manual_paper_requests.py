"""Independent manual request identities; preserve all old business facts."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0075_manual_paper_requests"
down_revision = "0074_economic_plan_windows"
branch_labels = None
depends_on = None


_MANUAL_COMPLETION_BRANCH = """
                OR (prior_status='researching' AND NEW.to_status='completed'
                  AND NEW.result_code='MANUAL_RESEARCH_COMPLETED'
                  AND EXISTS (
                    SELECT 1 FROM public.manual_paper_requests r
                    JOIN public.agent_paper_cycles c ON c.id=NEW.cycle_id
                    JOIN public.agent_paper_cycle_events first
                      ON first.cycle_id=c.id AND first.sequence=1
                    JOIN public.agent_portfolio_synthesis_runs run ON run.cycle_id=c.id
                    JOIN public.agent_portfolio_synthesis_outputs output ON output.run_id=run.id
                    JOIN LATERAL (
                      SELECT status,cycle_id FROM public.manual_paper_request_events
                      WHERE request_id=r.id ORDER BY sequence DESC LIMIT 1
                    ) receipt ON receipt.status='active' AND receipt.cycle_id=c.id
                    WHERE r.mode='research_only' AND r.account_id=c.account_id
                      AND r.candidate_batch_id=c.candidate_batch_id
                      AND c.decision_time >= r.requested_at
                      AND first.fact_ids->>'manual_request_id'=r.id::text
                      AND first.fact_ids->>'manual_mode'=r.mode
                      AND first.fact_ids->>'formal_model_config_id'=r.model_config_id::text
                      AND first.fact_ids->>'formal_model_hash'=r.model_hash
                      AND run.model_config_id=r.model_config_id AND run.model_hash=r.model_hash
                      AND run.status='completed' AND run.settled_cost_usd IS NOT NULL
                      AND output.status='completed'
                      AND NEW.fact_ids->>'portfolio_synthesis_output_id'=output.id::text
                      AND NOT EXISTS (SELECT 1 FROM public.strategy_decisions d
                                      WHERE d.agent_paper_cycle_id=c.id)
                  ))
"""
_CHAIN_ANCHOR = "OR (prior_status='decision_recorded' AND ("


def _manual_completion(enabled: bool) -> None:
    definition = op.get_bind().scalar(
        sa.text(
            "SELECT pg_get_functiondef('nexus_agent_paper_cycle_event_chain_valid()'::regprocedure)"
        )
    )
    if not isinstance(definition, str):
        raise RuntimeError("MANUAL_CYCLE_MIGRATION_INVALID")
    if enabled:
        if definition.count(_CHAIN_ANCHOR) != 1 or _MANUAL_COMPLETION_BRANCH in definition:
            raise RuntimeError("MANUAL_CYCLE_MIGRATION_INVALID")
        definition = definition.replace(_CHAIN_ANCHOR, _MANUAL_COMPLETION_BRANCH + _CHAIN_ANCHOR)
    else:
        if definition.count(_MANUAL_COMPLETION_BRANCH) != 1:
            raise RuntimeError("MANUAL_CYCLE_MIGRATION_INVALID")
        definition = definition.replace(_MANUAL_COMPLETION_BRANCH, "")
    op.execute(definition)


def upgrade() -> None:
    op.create_table(
        "manual_paper_requests",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "account_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("paper_accounts.id"),
            nullable=False,
        ),
        sa.Column(
            "candidate_batch_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_paper_candidate_batches.id"),
            nullable=False,
        ),
        sa.Column(
            "model_config_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_model_configs.id"),
            nullable=False,
        ),
        sa.Column("mode", sa.String(32), nullable=False),
        *[
            sa.Column(name, sa.String(64), nullable=False)
            for name in ("key_hash", "request_hash", "batch_hash", "model_hash")
        ],
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("account_id", "key_hash", name="uq_manual_paper_request_key"),
        sa.CheckConstraint("mode IN ('research_only','research_and_paper')", name="mode_valid"),
        sa.CheckConstraint(
            "key_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$' AND batch_hash ~ '^[0-9a-f]{64}$' AND model_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_valid",
        ),
    )
    op.create_table(
        "manual_paper_request_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("manual_paper_requests.id"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("code", sa.String(64), nullable=False),
        sa.Column(
            "cycle_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("agent_paper_cycles.id")
        ),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("request_id", "sequence", name="uq_manual_paper_event_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "status IN ('queued','preparing','active','completed','unavailable','unknown')",
            name="status_valid",
        ),
        sa.CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_valid"),
    )
    op.create_index(
        "uq_manual_paper_active_cycle",
        "manual_paper_request_events",
        ["cycle_id"],
        unique=True,
        postgresql_where=sa.text("status='active'"),
    )
    op.execute(
        "CREATE FUNCTION nexus_manual_request_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'MANUAL_PAPER_HISTORY_IMMUTABLE'; END $$"
    )
    for table in ("manual_paper_requests", "manual_paper_request_events"):
        op.execute(
            f"CREATE TRIGGER manual_history_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_manual_request_immutable()"
        )

    _manual_completion(True)
    op.execute("""
      CREATE FUNCTION nexus_manual_research_execution_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.agent_paper_cycle_id IS NOT NULL AND EXISTS (
          SELECT 1 FROM public.agent_paper_cycle_events first
          JOIN public.manual_paper_requests r ON first.fact_ids->>'manual_request_id'=r.id::text
          WHERE first.cycle_id=NEW.agent_paper_cycle_id AND first.sequence=1
            AND r.mode='research_only'
        ) THEN
          RAISE EXCEPTION 'MANUAL_RESEARCH_ONLY_EXECUTION_FORBIDDEN' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("""
      CREATE TRIGGER manual_research_execution_guard BEFORE INSERT ON strategy_decisions
      FOR EACH ROW EXECUTE FUNCTION nexus_manual_research_execution_guard()
    """)


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM manual_paper_requests) THEN RAISE EXCEPTION 'MANUAL_PAPER_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    _manual_completion(False)
    op.execute("DROP TRIGGER manual_research_execution_guard ON strategy_decisions")
    op.execute("DROP FUNCTION nexus_manual_research_execution_guard()")
    op.drop_table("manual_paper_request_events")
    op.drop_table("manual_paper_requests")
    op.execute("DROP FUNCTION nexus_manual_request_immutable()")
