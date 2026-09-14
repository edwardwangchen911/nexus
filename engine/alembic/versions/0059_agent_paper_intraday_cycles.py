"""Allow multiple immutable Agent paper cycles within one market session.

Revision ID: 0059_agent_paper_intraday_cycles
Revises: 0058_agent_paper_schwab_realtime
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0059_agent_paper_intraday_cycles"
down_revision: str | Sequence[str] | None = "0058_agent_paper_schwab_realtime"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOCK_KEY = "nexus:agent-paper-intraday-cycles:v1"


def upgrade() -> None:
    op.execute(f"SELECT pg_advisory_xact_lock(hashtextextended('{_LOCK_KEY}', 0))")
    op.execute(
        "LOCK TABLE public.agent_paper_cycles, public.strategy_decisions, "
        "public.risk_query_authorities IN ACCESS EXCLUSIVE MODE"
    )

    op.add_column(
        "agent_paper_cycles",
        sa.Column("decision_slot_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute("ALTER TABLE agent_paper_cycles DISABLE TRIGGER trg_agent_paper_cycles_immutable")
    op.execute(
        "UPDATE agent_paper_cycles SET decision_slot_at = "
        "date_trunc('hour', decision_time) "
        "+ floor(extract(minute FROM decision_time) / 5) * interval '5 minutes'"
    )
    op.execute("ALTER TABLE agent_paper_cycles ENABLE TRIGGER trg_agent_paper_cycles_immutable")
    op.alter_column("agent_paper_cycles", "decision_slot_at", nullable=False)
    op.drop_constraint(
        "uq_agent_paper_cycles_account_session", "agent_paper_cycles", type_="unique"
    )
    op.create_unique_constraint(
        "uq_agent_paper_cycles_account_slot",
        "agent_paper_cycles",
        ["account_id", "decision_slot_at"],
    )

    op.add_column(
        "strategy_decisions",
        sa.Column("agent_paper_cycle_id", sa.UUID(), nullable=True),
    )
    op.execute("ALTER TABLE strategy_decisions DISABLE TRIGGER trg_strategy_decisions_immutable")
    op.execute(
        "UPDATE strategy_decisions AS decision SET agent_paper_cycle_id=cycle.id "
        "FROM agent_paper_cycles AS cycle "
        "WHERE decision.decision_origin='agent_research' "
        "AND cycle.account_id=decision.account_id "
        "AND cycle.portfolio_id=decision.portfolio_id "
        "AND cycle.decision_session=decision.decision_session "
        "AND cycle.decision_time=decision.decision_time"
    )
    op.execute("ALTER TABLE strategy_decisions ENABLE TRIGGER trg_strategy_decisions_immutable")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.strategy_decisions
            WHERE decision_origin='agent_research' AND agent_paper_cycle_id IS NULL
          ) THEN
            RAISE EXCEPTION 'AGENT_PAPER_DECISION_CYCLE_MISSING' USING ERRCODE='23514';
          END IF;
        END $$
        """
    )
    op.drop_constraint(
        "uq_strategy_decisions_account_session", "strategy_decisions", type_="unique"
    )
    op.create_index(
        "uq_strategy_decisions_registered_account_session",
        "strategy_decisions",
        ["account_id", "decision_session"],
        unique=True,
        postgresql_where=sa.text("decision_origin='registered_strategy'"),
    )
    op.create_index(
        "uq_strategy_decisions_agent_cycle",
        "strategy_decisions",
        ["agent_paper_cycle_id"],
        unique=True,
        postgresql_where=sa.text("decision_origin='agent_research'"),
    )
    op.create_foreign_key(
        "fk_strategy_decisions_agent_paper_cycle",
        "strategy_decisions",
        "agent_paper_cycles",
        ["agent_paper_cycle_id"],
        ["id"],
    )
    op.create_check_constraint(
        op.f("ck_strategy_decisions_agent_cycle_binding"),
        "strategy_decisions",
        "((decision_origin='registered_strategy' AND agent_paper_cycle_id IS NULL) OR "
        "(decision_origin='agent_research' AND agent_paper_cycle_id IS NOT NULL))",
    )
    op.execute(
        """
        CREATE FUNCTION nexus_agent_decision_cycle_valid()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          cycle public.agent_paper_cycles%ROWTYPE;
        BEGIN
          IF NEW.decision_origin='agent_research' THEN
            SELECT * INTO cycle FROM public.agent_paper_cycles
            WHERE id=NEW.agent_paper_cycle_id FOR SHARE;
            IF NOT FOUND OR cycle.account_id IS DISTINCT FROM NEW.account_id
               OR cycle.portfolio_id IS DISTINCT FROM NEW.portfolio_id
               OR cycle.decision_session IS DISTINCT FROM NEW.decision_session
               OR cycle.decision_time IS DISTINCT FROM NEW.decision_time
               OR cycle.execution_session IS DISTINCT FROM NEW.execution_session
               OR cycle.decision_price_policy_id IS DISTINCT FROM NEW.source_policy_id
               OR cycle.execution_price_policy_id IS DISTINCT FROM NEW.execution_source_policy_id
            THEN
              RAISE EXCEPTION 'agent decision cycle binding is invalid' USING ERRCODE='23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_strategy_decisions_agent_cycle_valid "
        "BEFORE INSERT OR UPDATE OF agent_paper_cycle_id,account_id,portfolio_id,"
        "decision_session,decision_time,execution_session,source_policy_id,"
        "execution_source_policy_id ON strategy_decisions FOR EACH ROW "
        "EXECUTE FUNCTION nexus_agent_decision_cycle_valid()"
    )

    op.drop_constraint(
        "uq_risk_query_authorities_scope", "risk_query_authorities", type_="unique"
    )
    op.create_unique_constraint(
        "uq_risk_query_authorities_scope_available",
        "risk_query_authorities",
        [
            "source_policy_id",
            "universe_snapshot_id",
            "decision_session",
            "kind",
            "available_at",
        ],
    )


def downgrade() -> None:
    op.execute(f"SELECT pg_advisory_xact_lock(hashtextextended('{_LOCK_KEY}', 0))")
    op.execute(
        "LOCK TABLE public.agent_paper_cycles, public.strategy_decisions, "
        "public.risk_query_authorities IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.agent_paper_cycles
            GROUP BY account_id,decision_session HAVING count(*) > 1
          ) OR EXISTS (
            SELECT 1 FROM public.strategy_decisions
            GROUP BY account_id,decision_session HAVING count(*) > 1
          ) OR EXISTS (
            SELECT 1 FROM public.risk_query_authorities
            GROUP BY source_policy_id,universe_snapshot_id,decision_session,kind
            HAVING count(*) > 1
          ) THEN
            RAISE EXCEPTION 'AGENT_PAPER_INTRADAY_FACTS_EXIST' USING ERRCODE='23514';
          END IF;
        END $$
        """
    )

    op.drop_constraint(
        "uq_risk_query_authorities_scope_available",
        "risk_query_authorities",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_risk_query_authorities_scope",
        "risk_query_authorities",
        ["source_policy_id", "universe_snapshot_id", "decision_session", "kind"],
    )

    op.execute(
        "DROP TRIGGER trg_strategy_decisions_agent_cycle_valid ON strategy_decisions"
    )
    op.execute("DROP FUNCTION nexus_agent_decision_cycle_valid()")
    op.drop_constraint(
        op.f("ck_strategy_decisions_agent_cycle_binding"),
        "strategy_decisions",
        type_="check",
    )
    op.drop_constraint(
        "fk_strategy_decisions_agent_paper_cycle", "strategy_decisions", type_="foreignkey"
    )
    op.drop_index("uq_strategy_decisions_agent_cycle", table_name="strategy_decisions")
    op.drop_index(
        "uq_strategy_decisions_registered_account_session", table_name="strategy_decisions"
    )
    op.create_unique_constraint(
        "uq_strategy_decisions_account_session",
        "strategy_decisions",
        ["account_id", "decision_session"],
    )
    op.drop_column("strategy_decisions", "agent_paper_cycle_id")

    op.drop_constraint(
        "uq_agent_paper_cycles_account_slot", "agent_paper_cycles", type_="unique"
    )
    op.create_unique_constraint(
        "uq_agent_paper_cycles_account_session",
        "agent_paper_cycles",
        ["account_id", "decision_session"],
    )
    op.drop_column("agent_paper_cycles", "decision_slot_at")
