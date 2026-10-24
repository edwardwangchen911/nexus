"""Durable price-review plans, checks, episodes and pending trigger work.

Revision ID: 0087_research_monitoring
Revises: 0086_accounting_adoption
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0087_research_monitoring"
down_revision = "0086_accounting_adoption"
branch_labels = None
depends_on = None


def _id():
    return sa.Column("id", pg.UUID(), primary_key=True)


def _scope(*, primary_key=False):
    return (
        sa.Column(
            "account_id",
            pg.UUID(),
            sa.ForeignKey("paper_accounts.id"),
            nullable=False,
            primary_key=primary_key,
        ),
        sa.Column(
            "instrument_id",
            pg.UUID(),
            sa.ForeignKey("instruments.id"),
            nullable=False,
            primary_key=primary_key,
        ),
    )


def _created():
    return sa.Column(
        "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )


def upgrade():
    op.create_table(
        "research_monitor_plans",
        _id(),
        *_scope(),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column(
            "source_run_id", pg.UUID(), sa.ForeignKey("agent_research_runs.id"), nullable=False
        ),
        sa.Column(
            "source_output_id",
            pg.UUID(),
            sa.ForeignKey("agent_research_outputs.id"),
            nullable=False,
        ),
        sa.Column(
            "data_snapshot_id", pg.UUID(), sa.ForeignKey("data_snapshots.id"), nullable=False
        ),
        sa.Column("predecessor_id", pg.UUID(), sa.ForeignKey("research_monitor_plans.id")),
        sa.Column("condition_hash", sa.String(64), nullable=False),
        sa.Column("condition", pg.JSONB(), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("review_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        _created(),
        sa.UniqueConstraint("account_id", "instrument_id", "generation"),
        sa.UniqueConstraint("account_id", "source_output_id", name="uq_monitor_account_output"),
        sa.CheckConstraint("generation > 0", name="generation_positive"),
        sa.CheckConstraint(
            "effective_at < review_due_at AND review_due_at <= valid_until", name="time_order"
        ),
    )
    op.create_table(
        "research_monitor_episodes",
        *_scope(primary_key=True),
        sa.Column("condition_hash", sa.String(64), primary_key=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("latched", sa.Boolean(), nullable=False),
        sa.Column("observed_false", sa.Boolean(), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        _created(),
        sa.CheckConstraint("sequence >= 0", name="sequence_nonnegative"),
    )
    op.create_table(
        "research_monitor_checks",
        _id(),
        *_scope(),
        sa.Column("plan_id", pg.UUID(), sa.ForeignKey("research_monitor_plans.id"), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(64)),
        sa.Column("observation", pg.JSONB()),
        _created(),
        sa.UniqueConstraint("account_id", "instrument_id", "scheduled_at"),
        sa.CheckConstraint("result IN ('true','false','unknown')", name="result_allowed"),
        sa.CheckConstraint("scheduled_at <= checked_at", name="time_order"),
    )
    op.create_table(
        "research_monitor_triggers",
        _id(),
        *_scope(),
        sa.Column("condition_hash", sa.String(64), nullable=False),
        sa.Column("episode", sa.Integer(), nullable=False),
        sa.Column(
            "check_id", pg.UUID(), sa.ForeignKey("research_monitor_checks.id"), nullable=False
        ),
        sa.Column("cause", sa.String(24), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(64)),
        sa.Column("research_run_id", pg.UUID(), sa.ForeignKey("agent_research_runs.id")),
        sa.Column(
            "outbox_id", pg.UUID(), sa.ForeignKey("outbox_events.id"), nullable=False, unique=True
        ),
        _created(),
        sa.UniqueConstraint("account_id", "instrument_id", "condition_hash", "episode"),
        sa.UniqueConstraint("check_id"),
        sa.CheckConstraint("episode > 0", name="episode_positive"),
        sa.CheckConstraint(
            "status IN ('pending','blocked','admitted','completed','failed','unknown')",
            name="status_allowed",
        ),
    )
    op.execute("""CREATE FUNCTION nexus_monitor_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'MONITOR_FACT_IMMUTABLE'; END $$""")
    for name in ("research_monitor_plans", "research_monitor_checks"):
        op.execute(
            f"CREATE TRIGGER trg_{name}_immutable BEFORE UPDATE OR DELETE ON {name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
        )
    op.execute("""CREATE FUNCTION nexus_monitor_trigger_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='DELETE' THEN RAISE EXCEPTION 'MONITOR_TRIGGER_IMMUTABLE'; END IF;
      IF (to_jsonb(NEW)-ARRAY['status','reason','research_run_id']) IS DISTINCT FROM
         (to_jsonb(OLD)-ARRAY['status','reason','research_run_id'])
        OR OLD.status IN ('completed','failed','unknown')
        OR (OLD.research_run_id IS NOT NULL AND NEW.research_run_id IS DISTINCT FROM OLD.research_run_id)
        OR (OLD.status='admitted' AND NEW.status NOT IN ('completed','failed','unknown'))
        OR (OLD.status IN ('pending','blocked') AND NEW.status NOT IN ('pending','blocked','admitted'))
        OR ((NEW.status IN ('admitted','completed','failed','unknown')) <> (NEW.research_run_id IS NOT NULL))
      THEN RAISE EXCEPTION 'MONITOR_TRIGGER_TRANSITION_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute("""CREATE TRIGGER trg_research_monitor_trigger_guard BEFORE UPDATE OR DELETE
      ON research_monitor_triggers FOR EACH ROW EXECUTE FUNCTION nexus_monitor_trigger_guard()""")
    op.execute("""CREATE FUNCTION nexus_monitor_episode_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='DELETE' THEN RAISE EXCEPTION 'MONITOR_EPISODE_TRANSITION_INVALID'; END IF;
      IF (to_jsonb(NEW)-ARRAY['sequence','latched','observed_false','checked_at']) IS DISTINCT FROM
         (to_jsonb(OLD)-ARRAY['sequence','latched','observed_false','checked_at'])
        OR NEW.checked_at < OLD.checked_at
        OR NEW.sequence < OLD.sequence OR NEW.sequence > OLD.sequence+1
        OR (NEW.sequence=OLD.sequence+1 AND (OLD.latched OR NOT NEW.latched))
        OR (NEW.sequence=OLD.sequence AND NOT OLD.latched AND NEW.latched)
        OR (OLD.latched AND NOT NEW.latched AND NOT NEW.observed_false)
      THEN RAISE EXCEPTION 'MONITOR_EPISODE_TRANSITION_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute("""CREATE TRIGGER trg_research_monitor_episode_guard BEFORE UPDATE OR DELETE
      ON research_monitor_episodes FOR EACH ROW EXECUTE FUNCTION nexus_monitor_episode_guard()""")


def downgrade():
    op.execute("""LOCK TABLE research_monitor_plans,research_monitor_episodes,
      research_monitor_checks,research_monitor_triggers IN ACCESS EXCLUSIVE MODE NOWAIT""")
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_plans)
      OR EXISTS(SELECT 1 FROM research_monitor_checks) OR EXISTS(SELECT 1 FROM research_monitor_triggers)
      OR EXISTS(SELECT 1 FROM research_monitor_episodes)
      THEN RAISE EXCEPTION 'MONITOR_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    for name in (
        "research_monitor_triggers",
        "research_monitor_checks",
        "research_monitor_episodes",
        "research_monitor_plans",
    ):
        op.drop_table(name)
    op.execute("DROP FUNCTION nexus_monitor_trigger_guard()")
    op.execute("DROP FUNCTION nexus_monitor_episode_guard()")
    op.execute("DROP FUNCTION nexus_monitor_immutable()")
