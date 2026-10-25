"""Bind single-security monitor research and finite subscription call reservations.

Revision ID: 0088_monitor_increments
Revises: 0087_research_monitoring
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0088_monitor_increments"
down_revision = "0087_research_monitoring"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "research_monitor_increments",
        sa.Column("id", pg.UUID(), primary_key=True),
        *[
            sa.Column(name, pg.UUID(), sa.ForeignKey(target), nullable=False, unique=unique)
            for name, target, unique in (
                ("account_id", "paper_accounts.id", False),
                ("instrument_id", "instruments.id", False),
                ("trigger_id", "research_monitor_triggers.id", True),
                ("plan_id", "research_monitor_plans.id", False),
                ("run_id", "agent_research_runs.id", True),
                ("job_id", "jobs.id", True),
                ("snapshot_id", "data_snapshots.id", True),
            )
        ],
        sa.Column("research_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("reserved_sdk_calls", sa.Integer(), nullable=False),
        sa.Column("daily_sdk_call_limit", sa.Integer(), nullable=False),
        sa.Column("context_hash", sa.String(64), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("reserved_sdk_calls = 3", name="three_stage_calls"),
        sa.CheckConstraint("daily_sdk_call_limit BETWEEN 3 AND 90", name="bounded_call_limit"),
    )
    op.execute(
        "CREATE TRIGGER trg_research_monitor_increments_immutable BEFORE UPDATE OR DELETE "
        "ON research_monitor_increments FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
    )


def downgrade():
    op.execute("LOCK TABLE research_monitor_increments IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_increments) THEN "
        "RAISE EXCEPTION 'MONITOR_INCREMENT_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.drop_table("research_monitor_increments")
