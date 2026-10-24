"""Append immutable research-to-monitor coverage receipts.

Revision ID: 0090_monitor_coverage
Revises: 0089_monitor_evidence
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0090_monitor_coverage"
down_revision = "0089_monitor_evidence"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "research_monitor_coverage",
        sa.Column("id", pg.UUID(), primary_key=True),
        *[
            sa.Column(name, pg.UUID(), sa.ForeignKey(target), nullable=nullable)
            for name, target, nullable in (
                ("account_id", "paper_accounts.id", False),
                ("instrument_id", "instruments.id", False),
                ("run_id", "agent_research_runs.id", False),
                ("output_id", "agent_research_outputs.id", True),
                ("prior_plan_id", "research_monitor_plans.id", True),
                ("active_plan_id", "research_monitor_plans.id", True),
            )
        ],
        sa.Column("research_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("quality_status", sa.String(16)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("account_id", "run_id"),
        sa.CheckConstraint(
            "status IN ('installed','retained','gap','unknown','superseded','unverified')",
            name="status_allowed",
        ),
        sa.CheckConstraint(
            "quality_status IS NULL OR quality_status IN ('complete','partial')",
            name="quality_allowed",
        ),
        sa.CheckConstraint("observed_at >= research_as_of", name="time_order"),
        sa.CheckConstraint(
            "status NOT IN ('installed','retained') OR active_plan_id IS NOT NULL",
            name="active_plan_required",
        ),
        sa.CheckConstraint(
            "status NOT IN ('gap','superseded','unverified') OR active_plan_id IS NULL",
            name="gap_has_no_active_plan",
        ),
    )
    op.execute(
        "CREATE TRIGGER trg_research_monitor_coverage_immutable BEFORE UPDATE OR DELETE "
        "ON research_monitor_coverage FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
    )


def downgrade():
    op.execute("LOCK TABLE research_monitor_coverage IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_coverage) THEN "
        "RAISE EXCEPTION 'MONITOR_COVERAGE_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.drop_table("research_monitor_coverage")
