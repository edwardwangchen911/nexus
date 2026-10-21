"""Optional safe diagnostic on the existing immutable synthesis output.

Old applications ignore this column. Roll back application code while retaining
this schema; a destructive downgrade is refused once diagnostic facts exist.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0082_synthesis_failure_facts"
down_revision = "0081_historical_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_portfolio_synthesis_outputs",
        sa.Column("failure_diagnostic", postgresql.JSONB(none_as_null=True), nullable=True),
    )


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM agent_portfolio_synthesis_outputs
                 WHERE failure_diagnostic IS NOT NULL) THEN
        RAISE EXCEPTION 'SYNTHESIS_DIAGNOSTIC_FACTS_EXIST';
      END IF;
    END $$""")
    op.drop_column("agent_portfolio_synthesis_outputs", "failure_diagnostic")
