"""Guard the immutable paper performance peak and drawdown chain.

Revision ID: 0018_paper_performance_chain
Revises: 0017_paper_execution_time_order
Create Date: 2026-08-27
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0018_paper_performance_chain"
down_revision: str | Sequence[str] | None = "0017_paper_execution_time_order"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Reject a snapshot that makes the immutable peak/drawdown history inconsistent."""
    op.execute(
        """
        CREATE FUNCTION nexus_paper_performance_snapshot_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM (
              SELECT
                nav,
                peak_nav,
                drawdown_ratio,
                lag(peak_nav) OVER (ORDER BY as_of) AS prior_peak_nav
              FROM paper_performance_snapshots
              WHERE account_id = NEW.account_id
            ) ordered
            WHERE peak_nav IS DISTINCT FROM
                GREATEST(nav, COALESCE(prior_peak_nav, nav))
              OR abs(drawdown_ratio - ((peak_nav - nav) / peak_nav))
                > 0.0000000000005
          ) THEN
            RAISE EXCEPTION 'paper performance snapshot chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_performance_snapshots_chain "
        "AFTER INSERT ON paper_performance_snapshots "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_paper_performance_snapshot_chain_valid()"
    )


def downgrade() -> None:
    """Remove the Task 19 corrective performance-chain guard."""
    op.execute(
        "DROP TRIGGER trg_paper_performance_snapshots_chain "
        "ON paper_performance_snapshots"
    )
    op.execute("DROP FUNCTION nexus_paper_performance_snapshot_chain_valid()")
