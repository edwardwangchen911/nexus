"""Make internal-paper execution windows a monotonic source-frozen chain.

Revision ID: 0017_paper_execution_time_order
Revises: 0016_paper_execution_atomic
Create Date: 2026-08-27
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0017_paper_execution_time_order"
down_revision: str | Sequence[str] | None = "0016_paper_execution_atomic"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Reject gaps, time reversal, discontinuity, and source switches in raw SQL."""
    op.execute(
        """
        CREATE FUNCTION nexus_paper_execution_window_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM (
              SELECT
                window_seq,
                row_number() OVER (ORDER BY window_seq) AS expected_seq,
                window_start_at,
                evaluated_at,
                source,
                feed,
                delivery_mode,
                lag(window_end_at) OVER (ORDER BY window_seq) AS prior_end_at,
                lag(evaluated_at) OVER (ORDER BY window_seq) AS prior_evaluated_at,
                lag(source) OVER (ORDER BY window_seq) AS prior_source,
                lag(feed) OVER (ORDER BY window_seq) AS prior_feed,
                lag(delivery_mode) OVER (ORDER BY window_seq) AS prior_delivery_mode
              FROM paper_execution_window_authorities
              WHERE order_id = NEW.order_id
            ) ordered
            WHERE window_seq <> expected_seq
              OR (
                window_seq > 1
                AND (
                  window_start_at IS DISTINCT FROM prior_end_at
                  OR evaluated_at < prior_evaluated_at
                  OR source IS DISTINCT FROM prior_source
                  OR feed IS DISTINCT FROM prior_feed
                  OR delivery_mode IS DISTINCT FROM prior_delivery_mode
                )
              )
          ) THEN
            RAISE EXCEPTION 'paper execution window chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_execution_window_authorities_chain "
        "AFTER INSERT ON paper_execution_window_authorities "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_paper_execution_window_chain_valid()"
    )


def downgrade() -> None:
    """Remove the Task 19 corrective chain guard."""
    op.execute(
        "DROP TRIGGER trg_paper_execution_window_authorities_chain "
        "ON paper_execution_window_authorities"
    )
    op.execute("DROP FUNCTION nexus_paper_execution_window_chain_valid()")
