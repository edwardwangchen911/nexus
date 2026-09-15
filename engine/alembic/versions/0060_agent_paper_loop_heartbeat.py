"""Admit durable liveness for the Agent paper foreground loop.

Revision ID: 0060_agent_paper_loop_heartbeat
Revises: 0059_agent_paper_intraday_cycles
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0060_agent_paper_loop_heartbeat"
down_revision: str | Sequence[str] | None = "0059_agent_paper_intraday_cycles"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.drop_constraint("queue_allowed", "worker_heartbeats", type_="check")
    op.create_check_constraint(
        "queue_allowed",
        "worker_heartbeats",
        "queue IN ('general','provider','broker','strategy_rnd','agent_paper')",
    )


def downgrade() -> None:
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.worker_heartbeats WHERE queue='agent_paper') THEN
            RAISE EXCEPTION 'agent paper loop heartbeat prevents downgrade'
              USING ERRCODE='P3D01';
          END IF;
        END $$
        """
    )
    op.drop_constraint("queue_allowed", "worker_heartbeats", type_="check")
    op.create_check_constraint(
        "queue_allowed",
        "worker_heartbeats",
        "queue IN ('general','provider','broker','strategy_rnd')",
    )
