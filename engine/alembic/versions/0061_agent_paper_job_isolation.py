"""Normalize legacy Agent paper jobs into the explicit worker namespace.

Revision ID: 0061_agent_paper_job_isolation
Revises: 0060_agent_paper_loop_heartbeat
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0061_agent_paper_job_isolation"
down_revision: str | Sequence[str] | None = "0060_agent_paper_loop_heartbeat"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _retype_jobs(*, source: str, target: str) -> None:
    op.execute("LOCK TABLE public.jobs, public.agent_research_runs IN ACCESS EXCLUSIVE MODE")
    op.execute("ALTER TABLE public.jobs DISABLE TRIGGER trg_jobs_state")
    op.execute(
        f"""
        UPDATE public.jobs AS job
        SET job_type='{target}',
            job_key='{target}' || substring(job.job_key FROM {len(source) + 1})
        FROM public.agent_research_runs AS run
        WHERE job.job_type='{source}'
          AND run.actor_id='local-agent-paper-cycle'
          AND job.payload->>'run_id'=run.id::text
        """
    )
    op.execute("ALTER TABLE public.jobs ENABLE TRIGGER trg_jobs_state")


def upgrade() -> None:
    _retype_jobs(source="agent.research", target="agent.paper.research")


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.data_snapshots
            WHERE source='nexus_agent_paper'
              AND schema_version='agent_paper_research_v1'
          ) THEN
            RAISE EXCEPTION 'agent paper snapshots prevent downgrade'
              USING ERRCODE='P3D01';
          END IF;
        END $$
        """
    )
    _retype_jobs(source="agent.paper.research", target="agent.research")
