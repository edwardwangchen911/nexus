"""Exact once-only comparison claims; ordinary Workers remain excluded.

Only new, registered positions can claim. Native attempts, leases, stage limits
and terminal accounting stay authoritative. No inference route is enabled here.
"""

from alembic import op
from sqlalchemy import text

revision = "0103_comparison_claim"
down_revision = "0102_comparison_routes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE OR REPLACE FUNCTION research_comparison_claim_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$
      DECLARE chain research_comparison_chain;
      BEGIN
        IF NEW.job_type<>'agent.research.comparison' THEN RETURN NEW; END IF;
        IF NEW.manual_retry_count<>0 OR NEW.attempt_count>1 THEN
          RAISE EXCEPTION 'COMPARISON_POSITION_CONSUMED'; END IF;
        IF TG_OP='UPDATE' THEN
          IF OLD.attempt_count=1 THEN
            IF NEW.attempt_count<>1 OR NEW.status='queued' THEN
              RAISE EXCEPTION 'COMPARISON_POSITION_CONSUMED'; END IF;
            RETURN NEW;
          END IF;
        END IF;
        IF NEW.attempt_count=0 AND NEW.status='queued' THEN RETURN NEW; END IF;
        IF TG_OP='INSERT' THEN RAISE EXCEPTION 'COMPARISON_DISPATCH_NOT_ENABLED'; END IF;
        SELECT * INTO chain FROM research_comparison_chain WHERE job_id=NEW.id;
        IF chain.job_id IS NULL OR OLD.status<>'queued' OR OLD.attempt_count<>0
          OR NEW.status<>'running' OR NEW.attempt_count<>1
          OR current_setting('nexus.stage_protocol',true) IS DISTINCT FROM 'research-stages-v1'
          OR current_setting('nexus.comparison_position',true)
            IS DISTINCT FROM chain.experiment_id::text || ':' || chain.ordinal::text
        THEN RAISE EXCEPTION 'COMPARISON_DISPATCH_NOT_ENABLED'; END IF;
        PERFORM 1 FROM research_stage_scope WHERE id=chain.experiment_id FOR UPDATE;
        IF NOT EXISTS(
          SELECT 1 FROM research_stage_route r JOIN research_stage_scope s ON s.id=r.scope_id
          JOIN agent_daily_budget_reservations b ON b.id=chain.reserved_id
          WHERE r.run_id=chain.run_id AND r.job_id=NEW.id AND r.kind='comparison'
            AND r.version='research-stages-v1' AND s.comparison_id=chain.experiment_id
            AND NOT s.stopped AND b.state='reserved'
            AND b.business_date=(clock_timestamp() AT TIME ZONE 'UTC')::date
            AND research_comparison_binding(chain.run_id,NEW.id,chain.experiment_id)
        ) THEN RAISE EXCEPTION 'COMPARISON_CLAIM_BINDING_INVALID'; END IF;
        IF EXISTS(
          SELECT 1 FROM generate_series(1,chain.ordinal-1) prior(i)
          LEFT JOIN research_comparison_chain c ON c.experiment_id=chain.experiment_id
            AND c.ordinal=prior.i
          LEFT JOIN research_stage_terminal t ON t.run_id=c.run_id
          LEFT JOIN jobs j ON j.id=c.job_id
          WHERE t.known IS DISTINCT FROM true OR j.status NOT IN ('succeeded','failed')
            OR EXISTS(SELECT 1 FROM research_stage_authorization a
              LEFT JOIN research_stage_cleanup clean USING(run_id,stage)
              WHERE a.run_id=c.run_id AND clean.confirmed IS DISTINCT FROM true)
        ) THEN RAISE EXCEPTION 'COMPARISON_PREDECESSOR_UNCONFIRMED'; END IF;
        RETURN NEW;
      END $$""")


def downgrade() -> None:
    if op.get_bind().scalar(
        text("""SELECT EXISTS(SELECT 1 FROM jobs
      WHERE job_type='agent.research.comparison' AND attempt_count>0)""")
    ):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        text("""SELECT EXISTS(SELECT 1 FROM jobs
      WHERE job_type='agent.research.comparison' AND attempt_count>0)""")
    ):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("""CREATE OR REPLACE FUNCTION research_comparison_claim_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.job_type='agent.research.comparison' AND
          (NEW.status='running' OR NEW.attempt_count>0) THEN
          RAISE EXCEPTION 'COMPARISON_DISPATCH_NOT_ENABLED';
        END IF;
        RETURN NEW;
      END $$""")
