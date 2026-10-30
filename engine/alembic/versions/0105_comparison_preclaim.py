"""Precise unclaimed comparison closeout; preserve native stage terminal semantics.

Audit records remain append-only. Populated proof requires compatible application
rollback; schema downgrade never removes or reclassifies a closed position.
"""

from alembic import op
from sqlalchemy import text

revision = "0105_comparison_preclaim"
down_revision = "0104_comparison_chain_stop"
branch_labels = None
depends_on = None

_TRANSITION_OLD = "(OLD.status = 'queued' AND NEW.status = 'running')"
_TRANSITION_NEW = (
    _TRANSITION_OLD
    + """
            OR (OLD.status='queued' AND NEW.status='failed'
              AND NEW.job_type='agent.research.comparison' AND NEW.attempt_count=0
              AND research_comparison_preclaim_bound(NEW.id,NEW.finished_at))"""
)


def _replace_transition(old: str, new: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef('nexus_guard_job_state()'::regprocedure)")
    )
    if definition.count(old) != 1:
        raise RuntimeError("COMPARISON_JOB_TRANSITION_GUARD_CHANGED")
    op.execute(definition.replace(old, new))


OLD_CLAIM_GUARD = """CREATE OR REPLACE FUNCTION research_comparison_claim_guard() RETURNS trigger
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
      END $$"""

NEW_CLAIM_GUARD = """CREATE OR REPLACE FUNCTION research_comparison_claim_guard() RETURNS trigger
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
        IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM audit_events a
          WHERE a.action='research.comparison.preclaim_closed'
          AND a.payload->>'job_id'=NEW.id::text) THEN
          IF OLD.status='queued' AND OLD.attempt_count=0 AND NEW.attempt_count=0
            AND NEW.status='failed' AND NEW.started_at IS NULL
            AND NEW.last_error_code='COMPARISON_PRECLAIM_CAPACITY_REJECTED'
            AND NEW.result_code=NEW.last_error_code AND NEW.version=OLD.version+1
            AND (to_jsonb(NEW)-ARRAY['status','finished_at','last_error_code','result_code','version'])
              = (to_jsonb(OLD)-ARRAY['status','finished_at','last_error_code','result_code','version'])
            AND research_comparison_preclaim_bound(NEW.id,NEW.finished_at)
          THEN RETURN NEW; END IF;
          RAISE EXCEPTION 'COMPARISON_PRECLOSED';
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
      END $$"""


def upgrade() -> None:
    op.execute("""CREATE UNIQUE INDEX uq_comparison_preclaim_audit ON audit_events
      ((payload->>'experiment_id')) WHERE action='research.comparison.preclaim_closed'""")
    op.execute("""CREATE FUNCTION research_comparison_preclaim_bound(job uuid,finished timestamptz)
      RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT EXISTS(
        SELECT 1 FROM research_comparison_chain c
        JOIN research_comparison_manifest m ON m.id=c.experiment_id
        JOIN research_stage_scope s ON s.id=c.experiment_id
        JOIN agent_research_runs r ON r.id=c.run_id
        JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id
        JOIN audit_events a ON a.action='research.comparison.preclaim_closed'
          AND a.schema_version='comparison_preclaim_close_v1'
          AND a.payload->>'experiment_id'=m.id::text
        WHERE c.job_id=job AND c.ordinal=1 AND s.stopped AND s.comparison_id=m.id
          AND research_comparison_binding(c.run_id,c.job_id,c.experiment_id)
          AND a.actor_id='research-comparison:direct-responses-terra-v11'
          AND a.payload->>'manifest_hash'=m.manifest_hash
          AND a.payload->>'implementation_sha'=m.payload->>'implementation_sha'
          AND a.payload->>'job_id'=c.job_id::text AND a.payload->>'run_id'=c.run_id::text
          AND a.payload->>'reservation_id'=b.id::text
          AND a.payload->>'business_date'=b.business_date::text
          AND a.payload->>'disposition'='release_unspent_only'
          AND a.payload->>'authority'='native_unclaimed_permanently_stopped'
          AND a.occurred_at=finished AND r.status='failed'
          AND r.input_tokens IS NULL AND r.output_tokens IS NULL AND r.settled_cost_usd IS NULL
          AND b.state='released' AND b.settled_usd IS NULL
          AND (SELECT count(*) FROM research_comparison_chain WHERE experiment_id=m.id)=1
          AND NOT EXISTS(SELECT 1 FROM job_attempts WHERE job_id=job)
          AND NOT EXISTS(SELECT 1 FROM job_leases WHERE job_id=job)
          AND NOT EXISTS(SELECT 1 FROM research_stage_member WHERE run_id=c.run_id)
          AND NOT EXISTS(SELECT 1 FROM research_stage_authorization WHERE run_id=c.run_id)
          AND NOT EXISTS(SELECT 1 FROM research_stage_terminal WHERE run_id=c.run_id)
          AND NOT EXISTS(SELECT 1 FROM agent_research_outputs WHERE run_id=c.run_id)
          AND NOT EXISTS(SELECT 1 FROM agent_research_events WHERE run_id=c.run_id)
      ) $$""")
    op.execute(NEW_CLAIM_GUARD)
    _replace_transition(_TRANSITION_OLD, _TRANSITION_NEW)
    op.execute("ALTER TABLE jobs DROP CONSTRAINT ck_jobs_jobs_time_shape")
    op.execute("""ALTER TABLE jobs ADD CONSTRAINT ck_jobs_jobs_time_shape CHECK(
      (status='queued' AND started_at IS NULL AND finished_at IS NULL) OR
      (status='running' AND started_at IS NOT NULL AND finished_at IS NULL) OR
      (status IN ('succeeded','failed','skipped','dead_letter') AND started_at IS NOT NULL
        AND finished_at IS NOT NULL AND finished_at>=started_at) OR
      (job_type='agent.research.comparison' AND status='failed' AND attempt_count=0
        AND started_at IS NULL AND finished_at IS NOT NULL))""")
    op.execute("""CREATE FUNCTION research_comparison_admission_fence() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        PERFORM 1 FROM research_comparison_manifest WHERE id=NEW.experiment_id FOR UPDATE;
        PERFORM 1 FROM research_stage_scope WHERE id=NEW.experiment_id FOR UPDATE;
        IF EXISTS(SELECT 1 FROM research_stage_scope WHERE id=NEW.experiment_id AND stopped)
        THEN RAISE EXCEPTION 'COMPARISON_STOPPED'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER research_comparison_admission_fence BEFORE INSERT
      ON research_comparison_chain FOR EACH ROW EXECUTE FUNCTION research_comparison_admission_fence()""")
    op.execute("""CREATE FUNCTION research_comparison_install_fence() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NOT EXISTS(SELECT 1 FROM research_stage_scope
          WHERE id=NEW.scope_id AND comparison_id IS NOT NULL) THEN RETURN NEW; END IF;
        PERFORM 1 FROM research_stage_scope WHERE id=NEW.scope_id FOR UPDATE;
        IF EXISTS(SELECT 1 FROM research_stage_scope
          WHERE id=NEW.scope_id AND comparison_id IS NOT NULL AND stopped)
        THEN RAISE EXCEPTION 'COMPARISON_STOPPED'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER research_comparison_install_fence BEFORE INSERT
      ON research_stage_member FOR EACH ROW EXECUTE FUNCTION research_comparison_install_fence()""")


def downgrade() -> None:
    op.execute(
        "LOCK TABLE audit_events,jobs,research_comparison_chain,research_stage_member "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM audit_events WHERE action='research.comparison.preclaim_closed')"
        )
    ):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("DROP TRIGGER research_comparison_admission_fence ON research_comparison_chain")
    op.execute("DROP FUNCTION research_comparison_admission_fence()")
    op.execute("DROP TRIGGER research_comparison_install_fence ON research_stage_member")
    op.execute("DROP FUNCTION research_comparison_install_fence()")
    op.execute(OLD_CLAIM_GUARD)
    _replace_transition(_TRANSITION_NEW, _TRANSITION_OLD)
    op.execute("DROP FUNCTION research_comparison_preclaim_bound(uuid,timestamptz)")
    op.execute("DROP INDEX uq_comparison_preclaim_audit")
    op.execute("ALTER TABLE jobs DROP CONSTRAINT ck_jobs_jobs_time_shape")
    op.execute("""ALTER TABLE jobs ADD CONSTRAINT ck_jobs_jobs_time_shape CHECK(
      (status='queued' AND started_at IS NULL AND finished_at IS NULL) OR
      (status='running' AND started_at IS NOT NULL AND finished_at IS NULL) OR
      (status IN ('succeeded','failed','skipped','dead_letter') AND started_at IS NOT NULL
        AND finished_at IS NOT NULL AND finished_at>=started_at))""")
