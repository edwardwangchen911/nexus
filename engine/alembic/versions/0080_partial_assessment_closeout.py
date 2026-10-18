"""Allow a bound, non-executing partial assessment terminal; retain old facts."""

from alembic import op
from sqlalchemy import text

revision = "0080_partial_assessment_closeout"
down_revision = "0079_formal_v11_output_binding"
branch_labels = None
depends_on = None

_ANCHOR = "OR (prior_status='decision_recorded' AND ("
_BRANCH = """
OR (prior_status='researching' AND NEW.to_status='completed'
  AND NEW.result_code='PORTFOLIO_ASSESSMENT_DEFERRED'
  AND EXISTS (
    SELECT 1 FROM public.agent_portfolio_synthesis_runs run
    JOIN public.agent_portfolio_synthesis_outputs output ON output.run_id=run.id
    JOIN public.jobs job ON job.payload=jsonb_build_object('run_id',run.id::text)
    WHERE run.cycle_id=NEW.cycle_id AND run.status='completed'
      AND run.settled_cost_usd IS NOT NULL AND output.status='completed'
      AND job.job_type='agent.portfolio_synthesis' AND job.status='succeeded'
      AND job.finished_at <= NEW.occurred_at AND output.created_at <= NEW.occurred_at
      AND NEW.fact_ids=jsonb_build_object('portfolio_synthesis_output_id',output.id::text)
      AND jsonb_array_length(output.intent->'choices') > 0
      AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(output.intent->'choices') choice
                      WHERE choice->>'action' IS DISTINCT FROM 'defer')
      AND EXISTS (SELECT 1 FROM jsonb_array_elements(run.frozen_input->'security_outputs') source
                  WHERE source->'intent'->>'quality_status'='partial')
      AND NOT EXISTS (SELECT 1 FROM public.strategy_decisions d
                      WHERE d.agent_paper_cycle_id=NEW.cycle_id)
      AND NOT EXISTS (SELECT 1 FROM public.economic_lane_cycles e
                      WHERE e.cycle_id=NEW.cycle_id)
  ))
"""

_GUARD_ANCHOR = "IF NOT COALESCE(transition_valid,FALSE) THEN"
_GLOBAL_GUARD = (
    """
IF NEW.result_code='PORTFOLIO_ASSESSMENT_DEFERRED' THEN
  transition_valid := transition_valid AND (FALSE
"""
    + _BRANCH
    + """
  );
END IF;
"""
)


def _change(enabled: bool) -> None:
    definition = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('nexus_agent_paper_cycle_event_chain_valid()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or (
        definition.count(_ANCHOR) != 1 or definition.count(_GUARD_ANCHOR) != 1
    ):
        raise RuntimeError("ASSESSMENT_CLOSEOUT_GUARD_UNEXPECTED")
    if enabled:
        if _BRANCH in definition:
            raise RuntimeError("ASSESSMENT_CLOSEOUT_GUARD_UNEXPECTED")
        definition = definition.replace(_ANCHOR, _BRANCH + _ANCHOR)
        definition = definition.replace(_GUARD_ANCHOR, _GLOBAL_GUARD + _GUARD_ANCHOR)
    else:
        if definition.count(_BRANCH) != 2 or definition.count(_GLOBAL_GUARD) != 1:
            raise RuntimeError("ASSESSMENT_CLOSEOUT_GUARD_UNEXPECTED")
        definition = definition.replace(_GLOBAL_GUARD, "")
        definition = definition.replace(_BRANCH, "")
    op.execute(definition)


def upgrade() -> None:
    _change(True)
    op.execute("""
    CREATE FUNCTION nexus_deferred_assessment_execution_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE account_ref uuid;
    BEGIN
      SELECT account_id INTO account_ref FROM public.agent_paper_cycles
      WHERE id=NEW.agent_paper_cycle_id;
      IF account_ref IS NOT NULL THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(
          pg_catalog.hashtextextended('nexus:agent-paper-cycle:' || account_ref::text, 0)
        );
      END IF;
      IF EXISTS (SELECT 1 FROM public.agent_paper_cycle_events e
                 WHERE e.cycle_id=NEW.agent_paper_cycle_id
                   AND e.result_code='PORTFOLIO_ASSESSMENT_DEFERRED') THEN
        RAISE EXCEPTION 'DEFERRED_ASSESSMENT_EXECUTION_FORBIDDEN' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER deferred_assessment_execution_guard BEFORE INSERT ON strategy_decisions
    FOR EACH ROW EXECUTE FUNCTION nexus_deferred_assessment_execution_guard()
    """)


def downgrade() -> None:
    op.execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM public.agent_paper_cycle_events
                 WHERE result_code='PORTFOLIO_ASSESSMENT_DEFERRED') THEN
        RAISE EXCEPTION 'DEFERRED_ASSESSMENT_FACTS_EXIST';
      END IF;
    END $$
    """)
    _change(False)
    op.execute("DROP TRIGGER deferred_assessment_execution_guard ON strategy_decisions")
    op.execute("DROP FUNCTION nexus_deferred_assessment_execution_guard()")
