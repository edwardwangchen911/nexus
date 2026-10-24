"""Fence complete frozen monitor groups at transaction boundaries.

Revision ID: 0091_monitor_frozen_members
Revises: 0090_monitor_coverage
"""

from alembic import op
from sqlalchemy import text

revision = "0091_monitor_frozen_members"
down_revision = "0090_monitor_coverage"
branch_labels = None
depends_on = None


_ANCESTOR = r"""
CREATE FUNCTION public.nexus_monitor_same_condition_ancestor(source_id uuid, current_id uuid)
RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
  WITH RECURSIVE lineage AS (
    SELECT id,predecessor_id,generation,account_id,instrument_id,condition_hash
      FROM public.research_monitor_plans WHERE id=current_id
    UNION ALL
    SELECT p.id,p.predecessor_id,p.generation,p.account_id,p.instrument_id,p.condition_hash
      FROM public.research_monitor_plans p JOIN lineage c ON p.id=c.predecessor_id
      WHERE p.generation=c.generation-1 AND p.account_id=c.account_id
        AND p.instrument_id=c.instrument_id
  )
  SELECT EXISTS (
    SELECT 1 FROM lineage ancestor JOIN public.research_monitor_plans current ON current.id=current_id
    WHERE ancestor.id=source_id AND ancestor.condition_hash=current.condition_hash
  )
$$;
"""

_OLD_JOIN = "JOIN public.research_monitor_plans plan ON plan.id = checked.plan_id"
_NEW_JOIN = """JOIN public.research_monitor_plans source_plan ON source_plan.id = checked.plan_id
                JOIN public.research_monitor_plans plan
                  ON plan.id::text = binding.payload->'monitor_context'->>'plan_id'"""
_OLD_CHECK = "AND trigger.account_id = plan.account_id AND checked.result = 'true'"
_NEW_CHECK = """AND trigger.account_id = plan.account_id AND checked.result = 'true'
                  AND trigger.condition_hash = source_plan.condition_hash
                  AND public.nexus_monitor_same_condition_ancestor(source_plan.id,plan.id)"""


def _replace_ownership(old, new):
    if op.get_context().as_sql:
        op.execute(f"""DO $migration$ DECLARE definition text; BEGIN
          SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)
            INTO definition;
          IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$) <> 1
          THEN RAISE EXCEPTION 'MONITOR_OWNERSHIP_GUARD_UNEXPECTED'; END IF;
          EXECUTE replace(definition,$old${old}$old$,$new${new}$new$);
        END $migration$;""")
        return
    definition = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("MONITOR_OWNERSHIP_GUARD_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))


_VALIDATE = r"""
CREATE FUNCTION public.nexus_monitor_validate_frozen_members(snapshot_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE
  frozen public.data_snapshots%ROWTYPE;
  member public.research_monitor_increments%ROWTYPE;
  context jsonb;
  items jsonb;
  item jsonb;
  n integer;
  actual integer;
BEGIN
  SELECT * INTO frozen FROM public.data_snapshots WHERE id=snapshot_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  IF frozen.source <> 'nexus_price_monitor_increment'
     AND frozen.schema_version <> 'price_monitor_increment_v1'
     AND NOT (frozen.payload ? 'monitor_context') THEN RETURN; END IF;
  context := frozen.payload->'monitor_context';
  items := context->'trigger_checks';
  IF frozen.source <> 'nexus_price_monitor_increment'
     OR frozen.schema_version <> 'price_monitor_increment_v1'
     OR frozen.payload->>'purpose' IS DISTINCT FROM 'price_monitor_increment_v1'
     OR context->>'purpose' IS DISTINCT FROM 'price_monitor_increment_v1'
     OR jsonb_typeof(items) IS DISTINCT FROM 'array'
     OR context->>'instrument_id' IS DISTINCT FROM frozen.provider_object_id
     OR context->'execution_locked' IS DISTINCT FROM 'true'::jsonb
     OR (context->>'decision_time')::timestamptz IS DISTINCT FROM frozen.observed_at
  THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  n := jsonb_array_length(items);
  IF n < 1 OR n > 32
     OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(items) x) <> n
     OR (SELECT count(DISTINCT x->>'check_id') FROM jsonb_array_elements(items) x) <> n
  THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  IF NOT EXISTS (
    SELECT 1 FROM public.research_monitor_plans p
    WHERE p.id::text=context->>'plan_id'
      AND p.account_id::text=context->>'account_id'
      AND p.instrument_id::text=context->>'instrument_id'
      AND p.source_run_id::text=context->>'parent_run_id'
      AND p.source_output_id::text=context->>'parent_output_id'
      AND p.source_hash=context->>'parent_output_hash'
      AND p.generation=(context->>'plan_generation')::integer
  ) THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
    IF NOT EXISTS (
      SELECT 1 FROM public.research_monitor_triggers t
      JOIN public.research_monitor_checks c ON c.id=t.check_id
      JOIN public.research_monitor_plans p ON p.id=c.plan_id
      WHERE t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'
        AND t.account_id::text=context->>'account_id'
        AND t.instrument_id::text=context->>'instrument_id'
        AND c.account_id=t.account_id AND c.instrument_id=t.instrument_id
        AND p.account_id=t.account_id AND p.instrument_id=t.instrument_id
        AND p.condition_hash=t.condition_hash
        AND public.nexus_monitor_same_condition_ancestor(p.id,(context->>'plan_id')::uuid)
        AND c.result='true' AND t.cause=item->>'cause'
        AND c.checked_at=(item->>'checked_at')::timestamptz
        AND c.checked_at<=frozen.observed_at
        AND p.effective_at<=c.checked_at AND c.checked_at<p.valid_until
        AND c.observation=item->'observation' AND c.observation IS NOT NULL
    ) THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  END LOOP;
  -- Serialize final group checks without imposing a row-update order on callers.
  SELECT * INTO member FROM public.research_monitor_increments m
    WHERE m.snapshot_id=frozen.id FOR UPDATE;
  IF NOT FOUND THEN
    IF EXISTS (SELECT 1 FROM public.agent_research_runs r WHERE r.data_snapshot_id=frozen.id)
    THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
    RETURN;
  END IF;
  IF member.trigger_id::text IS DISTINCT FROM items->0->>'trigger_id'
     OR member.account_id::text IS DISTINCT FROM context->>'account_id'
     OR member.instrument_id::text IS DISTINCT FROM context->>'instrument_id'
     OR member.plan_id::text IS DISTINCT FROM context->>'plan_id'
     OR member.research_as_of IS DISTINCT FROM frozen.observed_at
     OR NOT EXISTS (
       SELECT 1 FROM public.agent_research_runs r JOIN public.jobs j ON j.id=member.job_id
       WHERE r.id=member.run_id AND r.data_snapshot_id=frozen.id
         AND r.instrument_id=member.instrument_id AND r.decision_time=member.research_as_of
         AND j.payload=jsonb_build_object('run_id',r.id::text)
         AND j.job_type='agent.paper.research'
     )
  THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  SELECT count(*) INTO actual FROM public.research_monitor_triggers t
    WHERE t.research_run_id=member.run_id;
  IF actual <> n OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(items) x
    LEFT JOIN public.research_monitor_triggers t ON t.id::text=x->>'trigger_id'
    WHERE t.research_run_id IS DISTINCT FROM member.run_id
       OR t.status NOT IN ('admitted','completed','failed','unknown')
  ) OR (
    SELECT count(DISTINCT (t.status,t.reason)) FROM public.research_monitor_triggers t
    WHERE t.research_run_id=member.run_id
  ) <> 1
  THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
END $$;
"""

_TRIGGER = r"""
CREATE FUNCTION public.nexus_monitor_frozen_members_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE snapshot_id uuid;
BEGIN
  IF TG_TABLE_NAME='data_snapshots' THEN
    snapshot_id := NEW.id;
  ELSIF TG_TABLE_NAME='research_monitor_increments' THEN
    snapshot_id := NEW.snapshot_id;
  ELSIF TG_TABLE_NAME='agent_research_runs' THEN
    snapshot_id := NEW.data_snapshot_id;
  ELSE
    IF NEW.research_run_id IS NULL THEN RETURN NULL; END IF;
    SELECT m.snapshot_id INTO snapshot_id FROM public.research_monitor_increments m
      WHERE m.run_id=NEW.research_run_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_INVALID'; END IF;
  END IF;
  PERFORM public.nexus_monitor_validate_frozen_members(snapshot_id);
  RETURN NULL;
END $$;
"""

_TABLES = (
    "data_snapshots",
    "agent_research_runs",
    "research_monitor_increments",
    "research_monitor_triggers",
)


def upgrade():
    op.execute(_ANCESTOR)
    _replace_ownership(_OLD_JOIN, _NEW_JOIN)
    _replace_ownership(_OLD_CHECK, _NEW_CHECK)
    op.execute(_VALIDATE)
    op.execute(_TRIGGER)
    for table in _TABLES:
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_{table}_monitor_frozen_members "
            f"AFTER INSERT OR UPDATE ON public.{table} DEFERRABLE INITIALLY DEFERRED "
            "FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_frozen_members_guard()"
        )
    op.execute("""DO $$ DECLARE row record; BEGIN
      FOR row IN SELECT id FROM public.data_snapshots
        WHERE source='nexus_price_monitor_increment' OR schema_version='price_monitor_increment_v1'
          OR payload ? 'monitor_context'
      LOOP PERFORM public.nexus_monitor_validate_frozen_members(row.id); END LOOP;
    END $$""")


def downgrade():
    op.execute(
        "LOCK TABLE public.data_snapshots,public.research_monitor_increments,"
        "public.research_monitor_triggers IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM public.data_snapshots
        WHERE source='nexus_price_monitor_increment'
          AND jsonb_array_length(payload->'monitor_context'->'trigger_checks')>1)
        OR EXISTS (SELECT 1 FROM public.data_snapshots s
          CROSS JOIN LATERAL jsonb_array_elements(s.payload->'monitor_context'->'trigger_checks') x
          JOIN public.research_monitor_checks c ON c.id::text=x->>'check_id'
          WHERE s.source='nexus_price_monitor_increment'
            AND c.plan_id::text IS DISTINCT FROM s.payload->'monitor_context'->>'plan_id')
        OR EXISTS (SELECT 1 FROM public.research_monitor_triggers
          WHERE research_run_id IS NOT NULL GROUP BY research_run_id HAVING count(*)>1)
      THEN RAISE EXCEPTION 'MONITOR_FROZEN_MEMBERS_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    _replace_ownership(_NEW_CHECK, _OLD_CHECK)
    _replace_ownership(_NEW_JOIN, _OLD_JOIN)
    for table in reversed(_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_monitor_frozen_members ON public.{table}")
    op.execute("DROP FUNCTION public.nexus_monitor_frozen_members_guard()")
    op.execute("DROP FUNCTION public.nexus_monitor_validate_frozen_members(uuid)")
    op.execute("DROP FUNCTION public.nexus_monitor_same_condition_ancestor(uuid,uuid)")
