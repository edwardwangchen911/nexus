"""Explicit typed monitor ownership; retain the original price-only fences.

Revision ID: 0095_monitor_mixed_increments
Revises: 0094_monitor_batches
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0095_monitor_mixed_increments"
down_revision = "0094_monitor_batches"
branch_labels = None
depends_on = None

_V2 = r"""
CREATE FUNCTION public.nexus_monitor_validate_mixed_members(snapshot_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots%ROWTYPE; b public.research_monitor_batches%ROWTYPE;
 m public.research_monitor_increments%ROWTYPE; c jsonb; items jsonb; item jsonb;
 material jsonb; parent jsonb; primary_id uuid; n integer; actual integer;
BEGIN
 SELECT * INTO s FROM public.data_snapshots WHERE id=snapshot_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_MIXED_MEMBERS_INVALID'; END IF;
 c:=s.payload->'monitor_context'; items:=c->'trigger_checks'; material:=c->'materials';
 IF s.source<>'nexus_monitor_increment_v2' OR s.schema_version<>'monitor_increment_v2'
  OR s.payload->>'purpose' IS DISTINCT FROM 'monitor_increment_v2'
  OR c->>'purpose' IS DISTINCT FROM 'monitor_increment_v2'
  OR c->>'instrument_id' IS DISTINCT FROM s.provider_object_id
  OR c->'execution_locked' IS DISTINCT FROM 'true'::jsonb
  OR (c->>'decision_time')::timestamptz IS DISTINCT FROM s.observed_at
  OR s.available_at<>s.observed_at OR s.received_at<>s.observed_at
  OR jsonb_typeof(items) IS DISTINCT FROM 'array'
  OR jsonb_typeof(material) IS DISTINCT FROM 'object'
 THEN RAISE EXCEPTION 'MONITOR_MIXED_MEMBERS_INVALID'; END IF;
 SELECT * INTO b FROM public.research_monitor_batches WHERE id::text=c->>'batch_id' FOR UPDATE;
 IF NOT FOUND OR b.account_id::text IS DISTINCT FROM c->>'account_id'
  OR b.instrument_id::text IS DISTINCT FROM c->>'instrument_id'
  OR greatest(b.created_at,b.sealed_at,b.checked_at)>s.observed_at
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BATCH_INVALID'; END IF;
 PERFORM public.nexus_monitor_validate_batch(b.id);
 n:=jsonb_array_length(items);
 IF n NOT BETWEEN 1 AND 32
  OR (SELECT jsonb_agg(x->>'trigger_id' ORDER BY ord) FROM jsonb_array_elements(items) WITH ORDINALITY a(x,ord)) IS DISTINCT FROM b.trigger_ids
  OR (SELECT count(DISTINCT x->>'check_id') FROM jsonb_array_elements(items) x)<>n
  OR NOT EXISTS (
   SELECT 1 FROM public.data_snapshots ms WHERE ms.id::text=c->>'materials_snapshot_id'
    AND ms.source='nexus_monitor_batch_inputs' AND ms.schema_version='monitor_batch_inputs_v1'
    AND ms.provider_object_id=b.id::text AND ms.revision=c->>'materials_hash'
    AND ms.revision ~ '^[0-9a-f]{64}$' AND ms.payload=material
    AND (material->>'prepared_at')::timestamptz=ms.observed_at
    AND greatest(ms.created_at,ms.observed_at,ms.available_at,ms.received_at)<=s.observed_at)
  OR material->>'schema_version' IS DISTINCT FROM 'monitor_batch_inputs_v1'
  OR material->>'batch_id' IS DISTINCT FROM b.id::text
  OR material->>'account_id' IS DISTINCT FROM b.account_id::text
  OR material->>'instrument_id' IS DISTINCT FROM b.instrument_id::text
  OR material->'execution_locked' IS DISTINCT FROM 'true'::jsonb
  OR material->>'impact_review' IS DISTINCT FROM 'not_performed'
  OR jsonb_typeof(material->'members') IS DISTINCT FROM 'array'
  OR jsonb_typeof(material->'parents') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'MONITOR_MIXED_MATERIALS_INVALID'; END IF;
 IF jsonb_array_length(material->'members')<>n
  OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(material->'members') x)<>n
  OR jsonb_array_length(material->'parents') NOT BETWEEN 1 AND 2
  OR (SELECT count(DISTINCT x->>'plan_id') FROM jsonb_array_elements(material->'parents') x)<>jsonb_array_length(material->'parents')
  OR (SELECT count(DISTINCT x->>'kind') FROM jsonb_array_elements(material->'parents') x)<>jsonb_array_length(material->'parents')
 THEN RAISE EXCEPTION 'MONITOR_MIXED_MATERIALS_INVALID'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
  IF NOT EXISTS (
   SELECT 1 FROM public.research_monitor_triggers t
   JOIN public.research_monitor_checks k ON k.id=t.check_id
   JOIN public.research_monitor_plans p ON p.id=k.plan_id
   WHERE t.id::text=item->>'trigger_id' AND k.id::text=item->>'check_id'
    AND t.batch_id=b.id AND t.account_id=b.account_id AND t.instrument_id=b.instrument_id
    AND k.account_id=b.account_id AND k.instrument_id=b.instrument_id
    AND p.account_id=b.account_id AND p.instrument_id=b.instrument_id
    AND t.kind=item->>'kind' AND k.kind=t.kind AND p.kind=t.kind
    AND t.event_key=item->>'event_key' AND k.event_key=t.event_key
    AND t.condition_hash=p.condition_hash AND t.cause=item->>'cause'
    AND k.result='true' AND k.checked_at=(item->>'checked_at')::timestamptz
    AND greatest(t.created_at,k.created_at,k.checked_at,p.created_at)<=s.observed_at
    AND p.effective_at<=k.checked_at AND k.checked_at<p.valid_until
    AND jsonb_typeof(item->'observation')='object' AND k.observation=item->'observation'
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(material->'members') mm
      WHERE mm->>'trigger_id'=t.id::text AND mm->>'check_id'=k.id::text
       AND mm->>'plan_id'=p.id::text AND mm->>'kind'=t.kind AND mm->>'event_key'=t.event_key
       AND mm->>'cause'=t.cause AND (mm->>'checked_at')::timestamptz=k.checked_at
       AND mm->'observation'=k.observation)
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(material->'parents') pp WHERE pp->>'plan_id'=p.id::text)
  ) THEN RAISE EXCEPTION 'MONITOR_MIXED_MEMBERS_INVALID'; END IF;
 END LOOP;
 FOR parent IN SELECT value FROM jsonb_array_elements(material->'parents') LOOP
  IF NOT EXISTS (
   SELECT 1 FROM public.research_monitor_plans p
   JOIN public.agent_research_runs r ON r.id=p.source_run_id
   JOIN public.agent_research_outputs o ON o.id=p.source_output_id
   WHERE p.id::text=parent->>'plan_id' AND p.kind=parent->>'kind'
    AND p.generation=(parent->>'generation')::integer
    AND p.account_id=b.account_id AND p.instrument_id=b.instrument_id
    AND p.source_run_id::text=parent->>'run_id' AND p.source_output_id::text=parent->>'output_id'
    AND p.source_hash=parent->>'output_hash' AND o.content_hash=p.source_hash
    AND o.run_id=r.id AND o.role IS NULL AND o.status='completed' AND r.status='completed'
    AND r.instrument_id=b.instrument_id AND o.intent=parent->'intent'
    AND greatest(r.created_at,r.decision_time,o.created_at)<=s.observed_at
    AND jsonb_array_length(parent->'role_outputs')=8
    AND parent->'role_outputs'=(SELECT jsonb_agg(ro.payload ORDER BY ro.sequence)
      FROM public.agent_research_outputs ro WHERE ro.run_id=r.id AND ro.role IS NOT NULL)
    AND EXISTS(SELECT 1 FROM public.research_monitor_checks k
      JOIN public.research_monitor_triggers t ON t.check_id=k.id WHERE t.batch_id=b.id AND k.plan_id=p.id)
  ) THEN RAISE EXCEPTION 'MONITOR_MIXED_PARENT_INVALID'; END IF;
 END LOOP;
 SELECT p.id INTO primary_id FROM public.research_monitor_plans p
  JOIN public.research_monitor_checks k ON k.plan_id=p.id
  JOIN public.research_monitor_triggers t ON t.check_id=k.id WHERE t.batch_id=b.id
  ORDER BY CASE WHEN p.kind='price' THEN 0 ELSE 1 END,p.id LIMIT 1;
 IF primary_id::text IS DISTINCT FROM c->>'plan_id' OR NOT EXISTS (
  SELECT 1 FROM public.research_monitor_plans p JOIN public.agent_research_outputs o ON o.id=p.source_output_id
  WHERE p.id=primary_id AND p.source_run_id::text=c->>'parent_run_id'
   AND p.source_output_id::text=c->>'parent_output_id' AND p.source_hash=c->>'parent_output_hash'
   AND p.generation=(c->>'plan_generation')::integer AND o.intent=c->'parent_intent')
 THEN RAISE EXCEPTION 'MONITOR_MIXED_PARENT_INVALID'; END IF;
 IF jsonb_typeof(c->'disclosure_monitoring') IS DISTINCT FROM 'object'
  OR c->'disclosure_monitoring'->>'schema_version' IS DISTINCT FROM 'disclosure_monitor_context_v1'
  OR c->'disclosure_monitoring'->>'instrument_id' IS DISTINCT FROM b.instrument_id::text
  OR (c->'disclosure_monitoring'->>'research_as_of')::timestamptz IS DISTINCT FROM s.observed_at
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BASELINE_INVALID'; END IF;
 IF c->'disclosure_monitoring'->'baseline'='null'::jsonb THEN
  IF c->'disclosure_monitoring'->>'gap_reason' IS NULL
   OR c->'disclosure_monitoring'->>'gap_reason' NOT IN ('SEC_BASELINE_MISSING','SEC_BASELINE_INVALID')
  THEN RAISE EXCEPTION 'MONITOR_MIXED_BASELINE_INVALID'; END IF;
 ELSIF jsonb_typeof(c->'disclosure_monitoring'->'baseline') IS DISTINCT FROM 'object'
  OR c->'disclosure_monitoring'->'baseline'->>'schema_version' IS DISTINCT FROM 'sec_monitor_baseline_v1'
  OR c->'disclosure_monitoring'->'gap_reason' IS DISTINCT FROM 'null'::jsonb
  OR NOT EXISTS(SELECT 1 FROM public.data_snapshots bs
    WHERE bs.id::text=c->'disclosure_monitoring'->'baseline'->>'snapshot_id'
     AND bs.source='sec_disclosure_index' AND bs.schema_version='sec_disclosure_index_v1'
     AND bs.revision=c->'disclosure_monitoring'->'baseline'->>'snapshot_hash'
     AND bs.payload->'index'->>'instrument_id'=b.instrument_id::text
     AND bs.payload->'index'->>'issuer_cik'=c->'disclosure_monitoring'->'baseline'->>'issuer_cik'
     AND c->'disclosure_monitoring'->'baseline'->>'instrument_id'=b.instrument_id::text
     AND (c->'disclosure_monitoring'->'baseline'->>'research_as_of')::timestamptz=s.observed_at
     AND greatest(bs.created_at,bs.observed_at,bs.available_at,bs.received_at)<=s.observed_at)
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BASELINE_INVALID'; END IF;
 SELECT * INTO m FROM public.research_monitor_increments WHERE batch_id=b.id FOR UPDATE;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM public.agent_research_runs WHERE data_snapshot_id=s.id)
   OR EXISTS(SELECT 1 FROM public.research_monitor_increments mi WHERE mi.snapshot_id=s.id)
  THEN RAISE EXCEPTION 'MONITOR_MIXED_BINDING_INVALID'; END IF;
  RETURN;
 END IF;
 IF m.snapshot_id<>s.id OR m.trigger_id::text IS DISTINCT FROM items->0->>'trigger_id'
  OR m.account_id<>b.account_id OR m.instrument_id<>b.instrument_id OR m.plan_id<>primary_id
  OR m.research_as_of<>s.observed_at OR m.reserved_sdk_calls<>3
  OR EXISTS(SELECT 1 FROM public.agent_research_runs WHERE data_snapshot_id=s.id AND id<>m.run_id)
  OR NOT EXISTS (
   SELECT 1 FROM public.agent_research_runs r JOIN public.jobs j ON j.id=m.job_id
   WHERE r.id=m.run_id AND r.data_snapshot_id=s.id AND r.instrument_id=m.instrument_id
    AND r.decision_time=m.research_as_of AND j.payload=jsonb_build_object('run_id',r.id::text)
    AND j.job_type='agent.paper.research' AND j.scope_kind='instrument'
    AND j.scope_id=m.instrument_id::text AND j.effective_at=m.research_as_of)
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BINDING_INVALID'; END IF;
 SELECT count(*) INTO actual FROM public.research_monitor_triggers WHERE research_run_id=m.run_id;
 IF actual<>n OR EXISTS(SELECT 1 FROM public.research_monitor_triggers t WHERE t.batch_id=b.id
   AND (t.research_run_id IS DISTINCT FROM m.run_id OR t.status NOT IN ('admitted','completed','failed','unknown')))
  OR (SELECT count(DISTINCT(t.status,t.reason)) FROM public.research_monitor_triggers t WHERE t.batch_id=b.id)<>1
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BINDING_INVALID'; END IF;
END $$;
"""

_ROUTER = r"""
CREATE OR REPLACE FUNCTION public.nexus_monitor_validate_frozen_members(snapshot_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots%ROWTYPE;
BEGIN
 SELECT * INTO s FROM public.data_snapshots WHERE id=snapshot_id;
 IF s.source='nexus_monitor_increment_v2' OR s.schema_version='monitor_increment_v2'
  OR s.payload->>'purpose'='monitor_increment_v2'
  OR s.payload->'monitor_context'->>'purpose'='monitor_increment_v2' THEN
  PERFORM public.nexus_monitor_validate_mixed_members(snapshot_id); RETURN;
 END IF;
 IF EXISTS(SELECT 1 FROM public.research_monitor_increments m WHERE m.snapshot_id=$1
   AND (m.batch_id IS NOT NULL OR s.source<>'nexus_price_monitor_increment'
    OR s.schema_version<>'price_monitor_increment_v1'))
 THEN RAISE EXCEPTION 'MONITOR_MIXED_BINDING_INVALID'; END IF;
 PERFORM public.nexus_monitor_validate_price_members_v1(snapshot_id);
END $$;
"""

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots binding
                JOIN public.research_monitor_batches batch
                  ON batch.id::text=binding.payload->'monitor_context'->>'batch_id'
                WHERE NEW.source='nexus_monitor_increment_v2'
                  AND binding.id=NEW.data_snapshot_id AND binding.source=NEW.source
                  AND binding.schema_version='monitor_increment_v2'
                  AND binding.provider_object_id=NEW.instrument_id::text
                  AND batch.instrument_id=NEW.instrument_id
                  AND NEW.provider_object_id=binding.payload->'monitor_context'->'trigger_checks'->0->>'trigger_id'
                  AND binding.payload->>'purpose'=binding.schema_version
                  AND binding.payload->'monitor_context'->>'purpose'=binding.schema_version
                  AND binding.created_at<=snapshot_time
                  AND binding.observed_at=NEW.available_at AND binding.available_at=NEW.available_at
                  AND binding.received_at=NEW.available_at AND NEW.revision=NEW.content_hash
                  AND NEW.location='memory://sha256/'||NEW.content_hash
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def _ownership(reverse=False):
    old, new = (_NEW, _OLD) if reverse else (_OLD, _NEW)
    op.execute(f"""DO $migration$ DECLARE definition text; BEGIN
      SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure) INTO definition;
      IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$)<>1
      THEN RAISE EXCEPTION 'MONITOR_MIXED_GUARD_UNEXPECTED'; END IF;
      EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")


def upgrade():
    op.add_column("research_monitor_increments", sa.Column("batch_id", pg.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_monitor_increment_batch",
        "research_monitor_increments",
        "research_monitor_batches",
        ["batch_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_monitor_increment_batch", "research_monitor_increments", ["batch_id"]
    )
    op.execute("""DO $$ DECLARE definition text; BEGIN
      SELECT pg_get_functiondef('public.nexus_monitor_validate_frozen_members(uuid)'::regprocedure) INTO definition;
      EXECUTE replace(definition,'FUNCTION public.nexus_monitor_validate_frozen_members(',
        'FUNCTION public.nexus_monitor_validate_price_members_v1('); END $$""")
    op.execute(_V2)
    op.execute(_ROUTER)
    _ownership()


def downgrade():
    op.execute(
        "LOCK TABLE research_monitor_increments,data_snapshots,agent_data_snapshot_evidence IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_increments WHERE batch_id IS NOT NULL)
      OR EXISTS(SELECT 1 FROM data_snapshots WHERE source='nexus_monitor_increment_v2'
        OR schema_version='monitor_increment_v2')
      OR EXISTS(SELECT 1 FROM agent_data_snapshot_evidence WHERE source='nexus_monitor_increment_v2')
      THEN RAISE EXCEPTION 'MONITOR_MIXED_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    _ownership(reverse=True)
    op.execute("""DO $$ DECLARE definition text; BEGIN
      SELECT pg_get_functiondef('public.nexus_monitor_validate_price_members_v1(uuid)'::regprocedure) INTO definition;
      EXECUTE replace(definition,'FUNCTION public.nexus_monitor_validate_price_members_v1(',
        'FUNCTION public.nexus_monitor_validate_frozen_members('); END $$""")
    op.execute("DROP FUNCTION public.nexus_monitor_validate_price_members_v1(uuid)")
    op.execute("DROP FUNCTION public.nexus_monitor_validate_mixed_members(uuid)")
    op.drop_column("research_monitor_increments", "batch_id")
