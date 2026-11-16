"""Seal one positive-only recent policy to the original v6 single-monitor root.

No historical research, request, fee, origin, capture or monitor fact is rewritten.
Audit immutability remains enforced by migration 0020's global audit trigger.
"""

from alembic import op

revision = "0124_sec_recent_positive"
down_revision = "0123_monitor_increment_once"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""DO $$ BEGIN
 IF EXISTS(SELECT FROM public.data_snapshots WHERE source='nexus_sec_recent_positive_policy')
 OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.recent_positive_policy_enabled')
 THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_PREEXISTING_FACTS' USING ERRCODE='23514'; END IF;
END $$""",
    r"""CREATE UNIQUE INDEX uq_sec_recent_positive_plan_once
ON public.data_snapshots(provider_object_id)
WHERE source='nexus_sec_recent_positive_policy'""",
    r"""CREATE UNIQUE INDEX uq_sec_recent_positive_root_once
ON public.data_snapshots((payload->>'account_id'),(payload->>'root_request_id'))
WHERE source='nexus_sec_recent_positive_policy'""",
    r"""CREATE UNIQUE INDEX uq_sec_recent_positive_audit_once
ON public.audit_events((payload->>'snapshot_id'))
WHERE action='research.monitor.recent_positive_policy_enabled'""",
    r"""CREATE FUNCTION public.sec_recent_positive_policy_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE
 p public.research_monitor_plans; cp public.research_monitor_plans;
 prior public.research_monitor_plans; origin public.data_snapshots;
 capture public.data_snapshots; own public.data_snapshots;
 root uuid; at_time timestamptz; seen uuid[]:=ARRAY[]::uuid[];
 expected jsonb; canonical_text text; proposal jsonb; current_proposal jsonb;
BEGIN
 IF TG_OP<>'INSERT' THEN
   IF OLD.source='nexus_sec_recent_positive_policy' OR
      (TG_OP='UPDATE' AND NEW.source='nexus_sec_recent_positive_policy') THEN
     RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_IMMUTABLE' USING ERRCODE='23514';
   END IF;
   IF TG_OP='DELETE' THEN RETURN OLD; END IF;
   RETURN NEW;
 END IF;
 IF NEW.source<>'nexus_sec_recent_positive_policy' THEN RETURN NEW; END IF;
 -- Cast errors and malformed documents become one stable boundary rejection.
 BEGIN
   SELECT * INTO p FROM public.research_monitor_plans
     WHERE id=NEW.provider_object_id::uuid;
   IF p.id IS NULL OR p.kind<>'disclosure' THEN
     RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514';
   END IF;
   PERFORM pg_advisory_xact_lock(hashtextextended(
     'research-monitor:'||p.account_id::text||':'||p.instrument_id::text,0));
   -- The native installation path takes the same lock. A table lock also fences
   -- direct SQL plan inserts, which must not race this insert-time latest check.
   LOCK TABLE public.research_monitor_plans IN SHARE MODE;
   root:=public.single_monitor_root_request(p.source_run_id);
   IF root IS NULL OR NOT EXISTS (
     SELECT FROM public.manual_paper_requests q
     JOIN public.research_request_manifests f ON f.request_id=q.id
     JOIN public.research_request_members m ON m.manifest_id=f.id
     JOIN public.agent_research_runs r ON r.id=m.run_id
     WHERE q.id=root AND q.account_id=p.account_id
       AND q.contract_version=6 AND q.mode='research_and_monitor'
       AND q.research_profile#>>'{single_sol,authorization_scope}'='single_monitor_root_20261116_once_v1'
       AND q.research_profile#>>'{single_sol,instrument_id}'=p.instrument_id::text
       AND f.member_count=1 AND m.ordinal=0 AND m.run_id=p.source_run_id
       AND m.instrument_id=p.instrument_id AND m.data_snapshot_id=r.data_snapshot_id
       AND r.instrument_id=p.instrument_id AND r.data_snapshot_id=p.data_snapshot_id
       AND r.status='completed' AND r.actor_id='local-single-monitor-sol-v1'
       AND r.idempotency_key='single-monitor-sol:'||q.id::text||':'||p.instrument_id::text
   ) OR EXISTS(SELECT FROM public.research_monitor_increments WHERE run_id=p.source_run_id)
     OR EXISTS(SELECT FROM public.research_monitor_plans newer
       WHERE newer.account_id=p.account_id AND newer.instrument_id=p.instrument_id
         AND newer.kind='disclosure' AND newer.generation>p.generation)
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
   at_time:=(NEW.payload->>'effective_at')::timestamptz;
   IF ((NEW.payload->>'effective_at') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?Z$') IS NOT TRUE
     OR at_time IS NULL OR NOT isfinite(at_time)
     OR at_time<GREATEST(p.created_at,p.effective_at) OR at_time>=p.valid_until
     OR at_time<transaction_timestamp()
        OR at_time>clock_timestamp()
     OR NEW.created_at IS DISTINCT FROM at_time
     OR NEW.observed_at IS DISTINCT FROM at_time
     OR NEW.available_at IS DISTINCT FROM at_time
     OR NEW.received_at IS DISTINCT FROM at_time
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
   SELECT intent->'disclosure_proposal' INTO proposal
   FROM public.agent_research_outputs o WHERE o.id=p.source_output_id
     AND o.run_id=p.source_run_id AND o.status='completed' AND o.role IS NULL
     AND o.content_hash=p.source_hash AND o.created_at<=at_time;
   SELECT * INTO own FROM public.data_snapshots s
     WHERE s.source='nexus_sec_forward_origin' AND s.provider_object_id=p.id::text;
   IF own.id IS NULL OR (SELECT count(*) FROM public.data_snapshots s
       WHERE s.source='nexus_sec_forward_origin' AND s.provider_object_id=p.id::text)<>1
     OR proposal IS NULL OR ((proposal->>'issuer_cik') ~ '^[0-9]{10}$') IS NOT TRUE
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
   -- Verify the complete saved custody chain, while preserving every old row.
   cp:=p; origin:=own;
   LOOP
     SELECT o.intent->'disclosure_proposal' INTO current_proposal
     FROM public.agent_research_outputs o
     JOIN public.agent_research_runs r ON r.id=o.run_id
     WHERE o.id=cp.source_output_id AND o.run_id=cp.source_run_id
       AND o.status='completed' AND o.role IS NULL AND o.content_hash=cp.source_hash
       AND r.status='completed' AND r.instrument_id=cp.instrument_id
       AND r.data_snapshot_id=cp.data_snapshot_id AND r.decision_time=cp.baseline_as_of
       AND GREATEST(o.created_at,r.created_at,r.decision_time)<=origin.created_at;
     IF current_proposal IS NULL
       OR origin.payload->>'baseline_snapshot_id' IS DISTINCT FROM cp.baseline_snapshot_id::text
       OR (origin.payload->>'baseline_as_of')::timestamptz IS DISTINCT FROM cp.baseline_as_of
       OR origin.payload->>'baseline_hash' IS DISTINCT FROM current_proposal->>'baseline_hash'
       OR current_proposal->>'issuer_cik' IS DISTINCT FROM proposal->>'issuer_cik'
       OR origin.payload->>'policy' IS DISTINCT FROM origin.schema_version
       OR cp.id=ANY(seen) OR origin.source<>'nexus_sec_forward_origin'
       OR origin.provider_object_id IS DISTINCT FROM cp.id::text
       OR origin.payload->>'plan_id' IS DISTINCT FROM cp.id::text
       OR origin.payload->>'account_id' IS DISTINCT FROM cp.account_id::text
       OR origin.payload->>'instrument_id' IS DISTINCT FROM cp.instrument_id::text
       OR origin.payload->>'source_hash' IS DISTINCT FROM cp.source_hash
       OR origin.payload->>'condition_hash' IS DISTINCT FROM cp.condition_hash
       OR GREATEST(origin.created_at,origin.available_at,origin.observed_at,origin.received_at)>at_time
       OR origin.created_at IS DISTINCT FROM (origin.payload->>'effective_at')::timestamptz
       OR origin.available_at IS DISTINCT FROM origin.created_at
       OR origin.observed_at IS DISTINCT FROM origin.created_at
       OR origin.received_at IS DISTINCT FROM origin.created_at
       OR origin.created_at<GREATEST(cp.created_at,cp.effective_at)
       OR origin.created_at>=cp.valid_until
     THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
     seen:=array_append(seen,cp.id);
     IF origin.schema_version='sec_forward_comparison_v2' THEN EXIT; END IF;
     IF origin.schema_version<>'sec_forward_inheritance_v1'
       OR origin.payload->>'root_origin_id' IS DISTINCT FROM NEW.payload->>'root_origin_id'
       OR origin.payload->>'root_origin_hash' IS DISTINCT FROM NEW.payload->>'root_origin_hash'
       OR origin.payload->>'issuer_cik' IS DISTINCT FROM proposal->>'issuer_cik'
     THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
     SELECT * INTO prior FROM public.research_monitor_plans WHERE id=cp.predecessor_id;
     IF prior.id IS NULL OR cp.generation<>prior.generation+1
       OR (cp.account_id,cp.instrument_id,cp.kind,cp.condition_hash) IS DISTINCT FROM
          (prior.account_id,prior.instrument_id,prior.kind,prior.condition_hash)
       OR origin.payload->>'predecessor_plan_id' IS DISTINCT FROM prior.id::text
     THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
     at_time:=origin.created_at;
     SELECT * INTO origin FROM public.data_snapshots s
       WHERE s.id=(origin.payload->>'predecessor_origin_id')::uuid
         AND s.revision=origin.payload->>'predecessor_origin_hash';
     IF origin.id IS NULL THEN
       RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
     cp:=prior;
   END LOOP;
   SELECT * INTO capture FROM public.data_snapshots s
     WHERE s.id=(origin.payload->>'capture_id')::uuid;
   IF capture.id IS NULL OR capture.source<>'sec_disclosure_listed_index'
     OR capture.schema_version<>'sec_disclosure_index_v2'
     OR capture.revision IS DISTINCT FROM origin.payload->>'capture_hash'
     OR capture.payload#>>'{index,instrument_id}' IS DISTINCT FROM p.instrument_id::text
     OR capture.payload#>>'{index,issuer_cik}' IS DISTINCT FROM proposal->>'issuer_cik'
     OR GREATEST(capture.created_at,capture.available_at,capture.observed_at,capture.received_at)>origin.created_at
     OR origin.payload->>'old_history_gap' IS DISTINCT FROM 'unknown'
     OR origin.payload->'original_hashes' IS DISTINCT FROM (
       SELECT jsonb_agg(item->'sha256' ORDER BY ordinal)
       FROM jsonb_array_elements(capture.payload->'originals') WITH ORDINALITY x(item,ordinal))
     OR origin.payload->>'baseline_snapshot_id' IS DISTINCT FROM cp.baseline_snapshot_id::text
     OR (origin.payload->>'baseline_as_of')::timestamptz IS DISTINCT FROM cp.baseline_as_of
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
   expected:=jsonb_build_object(
     'policy','sec_recent_positive_once_v1','plan_id',p.id::text,
     'account_id',p.account_id::text,'instrument_id',p.instrument_id::text,
     'source_run_id',p.source_run_id::text,'source_hash',p.source_hash,
     'condition_hash',p.condition_hash,'root_request_id',root::text,
     'own_origin_id',own.id::text,'own_origin_hash',own.revision,
     'root_origin_id',origin.id::text,'root_origin_hash',origin.revision,
     'comparison_snapshot_id',capture.id::text,'comparison_hash',capture.revision,
     'issuer_cik',proposal->>'issuer_cik','effective_at',NEW.payload->>'effective_at',
     'authorization_scope','single_plan_seen_additions_20261116_v1');
   -- Every permitted policy value is ASCII text. This is Python's sorted compact
   -- JSON encoding exactly, without relying on JSONB's different key ordering.
   SELECT '{'||string_agg(to_jsonb(key)::text||':'||value::text,',' ORDER BY key COLLATE "C")||'}'
     INTO canonical_text FROM jsonb_each(expected);
   IF NEW.schema_version<>'sec_recent_positive_once_v1' OR NEW.payload IS DISTINCT FROM expected
     OR NEW.revision IS DISTINCT FROM encode(sha256(convert_to(canonical_text,'UTF8')),'hex')
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514'; END IF;
   IF EXISTS(SELECT FROM public.data_snapshots s
     WHERE s.source='nexus_sec_recent_positive_policy'
       AND (s.provider_object_id=p.id::text OR (s.payload->>'account_id'=p.account_id::text
         AND s.payload->>'root_request_id'=root::text)))
   THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_ALREADY_ENABLED' USING ERRCODE='23514'; END IF;
 EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format OR datetime_field_overflow
   OR data_exception THEN
   RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_INVALID' USING ERRCODE='23514';
 END;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_recent_positive_policy_guard BEFORE INSERT OR UPDATE OR DELETE
ON public.data_snapshots FOR EACH ROW EXECUTE FUNCTION public.sec_recent_positive_policy_guard()""",
    r"""CREATE FUNCTION public.sec_recent_positive_policy_audit_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots; n integer;
BEGIN
 IF TG_TABLE_NAME='data_snapshots' THEN
   IF NEW.source<>'nexus_sec_recent_positive_policy' THEN RETURN NEW; END IF;
   s:=NEW;
 ELSE
   IF NEW.action<>'research.monitor.recent_positive_policy_enabled' THEN RETURN NEW; END IF;
   SELECT * INTO s FROM public.data_snapshots
     WHERE id::text=NEW.payload->>'snapshot_id' AND source='nexus_sec_recent_positive_policy';
 END IF;
 SELECT count(*) INTO n FROM public.audit_events a
 WHERE a.action='research.monitor.recent_positive_policy_enabled'
   AND a.schema_version='sec_recent_positive_once_v1' AND a.actor_id='research-monitor'
   AND a.payload=s.payload||jsonb_build_object('snapshot_id',s.id::text,'policy_hash',s.revision)
   AND a.occurred_at=s.created_at;
 IF s.id IS NULL OR n<>1 THEN
   RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE CONSTRAINT TRIGGER sec_recent_positive_complete
AFTER INSERT ON public.data_snapshots DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.sec_recent_positive_policy_audit_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_recent_positive_complete
AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.sec_recent_positive_policy_audit_complete()""",
)

DOWNGRADE_SQL = (
    r"""LOCK TABLE public.data_snapshots,public.audit_events IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    r"""DO $$ BEGIN
IF EXISTS(SELECT FROM public.data_snapshots WHERE source='nexus_sec_recent_positive_policy')
OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.recent_positive_policy_enabled')
THEN RAISE EXCEPTION 'SEC_RECENT_POSITIVE_POLICY_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF;
END $$""",
    r"""DROP TRIGGER sec_recent_positive_complete ON public.audit_events""",
    r"""DROP TRIGGER sec_recent_positive_complete ON public.data_snapshots""",
    r"""DROP TRIGGER sec_recent_positive_policy_guard ON public.data_snapshots""",
    r"""DROP FUNCTION public.sec_recent_positive_policy_audit_complete()""",
    r"""DROP FUNCTION public.sec_recent_positive_policy_guard()""",
    r"""DROP INDEX public.uq_sec_recent_positive_audit_once""",
    r"""DROP INDEX public.uq_sec_recent_positive_root_once""",
    r"""DROP INDEX public.uq_sec_recent_positive_plan_once""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
