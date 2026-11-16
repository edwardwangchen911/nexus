"""Append-only seven-day positive-only scope and independently admitted plan members.

0124 and its original one-plan grant remain untouched. No historical facts change.
Once facts exist downgrade is intentionally forbidden; use a compatible image.
"""

from alembic import op

revision = "0125_sec_positive_chain"
down_revision = "0124_sec_recent_positive"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""DO $$ BEGIN
 IF EXISTS(SELECT FROM public.data_snapshots WHERE source IN
 ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke'))
 OR EXISTS(SELECT FROM public.audit_events WHERE action IN
 ('research.monitor.positive_chain_scope_enabled','research.monitor.positive_chain_member_registered','research.monitor.positive_chain_scope_revoked'))
 THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_PREEXISTING_FACTS' USING ERRCODE='23514'; END IF;
END $$""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_anchor ON public.data_snapshots((payload->>'anchor_policy_snapshot_id')) WHERE source='nexus_sec_positive_chain_scope'""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_root ON public.data_snapshots((payload->>'account_id'),(payload->>'root_request_id')) WHERE source='nexus_sec_positive_chain_scope'""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_plan ON public.data_snapshots(provider_object_id) WHERE source='nexus_sec_positive_chain_member'""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_member ON public.data_snapshots((payload->>'scope_snapshot_id'),(payload->>'plan_id')) WHERE source='nexus_sec_positive_chain_member'""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_revoke ON public.data_snapshots(provider_object_id) WHERE source='nexus_sec_positive_chain_revoke'""",
    r"""CREATE UNIQUE INDEX uq_sec_positive_chain_audit ON public.audit_events((payload->>'snapshot_id')) WHERE action IN ('research.monitor.positive_chain_scope_enabled','research.monitor.positive_chain_member_registered','research.monitor.positive_chain_scope_revoked')""",
    r"""CREATE FUNCTION public.sec_positive_chain_canonical(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,public AS $$
DECLARE result text; escaped text:=''; ch text; point integer; pos integer;
BEGIN
 IF jsonb_typeof(v)='object' THEN
 SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||public.sec_positive_chain_canonical(value),',' ORDER BY key COLLATE "C"),'')||'}' INTO result FROM jsonb_each(v);
 ELSIF jsonb_typeof(v)='array' THEN
 SELECT '['||coalesce(string_agg(public.sec_positive_chain_canonical(value),',' ORDER BY ordinal),'')||']' INTO result FROM jsonb_array_elements(v) WITH ORDINALITY x(value,ordinal);
 ELSE
 result:=v::text;
 IF jsonb_typeof(v)='string' THEN
  FOR pos IN 1..char_length(result) LOOP
   ch:=substr(result,pos,1); point:=ascii(ch);
   IF point>65535 THEN
    point:=point-65536;
    escaped:=escaped||chr(92)||'u'||lpad(to_hex(55296+(point/1024)),4,'0')||chr(92)||'u'||lpad(to_hex(56320+(point%1024)),4,'0');
   ELSIF point>127 THEN escaped:=escaped||chr(92)||'u'||lpad(to_hex(point),4,'0');
   ELSE escaped:=escaped||ch; END IF;
  END LOOP;
  result:=escaped;
 END IF;
 END IF;
 RETURN result;
END $$""",
    r"""CREATE FUNCTION public.sec_positive_chain_binding(plan_id uuid, at_time timestamptz) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
DECLARE p public.research_monitor_plans; cp public.research_monitor_plans; prior public.research_monitor_plans;
 own public.data_snapshots; origin public.data_snapshots; capture public.data_snapshots;
 proposal jsonb; current_proposal jsonb; seen uuid[]:=ARRAY[]::uuid[]; cutoff timestamptz:=at_time;
 root_id text; root_hash text;
BEGIN
 SELECT * INTO p FROM public.research_monitor_plans WHERE id=plan_id AND kind='disclosure';
 SELECT * INTO own FROM public.data_snapshots WHERE source='nexus_sec_forward_origin' AND provider_object_id=p.id::text;
 IF p.id IS NULL OR own.id IS NULL THEN RETURN NULL; END IF;
 cp:=p; origin:=own;
 LOOP
  SELECT o.intent->'disclosure_proposal' INTO current_proposal
   FROM public.agent_research_outputs o JOIN public.agent_research_runs r ON r.id=o.run_id
   WHERE o.id=cp.source_output_id AND o.run_id=cp.source_run_id AND o.role IS NULL
    AND o.status='completed' AND o.content_hash=cp.source_hash AND r.status='completed'
    AND r.instrument_id=cp.instrument_id AND r.data_snapshot_id=cp.data_snapshot_id
    AND r.decision_time=cp.baseline_as_of
    AND GREATEST(o.created_at,r.created_at,r.decision_time)<=origin.created_at;
  IF proposal IS NULL THEN proposal:=current_proposal; END IF;
  IF current_proposal IS NULL OR cp.id=ANY(seen)
   OR ((proposal->>'issuer_cik') ~ '^[0-9]{10}$') IS NOT TRUE
   OR current_proposal->>'issuer_cik' IS DISTINCT FROM proposal->>'issuer_cik'
   OR current_proposal->>'baseline_snapshot_id' IS DISTINCT FROM cp.baseline_snapshot_id::text
   OR (current_proposal->>'baseline_as_of')::timestamptz IS DISTINCT FROM cp.baseline_as_of
   OR origin.source<>'nexus_sec_forward_origin' OR origin.provider_object_id IS DISTINCT FROM cp.id::text
   OR (SELECT count(*) FROM public.data_snapshots WHERE source='nexus_sec_forward_origin' AND provider_object_id=cp.id::text)<>1
   OR origin.payload->>'policy' IS DISTINCT FROM origin.schema_version
   OR origin.payload->>'plan_id' IS DISTINCT FROM cp.id::text
   OR origin.payload->>'account_id' IS DISTINCT FROM cp.account_id::text
   OR origin.payload->>'instrument_id' IS DISTINCT FROM cp.instrument_id::text
   OR origin.payload->>'source_hash' IS DISTINCT FROM cp.source_hash
   OR origin.payload->>'condition_hash' IS DISTINCT FROM cp.condition_hash
   OR origin.payload->>'baseline_snapshot_id' IS DISTINCT FROM cp.baseline_snapshot_id::text
   OR origin.payload->>'baseline_hash' IS DISTINCT FROM current_proposal->>'baseline_hash'
   OR (origin.payload->>'baseline_as_of')::timestamptz IS DISTINCT FROM cp.baseline_as_of
   OR origin.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(origin.payload),'UTF8')),'hex')
   OR origin.created_at IS DISTINCT FROM (origin.payload->>'effective_at')::timestamptz
   OR origin.observed_at IS DISTINCT FROM origin.created_at OR origin.available_at IS DISTINCT FROM origin.created_at OR origin.received_at IS DISTINCT FROM origin.created_at
   OR origin.created_at<GREATEST(cp.created_at,cp.effective_at) OR origin.created_at>=cp.valid_until OR origin.created_at>cutoff
   OR NOT EXISTS(SELECT FROM public.audit_events a WHERE a.action='research.monitor.disclosure_plan_installed'
     AND a.actor_id='research-monitor' AND a.schema_version='disclosure_plan_installation_v1'
     AND a.payload=jsonb_build_object('plan_id',cp.id::text,'source_output_id',cp.source_output_id::text,
      'predecessor_id',cp.predecessor_id::text,'baseline_snapshot_id',cp.baseline_snapshot_id::text,
      'proposal_hash',encode(sha256(convert_to(public.sec_positive_chain_canonical(current_proposal),'UTF8')),'hex'))
     AND a.occurred_at<=cutoff)
  THEN RETURN NULL; END IF;
  seen:=array_append(seen,cp.id);
  IF origin.schema_version='sec_forward_comparison_v2' THEN
   IF root_id IS NOT NULL AND (root_id,root_hash) IS DISTINCT FROM (origin.id::text,origin.revision) THEN RETURN NULL; END IF;
   EXIT;
  END IF;
  IF origin.schema_version<>'sec_forward_inheritance_v1'
   OR origin.payload->>'issuer_cik' IS DISTINCT FROM proposal->>'issuer_cik'
   OR (root_id IS NOT NULL AND (root_id,root_hash) IS DISTINCT FROM (origin.payload->>'root_origin_id',origin.payload->>'root_origin_hash'))
  THEN RETURN NULL; END IF;
  root_id:=origin.payload->>'root_origin_id'; root_hash:=origin.payload->>'root_origin_hash';
  SELECT * INTO prior FROM public.research_monitor_plans WHERE id=cp.predecessor_id;
  IF prior.id IS NULL OR cp.generation<>prior.generation+1
   OR (cp.account_id,cp.instrument_id,cp.kind,cp.condition_hash) IS DISTINCT FROM (prior.account_id,prior.instrument_id,prior.kind,prior.condition_hash)
   OR origin.payload->>'predecessor_plan_id' IS DISTINCT FROM prior.id::text THEN RETURN NULL; END IF;
  cutoff:=origin.created_at;
  SELECT * INTO origin FROM public.data_snapshots s WHERE s.id=(origin.payload->>'predecessor_origin_id')::uuid AND s.revision=origin.payload->>'predecessor_origin_hash';
  IF origin.id IS NULL THEN RETURN NULL; END IF;
  cp:=prior;
 END LOOP;
 SELECT * INTO capture FROM public.data_snapshots WHERE id=(origin.payload->>'capture_id')::uuid;
 IF capture.id IS NULL OR capture.source<>'sec_disclosure_listed_index' OR capture.schema_version<>'sec_disclosure_index_v2'
  OR capture.revision IS DISTINCT FROM origin.payload->>'capture_hash'
  OR capture.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(capture.payload),'UTF8')),'hex')
  OR capture.payload#>>'{index,instrument_id}' IS DISTINCT FROM p.instrument_id::text
  OR capture.payload#>>'{index,issuer_cik}' IS DISTINCT FROM proposal->>'issuer_cik'
  OR GREATEST(capture.created_at,capture.observed_at,capture.available_at,capture.received_at)>origin.created_at
  OR origin.payload->>'old_history_gap' IS DISTINCT FROM 'unknown'
  OR origin.payload->'original_hashes' IS DISTINCT FROM (SELECT jsonb_agg(item->'sha256' ORDER BY ordinal) FROM jsonb_array_elements(capture.payload->'originals') WITH ORDINALITY x(item,ordinal))
 THEN RETURN NULL; END IF;
 RETURN jsonb_build_object('plan_id',p.id::text,'account_id',p.account_id::text,'instrument_id',p.instrument_id::text,
 'source_run_id',p.source_run_id::text,'source_hash',p.source_hash,'condition_hash',p.condition_hash,
 'own_origin_id',own.id::text,'own_origin_hash',own.revision,'root_origin_id',origin.id::text,'root_origin_hash',origin.revision,
 'comparison_snapshot_id',capture.id::text,'comparison_hash',capture.revision,'issuer_cik',proposal->>'issuer_cik');
END $$""",
    r"""CREATE FUNCTION public.sec_positive_chain_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE p public.research_monitor_plans; anchor public.research_monitor_plans; cp public.research_monitor_plans;
 scope public.data_snapshots; old_policy public.data_snapshots; expected jsonb; binding jsonb;
 at_time timestamptz; latest uuid; seen uuid[]:=ARRAY[]::uuid[];
BEGIN
 IF TG_OP<>'INSERT' THEN
  IF OLD.source IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke')
   OR (TG_OP='UPDATE' AND NEW.source IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke'))
  THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_IMMUTABLE' USING ERRCODE='23514'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
 END IF;
 IF NEW.source NOT IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke') THEN RETURN NEW; END IF;
 IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
 BEGIN
  at_time:=(NEW.payload->>'effective_at')::timestamptz;
  IF ((NEW.payload->>'effective_at') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?Z$') IS NOT TRUE
   OR at_time IS NULL OR NOT isfinite(at_time) OR at_time<transaction_timestamp() OR at_time>clock_timestamp()
   OR NEW.created_at IS DISTINCT FROM at_time OR NEW.observed_at IS DISTINCT FROM at_time OR NEW.available_at IS DISTINCT FROM at_time OR NEW.received_at IS DISTINCT FROM at_time
  THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
  IF NEW.source='nexus_sec_positive_chain_scope' THEN
   SELECT * INTO old_policy FROM public.data_snapshots WHERE id=(NEW.payload->>'anchor_policy_snapshot_id')::uuid
    AND source='nexus_sec_recent_positive_policy' AND schema_version='sec_recent_positive_once_v1';
   IF old_policy.id IS NULL OR old_policy.revision IS DISTINCT FROM NEW.payload->>'anchor_policy_hash'
    OR old_policy.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(old_policy.payload),'UTF8')),'hex')
    OR old_policy.created_at>at_time OR NOT EXISTS(SELECT FROM public.audit_events a
      WHERE a.action='research.monitor.recent_positive_policy_enabled' AND a.actor_id='research-monitor'
      AND a.schema_version=old_policy.schema_version AND a.occurred_at=old_policy.created_at
      AND a.payload=old_policy.payload||jsonb_build_object('snapshot_id',old_policy.id::text,'policy_hash',old_policy.revision))
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   expected:=old_policy.payload||jsonb_build_object('policy','sec_positive_chain_scope_v1','authorization_scope','single_c1_seen_additions_7d_v1',
    'anchor_policy_snapshot_id',old_policy.id::text,'anchor_policy_hash',old_policy.revision,'effective_at',NEW.payload->>'effective_at','expires_at',NEW.payload->>'expires_at');
   IF (NEW.payload->>'expires_at')::timestamptz IS DISTINCT FROM at_time+interval '168 hours'
    OR ((NEW.payload->>'expires_at') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?Z$') IS NOT TRUE
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
  ELSE
   SELECT * INTO scope FROM public.data_snapshots WHERE id=(NEW.payload->>'scope_snapshot_id')::uuid AND source='nexus_sec_positive_chain_scope' AND schema_version='sec_positive_chain_scope_v1';
   IF scope.id IS NULL OR scope.revision IS DISTINCT FROM NEW.payload->>'scope_hash' OR scope.created_at>at_time THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   PERFORM pg_advisory_xact_lock(hashtextextended('sec-positive-chain:'||scope.id::text,0));
   PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||(scope.payload->>'account_id')||':'||(scope.payload->>'instrument_id'),0));
   IF EXISTS(SELECT FROM public.data_snapshots WHERE source='nexus_sec_positive_chain_revoke' AND provider_object_id=scope.id::text) THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   IF NEW.source='nexus_sec_positive_chain_revoke' THEN
    expected:=jsonb_build_object('policy','sec_positive_chain_revoke_v1','scope_snapshot_id',scope.id::text,'scope_hash',scope.revision,
     'account_id',scope.payload->>'account_id','instrument_id',scope.payload->>'instrument_id','effective_at',NEW.payload->>'effective_at');
    IF NEW.provider_object_id IS DISTINCT FROM scope.id::text OR EXISTS(SELECT FROM public.research_monitor_checks c
      WHERE c.plan_id IN (SELECT (m.payload->>'plan_id')::uuid FROM public.data_snapshots m
       WHERE m.source='nexus_sec_positive_chain_member' AND m.payload->>'scope_snapshot_id'=scope.id::text) AND c.checked_at>=at_time)
    THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   ELSE
    IF at_time>=(scope.payload->>'expires_at')::timestamptz THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   END IF;
  END IF;
  IF NEW.source<>'nexus_sec_positive_chain_revoke' THEN
   IF NEW.source='nexus_sec_positive_chain_scope' THEN
    SELECT * INTO anchor FROM public.research_monitor_plans WHERE id=NEW.provider_object_id::uuid AND kind='disclosure';
    binding:=public.sec_positive_chain_binding(anchor.id,at_time);
    IF binding IS NULL OR binding IS DISTINCT FROM old_policy.payload-ARRAY['policy','root_request_id','effective_at','authorization_scope']
     THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
    SELECT * INTO p FROM public.research_monitor_plans WHERE account_id=anchor.account_id AND instrument_id=anchor.instrument_id AND kind='disclosure' ORDER BY generation DESC LIMIT 1;
   ELSE
    SELECT * INTO p FROM public.research_monitor_plans WHERE id=NEW.provider_object_id::uuid AND kind='disclosure';
    SELECT * INTO anchor FROM public.research_monitor_plans WHERE id=(scope.payload->>'plan_id')::uuid;
   END IF;
   IF p.id IS NULL THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||p.account_id::text||':'||p.instrument_id::text,0));
   LOCK TABLE public.research_monitor_plans IN SHARE MODE;
   IF at_time<GREATEST(p.created_at,p.effective_at) OR at_time>=p.valid_until
    OR EXISTS(SELECT FROM public.research_monitor_plans n WHERE n.account_id=p.account_id AND n.instrument_id=p.instrument_id AND n.kind='disclosure' AND n.generation>p.generation)
    OR EXISTS(SELECT FROM public.research_monitor_checks c WHERE c.plan_id=p.id AND c.checked_at>=at_time)
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   binding:=public.sec_positive_chain_binding(p.id,at_time);
   IF binding IS NULL OR NOT EXISTS(SELECT FROM public.instrument_aliases a WHERE a.instrument_id=p.instrument_id AND a.symbol='NVDA' AND a.valid_from<=at_time::date AND (a.valid_to IS NULL OR a.valid_to>=at_time::date))
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   SELECT r.id INTO latest FROM public.agent_research_runs r JOIN public.data_snapshots d ON d.id=r.data_snapshot_id
    LEFT JOIN public.agent_paper_cycles c ON d.payload#>>'{agent_paper_context,cycle_id}'=c.id::text
    LEFT JOIN public.research_monitor_increments i ON i.run_id=r.id
    WHERE (c.account_id=p.account_id OR i.account_id=p.account_id OR d.payload#>>'{independent_monitor_context,account_id}'=p.account_id::text)
    AND r.instrument_id=p.instrument_id AND r.decision_time<=at_time AND r.created_at<=at_time
    AND r.prompt_manifest#>>'{portfolio,version}' IN ('local-portfolio-synthesis-v8-price-monitor','local-portfolio-synthesis-v9-daily-pending','local-portfolio-synthesis-v10-sec-monitor','independent-monitor-research-v1','independent-monitor-research-v2')
    ORDER BY r.decision_time DESC,r.created_at DESC,r.id DESC LIMIT 1;
   IF latest IS DISTINCT FROM p.source_run_id AND NOT EXISTS(SELECT FROM public.research_monitor_coverage c WHERE c.kind='disclosure' AND c.account_id=p.account_id AND c.run_id=latest AND c.active_plan_id=p.id AND c.status IN ('installed','retained') AND c.observed_at<=at_time)
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   cp:=p;
    LOOP
     IF cp.id IS NULL OR cp.id=ANY(seen) OR (cp.account_id,cp.instrument_id,cp.kind,cp.condition_hash) IS DISTINCT FROM (anchor.account_id,anchor.instrument_id,anchor.kind,anchor.condition_hash)
      THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
     IF cp.id=anchor.id THEN EXIT; END IF;
     seen:=array_append(seen,cp.id);
     SELECT * INTO cp FROM public.research_monitor_plans WHERE id=cp.predecessor_id AND generation=cp.generation-1;
    END LOOP;
    IF (binding->>'root_origin_id',binding->>'root_origin_hash',binding->>'comparison_snapshot_id',binding->>'comparison_hash',binding->>'issuer_cik') IS DISTINCT FROM
       (coalesce(scope.payload,old_policy.payload)->>'root_origin_id',coalesce(scope.payload,old_policy.payload)->>'root_origin_hash',coalesce(scope.payload,old_policy.payload)->>'comparison_snapshot_id',coalesce(scope.payload,old_policy.payload)->>'comparison_hash',coalesce(scope.payload,old_policy.payload)->>'issuer_cik') THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
   IF NEW.source='nexus_sec_positive_chain_member' THEN
    expected:=binding||jsonb_build_object('policy','sec_positive_chain_member_v1','scope_snapshot_id',scope.id::text,'scope_hash',scope.revision,'effective_at',NEW.payload->>'effective_at');
   END IF;
  END IF;
  IF NEW.payload IS DISTINCT FROM expected OR NEW.schema_version IS DISTINCT FROM expected->>'policy'
   OR NEW.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(expected),'UTF8')),'hex')
   THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514'; END IF;
 EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format OR datetime_field_overflow OR data_exception THEN
  RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_INVALID' USING ERRCODE='23514';
 END;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_positive_chain_guard BEFORE INSERT OR UPDATE OR DELETE ON public.data_snapshots FOR EACH ROW EXECUTE FUNCTION public.sec_positive_chain_guard()""",
    r"""CREATE FUNCTION public.sec_positive_chain_audit_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots; expected_action text; n integer;
BEGIN
 IF TG_TABLE_NAME='data_snapshots' THEN
  IF NEW.source NOT IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke') THEN RETURN NEW; END IF; s:=NEW;
 ELSE
  IF NEW.action NOT IN ('research.monitor.positive_chain_scope_enabled','research.monitor.positive_chain_member_registered','research.monitor.positive_chain_scope_revoked') THEN RETURN NEW; END IF;
  SELECT * INTO s FROM public.data_snapshots WHERE id::text=NEW.payload->>'snapshot_id' AND source IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke');
 END IF;
 expected_action:=CASE s.source WHEN 'nexus_sec_positive_chain_scope' THEN 'research.monitor.positive_chain_scope_enabled' WHEN 'nexus_sec_positive_chain_member' THEN 'research.monitor.positive_chain_member_registered' WHEN 'nexus_sec_positive_chain_revoke' THEN 'research.monitor.positive_chain_scope_revoked' END;
 SELECT count(*) INTO n FROM public.audit_events a WHERE a.action=expected_action AND a.schema_version=s.schema_version AND a.actor_id='research-monitor'
  AND a.payload=s.payload||jsonb_build_object('snapshot_id',s.id::text,'policy_hash',s.revision) AND a.occurred_at=s.created_at;
 IF s.source='nexus_sec_positive_chain_scope' AND NOT EXISTS(
  SELECT FROM public.data_snapshots m WHERE m.source='nexus_sec_positive_chain_member'
   AND m.payload->>'scope_snapshot_id'=s.id::text AND m.payload->>'scope_hash'=s.revision
   AND m.created_at>=s.created_at AND m.payload->>'plan_id'=(SELECT p.id::text FROM public.research_monitor_plans p
    WHERE p.account_id=(s.payload->>'account_id')::uuid AND p.instrument_id=(s.payload->>'instrument_id')::uuid
     AND p.kind='disclosure' AND p.created_at<=s.created_at ORDER BY p.generation DESC LIMIT 1))
 THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 IF s.id IS NULL OR n<>1 THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 IF TG_TABLE_NAME='audit_events' THEN
  IF NEW.action IS DISTINCT FROM expected_action THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$""",
    r"""CREATE CONSTRAINT TRIGGER sec_positive_chain_complete AFTER INSERT ON public.data_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_positive_chain_audit_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_positive_chain_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_positive_chain_audit_complete()""",
)

DOWNGRADE_SQL = (
    r"""LOCK TABLE public.data_snapshots,public.audit_events IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    r"""DO $$ BEGIN
 IF EXISTS(SELECT FROM public.data_snapshots WHERE source IN ('nexus_sec_positive_chain_scope','nexus_sec_positive_chain_member','nexus_sec_positive_chain_revoke'))
 OR EXISTS(SELECT FROM public.audit_events WHERE action IN ('research.monitor.positive_chain_scope_enabled','research.monitor.positive_chain_member_registered','research.monitor.positive_chain_scope_revoked'))
 THEN RAISE EXCEPTION 'SEC_POSITIVE_CHAIN_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF;
END $$""",
    r"""DROP TRIGGER sec_positive_chain_complete ON public.audit_events""",
    r"""DROP TRIGGER sec_positive_chain_complete ON public.data_snapshots""",
    r"""DROP TRIGGER sec_positive_chain_guard ON public.data_snapshots""",
    r"""DROP FUNCTION public.sec_positive_chain_audit_complete()""",
    r"""DROP FUNCTION public.sec_positive_chain_guard()""",
    r"""DROP FUNCTION public.sec_positive_chain_binding(uuid,timestamptz)""",
    r"""DROP FUNCTION public.sec_positive_chain_canonical(jsonb)""",
    r"""DROP INDEX public.uq_sec_positive_chain_audit""",
    r"""DROP INDEX public.uq_sec_positive_chain_revoke""",
    r"""DROP INDEX public.uq_sec_positive_chain_member""",
    r"""DROP INDEX public.uq_sec_positive_chain_plan""",
    r"""DROP INDEX public.uq_sec_positive_chain_root""",
    r"""DROP INDEX public.uq_sec_positive_chain_anchor""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
