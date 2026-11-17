"""Check the actual frozen SEC dependencies of every new model send."""

from alembic import op

revision = "0133_sec_send_scope"
down_revision = "0132_sec_daily_pending"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""CREATE FUNCTION public.sec_research_run_has_dependencies(run_uuid uuid) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
SELECT EXISTS(
 SELECT FROM public.research_monitor_increments m LEFT JOIN public.research_monitor_batches b ON b.id=m.batch_id
 WHERE m.run_id=run_uuid AND (b.routing_version='durable_research_v2' OR EXISTS(
  SELECT FROM public.research_monitor_triggers t JOIN public.sec_monitor_rules rule_row ON rule_row.account_id=t.account_id AND rule_row.instrument_id=t.instrument_id
  WHERE t.kind='disclosure' AND t.account_id=m.account_id AND t.instrument_id=m.instrument_id
   AND (t.id=m.trigger_id OR (m.batch_id IS NOT NULL AND t.batch_id=m.batch_id AND b.trigger_ids ? t.id::text))
  ))) OR EXISTS(
 SELECT FROM public.agent_research_runs r JOIN public.data_snapshots s ON s.id=r.data_snapshot_id
 WHERE r.id=run_uuid AND s.payload#>'{agent_paper_context,pending_durable_sec}' IS NOT NULL
  AND s.payload#>'{agent_paper_context,pending_durable_sec}'<>'null'::jsonb)
$$""",
    r"""CREATE FUNCTION public.sec_research_require_send_event(scope_account uuid,scope_instrument uuid,rule_uuid uuid,stable_key text,frozen_entry jsonb,plan_uuid uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE rule_row public.sec_monitor_rules; latest public.data_snapshots; plan_row public.research_monitor_plans; at_time timestamptz;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('research-monitor:'||scope_account::text||':'||scope_instrument::text,0))
 OR NOT pg_try_advisory_xact_lock(hashtextextended('sec-durable-rule:'||rule_uuid::text,0)) THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_BUSY' USING ERRCODE='55P03'; END IF;
 at_time:=clock_timestamp();
 SELECT * INTO rule_row FROM public.sec_monitor_rules WHERE id=rule_uuid;
 IF rule_row.id IS NULL OR (rule_row.account_id,rule_row.instrument_id) IS DISTINCT FROM (scope_account,scope_instrument)
 OR greatest(rule_row.created_at,rule_row.effective_at)>at_time OR NOT public.sec_durable_active(rule_uuid)
 OR stable_key IS DISTINCT FROM 'sec:'||rule_row.issuer_cik||':'||(frozen_entry->>'accession_number')
 OR NOT(rule_row.forms ? (frozen_entry->>'form'))
 OR EXISTS(SELECT FROM public.sec_disclosure_key_conflicts conflict_row WHERE conflict_row.account_id=scope_account AND conflict_row.instrument_id=scope_instrument AND conflict_row.event_key=stable_key)
 OR EXISTS(SELECT FROM public.sec_discovery_events event_row JOIN public.sec_discovery_event_revisions revision_row ON revision_row.event_id=event_row.id WHERE event_row.account_id=scope_account AND event_row.instrument_id=scope_instrument AND event_row.event_key=stable_key)
 THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
 IF plan_uuid IS NOT NULL THEN
  SELECT * INTO plan_row FROM public.research_monitor_plans WHERE id=plan_uuid;
  IF plan_row.id IS NULL OR plan_row.kind<>'disclosure' OR (plan_row.account_id,plan_row.instrument_id) IS DISTINCT FROM (scope_account,scope_instrument)
   OR NOT(plan_row.effective_at<=at_time AND at_time<plan_row.valid_until)
   OR EXISTS(SELECT FROM public.research_monitor_plans newer WHERE newer.account_id=scope_account AND newer.instrument_id=scope_instrument AND newer.kind='disclosure' AND newer.generation>plan_row.generation)
  THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
 END IF;
 SELECT * INTO latest FROM public.data_snapshots capture_row
 WHERE capture_row.source IN ('sec_disclosure_index','sec_disclosure_listed_index','sec_durable_disclosure_index')
  AND (capture_row.provider_object_id LIKE scope_instrument::text||':%' OR capture_row.payload#>>'{index,instrument_id}'=scope_instrument::text)
  AND greatest(capture_row.created_at,capture_row.observed_at,capture_row.available_at,capture_row.received_at)<=at_time
 ORDER BY capture_row.observed_at DESC,capture_row.available_at DESC,capture_row.received_at DESC,capture_row.created_at DESC,capture_row.id DESC LIMIT 1;
 IF latest.id IS NULL OR NOT((latest.source='sec_disclosure_index' AND latest.schema_version='sec_disclosure_index_v1') OR (latest.source='sec_disclosure_listed_index' AND latest.schema_version='sec_disclosure_index_v2') OR (latest.source='sec_durable_disclosure_index' AND latest.schema_version='sec_durable_index_v1' AND latest.payload->>'schema_version'='sec_durable_index_v1'))
 OR latest.payload#>>'{index,instrument_id}' IS DISTINCT FROM scope_instrument::text OR latest.payload#>>'{index,issuer_cik}' IS DISTINCT FROM rule_row.issuer_cik
 OR latest.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(latest.payload),'UTF8')),'hex')
 OR latest.payload#>>'{index,received_at}' IS NULL OR at_time-(latest.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
 OR NOT EXISTS(SELECT FROM jsonb_array_elements(latest.payload#>'{index,entries}') entry_row WHERE entry_row=frozen_entry)
 OR (latest.source='sec_durable_disclosure_index' AND EXISTS(SELECT FROM jsonb_array_elements(latest.payload->'issues') issue_row WHERE issue_row->>'accession_number'=frozen_entry->>'accession_number'))
 THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
END $$""",
    r"""CREATE FUNCTION public.sec_research_require_send_run(run_uuid uuid) RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE member public.research_monitor_increments; batch_row public.research_monitor_batches; dependency record; run_row public.agent_research_runs; source_row public.data_snapshots; frozen jsonb; event_row public.sec_discovery_events; has_dependency boolean:=false;
BEGIN
 SELECT * INTO member FROM public.research_monitor_increments WHERE run_id=run_uuid;
 IF member.id IS NOT NULL THEN
  SELECT * INTO batch_row FROM public.research_monitor_batches WHERE id=member.batch_id;
  IF batch_row.routing_version='durable_research_v2' THEN
   PERFORM public.sec_research_require_current_batch(batch_row.id);
   RETURN true;
  END IF;
  IF member.batch_id IS NOT NULL THEN PERFORM public.nexus_monitor_validate_batch_readonly(member.batch_id); END IF;
  FOR dependency IN
   SELECT trigger_row.event_key,check_row.observation,check_row.plan_id,rule_row.id AS rule_id
   FROM public.research_monitor_triggers trigger_row JOIN public.research_monitor_checks check_row ON check_row.id=trigger_row.check_id
   JOIN public.sec_monitor_rules rule_row ON rule_row.account_id=trigger_row.account_id AND rule_row.instrument_id=trigger_row.instrument_id
   WHERE trigger_row.kind='disclosure' AND trigger_row.account_id=member.account_id AND trigger_row.instrument_id=member.instrument_id
    AND (trigger_row.id=member.trigger_id OR (member.batch_id IS NOT NULL AND trigger_row.batch_id=member.batch_id AND batch_row.trigger_ids ? trigger_row.id::text))
   ORDER BY rule_row.id,trigger_row.id
  LOOP
   has_dependency:=true;
   PERFORM public.sec_research_require_send_event(member.account_id,member.instrument_id,dependency.rule_id,dependency.event_key,dependency.observation->'entry',dependency.plan_id);
  END LOOP;
 END IF;
 SELECT * INTO run_row FROM public.agent_research_runs WHERE id=run_uuid;
 SELECT * INTO source_row FROM public.data_snapshots WHERE id=run_row.data_snapshot_id;
 IF source_row.payload#>'{agent_paper_context,pending_durable_sec}' IS NOT NULL AND source_row.payload#>'{agent_paper_context,pending_durable_sec}'<>'null'::jsonb THEN
  IF source_row.payload#>>'{agent_paper_context,pending_durable_sec,purpose}' IS DISTINCT FROM 'durable_sec_pending_v1'
   OR source_row.payload#>>'{agent_paper_context,pending_durable_sec,instrument_id}' IS DISTINCT FROM run_row.instrument_id::text
   OR jsonb_typeof(source_row.payload#>'{agent_paper_context,pending_durable_sec,events}') IS DISTINCT FROM 'array'
   OR jsonb_array_length(source_row.payload#>'{agent_paper_context,pending_durable_sec,events}') NOT BETWEEN 1 AND 32
  THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
  FOR frozen IN SELECT x FROM jsonb_array_elements(source_row.payload#>'{agent_paper_context,pending_durable_sec,events}') x ORDER BY x->>'rule_id',x->>'event_id' LOOP
   SELECT * INTO event_row FROM public.sec_discovery_events WHERE id=(frozen->>'event_id')::uuid;
   IF event_row.id IS NULL OR event_row.instrument_id IS DISTINCT FROM run_row.instrument_id
    OR event_row.account_id::text IS DISTINCT FROM source_row.payload#>>'{agent_paper_context,pending_durable_sec,account_id}'
    OR event_row.rule_id::text IS DISTINCT FROM frozen->>'rule_id' OR event_row.entry IS DISTINCT FROM frozen->'entry'
    OR event_row.metadata_hash IS DISTINCT FROM frozen->>'metadata_hash' OR event_row.event_key IS DISTINCT FROM frozen->>'event_key'
   THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;
   has_dependency:=true;
   PERFORM public.sec_research_require_send_event(event_row.account_id,event_row.instrument_id,event_row.rule_id,event_row.event_key,event_row.entry,NULL);
  END LOOP;
 END IF;
 RETURN has_dependency;
END $$""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.sec_monitor_rules,public.research_stage_authorization IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_monitor_rules) THEN RAISE EXCEPTION 'SEC_SEND_SCOPE_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.execute("DROP FUNCTION public.sec_research_require_send_run(uuid)")
    op.execute(
        "DROP FUNCTION public.sec_research_require_send_event(uuid,uuid,uuid,text,jsonb,uuid)"
    )
    op.execute("DROP FUNCTION public.sec_research_run_has_dependencies(uuid)")
