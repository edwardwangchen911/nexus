"""Versioned SEC research custody, preserving old notification and native admission facts."""

from alembic import op

revision = "0127_sec_research_bridge"
down_revision = "0126_sec_durable_monitor"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""CREATE TABLE public.sec_research_function_backups(name text PRIMARY KEY,definition text NOT NULL)""",
    r"""INSERT INTO public.sec_research_function_backups(name,definition) SELECT p.oid::regprocedure::text,pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN ('nexus_monitor_batch_guard','sec_durable_legacy_notice_guard')""",
    r"""
CREATE TABLE sec_event_research_bindings (
	id UUID NOT NULL,
	event_id UUID NOT NULL,
	account_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	rule_id UUID NOT NULL,
	discovery_check_id UUID NOT NULL,
	metadata_hash VARCHAR(64) NOT NULL,
	condition_hash VARCHAR(64) NOT NULL,
	plan_id UUID NOT NULL,
	source_run_id UUID NOT NULL,
	source_output_id UUID NOT NULL,
	source_hash VARCHAR(64) NOT NULL,
	capture_id UUID NOT NULL,
	capture_hash VARCHAR(64) NOT NULL,
	check_id UUID NOT NULL,
	trigger_id UUID NOT NULL,
	batch_id UUID NOT NULL,
	bound_at TIMESTAMP WITH TIME ZONE NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_event_research_bindings PRIMARY KEY (id),
	CONSTRAINT uq_sec_event_research_bindings_event_id UNIQUE (event_id, plan_id),
	CONSTRAINT uq_sec_event_research_bindings_check_id UNIQUE (check_id),
	CONSTRAINT uq_sec_event_research_bindings_trigger_id UNIQUE (trigger_id),
	CONSTRAINT fk_sec_event_research_bindings_event_id_sec_discovery_events FOREIGN KEY(event_id) REFERENCES sec_discovery_events (id),
	CONSTRAINT fk_sec_event_research_bindings_account_id_paper_accounts FOREIGN KEY(account_id) REFERENCES paper_accounts (id),
	CONSTRAINT fk_sec_event_research_bindings_instrument_id_instruments FOREIGN KEY(instrument_id) REFERENCES instruments (id),
	CONSTRAINT fk_sec_event_research_bindings_rule_id_sec_monitor_rules FOREIGN KEY(rule_id) REFERENCES sec_monitor_rules (id),
	CONSTRAINT fk_sec_event_research_bindings_discovery_check_id_sec_d_4adf FOREIGN KEY(discovery_check_id) REFERENCES sec_discovery_checks (id),
	CONSTRAINT fk_sec_event_research_bindings_plan_id_research_monitor_plans FOREIGN KEY(plan_id) REFERENCES research_monitor_plans (id),
	CONSTRAINT fk_sec_event_research_bindings_source_run_id_agent_rese_ebe6 FOREIGN KEY(source_run_id) REFERENCES agent_research_runs (id),
	CONSTRAINT fk_sec_event_research_bindings_source_output_id_agent_r_0157 FOREIGN KEY(source_output_id) REFERENCES agent_research_outputs (id),
	CONSTRAINT fk_sec_event_research_bindings_capture_id_data_snapshots FOREIGN KEY(capture_id) REFERENCES data_snapshots (id),
	CONSTRAINT fk_sec_event_research_bindings_check_id_research_monitor_checks FOREIGN KEY(check_id) REFERENCES research_monitor_checks (id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT fk_sec_event_research_bindings_trigger_id_research_moni_2eef FOREIGN KEY(trigger_id) REFERENCES research_monitor_triggers (id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT fk_sec_event_research_bindings_batch_id_research_monito_769a FOREIGN KEY(batch_id) REFERENCES research_monitor_batches (id) DEFERRABLE INITIALLY DEFERRED
)

""",
    r"""
CREATE TABLE sec_event_consumption_receipts (
	id UUID NOT NULL,
	binding_id UUID NOT NULL,
	event_id UUID NOT NULL,
	status VARCHAR(16) DEFAULT 'addressed' NOT NULL,
	mixed_receipt_id UUID,
	daily_receipt_id UUID,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_event_consumption_receipts PRIMARY KEY (id),
	CONSTRAINT ck_sec_event_consumption_receipts_exact_proof CHECK (num_nonnulls(mixed_receipt_id,daily_receipt_id)=1),
	CONSTRAINT ck_sec_event_consumption_receipts_addressed_only CHECK (status='addressed'),
	CONSTRAINT uq_sec_event_consumption_receipts_mixed_receipt_id UNIQUE (mixed_receipt_id),
	CONSTRAINT uq_sec_event_consumption_receipts_daily_receipt_id UNIQUE (daily_receipt_id),
	CONSTRAINT fk_sec_event_consumption_receipts_binding_id_sec_event__b2fc FOREIGN KEY(binding_id) REFERENCES sec_event_research_bindings (id),
	CONSTRAINT fk_sec_event_consumption_receipts_event_id_sec_discovery_events FOREIGN KEY(event_id) REFERENCES sec_discovery_events (id),
	CONSTRAINT fk_sec_event_consumption_receipts_mixed_receipt_id_rese_1c60 FOREIGN KEY(mixed_receipt_id) REFERENCES research_monitor_mixed_event_receipts (id),
	CONSTRAINT fk_sec_event_consumption_receipts_daily_receipt_id_rese_b055 FOREIGN KEY(daily_receipt_id) REFERENCES research_monitor_daily_event_receipts (id)
)

""",
    r"""ALTER TABLE public.research_monitor_batches ADD COLUMN routing_version varchar(32) NOT NULL DEFAULT 'legacy_v1' CHECK(routing_version IN ('legacy_v1','durable_research_v2'))""",
    r"""ALTER TABLE public.research_monitor_triggers ALTER COLUMN outbox_id DROP NOT NULL""",
    r"""ALTER TABLE public.research_monitor_triggers ADD COLUMN durable_binding_id uuid UNIQUE REFERENCES public.sec_event_research_bindings(id) DEFERRABLE INITIALLY DEFERRED""",
    r"""ALTER TABLE public.research_monitor_triggers ADD CONSTRAINT ck_research_monitor_triggers_research_notice_routing CHECK ((durable_binding_id IS NULL AND outbox_id IS NOT NULL) OR (durable_binding_id IS NOT NULL AND outbox_id IS NULL AND batch_id IS NOT NULL AND kind='disclosure'))""",
    r"""DROP INDEX public.uq_monitor_disclosure_event""",
    r"""CREATE UNIQUE INDEX uq_monitor_disclosure_event ON public.research_monitor_triggers(account_id,instrument_id,event_key) WHERE kind='disclosure' AND durable_binding_id IS NULL""",
    r"""CREATE UNIQUE INDEX uq_sec_research_audit ON public.audit_events((payload->>'table'),(payload->>'row_id')) WHERE action='research.monitor.sec_research_fact'""",
    r"""CREATE FUNCTION public.sec_research_require_lineage(source_run uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF EXISTS(
  WITH RECURSIVE ancestry(run_id) AS (
   SELECT source_run UNION
   SELECT p.source_run_id FROM ancestry a JOIN public.research_monitor_increments m ON m.run_id=a.run_id
   JOIN public.data_snapshots s ON s.id=m.snapshot_id
   CROSS JOIN LATERAL (SELECT m.plan_id AS id UNION SELECT (x->>'plan_id')::uuid FROM jsonb_array_elements(coalesce(s.payload#>'{monitor_context,materials,parents}','[]'::jsonb)) x) links
   JOIN public.research_monitor_plans p ON p.id=links.id
  ) SELECT FROM ancestry a JOIN public.agent_research_runs r ON r.id=a.run_id
  WHERE (r.actor_id='local-single-monitor-sol-v1' OR r.idempotency_key LIKE 'single-monitor-sol:%')
   AND public.single_monitor_root_request(r.id) IS NULL
 ) THEN RAISE EXCEPTION 'SEC_RESEARCH_LEGACY_LINEAGE_UNPROVEN' USING ERRCODE='23514'; END IF;
END $$""",
    r"""CREATE FUNCTION public.sec_research_binding_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE b public.sec_event_research_bindings; e public.sec_discovery_events; p public.research_monitor_plans; t public.research_monitor_triggers; c public.research_monitor_checks; r public.sec_monitor_rules; s public.data_snapshots;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'SEC_RESEARCH_IMMUTABLE' USING ERRCODE='23514'; END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'SEC_RESEARCH_INVALID' USING ERRCODE='23514'; END IF;
 SELECT * INTO e FROM public.sec_discovery_events WHERE id=NEW.event_id;
 IF e.id IS NULL THEN RAISE EXCEPTION 'SEC_RESEARCH_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||e.account_id::text||':'||e.instrument_id::text,0));
 SELECT * INTO r FROM public.sec_monitor_rules WHERE id=e.rule_id;
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||r.id::text,0));
 IF NOT public.sec_durable_active(r.id) OR EXISTS(SELECT FROM public.sec_discovery_event_revisions WHERE event_id=e.id) THEN RAISE EXCEPTION 'SEC_RESEARCH_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM public.sec_research_require_lineage(NEW.source_run_id);
 SELECT * INTO p FROM public.research_monitor_plans WHERE id=NEW.plan_id;
 SELECT * INTO s FROM public.data_snapshots WHERE id=NEW.capture_id;
 IF (NEW.account_id,NEW.instrument_id) IS DISTINCT FROM (e.account_id,e.instrument_id)
 OR NEW.rule_id IS DISTINCT FROM e.rule_id OR NEW.metadata_hash IS DISTINCT FROM e.metadata_hash
 OR NEW.condition_hash IS DISTINCT FROM p.condition_hash
 OR NOT EXISTS(SELECT FROM public.sec_discovery_checks d WHERE d.id=NEW.discovery_check_id AND d.rule_id=e.rule_id AND d.capture_id=e.capture_id AND d.result='true' AND d.checked_at<=NEW.bound_at AND d.observation @> jsonb_build_object('events',jsonb_build_array(jsonb_build_object('event_key',e.event_key,'classification','new','entry',e.entry))))
 OR p.id IS NULL OR p.kind<>'disclosure' OR (p.account_id,p.instrument_id) IS DISTINCT FROM (e.account_id,e.instrument_id)
 OR NOT EXISTS(SELECT FROM public.agent_research_outputs source_output WHERE source_output.id=p.source_output_id AND source_output.run_id=p.source_run_id AND source_output.role IS NULL AND source_output.status='completed' AND source_output.content_hash=p.source_hash AND source_output.intent#>>'{disclosure_proposal,issuer_cik}'=r.issuer_cik)
 OR (NEW.source_run_id,NEW.source_output_id,NEW.source_hash) IS DISTINCT FROM (p.source_run_id,p.source_output_id,p.source_hash)
 OR NOT(p.effective_at<=NEW.bound_at AND NEW.bound_at<p.valid_until)
 OR NEW.bound_at<>NEW.created_at OR NEW.bound_at<transaction_timestamp() OR NEW.bound_at>clock_timestamp()
 OR EXISTS(SELECT FROM public.research_monitor_plans newer WHERE newer.account_id=p.account_id AND newer.instrument_id=p.instrument_id AND newer.kind=p.kind AND newer.generation>p.generation)
 OR s.id IS NULL OR s.source<>'sec_durable_disclosure_index'
 OR s.schema_version<>'sec_durable_index_v1' OR s.payload->>'schema_version' IS DISTINCT FROM 'sec_durable_index_v1' OR s.revision<>NEW.capture_hash
 OR s.payload#>>'{index,instrument_id}' IS DISTINCT FROM e.instrument_id::text OR s.payload#>>'{index,issuer_cik}' IS DISTINCT FROM r.issuer_cik
 OR s.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(s.payload),'UTF8')),'hex')
 OR NOT EXISTS(SELECT FROM jsonb_array_elements(s.payload#>'{index,entries}') x WHERE x=e.entry)
 OR EXISTS(SELECT FROM jsonb_array_elements(s.payload->'issues') x WHERE x->>'accession_number'=e.entry->>'accession_number')
 OR greatest(s.created_at,s.received_at,s.observed_at,s.available_at)>NEW.bound_at
 OR NEW.bound_at-(s.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
 OR EXISTS(SELECT FROM public.sec_event_research_bindings saved_binding JOIN public.research_monitor_increments m ON m.batch_id=saved_binding.batch_id WHERE saved_binding.event_id=e.id)
 OR EXISTS(SELECT FROM public.sec_event_consumption_receipts WHERE event_id=e.id)
 THEN RAISE EXCEPTION 'SEC_RESEARCH_INVALID' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_research_binding_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_event_research_bindings FOR EACH ROW EXECUTE FUNCTION public.sec_research_binding_guard()""",
    r"""CREATE FUNCTION public.sec_research_validate_batch(batch_uuid uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE b public.research_monitor_batches; n int;
BEGIN
 SELECT * INTO b FROM public.research_monitor_batches WHERE id=batch_uuid;
 IF b.id IS NULL OR b.routing_version<>'durable_research_v2' OR b.outbox_id IS NOT NULL OR jsonb_array_length(b.plan_ids)<>1 OR jsonb_array_length(b.trigger_ids) NOT BETWEEN 1 AND 32 OR jsonb_array_length(b.check_ids)<>jsonb_array_length(b.trigger_ids) THEN RAISE EXCEPTION 'SEC_RESEARCH_BATCH_INVALID' USING ERRCODE='23514'; END IF;
 SELECT count(*) INTO n FROM public.research_monitor_triggers t JOIN public.research_monitor_checks c ON c.id=t.check_id JOIN public.sec_event_research_bindings x ON x.id=t.durable_binding_id JOIN public.sec_discovery_events e ON e.id=x.event_id JOIN public.research_monitor_plans p ON p.id=x.plan_id
 WHERE t.batch_id=b.id AND t.id=x.trigger_id AND c.id=x.check_id AND x.batch_id=b.id AND t.outbox_id IS NULL
 AND t.kind='disclosure' AND c.kind=t.kind AND p.kind=t.kind AND t.event_key=e.event_key AND c.event_key=e.event_key
 AND (t.account_id,t.instrument_id,c.account_id,c.instrument_id,x.account_id,x.instrument_id)=(b.account_id,b.instrument_id,b.account_id,b.instrument_id,b.account_id,b.instrument_id)
 AND c.plan_id=p.id AND t.condition_hash=p.condition_hash AND b.plan_ids ? p.id::text AND b.check_ids ? c.id::text AND b.trigger_ids ? t.id::text
 AND c.result='true' AND c.reason IS NULL AND c.checked_at=x.bound_at AND c.checked_at<=b.checked_at AND greatest(t.created_at,c.created_at,x.created_at)<=b.sealed_at
 AND c.observation=jsonb_build_object('policy','durable_sec_research_binding_v1','binding_id',x.id::text,'event_id',e.id::text,'rule_id',e.rule_id::text,'event_key',e.event_key,'current_snapshot_id',x.capture_id::text,'current_hash',x.capture_hash,'received_at',c.observation->>'received_at','entry',e.entry)
 AND (c.observation->>'received_at')::timestamptz=(SELECT (payload#>>'{index,received_at}')::timestamptz FROM public.data_snapshots WHERE id=x.capture_id);
 IF b.observation_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM jsonb_array_elements_text(b.trigger_ids) AS event_ids(value))),'UTF8')),'hex') THEN RAISE EXCEPTION 'SEC_RESEARCH_BATCH_HASH_INVALID' USING ERRCODE='23514'; END IF;
 IF n<>jsonb_array_length(b.trigger_ids) OR n<>(SELECT count(*) FROM public.research_monitor_triggers WHERE batch_id=b.id)
 OR (SELECT count(DISTINCT v) FROM jsonb_array_elements_text(b.trigger_ids) v)<>n OR (SELECT count(DISTINCT v) FROM jsonb_array_elements_text(b.check_ids) v)<>n THEN RAISE EXCEPTION 'SEC_RESEARCH_BATCH_INVALID' USING ERRCODE='23514'; END IF;
END $$""",
    r"""ALTER FUNCTION public.nexus_monitor_validate_batch(uuid) RENAME TO nexus_monitor_validate_batch_legacy_v1""",
    r"""ALTER FUNCTION public.nexus_monitor_validate_batch_readonly(uuid) RENAME TO nexus_monitor_validate_batch_readonly_legacy_v1""",
    r"""CREATE FUNCTION public.nexus_monitor_validate_batch(b uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN IF EXISTS(SELECT FROM public.research_monitor_batches WHERE id=b AND routing_version='durable_research_v2') THEN PERFORM public.sec_research_validate_batch(b); ELSE PERFORM public.nexus_monitor_validate_batch_legacy_v1(b); END IF; END $$""",
    r"""CREATE FUNCTION public.nexus_monitor_validate_batch_readonly(b uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN IF EXISTS(SELECT FROM public.research_monitor_batches WHERE id=b AND routing_version='durable_research_v2') THEN PERFORM public.sec_research_validate_batch(b); ELSE PERFORM public.nexus_monitor_validate_batch_readonly_legacy_v1(b); END IF; END $$""",
    r"""DO $outer$ DECLARE d text; BEGIN
 SELECT pg_get_functiondef('public.nexus_monitor_batch_guard()'::regprocedure) INTO d;
 IF strpos(d,'IF TG_TABLE_NAME=''research_monitor_triggers'' THEN')=0 THEN RAISE EXCEPTION 'SEC_RESEARCH_PREDECESSOR_INVALID'; END IF;
 EXECUTE replace(d,'IF TG_TABLE_NAME=''research_monitor_triggers'' THEN','IF TG_TABLE_NAME=''research_monitor_triggers'' THEN IF NEW.durable_binding_id IS NOT NULL THEN PERFORM public.sec_research_validate_batch(NEW.batch_id); RETURN NULL; END IF;');
 SELECT pg_get_functiondef('public.sec_durable_legacy_notice_guard()'::regprocedure) INTO d;
 IF strpos(d,'IF NEW.kind<>''disclosure'' THEN RETURN NEW; END IF;')=0 THEN RAISE EXCEPTION 'SEC_RESEARCH_PREDECESSOR_INVALID'; END IF;
 EXECUTE replace(d,'IF NEW.kind<>''disclosure'' THEN RETURN NEW; END IF;','IF NEW.kind<>''disclosure'' THEN RETURN NEW; END IF; IF NEW.durable_binding_id IS NOT NULL THEN IF NEW.outbox_id IS NOT NULL OR NOT EXISTS(SELECT FROM public.sec_event_research_bindings x JOIN public.sec_discovery_events e ON e.id=x.event_id JOIN public.sec_monitor_rules scope_rule ON scope_rule.id=e.rule_id WHERE x.id=NEW.durable_binding_id AND x.trigger_id=NEW.id AND x.check_id=NEW.check_id AND x.batch_id=NEW.batch_id AND x.account_id=NEW.account_id AND x.instrument_id=NEW.instrument_id AND e.event_key=NEW.event_key AND public.sec_durable_active(scope_rule.id)) THEN RAISE EXCEPTION ''SEC_RESEARCH_INVALID'' USING ERRCODE=''23514''; END IF; RETURN NEW; END IF;');
END $outer$""",
    r"""CREATE FUNCTION public.sec_research_fact_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE v jsonb; t text; rid text; n int; x public.sec_event_research_bindings;
BEGIN
 IF TG_TABLE_NAME='audit_events' THEN IF NEW.action<>'research.monitor.sec_research_fact' THEN RETURN NEW; END IF; t:=NEW.payload->>'table';rid:=NEW.payload->>'row_id'; IF t NOT IN ('sec_event_research_bindings','sec_event_consumption_receipts') THEN RAISE EXCEPTION 'SEC_RESEARCH_AUDIT_INVALID'; END IF; EXECUTE format('SELECT to_jsonb(x) FROM public.%I x WHERE id::text=$1',t) INTO v USING rid;
 ELSE t:=TG_TABLE_NAME;v:=to_jsonb(NEW);rid:=NEW.id::text; END IF;
 SELECT count(*) INTO n FROM public.audit_events a WHERE a.action='research.monitor.sec_research_fact' AND a.schema_version='durable_sec_research_binding_v1' AND a.actor_id='research-monitor' AND a.occurred_at=(v->>'created_at')::timestamptz AND a.payload=jsonb_build_object('table',t,'row_id',rid,'binding',v-'created_at');
 IF v IS NULL OR n<>1 THEN RAISE EXCEPTION 'SEC_RESEARCH_AUDIT_INVALID'; END IF;
 IF t='sec_event_research_bindings' THEN
  SELECT * INTO x FROM public.sec_event_research_bindings WHERE id=(v->>'id')::uuid;
  IF NOT EXISTS(SELECT FROM public.research_monitor_triggers trigger_row
   JOIN public.research_monitor_checks check_row ON check_row.id=x.check_id
   JOIN public.research_monitor_batches batch_row ON batch_row.id=x.batch_id
   JOIN public.sec_discovery_events event_row ON event_row.id=x.event_id
   WHERE trigger_row.id=x.trigger_id AND trigger_row.durable_binding_id=x.id
    AND trigger_row.check_id=x.check_id AND trigger_row.batch_id=x.batch_id
    AND check_row.plan_id=x.plan_id AND check_row.event_key=event_row.event_key
    AND trigger_row.event_key=event_row.event_key
    AND (check_row.account_id,check_row.instrument_id,trigger_row.account_id,trigger_row.instrument_id,batch_row.account_id,batch_row.instrument_id)=(x.account_id,x.instrument_id,x.account_id,x.instrument_id,x.account_id,x.instrument_id)
    AND batch_row.plan_ids ? x.plan_id::text AND batch_row.check_ids ? x.check_id::text
    AND batch_row.trigger_ids ? x.trigger_id::text)
  THEN RAISE EXCEPTION 'SEC_RESEARCH_BINDING_REVERSE_INVALID' USING ERRCODE='23514'; END IF;
  PERFORM public.sec_research_validate_batch(x.batch_id);
 ELSE
  SELECT * INTO x FROM public.sec_event_research_bindings WHERE id=(v->>'binding_id')::uuid AND event_id=(v->>'event_id')::uuid;
  IF x.id IS NULL OR NOT (EXISTS(SELECT FROM public.research_monitor_mixed_event_receipts m WHERE m.id=(v->>'mixed_receipt_id')::uuid AND m.trigger_id=x.trigger_id AND m.batch_id=x.batch_id AND greatest(m.created_at,m.observed_at)<=(v->>'created_at')::timestamptz) OR EXISTS(SELECT FROM public.research_monitor_daily_event_receipts d WHERE d.id=(v->>'daily_receipt_id')::uuid AND d.trigger_id=x.trigger_id AND d.event_key=(SELECT event_key FROM public.sec_discovery_events WHERE id=x.event_id) AND greatest(d.created_at,d.observed_at)<=(v->>'created_at')::timestamptz)) THEN RAISE EXCEPTION 'SEC_RESEARCH_CONSUMPTION_INVALID'; END IF;
 END IF;
 RETURN NEW;
END $$""",
    r"""CREATE FUNCTION public.sec_research_receipt_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE b public.sec_event_research_bindings;
BEGIN
 IF NEW.created_at<transaction_timestamp() OR NEW.created_at>clock_timestamp() THEN RAISE EXCEPTION 'SEC_RESEARCH_RECEIPT_TIME_INVALID' USING ERRCODE='23514'; END IF;
 SELECT * INTO b FROM public.sec_event_research_bindings WHERE id=NEW.binding_id AND event_id=NEW.event_id;
 IF b.id IS NULL OR b.bound_at>NEW.created_at THEN RAISE EXCEPTION 'SEC_RESEARCH_CONSUMPTION_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||b.account_id::text||':'||b.instrument_id::text,0));
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_research_receipt_guard BEFORE INSERT ON public.sec_event_consumption_receipts FOR EACH ROW EXECUTE FUNCTION public.sec_research_receipt_guard()""",
    r"""CREATE TRIGGER sec_research_receipt_immutable BEFORE UPDATE OR DELETE ON public.sec_event_consumption_receipts FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()""",
    r"""CREATE CONSTRAINT TRIGGER sec_research_fact_complete AFTER INSERT ON public.sec_event_research_bindings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_research_fact_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_research_fact_complete AFTER INSERT ON public.sec_event_consumption_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_research_fact_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_research_fact_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_research_fact_complete()""",
    "CREATE FUNCTION public.sec_research_require_current_batch(batch_uuid uuid) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$\nDECLARE batch_row public.research_monitor_batches; evidence public.data_snapshots; at_time timestamptz:=clock_timestamp(); link record;\nBEGIN\n SELECT * INTO batch_row FROM public.research_monitor_batches WHERE id=batch_uuid;\n IF batch_row.id IS NULL OR batch_row.routing_version<>'durable_research_v2' THEN RETURN; END IF;\n IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;\n IF NOT pg_try_advisory_xact_lock(hashtextextended('research-monitor:'||batch_row.account_id::text||':'||batch_row.instrument_id::text,0)) THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_BUSY' USING ERRCODE='55P03'; END IF;\n FOR link IN SELECT DISTINCT scope_rule.id FROM public.sec_event_research_bindings binding JOIN public.sec_monitor_rules scope_rule ON scope_rule.id=binding.rule_id WHERE binding.batch_id=batch_uuid ORDER BY scope_rule.id LOOP\n  IF NOT pg_try_advisory_xact_lock(hashtextextended('sec-durable-rule:'||link.id::text,0)) THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_BUSY' USING ERRCODE='55P03'; END IF;\n END LOOP;\n PERFORM public.sec_research_validate_batch(batch_uuid);\n SELECT * INTO evidence FROM public.data_snapshots source_row WHERE source_row.source='sec_durable_disclosure_index'\n AND (source_row.provider_object_id LIKE batch_row.instrument_id::text||':%' OR source_row.payload#>>'{index,instrument_id}'=batch_row.instrument_id::text)\n AND greatest(source_row.created_at,source_row.observed_at,source_row.available_at,source_row.received_at)<=at_time\n ORDER BY source_row.observed_at DESC,source_row.available_at DESC,source_row.id DESC LIMIT 1;\n IF evidence.id IS NULL OR evidence.schema_version<>'sec_durable_index_v1' OR evidence.payload->>'schema_version' IS DISTINCT FROM 'sec_durable_index_v1'\n OR evidence.payload#>>'{index,instrument_id}' IS DISTINCT FROM batch_row.instrument_id::text\n OR evidence.payload#>>'{index,received_at}' IS NULL OR jsonb_typeof(evidence.payload#>'{index,entries}') IS DISTINCT FROM 'array'\n OR at_time-(evidence.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'\n OR evidence.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(evidence.payload),'UTF8')),'hex')\n OR EXISTS(SELECT FROM public.sec_event_research_bindings binding JOIN public.sec_discovery_events event_row ON event_row.id=binding.event_id JOIN public.sec_monitor_rules scope_rule ON scope_rule.id=binding.rule_id JOIN public.research_monitor_plans plan_row ON plan_row.id=binding.plan_id\n  WHERE binding.batch_id=batch_uuid AND (\n   NOT public.sec_durable_active(scope_rule.id) OR (scope_rule.account_id,scope_rule.instrument_id) IS DISTINCT FROM (binding.account_id,binding.instrument_id)\n   OR event_row.rule_id IS DISTINCT FROM scope_rule.id OR event_row.metadata_hash IS DISTINCT FROM binding.metadata_hash\n   OR evidence.payload#>>'{index,issuer_cik}' IS DISTINCT FROM scope_rule.issuer_cik\n   OR EXISTS(SELECT FROM public.sec_discovery_event_revisions revision_row WHERE revision_row.event_id=event_row.id)\n   OR NOT EXISTS(SELECT FROM jsonb_array_elements(evidence.payload#>'{index,entries}') entry_row WHERE entry_row=event_row.entry)\n   OR EXISTS(SELECT FROM jsonb_array_elements(evidence.payload->'issues') issue_row WHERE issue_row->>'accession_number'=event_row.entry->>'accession_number')\n   OR NOT(plan_row.effective_at<=at_time AND at_time<plan_row.valid_until)\n   OR EXISTS(SELECT FROM public.research_monitor_plans newer WHERE newer.account_id=plan_row.account_id AND newer.instrument_id=plan_row.instrument_id AND newer.kind=plan_row.kind AND newer.generation>plan_row.generation)\n  )) THEN RAISE EXCEPTION 'SEC_RESEARCH_PERMISSION_INVALID' USING ERRCODE='23514'; END IF;\nEND $$",
    "CREATE FUNCTION public.sec_research_increment_permission() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN PERFORM public.sec_research_require_current_batch(NEW.batch_id); RETURN NEW; END $$",
    "CREATE TRIGGER sec_research_increment_permission BEFORE INSERT ON public.research_monitor_increments FOR EACH ROW EXECUTE FUNCTION public.sec_research_increment_permission()",
)

DOWNGRADE_SQL = (
    r"""LOCK TABLE public.sec_event_research_bindings,public.sec_event_consumption_receipts,public.research_monitor_batches,public.research_monitor_triggers IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    r"""DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_event_research_bindings) OR EXISTS(SELECT FROM public.sec_event_consumption_receipts) OR EXISTS(SELECT FROM public.research_monitor_batches WHERE routing_version='durable_research_v2') OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.sec_research_fact') THEN RAISE EXCEPTION 'SEC_RESEARCH_DOWNGRADE_FORBIDDEN'; END IF; END $$""",
    "DROP TRIGGER sec_research_increment_permission ON public.research_monitor_increments",
    "DROP FUNCTION public.sec_research_increment_permission()",
    "DROP FUNCTION public.sec_research_require_current_batch(uuid)",
    r"""DROP TRIGGER sec_research_fact_complete ON public.audit_events""",
    r"""DROP FUNCTION public.nexus_monitor_validate_batch(uuid)""",
    r"""DROP FUNCTION public.nexus_monitor_validate_batch_readonly(uuid)""",
    r"""ALTER FUNCTION public.nexus_monitor_validate_batch_legacy_v1(uuid) RENAME TO nexus_monitor_validate_batch""",
    r"""ALTER FUNCTION public.nexus_monitor_validate_batch_readonly_legacy_v1(uuid) RENAME TO nexus_monitor_validate_batch_readonly""",
    r"""DO $$ DECLARE d text; BEGIN FOR d IN SELECT definition FROM public.sec_research_function_backups LOOP EXECUTE d; END LOOP; END $$""",
    r"""ALTER TABLE public.research_monitor_triggers DROP CONSTRAINT ck_research_monitor_triggers_research_notice_routing""",
    r"""DROP INDEX public.uq_monitor_disclosure_event""",
    r"""ALTER TABLE public.research_monitor_triggers DROP COLUMN durable_binding_id""",
    r"""ALTER TABLE public.research_monitor_triggers ALTER COLUMN outbox_id SET NOT NULL""",
    r"""CREATE UNIQUE INDEX uq_monitor_disclosure_event ON public.research_monitor_triggers(account_id,instrument_id,event_key) WHERE kind='disclosure'""",
    r"""ALTER TABLE public.research_monitor_batches DROP COLUMN routing_version""",
    r"""DROP TABLE public.sec_event_consumption_receipts""",
    r"""DROP TABLE public.sec_event_research_bindings""",
    r"""DROP FUNCTION public.sec_research_fact_complete()""",
    r"""DROP FUNCTION public.sec_research_receipt_guard()""",
    r"""DROP FUNCTION public.sec_research_validate_batch(uuid)""",
    r"""DROP FUNCTION public.sec_research_binding_guard()""",
    r"""DROP FUNCTION public.sec_research_require_lineage(uuid)""",
    r"""DROP TABLE public.sec_research_function_backups""",
    r"""DROP INDEX public.uq_sec_research_audit""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
