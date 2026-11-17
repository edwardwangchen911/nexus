"""Durable SEC facts, shared notice identity and per-security NY session claims.

Only scopes explicitly registered in sec_monitor_rules gain the new guards.
No legacy fact, old once limit, budget or authorization is changed.
"""

from alembic import op

revision = "0126_sec_durable_monitor"
down_revision = "0125_sec_positive_chain"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""CREATE TABLE sec_monitor_rules (
	id UUID NOT NULL,
	account_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	issuer_cik VARCHAR(10) NOT NULL,
	identity_snapshot_id UUID NOT NULL,
	anchor_plan_id UUID NOT NULL,
	effective_at TIMESTAMP WITH TIME ZONE NOT NULL,
	forms JSONB NOT NULL,
	policy_version VARCHAR(32) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_monitor_rules PRIMARY KEY (id),
	CONSTRAINT uq_sec_monitor_rules_account_id UNIQUE (account_id, instrument_id),
	CONSTRAINT ck_sec_monitor_rules_issuer CHECK (issuer_cik ~ '^[0-9]{10}$'),
	CONSTRAINT ck_sec_monitor_rules_policy CHECK (policy_version='durable_sec_v1'),
	CONSTRAINT fk_sec_monitor_rules_account_id_paper_accounts FOREIGN KEY(account_id) REFERENCES paper_accounts (id),
	CONSTRAINT fk_sec_monitor_rules_instrument_id_instruments FOREIGN KEY(instrument_id) REFERENCES instruments (id),
	CONSTRAINT fk_sec_monitor_rules_identity_snapshot_id_data_snapshots FOREIGN KEY(identity_snapshot_id) REFERENCES data_snapshots (id),
	CONSTRAINT fk_sec_monitor_rules_anchor_plan_id_research_monitor_plans FOREIGN KEY(anchor_plan_id) REFERENCES research_monitor_plans (id)
)

""",
    r"""CREATE TABLE sec_monitor_rule_states (
	id UUID NOT NULL,
	rule_id UUID NOT NULL,
	sequence INTEGER NOT NULL,
	state VARCHAR(16) NOT NULL,
	effective_at TIMESTAMP WITH TIME ZONE NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_monitor_rule_states PRIMARY KEY (id),
	CONSTRAINT uq_sec_monitor_rule_states_rule_id UNIQUE (rule_id, sequence),
	CONSTRAINT ck_sec_monitor_rule_states_state CHECK (state IN ('active','paused','revoked')),
	CONSTRAINT ck_sec_monitor_rule_states_sequence CHECK (sequence>=0),
	CONSTRAINT fk_sec_monitor_rule_states_rule_id_sec_monitor_rules FOREIGN KEY(rule_id) REFERENCES sec_monitor_rules (id)
)

""",
    r"""CREATE TABLE sec_discovery_checks (
	id UUID NOT NULL,
	rule_id UUID NOT NULL,
	scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
	checked_at TIMESTAMP WITH TIME ZONE NOT NULL,
	capture_id UUID,
	result VARCHAR(16) NOT NULL,
	coverage VARCHAR(16) NOT NULL,
	observation JSONB NOT NULL,
	reason VARCHAR(100),
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_discovery_checks PRIMARY KEY (id),
	CONSTRAINT uq_sec_discovery_checks_rule_id UNIQUE (rule_id, scheduled_at),
	CONSTRAINT ck_sec_discovery_checks_result CHECK (result IN ('true','false','unknown')),
	CONSTRAINT ck_sec_discovery_checks_coverage CHECK (coverage IN ('complete','partial','unknown')),
	CONSTRAINT ck_sec_discovery_checks_time_order CHECK (scheduled_at<=checked_at),
	CONSTRAINT ck_sec_discovery_checks_reason CHECK ((result='unknown' AND reason IS NOT NULL) OR (result<>'unknown' AND reason IS NULL)),
	CONSTRAINT ck_sec_discovery_checks_negative_coverage CHECK (result<>'false' OR coverage='complete'),
	CONSTRAINT fk_sec_discovery_checks_rule_id_sec_monitor_rules FOREIGN KEY(rule_id) REFERENCES sec_monitor_rules (id),
	CONSTRAINT fk_sec_discovery_checks_capture_id_data_snapshots FOREIGN KEY(capture_id) REFERENCES data_snapshots (id)
)

""",
    r"""CREATE TABLE sec_discovery_events (
	id UUID NOT NULL,
	account_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	event_key VARCHAR(128) NOT NULL,
	rule_id UUID NOT NULL,
	capture_id UUID NOT NULL,
	entry JSONB NOT NULL,
	metadata_hash VARCHAR(64) NOT NULL,
	first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_discovery_events PRIMARY KEY (id),
	CONSTRAINT uq_sec_discovery_events_account_id UNIQUE (account_id, instrument_id, event_key),
	CONSTRAINT ck_sec_discovery_events_hash CHECK (metadata_hash ~ '^[0-9a-f]{64}$'),
	CONSTRAINT fk_sec_discovery_events_account_id_paper_accounts FOREIGN KEY(account_id) REFERENCES paper_accounts (id),
	CONSTRAINT fk_sec_discovery_events_instrument_id_instruments FOREIGN KEY(instrument_id) REFERENCES instruments (id),
	CONSTRAINT fk_sec_discovery_events_rule_id_sec_monitor_rules FOREIGN KEY(rule_id) REFERENCES sec_monitor_rules (id),
	CONSTRAINT fk_sec_discovery_events_capture_id_data_snapshots FOREIGN KEY(capture_id) REFERENCES data_snapshots (id)
)

""",
    r"""CREATE TABLE sec_discovery_event_revisions (
	id UUID NOT NULL,
	event_id UUID NOT NULL,
	capture_id UUID NOT NULL,
	entry JSONB NOT NULL,
	metadata_hash VARCHAR(64) NOT NULL,
	observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_discovery_event_revisions PRIMARY KEY (id),
	CONSTRAINT uq_sec_discovery_event_revisions_event_id UNIQUE (event_id, metadata_hash),
	CONSTRAINT ck_sec_discovery_event_revisions_hash CHECK (metadata_hash ~ '^[0-9a-f]{64}$'),
	CONSTRAINT fk_sec_discovery_event_revisions_event_id_sec_discovery_events FOREIGN KEY(event_id) REFERENCES sec_discovery_events (id),
	CONSTRAINT fk_sec_discovery_event_revisions_capture_id_data_snapshots FOREIGN KEY(capture_id) REFERENCES data_snapshots (id)
)

""",
    r"""CREATE TABLE sec_notification_claims (
	id UUID NOT NULL,
	account_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	event_key VARCHAR(128) NOT NULL,
	outbox_id UUID NOT NULL,
	legacy_trigger_id UUID,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_notification_claims PRIMARY KEY (id),
	CONSTRAINT uq_sec_notification_claims_account_id UNIQUE (account_id, instrument_id, event_key),
	CONSTRAINT uq_sec_notification_claims_legacy_trigger_id UNIQUE (legacy_trigger_id),
	CONSTRAINT fk_sec_notification_claims_account_id_paper_accounts FOREIGN KEY(account_id) REFERENCES paper_accounts (id),
	CONSTRAINT fk_sec_notification_claims_instrument_id_instruments FOREIGN KEY(instrument_id) REFERENCES instruments (id),
	CONSTRAINT fk_sec_notification_claims_outbox_id_outbox_events FOREIGN KEY(outbox_id) REFERENCES outbox_events (id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT fk_sec_notification_claims_legacy_trigger_id_research_m_7ba9 FOREIGN KEY(legacy_trigger_id) REFERENCES research_monitor_triggers (id) DEFERRABLE INITIALLY DEFERRED
)

""",
    r"""CREATE TABLE sec_auto_increment_day_claims (
	account_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	session_date DATE NOT NULL,
	calendar_authority_id UUID NOT NULL,
	increment_id UUID NOT NULL,
	run_id UUID NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_auto_increment_day_claims PRIMARY KEY (account_id, instrument_id, session_date),
	CONSTRAINT uq_sec_auto_increment_day_claims_increment_id UNIQUE (increment_id),
	CONSTRAINT uq_sec_auto_increment_day_claims_run_id UNIQUE (run_id),
	CONSTRAINT fk_sec_auto_increment_day_claims_account_id_paper_accounts FOREIGN KEY(account_id) REFERENCES paper_accounts (id),
	CONSTRAINT fk_sec_auto_increment_day_claims_instrument_id_instruments FOREIGN KEY(instrument_id) REFERENCES instruments (id),
	CONSTRAINT fk_sec_auto_increment_day_claims_calendar_authority_id__3e2d FOREIGN KEY(calendar_authority_id) REFERENCES exchange_calendar_authorities (id),
	CONSTRAINT fk_sec_auto_increment_day_claims_increment_id_research__8bbd FOREIGN KEY(increment_id) REFERENCES research_monitor_increments (id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT fk_sec_auto_increment_day_claims_run_id_agent_research_runs FOREIGN KEY(run_id) REFERENCES agent_research_runs (id)
)

""",
    r"""CREATE FUNCTION public.sec_durable_row_id(t text,v jsonb) RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT CASE WHEN t='sec_auto_increment_day_claims' THEN (v->>'account_id')||':'||(v->>'instrument_id')||':'||(v->>'session_date') ELSE v->>'id' END $$""",
    r"""CREATE UNIQUE INDEX uq_sec_durable_fact_audit ON public.audit_events((payload->>'table'),(payload->>'row_id')) WHERE action='research.monitor.durable_fact'""",
    r"""CREATE FUNCTION public.sec_durable_active(rule uuid) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
SELECT coalesce((SELECT state='active' FROM public.sec_monitor_rule_states WHERE rule_id=rule ORDER BY sequence DESC LIMIT 1),false) $$""",
    r"""CREATE FUNCTION public.sec_durable_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE v jsonb:=to_jsonb(NEW); r public.sec_monitor_rules; ev public.sec_discovery_events;
 p public.research_monitor_plans; s public.data_snapshots; prev public.sec_monitor_rule_states;
 a uuid; i uuid; n integer; entry jsonb; at_time timestamptz; accepted timestamptz;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'SEC_DURABLE_IMMUTABLE' USING ERRCODE='23514'; END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'SEC_DURABLE_INVALID' USING ERRCODE='23514'; END IF;
 BEGIN
 IF TG_TABLE_NAME='sec_monitor_rules' THEN
  a:=(v->>'account_id')::uuid;i:=(v->>'instrument_id')::uuid;
 ELSE
  IF TG_TABLE_NAME IN ('sec_monitor_rule_states','sec_discovery_checks','sec_discovery_events') THEN
   SELECT * INTO r FROM public.sec_monitor_rules WHERE id=(v->>'rule_id')::uuid;
  ELSIF TG_TABLE_NAME='sec_discovery_event_revisions' THEN
   SELECT * INTO ev FROM public.sec_discovery_events WHERE id=(v->>'event_id')::uuid;
   SELECT * INTO r FROM public.sec_monitor_rules WHERE id=ev.rule_id;
  ELSE
   SELECT * INTO r FROM public.sec_monitor_rules WHERE account_id=(v->>'account_id')::uuid AND instrument_id=(v->>'instrument_id')::uuid;
  END IF;
  IF r.id IS NULL THEN RAISE EXCEPTION 'SEC_DURABLE_INVALID' USING ERRCODE='23514'; END IF;
  a:=r.account_id;i:=r.instrument_id;
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||a::text||':'||i::text,0));
 IF TG_TABLE_NAME<>'sec_monitor_rules' THEN PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||r.id::text,0)); END IF;
 IF (v->>'created_at')::timestamptz>clock_timestamp() OR (v->>'created_at') IS NULL THEN RAISE EXCEPTION 'SEC_DURABLE_INVALID' USING ERRCODE='23514'; END IF;
 IF TG_TABLE_NAME='sec_monitor_rules' THEN
  SELECT * INTO p FROM public.research_monitor_plans WHERE id=(v->>'anchor_plan_id')::uuid AND kind='disclosure' AND account_id=a AND instrument_id=i;
  IF p.id IS NULL OR v->>'policy_version'<>'durable_sec_v1' OR v->'forms' IS DISTINCT FROM p.condition->'forms'
   OR NOT EXISTS(SELECT FROM public.agent_research_outputs o WHERE o.id=p.source_output_id AND o.run_id=p.source_run_id AND o.status='completed' AND o.content_hash=p.source_hash AND o.intent#>>'{disclosure_proposal,issuer_cik}'=v->>'issuer_cik')
   OR NOT EXISTS(SELECT FROM public.data_snapshots d JOIN public.data_snapshots baseline ON baseline.id=p.baseline_snapshot_id
     WHERE d.id=(v->>'identity_snapshot_id')::uuid AND baseline.payload#>>'{index,identity_snapshot_id}'=d.id::text
      AND baseline.payload#>>'{index,instrument_id}'=i::text AND baseline.payload#>>'{index,issuer_cik}'=v->>'issuer_cik'
      AND GREATEST(d.created_at,d.available_at,d.received_at,d.observed_at,baseline.created_at,baseline.available_at)<=(v->>'effective_at')::timestamptz)
   OR (v->>'effective_at')::timestamptz IS DISTINCT FROM (v->>'created_at')::timestamptz
   OR (v->>'effective_at')::timestamptz<transaction_timestamp()
  THEN RAISE EXCEPTION 'SEC_DURABLE_INVALID' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='sec_monitor_rule_states' THEN
  SELECT * INTO prev FROM public.sec_monitor_rule_states WHERE rule_id=r.id ORDER BY sequence DESC LIMIT 1;
  IF (v->>'effective_at')::timestamptz IS DISTINCT FROM (v->>'created_at')::timestamptz OR (v->>'effective_at')::timestamptz<transaction_timestamp()
   OR (prev.id IS NULL AND ((v->>'sequence')::int<>0 OR v->>'state'<>'active' OR (v->>'effective_at')::timestamptz<>r.effective_at))
   OR (prev.id IS NOT NULL AND (prev.state='revoked' OR (v->>'sequence')::int<>prev.sequence+1 OR (v->>'effective_at')::timestamptz<=prev.effective_at OR v->>'state'=prev.state))
   OR (v->>'effective_at')::timestamptz<r.effective_at
  THEN RAISE EXCEPTION 'SEC_DURABLE_STATE_INVALID' USING ERRCODE='23514'; END IF;
 ELSE
  IF TG_TABLE_NAME IN ('sec_discovery_checks','sec_discovery_events','sec_discovery_event_revisions','sec_notification_claims','sec_auto_increment_day_claims')
   AND NOT public.sec_durable_active(r.id) THEN RAISE EXCEPTION 'SEC_DURABLE_RULE_INACTIVE' USING ERRCODE='23514'; END IF;
  IF TG_TABLE_NAME='sec_discovery_checks' THEN
   IF (v->>'scheduled_at')::timestamptz<>date_trunc('hour',(v->>'scheduled_at')::timestamptz)+floor(extract(minute from (v->>'scheduled_at')::timestamptz)/5)*interval '5 minutes'
    OR (v->>'checked_at')::timestamptz<r.effective_at OR (v->>'checked_at')::timestamptz>clock_timestamp()
    OR (v->>'result'<>'unknown' AND v->>'capture_id' IS NULL)
    OR (v->>'result'='false' AND (v->>'coverage'<>'complete' OR NOT (v->'observation' ? 'coverage_proof')))
   THEN RAISE EXCEPTION 'SEC_DURABLE_CHECK_INVALID' USING ERRCODE='23514'; END IF;
   IF v->>'result'<>'unknown' THEN
    SELECT * INTO s FROM public.data_snapshots WHERE id=(v->>'capture_id')::uuid AND (
      source='sec_disclosure_index' OR
      (source='sec_durable_disclosure_index' AND schema_version='sec_durable_index_v1'
       AND payload->>'schema_version'='sec_durable_index_v1'));
    IF s.id IS NULL OR s.payload#>>'{index,received_at}' IS NULL
     OR s.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(s.payload),'UTF8')),'hex')
     OR s.payload#>>'{index,instrument_id}' IS DISTINCT FROM i::text OR s.payload#>>'{index,issuer_cik}' IS DISTINCT FROM r.issuer_cik
     OR GREATEST(s.created_at,s.available_at,s.received_at,s.observed_at)>(v->>'checked_at')::timestamptz
     OR (v->>'checked_at')::timestamptz-(s.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
    THEN RAISE EXCEPTION 'SEC_DURABLE_CHECK_INVALID' USING ERRCODE='23514'; END IF;
    IF v->>'result'='false' AND (
      (s.payload#>>'{index,history_complete}') IS DISTINCT FROM 'true'
      OR (s.source='sec_durable_disclosure_index' AND s.payload->'issues' IS DISTINCT FROM '[]'::jsonb)
      OR (s.payload#>>'{index,coverage_start_at}') IS NULL OR (s.payload#>>'{index,coverage_start_at}')::timestamptz>r.effective_at
      OR v#>>'{observation,coverage_proof,capture_id}' IS DISTINCT FROM s.id::text
      OR v#>>'{observation,coverage_proof,capture_hash}' IS DISTINCT FROM s.revision
      OR (v#>>'{observation,coverage_proof,from}')::timestamptz IS DISTINCT FROM r.effective_at
      OR (v#>>'{observation,coverage_proof,to}')::timestamptz IS DISTINCT FROM (s.payload#>>'{index,received_at}')::timestamptz)
    THEN RAISE EXCEPTION 'SEC_DURABLE_CHECK_INVALID' USING ERRCODE='23514'; END IF;
   END IF;
  ELSIF TG_TABLE_NAME IN ('sec_discovery_events','sec_discovery_event_revisions') THEN
   entry:=v->'entry';at_time:=coalesce((v->>'first_seen_at')::timestamptz,(v->>'observed_at')::timestamptz);
   SELECT * INTO s FROM public.data_snapshots WHERE id=(v->>'capture_id')::uuid AND (
      source='sec_disclosure_index' OR
      (source='sec_durable_disclosure_index' AND schema_version='sec_durable_index_v1'
       AND payload->>'schema_version'='sec_durable_index_v1'));
   IF TG_TABLE_NAME='sec_discovery_events' THEN
    IF (v->>'account_id',v->>'instrument_id') IS DISTINCT FROM (a::text,i::text) OR v->>'event_key' IS DISTINCT FROM 'sec:'||r.issuer_cik||':'||(entry->>'accession_number') THEN RAISE EXCEPTION 'SEC_DURABLE_EVENT_INVALID' USING ERRCODE='23514'; END IF;
   ELSE
    IF entry->>'accession_number' IS DISTINCT FROM ev.entry->>'accession_number' OR v->>'metadata_hash'=ev.metadata_hash THEN RAISE EXCEPTION 'SEC_DURABLE_EVENT_INVALID' USING ERRCODE='23514'; END IF;
   END IF;
   accepted:=(entry->>'accepted_at')::timestamptz;
   IF s.id IS NULL OR s.payload#>>'{index,received_at}' IS NULL
     OR s.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(s.payload),'UTF8')),'hex')
     OR s.payload#>>'{index,instrument_id}' IS DISTINCT FROM i::text OR s.payload#>>'{index,issuer_cik}' IS DISTINCT FROM r.issuer_cik
    OR NOT EXISTS(SELECT FROM jsonb_array_elements(
       CASE WHEN TG_TABLE_NAME='sec_discovery_event_revisions'
                 AND s.source='sec_durable_disclosure_index'
            THEN s.payload->'metadata_entries' ELSE s.payload#>'{index,entries}' END
       ) x WHERE x=entry)
    OR accepted IS NULL OR accepted>at_time
    OR (TG_TABLE_NAME='sec_discovery_events' AND
        (NOT (r.forms ? (entry->>'form')) OR accepted<=r.effective_at))
    OR at_time<r.effective_at OR at_time>clock_timestamp() OR GREATEST(s.created_at,s.available_at,s.received_at,s.observed_at)>at_time
    OR at_time-(s.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
    OR v->>'metadata_hash' IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(entry),'UTF8')),'hex')
   THEN RAISE EXCEPTION 'SEC_DURABLE_EVENT_INVALID' USING ERRCODE='23514'; END IF;
  ELSIF TG_TABLE_NAME='sec_notification_claims' THEN
   PERFORM pg_advisory_xact_lock(hashtextextended('sec-event:'||a::text||':'||i::text||':'||(v->>'event_key'),0));
   IF EXISTS(SELECT FROM public.sec_notification_claims c WHERE c.account_id=a AND c.instrument_id=i AND c.event_key=v->>'event_key') THEN RAISE EXCEPTION 'SEC_NOTIFICATION_IDENTITY_OCCUPIED' USING ERRCODE='23514'; END IF;
   -- Existing legacy rows may be imported exactly; an attempted new trigger is
   -- not yet present and therefore cannot masquerade as this historical branch.
   IF NOT EXISTS(SELECT FROM public.research_monitor_triggers t WHERE t.id=(v->>'legacy_trigger_id')::uuid AND t.account_id=a AND t.instrument_id=i AND t.kind='disclosure' AND t.event_key=v->>'event_key' AND t.outbox_id=(v->>'outbox_id')::uuid AND t.created_at<=r.effective_at) THEN
    IF EXISTS(SELECT FROM public.research_monitor_triggers t WHERE t.account_id=a AND t.instrument_id=i AND t.kind='disclosure' AND t.event_key=v->>'event_key')
     OR NOT EXISTS(SELECT FROM public.sec_discovery_events e WHERE e.account_id=a AND e.instrument_id=i AND e.event_key=v->>'event_key' AND NOT EXISTS(SELECT FROM public.sec_discovery_event_revisions rev WHERE rev.event_id=e.id))
    THEN RAISE EXCEPTION 'SEC_NOTIFICATION_EVENT_UNPROVEN' USING ERRCODE='23514'; END IF;
   END IF;
  ELSIF TG_TABLE_NAME='sec_auto_increment_day_claims' THEN
   PERFORM pg_advisory_xact_lock(hashtextextended('sec-session:'||a::text||':'||i::text||':'||(v->>'session_date'),0));
   IF EXISTS(SELECT FROM public.research_monitor_increments x WHERE x.account_id=a AND x.instrument_id=i AND (x.session_date=(v->>'session_date')::date OR (x.research_as_of AT TIME ZONE 'America/New_York')::date=(v->>'session_date')::date))
    OR EXISTS(SELECT FROM public.sec_auto_increment_day_claims x WHERE x.account_id=a AND x.instrument_id=i AND x.session_date=(v->>'session_date')::date)
   THEN RAISE EXCEPTION 'SEC_AUTO_INCREMENT_DAY_USED' USING ERRCODE='23514'; END IF;
  END IF;
 END IF;
 EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format OR datetime_field_overflow OR data_exception THEN RAISE EXCEPTION 'SEC_DURABLE_INVALID' USING ERRCODE='23514'; END;
 RETURN NEW;
END $$""",
    r"""CREATE FUNCTION public.sec_durable_increment_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE r public.sec_monitor_rules; c public.sec_auto_increment_day_claims;
BEGIN
 SELECT * INTO r FROM public.sec_monitor_rules WHERE account_id=NEW.account_id AND instrument_id=NEW.instrument_id;
 IF r.id IS NULL THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||NEW.account_id::text||':'||NEW.instrument_id::text,0));
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||r.id::text,0));
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-session:'||NEW.account_id::text||':'||NEW.instrument_id::text||':'||NEW.session_date::text,0));
 IF NOT public.sec_durable_active(r.id) THEN RAISE EXCEPTION 'SEC_DURABLE_RULE_INACTIVE' USING ERRCODE='23514'; END IF;
 IF EXISTS(SELECT FROM public.research_monitor_increments i WHERE i.account_id=NEW.account_id AND i.instrument_id=NEW.instrument_id AND i.session_date=NEW.session_date)
 THEN RAISE EXCEPTION 'SEC_AUTO_INCREMENT_DAY_USED' USING ERRCODE='23514'; END IF;
 SELECT * INTO c FROM public.sec_auto_increment_day_claims WHERE account_id=NEW.account_id AND instrument_id=NEW.instrument_id AND session_date=NEW.session_date;
 IF c.increment_id IS DISTINCT FROM NEW.id OR c.run_id IS DISTINCT FROM NEW.run_id THEN RAISE EXCEPTION 'SEC_AUTO_INCREMENT_CLAIM_REQUIRED' USING ERRCODE='23514'; END IF;
 IF NOT EXISTS(SELECT FROM public.data_snapshots s JOIN public.exchange_calendar_authorities a ON a.id=c.calendar_authority_id AND a.completeness_status='complete'
  JOIN public.exchange_calendar_sessions cs ON cs.calendar_authority_id=a.id AND cs.session=NEW.session_date
  WHERE s.id=NEW.snapshot_id AND s.payload#>>'{monitor_context,authority,calendar_authority_id}'=a.id::text
   AND s.payload#>>'{monitor_context,authority,decision_session}'=cs.session::text
   AND (cs.market_open_at AT TIME ZONE 'America/New_York')::date=cs.session
   AND cs.market_open_at<=NEW.research_as_of AND NEW.research_as_of<cs.market_close_at
   AND GREATEST(a.created_at,a.published_at,cs.created_at)<=NEW.research_as_of)
 THEN RAISE EXCEPTION 'SEC_AUTO_INCREMENT_SESSION_INVALID' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_durable_increment BEFORE INSERT ON public.research_monitor_increments FOR EACH ROW EXECUTE FUNCTION public.sec_durable_increment_guard()""",
    r"""CREATE FUNCTION public.sec_durable_legacy_notice_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE r public.sec_monitor_rules;
BEGIN
 IF NEW.kind<>'disclosure' THEN RETURN NEW; END IF;
 SELECT * INTO r FROM public.sec_monitor_rules WHERE account_id=NEW.account_id AND instrument_id=NEW.instrument_id;
 IF r.id IS NULL THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||NEW.account_id::text||':'||NEW.instrument_id::text,0));
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||r.id::text,0));
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-event:'||NEW.account_id::text||':'||NEW.instrument_id::text||':'||NEW.event_key,0));
 IF NOT public.sec_durable_active(r.id) THEN RAISE EXCEPTION 'SEC_DURABLE_RULE_INACTIVE' USING ERRCODE='23514'; END IF;
 IF NOT EXISTS(SELECT FROM public.sec_notification_claims c WHERE c.account_id=NEW.account_id AND c.instrument_id=NEW.instrument_id AND c.event_key=NEW.event_key AND c.legacy_trigger_id=NEW.id AND c.outbox_id=NEW.outbox_id)
 THEN RAISE EXCEPTION 'SEC_NOTIFICATION_CLAIM_REQUIRED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_durable_legacy_notice BEFORE INSERT ON public.research_monitor_triggers FOR EACH ROW EXECUTE FUNCTION public.sec_durable_legacy_notice_guard()""",
    r"""CREATE FUNCTION public.sec_durable_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE v jsonb; t text; rid text; n integer; a public.audit_events; r public.sec_monitor_rules; c public.sec_auto_increment_day_claims;
BEGIN
 IF TG_TABLE_NAME='audit_events' THEN
  IF NEW.action<>'research.monitor.durable_fact' THEN RETURN NEW; END IF;
  t:=NEW.payload->>'table';rid:=NEW.payload->>'row_id';
  IF t NOT IN ('sec_monitor_rules','sec_monitor_rule_states','sec_discovery_checks','sec_discovery_events','sec_discovery_event_revisions','sec_notification_claims','sec_auto_increment_day_claims') THEN RAISE EXCEPTION 'SEC_DURABLE_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
  EXECUTE format('SELECT to_jsonb(x) FROM public.%I x WHERE public.sec_durable_row_id(%L,to_jsonb(x))=$1',t,t) INTO v USING rid;
 ELSE t:=TG_TABLE_NAME;v:=to_jsonb(NEW);rid:=public.sec_durable_row_id(t,v); END IF;
 SELECT count(*) INTO n FROM public.audit_events e WHERE e.action='research.monitor.durable_fact' AND e.actor_id='research-monitor' AND e.schema_version='durable_sec_v1'
  AND e.occurred_at=(v->>'created_at')::timestamptz AND e.payload=jsonb_build_object('table',t,'row_id',rid,'binding',v-'created_at');
 IF v IS NULL OR n<>1 THEN RAISE EXCEPTION 'SEC_DURABLE_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 IF t='sec_monitor_rules' AND NOT EXISTS(SELECT FROM public.sec_monitor_rule_states s WHERE s.rule_id=(v->>'id')::uuid AND s.sequence=0 AND s.state='active' AND s.effective_at=(v->>'effective_at')::timestamptz)
 THEN RAISE EXCEPTION 'SEC_DURABLE_STATE_INVALID' USING ERRCODE='23514'; END IF;
 IF t='sec_auto_increment_day_claims' THEN
  IF NOT EXISTS(SELECT FROM public.research_monitor_increments i JOIN public.agent_research_runs x ON x.id=i.run_id AND x.instrument_id=i.instrument_id AND x.data_snapshot_id=i.snapshot_id
   JOIN public.jobs j ON j.id=i.job_id AND j.payload->>'run_id'=i.run_id::text
   JOIN public.agent_daily_budget_reservations b ON b.run_id=i.run_id AND b.reserved_usd=x.reserved_cost_usd
   WHERE i.id=(v->>'increment_id')::uuid AND i.run_id=(v->>'run_id')::uuid AND i.account_id=(v->>'account_id')::uuid AND i.instrument_id=(v->>'instrument_id')::uuid AND i.session_date=(v->>'session_date')::date)
  THEN RAISE EXCEPTION 'SEC_AUTO_INCREMENT_CLAIM_INVALID' USING ERRCODE='23514'; END IF;
 END IF;
 IF t='sec_notification_claims' THEN
  IF v->>'legacy_trigger_id' IS NOT NULL THEN
   IF NOT EXISTS(SELECT FROM public.research_monitor_triggers x JOIN public.outbox_events o ON o.id=x.outbox_id
     WHERE x.id=(v->>'legacy_trigger_id')::uuid AND x.account_id=(v->>'account_id')::uuid AND x.instrument_id=(v->>'instrument_id')::uuid AND x.kind='disclosure' AND x.event_key=v->>'event_key' AND x.outbox_id=(v->>'outbox_id')::uuid
      AND ((o.payload->'event_ids' ? (v->>'event_key')) OR o.payload->>'event_key'=v->>'event_key'))
    THEN RAISE EXCEPTION 'SEC_NOTIFICATION_CLAIM_INVALID' USING ERRCODE='23514'; END IF;
  ELSE
   IF NOT EXISTS(SELECT FROM public.sec_discovery_events e JOIN public.outbox_events o ON o.id=(v->>'outbox_id')::uuid
    WHERE e.account_id=(v->>'account_id')::uuid AND e.instrument_id=(v->>'instrument_id')::uuid AND e.event_key=v->>'event_key'
     AND o.event_type='research.sec.discovered' AND o.schema_version='durable_sec_notice_v1' AND o.aggregate_id=e.id
     AND o.payload->>'discovery_id'=e.id::text AND o.payload->>'instrument_id'=e.instrument_id::text AND o.payload->>'rule_id'=e.rule_id::text AND o.payload->>'event_key'=e.event_key AND o.payload->>'form'=e.entry->>'form'
     AND (o.payload->>'accepted_at')::timestamptz=(e.entry->>'accepted_at')::timestamptz
     AND (o.payload->>'discovered_at')::timestamptz=e.first_seen_at)
    THEN RAISE EXCEPTION 'SEC_NOTIFICATION_CLAIM_INVALID' USING ERRCODE='23514'; END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_monitor_rules FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_monitor_rules DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_monitor_rule_states FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_monitor_rule_states DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_discovery_checks FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_discovery_checks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_discovery_events FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_discovery_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_discovery_event_revisions FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_discovery_event_revisions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_notification_claims FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_notification_claims DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE TRIGGER sec_durable_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_auto_increment_day_claims FOR EACH ROW EXECUTE FUNCTION public.sec_durable_guard()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.sec_auto_increment_day_claims DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_durable_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_durable_complete()""",
)

DOWNGRADE_SQL = (
    r"""LOCK TABLE public.sec_monitor_rules,public.sec_monitor_rule_states,public.sec_discovery_checks,public.sec_discovery_events,public.sec_discovery_event_revisions,public.sec_notification_claims,public.sec_auto_increment_day_claims,public.audit_events,public.research_monitor_triggers,public.research_monitor_increments IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    r"""DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_monitor_rules) OR EXISTS(SELECT FROM public.sec_monitor_rule_states) OR EXISTS(SELECT FROM public.sec_discovery_checks) OR EXISTS(SELECT FROM public.sec_discovery_events) OR EXISTS(SELECT FROM public.sec_discovery_event_revisions) OR EXISTS(SELECT FROM public.sec_notification_claims) OR EXISTS(SELECT FROM public.sec_auto_increment_day_claims) OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.durable_fact') THEN RAISE EXCEPTION 'SEC_DURABLE_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF; END $$""",
    r"""DROP TRIGGER sec_durable_complete ON public.audit_events""",
    r"""DROP TRIGGER sec_durable_legacy_notice ON public.research_monitor_triggers""",
    r"""DROP TRIGGER sec_durable_increment ON public.research_monitor_increments""",
    r"""DROP TABLE public.sec_auto_increment_day_claims""",
    r"""DROP TABLE public.sec_notification_claims""",
    r"""DROP TABLE public.sec_discovery_event_revisions""",
    r"""DROP TABLE public.sec_discovery_events""",
    r"""DROP TABLE public.sec_discovery_checks""",
    r"""DROP TABLE public.sec_monitor_rule_states""",
    r"""DROP TABLE public.sec_monitor_rules""",
    r"""DROP FUNCTION public.sec_durable_complete()""",
    r"""DROP FUNCTION public.sec_durable_legacy_notice_guard()""",
    r"""DROP FUNCTION public.sec_durable_increment_guard()""",
    r"""DROP FUNCTION public.sec_durable_guard()""",
    r"""DROP FUNCTION public.sec_durable_active(uuid)""",
    r"""DROP FUNCTION public.sec_durable_row_id(text,jsonb)""",
    r"""DROP INDEX public.uq_sec_durable_fact_audit""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
