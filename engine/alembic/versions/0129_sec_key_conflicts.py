"""Preserve conflict evidence for a stable SEC key, including legacy-only notices."""

from alembic import op

revision = "0129_sec_key_conflicts"
down_revision = "0128_sec_durable_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE public.sec_disclosure_key_conflicts (
      id uuid PRIMARY KEY, account_id uuid NOT NULL REFERENCES public.paper_accounts(id),
      instrument_id uuid NOT NULL REFERENCES public.instruments(id), event_key varchar(128) NOT NULL,
      rule_id uuid NOT NULL REFERENCES public.sec_monitor_rules(id),
      capture_id uuid NOT NULL REFERENCES public.data_snapshots(id),
      original_event_id uuid REFERENCES public.sec_discovery_events(id),
      legacy_trigger_id uuid REFERENCES public.research_monitor_triggers(id),
      entry jsonb NOT NULL, metadata_hash varchar(64) NOT NULL,
      observed_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(account_id,instrument_id,event_key,metadata_hash),
      CHECK(num_nonnulls(original_event_id,legacy_trigger_id)=1),
      CHECK(metadata_hash ~ '^[0-9a-f]{64}$'))""")
    op.execute("""CREATE FUNCTION public.sec_key_conflict_guard() RETURNS trigger LANGUAGE plpgsql
    SET search_path=pg_catalog,public AS $$
    DECLARE r public.sec_monitor_rules;s public.data_snapshots;old_entry jsonb; a uuid;i uuid;k text;
    BEGIN
      SELECT * INTO r FROM public.sec_monitor_rules WHERE id=NEW.rule_id;
      PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||NEW.account_id||':'||NEW.instrument_id,0));
      PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||NEW.rule_id,0));
      IF NEW.original_event_id IS NOT NULL THEN
        SELECT account_id,instrument_id,event_key,entry INTO a,i,k,old_entry FROM public.sec_discovery_events WHERE id=NEW.original_event_id AND created_at<=NEW.observed_at;
      ELSE
        SELECT t.account_id,t.instrument_id,t.event_key,c.observation->'entry' INTO a,i,k,old_entry
        FROM public.research_monitor_triggers t JOIN public.research_monitor_checks c ON c.id=t.check_id
        WHERE t.id=NEW.legacy_trigger_id AND t.kind='disclosure' AND t.created_at<=NEW.observed_at AND c.created_at<=NEW.observed_at;
      END IF;
      SELECT * INTO s FROM public.data_snapshots WHERE id=NEW.capture_id;
      IF r.id IS NULL OR (r.account_id,r.instrument_id) IS DISTINCT FROM (NEW.account_id,NEW.instrument_id)
       OR (a,i,k) IS DISTINCT FROM (NEW.account_id,NEW.instrument_id,NEW.event_key) OR old_entry IS NULL
       OR NEW.event_key IS DISTINCT FROM 'sec:'||r.issuer_cik||':'||(NEW.entry->>'accession_number')
       OR old_entry->>'accession_number' IS DISTINCT FROM NEW.entry->>'accession_number'
       OR old_entry=NEW.entry OR s.id IS NULL OR s.payload#>>'{index,received_at}' IS NULL OR NEW.entry->>'accepted_at' IS NULL
       OR NOT (s.source='sec_disclosure_index' OR (s.source='sec_durable_disclosure_index' AND s.schema_version='sec_durable_index_v1' AND s.payload->>'schema_version'='sec_durable_index_v1'))
       OR s.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(s.payload),'UTF8')),'hex')
       OR s.payload#>>'{index,instrument_id}' IS DISTINCT FROM NEW.instrument_id::text
       OR s.payload#>>'{index,issuer_cik}' IS DISTINCT FROM r.issuer_cik
       OR NOT EXISTS(SELECT FROM jsonb_array_elements(CASE WHEN s.source='sec_durable_disclosure_index' THEN s.payload->'metadata_entries' ELSE s.payload#>'{index,entries}' END) x WHERE x=NEW.entry)
       OR NEW.metadata_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(NEW.entry),'UTF8')),'hex')
       OR NEW.observed_at<>NEW.created_at OR NEW.observed_at<r.effective_at OR NEW.observed_at>clock_timestamp()
       OR GREATEST(s.created_at,s.available_at,s.received_at,s.observed_at)>NEW.observed_at
       OR NEW.observed_at-(s.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
       OR (NEW.entry->>'accepted_at')::timestamptz>NEW.observed_at
      THEN RAISE EXCEPTION 'SEC_KEY_CONFLICT_INVALID' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER sec_key_conflict_guard BEFORE INSERT ON public.sec_disclosure_key_conflicts FOR EACH ROW EXECUTE FUNCTION public.sec_key_conflict_guard()"
    )
    op.execute(
        "CREATE TRIGGER sec_key_conflict_immutable BEFORE UPDATE OR DELETE ON public.sec_disclosure_key_conflicts FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_sec_key_conflict_audit ON public.audit_events((payload->>'row_id')) WHERE action='research.monitor.sec_key_conflict'"
    )
    op.execute("""CREATE FUNCTION public.sec_key_conflict_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
    DECLARE v jsonb;rid text;n int;
    BEGIN
      IF TG_TABLE_NAME='audit_events' THEN
        IF NEW.action<>'research.monitor.sec_key_conflict' THEN RETURN NEW; END IF;
        rid:=NEW.payload->>'row_id';
        SELECT to_jsonb(x) INTO v FROM public.sec_disclosure_key_conflicts x WHERE id::text=rid;
      ELSE v:=to_jsonb(NEW);rid:=NEW.id::text; END IF;
      SELECT count(*) INTO n FROM public.audit_events a WHERE a.actor_id='research-monitor'
       AND a.action='research.monitor.sec_key_conflict' AND a.schema_version='sec_key_conflict_v1'
       AND a.occurred_at=(v->>'created_at')::timestamptz
       AND a.payload=jsonb_build_object('table','sec_disclosure_key_conflicts','row_id',rid,'binding',v-'created_at');
      IF v IS NULL OR n<>1 THEN RAISE EXCEPTION 'SEC_KEY_CONFLICT_AUDIT_INVALID' USING ERRCODE='23514';END IF;
      RETURN NEW;
    END $$""")
    for table in ("sec_disclosure_key_conflicts", "audit_events"):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER sec_key_conflict_complete AFTER INSERT ON public.{table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_key_conflict_complete()"
        )


def downgrade() -> None:
    op.execute(
        """DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_disclosure_key_conflicts) OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.sec_key_conflict') THEN RAISE EXCEPTION 'SEC_KEY_CONFLICT_DOWNGRADE_FORBIDDEN';END IF;END $$"""
    )
    op.execute("DROP TRIGGER sec_key_conflict_complete ON public.audit_events")
    op.execute("DROP TABLE public.sec_disclosure_key_conflicts")
    op.execute("DROP FUNCTION public.sec_key_conflict_guard()")
    op.execute("DROP FUNCTION public.sec_key_conflict_complete()")
    op.execute("DROP INDEX public.uq_sec_key_conflict_audit")
