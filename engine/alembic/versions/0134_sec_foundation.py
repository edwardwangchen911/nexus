"""Forward-only existing-scope SEC initialization, with no synthetic research plan."""

from alembic import op

revision = "0134_sec_foundation"
down_revision = "0133_sec_send_scope"
branch_labels = None
depends_on = None

REQUIRE = r"""CREATE FUNCTION public.sec_foundation_require(v jsonb) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots; lim public.data_snapshots; cap public.data_snapshots;
 ident public.data_snapshots; latest public.data_snapshots; b public.agent_paper_candidate_batches;
 at_time timestamptz; a uuid; i uuid; x jsonb;
BEGIN
 a:=(v->>'account_id')::uuid; i:=(v->>'instrument_id')::uuid;
 at_time:=(v->>'effective_at')::timestamptz;
 SELECT * INTO s FROM public.data_snapshots WHERE source='nexus_sec_foundation_scope'
  AND provider_object_id=v->>'id';
 x:=s.payload;
 SELECT * INTO lim FROM public.data_snapshots WHERE id=(x->>'scope_id')::uuid
  AND source='nexus_auto_increment_policy' AND provider_object_id=a::text;
 SELECT * INTO b FROM public.agent_paper_candidate_batches WHERE id=(x->>'candidate_batch_id')::uuid;
 SELECT * INTO cap FROM public.data_snapshots WHERE id=(x->>'capture_id')::uuid;
 SELECT * INTO ident FROM public.data_snapshots WHERE id=(x->>'identity_snapshot_id')::uuid;
 SELECT * INTO latest FROM public.data_snapshots WHERE id=(x->>'latest_identity_snapshot_id')::uuid;
 IF s.id IS NULL OR lim.id IS NULL OR b.id IS NULL OR cap.id IS NULL OR ident.id IS NULL OR latest.id IS NULL
 OR v->>'anchor_plan_id' IS NOT NULL OR v->>'policy_version' IS DISTINCT FROM 'durable_sec_v1'
 OR v->'forms' IS DISTINCT FROM '["8-K","8-K/A"]'::jsonb
 OR v->>'identity_snapshot_id' IS DISTINCT FROM ident.id::text
 OR s.schema_version IS DISTINCT FROM 'sec_foundation_scope_v1'
 OR s.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(x),'UTF8')),'hex')
 OR x IS DISTINCT FROM jsonb_build_object(
  'schema_version','sec_foundation_scope_v1','rule_id',v->>'id','account_id',a::text,
  'instrument_id',i::text,'issuer_cik',v->>'issuer_cik','forms',v->'forms',
  'candidate_batch_id',b.id::text,'candidate_hash',b.batch_hash,
  'scope_id',lim.id::text,'scope_hash',lim.revision,
  'identity_snapshot_id',ident.id::text,'identity_hash',ident.revision,
  'latest_identity_snapshot_id',latest.id::text,'latest_identity_hash',latest.revision,
  'capture_id',cap.id::text,'capture_hash',cap.revision,'effective_at',x->>'effective_at')
 OR (x->>'effective_at')::timestamptz IS DISTINCT FROM at_time
 OR (v->>'created_at')::timestamptz IS DISTINCT FROM at_time
 OR s.created_at IS DISTINCT FROM at_time OR s.observed_at IS DISTINCT FROM at_time
 OR s.available_at IS DISTINCT FROM at_time OR s.received_at IS DISTINCT FROM at_time
 OR b.account_id IS DISTINCT FROM a OR b.source IS DISTINCT FROM 'explicit_research_scope'
 OR NOT coalesce(i=ANY(b.candidate_instrument_ids),false)
 OR b.batch_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(jsonb_build_object(
   'account_id',a::text,'candidate_instrument_ids',to_jsonb(b.candidate_instrument_ids),
   'schema','agent_paper_candidate_batch_v1','source','explicit_research_scope',
   'universe_snapshot_id',b.universe_snapshot_id::text)),'UTF8')),'hex')
 OR lim.payload->>'candidate_batch_id' IS DISTINCT FROM b.id::text
 OR lim.payload->>'candidate_hash' IS DISTINCT FROM b.batch_hash
 OR greatest(b.created_at,lim.created_at,lim.available_at,lim.received_at,lim.observed_at,
  cap.created_at,cap.available_at,cap.received_at,cap.observed_at,
  ident.created_at,ident.available_at,ident.received_at,ident.observed_at,
  latest.created_at,latest.available_at,latest.received_at,latest.observed_at)>at_time
 OR NOT ((cap.source='sec_disclosure_index' AND cap.schema_version='sec_disclosure_index_v1')
  OR (cap.source='sec_durable_disclosure_index' AND cap.schema_version='sec_durable_index_v1'))
 OR cap.payload->>'schema_version' IS DISTINCT FROM cap.schema_version
 OR cap.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(cap.payload),'UTF8')),'hex')
 OR cap.payload#>>'{index,instrument_id}' IS DISTINCT FROM i::text
 OR cap.payload#>>'{index,issuer_cik}' IS DISTINCT FROM v->>'issuer_cik'
 OR cap.payload#>>'{index,identity_snapshot_id}' IS DISTINCT FROM ident.id::text
 OR cap.payload#>>'{index,received_at}' IS NULL
 OR at_time-(cap.payload#>>'{index,received_at}')::timestamptz NOT BETWEEN interval '0 seconds' AND interval '300 seconds'
 OR ident.payload->>'instrument_id' IS DISTINCT FROM i::text
 OR latest.payload->>'instrument_id' IS DISTINCT FROM i::text
 OR ident.payload#>>'{identity,cik}' IS DISTINCT FROM v->>'issuer_cik'
 OR latest.payload#>>'{identity,cik}' IS DISTINCT FROM v->>'issuer_cik'
 OR NOT ((ident.source='current_instrument_identity' AND ident.schema_version='current_instrument_identity_v1')
  OR (ident.source='current_listing_revision' AND ident.schema_version='current_listing_revision_v1'))
 OR NOT ((latest.source='current_instrument_identity' AND latest.schema_version='current_instrument_identity_v1')
  OR (latest.source='current_listing_revision' AND latest.schema_version='current_listing_revision_v1'))
 OR ident.payload->>'checked_at' IS NULL OR latest.payload->>'checked_at' IS NULL
 OR (ident.payload->>'checked_at')::timestamptz>at_time
 OR (latest.payload->>'checked_at')::timestamptz>at_time
 OR latest.id IS DISTINCT FROM (SELECT d.id FROM public.data_snapshots d
  WHERE d.source IN ('current_instrument_identity','current_listing_revision')
  AND d.payload->>'instrument_id'=i::text
  AND greatest(d.created_at,d.available_at,d.received_at,d.observed_at)<=at_time
  ORDER BY d.observed_at DESC,d.created_at DESC,d.id DESC LIMIT 1)
 OR NOT EXISTS(SELECT FROM public.audit_events audit WHERE audit.actor_id='research-monitor'
  AND audit.action='research.monitor.foundation_scope' AND audit.schema_version='sec_foundation_scope_v1'
  AND audit.occurred_at=s.created_at AND audit.created_at<=clock_timestamp()
  AND audit.payload=jsonb_build_object('snapshot_id',s.id::text,'scope_hash',s.revision))
 THEN RAISE EXCEPTION 'SEC_FOUNDATION_SCOPE_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM public.sec_auto_increment_scoped(a,i);
END $$"""

COMPLETE = r"""CREATE FUNCTION public.sec_foundation_complete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots; r public.sec_monitor_rules;
BEGIN
 IF TG_TABLE_NAME='data_snapshots' THEN
  IF NEW.source<>'nexus_sec_foundation_scope' THEN RETURN NEW; END IF;
  s:=NEW;
 ELSE
  IF NEW.action<>'research.monitor.foundation_scope' THEN RETURN NEW; END IF;
  SELECT * INTO s FROM public.data_snapshots WHERE id=(NEW.payload->>'snapshot_id')::uuid
   AND source='nexus_sec_foundation_scope';
 END IF;
 SELECT * INTO r FROM public.sec_monitor_rules WHERE id=s.provider_object_id::uuid;
 IF s.id IS NULL OR r.id IS NULL OR r.anchor_plan_id IS NOT NULL
 OR NOT EXISTS(SELECT FROM public.sec_monitor_rule_states t WHERE t.rule_id=r.id
   AND t.sequence=0 AND t.state='active' AND t.created_at=r.created_at AND t.effective_at=r.effective_at)
 THEN RAISE EXCEPTION 'SEC_FOUNDATION_INCOMPLETE' USING ERRCODE='23514'; END IF;
 PERFORM public.sec_foundation_require(to_jsonb(r));
 RETURN NEW;
END $$"""


def upgrade() -> None:
    op.execute(
        "LOCK TABLE public.sec_monitor_rules,public.data_snapshots IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute("CREATE TABLE public.sec_foundation_guard_backup(definition text NOT NULL)")
    op.execute(
        "INSERT INTO public.sec_foundation_guard_backup SELECT pg_get_functiondef('public.sec_durable_guard()'::regprocedure)"
    )
    op.execute(REQUIRE)
    op.execute(COMPLETE)
    op.execute("""CREATE FUNCTION public.sec_foundation_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='DELETE' THEN
       IF OLD.source='nexus_sec_foundation_scope' THEN RAISE EXCEPTION 'SEC_FOUNDATION_IMMUTABLE' USING ERRCODE='23514'; END IF;
       RETURN OLD;
      END IF;
      IF TG_OP<>'INSERT' AND (OLD.source='nexus_sec_foundation_scope' OR NEW.source='nexus_sec_foundation_scope')
      THEN RAISE EXCEPTION 'SEC_FOUNDATION_IMMUTABLE' USING ERRCODE='23514'; END IF;
      IF TG_OP='INSERT' AND NEW.source='nexus_sec_foundation_scope' AND
       (current_setting('transaction_isolation')<>'read committed' OR NEW.created_at<transaction_timestamp()
        OR NEW.created_at>clock_timestamp())
      THEN RAISE EXCEPTION 'SEC_FOUNDATION_SCOPE_INVALID' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER sec_foundation_immutable BEFORE INSERT OR UPDATE OR DELETE ON public.data_snapshots FOR EACH ROW EXECUTE FUNCTION public.sec_foundation_immutable()"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_sec_foundation_origin ON public.data_snapshots(provider_object_id) WHERE source='nexus_sec_foundation_scope'"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER sec_foundation_complete AFTER INSERT ON public.data_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_foundation_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER sec_foundation_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_foundation_complete()"
    )
    op.execute("""DO $migration$ DECLARE d text; needle text:=$needle$ IF TG_TABLE_NAME='sec_monitor_rules' THEN
  SELECT * INTO p$needle$; BEGIN
     SELECT definition INTO STRICT d FROM public.sec_foundation_guard_backup;
     IF encode(sha256(convert_to(d,'UTF8')),'hex')<>'e16020efefebd8528d127a61622ce26c620b3c008d619d723231ec51446f4c20'
      OR (length(d)-length(replace(d,needle,'')))/length(needle)<>1
     THEN RAISE EXCEPTION 'SEC_FOUNDATION_PREDECESSOR_INVALID'; END IF;
     EXECUTE replace(d,needle,$patch$ IF TG_TABLE_NAME='sec_monitor_rules' THEN
  IF v->>'anchor_plan_id' IS NULL THEN
   IF (v->>'effective_at')::timestamptz<transaction_timestamp()
    THEN RAISE EXCEPTION 'SEC_FOUNDATION_SCOPE_INVALID' USING ERRCODE='23514'; END IF;
   PERFORM public.sec_foundation_require(v); RETURN NEW;
  END IF;
  SELECT * INTO p$patch$);
    END $migration$""")
    op.execute(
        "CREATE UNIQUE INDEX uq_sec_foundation_audit ON public.audit_events((payload->>'snapshot_id')) WHERE action='research.monitor.foundation_scope'"
    )
    op.execute("ALTER TABLE public.sec_monitor_rules ALTER COLUMN anchor_plan_id DROP NOT NULL")
    op.execute(
        "CREATE TRIGGER sec_foundation_backup_immutable BEFORE INSERT OR UPDATE OR DELETE ON public.sec_foundation_guard_backup FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()"
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.sec_monitor_rules,public.data_snapshots IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_monitor_rules WHERE anchor_plan_id IS NULL)
    OR EXISTS(SELECT FROM public.data_snapshots WHERE source='nexus_sec_foundation_scope')
    OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.foundation_scope')
    THEN RAISE EXCEPTION 'SEC_FOUNDATION_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF; END $$""")
    op.execute("ALTER TABLE public.sec_monitor_rules ALTER COLUMN anchor_plan_id SET NOT NULL")
    op.execute(
        "DO $$ BEGIN EXECUTE (SELECT definition FROM public.sec_foundation_guard_backup); END $$"
    )
    for table in ("data_snapshots", "audit_events"):
        op.execute(f"DROP TRIGGER sec_foundation_complete ON public.{table}")
    op.execute("DROP TRIGGER sec_foundation_immutable ON public.data_snapshots")
    op.execute("DROP INDEX public.uq_sec_foundation_origin")
    op.execute("DROP INDEX public.uq_sec_foundation_audit")
    op.execute("DROP FUNCTION public.sec_foundation_complete()")
    op.execute("DROP FUNCTION public.sec_foundation_require(jsonb)")
    op.execute("DROP FUNCTION public.sec_foundation_immutable()")
    op.execute("DROP TABLE public.sec_foundation_guard_backup")
