"""Independent foundation SEC daily context and immutable addressed proofs."""

from alembic import op

revision = "0132_sec_daily_pending"
down_revision = "0131_sec_research_refreeze"
branch_labels = None
depends_on = None
_REFS = "\nCREATE FUNCTION public.sec_daily_refs_valid(refs jsonb, snapshot_id uuid, instrument_id uuid)\nRETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$\nDECLARE paper_hash text;\nBEGIN\n IF jsonb_typeof(refs) IS DISTINCT FROM 'array' THEN RETURN false; END IF;\n IF jsonb_array_length(refs) NOT BETWEEN 1 AND 4 THEN RETURN false; END IF;\n SELECT encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(payload->'agent_paper_context'),'UTF8')),'hex')\n INTO paper_hash FROM public.data_snapshots WHERE id=$2;\n RETURN EXISTS(SELECT 1 FROM jsonb_array_elements(refs) x WHERE x->>'source'='nexus_agent_paper_context'\n   AND x->>'content_hash'=paper_hash)\n  AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(refs) x WHERE NOT EXISTS(\n   SELECT 1 FROM public.agent_data_snapshot_evidence e WHERE e.data_snapshot_id=$2 AND e.instrument_id=$3\n    AND e.source=x->>'source' AND e.provider_object_id=x->>'object_id'\n    AND e.available_at=(x->>'available_at')::timestamptz AND e.location=x->>'location'\n    AND e.content_hash=x->>'content_hash') AND NOT EXISTS(\n   SELECT 1 FROM public.research_evidence_refs e JOIN public.data_snapshots ds ON ds.id=$2\n    WHERE x->>'source'='nexus_agent_paper_context' AND e.source=x->>'source'\n     AND e.provider_object_id=x->>'object_id'\n     AND e.provider_object_id=(ds.payload->'agent_paper_context'->>'cycle_id')||':'||$3::text\n     AND e.available_at=ds.observed_at AND e.available_at=(x->>'available_at')::timestamptz\n     AND e.revision=paper_hash AND e.content_hash=paper_hash AND x->>'content_hash'=paper_hash\n     AND e.location=x->>'location' AND e.location='memory://sha256/'||paper_hash));\nEND $$;\n"

UPGRADE_SQL = (
    _REFS,
    """CREATE TABLE public.sec_durable_daily_event_receipts(
 id uuid PRIMARY KEY,event_id uuid NOT NULL UNIQUE REFERENCES public.sec_discovery_events(id),
 cycle_id uuid NOT NULL REFERENCES public.agent_paper_cycles(id),
 run_id uuid NOT NULL REFERENCES public.agent_research_runs(id),
 output_id uuid NOT NULL REFERENCES public.agent_research_outputs(id),
 snapshot_id uuid NOT NULL REFERENCES public.data_snapshots(id),
 context_hash varchar(64) NOT NULL CHECK(context_hash ~ '^[0-9a-f]{64}$'),
 proposal_hash varchar(64) NOT NULL CHECK(proposal_hash ~ '^[0-9a-f]{64}$'),
 review_hash varchar(64) NOT NULL CHECK(review_hash ~ '^[0-9a-f]{64}$'),
 observed_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT now())""",
    """CREATE UNIQUE INDEX uq_sec_daily_audit ON public.audit_events((payload->>'row_id')) WHERE action='research.monitor.sec_daily_consumed' """,
    """CREATE FUNCTION public.sec_daily_exclusive() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE e public.sec_discovery_events; eid uuid;
BEGIN
 eid:=NEW.event_id;
 SELECT * INTO e FROM public.sec_discovery_events WHERE id=eid;
 IF e.id IS NULL THEN RAISE EXCEPTION 'SEC_DAILY_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||e.account_id::text||':'||e.instrument_id::text,0));
 PERFORM pg_advisory_xact_lock(hashtextextended('sec-durable-rule:'||e.rule_id::text,0));
 IF TG_TABLE_NAME='sec_durable_daily_event_receipts' THEN
  IF NOT public.sec_durable_active(e.rule_id) OR EXISTS(SELECT FROM public.sec_discovery_event_revisions WHERE event_id=e.id)
   OR EXISTS(SELECT FROM public.sec_event_research_bindings WHERE event_id=e.id)
   OR EXISTS(SELECT FROM public.sec_event_consumption_receipts WHERE event_id=e.id)
  THEN RAISE EXCEPTION 'SEC_DAILY_NOT_ELIGIBLE' USING ERRCODE='23514'; END IF;
 ELSE
  IF EXISTS(SELECT FROM public.sec_durable_daily_event_receipts WHERE event_id=e.id)
  THEN RAISE EXCEPTION 'SEC_DAILY_ALREADY_CONSUMED' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$""",
    """CREATE TRIGGER sec_daily_exclusive BEFORE INSERT ON public.sec_durable_daily_event_receipts FOR EACH ROW EXECUTE FUNCTION public.sec_daily_exclusive()""",
    """CREATE TRIGGER sec_daily_exclusive BEFORE INSERT ON public.sec_event_research_bindings FOR EACH ROW EXECUTE FUNCTION public.sec_daily_exclusive()""",
    """CREATE TRIGGER sec_daily_exclusive BEFORE INSERT ON public.sec_event_consumption_receipts FOR EACH ROW EXECUTE FUNCTION public.sec_daily_exclusive()""",
    """CREATE TRIGGER sec_daily_immutable BEFORE UPDATE OR DELETE ON public.sec_durable_daily_event_receipts FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()""",
    """CREATE FUNCTION public.sec_daily_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE p public.sec_durable_daily_event_receipts; e public.sec_discovery_events; r public.agent_research_runs; o public.agent_research_outputs; s public.data_snapshots; c jsonb; proposal jsonb; frozen jsonb; item jsonb; roles jsonb; reviewer text; review jsonb; n int;
BEGIN
 IF TG_TABLE_NAME='audit_events' THEN
  IF NEW.action<>'research.monitor.sec_daily_consumed' THEN RETURN NEW; END IF;
  SELECT * INTO p FROM public.sec_durable_daily_event_receipts WHERE id::text=NEW.payload->>'row_id';
 ELSE p:=NEW; END IF;
 IF p.id IS NULL OR (SELECT count(*) FROM public.audit_events a WHERE a.action='research.monitor.sec_daily_consumed' AND a.actor_id='research-monitor' AND a.schema_version='durable_sec_pending_v1' AND a.occurred_at=p.created_at AND a.payload=jsonb_build_object('row_id',p.id::text,'binding',to_jsonb(p)-'created_at'))<>1
 THEN RAISE EXCEPTION 'SEC_DAILY_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 SELECT * INTO e FROM public.sec_discovery_events WHERE id=p.event_id;
 SELECT * INTO r FROM public.agent_research_runs WHERE id=p.run_id;
 SELECT * INTO o FROM public.agent_research_outputs WHERE id=p.output_id;
 SELECT * INTO s FROM public.data_snapshots WHERE id=p.snapshot_id;
 c:=s.payload#>'{agent_paper_context,pending_durable_sec}';proposal:=o.intent->'durable_sec_pending_proposal';
 IF c IS NULL OR c->>'purpose' IS DISTINCT FROM 'durable_sec_pending_v1'
 OR c->>'account_id' IS DISTINCT FROM e.account_id::text OR c->>'instrument_id' IS DISTINCT FROM e.instrument_id::text
 OR c->>'cycle_id' IS DISTINCT FROM p.cycle_id::text OR (c->>'decision_time')::timestamptz IS DISTINCT FROM r.decision_time
 OR r.data_snapshot_id IS DISTINCT FROM s.id OR r.instrument_id IS DISTINCT FROM e.instrument_id
 OR o.run_id IS DISTINCT FROM r.id OR o.role IS NOT NULL OR o.status<>'completed'
 OR r.status<>'completed' OR r.retry_count<>0 OR r.input_tokens IS NULL OR r.output_tokens IS NULL OR r.settled_cost_usd IS NULL
 OR p.observed_at>clock_timestamp() OR p.observed_at<>p.created_at OR p.observed_at<greatest(r.created_at,o.created_at,r.decision_time)
 OR NOT EXISTS(SELECT FROM public.agent_paper_cycles cy WHERE cy.id=p.cycle_id AND cy.account_id=e.account_id)
 OR NOT EXISTS(SELECT FROM public.agent_daily_budget_reservations b WHERE b.run_id=r.id AND b.state='settled' AND b.settled_usd=r.settled_cost_usd AND b.reserved_usd=r.reserved_cost_usd AND b.pricing_version=r.pricing_version)
 OR NOT EXISTS(SELECT FROM public.jobs j JOIN public.job_leases l ON l.job_id=j.id JOIN public.job_attempts a ON a.job_id=j.id
 WHERE j.payload=jsonb_build_object('run_id',r.id::text) AND j.status='succeeded' AND j.job_type='agent.paper.research'
 AND j.attempt_count=1 AND j.max_attempts=1 AND j.manual_retry_count=0 AND j.finished_at<=p.observed_at
 AND a.outcome='succeeded' AND a.attempt_no=1 AND a.worker_id=l.worker_id AND a.lease_token=l.lease_token AND a.fence_token=l.fence_token
 AND a.started_at=l.acquired_at AND a.finished_at=j.finished_at AND j.finished_at<l.expires_at)
 OR NOT EXISTS(SELECT FROM public.research_stage_terminal st WHERE st.run_id=r.id AND st.known AND st.closed_at<=p.observed_at)
 OR p.context_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(c),'UTF8')),'hex')
 OR proposal->>'context_hash' IS DISTINCT FROM p.context_hash
 OR p.proposal_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(proposal),'UTF8')),'hex')
 OR jsonb_typeof(c->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(c->'events') NOT BETWEEN 1 AND 32
 OR jsonb_typeof(proposal->'items') IS DISTINCT FROM 'array' OR jsonb_array_length(proposal->'items')<>jsonb_array_length(c->'events')
 OR (SELECT count(DISTINCT x->>'event_id') FROM jsonb_array_elements(proposal->'items') x)<>jsonb_array_length(c->'events')
 OR EXISTS(SELECT FROM jsonb_array_elements(proposal->'items') pi WHERE pi->>'status' IS NULL OR pi->>'status' NOT IN ('addressed','unresolved') OR jsonb_array_length(pi->'evidence_refs') NOT BETWEEN 1 AND 3 OR NOT public.sec_daily_refs_valid(pi->'evidence_refs',s.id,e.instrument_id) OR NOT EXISTS(SELECT FROM jsonb_array_elements(c->'events') f WHERE f->>'event_id'=pi->>'event_id'))
 THEN RAISE EXCEPTION 'SEC_DAILY_PROOF_INVALID' USING ERRCODE='23514'; END IF;
 SELECT x INTO frozen FROM jsonb_array_elements(c->'events') x WHERE x->>'event_id'=e.id::text;
 SELECT x INTO item FROM jsonb_array_elements(proposal->'items') x WHERE x->>'event_id'=e.id::text;
 IF item->>'status' IS DISTINCT FROM 'addressed' OR frozen->>'rule_id' IS DISTINCT FROM e.rule_id::text OR frozen->>'event_key' IS DISTINCT FROM e.event_key OR frozen->>'metadata_hash' IS DISTINCT FROM e.metadata_hash OR frozen->'entry' IS DISTINCT FROM e.entry
 OR (frozen->>'first_seen_at')::timestamptz IS DISTINCT FROM e.first_seen_at
 OR NOT EXISTS(SELECT FROM public.data_snapshots cap WHERE cap.id::text=frozen->>'capture_id' AND cap.source='sec_durable_disclosure_index' AND cap.schema_version='sec_durable_index_v1' AND cap.payload->>'schema_version'='sec_durable_index_v1' AND cap.revision=frozen->>'capture_hash' AND cap.revision=encode(sha256(convert_to(public.sec_positive_chain_canonical(cap.payload),'UTF8')),'hex') AND cap.payload#>>'{index,instrument_id}'=e.instrument_id::text AND cap.payload#>>'{index,issuer_cik}'=(SELECT issuer_cik FROM public.sec_monitor_rules WHERE id=e.rule_id) AND NOT EXISTS(SELECT FROM jsonb_array_elements(cap.payload->'issues') iss WHERE iss->>'accession_number'=e.entry->>'accession_number') AND EXISTS(SELECT FROM jsonb_array_elements(cap.payload#>'{index,entries}') x WHERE x=e.entry) AND greatest(cap.created_at,cap.observed_at,cap.received_at,cap.available_at)<=r.decision_time AND r.decision_time-(cap.payload#>>'{index,received_at}')::timestamptz BETWEEN interval '0 seconds' AND interval '300 seconds')
 OR NOT EXISTS(SELECT FROM public.data_snapshots fc WHERE fc.id::text=frozen#>>'{content,snapshot_id}' AND fc.source='nexus_sec_monitor_filing' AND fc.revision=frozen#>>'{content,canonical_hash}' AND fc.payload=frozen#>'{content,archive}' AND fc.payload->'entry'=e.entry AND greatest(fc.created_at,fc.observed_at,fc.received_at,fc.available_at)<=r.decision_time)
 THEN RAISE EXCEPTION 'SEC_DAILY_EVENT_INVALID' USING ERRCODE='23514'; END IF;
 SELECT jsonb_object_agg(role,payload) INTO roles FROM public.agent_research_outputs WHERE run_id=r.id AND role IS NOT NULL;
 IF (SELECT count(*) FROM public.agent_research_outputs WHERE run_id=r.id AND role IS NOT NULL)<>8
 OR roles#>'{orchestrator,durable_sec_pending_proposal}' IS DISTINCT FROM proposal
 OR roles#>>'{portfolio,durable_sec_pending_adoption_hash}' IS DISTINCT FROM p.proposal_hash
 OR roles#>>'{evidence_audit,vote}' IS DISTINCT FROM 'support'
 OR p.review_hash IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(jsonb_build_object('bear',roles#>'{bear,durable_sec_pending_review}','evidence_audit',roles#>'{evidence_audit,durable_sec_pending_review}')),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'SEC_DAILY_REVIEW_INVALID' USING ERRCODE='23514'; END IF;
 FOREACH reviewer IN ARRAY ARRAY['bear','evidence_audit'] LOOP
  review:=roles->reviewer->'durable_sec_pending_review';
  IF review->>'decision' IS DISTINCT FROM 'accepted' OR review->>'proposal_hash' IS DISTINCT FROM p.proposal_hash OR NOT public.sec_daily_refs_valid(review->'evidence_refs',s.id,e.instrument_id)
  THEN RAISE EXCEPTION 'SEC_DAILY_REVIEW_INVALID' USING ERRCODE='23514'; END IF;
 END LOOP;
 RETURN NEW;
END $$""",
    """CREATE CONSTRAINT TRIGGER sec_daily_complete AFTER INSERT ON public.sec_durable_daily_event_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_daily_complete()""",
    """CREATE CONSTRAINT TRIGGER sec_daily_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_daily_complete()""",
)


def upgrade():
    for sql in UPGRADE_SQL:
        op.execute(sql)


def downgrade():
    op.execute(
        "LOCK TABLE public.sec_durable_daily_event_receipts,public.data_snapshots IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(
        """DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_durable_daily_event_receipts) OR EXISTS(SELECT FROM public.data_snapshots WHERE payload->'agent_paper_context' ?| ARRAY['pending_durable_sec','pending_durable_sec_gap']) THEN RAISE EXCEPTION 'SEC_DAILY_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF; END $$"""
    )
    op.execute("DROP TRIGGER sec_daily_complete ON public.audit_events")
    op.execute("DROP TRIGGER sec_daily_exclusive ON public.sec_event_research_bindings")
    op.execute("DROP TRIGGER sec_daily_exclusive ON public.sec_event_consumption_receipts")
    op.execute("DROP TABLE public.sec_durable_daily_event_receipts")
    op.execute("DROP FUNCTION public.sec_daily_complete()")
    op.execute("DROP FUNCTION public.sec_daily_refs_valid(jsonb,uuid,uuid)")
    op.execute("DROP FUNCTION public.sec_daily_exclusive()")
    op.execute("DROP INDEX public.uq_sec_daily_audit")
