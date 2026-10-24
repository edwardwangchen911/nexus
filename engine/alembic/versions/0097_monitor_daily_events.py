"""Independent future daily event proofs; preserve original trigger/run ownership.

Revision ID: 0097_monitor_daily_events
Revises: 0096_monitor_event_receipts
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0097_monitor_daily_events"
down_revision = "0096_monitor_event_receipts"
branch_labels = None
depends_on = None

_RECEIPT = r"""

CREATE FUNCTION public.nexus_validate_daily_event_result(proof public.research_monitor_daily_event_receipts, require_addressed boolean)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE NEW public.research_monitor_daily_event_receipts%ROWTYPE; item jsonb; hash text;
  trigger_row public.research_monitor_triggers%ROWTYPE;
  frozen public.data_snapshots%ROWTYPE; context jsonb; roles jsonb; proposal jsonb;
  original jsonb; bear jsonb; audit jsonb; portfolio jsonb; n integer;
BEGIN
  NEW:=proof;
  SELECT * INTO trigger_row FROM public.research_monitor_triggers WHERE id=NEW.trigger_id;
  IF NOT FOUND OR trigger_row.account_id<>NEW.account_id OR trigger_row.instrument_id<>NEW.instrument_id
    OR trigger_row.kind IS DISTINCT FROM NEW.kind OR trigger_row.event_key IS DISTINCT FROM NEW.event_key
    OR trigger_row.batch_id IS DISTINCT FROM NEW.batch_id
    OR trigger_row.research_run_id IS DISTINCT FROM NEW.original_run_id
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_TRIGGER'; END IF;
  SELECT * INTO frozen FROM public.data_snapshots WHERE id=NEW.snapshot_id;
  PERFORM public.nexus_validate_daily_monitor_v2(NEW.snapshot_id);
  context := frozen.payload->'agent_paper_context'->'pending_monitoring_v2';
  SELECT x INTO item FROM jsonb_array_elements(context->'trigger_checks') x WHERE x->>'trigger_id'=NEW.trigger_id::text;
  IF item IS NULL
    OR item->>'kind' IS DISTINCT FROM NEW.kind OR item->>'event_key' IS DISTINCT FROM NEW.event_key
    OR item->>'original_run_id' IS DISTINCT FROM NEW.original_run_id::text
    OR item->>'original_output_id' IS DISTINCT FROM NEW.original_output_id::text
    OR item->>'batch_id' IS DISTINCT FROM NEW.batch_id::text
    OR item->>'materials_snapshot_id' IS DISTINCT FROM NEW.materials_snapshot_id::text
    OR encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(context),'UTF8')),'hex') IS DISTINCT FROM NEW.context_hash
    OR context IS NULL OR context->>'cycle_id' IS DISTINCT FROM NEW.cycle_id::text
    OR context->>'account_id' IS DISTINCT FROM NEW.account_id::text
    OR context->>'instrument_id' IS DISTINCT FROM NEW.instrument_id::text
    OR NEW.observed_at<frozen.observed_at
    OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(context->'trigger_checks') x
      WHERE x->>'trigger_id'=NEW.trigger_id::text)
    OR NOT EXISTS (
      SELECT 1 FROM public.agent_research_runs r
      JOIN public.agent_research_outputs o ON o.id=NEW.output_id AND o.run_id=r.id
      JOIN public.agent_daily_budget_reservations b ON b.run_id=r.id
      JOIN public.agent_model_configs model ON model.id=r.model_config_id
      JOIN public.jobs j ON j.payload=jsonb_build_object('run_id',r.id::text)
      JOIN public.job_leases l ON l.job_id=j.id
      JOIN public.job_attempts a ON a.job_id=j.id AND a.attempt_no=j.attempt_count
      WHERE r.id=NEW.run_id AND r.data_snapshot_id=NEW.snapshot_id
        AND r.instrument_id=NEW.instrument_id AND r.decision_time=frozen.observed_at
        AND r.actor_id='local-agent-paper-cycle'
        AND r.idempotency_key='agent-paper:'||NEW.cycle_id::text||':'||NEW.instrument_id::text
        AND r.status='completed' AND r.retry_count=0
        AND r.input_tokens IS NOT NULL AND r.output_tokens IS NOT NULL
        AND r.settled_cost_usd IS NOT NULL AND b.state='settled'
        AND b.settled_usd=r.settled_cost_usd AND b.reserved_usd=r.reserved_cost_usd
        AND b.pricing_version=r.pricing_version AND model.version=r.pricing_version
        AND model.config_hash=r.model_hash
        AND o.status='completed' AND o.role IS NULL AND o.intent IS NOT NULL
        AND greatest(r.created_at,o.created_at)<=NEW.observed_at
        AND j.job_type='agent.paper.research' AND j.schema_version='agent_research_job_v1'
        AND j.scope_kind='instrument' AND j.scope_id=NEW.instrument_id::text
        AND j.effective_at=r.decision_time AND j.status='succeeded'
        AND j.attempt_count=1 AND j.max_attempts=1 AND j.manual_retry_count=0
        AND a.outcome='succeeded' AND a.worker_id=l.worker_id
        AND a.lease_token=l.lease_token AND a.fence_token=l.fence_token
        AND a.started_at=l.acquired_at AND a.finished_at=j.finished_at
        AND l.acquired_at<=l.heartbeat_at AND l.heartbeat_at<=j.finished_at
        AND j.finished_at<=NEW.observed_at AND j.finished_at<l.expires_at
        AND j.last_error_code IS NULL AND a.error_code IS NULL
        AND (SELECT count(*) FROM public.jobs x WHERE x.payload=jsonb_build_object('run_id',r.id::text))=1
        AND (SELECT count(*) FROM public.job_leases x WHERE x.job_id=j.id)=1
        AND (SELECT count(*) FROM public.job_attempts x WHERE x.job_id=j.id)=1
        AND EXISTS (SELECT 1 FROM public.agent_paper_cycle_events prepared
          WHERE prepared.cycle_id=NEW.cycle_id AND prepared.result_code='CYCLE_PREPARED'
            AND prepared.fact_ids->'formal_stage_capacity'='true'::jsonb
            AND prepared.fact_ids->>'formal_model_config_id'=r.model_config_id::text
            AND prepared.fact_ids->>'formal_model_hash'=r.model_hash)
        AND EXISTS (SELECT 1 FROM public.agent_paper_cycle_events e
          WHERE e.cycle_id=NEW.cycle_id AND e.fact_ids->'research_run_ids' ? r.id::text)
    )
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_NATIVE'; END IF;
  SELECT intent->'daily_pending_proposal' INTO proposal FROM public.agent_research_outputs
    WHERE id=NEW.output_id;
  SELECT jsonb_agg(payload ORDER BY sequence),count(*) INTO roles,n
    FROM public.agent_research_outputs WHERE run_id=NEW.run_id AND role IS NOT NULL
      AND role=payload->>'role' AND status='running' AND (intent IS NULL OR intent='null'::jsonb)
      AND created_at<=NEW.observed_at;
  IF n<>8 OR (SELECT count(*) FROM public.agent_research_outputs
       WHERE run_id=NEW.run_id AND role IS NOT NULL)<>8
    OR (SELECT count(DISTINCT x->>'role') FROM jsonb_array_elements(roles) x)<>8
    OR NOT roles @> '[{"role":"orchestrator"},{"role":"fundamental"},{"role":"market_state"},
       {"role":"macro_sector"},{"role":"bull"},{"role":"bear"},
       {"role":"evidence_audit"},{"role":"portfolio"}]'::jsonb
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_ROLES'; END IF;
  SELECT x->'daily_pending_proposal' INTO original FROM jsonb_array_elements(roles) x WHERE x->>'role'='orchestrator';
  SELECT x INTO bear FROM jsonb_array_elements(roles) x WHERE x->>'role'='bear';
  SELECT x INTO audit FROM jsonb_array_elements(roles) x WHERE x->>'role'='evidence_audit';
  SELECT x INTO portfolio FROM jsonb_array_elements(roles) x WHERE x->>'role'='portfolio';
  IF proposal IS NULL OR jsonb_typeof(proposal) IS DISTINCT FROM 'object'
    OR proposal IS DISTINCT FROM original OR proposal->>'context_hash' IS DISTINCT FROM NEW.context_hash
    OR bear->'daily_pending_review'->>'decision' IS DISTINCT FROM 'accepted'
    OR audit->'daily_pending_review'->>'decision' IS DISTINCT FROM 'accepted'
    OR audit->>'vote' IS DISTINCT FROM 'support'
    OR bear->'daily_pending_review'->>'proposal_hash' IS DISTINCT FROM NEW.proposal_hash
    OR audit->'daily_pending_review'->>'proposal_hash' IS DISTINCT FROM NEW.proposal_hash
    OR portfolio->>'daily_pending_adoption_hash' IS DISTINCT FROM NEW.proposal_hash
    OR jsonb_typeof(proposal->'items') IS DISTINCT FROM 'array'
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_REVIEW'; END IF;
  IF jsonb_array_length(proposal->'items')<>jsonb_array_length(context->'trigger_checks')
    OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(proposal->'items') x)
       <>jsonb_array_length(proposal->'items')
    OR EXISTS (SELECT 1 FROM jsonb_array_elements(proposal->'items') x
       WHERE x->>'status' NOT IN ('addressed','unresolved') OR x->>'status' IS NULL
         OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(context->'trigger_checks') y
           WHERE y->>'trigger_id'=x->>'trigger_id'))
    OR (require_addressed AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(proposal->'items') x
       WHERE x->>'trigger_id'=NEW.trigger_id::text AND x->>'status'='addressed'))
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_ITEMS'; END IF;
  hash:=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(proposal),'UTF8')),'hex');
  IF hash IS DISTINCT FROM NEW.proposal_hash
   OR encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(jsonb_build_object(
     'bear',bear->'daily_pending_review','evidence_audit',audit->'daily_pending_review')),'UTF8')),'hex') IS DISTINCT FROM NEW.review_hash
   OR NOT public.nexus_daily_v2_refs_valid(bear->'daily_pending_review'->'evidence_refs',NEW.snapshot_id,NEW.instrument_id)
   OR NOT public.nexus_daily_v2_refs_valid(audit->'daily_pending_review'->'evidence_refs',NEW.snapshot_id,NEW.instrument_id)
   OR (SELECT count(*) FROM public.agent_research_outputs o WHERE o.run_id=NEW.run_id AND o.role IS NULL AND o.status='completed')<>1
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_REVIEW_INVALID'; END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(proposal->'items') LOOP
   IF NOT public.nexus_daily_v2_refs_valid(item->'evidence_refs',NEW.snapshot_id,NEW.instrument_id)
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_REFS_INVALID'; END IF;
  END LOOP;
  IF (SELECT count(*) FROM public.research_monitor_coverage v WHERE v.run_id=NEW.run_id
    AND v.account_id=NEW.account_id AND v.instrument_id=NEW.instrument_id AND v.output_id=NEW.output_id
    AND v.observed_at=NEW.observed_at AND v.research_as_of=frozen.observed_at
    AND v.kind IN ('price','disclosure') AND v.status IN ('installed','retained','gap'))<>2
   OR EXISTS(SELECT 1 FROM public.research_monitor_coverage v WHERE v.run_id=NEW.run_id
    AND v.prior_plan_id::text IS DISTINCT FROM context->'predecessors'->v.kind->>'plan_id')
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_COVERAGE_INVALID'; END IF;
  RETURN;
END $$;
"""

_CONTEXT = r"""
CREATE FUNCTION public.nexus_validate_daily_monitor_v2(snapshot_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots%ROWTYPE; paper jsonb; c jsonb; item jsonb; material jsonb;
 parent jsonb; predecessor jsonb; content jsonb; t public.research_monitor_triggers%ROWTYPE;
 b public.research_monitor_batches%ROWTYPE; n integer; k text; legacy jsonb;
BEGIN
 SELECT * INTO s FROM public.data_snapshots WHERE id=snapshot_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_CONTEXT_INVALID'; END IF;
 paper:=s.payload->'agent_paper_context';
 IF paper IS NULL OR NOT (paper ? 'pending_monitoring_v2') THEN RETURN; END IF;
 c:=paper->'pending_monitoring_v2';
 IF s.source<>'nexus_agent_paper' OR s.schema_version<>'agent_paper_research_v1'
  OR c->>'purpose' IS DISTINCT FROM 'daily_monitor_context_v2'
  OR c->>'instrument_id' IS DISTINCT FROM s.provider_object_id
  OR c->>'instrument_id' IS DISTINCT FROM paper->>'instrument_id'
  OR c->>'cycle_id' IS DISTINCT FROM paper->>'cycle_id'
  OR (c->>'decision_time')::timestamptz IS DISTINCT FROM s.observed_at
  OR (paper->>'decision_time')::timestamptz IS DISTINCT FROM s.observed_at
  OR paper->'execution_locked' IS DISTINCT FROM 'true'::jsonb
  OR greatest(s.available_at,s.received_at,s.created_at)>s.observed_at
  OR jsonb_typeof(c->'trigger_checks') IS DISTINCT FROM 'array'
  OR jsonb_typeof(c->'materials') IS DISTINCT FROM 'array'
  OR jsonb_typeof(c->'predecessors') IS DISTINCT FROM 'object'
  OR NOT (c->'predecessors' ?& ARRAY['price','disclosure'])
  OR (SELECT count(*) FROM jsonb_object_keys(c->'predecessors'))<>2
  OR (paper ? 'pending_monitoring') OR (paper ? 'pending_monitoring_v2_gap')
  OR NOT EXISTS(SELECT 1 FROM public.agent_paper_cycles cy WHERE cy.id::text=c->>'cycle_id'
    AND cy.account_id::text=c->>'account_id' AND cy.decision_time=s.observed_at
    AND cy.execution_locked AND cy.candidate_batch_id IS NOT NULL
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(cy.candidate_manifest) x WHERE x->>'instrument_id'=s.provider_object_id))
 THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_CONTEXT_INVALID'; END IF;
 n:=jsonb_array_length(c->'trigger_checks');
 IF n NOT BETWEEN 1 AND 32
  OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(c->'trigger_checks') x)<>n
  OR (SELECT count(DISTINCT x->>'check_id') FROM jsonb_array_elements(c->'trigger_checks') x)<>n
  OR (SELECT count(DISTINCT x->>'snapshot_id') FROM jsonb_array_elements(c->'materials') x)<>jsonb_array_length(c->'materials')
 THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_CONTEXT_INVALID'; END IF;
 FOREACH k IN ARRAY ARRAY['price','disclosure'] LOOP
  predecessor:=c->'predecessors'->k;
  IF jsonb_typeof(predecessor) IS DISTINCT FROM 'object'
   OR NOT (predecessor ?& ARRAY['plan_id','generation'])
   OR ((predecessor->'plan_id'='null'::jsonb) IS DISTINCT FROM (predecessor->'generation'='null'::jsonb))
   OR (predecessor->>'plan_id' IS NULL AND EXISTS(SELECT 1 FROM public.research_monitor_plans p
     WHERE p.kind=k AND p.account_id::text=c->>'account_id' AND p.instrument_id::text=c->>'instrument_id'
      AND greatest(p.created_at,p.effective_at)<=s.observed_at))
   OR EXISTS(SELECT 1 FROM public.research_monitor_plans p WHERE p.kind=k
     AND p.account_id::text=c->>'account_id' AND p.instrument_id::text=c->>'instrument_id'
     AND greatest(p.created_at,p.effective_at)<=s.observed_at
     AND p.generation>(predecessor->>'generation')::integer)
   OR (predecessor->>'plan_id' IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.research_monitor_plans p
     WHERE p.id::text=predecessor->>'plan_id' AND p.generation=(predecessor->>'generation')::integer
      AND p.kind=k AND p.account_id::text=c->>'account_id' AND p.instrument_id::text=c->>'instrument_id'
      AND greatest(p.created_at,p.effective_at)<=s.observed_at))
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_PREDECESSOR_INVALID'; END IF;
 END LOOP;
 FOR material IN SELECT value FROM jsonb_array_elements(c->'materials') LOOP
  IF NOT EXISTS(SELECT 1 FROM public.data_snapshots ms WHERE ms.id::text=material->>'snapshot_id'
    AND ms.source='nexus_monitor_batch_inputs' AND ms.schema_version='monitor_batch_inputs_v1'
    AND ms.payload=material->'payload' AND ms.provider_object_id=material->'payload'->>'batch_id'
    AND ms.revision=material->>'content_hash'
    AND ms.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(ms.payload),'UTF8')),'hex')
    AND greatest(ms.created_at,ms.observed_at,ms.available_at,ms.received_at)<=s.observed_at
    AND ms.observed_at=(ms.payload->>'prepared_at')::timestamptz)
   OR material->'payload'->>'schema_version' IS DISTINCT FROM 'monitor_batch_inputs_v1'
   OR material->'payload'->>'account_id' IS DISTINCT FROM c->>'account_id'
   OR material->'payload'->>'instrument_id' IS DISTINCT FROM c->>'instrument_id'
   OR material->'payload'->'execution_locked' IS DISTINCT FROM 'true'::jsonb
   OR material->'payload'->>'impact_review' IS DISTINCT FROM 'not_performed'
   OR jsonb_typeof(material->'payload'->'members') IS DISTINCT FROM 'array'
   OR jsonb_typeof(material->'payload'->'parents') IS DISTINCT FROM 'array'
   OR jsonb_typeof(material->'payload'->'contents') IS DISTINCT FROM 'array'
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_MATERIAL_SOURCE_INVALID'; END IF;
  SELECT * INTO b FROM public.research_monitor_batches WHERE id::text=material->'payload'->>'batch_id';
  IF NOT FOUND OR b.account_id::text<>c->>'account_id' OR b.instrument_id::text<>c->>'instrument_id'
   OR greatest(b.created_at,b.checked_at,b.sealed_at)>s.observed_at
   OR (SELECT jsonb_agg(x->>'trigger_id' ORDER BY ord) FROM jsonb_array_elements(material->'payload'->'members') WITH ORDINALITY a(x,ord)) IS DISTINCT FROM b.trigger_ids
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(c->'trigger_checks') x WHERE x->>'materials_snapshot_id'=material->>'snapshot_id')
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_MATERIAL_BATCH_INVALID'; END IF;
  PERFORM public.nexus_monitor_validate_batch_readonly(b.id);
  FOR parent IN SELECT value FROM jsonb_array_elements(material->'payload'->'parents') LOOP
   IF NOT EXISTS(SELECT 1 FROM public.research_monitor_plans p
    JOIN public.agent_research_runs r ON r.id=p.source_run_id
    JOIN public.agent_research_outputs o ON o.id=p.source_output_id
    WHERE p.id::text=parent->>'plan_id' AND p.kind=parent->>'kind'
     AND p.account_id=b.account_id AND p.instrument_id=b.instrument_id
     AND p.generation=(parent->>'generation')::integer
     AND p.source_run_id::text=parent->>'run_id' AND p.source_output_id::text=parent->>'output_id'
     AND p.source_hash=parent->>'output_hash' AND o.content_hash=p.source_hash AND o.intent=parent->'intent'
     AND r.status='completed' AND o.status='completed' AND o.role IS NULL AND o.run_id=r.id
     AND greatest(p.created_at,r.created_at,o.created_at,r.decision_time)<=s.observed_at
     AND jsonb_array_length(parent->'role_outputs')=8
     AND parent->'role_outputs'=(SELECT jsonb_agg(ro.payload ORDER BY ro.sequence)
       FROM public.agent_research_outputs ro WHERE ro.run_id=r.id AND ro.role IS NOT NULL))
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_PARENT_INVALID'; END IF;
  END LOOP;
  FOR content IN SELECT value FROM jsonb_array_elements(material->'payload'->'contents') LOOP
   IF NOT EXISTS(SELECT 1 FROM public.data_snapshots cs WHERE cs.id::text=content->>'snapshot_id'
    AND cs.source='nexus_sec_monitor_filing' AND cs.revision=content->>'canonical_hash'
    AND cs.schema_version='sec_monitor_filing_v1' AND cs.payload=content->'archive'
    AND cs.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(cs.payload),'UTF8')),'hex')
    AND cs.payload->>'instrument_id'=c->>'instrument_id'
    AND EXISTS(SELECT 1 FROM public.research_evidence_refs er WHERE er.id::text=content->>'evidence_ref_id'
      AND er.source=cs.source AND er.provider_object_id=cs.provider_object_id
      AND er.revision=cs.revision AND er.content_hash=cs.revision AND er.available_at=cs.available_at)
    AND greatest(cs.created_at,cs.observed_at,cs.available_at,cs.received_at)<=s.observed_at)
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_CONTENT_INVALID'; END IF;
  END LOOP;
 END LOOP;
 FOR item IN SELECT value FROM jsonb_array_elements(c->'trigger_checks') LOOP
  SELECT * INTO t FROM public.research_monitor_triggers WHERE id::text=item->>'trigger_id';
  IF NOT FOUND OR t.account_id::text<>c->>'account_id' OR t.instrument_id::text<>c->>'instrument_id'
   OR t.kind IS DISTINCT FROM item->>'kind' OR t.event_key IS DISTINCT FROM item->>'event_key'
   OR t.batch_id::text IS DISTINCT FROM item->>'batch_id'
   OR t.research_run_id::text IS DISTINCT FROM item->>'original_run_id'
   OR NOT (item ?& ARRAY['original_run_id','original_output_id','batch_id','materials_snapshot_id'])
   OR NOT EXISTS(SELECT 1 FROM public.research_monitor_checks ch JOIN public.research_monitor_plans p ON p.id=ch.plan_id
    WHERE ch.id=t.check_id AND ch.id::text=item->>'check_id' AND p.id::text=item->>'plan_id'
     AND ch.kind=t.kind AND p.kind=t.kind AND ch.event_key=t.event_key
     AND ch.account_id=t.account_id AND ch.instrument_id=t.instrument_id
     AND p.account_id=t.account_id AND p.instrument_id=t.instrument_id
     AND t.condition_hash=p.condition_hash AND t.cause=item->>'cause' AND ch.result='true'
     AND ch.checked_at=(item->>'checked_at')::timestamptz AND ch.observation=item->'observation'
     AND jsonb_typeof(ch.observation)='object'
     AND p.effective_at<=ch.checked_at AND ch.checked_at<p.valid_until
     AND greatest(t.created_at,ch.created_at,ch.checked_at,p.created_at)<=s.observed_at)
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_MEMBER_INVALID'; END IF;
  IF t.batch_id IS NULL THEN
   IF t.kind<>'price' OR t.research_run_id IS NOT NULL OR t.status NOT IN ('pending','blocked')
    OR item->>'materials_snapshot_id' IS NOT NULL OR item->>'original_output_id' IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(c->'legacy'->'trigger_checks') l
      WHERE l=item-ARRAY['kind','event_key','original_run_id','original_output_id','batch_id','materials_snapshot_id'])
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_LEGACY_INVALID'; END IF;
  ELSE
   SELECT x INTO material FROM jsonb_array_elements(c->'materials') x WHERE x->>'snapshot_id'=item->>'materials_snapshot_id';
   IF material IS NULL OR material->'payload'->>'batch_id' IS DISTINCT FROM t.batch_id::text
    OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(material->'payload'->'members') mm
      WHERE mm-'checked_at'=item-ARRAY['original_run_id','original_output_id','batch_id','materials_snapshot_id','checked_at']
       AND (mm->>'checked_at')::timestamptz=(item->>'checked_at')::timestamptz)
    OR (t.kind='disclosure' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(material->'payload'->'contents') cc
      WHERE 'sec:'||(cc->'archive'->>'issuer_cik')||':'||(cc->'archive'->'entry'->>'accession_number')=t.event_key))
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_MATERIAL_MEMBER_INVALID'; END IF;
   IF t.research_run_id IS NULL THEN
    IF t.status NOT IN ('pending','blocked') OR item->>'original_output_id' IS NOT NULL
    THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_ORIGINAL_INVALID'; END IF;
   ELSIF t.status<>'completed' OR NOT EXISTS(
    SELECT 1 FROM public.research_monitor_increments m JOIN public.agent_research_runs r ON r.id=m.run_id
    JOIN public.agent_research_outputs o ON o.run_id=r.id AND o.id::text=item->>'original_output_id'
    JOIN public.agent_research_outputs proposer ON proposer.run_id=r.id AND proposer.role='market_state'
    JOIN public.agent_daily_budget_reservations br ON br.run_id=r.id JOIN public.jobs j ON j.id=m.job_id
    JOIN public.job_leases l ON l.job_id=j.id JOIN public.job_attempts a ON a.job_id=j.id
    WHERE m.batch_id=t.batch_id AND m.run_id=t.research_run_id AND m.account_id=t.account_id AND m.instrument_id=t.instrument_id
     AND r.data_snapshot_id=m.snapshot_id AND r.instrument_id=t.instrument_id
     AND j.scope_kind='instrument' AND j.scope_id=t.instrument_id::text AND j.effective_at=r.decision_time
     AND (SELECT count(*) FROM public.agent_research_outputs final WHERE final.run_id=r.id AND final.role IS NULL AND final.status='completed')=1
     AND r.status='completed' AND r.retry_count=0 AND r.input_tokens IS NOT NULL AND r.output_tokens IS NOT NULL
     AND r.settled_cost_usd IS NOT NULL AND br.state='settled' AND br.settled_usd=r.settled_cost_usd
     AND br.reserved_usd=r.reserved_cost_usd AND br.pricing_version=r.pricing_version
     AND o.role IS NULL AND o.status='completed' AND greatest(r.created_at,o.created_at,j.finished_at)<=s.observed_at
     AND j.status='succeeded' AND j.payload=jsonb_build_object('run_id',r.id::text)
     AND j.job_type='agent.paper.research' AND j.schema_version='agent_research_job_v1'
     AND j.attempt_count=1 AND j.max_attempts=1 AND j.manual_retry_count=0
     AND a.outcome='succeeded' AND a.attempt_no=1 AND a.worker_id=l.worker_id
     AND a.lease_token=l.lease_token AND a.fence_token=l.fence_token
     AND a.started_at=l.acquired_at AND a.finished_at=j.finished_at
     AND l.acquired_at<=l.heartbeat_at AND l.heartbeat_at<=j.finished_at AND j.finished_at<l.expires_at
     AND j.last_error_code IS NULL AND a.error_code IS NULL
     AND (SELECT count(*) FROM public.jobs q WHERE q.payload=jsonb_build_object('run_id',r.id::text))=1
     AND (SELECT count(*) FROM public.job_leases q WHERE q.job_id=j.id)=1
     AND (SELECT count(*) FROM public.job_attempts q WHERE q.job_id=j.id)=1
     AND proposer.payload->'mixed_event_proposal'->>'context_hash'=m.context_hash
     AND EXISTS(SELECT 1 FROM public.data_snapshots original_snapshot WHERE original_snapshot.id=m.snapshot_id
      AND original_snapshot.source='nexus_monitor_increment_v2'
      AND original_snapshot.payload->'monitor_context'->>'batch_id'=t.batch_id::text
      AND original_snapshot.payload->'monitor_context'->>'account_id'=t.account_id::text
      AND original_snapshot.payload->'monitor_context'->>'instrument_id'=t.instrument_id::text
      AND jsonb_array_length(proposer.payload->'mixed_event_proposal'->'items')=jsonb_array_length(original_snapshot.payload->'monitor_context'->'trigger_checks')
      AND (SELECT count(DISTINCT z->>'trigger_id') FROM jsonb_array_elements(proposer.payload->'mixed_event_proposal'->'items') z)=jsonb_array_length(proposer.payload->'mixed_event_proposal'->'items')
      AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proposer.payload->'mixed_event_proposal'->'items') pi
        WHERE pi->>'status' IS NULL OR pi->>'status' NOT IN ('addressed','unresolved')
         OR NOT public.nexus_mixed_receipt_refs_valid(pi->'evidence_refs',original_snapshot.id,t.instrument_id,m.context_hash)
         OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(original_snapshot.payload->'monitor_context'->'trigger_checks') oi
          WHERE oi->>'trigger_id'=pi->>'trigger_id' AND oi->>'kind'=pi->>'kind' AND oi->>'event_key'=pi->>'event_key')
         OR ((pi->>'kind'='price' AND pi->'content_snapshot_id'='null'::jsonb)
          OR (pi->>'kind'='disclosure' AND EXISTS(SELECT 1 FROM jsonb_array_elements(original_snapshot.payload->'monitor_context'->'materials'->'contents') oc
           WHERE oc->>'snapshot_id'=pi->>'content_snapshot_id'
            AND 'sec:'||(oc->'archive'->>'issuer_cik')||':'||(oc->'archive'->'entry'->>'accession_number')=pi->>'event_key'))) IS NOT TRUE)
      AND EXISTS(SELECT 1 FROM jsonb_array_elements(original_snapshot.payload->'monitor_context'->'trigger_checks') original_item
       WHERE original_item->>'trigger_id'=t.id::text AND original_item->>'kind'=t.kind AND original_item->>'event_key'=t.event_key))
     AND EXISTS(SELECT 1 FROM jsonb_array_elements(proposer.payload->'mixed_event_proposal'->'items') pi
      WHERE pi->>'trigger_id'=t.id::text AND pi->>'kind'=t.kind AND pi->>'event_key'=t.event_key AND pi->>'status'='unresolved')
     AND proposer.status='running' AND proposer.payload->>'role'='market_state'
     AND EXISTS(SELECT 1 FROM public.agent_research_outputs sy WHERE sy.run_id=r.id AND sy.role IS NULL AND sy.status='synthesized' AND sy.intent=o.intent)
   ) THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_ORIGINAL_INVALID'; END IF;
  END IF;
 END LOOP;
 legacy:=c->'legacy';
 IF legacy IS DISTINCT FROM 'null'::jsonb THEN
  IF jsonb_typeof(legacy) IS DISTINCT FROM 'object' OR legacy->>'purpose' IS DISTINCT FROM 'daily_monitor_context_v1'
   OR legacy->>'cycle_id' IS DISTINCT FROM c->>'cycle_id' OR legacy->>'account_id' IS DISTINCT FROM c->>'account_id'
   OR legacy->>'instrument_id' IS DISTINCT FROM c->>'instrument_id' OR legacy->>'decision_time' IS DISTINCT FROM c->>'decision_time'
   OR (SELECT count(*) FROM jsonb_array_elements(c->'trigger_checks') x WHERE x->>'batch_id' IS NULL)<>jsonb_array_length(legacy->'trigger_checks')
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_LEGACY_INVALID'; END IF;
  IF jsonb_typeof(legacy->'parents') IS DISTINCT FROM 'array'
   OR jsonb_array_length(legacy->'parents') NOT BETWEEN 1 AND 32
   OR (SELECT count(DISTINCT x->>'plan_id') FROM jsonb_array_elements(legacy->'parents') x)<>jsonb_array_length(legacy->'parents')
   OR EXISTS((SELECT x->>'plan_id' FROM jsonb_array_elements(legacy->'parents') x EXCEPT SELECT x->>'plan_id' FROM jsonb_array_elements(legacy->'trigger_checks') x)
    UNION ALL (SELECT x->>'plan_id' FROM jsonb_array_elements(legacy->'trigger_checks') x EXCEPT SELECT x->>'plan_id' FROM jsonb_array_elements(legacy->'parents') x))
  THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_LEGACY_INVALID'; END IF;
  FOR parent IN SELECT value FROM jsonb_array_elements(legacy->'parents') LOOP
   IF NOT EXISTS(SELECT 1 FROM public.research_monitor_plans p JOIN public.agent_research_outputs o ON o.id=p.source_output_id
    JOIN public.agent_research_runs r ON r.id=p.source_run_id
    WHERE p.id::text=parent->>'plan_id' AND p.kind='price' AND p.account_id::text=c->>'account_id'
     AND p.instrument_id::text=c->>'instrument_id' AND p.generation=(parent->>'generation')::integer
     AND p.condition=parent->'condition' AND p.condition_hash=parent->>'condition_hash'
     AND p.source_run_id::text=parent->>'parent_run_id' AND p.source_output_id::text=parent->>'parent_output_id'
     AND p.source_hash=parent->>'parent_output_hash' AND o.content_hash=p.source_hash AND o.intent=parent->'parent_intent'
     AND r.status='completed' AND o.status='completed' AND o.role IS NULL
     AND greatest(p.created_at,r.created_at,o.created_at,r.decision_time)<=s.observed_at)
   THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_LEGACY_INVALID'; END IF;
  END LOOP;
 END IF;
END $$;
"""

_REFS = r"""
CREATE FUNCTION public.nexus_daily_v2_refs_valid(refs jsonb, snapshot_id uuid, instrument_id uuid)
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
DECLARE paper_hash text;
BEGIN
 IF jsonb_typeof(refs) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(refs) NOT BETWEEN 1 AND 3 THEN RETURN false; END IF;
 SELECT encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(payload->'agent_paper_context'),'UTF8')),'hex')
 INTO paper_hash FROM public.data_snapshots WHERE id=$2;
 RETURN EXISTS(SELECT 1 FROM jsonb_array_elements(refs) x WHERE x->>'source'='nexus_agent_paper_context'
   AND x->>'content_hash'=paper_hash)
  AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(refs) x WHERE NOT EXISTS(
   SELECT 1 FROM public.agent_data_snapshot_evidence e WHERE e.data_snapshot_id=$2 AND e.instrument_id=$3
    AND e.source=x->>'source' AND e.provider_object_id=x->>'object_id'
    AND e.available_at=(x->>'available_at')::timestamptz AND e.location=x->>'location'
    AND e.content_hash=x->>'content_hash') AND NOT EXISTS(
   SELECT 1 FROM public.research_evidence_refs e JOIN public.data_snapshots ds ON ds.id=$2
    WHERE x->>'source'='nexus_agent_paper_context' AND e.source=x->>'source'
     AND e.provider_object_id=x->>'object_id'
     AND e.provider_object_id=(ds.payload->'agent_paper_context'->>'cycle_id')||':'||$3::text
     AND e.available_at=ds.observed_at AND e.available_at=(x->>'available_at')::timestamptz
     AND e.revision=paper_hash AND e.content_hash=paper_hash AND x->>'content_hash'=paper_hash
     AND e.location=x->>'location' AND e.location='memory://sha256/'||paper_hash));
END $$;
"""

_EXCLUSIVE = r"""
CREATE FUNCTION public.nexus_daily_v2_exclusive_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM 1 FROM public.research_monitor_triggers WHERE id=NEW.trigger_id FOR UPDATE;
 IF (TG_TABLE_NAME<>'research_monitor_daily_trigger_receipts' AND EXISTS(
    SELECT 1 FROM public.research_monitor_daily_trigger_receipts WHERE trigger_id=NEW.trigger_id))
  OR (TG_TABLE_NAME<>'research_monitor_mixed_event_receipts' AND EXISTS(
    SELECT 1 FROM public.research_monitor_mixed_event_receipts WHERE trigger_id=NEW.trigger_id))
  OR (TG_TABLE_NAME<>'research_monitor_daily_event_receipts' AND EXISTS(
    SELECT 1 FROM public.research_monitor_daily_event_receipts WHERE trigger_id=NEW.trigger_id))
 THEN RAISE EXCEPTION 'MONITOR_EVENT_ALREADY_CONSUMED'; END IF;
 RETURN NEW;
END $$;
"""

_COVERAGE = r"""
CREATE FUNCTION public.nexus_daily_v2_coverage_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots%ROWTYPE; c jsonb;
BEGIN
 SELECT d.* INTO s FROM public.agent_research_runs r JOIN public.data_snapshots d ON d.id=r.data_snapshot_id WHERE r.id=NEW.run_id;
 c:=s.payload->'agent_paper_context'->'pending_monitoring_v2';
 IF c IS NULL THEN RETURN NULL; END IF;
 IF NEW.account_id::text IS DISTINCT FROM c->>'account_id' OR NEW.instrument_id::text IS DISTINCT FROM c->>'instrument_id'
  OR NEW.research_as_of<>s.observed_at
  OR (SELECT count(*) FROM public.research_monitor_coverage v WHERE v.run_id=NEW.run_id
   AND v.account_id=NEW.account_id AND v.instrument_id=NEW.instrument_id
   AND v.output_id IS NOT DISTINCT FROM NEW.output_id AND v.observed_at=NEW.observed_at
   AND v.research_as_of=NEW.research_as_of AND v.kind IN ('price','disclosure'))<>2
 THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_COVERAGE_INVALID'; END IF;
 RETURN NULL;
END $$;
"""

_PLAN = r"""
CREATE FUNCTION public.nexus_daily_v2_plan_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.data_snapshots%ROWTYPE; c jsonb; proof public.research_monitor_daily_event_receipts%ROWTYPE; item jsonb; proposal jsonb; reviews jsonb;
BEGIN
 SELECT d.* INTO s FROM public.agent_research_runs r JOIN public.data_snapshots d ON d.id=r.data_snapshot_id WHERE r.id=NEW.source_run_id;
 c:=s.payload->'agent_paper_context'->'pending_monitoring_v2';
 IF c IS NULL THEN RETURN NULL; END IF;
 IF NEW.account_id::text IS DISTINCT FROM c->>'account_id' OR NEW.instrument_id::text IS DISTINCT FROM c->>'instrument_id'
  OR NEW.data_snapshot_id<>s.id
  OR NEW.predecessor_id::text IS DISTINCT FROM c->'predecessors'->NEW.kind->>'plan_id'
  OR NOT EXISTS(SELECT 1 FROM public.agent_research_outputs o WHERE o.id=NEW.source_output_id
   AND o.run_id=NEW.source_run_id AND o.role IS NULL AND o.status='completed' AND o.content_hash=NEW.source_hash)
  OR (SELECT count(*) FROM public.research_monitor_coverage v WHERE v.run_id=NEW.source_run_id
   AND v.account_id=NEW.account_id AND v.instrument_id=NEW.instrument_id
   AND v.output_id=NEW.source_output_id AND v.observed_at=NEW.effective_at AND v.research_as_of=s.observed_at
   AND v.kind IN ('price','disclosure'))<>2
  OR NOT EXISTS(SELECT 1 FROM public.research_monitor_coverage v WHERE v.run_id=NEW.source_run_id
   AND v.kind=NEW.kind AND v.status='installed' AND v.active_plan_id=NEW.id AND v.output_id=NEW.source_output_id)
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(c->'trigger_checks') x WHERE x->>'kind'=NEW.kind
   AND NOT EXISTS(SELECT 1 FROM public.research_monitor_daily_event_receipts e
    WHERE e.trigger_id::text=x->>'trigger_id' AND e.kind=NEW.kind AND e.run_id=NEW.source_run_id
     AND e.output_id=NEW.source_output_id AND e.account_id=NEW.account_id AND e.instrument_id=NEW.instrument_id
     AND e.snapshot_id=s.id AND e.observed_at=NEW.effective_at))
  OR NOT EXISTS(SELECT 1 FROM public.agent_research_runs r JOIN public.agent_daily_budget_reservations br ON br.run_id=r.id
   WHERE r.id=NEW.source_run_id AND r.status='completed' AND r.input_tokens IS NOT NULL AND r.output_tokens IS NOT NULL
    AND br.state='settled' AND br.settled_usd=r.settled_cost_usd)
 THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_PLAN_INVALID'; END IF;
 item:=c->'trigger_checks'->0;
 SELECT intent->'daily_pending_proposal' INTO proposal FROM public.agent_research_outputs WHERE id=NEW.source_output_id;
 SELECT jsonb_object_agg(role,payload->'daily_pending_review') INTO reviews FROM public.agent_research_outputs
  WHERE run_id=NEW.source_run_id AND role IN ('bear','evidence_audit');
 proof.account_id:=NEW.account_id; proof.instrument_id:=NEW.instrument_id;
 proof.trigger_id:=(item->>'trigger_id')::uuid; proof.kind:=item->>'kind'; proof.event_key:=item->>'event_key';
 proof.original_run_id:=(item->>'original_run_id')::uuid; proof.original_output_id:=(item->>'original_output_id')::uuid;
 proof.batch_id:=(item->>'batch_id')::uuid; proof.materials_snapshot_id:=(item->>'materials_snapshot_id')::uuid;
 proof.cycle_id:=(c->>'cycle_id')::uuid; proof.run_id:=NEW.source_run_id; proof.output_id:=NEW.source_output_id;
 proof.snapshot_id:=s.id; proof.observed_at:=NEW.effective_at;
 proof.context_hash:=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(c),'UTF8')),'hex');
 proof.proposal_hash:=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(proposal),'UTF8')),'hex');
 proof.review_hash:=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(reviews),'UTF8')),'hex');
 PERFORM public.nexus_validate_daily_event_result(proof,false);
 RETURN NULL;
END $$;
"""


def upgrade():
    op.create_table(
        "research_monitor_daily_event_receipts",
        sa.Column("id", pg.UUID(), primary_key=True),
        *[
            sa.Column(name, pg.UUID(), sa.ForeignKey(target), nullable=nullable)
            for name, target, nullable in (
                ("account_id", "paper_accounts.id", False),
                ("instrument_id", "instruments.id", False),
                ("trigger_id", "research_monitor_triggers.id", False),
                ("original_run_id", "agent_research_runs.id", True),
                ("original_output_id", "agent_research_outputs.id", True),
                ("batch_id", "research_monitor_batches.id", True),
                ("materials_snapshot_id", "data_snapshots.id", True),
                ("cycle_id", "agent_paper_cycles.id", False),
                ("run_id", "agent_research_runs.id", False),
                ("output_id", "agent_research_outputs.id", False),
                ("snapshot_id", "data_snapshots.id", False),
            )
        ],
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("event_key", sa.String(128), nullable=False),
        *[
            sa.Column(name, sa.String(64), nullable=False)
            for name in ("context_hash", "proposal_hash", "review_hash")
        ],
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("trigger_id"),
        sa.CheckConstraint("kind IN ('price','disclosure')", name="kind_allowed"),
        *[
            sa.CheckConstraint(f"{name} ~ '^[0-9a-f]{{64}}$'", name=name + "_canonical")
            for name in ("context_hash", "proposal_hash", "review_hash")
        ],
    )
    for definition in (_CONTEXT, _REFS, _RECEIPT, _EXCLUSIVE, _COVERAGE, _PLAN):
        op.execute(definition)
    op.execute("""CREATE FUNCTION public.nexus_validate_daily_event_receipt(receipt_id uuid)
      RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      DECLARE proof public.research_monitor_daily_event_receipts%ROWTYPE;
      BEGIN SELECT * INTO proof FROM public.research_monitor_daily_event_receipts WHERE id=receipt_id;
       IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_RECEIPT_MISSING'; END IF;
       PERFORM public.nexus_validate_daily_event_result(proof,true); END $$""")
    for table in (
        "research_monitor_daily_event_receipts",
        "research_monitor_mixed_event_receipts",
        "research_monitor_daily_trigger_receipts",
    ):
        op.execute(
            f"CREATE TRIGGER trg_daily_v2_exclusive BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_daily_v2_exclusive_guard()"
        )
    op.execute(
        "CREATE TRIGGER trg_daily_v2_immutable BEFORE UPDATE OR DELETE ON research_monitor_daily_event_receipts FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
    )
    op.execute("""CREATE FUNCTION public.nexus_daily_v2_snapshot_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN PERFORM public.nexus_validate_daily_monitor_v2(NEW.id); RETURN NULL; END $$""")
    op.execute("""CREATE FUNCTION public.nexus_daily_v2_receipt_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN PERFORM public.nexus_validate_daily_event_receipt(NEW.id); RETURN NULL; END $$""")
    op.execute("""CREATE FUNCTION public.nexus_daily_v2_trigger_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN IF EXISTS(SELECT 1 FROM public.research_monitor_daily_event_receipts e WHERE e.trigger_id=NEW.id
       AND e.original_run_id IS DISTINCT FROM NEW.research_run_id) THEN RAISE EXCEPTION 'MONITOR_EVENT_ALREADY_CONSUMED'; END IF; RETURN NULL; END $$""")
    for name, table, event, function in (
        ("snapshot", "data_snapshots", "INSERT", "snapshot"),
        ("receipt", "research_monitor_daily_event_receipts", "INSERT", "receipt"),
        ("coverage", "research_monitor_coverage", "INSERT", "coverage"),
        ("plan", "research_monitor_plans", "INSERT", "plan"),
        ("trigger", "research_monitor_triggers", "INSERT OR UPDATE", "trigger"),
    ):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_daily_v2_{name} AFTER {event} ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_daily_v2_{function}_guard()"
        )


def downgrade():
    op.execute(
        "LOCK TABLE research_monitor_daily_event_receipts,data_snapshots,research_monitor_triggers IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_daily_event_receipts)
      OR EXISTS(SELECT 1 FROM data_snapshots WHERE payload->'agent_paper_context' ?| ARRAY['pending_monitoring_v2','pending_monitoring_v2_gap'])
      THEN RAISE EXCEPTION 'MONITOR_DAILY_V2_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    for name, table in (
        ("snapshot", "data_snapshots"),
        ("coverage", "research_monitor_coverage"),
        ("plan", "research_monitor_plans"),
        ("trigger", "research_monitor_triggers"),
    ):
        op.execute(f"DROP TRIGGER trg_daily_v2_{name} ON {table}")
    for table in (
        "research_monitor_mixed_event_receipts",
        "research_monitor_daily_trigger_receipts",
    ):
        op.execute(f"DROP TRIGGER trg_daily_v2_exclusive ON {table}")
    op.execute("DROP TRIGGER trg_daily_v2_receipt ON research_monitor_daily_event_receipts")
    op.execute("DROP TRIGGER trg_daily_v2_exclusive ON research_monitor_daily_event_receipts")
    for name in ("snapshot", "receipt", "coverage", "plan", "trigger", "exclusive"):
        op.execute(f"DROP FUNCTION public.nexus_daily_v2_{name}_guard()")
    op.execute("DROP FUNCTION public.nexus_validate_daily_event_receipt(uuid)")
    op.execute(
        "DROP FUNCTION public.nexus_validate_daily_event_result(public.research_monitor_daily_event_receipts,boolean)"
    )
    op.drop_table("research_monitor_daily_event_receipts")
    op.execute("DROP FUNCTION public.nexus_daily_v2_refs_valid(jsonb,uuid,uuid)")
    op.execute("DROP FUNCTION public.nexus_validate_daily_monitor_v2(uuid)")
