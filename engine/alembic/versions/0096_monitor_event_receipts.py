"""Append-only mixed event receipts, bound to original reviews and typed coverage.

Revision ID: 0096_monitor_event_receipts
Revises: 0095_monitor_mixed_increments
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0096_monitor_event_receipts"
down_revision = "0095_monitor_mixed_increments"
branch_labels = None
depends_on = None

_CANONICAL = r"""
CREATE FUNCTION public.nexus_mixed_receipt_canonical(value jsonb)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,public AS $$
DECLARE result text; raw text; ch text; cp integer; i integer;
BEGIN
 CASE jsonb_typeof(value)
 WHEN 'object' THEN
  SELECT '{'||coalesce(string_agg(public.nexus_mixed_receipt_canonical(to_jsonb(key))||':'||public.nexus_mixed_receipt_canonical(v),',' ORDER BY key COLLATE "C"),'')||'}'
   INTO result FROM jsonb_each(value) x(key,v);
 WHEN 'array' THEN
  SELECT '['||coalesce(string_agg(public.nexus_mixed_receipt_canonical(v),',' ORDER BY ord),'')||']'
   INTO result FROM jsonb_array_elements(value) WITH ORDINALITY x(v,ord);
 WHEN 'string' THEN
  raw:=to_json(value #>> '{}')::text; result:='';
  FOR i IN 1..length(raw) LOOP
   ch:=substr(raw,i,1); cp:=ascii(ch);
   IF cp<127 THEN result:=result||ch;
   ELSIF cp<=65535 THEN result:=result||E'\\u'||lpad(to_hex(cp),4,'0');
   ELSE cp:=cp-65536; result:=result||E'\\u'||to_hex(55296+(cp/1024))||E'\\u'||to_hex(56320+(cp%1024)); END IF;
  END LOOP;
 ELSE result:=value::text;
 END CASE;
 RETURN result;
END $$;
"""

_RECEIPT = r"""
CREATE FUNCTION public.nexus_mixed_event_receipt_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE m public.research_monitor_increments%ROWTYPE; t public.research_monitor_triggers%ROWTYPE;
 s public.data_snapshots%ROWTYPE; c jsonb; roles jsonb; proposal jsonb;
 bear jsonb; audit jsonb; portfolio jsonb; item jsonb; n integer; hash text;
BEGIN
 SELECT * INTO m FROM public.research_monitor_increments WHERE batch_id=NEW.batch_id FOR UPDATE;
 IF NOT FOUND OR m.account_id<>NEW.account_id OR m.instrument_id<>NEW.instrument_id OR m.run_id<>NEW.run_id
  OR m.context_hash<>NEW.context_hash THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_BINDING_INVALID'; END IF;
 SELECT * INTO t FROM public.research_monitor_triggers WHERE id=NEW.trigger_id FOR UPDATE;
 IF NOT FOUND OR t.batch_id IS DISTINCT FROM NEW.batch_id OR t.account_id<>NEW.account_id
  OR t.instrument_id<>NEW.instrument_id OR t.research_run_id IS DISTINCT FROM NEW.run_id
  OR t.status<>'completed' THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_BINDING_INVALID'; END IF;
 SELECT * INTO s FROM public.data_snapshots WHERE id=m.snapshot_id;
 PERFORM public.nexus_monitor_validate_mixed_members(s.id);
 c:=s.payload->'monitor_context';
 IF c->>'materials_snapshot_id' IS DISTINCT FROM NEW.materials_snapshot_id::text
  OR NEW.observed_at<s.observed_at
  OR NOT EXISTS(SELECT 1 FROM public.data_snapshots material WHERE material.id=NEW.materials_snapshot_id
    AND material.source='nexus_monitor_batch_inputs' AND material.provider_object_id=NEW.batch_id::text
    AND material.revision=c->>'materials_hash' AND material.payload=c->'materials')
  OR NOT EXISTS(SELECT 1 FROM public.agent_data_snapshot_evidence e WHERE e.instrument_id=NEW.instrument_id
    AND e.data_snapshot_id=s.id AND e.source='nexus_monitor_increment_v2' AND e.content_hash=NEW.context_hash)
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_BINDING_INVALID'; END IF;
 IF NOT EXISTS (
  SELECT 1 FROM public.agent_research_runs r
  JOIN public.agent_research_outputs o ON o.id=NEW.output_id AND o.run_id=r.id
  JOIN public.agent_daily_budget_reservations b ON b.run_id=r.id
  JOIN public.jobs j ON j.id=m.job_id
  JOIN public.job_leases l ON l.job_id=j.id
  JOIN public.job_attempts a ON a.job_id=j.id AND a.attempt_no=j.attempt_count
  WHERE r.id=NEW.run_id AND r.data_snapshot_id=s.id AND r.instrument_id=NEW.instrument_id
   AND r.status='completed' AND r.retry_count=0 AND r.input_tokens IS NOT NULL AND r.output_tokens IS NOT NULL
   AND r.settled_cost_usd IS NOT NULL AND b.state='settled' AND b.settled_usd=r.settled_cost_usd
   AND b.reserved_usd=r.reserved_cost_usd AND b.pricing_version=r.pricing_version
   AND o.status='completed' AND o.role IS NULL AND jsonb_typeof(o.intent)='object'
   AND greatest(r.created_at,o.created_at)<=NEW.observed_at
   AND j.payload=jsonb_build_object('run_id',r.id::text) AND j.job_type='agent.paper.research'
   AND j.schema_version='agent_research_job_v1' AND j.scope_kind='instrument'
   AND j.scope_id=NEW.instrument_id::text AND j.effective_at=r.decision_time AND j.status='succeeded'
   AND j.attempt_count=1 AND j.max_attempts=1 AND j.manual_retry_count=0
   AND a.outcome='succeeded' AND a.worker_id=l.worker_id AND a.lease_token=l.lease_token
   AND a.fence_token=l.fence_token AND a.started_at=l.acquired_at AND a.finished_at=j.finished_at
   AND l.acquired_at<=l.heartbeat_at AND l.heartbeat_at<=j.finished_at
   AND j.finished_at<=NEW.observed_at AND j.finished_at<l.expires_at
   AND j.last_error_code IS NULL AND a.error_code IS NULL
   AND (SELECT count(*) FROM public.jobs x WHERE x.payload=jsonb_build_object('run_id',r.id::text))=1
   AND (SELECT count(*) FROM public.job_leases x WHERE x.job_id=j.id)=1
   AND (SELECT count(*) FROM public.job_attempts x WHERE x.job_id=j.id)=1
   AND (SELECT count(*) FROM public.agent_research_outputs x WHERE x.run_id=r.id AND x.role IS NULL AND x.status='completed')=1
 ) THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_NATIVE_INVALID'; END IF;
 SELECT jsonb_agg(payload ORDER BY sequence),count(*) INTO roles,n FROM public.agent_research_outputs
  WHERE run_id=NEW.run_id AND role IS NOT NULL AND role=payload->>'role' AND status='running'
   AND (intent IS NULL OR intent='null'::jsonb) AND created_at<=NEW.observed_at;
 IF n<>8 OR (SELECT count(*) FROM public.agent_research_outputs WHERE run_id=NEW.run_id AND role IS NOT NULL)<>8
  OR (SELECT count(DISTINCT x->>'role') FROM jsonb_array_elements(roles) x)<>8
  OR NOT roles @> '[{"role":"orchestrator"},{"role":"fundamental"},{"role":"market_state"},{"role":"macro_sector"},{"role":"bull"},{"role":"bear"},{"role":"evidence_audit"},{"role":"portfolio"}]'::jsonb
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_ROLES_INVALID'; END IF;
 SELECT x->'mixed_event_proposal' INTO proposal FROM jsonb_array_elements(roles) x WHERE x->>'role'='market_state';
 SELECT x INTO bear FROM jsonb_array_elements(roles) x WHERE x->>'role'='bear';
 SELECT x INTO audit FROM jsonb_array_elements(roles) x WHERE x->>'role'='evidence_audit';
 SELECT x INTO portfolio FROM jsonb_array_elements(roles) x WHERE x->>'role'='portfolio';
 hash:=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(proposal),'UTF8')),'hex');
 IF jsonb_typeof(proposal) IS DISTINCT FROM 'object' OR proposal->>'context_hash' IS DISTINCT FROM NEW.context_hash
  OR hash IS DISTINCT FROM NEW.proposal_hash
  OR jsonb_typeof(bear->'mixed_event_review'->'evidence_refs') IS DISTINCT FROM 'array'
  OR jsonb_array_length(bear->'mixed_event_review'->'evidence_refs') NOT BETWEEN 1 AND 3
  OR jsonb_typeof(audit->'mixed_event_review'->'evidence_refs') IS DISTINCT FROM 'array'
  OR jsonb_array_length(audit->'mixed_event_review'->'evidence_refs') NOT BETWEEN 1 AND 3
  OR bear->'mixed_event_review'->>'decision' IS DISTINCT FROM 'accepted'
  OR audit->'mixed_event_review'->>'decision' IS DISTINCT FROM 'accepted' OR audit->>'vote' IS DISTINCT FROM 'support'
  OR bear->'mixed_event_review'->>'proposal_hash' IS DISTINCT FROM hash
  OR audit->'mixed_event_review'->>'proposal_hash' IS DISTINCT FROM hash
  OR portfolio->>'mixed_event_adoption_hash' IS DISTINCT FROM hash
  OR encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(jsonb_build_object(
    'bear',bear->'mixed_event_review','evidence_audit',audit->'mixed_event_review')),'UTF8')),'hex') IS DISTINCT FROM NEW.review_hash
  OR NOT EXISTS(SELECT 1 FROM public.agent_research_outputs o WHERE o.id=NEW.output_id AND EXISTS(SELECT 1 FROM public.agent_research_outputs synthesized WHERE synthesized.run_id=NEW.run_id AND synthesized.role IS NULL AND synthesized.status='synthesized' AND synthesized.intent=o.intent))
  OR jsonb_typeof(proposal->'items') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_REVIEW_INVALID'; END IF;
 n:=jsonb_array_length(proposal->'items');
 IF n<>jsonb_array_length(c->'trigger_checks')
  OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(proposal->'items') x)<>n
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_ITEMS_INVALID'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(proposal->'items') LOOP
  IF item->>'status' IS NULL OR item->>'status' NOT IN ('addressed','unresolved')
   OR jsonb_typeof(item->'evidence_refs') IS DISTINCT FROM 'array'
   OR jsonb_array_length(item->'evidence_refs')>3
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(c->'trigger_checks') x
     WHERE x->>'trigger_id'=item->>'trigger_id' AND x->>'kind'=item->>'kind' AND x->>'event_key'=item->>'event_key')
   OR ((item->>'kind'='price' AND item->'content_snapshot_id'='null'::jsonb)
     OR (item->>'kind'='disclosure' AND EXISTS(SELECT 1 FROM jsonb_array_elements(c->'materials'->'contents') content
       WHERE content->>'snapshot_id'=item->>'content_snapshot_id'
        AND 'sec:'||(content->'archive'->>'issuer_cik')||':'||(content->'archive'->'entry'->>'accession_number')=item->>'event_key'))) IS NOT TRUE
  THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_ITEMS_INVALID'; END IF;
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(proposal->'items') x WHERE x->>'trigger_id'=NEW.trigger_id::text AND x->>'status'='addressed')
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_ITEMS_INVALID'; END IF;
 IF (SELECT count(*) FROM public.research_monitor_coverage v WHERE v.run_id=NEW.run_id
   AND v.account_id=NEW.account_id AND v.instrument_id=NEW.instrument_id AND v.output_id=NEW.output_id
   AND v.research_as_of=s.observed_at AND v.observed_at=NEW.observed_at
   AND v.kind IN ('price','disclosure') AND v.status IN ('installed','retained','gap','superseded'))<>2
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_COVERAGE_INVALID'; END IF;
 RETURN NULL;
END $$;
"""


_COVERAGE = r"""
CREATE FUNCTION public.nexus_mixed_coverage_atomic_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE m public.research_monitor_increments%ROWTYPE;
BEGIN
 SELECT * INTO m FROM public.research_monitor_increments WHERE run_id=NEW.run_id FOR UPDATE;
 IF NOT FOUND OR m.batch_id IS NULL THEN RETURN NULL; END IF;
 IF NEW.account_id<>m.account_id OR NEW.instrument_id<>m.instrument_id OR NEW.research_as_of<>m.research_as_of
  OR (SELECT count(*) FROM public.research_monitor_coverage v WHERE v.run_id=m.run_id
   AND v.account_id=m.account_id AND v.instrument_id=m.instrument_id
   AND v.research_as_of=m.research_as_of AND v.observed_at=NEW.observed_at
   AND v.output_id IS NOT DISTINCT FROM NEW.output_id AND v.kind IN ('price','disclosure'))<>2
 THEN RAISE EXCEPTION 'MONITOR_EVENT_RECEIPT_COVERAGE_INVALID'; END IF;
 RETURN NULL;
END $$;
"""


def upgrade():
    op.create_table(
        "research_monitor_mixed_event_receipts",
        sa.Column("id", pg.UUID(), primary_key=True),
        *[
            sa.Column(name, pg.UUID(), sa.ForeignKey(target), nullable=False)
            for name, target in (
                ("account_id", "paper_accounts.id"),
                ("instrument_id", "instruments.id"),
                ("batch_id", "research_monitor_batches.id"),
                ("trigger_id", "research_monitor_triggers.id"),
                ("run_id", "agent_research_runs.id"),
                ("output_id", "agent_research_outputs.id"),
                ("materials_snapshot_id", "data_snapshots.id"),
            )
        ],
        *[
            sa.Column(name, sa.String(64), nullable=False)
            for name in ("context_hash", "proposal_hash", "review_hash")
        ],
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("trigger_id"),
        *[
            sa.CheckConstraint(f"{name} ~ '^[0-9a-f]{{64}}$'", name=name + "_canonical")
            for name in ("context_hash", "proposal_hash", "review_hash")
        ],
    )
    op.execute(_CANONICAL)
    op.execute(_RECEIPT)
    op.execute(_COVERAGE)
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_mixed_coverage_atomic AFTER INSERT ON research_monitor_coverage DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_mixed_coverage_atomic_guard()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_mixed_event_receipt AFTER INSERT ON research_monitor_mixed_event_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_mixed_event_receipt_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_mixed_event_receipt_immutable BEFORE UPDATE OR DELETE ON research_monitor_mixed_event_receipts FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
    )


def downgrade():
    op.execute(
        "LOCK TABLE research_monitor_mixed_event_receipts,research_monitor_coverage IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_mixed_event_receipts)
      OR EXISTS(SELECT 1 FROM research_monitor_coverage c JOIN research_monitor_increments m ON m.run_id=c.run_id WHERE m.batch_id IS NOT NULL)
      THEN RAISE EXCEPTION 'MONITOR_EVENT_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    op.execute("DROP TRIGGER trg_mixed_coverage_atomic ON research_monitor_coverage")
    op.execute("DROP FUNCTION public.nexus_mixed_coverage_atomic_guard()")
    op.drop_table("research_monitor_mixed_event_receipts")
    op.execute("DROP FUNCTION public.nexus_mixed_event_receipt_guard()")
    op.execute("DROP FUNCTION public.nexus_mixed_receipt_canonical(jsonb)")
