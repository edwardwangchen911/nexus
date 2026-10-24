"""Bind append-only daily trigger receipts to frozen scope and original reviews.

Revision ID: 0092_monitor_daily_receipts
Revises: 0091_monitor_frozen_members
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0092_monitor_daily_receipts"
down_revision = "0091_monitor_frozen_members"
branch_labels = None
depends_on = None

_CONTEXT = r"""
CREATE FUNCTION public.nexus_validate_daily_monitor_context(snapshot_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE frozen public.data_snapshots%ROWTYPE; paper jsonb; context jsonb;
  parents jsonb; items jsonb; item jsonb; parent jsonb; n integer;
BEGIN
  SELECT * INTO frozen FROM public.data_snapshots WHERE id=snapshot_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_DAILY_CONTEXT_INVALID'; END IF;
  paper := frozen.payload->'agent_paper_context';
  IF NOT (paper ? 'pending_monitoring') OR paper IS NULL THEN RETURN; END IF;
  context := paper->'pending_monitoring';
  parents := context->'parents'; items := context->'trigger_checks';
  IF frozen.source <> 'nexus_agent_paper' OR frozen.schema_version <> 'agent_paper_research_v1'
    OR context->>'purpose' IS DISTINCT FROM 'daily_monitor_context_v1'
    OR context->>'instrument_id' IS DISTINCT FROM frozen.provider_object_id
    OR context->>'instrument_id' IS DISTINCT FROM paper->>'instrument_id'
    OR context->>'cycle_id' IS DISTINCT FROM paper->>'cycle_id'
    OR (context->>'decision_time')::timestamptz IS DISTINCT FROM frozen.observed_at
    OR (paper->>'decision_time')::timestamptz IS DISTINCT FROM frozen.observed_at
    OR paper->'execution_locked' IS DISTINCT FROM 'true'::jsonb
    OR frozen.available_at>frozen.observed_at OR frozen.received_at>frozen.observed_at
    OR frozen.created_at>frozen.observed_at
    OR jsonb_typeof(parents) IS DISTINCT FROM 'array'
    OR jsonb_typeof(items) IS DISTINCT FROM 'array'
  THEN RAISE EXCEPTION 'MONITOR_DAILY_CONTEXT_INVALID'; END IF;
  n := jsonb_array_length(items);
  IF n<1 OR n>32 OR jsonb_array_length(parents)<1 OR jsonb_array_length(parents)>32
    OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(items) x)<>n
    OR (SELECT count(DISTINCT x->>'check_id') FROM jsonb_array_elements(items) x)<>n
    OR (SELECT count(DISTINCT x->>'plan_id') FROM jsonb_array_elements(parents) x)
       <>jsonb_array_length(parents)
    OR EXISTS (
      (SELECT x->>'plan_id' FROM jsonb_array_elements(parents) x
       EXCEPT SELECT x->>'plan_id' FROM jsonb_array_elements(items) x)
      UNION ALL
      (SELECT x->>'plan_id' FROM jsonb_array_elements(items) x
       EXCEPT SELECT x->>'plan_id' FROM jsonb_array_elements(parents) x)
    )
    OR NOT EXISTS (
      SELECT 1 FROM public.agent_paper_cycles c
      WHERE c.id::text=context->>'cycle_id' AND c.account_id::text=context->>'account_id'
        AND c.decision_time=frozen.observed_at AND c.execution_locked
        AND c.candidate_batch_id IS NOT NULL
        AND EXISTS (SELECT 1 FROM jsonb_array_elements(c.candidate_manifest) m
          WHERE m->>'instrument_id'=frozen.provider_object_id)
    )
  THEN RAISE EXCEPTION 'MONITOR_DAILY_CONTEXT_INVALID'; END IF;
  FOR parent IN SELECT value FROM jsonb_array_elements(parents) LOOP
    IF NOT EXISTS (
      SELECT 1 FROM public.research_monitor_plans p
      JOIN public.agent_research_outputs o ON o.id=p.source_output_id
      JOIN public.agent_research_runs r ON r.id=p.source_run_id
      WHERE p.id::text=parent->>'plan_id' AND p.account_id::text=context->>'account_id'
        AND p.instrument_id::text=context->>'instrument_id'
        AND p.generation=(parent->>'generation')::integer
        AND p.condition=parent->'condition' AND p.condition_hash=parent->>'condition_hash'
        AND p.source_run_id::text=parent->>'parent_run_id'
        AND p.source_output_id::text=parent->>'parent_output_id'
        AND p.source_hash=parent->>'parent_output_hash' AND o.content_hash=p.source_hash
        AND o.run_id=r.id AND o.status='completed' AND o.role IS NULL
        AND o.intent=parent->'parent_intent' AND r.instrument_id=p.instrument_id AND r.status='completed'
        AND r.decision_time<frozen.observed_at
        AND greatest(p.created_at,o.created_at,r.created_at)<=frozen.observed_at
    ) THEN RAISE EXCEPTION 'MONITOR_DAILY_CONTEXT_INVALID'; END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
    IF NOT EXISTS (
      SELECT 1 FROM public.research_monitor_triggers t
      JOIN public.research_monitor_checks c ON c.id=t.check_id
      JOIN public.research_monitor_plans p ON p.id=c.plan_id
      WHERE t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'
        AND p.id::text=item->>'plan_id'
        AND t.account_id::text=context->>'account_id'
        AND t.instrument_id::text=context->>'instrument_id'
        AND c.account_id=t.account_id AND c.instrument_id=t.instrument_id
        AND p.account_id=t.account_id AND p.instrument_id=t.instrument_id
        AND t.condition_hash=p.condition_hash AND t.cause=item->>'cause' AND c.result='true'
        AND c.observation=item->'observation' AND c.observation IS NOT NULL
        AND c.checked_at=(item->>'checked_at')::timestamptz
        AND p.effective_at<=c.checked_at AND c.checked_at<p.valid_until
        AND greatest(t.created_at,c.created_at,c.checked_at)<=frozen.observed_at
        AND (c.observation->>'observed_at')::timestamptz<=frozen.observed_at
        AND (c.observation->>'available_at')::timestamptz<=frozen.observed_at
        AND (c.observation->>'received_at')::timestamptz<=frozen.observed_at
    ) THEN RAISE EXCEPTION 'MONITOR_DAILY_CONTEXT_INVALID'; END IF;
  END LOOP;
END $$;
CREATE FUNCTION public.nexus_daily_monitor_snapshot_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN PERFORM public.nexus_validate_daily_monitor_context(NEW.id); RETURN NULL; END $$;
"""

_RECEIPT = r"""
CREATE FUNCTION public.nexus_daily_monitor_receipt_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE trigger_row public.research_monitor_triggers%ROWTYPE;
  frozen public.data_snapshots%ROWTYPE; context jsonb; roles jsonb; proposal jsonb;
  original jsonb; bear jsonb; audit jsonb; portfolio jsonb; n integer;
BEGIN
  SELECT * INTO trigger_row FROM public.research_monitor_triggers WHERE id=NEW.trigger_id FOR UPDATE;
  IF NOT FOUND OR trigger_row.account_id<>NEW.account_id OR trigger_row.instrument_id<>NEW.instrument_id
    OR trigger_row.status NOT IN ('pending','blocked') OR trigger_row.research_run_id IS NOT NULL
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_TRIGGER'; END IF;
  SELECT * INTO frozen FROM public.data_snapshots WHERE id=NEW.snapshot_id;
  PERFORM public.nexus_validate_daily_monitor_context(NEW.snapshot_id);
  context := frozen.payload->'agent_paper_context'->'pending_monitoring';
  IF context IS NULL OR context->>'cycle_id' IS DISTINCT FROM NEW.cycle_id::text
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
    OR bear->'daily_pending_review'->>'proposal_hash' IS DISTINCT FROM NEW.review_bundle_hash
    OR audit->'daily_pending_review'->>'proposal_hash' IS DISTINCT FROM NEW.review_bundle_hash
    OR portfolio->>'daily_pending_adoption_hash' IS DISTINCT FROM NEW.review_bundle_hash
    OR jsonb_typeof(proposal->'items') IS DISTINCT FROM 'array'
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_REVIEW'; END IF;
  IF jsonb_array_length(proposal->'items')<>jsonb_array_length(context->'trigger_checks')
    OR (SELECT count(DISTINCT x->>'trigger_id') FROM jsonb_array_elements(proposal->'items') x)
       <>jsonb_array_length(proposal->'items')
    OR EXISTS (SELECT 1 FROM jsonb_array_elements(proposal->'items') x
       WHERE x->>'status' NOT IN ('addressed','unresolved') OR x->>'status' IS NULL
         OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(context->'trigger_checks') y
           WHERE y->>'trigger_id'=x->>'trigger_id'))
    OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(proposal->'items') x
       WHERE x->>'trigger_id'=NEW.trigger_id::text AND x->>'status'='addressed')
  THEN RAISE EXCEPTION 'MONITOR_DAILY_RECEIPT_INVALID_ITEMS'; END IF;
  RETURN NEW;
END $$;
CREATE FUNCTION public.nexus_daily_monitor_exclusive_guard()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  IF NEW.research_run_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.research_monitor_daily_trigger_receipts WHERE trigger_id=NEW.id
  ) THEN RAISE EXCEPTION 'MONITOR_DAILY_TRIGGER_ALREADY_COVERED'; END IF;
  RETURN NULL;
END $$;
"""


def upgrade():
    op.create_table(
        "research_monitor_daily_trigger_receipts",
        sa.Column("id", pg.UUID(), primary_key=True),
        *[
            sa.Column(name, pg.UUID(), sa.ForeignKey(target), nullable=False)
            for name, target in (
                ("trigger_id", "research_monitor_triggers.id"),
                ("account_id", "paper_accounts.id"),
                ("instrument_id", "instruments.id"),
                ("cycle_id", "agent_paper_cycles.id"),
                ("run_id", "agent_research_runs.id"),
                ("output_id", "agent_research_outputs.id"),
                ("snapshot_id", "data_snapshots.id"),
            )
        ],
        sa.Column("context_hash", sa.String(64), nullable=False),
        sa.Column("review_bundle_hash", sa.String(64), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("trigger_id"),
        sa.CheckConstraint("context_hash ~ '^[0-9a-f]{64}$'", name="context_hash_canonical"),
        sa.CheckConstraint("review_bundle_hash ~ '^[0-9a-f]{64}$'", name="review_hash_canonical"),
    )
    for definitions in (_CONTEXT, _RECEIPT):
        for definition in definitions.strip().split("\nCREATE FUNCTION "):
            op.execute(
                definition
                if definition.startswith("CREATE FUNCTION ")
                else "CREATE FUNCTION " + definition
            )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_daily_monitor_snapshot AFTER INSERT ON public.data_snapshots "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.nexus_daily_monitor_snapshot_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_daily_monitor_receipt BEFORE INSERT ON public.research_monitor_daily_trigger_receipts "
        "FOR EACH ROW EXECUTE FUNCTION public.nexus_daily_monitor_receipt_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_daily_monitor_receipt_immutable BEFORE UPDATE OR DELETE ON public.research_monitor_daily_trigger_receipts "
        "FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_daily_monitor_exclusive AFTER INSERT OR UPDATE ON public.research_monitor_triggers "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.nexus_daily_monitor_exclusive_guard()"
    )
    op.execute("""DO $$ DECLARE row record; BEGIN
      FOR row IN SELECT id FROM public.data_snapshots
        WHERE (payload->'agent_paper_context') ? 'pending_monitoring'
      LOOP PERFORM public.nexus_validate_daily_monitor_context(row.id); END LOOP;
    END $$""")


def downgrade():
    op.execute(
        "LOCK TABLE public.research_monitor_daily_trigger_receipts,public.data_snapshots,public.research_monitor_triggers IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $$ BEGIN IF EXISTS (SELECT 1 FROM public.research_monitor_daily_trigger_receipts)
      OR EXISTS (SELECT 1 FROM public.data_snapshots WHERE (payload->'agent_paper_context') ? 'pending_monitoring')
      THEN RAISE EXCEPTION 'MONITOR_DAILY_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    op.execute("DROP TRIGGER trg_daily_monitor_snapshot ON public.data_snapshots")
    op.execute("DROP TRIGGER trg_daily_monitor_exclusive ON public.research_monitor_triggers")
    op.drop_table("research_monitor_daily_trigger_receipts")
    op.execute("DROP FUNCTION public.nexus_daily_monitor_exclusive_guard()")
    op.execute("DROP FUNCTION public.nexus_daily_monitor_receipt_guard()")
    op.execute("DROP FUNCTION public.nexus_daily_monitor_snapshot_guard()")
    op.execute("DROP FUNCTION public.nexus_validate_daily_monitor_context(uuid)")
