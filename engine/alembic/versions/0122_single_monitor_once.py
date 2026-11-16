"""One immutable single-stock monitoring root; retain all prior owners and liabilities."""

from alembic import op
from sqlalchemy import text

revision = "0122_single_monitor_once"
down_revision = "0121_sec_cash_flow_components"
branch_labels = None
depends_on = None

# These fragments are deliberately exact predecessor matches. No global version
# substitution may change v3's full batch or v4/v5's research-only semantics.
_OWNER = "(r.contract_version=3 AND r.mode='research_and_monitor')"
_MONITOR_OWNER = "((r.contract_version=3 AND r.mode='research_and_monitor') OR (r.contract_version=6 AND r.mode='research_and_monitor'))"
_SELECTED = "CASE WHEN r.contract_version IN (4,5) THEN"
_MONITOR_SELECTED = "CASE WHEN r.contract_version IN (4,5,6) THEN"
_PURPOSE = "CASE WHEN r.contract_version=3 THEN 'independent_monitor_research_v1' ELSE 'research_only_v2' END"
_MONITOR_PURPOSE = "CASE WHEN r.contract_version IN (3,6) THEN 'independent_monitor_research_v1' ELSE 'research_only_v2' END"
_TAIL = "AND a.idempotency_key='budgeted-single-sol:'||r.id::text||':'||x.instrument_id::text))"
_MONITOR_MANIFEST = """ OR (r.contract_version=6 AND NOT EXISTS (
 SELECT FROM research_request_members x JOIN agent_research_runs a ON a.id=x.run_id
 JOIN agent_daily_budget_reservations b ON b.run_id=a.id
 JOIN single_sol_budget_scopes g ON g.budget_day=b.business_date
 JOIN research_stage_route rt ON rt.run_id=a.id AND rt.job_id=x.job_id
 JOIN data_snapshots s ON s.id=x.data_snapshot_id
 JOIN agent_paper_candidate_batches bs ON bs.id=r.candidate_batch_id
 WHERE x.manifest_id=m.id AND m.member_count=1 AND x.ordinal=0
 AND rt.kind='independent' AND rt.scope_id=r.id AND rt.platform_binding IS NOT NULL
 AND g.account_id=r.account_id AND g.batch_id=r.candidate_batch_id AND g.model_id=r.model_config_id
 AND g.budget_day::text=r.research_profile#>>'{single_sol,budget_day}'
 AND g.calendar_id::text=r.research_profile#>>'{single_sol,calendar_id}'
 AND single_sol_day_context_hash(g.budget_day,g.account_id,g.batch_id,g.model_id,
     g.calendar_id,g.slot,g.daily_limit,g.maximum)=r.research_profile#>>'{single_sol,protection_context_hash}'
 AND x.instrument_id::text=r.research_profile#>>'{single_sol,instrument_id}'
 AND bs.account_id=r.account_id AND x.instrument_id=ANY(bs.candidate_instrument_ids)
 AND b.reserved_usd=a.reserved_cost_usd AND b.pricing_version=a.pricing_version
 AND a.actor_id='local-single-monitor-sol-v1'
 AND a.idempotency_key='single-monitor-sol:'||r.id::text||':'||x.instrument_id::text
 AND s.source='nexus_independent_monitor_research' AND s.schema_version='independent_monitor_research_v1'
 AND s.payload#>>'{independent_monitor_context,request_id}'=r.id::text
 AND s.payload#>>'{independent_monitor_context,request_hash}'=r.request_hash
 AND s.payload#>>'{independent_monitor_context,account_id}'=r.account_id::text
 AND s.payload#>>'{independent_monitor_context,candidate_batch_id}'=r.candidate_batch_id::text
 AND s.payload#>>'{independent_monitor_context,batch_hash}'=r.batch_hash
 AND s.payload#>>'{independent_monitor_context,instrument_id}'=x.instrument_id::text
 AND s.payload#>'{independent_monitor_context,candidate_ids}'=to_jsonb(bs.candidate_instrument_ids)
 AND s.payload#>'{independent_monitor_context,execution_locked}'='true'::jsonb
 AND s.payload#>'{independent_monitor_context,formal_consumption_allowed}'='false'::jsonb))"""
_ROUTE_TAIL = (
    "AND r.idempotency_key='budgeted-single-sol:'||q.id::text||':'||m.instrument_id::text))"
)
_MONITOR_ROUTE_TAIL = (
    _ROUTE_TAIL[:-1]
    + """ OR
 (q.contract_version=6 AND q.mode='research_and_monitor' AND f.member_count=1 AND m.ordinal=0
 AND m.instrument_id::text=q.research_profile#>>'{single_sol,instrument_id}'
 AND r.actor_id='local-single-monitor-sol-v1'
 AND r.idempotency_key='single-monitor-sol:'||q.id::text||':'||m.instrument_id::text))"""
)
_EVIDENCE = "AND request.contract_version=3 AND request.mode='research_and_monitor'"
_MONITOR_EVIDENCE = """AND ((request.contract_version=3 AND request.mode='research_and_monitor') OR
                    (request.contract_version=6 AND request.mode='research_and_monitor'
                     AND NEW.instrument_id::text=request.research_profile#>>'{single_sol,instrument_id}'))"""

# Preserve the old constraints byte-for-byte at the semantic level on downgrade;
# the original v4/v5 policies are not widened by the new branch.
_LEGACY = """
 (contract_version=1 AND mode IN ('research_only','research_and_paper')) OR
 (contract_version=2 AND mode='research_only') OR
 (contract_version=3 AND mode='research_and_monitor') OR
 (contract_version=4 AND mode='research_only' AND
  (research_profile->>'runtime_mode'='responses'
   AND jsonb_typeof(research_profile->'single_sol')='object'
   AND research_profile->'single_sol' ?& ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline']
   AND research_profile->'single_sol' ?& ARRAY['history_policy','scheduled_proof','preparation_proof']
   AND research_profile#>>'{single_sol,history_policy}'='single_sol_original_acceptances_v1'
   AND COALESCE(research_profile#>>'{single_sol,scheduled_proof}','') ~ '^([0-9a-f]{64})?$'
   AND COALESCE(research_profile#>>'{single_sol,preparation_proof}','') ~ '^([0-9a-f]{64})?$'
   AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$') IS TRUE) OR
 (contract_version=5 AND mode='research_only' AND
  (research_profile->>'runtime_mode'='responses'
   AND jsonb_typeof(research_profile->'single_sol')='object'
   AND research_profile->'single_sol' ?& ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline',
     'history_policy','scheduled_proof','preparation_proof','admission_policy','protection_context_hash']
   AND research_profile#>>'{single_sol,history_policy}'='single_sol_original_acceptances_v1'
   AND research_profile#>>'{single_sol,admission_policy}'='shared_budget_v1'
   AND research_profile#>>'{single_sol,protection_context_hash}' ~ '^[0-9a-f]{64}$'
   AND COALESCE(research_profile#>>'{single_sol,scheduled_proof}','') ~ '^([0-9a-f]{64})?$'
   AND COALESCE(research_profile#>>'{single_sol,preparation_proof}','') ~ '^([0-9a-f]{64})?$'
   AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$') IS TRUE)
"""
_V6 = """(contract_version=6 AND mode='research_and_monitor' AND (
 research_profile->>'runtime_mode'='responses'
 AND jsonb_typeof(research_profile->'single_sol')='object'
 AND research_profile->'single_sol' ?& ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline',
   'history_policy','scheduled_proof','preparation_proof','admission_policy','protection_context_hash','authorization_scope']
 AND (research_profile->'single_sol')-ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline',
   'history_policy','scheduled_proof','preparation_proof','admission_policy','protection_context_hash','authorization_scope']='{}'::jsonb
 AND research_profile#>>'{single_sol,history_policy}'='single_monitor_original_acceptances_once_v1'
 AND research_profile#>>'{single_sol,authorization_scope}'='single_monitor_root_20261116_once_v1'
 AND research_profile#>>'{single_sol,admission_policy}'='shared_budget_v1'
 AND research_profile#>>'{single_sol,protection_context_hash}' ~ '^[0-9a-f]{64}$'
 AND COALESCE(research_profile#>>'{single_sol,scheduled_proof}','') ~ '^([0-9a-f]{64})?$'
 AND COALESCE(research_profile#>>'{single_sol,preparation_proof}','') ~ '^([0-9a-f]{64})?$'
 AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$'
 AND research_profile#>>'{single_sol,instrument_id}' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
 AND research_profile#>>'{single_sol,calendar_id}' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
 AND research_profile#>>'{single_sol,budget_day}' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
 AND jsonb_typeof(research_profile#>'{single_sol,deadline}')='string'
 ) IS TRUE)"""


def _replace(name: str, before: str, after: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:signature))"),
        {"signature": "public." + name + "()"},
    )
    if not isinstance(definition, str) or definition.count(before) != 1:
        raise RuntimeError("SINGLE_MONITOR_ONCE_PREDECESSOR_INVALID")
    op.get_bind().exec_driver_sql(definition.replace(before, after))


def _guards(*, forward: bool) -> None:
    for name, before, after in (
        ("nexus_research_manifest_complete", _OWNER, _MONITOR_OWNER),
        ("nexus_research_manifest_complete", _SELECTED, _MONITOR_SELECTED),
        ("nexus_research_manifest_complete", _PURPOSE, _MONITOR_PURPOSE),
        ("nexus_research_manifest_complete", _TAIL, _TAIL + _MONITOR_MANIFEST),
        ("research_stage_route_guard", _ROUTE_TAIL, _MONITOR_ROUTE_TAIL),
        ("nexus_validate_agent_snapshot_evidence", _EVIDENCE, _MONITOR_EVIDENCE),
    ):
        _replace(name, before if forward else after, after if forward else before)


def upgrade() -> None:
    prior = op.get_bind().scalar(
        text("""SELECT pg_get_expr(conbin,conrelid) FROM pg_constraint
      WHERE conrelid='public.manual_paper_requests'::regclass
      AND conname='ck_manual_paper_requests_contract_version_valid'""")
    )
    if not isinstance(prior, str):
        raise RuntimeError("SINGLE_MONITOR_ONCE_PREDECESSOR_INVALID")
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint(
        "contract_version_valid", "manual_paper_requests", f"({prior}) OR {_V6}"
    )
    op.execute("""CREATE UNIQUE INDEX uq_single_monitor_once_account
      ON public.manual_paper_requests(account_id) WHERE contract_version=6""")
    _guards(forward=True)


def downgrade() -> None:
    # A root is a permanent authorization fact even if no job has run. Fence
    # concurrent inserts before checking; never delete facts to enable rollback.
    op.execute("LOCK TABLE public.manual_paper_requests IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    op.execute("""DO $$ BEGIN
      IF EXISTS(SELECT FROM public.manual_paper_requests WHERE contract_version=6)
      THEN RAISE EXCEPTION 'SINGLE_MONITOR_ONCE_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    _guards(forward=False)
    op.execute("DROP INDEX public.uq_single_monitor_once_account")
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint("contract_version_valid", "manual_paper_requests", _LEGACY)
