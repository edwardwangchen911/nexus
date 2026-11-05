"""Version explicit independent owners without duplicating daily protection or credits."""

from alembic import op
from sqlalchemy import text

revision = "0117_budgeted_single_sol"
down_revision = "0116_single_sol_request"
branch_labels = None
depends_on = None


def _replace(name: str, before: str, after: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:name))"), {"name": "public." + name + "()"}
    )
    if definition is None or definition.count(before) != 1:
        raise RuntimeError("BUDGETED_SINGLE_SOL_PREDECESSOR_INVALID")
    op.execute(definition.replace(before, after))


_MANIFEST_TAIL = "AND a.idempotency_key='single-sol:'||r.id::text||':'||x.instrument_id::text))"
_ADDITIONAL_OWNER = """ OR (r.contract_version=5 AND NOT EXISTS (
 SELECT FROM research_request_members x JOIN agent_research_runs a ON a.id=x.run_id
 JOIN agent_daily_budget_reservations b ON b.run_id=a.id
 JOIN single_sol_budget_scopes g ON g.budget_day=b.business_date
 JOIN research_stage_route rt ON rt.run_id=a.id AND rt.job_id=x.job_id
 WHERE x.manifest_id=m.id AND m.member_count=1 AND x.ordinal=0
 AND rt.kind='independent' AND rt.scope_id=r.id AND rt.platform_binding IS NOT NULL
 AND g.account_id=r.account_id AND g.batch_id=r.candidate_batch_id AND g.model_id=r.model_config_id
 AND g.budget_day::text=r.research_profile#>>'{single_sol,budget_day}'
 AND g.calendar_id::text=r.research_profile#>>'{single_sol,calendar_id}'
 AND single_sol_day_context_hash(g.budget_day,g.account_id,g.batch_id,g.model_id,
     g.calendar_id,g.slot,g.daily_limit,g.maximum)=r.research_profile#>>'{single_sol,protection_context_hash}'
 AND x.instrument_id::text=r.research_profile#>>'{single_sol,instrument_id}'
 AND b.reserved_usd=a.reserved_cost_usd AND b.pricing_version=a.pricing_version
 AND a.actor_id='local-budgeted-single-sol-v1'
 AND a.idempotency_key='budgeted-single-sol:'||r.id::text||':'||x.instrument_id::text))"""
_ROUTE_TAIL = "AND r.idempotency_key='single-sol:'||q.id::text||':'||m.instrument_id::text))"
_NEW_ROUTE_TAIL = (
    _ROUTE_TAIL[:-1]
    + """ OR
 (q.contract_version=5 AND q.mode='research_only' AND f.member_count=1
 AND m.instrument_id::text=q.research_profile#>>'{single_sol,instrument_id}'
 AND r.actor_id='local-budgeted-single-sol-v1'
 AND r.idempotency_key='budgeted-single-sol:'||q.id::text||':'||m.instrument_id::text))"""
)


def upgrade() -> None:
    op.execute("""CREATE FUNCTION single_sol_day_context_hash(
      day date,account uuid,batch uuid,model uuid,calendar uuid,slot timestamptz,
      daily_limit numeric,maximum numeric) RETURNS text LANGUAGE sql IMMUTABLE STRICT
      SET search_path=pg_catalog,public AS $$
      SELECT encode(sha256(convert_to(concat_ws('|','single-sol-day-protection-v1',
        to_char(day,'YYYY-MM-DD'),account::text,batch::text,model::text,calendar::text,
        to_char(slot AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        trim_scale(daily_limit)::text,trim_scale(maximum)::text),'UTF8')),'hex')
      $$""")
    prior = op.get_bind().scalar(
        text("""SELECT pg_get_expr(conbin,conrelid) FROM pg_constraint
      WHERE conrelid='manual_paper_requests'::regclass AND conname='ck_manual_paper_requests_contract_version_valid'""")
    )
    if prior is None:
        raise RuntimeError("BUDGETED_SINGLE_SOL_PREDECESSOR_INVALID")
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint(
        "contract_version_valid",
        "manual_paper_requests",
        prior
        + """ OR
      (contract_version=5 AND mode='research_only' AND (
       research_profile->>'runtime_mode'='responses'
       AND jsonb_typeof(research_profile->'single_sol')='object'
       AND research_profile->'single_sol' ?& ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline',
         'history_policy','scheduled_proof','preparation_proof','admission_policy','protection_context_hash']
       AND research_profile#>>'{single_sol,history_policy}'='single_sol_original_acceptances_v1'
       AND research_profile#>>'{single_sol,admission_policy}'='shared_budget_v1'
       AND research_profile#>>'{single_sol,protection_context_hash}' ~ '^[0-9a-f]{64}$'
       AND COALESCE(research_profile#>>'{single_sol,scheduled_proof}','') ~ '^([0-9a-f]{64})?$'
       AND COALESCE(research_profile#>>'{single_sol,preparation_proof}','') ~ '^([0-9a-f]{64})?$'
       AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$') IS TRUE)""",
    )
    _replace(
        "nexus_research_manifest_complete",
        "(r.contract_version IN (2,4) AND r.mode='research_only')",
        "(r.contract_version IN (2,4,5) AND r.mode='research_only')",
    )
    _replace(
        "nexus_research_manifest_complete",
        "CASE WHEN r.contract_version=4 THEN",
        "CASE WHEN r.contract_version IN (4,5) THEN",
    )
    _replace("nexus_research_manifest_complete", _MANIFEST_TAIL, _MANIFEST_TAIL + _ADDITIONAL_OWNER)
    _replace("research_stage_route_guard", _ROUTE_TAIL, _NEW_ROUTE_TAIL)


def downgrade() -> None:
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT FROM manual_paper_requests WHERE contract_version=5)
      THEN RAISE EXCEPTION 'BUDGETED_SINGLE_SOL_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    _replace("research_stage_route_guard", _NEW_ROUTE_TAIL, _ROUTE_TAIL)
    _replace("nexus_research_manifest_complete", _MANIFEST_TAIL + _ADDITIONAL_OWNER, _MANIFEST_TAIL)
    _replace(
        "nexus_research_manifest_complete",
        "CASE WHEN r.contract_version IN (4,5) THEN",
        "CASE WHEN r.contract_version=4 THEN",
    )
    _replace(
        "nexus_research_manifest_complete",
        "(r.contract_version IN (2,4,5) AND r.mode='research_only')",
        "(r.contract_version IN (2,4) AND r.mode='research_only')",
    )
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint(
        "contract_version_valid",
        "manual_paper_requests",
        """
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
       AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$') IS TRUE)""",
    )
    op.execute(
        "DROP FUNCTION single_sol_day_context_hash(date,uuid,uuid,uuid,uuid,timestamptz,numeric,numeric)"
    )
