"""Version a single-stock, research-only Responses owner without changing old identities."""

from alembic import op
from sqlalchemy import text

revision = "0116_single_sol_request"
down_revision = "0115_single_sol_budget"
branch_labels = None
depends_on = None

_LEGACY = (
    "(contract_version=1 AND mode IN ('research_only','research_and_paper')) OR "
    "(contract_version=2 AND mode='research_only') OR "
    "(contract_version=3 AND mode='research_and_monitor')"
)
_ROUTE = "q.contract_version=3 AND q.mode='research_and_monitor'"
_SINGLE_ROUTE = """((q.contract_version=3 AND q.mode='research_and_monitor') OR
 (q.contract_version=4 AND q.mode='research_only' AND f.member_count=1
 AND m.instrument_id::text=q.research_profile#>>'{single_sol,instrument_id}'
 AND r.actor_id='local-single-sol-research-v1' AND r.idempotency_key='single-sol:'||q.id::text||':'||m.instrument_id::text))"""
_MANIFEST = "IS DISTINCT FROM (SELECT candidate_instrument_ids FROM agent_paper_candidate_batches WHERE id=r.candidate_batch_id)"
_SINGLE_MANIFEST = """IS DISTINCT FROM (SELECT CASE WHEN r.contract_version=4 THEN
 ARRAY[(r.research_profile#>>'{single_sol,instrument_id}')::uuid] ELSE candidate_instrument_ids END
 FROM agent_paper_candidate_batches WHERE id=r.candidate_batch_id)
 OR (r.contract_version=4 AND NOT EXISTS (
 SELECT FROM research_request_members x JOIN agent_research_runs a ON a.id=x.run_id
 JOIN single_sol_budget_scopes g ON g.source_run_id=x.run_id
 JOIN research_stage_route rt ON rt.run_id=a.id AND rt.job_id=x.job_id
 WHERE x.manifest_id=m.id AND m.member_count=1 AND x.ordinal=0
 AND rt.kind='independent' AND rt.scope_id=r.id AND rt.platform_binding IS NOT NULL
 AND g.selected_id=x.instrument_id AND g.account_id=r.account_id AND g.batch_id=r.candidate_batch_id
 AND g.model_id=r.model_config_id AND g.budget_day::text=r.research_profile#>>'{single_sol,budget_day}'
 AND g.calendar_id::text=r.research_profile#>>'{single_sol,calendar_id}'
 AND a.actor_id='local-single-sol-research-v1'
 AND a.idempotency_key='single-sol:'||r.id::text||':'||x.instrument_id::text))"""
_OLD_OWNER = "(r.contract_version=2 AND r.mode='research_only')"
_NEW_OWNER = "(r.contract_version IN (2,4) AND r.mode='research_only')"


def _replace(name: str, before: str, after: str) -> None:
    conn = op.get_bind()
    definition = conn.scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:name))"), {"name": "public." + name + "()"}
    )
    if definition is None or definition.count(before) != 1:
        raise RuntimeError("SINGLE_SOL_SCHEMA_PREDECESSOR_INVALID")
    op.execute(definition.replace(before, after))


def _versions(condition: str) -> None:
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint("contract_version_valid", "manual_paper_requests", condition)


def upgrade() -> None:
    # 0115 also has a separate maximum <= daily_limit check. Identify the
    # original slot constraint by its expression before replacing that one.
    op.execute("""DO $$ BEGIN IF NOT EXISTS (
        SELECT FROM pg_constraint WHERE conrelid='single_sol_budget_scopes'::regclass
        AND conname='single_sol_budget_scopes_check1'
        AND pg_get_constraintdef(oid) LIKE '%created_at < slot%'
    ) THEN RAISE EXCEPTION 'SINGLE_SOL_SCHEMA_PREDECESSOR_INVALID'; END IF; END $$""")
    op.execute(
        "ALTER TABLE single_sol_budget_scopes DROP CONSTRAINT single_sol_budget_scopes_check1"
    )
    op.execute("""ALTER TABLE single_sol_budget_scopes ADD CONSTRAINT single_sol_scope_day CHECK
        ((slot AT TIME ZONE 'UTC')::date=budget_day AND (created_at AT TIME ZONE 'UTC')::date=budget_day)""")
    op.execute("""CREATE FUNCTION single_sol_closed_slot_scope() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
        DECLARE members uuid[]; c uuid; n integer;
        BEGIN
          IF NEW.created_at<NEW.slot THEN RETURN NEW; END IF;
          SELECT candidate_instrument_ids INTO STRICT members FROM agent_paper_candidate_batches
            WHERE id=NEW.batch_id AND account_id=NEW.account_id;
          SELECT s.cycle_id INTO c FROM single_sol_budget_credits x
            JOIN agent_portfolio_synthesis_runs s ON s.id=x.run_id
            JOIN agent_portfolio_synthesis_budget_reservations b ON b.run_id=s.id
            WHERE x.budget_day=NEW.budget_day AND x.task='synthesis'
            AND s.status='completed' AND b.state='settled' AND b.settled_usd=s.settled_cost_usd;
          SELECT count(*) INTO n FROM single_sol_budget_credits x
            JOIN agent_research_runs r ON r.id=x.run_id
            JOIN agent_daily_budget_reservations b ON b.run_id=r.id
            WHERE x.budget_day=NEW.budget_day AND x.task='research:'||r.instrument_id::text
            AND r.instrument_id=ANY(members) AND r.status IN ('completed','abstained')
            AND b.state='settled' AND b.settled_usd=r.settled_cost_usd;
          IF c IS NULL OR n<>cardinality(members) OR NOT EXISTS (
            SELECT FROM idempotency_records t WHERE t.route='agent-paper-schwab-trigger-v2'
            AND t.actor_id='local-agent-paper-schwab:'||NEW.account_id::text
            AND t.response_payload->>'outcome'='completed' AND t.response_payload#>>'{result,cycle_id}'=c::text)
            OR COALESCE((SELECT e.to_status FROM agent_paper_cycle_events e WHERE e.cycle_id=c
                ORDER BY e.sequence DESC LIMIT 1),'') NOT IN ('completed','unavailable')
          THEN RAISE EXCEPTION 'SINGLE_SOL_ORIGINAL_SETTLEMENT_UNPROVEN'; END IF;
          RETURN NEW;
        END $$""")
    op.execute("""CREATE CONSTRAINT TRIGGER single_sol_post_slot_guard AFTER INSERT ON single_sol_budget_scopes
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION single_sol_closed_slot_scope()""")
    _versions(
        _LEGACY
        + """ OR (contract_version=4 AND mode='research_only' AND
      (research_profile->>'runtime_mode'='responses'
       AND jsonb_typeof(research_profile->'single_sol')='object'
       AND research_profile->'single_sol' ?& ARRAY['instrument_id','selection_hash','budget_day','calendar_id','deadline']
       AND research_profile->'single_sol' ?& ARRAY['history_policy','scheduled_proof','preparation_proof']
       AND research_profile#>>'{single_sol,history_policy}'='single_sol_original_acceptances_v1'
       AND COALESCE(research_profile#>>'{single_sol,scheduled_proof}','') ~ '^([0-9a-f]{64})?$'
       AND COALESCE(research_profile#>>'{single_sol,preparation_proof}','') ~ '^([0-9a-f]{64})?$'
       AND research_profile#>>'{single_sol,selection_hash}' ~ '^[0-9a-f]{64}$') IS TRUE)"""
    )
    _replace("nexus_research_manifest_complete", _OLD_OWNER, _NEW_OWNER)
    _replace("nexus_research_manifest_complete", _MANIFEST, _SINGLE_MANIFEST)
    _replace("research_stage_route_guard", _ROUTE, _SINGLE_ROUTE)


def downgrade() -> None:
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT FROM manual_paper_requests WHERE contract_version=4)
    THEN RAISE EXCEPTION 'SINGLE_SOL_REQUEST_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    op.execute("DROP TRIGGER single_sol_post_slot_guard ON single_sol_budget_scopes")
    op.execute("DROP FUNCTION single_sol_closed_slot_scope()")
    op.execute("ALTER TABLE single_sol_budget_scopes DROP CONSTRAINT single_sol_scope_day")
    op.execute("""ALTER TABLE single_sol_budget_scopes ADD CONSTRAINT single_sol_budget_scopes_check1 CHECK
        ((slot AT TIME ZONE 'UTC')::date=budget_day AND
         (created_at AT TIME ZONE 'UTC')::date=budget_day AND created_at<slot)""")
    _replace("research_stage_route_guard", _SINGLE_ROUTE, _ROUTE)
    _replace("nexus_research_manifest_complete", _SINGLE_MANIFEST, _MANIFEST)
    _replace("nexus_research_manifest_complete", _NEW_OWNER, _OLD_OWNER)
    _versions(_LEGACY)
