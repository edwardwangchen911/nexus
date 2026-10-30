"""Select exact experimental owners in the existing stage protocol, without dispatch.

Old business predicates stay intact. A populated experimental scope requires a
compatible reader rollback; evidence is never removed to downgrade.
"""

from alembic import op
from sqlalchemy import text

revision = "0102_comparison_routes"
down_revision = "0101_comparison_inputs"
branch_labels = None
depends_on = None

_ROUTE_OLD = "AND i.run_id=r.id AND i.job_id=j.id))))"
_ROUTE_NEW = """AND i.run_id=r.id AND i.job_id=j.id))
            OR (NEW.kind='comparison' AND s.comparison_id=NEW.scope_id AND NOT s.stopped
              AND research_comparison_binding(r.id,j.id,s.comparison_id))))"""
_KIND_OLD = "CASE WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END"
_KIND_NEW = """CASE WHEN s.comparison_id IS NOT NULL THEN 'comparison'
              WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END"""
_STOP_OLD = "OR NEW.request_id IS DISTINCT FROM OLD.request_id"
_STOP_NEW = _STOP_OLD + "\n        OR NEW.comparison_id IS DISTINCT FROM OLD.comparison_id"
_LOCK_OLD = "BEGIN\n          IF NOT EXISTS(SELECT 1 FROM jobs j"
_LOCK_NEW = """BEGIN
          IF NEW.kind='comparison' THEN
            PERFORM 1 FROM research_stage_scope WHERE id=NEW.scope_id FOR UPDATE;
          END IF;
          IF NOT EXISTS(SELECT 1 FROM jobs j"""


def replace_guard(name: str, old: str, new: str) -> None:
    op.execute(f"""DO $migration$ DECLARE definition text;
      BEGIN SELECT pg_get_functiondef('public.{name}()'::regprocedure) INTO definition;
      IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$)<>1
      THEN RAISE EXCEPTION 'COMPARISON_ROUTE_GUARD_UNEXPECTED'; END IF;
      EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")


def upgrade() -> None:
    op.execute("""ALTER TABLE research_stage_scope
      ADD COLUMN comparison_id uuid REFERENCES research_comparison_manifest(id),
      DROP CONSTRAINT research_stage_scope_check,
      DROP CONSTRAINT research_stage_scope_check1,
      ADD CONSTRAINT research_stage_scope_check CHECK(
        num_nonnulls(cycle_id,increment_id,request_id,comparison_id)=1),
      ADD CONSTRAINT research_stage_scope_check1 CHECK(
        id=coalesce(cycle_id,increment_id,request_id,comparison_id))""")
    op.execute("""ALTER TABLE research_stage_route DROP CONSTRAINT research_stage_route_kind_check,
      ADD CONSTRAINT research_stage_route_kind_check
        CHECK(kind IN ('formal','independent','increment','comparison'))""")
    op.execute("""CREATE FUNCTION research_comparison_binding(run uuid,job uuid,experiment uuid)
      RETURNS boolean LANGUAGE sql STABLE AS $$
      SELECT EXISTS(
        SELECT 1 FROM research_comparison_chain c
        JOIN research_comparison_manifest m ON m.id=c.experiment_id
        JOIN agent_research_runs r ON r.id=c.run_id
        JOIN jobs j ON j.id=c.job_id
        JOIN agent_model_configs model ON model.id=r.model_config_id
        JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id AND b.run_id=r.id
        JOIN data_snapshots s ON s.id=r.data_snapshot_id
        CROSS JOIN LATERAL (SELECT (ARRAY[0,0,1,1,2,2,2,2,1,1,0,0])[c.ordinal] AS i) slot
        WHERE c.run_id=run AND c.job_id=job AND c.experiment_id=experiment
          AND m.purpose='direct-responses-terra-v11-20261030'
          AND m.payload->>'experiment_id'=m.id::text
          AND r.actor_id='research-comparison:direct-responses-terra-v11'
          AND c.input_hash=r.input_hash
          AND m.payload->'input_hashes'->>slot.i=c.input_hash
          AND m.payload->'instrument_ids'->>slot.i=r.instrument_id::text
          AND r.model_hash=model.config_hash AND r.pricing_version=model.version
          AND model.config_hash=m.payload->>CASE WHEN c.ordinal IN (1,3,5,8,10,12)
            THEN 'sdk_config_hash' ELSE 'api_config_hash' END
          AND model.model_id='gpt-5.6-terra' AND model.max_input_tokens=96000
          AND model.max_output_tokens=8192 AND model.max_tool_calls=0
          AND b.pricing_version=r.pricing_version AND b.reserved_usd=r.reserved_cost_usd
          AND r.idempotency_key='comparison:' || m.id::text || ':' || c.ordinal::text
          AND j.job_key=r.idempotency_key
          AND j.job_type='agent.research.comparison' AND j.schema_version='comparison-v1'
          AND j.queue='strategy_rnd' AND j.scope_kind='instrument'
          AND j.scope_id=r.instrument_id::text AND j.effective_at=r.decision_time
          AND j.payload=jsonb_build_object('run_id',r.id::text)
          AND j.retry_policy='never' AND j.max_attempts=1 AND j.manual_retry_count=0
          AND r.retry_count=0 AND s.source='nexus_research_comparison'
          AND s.schema_version='comparison-input-v1' AND s.payload->>'purpose'=m.purpose
          AND s.payload->>'experiment_id'=m.id::text
          AND s.payload->>'input_hash'=c.input_hash
          AND s.payload->>'source_manifest_hash'=m.payload->>'source_manifest_hash'
          AND s.payload->>'execution_locked'='true'
          AND s.payload->>'formal_consumption_allowed'='false'
          AND s.payload->'frozen'->'request'->>'data_snapshot_id'=s.id::text
          AND s.payload->'frozen'->'request'->>'instrument_id'=r.instrument_id::text
          AND (s.payload->'frozen'->'request'->>'decision_time')::timestamptz=r.decision_time
      ) $$""")
    replace_guard("research_stage_route_guard", _ROUTE_OLD, _ROUTE_NEW)
    replace_guard("research_stage_route_guard", _LOCK_OLD, _LOCK_NEW)
    replace_guard("research_stage_stop_guard", _STOP_OLD, _STOP_NEW)
    for name in ("research_stage_route_member_guard", "research_stage_root_authorize_guard"):
        replace_guard(name, _KIND_OLD, _KIND_NEW)


def downgrade() -> None:
    # Refuse before acquiring potentially conflicting table locks; then fence
    # preparation and recheck. NOWAIT cannot interrupt another active owner.
    query = text(
        "SELECT EXISTS(SELECT 1 FROM research_stage_scope WHERE comparison_id IS NOT NULL)"
    )
    if op.get_bind().scalar(query):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute(
        "LOCK TABLE jobs, research_stage_scope, research_stage_route IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    if op.get_bind().scalar(query):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    for name in ("research_stage_route_member_guard", "research_stage_root_authorize_guard"):
        replace_guard(name, _KIND_NEW, _KIND_OLD)
    replace_guard("research_stage_stop_guard", _STOP_NEW, _STOP_OLD)
    replace_guard("research_stage_route_guard", _ROUTE_NEW, _ROUTE_OLD)
    replace_guard("research_stage_route_guard", _LOCK_NEW, _LOCK_OLD)
    op.execute("DROP FUNCTION research_comparison_binding(uuid,uuid,uuid)")
    op.execute("""ALTER TABLE research_stage_route DROP CONSTRAINT research_stage_route_kind_check,
      ADD CONSTRAINT research_stage_route_kind_check
        CHECK(kind IN ('formal','independent','increment'))""")
    op.execute("""ALTER TABLE research_stage_scope
      DROP CONSTRAINT research_stage_scope_check,
      DROP CONSTRAINT research_stage_scope_check1,
      DROP COLUMN comparison_id,
      ADD CONSTRAINT research_stage_scope_check CHECK(num_nonnulls(cycle_id,increment_id,request_id)=1),
      ADD CONSTRAINT research_stage_scope_check1 CHECK(id=coalesce(cycle_id,increment_id,request_id))""")
