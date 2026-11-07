"""Bind qualified native Sol formal outputs without changing historical guards.

No historical rows change. Retain this reader schema once a native decision
binding exists; application rollback uses a schema-compatible maintenance image.
"""

from alembic import op
from sqlalchemy import text

revision = "0120_sol_formal_output_binding"
down_revision = "0119_compact_budget_obligations"
branch_labels = None
depends_on = None

_OLD = "OR NOT (\n               run_version_ref IN ("
_NEW = """OR NOT (
               public.nexus_sol_formal_output_binding_valid(
                 NEW.strategy_decision_id, NEW.agent_research_output_id) OR
               run_version_ref IN ("""


def _replace(before: str, after: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef('public.nexus_agent_output_binding_valid()'::regprocedure)")
    )
    if not isinstance(definition, str) or definition.count(before) != 1:
        raise RuntimeError("SOL_OUTPUT_BINDING_PREDECESSOR_INVALID")
    op.execute(definition.replace(before, after))


def upgrade() -> None:
    op.execute("""
    CREATE FUNCTION public.nexus_sol_formal_output_binding_valid(decision_id uuid, output_id uuid)
    RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
      SELECT EXISTS (
        SELECT 1 FROM public.strategy_decisions d
        JOIN public.agent_paper_cycles c ON c.id=d.agent_paper_cycle_id
        JOIN public.agent_paper_cycle_events e ON e.cycle_id=c.id AND e.sequence=1
        JOIN public.agent_research_outputs o ON o.id=output_id
        JOIN public.agent_research_runs r ON r.id=o.run_id
        JOIN public.agent_model_configs m ON m.id=r.model_config_id
        JOIN public.research_stage_route route ON route.run_id=r.id
        JOIN public.jobs j ON j.id=route.job_id
        WHERE d.id=decision_id AND d.decision_origin='agent_research'
          AND c.candidate_batch_id IS NOT NULL AND c.execution_locked=true
          AND e.result_code='CYCLE_PREPARED'
          AND e.fact_ids->'formal_stage_capacity'='true'::jsonb
          AND e.fact_ids->>'formal_model_config_id'=m.id::text
          AND e.fact_ids->>'formal_model_hash'=m.config_hash
          AND r.actor_id='local-agent-paper-cycle'
          AND r.idempotency_key='agent-paper:'||c.id::text||':'||r.instrument_id::text
          AND r.status='completed' AND r.retry_count=0 AND o.status='completed'
          AND r.decision_time=c.decision_time
          AND r.pricing_version=m.version AND r.model_hash=m.config_hash
          AND m.platform_profile IS NOT NULL AND public.nexus_sol_model_profile_valid(m)
          AND m.created_at<=c.decision_time AND m.pricing_available_at<=c.decision_time
          AND c.decision_time<m.pricing_expires_at
          AND route.kind='formal' AND route.scope_id=c.id
          AND route.version='research-stages-v1' AND route.platform_binding IS NOT NULL
          AND public.nexus_sol_native_route_valid(route)
          AND j.job_type='agent.paper.research' AND j.payload->>'run_id'=r.id::text
      )
    $$
    """)
    _replace(_OLD, _NEW)


def downgrade() -> None:
    # Serialize with output-binding writers before checking for persisted use.
    op.execute("LOCK TABLE strategy_decision_agent_outputs IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    op.execute("""DO $$ BEGIN
      IF EXISTS (
        SELECT 1 FROM strategy_decision_agent_outputs b
        JOIN agent_research_outputs o ON o.id=b.agent_research_output_id
        JOIN agent_research_runs r ON r.id=o.run_id
        JOIN agent_model_configs m ON m.id=r.model_config_id
        WHERE m.platform_profile IS NOT NULL
      ) THEN RAISE EXCEPTION 'SOL_OUTPUT_BINDING_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    _replace(_NEW, _OLD)
    op.execute("DROP FUNCTION public.nexus_sol_formal_output_binding_valid(uuid,uuid)")
