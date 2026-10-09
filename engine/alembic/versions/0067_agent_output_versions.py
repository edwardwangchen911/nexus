"""Align the output-binding version gate with formal and isolated cycle contracts.

Revision ID: 0067_agent_output_versions
Revises: 0066_research_identity_binding
"""

from alembic import op
from sqlalchemy import text

revision = "0067_agent_output_versions"
down_revision = "0066_research_identity_binding"
branch_labels = None
depends_on = None

_OLD = "run_version_ref IS DISTINCT FROM 'codex-sdk-chatgpt-v4-actionable'"
# Frozen migration contract; do not import mutable application version sets.
_NEW = """NOT (
               run_version_ref IN (
                 'codex-sdk-chatgpt-v4-actionable',
                 'codex-sdk-chatgpt-v5-paper-scope',
                 'codex-sdk-chatgpt-v6-stage-time-budget'
               ) OR (
                 run_version_ref = 'codex-sdk-chatgpt-v7-stage-capacity-candidate'
                 AND current_database() ~ '^nexus_(test|diag)_'
                 AND EXISTS (
                   SELECT 1 FROM public.strategy_decisions decision
                   JOIN public.agent_paper_cycles cycle
                     ON cycle.id = decision.agent_paper_cycle_id
                   JOIN public.agent_paper_cycle_events prepared
                     ON prepared.cycle_id = cycle.id AND prepared.sequence = 1
                   JOIN public.agent_research_outputs output
                     ON output.id = NEW.agent_research_output_id
                   JOIN public.agent_research_runs run ON run.id = output.run_id
                   JOIN public.agent_model_configs model ON model.id = run.model_config_id
                   WHERE decision.id = NEW.strategy_decision_id
                     AND cycle.candidate_batch_id IS NOT NULL
                     AND cycle.execution_locked = true
                     AND prepared.result_code = 'CYCLE_PREPARED'
                     AND prepared.fact_ids->'candidate_stage_capacity' = 'true'::jsonb
                     AND prepared.fact_ids->>'candidate_model_config_id' = model.id::text
                     AND prepared.fact_ids->>'candidate_model_hash' = model.config_hash
                     AND run.actor_id = 'local-agent-paper-cycle'
                     AND run.idempotency_key = 'agent-paper:' || cycle.id::text
                         || ':' || run.instrument_id::text
                     AND run.status = 'completed' AND run.retry_count = 0
                     AND run.decision_time = cycle.decision_time
                     AND run.pricing_version = model.version
                     AND run.model_hash = model.config_hash
                     AND model.version = 'codex-sdk-chatgpt-v7-stage-capacity-candidate'
                     AND model.model_id = 'gpt-5.6-terra'
                     AND model.config_hash =
                       '175ef8cf95fbafb050f89b4bb7f16aae278c8c9b99ee25c60cf35f044688141f'
                     AND model.max_input_tokens = 96000
                     AND model.max_output_tokens = 8192 AND model.max_tool_calls = 0
                     AND model.input_usd_per_million = 0 AND model.output_usd_per_million = 0
                     AND model.pricing_available_at = '2026-09-02T00:00:00Z'::timestamptz
                     AND model.pricing_expires_at = '2027-09-02T00:00:00Z'::timestamptz
                     AND model.created_at <= cycle.decision_time
                     AND model.pricing_available_at <= cycle.decision_time
                     AND cycle.decision_time < model.pricing_expires_at
                 )
               )
             )"""


def _replace(old: str, new: str) -> None:
    if op.get_context().as_sql:
        op.execute(f"""
            DO $migration$
            DECLARE definition text;
              old_fragment text := $old${old}$old$;
              new_fragment text := $new${new}$new$;
            BEGIN
              SELECT pg_get_functiondef(
                'public.nexus_agent_output_binding_valid()'::regprocedure) INTO definition;
              IF definition IS NULL OR
                (length(definition) - length(replace(definition, old_fragment, '')))
                  / length(old_fragment) <> 1 THEN
                RAISE EXCEPTION 'AGENT_OUTPUT_VERSION_GUARD_UNEXPECTED';
              END IF;
              EXECUTE replace(definition, old_fragment, new_fragment);
            END;
            $migration$;
        """)
        return
    connection = op.get_bind()
    definition = connection.execute(
        text("SELECT pg_get_functiondef('nexus_agent_output_binding_valid()'::regprocedure)")
    ).scalar_one()
    if definition.count(old) != 1:
        raise RuntimeError("AGENT_OUTPUT_VERSION_GUARD_UNEXPECTED")
    connection.execute(text(definition.replace(old, new)))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    _replace(_NEW, _OLD)
