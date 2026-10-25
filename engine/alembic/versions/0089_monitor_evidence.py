"""Recognize only instrument-bound immutable monitor context evidence.

Revision ID: 0089_monitor_evidence
Revises: 0088_monitor_increments
"""

from alembic import op
from sqlalchemy import text

revision = "0089_monitor_evidence"
down_revision = "0088_monitor_increments"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots binding
                JOIN public.research_monitor_triggers trigger
                  ON trigger.id::text = NEW.provider_object_id
                JOIN public.research_monitor_checks checked ON checked.id = trigger.check_id
                JOIN public.research_monitor_plans plan ON plan.id = checked.plan_id
                WHERE NEW.source = 'nexus_price_monitor_increment'
                  AND binding.id = NEW.data_snapshot_id
                  AND binding.source = NEW.source
                  AND binding.schema_version = 'price_monitor_increment_v1'
                  AND binding.provider_object_id = NEW.instrument_id::text
                  AND binding.payload->>'purpose' = binding.schema_version
                  AND binding.payload->'monitor_context'->>'purpose' = binding.schema_version
                  AND binding.payload->'monitor_context'->>'instrument_id' = NEW.instrument_id::text
                  AND binding.payload->'monitor_context'->>'account_id' = plan.account_id::text
                  AND binding.payload->'monitor_context'->>'plan_id' = plan.id::text
                  AND binding.payload->'monitor_context'->>'parent_run_id' = plan.source_run_id::text
                  AND binding.payload->'monitor_context'->>'parent_output_id' = plan.source_output_id::text
                  AND binding.payload->'monitor_context'->>'parent_output_hash' = plan.source_hash
                  AND binding.payload->'monitor_context'->'execution_locked' = 'true'::jsonb
                  AND binding.payload->'monitor_context'->'trigger_checks'->0->>'trigger_id' = trigger.id::text
                  AND binding.payload->'monitor_context'->'trigger_checks'->0->>'check_id' = checked.id::text
                  AND binding.payload->'monitor_context'->'trigger_checks'->0->'observation' = checked.observation
                  AND plan.instrument_id = NEW.instrument_id
                  AND trigger.instrument_id = NEW.instrument_id
                  AND trigger.account_id = plan.account_id AND checked.result = 'true'
                  AND checked.checked_at <= snapshot_time
                  AND binding.created_at <= snapshot_time
                  AND binding.observed_at = NEW.available_at
                  AND binding.available_at = NEW.available_at
                  AND binding.received_at = NEW.available_at
                  AND NEW.revision = NEW.content_hash
                  AND NEW.location = 'memory://sha256/' || NEW.content_hash
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def replace(old, new):
    if op.get_context().as_sql:
        op.execute(f"""DO $migration$ DECLARE definition text;
        BEGIN SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure) INTO definition;
        IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$) <> 1
        THEN RAISE EXCEPTION 'MONITOR_OWNERSHIP_GUARD_UNEXPECTED'; END IF;
        EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")
        return
    definition = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("MONITOR_OWNERSHIP_GUARD_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))


def upgrade():
    replace(_OLD, _NEW)


def downgrade():
    op.execute("LOCK TABLE agent_data_snapshot_evidence IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM agent_data_snapshot_evidence WHERE source='nexus_price_monitor_increment') THEN RAISE EXCEPTION 'MONITOR_EVIDENCE_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    replace(_NEW, _OLD)
