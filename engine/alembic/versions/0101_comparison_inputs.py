"""Bind experimental context evidence only to an exact registered input snapshot.

Historical business/price clauses remain byte-identical. Populated snapshots
require a compatible reader rollback; this migration does not enable dispatch.
"""

from alembic import op
from sqlalchemy import text

revision = "0101_comparison_inputs"
down_revision = "0100_research_comparison"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots s
                JOIN public.research_comparison_manifest m
                  ON m.id::text=s.payload->>'experiment_id'
                CROSS JOIN generate_series(0,2) AS slot(i)
                CROSS JOIN LATERAL (SELECT s.payload->'frozen'->'inputs'
                  ->'security_snapshot'->'independent_monitor_context' AS body) c
                WHERE NEW.source='nexus_independent_monitor_research'
                  AND NEW.authority_kind='research' AND NEW.selected_at IS NULL
                  AND s.id=NEW.data_snapshot_id
                  AND s.source='nexus_research_comparison'
                  AND s.schema_version='comparison-input-v1'
                  AND s.payload->>'purpose'=m.purpose
                  AND m.purpose='direct-responses-terra-v11-20261030'
                  AND s.payload->>'execution_locked'='true'
                  AND s.payload->>'formal_consumption_allowed'='false'
                  AND s.payload->>'source_manifest_hash'=m.payload->>'source_manifest_hash'
                  AND m.payload->'instrument_ids'->>slot.i=NEW.instrument_id::text
                  AND m.payload->'input_hashes'->>slot.i=s.payload->>'input_hash'
                  AND s.provider_object_id=m.id::text || ':' || NEW.instrument_id::text
                  AND s.payload->'frozen'->'request'->>'data_snapshot_id'=s.id::text
                  AND s.payload->'frozen'->'request'->>'instrument_id'=NEW.instrument_id::text
                  AND (s.payload->'frozen'->'request'->>'decision_time')::timestamptz=s.observed_at
                  AND s.created_at=s.received_at AND s.available_at=s.received_at
                  AND s.created_at>=s.observed_at
                  AND c.body->>'purpose'='independent_monitor_research_v1'
                  AND c.body->>'instrument_id'=NEW.instrument_id::text
                  AND c.body->>'execution_locked'='true'
                  AND c.body->>'formal_consumption_allowed'='false'
                  AND (c.body->>'decision_time')::timestamptz=s.observed_at
                  AND NEW.available_at=s.observed_at
                  AND NEW.provider_object_id=(c.body->>'request_id') || ':' || NEW.instrument_id::text
                  AND NEW.revision=NEW.content_hash
                  AND NEW.location='memory://sha256/' || NEW.content_hash
                  AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.payload->'frozen'->'inputs'->'security_snapshot'->'items') item
                    WHERE item->>'evidence_ref_id'=NEW.evidence_ref_id::text
                      AND item->>'source'=NEW.source
                      AND item->>'provider_object_id'=NEW.provider_object_id
                      AND item->>'revision'=NEW.revision
                      AND item->>'content_hash'=NEW.content_hash
                      AND (item->>'available_at')::timestamptz=NEW.available_at)
                  AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.payload->'frozen'->'evidence') ref
                    WHERE ref->>'source'=NEW.source AND ref->>'object_id'=NEW.provider_object_id
                      AND ref->>'location'=NEW.location AND ref->>'content_hash'=NEW.content_hash
                      AND (ref->>'available_at')::timestamptz=NEW.available_at)
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def replace_guard(old: str, new: str) -> None:
    op.execute(f"""DO $migration$ DECLARE definition text;
      BEGIN SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure) INTO definition;
      IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$)<>1
      THEN RAISE EXCEPTION 'COMPARISON_EVIDENCE_GUARD_UNEXPECTED'; END IF;
      EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")


def upgrade() -> None:
    replace_guard(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM data_snapshots WHERE source='nexus_research_comparison' OR schema_version='comparison-input-v1' OR payload->>'purpose'='direct-responses-terra-v11-20261030')"
        )
    ):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    replace_guard(_NEW, _OLD)
