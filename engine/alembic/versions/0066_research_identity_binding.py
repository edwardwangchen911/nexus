"""Recognize receipt-bound research identity evidence without execution authority.

Revision ID: 0066_research_identity_binding
Revises: 0065_portfolio_management
"""

from alembic import op
from sqlalchemy import text

revision = "0066_research_identity_binding"
down_revision = "0065_portfolio_management"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS binding
                JOIN public.data_artifacts AS canonical
                  ON canonical.sha256 = binding.revision
                JOIN public.data_snapshots AS identity
                  ON identity.id::text = binding.payload->>'identity_snapshot_id'
                WHERE NEW.source = 'research_current_identity'
                  AND binding.source = NEW.source
                  AND binding.schema_version = 'research_identity_binding_v1'
                  AND binding.provider_object_id = NEW.provider_object_id
                  AND binding.provider_object_id = NEW.instrument_id::text
                  AND binding.revision = NEW.revision
                  AND binding.revision = NEW.content_hash
                  AND binding.payload->>'schema_version' = binding.schema_version
                  AND binding.payload->>'instrument_id' = NEW.instrument_id::text
                  AND binding.payload->>'policy_id' = 'research_current_identity_session_v1'
                  AND binding.payload->>'purpose' = 'research_issuer_security_binding_only'
                  AND binding.payload->'execution_locked' = 'true'::jsonb
                  AND binding.payload->'price_authority_eligible' = 'false'::jsonb
                  AND binding.payload->'risk_authority_eligible' = 'false'::jsonb
                  AND binding.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND binding.payload->>'decision_session' =
                    (snapshot_time AT TIME ZONE 'America/New_York')::date::text
                  AND binding.available_at = NEW.available_at
                  AND (binding.payload->>'available_at')::timestamptz = binding.available_at
                  AND binding.observed_at = binding.available_at
                  AND binding.received_at = binding.available_at
                  AND binding.created_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND canonical.source = NEW.source
                  AND canonical.schema_version = binding.schema_version
                  AND canonical.uri = NEW.location
                  AND canonical.byte_size = authority.byte_length
                  AND canonical.byte_size BETWEEN 1 AND 32768
                  AND canonical.created_at <= snapshot_time
                  AND canonical.observed_at = binding.available_at
                  AND canonical.received_at = binding.available_at
                  AND canonical.available_at = binding.available_at
                  AND identity.source = binding.payload->>'identity_source'
                  AND identity.source IN ('current_instrument_identity', 'current_listing_revision')
                  AND identity.revision = binding.payload->>'identity_revision'
                  AND identity.payload->>'instrument_id' = NEW.instrument_id::text
                  AND identity.created_at <= snapshot_time
                  AND identity.available_at <= snapshot_time
                  AND identity.observed_at <= snapshot_time
                  AND identity.received_at <= snapshot_time
                  AND jsonb_array_length(binding.payload->'source_received_at') = 2
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements_text(
                      binding.payload->'source_received_at') AS receipt(value)
                    WHERE value::timestamptz > snapshot_time
                      OR (value::timestamptz AT TIME ZONE 'America/New_York')::date
                        <> (snapshot_time AT TIME ZONE 'America/New_York')::date
                  )
                  AND jsonb_array_length(binding.payload->'identity_artifacts') = 4
                  AND jsonb_array_length(binding.payload->'source_artifacts') = 4
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(
                      (binding.payload->'identity_artifacts') ||
                      (binding.payload->'source_artifacts')) AS original(value)
                    WHERE NOT EXISTS (
                      SELECT 1 FROM public.data_artifacts AS artifact
                      WHERE artifact.sha256 = original.value->>'sha256'
                        AND artifact.uri = original.value->>'uri'
                        AND artifact.source = original.value->>'source'
                        AND artifact.schema_version = original.value->>'schema_version'
                        AND artifact.byte_size = (original.value->>'byte_size')::bigint
                        AND artifact.created_at <= snapshot_time
                        AND artifact.available_at <= snapshot_time
                        AND artifact.received_at <= snapshot_time
                        AND artifact.observed_at <= snapshot_time
                    )
                  )
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def _replace(old: str, new: str) -> None:
    if op.get_context().as_sql:
        op.execute(f"""
            DO $migration$
            DECLARE definition text;
              old_fragment text := $old${old}$old$;
              new_fragment text := $new${new}$new$;
            BEGIN
              SELECT pg_get_functiondef(
                'public.nexus_validate_agent_snapshot_evidence()'::regprocedure)
                INTO definition;
              IF definition IS NULL OR
                (length(definition) - length(replace(definition, old_fragment, '')))
                  / length(old_fragment) <> 1 THEN
                RAISE EXCEPTION 'research identity migration requires the expected ownership guard';
              END IF;
              EXECUTE replace(definition, old_fragment, new_fragment);
            END;
            $migration$;
        """)
        return
    connection = op.get_bind()
    definition = connection.scalar(
        text(
            "SELECT pg_get_functiondef("
            "'public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("research identity migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_context().as_sql:
        op.execute("""
            DO $migration$ BEGIN
              IF EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence
                         WHERE source = 'research_current_identity') THEN
                RAISE EXCEPTION 'cannot remove ownership support for frozen research identity';
              END IF;
            END; $migration$;
        """)
        _replace(_NEW, _OLD)
        return
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence "
            "WHERE source = 'research_current_identity')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen research identity")
    _replace(_NEW, _OLD)
