"""Bind current issuer distribution observations to their exact research owner.

Revision ID: 0035_issuer_research_evidence
Revises: 0034_capability_versions
"""

from alembic import op
from sqlalchemy import text

revision = "0035_issuer_research_evidence"
down_revision = "0034_capability_versions"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS observation
                JOIN public.data_artifacts AS raw
                  ON raw.sha256 = observation.payload->>'raw_content_hash'
                JOIN public.data_artifacts AS identity_raw
                  ON identity_raw.sha256 = observation.payload->>'identity_content_hash'
                WHERE NEW.source = 'ssga_distribution'
                  AND observation.source = NEW.source
                  AND observation.schema_version = 'issuer_distribution_v1'
                  AND observation.provider_object_id = NEW.provider_object_id
                  AND observation.revision = NEW.revision
                  AND observation.revision = NEW.content_hash
                  AND observation.payload->>'instrument_id' = NEW.instrument_id::text
                  AND observation.payload->>'observation_kind' = 'issuer_historical_distribution'
                  AND observation.payload->>'currency' = 'USD'
                  AND observation.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND observation.payload->'source_reported_available_at' = 'null'::jsonb
                  AND (observation.payload->>'available_at')::timestamptz = NEW.available_at
                  AND observation.available_at = NEW.available_at
                  AND observation.received_at = NEW.available_at
                  AND observation.created_at <= snapshot_time
                  AND observation.received_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND raw.source = 'ssga' AND identity_raw.source = 'ssga'
                  AND raw.byte_size = (observation.payload->>'raw_byte_length')::bigint
                  AND identity_raw.byte_size = (observation.payload->>'identity_byte_length')::bigint
                  AND raw.created_at <= snapshot_time AND identity_raw.created_at <= snapshot_time
                  AND raw.received_at <= observation.received_at
                  AND identity_raw.received_at <= observation.received_at
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def _replace(old: str, new: str) -> None:
    connection = op.get_bind()
    definition = connection.scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("issuer evidence migration requires the expected ownership guard")
    # Preserve the complete existing price/owner checks and hardened search_path.
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence "
            "WHERE source = 'ssga_distribution')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen issuer evidence")
    _replace(_NEW, _OLD)
