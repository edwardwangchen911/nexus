"""Bind current issuer fund observations to their exact research owner.

Revision ID: 0038_issuer_fund_observations
Revises: 0037_strategy_rank_checkpoints
"""

from alembic import op
from sqlalchemy import text

revision = "0038_issuer_fund_observations"
down_revision = "0037_strategy_rank_checkpoints"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS observation
                JOIN public.data_artifacts AS raw
                  ON raw.sha256 = observation.payload->>'workbook_content_hash'
                JOIN public.data_artifacts AS identity_raw
                  ON identity_raw.sha256 = observation.payload->>'html_content_hash'
                WHERE NEW.source = 'ssga_fund_observation'
                  AND observation.source = NEW.source
                  AND observation.schema_version = 'issuer_fund_observation_v1'
                  AND observation.provider_object_id = NEW.provider_object_id
                  AND observation.revision = NEW.revision
                  AND observation.revision = NEW.content_hash
                  AND observation.payload->>'instrument_id' = NEW.instrument_id::text
                  AND observation.payload->>'classification_level' = 'industry'
                  AND observation.payload->>'weight_unit' = 'percent'
                  AND observation.payload->'sector_risk_eligible' = 'false'::jsonb
                  AND observation.payload->'denominator' = 'null'::jsonb
                  AND observation.payload->'valid_until' = 'null'::jsonb
                  AND observation.payload->'residual_classification' = 'null'::jsonb
                  AND observation.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND observation.payload->'source_published_at' = 'null'::jsonb
                  AND (observation.payload->>'available_at')::timestamptz = NEW.available_at
                  AND observation.available_at = NEW.available_at
                  AND observation.received_at = NEW.available_at
                  AND observation.created_at <= snapshot_time
                  AND observation.received_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND raw.source = 'ssga' AND identity_raw.source = 'ssga'
                  AND raw.byte_size = (observation.payload->>'workbook_byte_length')::bigint
                  AND identity_raw.byte_size = (observation.payload->>'html_byte_length')::bigint
                  AND raw.created_at <= snapshot_time AND identity_raw.created_at <= snapshot_time
                  AND raw.received_at <= (observation.payload->>'workbook_received_at')::timestamptz
                  AND identity_raw.received_at <= (observation.payload->>'html_received_at')::timestamptz
                  AND observation.available_at = greatest(
                    (observation.payload->>'workbook_received_at')::timestamptz,
                    (observation.payload->>'html_received_at')::timestamptz
                  )
                  AND raw.available_at <= observation.available_at
                  AND identity_raw.available_at <= observation.available_at
                  AND raw.schema_version = 'issuer_raw_v1'
                  AND identity_raw.schema_version = 'issuer_raw_v1' 
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
            "WHERE source = 'ssga_fund_observation')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen issuer evidence")
    _replace(_NEW, _OLD)
