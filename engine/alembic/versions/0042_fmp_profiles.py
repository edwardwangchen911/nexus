"""Bind receipt-backed FMP company context without granting financial authority.

Revision ID: 0042_fmp_profiles
Revises: 0041_action_observations
"""

from alembic import op
from sqlalchemy import text

revision = "0042_fmp_profiles"
down_revision = "0041_action_observations"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS profile
                JOIN public.data_artifacts AS canonical
                  ON canonical.sha256 = profile.revision
                JOIN public.data_artifacts AS raw
                  ON raw.sha256 = profile.payload->>'raw_content_hash'
                JOIN public.data_artifacts AS receipt
                  ON receipt.sha256 = profile.payload->>'receipt_content_hash'
                WHERE NEW.source = 'fmp_company_profile'
                  AND profile.source = NEW.source
                  AND profile.schema_version = 'fmp_company_profile_v1'
                  AND profile.provider_object_id = NEW.provider_object_id
                  AND profile.provider_object_id =
                    'profile:' || NEW.instrument_id::text || ':' || (profile.payload->>'symbol')
                  AND profile.revision = NEW.revision
                  AND profile.revision = NEW.content_hash
                  AND profile.payload->>'instrument_id' = NEW.instrument_id::text
                  AND profile.payload->>'revision_semantics' = 'local_content_hash'
                  AND profile.payload->'source_published_at' = 'null'::jsonb
                  AND profile.payload->'valid_until' = 'null'::jsonb
                  AND profile.payload->'sector_taxonomy' = 'null'::jsonb
                  AND profile.payload->'sector_risk_eligible' = 'false'::jsonb
                  AND profile.payload->'financial_fact_eligible' = 'false'::jsonb
                  AND profile.payload->'price_authority_eligible' = 'false'::jsonb
                  AND profile.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND profile.payload->'execution_locked' = 'true'::jsonb
                  AND (profile.payload->>'available_at')::timestamptz = NEW.available_at
                  AND (profile.payload->>'received_at')::timestamptz = NEW.available_at
                  AND profile.available_at = NEW.available_at
                  AND profile.received_at = NEW.available_at
                  AND profile.observed_at = NEW.available_at
                  AND profile.created_at <= snapshot_time
                  AND profile.received_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND canonical.source = 'fmp'
                  AND canonical.schema_version = 'fmp_company_profile_v1'
                  AND canonical.uri = NEW.location
                  AND canonical.byte_size = authority.byte_length
                  AND canonical.created_at <= snapshot_time
                  AND canonical.received_at <= profile.available_at
                  AND canonical.available_at <= profile.available_at
                  AND canonical.observed_at <= profile.available_at
                  AND raw.source = 'fmp' AND receipt.source = 'fmp'
                  AND raw.schema_version = 'fmp_profile_raw_v1'
                  AND receipt.schema_version = 'fmp_profile_receipt_v1'
                  AND raw.byte_size = (profile.payload->>'raw_byte_length')::bigint
                  AND receipt.byte_size = (profile.payload->>'receipt_byte_length')::bigint
                  AND raw.byte_size BETWEEN 1 AND 1048576
                  AND receipt.byte_size BETWEEN 1 AND 65536
                  AND raw.created_at <= snapshot_time
                  AND receipt.created_at <= snapshot_time
                  AND raw.received_at <= profile.available_at
                  AND receipt.received_at <= profile.available_at
                  AND raw.available_at <= profile.available_at
                  AND receipt.available_at <= profile.available_at
                  AND raw.observed_at <= profile.available_at
                  AND receipt.observed_at <= profile.available_at
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
        raise RuntimeError("FMP profile migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence "
            "WHERE source = 'fmp_company_profile')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen FMP profile")
    _replace(_NEW, _OLD)
