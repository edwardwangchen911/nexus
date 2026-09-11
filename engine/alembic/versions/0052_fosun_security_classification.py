"""Freeze raw-bound Fosun classification routing hints.

Revision ID: 0052_fosun_classification
Revises: 0051_scenario_selections
"""

from alembic import op
from sqlalchemy import text

revision = "0052_fosun_classification"
down_revision = "0051_scenario_selections"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS classification
                JOIN public.data_artifacts AS canonical
                  ON canonical.sha256 = classification.revision
                JOIN public.data_artifacts AS raw
                  ON raw.sha256 = classification.payload->'raw_quote'->>'artifact_sha256'
                WHERE NEW.source = 'fosun_security_classification'
                  AND classification.source = NEW.source
                  AND classification.schema_version = 'fosun_security_classification_v1'
                  AND classification.provider_object_id = NEW.provider_object_id
                  AND classification.provider_object_id =
                    'classification:' || NEW.instrument_id::text || ':'
                    || (classification.payload->>'provider_code')
                  AND classification.revision = NEW.revision
                  AND classification.revision = NEW.content_hash
                  AND classification.payload->>'schema_version' = classification.schema_version
                  AND classification.payload->>'source' = NEW.source
                  AND classification.payload->>'instrument_id' = NEW.instrument_id::text
                  AND classification.payload->>'revision_semantics' = 'local_content_hash'
                  AND classification.payload->'canonical_asset_type' = 'null'::jsonb
                  AND classification.payload->'identity_authoritative' = 'false'::jsonb
                  AND classification.payload->'price_authority_eligible' = 'false'::jsonb
                  AND classification.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND classification.payload->'execution_locked' = 'true'::jsonb
                  AND classification.payload->>'routing_family' IN (
                    'company_equity_like', 'fund_or_structured_like', 'unknown')
                  AND (classification.payload->>'received_at')::timestamptz =
                    classification.received_at
                  AND (classification.payload->>'available_at')::timestamptz =
                    classification.available_at
                  AND classification.observed_at = classification.received_at
                  AND classification.received_at = classification.available_at
                  AND classification.available_at = NEW.available_at
                  AND classification.created_at <= snapshot_time
                  AND classification.received_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND canonical.source = NEW.source
                  AND canonical.schema_version = classification.schema_version
                  AND canonical.uri = NEW.location
                  AND canonical.byte_size = authority.byte_length
                  AND canonical.byte_size BETWEEN 1 AND 16384
                  AND canonical.created_at <= snapshot_time
                  AND canonical.observed_at = classification.received_at
                  AND canonical.received_at = classification.received_at
                  AND canonical.available_at = classification.available_at
                  AND raw.source = 'fosun'
                  AND raw.schema_version = 'fosun_gateway_market_snapshot_v1'
                  AND raw.uri = classification.payload->'raw_quote'->>'artifact_uri'
                  AND raw.byte_size =
                    (classification.payload->'raw_quote'->>'artifact_byte_length')::bigint
                  AND raw.byte_size BETWEEN 1 AND 1048576
                  AND raw.created_at <= snapshot_time
                  AND raw.observed_at <= classification.available_at
                  AND raw.received_at <= classification.available_at
                  AND raw.available_at <= classification.available_at
                  AND classification.payload->'raw_quote'->>'artifact_sha256'
                    ~ '^[0-9a-f]{64}$'
                  AND classification.payload->'raw_quote'->>'request_body_sha256'
                    ~ '^[0-9a-f]{64}$'
                  AND classification.payload->'raw_quote'->>'gateway_request_id'
                    ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def _replace(old: str, new: str) -> None:
    connection = op.get_bind()
    definition = connection.scalar(
        text(
            "SELECT pg_get_functiondef("
            "'public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("Fosun classification migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence "
            "WHERE source = 'fosun_security_classification')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen Fosun classification")
    _replace(_NEW, _OLD)
