"""Bind archived current corporate-action observations to frozen research.

Revision ID: 0041_action_observations
Revises: 0040_formal_decision_cutoff
"""

from alembic import op
from sqlalchemy import text

revision = "0041_action_observations"
down_revision = "0040_formal_decision_cutoff"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS observation
                JOIN public.data_artifacts AS manifest
                  ON manifest.sha256 = observation.payload->>'manifest_content_hash'
                JOIN public.data_artifacts AS canonical
                  ON canonical.sha256 = observation.revision
                WHERE NEW.source = 'alpaca_action_observation'
                  AND observation.source = NEW.source
                  AND observation.schema_version = 'alpaca_action_observation_v1'
                  AND observation.provider_object_id = NEW.provider_object_id
                  AND observation.revision = NEW.revision
                  AND observation.revision = NEW.content_hash
                  AND observation.payload->>'instrument_id' = NEW.instrument_id::text
                  AND observation.payload->>'source_completeness' = 'unknown'
                  AND observation.payload->>'revision_semantics' = 'local_content_hash'
                  AND observation.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND observation.payload->'ledger_eligible' = 'false'::jsonb
                  AND observation.payload->'execution_locked' = 'true'::jsonb
                  AND observation.payload->'pagination_exhausted' = 'true'::jsonb
                  AND (observation.payload->>'available_at')::timestamptz = NEW.available_at
                  AND observation.available_at = NEW.available_at
                  AND observation.received_at = NEW.available_at
                  AND observation.created_at <= snapshot_time
                  AND observation.received_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND canonical.source = 'alpaca'
                  AND canonical.schema_version = 'alpaca_action_observation_v1'
                  AND canonical.uri = NEW.location
                  AND canonical.byte_size = authority.byte_length
                  AND canonical.created_at <= snapshot_time
                  AND canonical.received_at <= observation.available_at
                  AND canonical.available_at <= observation.available_at
                  AND manifest.source = 'alpaca'
                  AND manifest.schema_version = 'alpaca_actions_manifest_v1'
                  AND manifest.byte_size = (observation.payload->>'manifest_byte_length')::bigint
                  AND manifest.created_at <= snapshot_time
                  AND manifest.received_at <= observation.available_at
                  AND manifest.available_at <= observation.available_at
                  AND jsonb_typeof(observation.payload->'pages') = 'array'
                  AND jsonb_array_length(observation.payload->'pages') BETWEEN 1 AND 100
                  AND observation.available_at = (
                    SELECT max((page->>'received_at')::timestamptz)
                    FROM jsonb_array_elements(observation.payload->'pages') AS page
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(observation.payload->'pages') AS page
                    WHERE NOT EXISTS (
                      SELECT 1 FROM public.data_artifacts AS raw
                      JOIN public.data_artifacts AS receipt
                        ON receipt.sha256 = page->>'receipt_content_hash'
                      WHERE raw.sha256 = page->>'raw_content_hash'
                        AND raw.source = 'alpaca' AND receipt.source = 'alpaca'
                        AND raw.schema_version = 'alpaca_actions_raw_v1'
                        AND receipt.schema_version = 'alpaca_actions_receipt_v1'
                        AND raw.byte_size = (page->>'raw_byte_length')::bigint
                        AND receipt.byte_size = (page->>'receipt_byte_length')::bigint
                        AND raw.created_at <= snapshot_time
                        AND receipt.created_at <= snapshot_time
                        AND raw.received_at <= (page->>'received_at')::timestamptz
                        AND receipt.received_at <= (page->>'received_at')::timestamptz
                        AND raw.available_at <= (page->>'received_at')::timestamptz
                        AND receipt.available_at <= (page->>'received_at')::timestamptz
                    )
                  )
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
        raise RuntimeError("action observation migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence "
            "WHERE source = 'alpaca_action_observation')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for frozen action observation")
    _replace(_NEW, _OLD)
