"""Bind exact receipt-backed news observations without changing older evidence.

Revision ID: 0044_news_research
Revises: 0043_current_price_trends
"""

from alembic import op
from sqlalchemy import text

revision = "0044_news_research"
down_revision = "0043_current_price_trends"
branch_labels = None
depends_on = None

_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS news
                JOIN public.data_artifacts AS canonical ON canonical.sha256 = news.revision
                JOIN public.data_snapshots AS original
                  ON original.id::text = news.payload->>'news_snapshot_id'
                JOIN public.data_artifacts AS raw ON raw.sha256 = news.payload->>'raw_hash'
                JOIN public.data_artifacts AS receipt ON receipt.sha256 = news.payload->>'receipt_hash'
                WHERE NEW.source = 'alpaca_news'
                  AND news.source = NEW.source AND news.schema_version = 'news_research_v1'
                  AND news.provider_object_id = NEW.provider_object_id
                  AND news.provider_object_id = 'news-research:' || NEW.instrument_id::text
                    || ':' || original.id::text || ':latest5-v1'
                  AND news.revision = NEW.revision AND news.revision = NEW.content_hash
                  AND news.payload->>'instrument_id' = NEW.instrument_id::text
                  AND news.payload->>'schema_version' = news.schema_version
                  AND news.payload->>'selection' = 'latest_updated_5_v1'
                  AND news.payload->'primary_source_verified' = 'false'::jsonb
                  AND news.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND news.payload->'execution_locked' = 'true'::jsonb
                  AND jsonb_typeof(news.payload->'articles') = 'array'
                  AND jsonb_array_length(news.payload->'articles') BETWEEN 1 AND 5
                  AND (news.payload->>'available_at')::timestamptz = news.available_at
                  AND news.observed_at = news.available_at AND news.received_at = news.available_at
                  AND news.available_at = NEW.available_at AND news.available_at <= snapshot_time
                  AND news.created_at <= snapshot_time AND authority.created_at <= snapshot_time
                  AND canonical.source = NEW.source AND canonical.schema_version = news.schema_version
                  AND canonical.uri = NEW.location AND canonical.byte_size = authority.byte_length
                  AND canonical.created_at <= snapshot_time
                  AND canonical.available_at = news.available_at
                  AND canonical.received_at = news.available_at AND canonical.observed_at = news.available_at
                  AND original.source = NEW.source AND original.schema_version = 'nexus_news_v1'
                  AND original.provider_object_id = 'news:' || NEW.instrument_id::text
                  AND original.payload->>'instrument_id' = NEW.instrument_id::text
                  AND original.revision = news.payload->>'receipt_hash'
                  AND original.payload->'receipt'->>'symbol' = news.payload->>'symbol'
                  AND original.payload->'receipt'->>'schema' = 'nexus_news_receipt_v1'
                  AND original.payload->'receipt'->>'source' = 'alpaca'
                  AND original.payload->'receipt'->>'raw_sha256' = raw.sha256
                  AND (original.payload->'receipt'->>'raw_byte_length')::bigint = raw.byte_size
                  AND original.created_at <= news.available_at
                  AND original.observed_at = original.available_at
                  AND original.available_at = original.received_at
                  AND original.received_at <= news.available_at
                  AND (news.payload->>'received_at')::timestamptz = original.received_at
                  AND receipt.source = NEW.source AND receipt.schema_version = 'news_receipt_v1'
                  AND receipt.created_at <= news.available_at
                  AND receipt.available_at <= original.received_at
                  AND receipt.received_at <= original.received_at AND receipt.observed_at <= original.received_at
                  AND octet_length(news.payload->>'receipt_text') = receipt.byte_size
                  AND raw.source = NEW.source AND raw.schema_version = 'news_raw_v1'
                  AND raw.created_at <= news.available_at AND raw.available_at <= original.received_at
                  AND raw.received_at <= original.received_at AND raw.observed_at <= original.received_at
                  AND octet_length(news.payload->>'raw_text') = raw.byte_size
                  AND EXISTS (
                    SELECT 1 FROM public.instrument_aliases AS alias
                    WHERE alias.instrument_id = NEW.instrument_id AND alias.alias_type = 'ticker'
                      AND alias.symbol = news.payload->>'symbol'
                      AND alias.valid_from <= original.received_at::date
                      AND (alias.valid_to IS NULL OR alias.valid_to >= original.received_at::date)
                      AND alias.created_at <= news.available_at
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
        raise RuntimeError("news research migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.research_evidence_refs WHERE source='alpaca_news') "
            "OR EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence WHERE source='alpaca_news')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for persisted news research")
    _replace(_NEW, _OLD)
