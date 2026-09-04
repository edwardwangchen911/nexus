"""Freeze receipt-backed split trends as research supplements, not market prices.

Revision ID: 0043_current_price_trends
Revises: 0042_fmp_profiles
"""

import json

from alembic import op
from sqlalchemy import text

revision = "0043_current_price_trends"
down_revision = "0042_fmp_profiles"
branch_labels = None
depends_on = None

_PURPOSE_ID = "c15c693f-d286-54bd-a1bf-f74d6651cb9e"
_PURPOSE = {
    "source": "alpaca_current_trends",
    "schema": "alpaca_current_trends_v1",
    "feed": "sip",
    "adjustment": "split",
    "currency": "USD",
    "timeframe": "1Day",
    "calendar": "XNYS",
    "features": ["close", "sma_20", "sma_200", "rsi_14", "volatility_20"],
    "missing_session_rule": "last_contiguous_observed_segment",
    "algorithm": "nexus_decimal_price_features_v1",
    "historical_backtest_eligible": False,
    "market_price_eligible": False,
    "total_return_eligible": False,
    "execution_locked": True,
}
_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots AS trends
                JOIN public.data_artifacts AS canonical ON canonical.sha256 = trends.revision
                JOIN public.diagnostic_policies AS purpose
                  ON purpose.id = 'c15c693f-d286-54bd-a1bf-f74d6651cb9e'::uuid
                JOIN public.diagnostic_policies AS calendar
                  ON calendar.id = '4f717aca-5b62-5468-a53c-6dd93763434c'::uuid
                WHERE NEW.source = 'alpaca_current_trends'
                  AND trends.source = NEW.source
                  AND trends.schema_version = 'alpaca_current_trends_v1'
                  AND trends.provider_object_id = NEW.provider_object_id
                  AND trends.provider_object_id = 'collection:' || NEW.instrument_id::text
                    || ':' || (trends.payload->>'manifest_content_hash')
                  AND trends.revision = NEW.revision AND trends.revision = NEW.content_hash
                  AND trends.payload->>'schema_version' = trends.schema_version
                  AND trends.payload->>'source' = NEW.source
                  AND trends.payload->>'instrument_id' = NEW.instrument_id::text
                  AND trends.payload->>'feed' = 'sip'
                  AND trends.payload->>'adjustment' = 'split'
                  AND trends.payload->>'currency' = 'USD'
                  AND trends.payload->>'timeframe' = '1Day'
                  AND trends.payload->'historical_backtest_eligible' = 'false'::jsonb
                  AND trends.payload->'market_price_eligible' = 'false'::jsonb
                  AND trends.payload->'total_return_eligible' = 'false'::jsonb
                  AND trends.payload->'execution_locked' = 'true'::jsonb
                  AND (trends.payload->>'received_at')::timestamptz = trends.received_at
                  AND (trends.payload->>'materialized_at')::timestamptz = trends.available_at
                  AND (trends.payload->>'available_at')::timestamptz = NEW.available_at
                  AND trends.available_at = NEW.available_at
                  AND trends.observed_at = trends.received_at
                  AND trends.received_at <= trends.available_at
                  AND trends.available_at <= snapshot_time AND trends.created_at <= snapshot_time
                  AND authority.created_at <= snapshot_time
                  AND canonical.source = NEW.source
                  AND canonical.schema_version = trends.schema_version
                  AND canonical.uri = NEW.location AND canonical.byte_size = authority.byte_length
                  AND canonical.byte_size BETWEEN 1 AND 2097152
                  AND canonical.created_at <= snapshot_time
                  AND canonical.received_at = trends.available_at
                  AND canonical.observed_at = trends.available_at
                  AND canonical.available_at = trends.available_at
                  AND purpose.key = 'alpaca_current_trends'
                  AND purpose.version = 'alpaca_current_trends_v1'
                  AND purpose.schema_version = '1'
                  AND purpose.id::text = trends.payload->'purpose_policy'->>'row_id'
                  AND purpose.key = trends.payload->'purpose_policy'->>'key'
                  AND purpose.version = trends.payload->'purpose_policy'->>'version'
                  AND purpose.schema_version = trends.payload->'purpose_policy'->>'schema_version'
                  AND purpose.payload = trends.payload->'purpose_policy'->'payload'
                  AND purpose.created_at =
                    (trends.payload->'purpose_policy'->>'created_at')::timestamptz
                  AND purpose.created_at <= trends.available_at
                  AND calendar.key = 'xnys_calendar_adoption'
                  AND calendar.version = 'xnys_exchange_calendars_v1'
                  AND calendar.id::text = trends.payload->'calendar'->>'row_id'
                  AND calendar.key = trends.payload->'calendar'->>'key'
                  AND calendar.version = trends.payload->'calendar'->>'version'
                  AND calendar.schema_version = trends.payload->'calendar'->>'schema_version'
                  AND calendar.payload = trends.payload->'calendar'->'payload'
                  AND calendar.created_at =
                    (trends.payload->'calendar'->>'created_at')::timestamptz
                  AND calendar.created_at <= trends.available_at
                  AND jsonb_typeof(trends.payload->'originals') = 'array'
                  AND jsonb_array_length(trends.payload->'originals') BETWEEN 3 AND 21
                  AND (SELECT count(DISTINCT item->>'name')
                    FROM jsonb_array_elements(trends.payload->'originals') AS item)
                    = jsonb_array_length(trends.payload->'originals')
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(trends.payload->'originals') AS item
                    LEFT JOIN public.data_artifacts AS original
                      ON original.sha256 = item->>'content_hash'
                    WHERE (
                      jsonb_typeof(item) = 'object'
                      AND jsonb_typeof(item->'name') = 'string'
                      AND item->>'name' ~ '^(manifest[.]json|page-[0-9]{3}([.]receipt)?[.]json)$'
                      AND jsonb_typeof(item->'raw_text') = 'string'
                      AND jsonb_typeof(item->'byte_length') = 'number'
                      AND jsonb_typeof(item->'received_at') = 'string'
                      AND jsonb_typeof(item->'content_hash') = 'string'
                      AND item->>'content_hash' ~ '^[0-9a-f]{64}$'
                      AND original.id IS NOT NULL AND original.source = NEW.source
                      AND original.schema_version = CASE
                        WHEN item->>'name' = 'manifest.json' THEN 'alpaca_trends_manifest_v1'
                        WHEN item->>'name' LIKE 'page-%.receipt.json' THEN 'alpaca_trends_receipt_v1'
                        ELSE 'alpaca_trends_raw_v1' END
                      AND original.byte_size = (item->>'byte_length')::bigint
                      AND original.byte_size BETWEEN 1 AND 2097152
                      AND octet_length(item->>'raw_text') = original.byte_size
                      AND original.created_at <= snapshot_time
                      AND original.received_at <= (item->>'received_at')::timestamptz
                      AND original.observed_at <= (item->>'received_at')::timestamptz
                      AND original.available_at <= (item->>'received_at')::timestamptz
                      AND (item->>'received_at')::timestamptz <= trends.available_at
                      AND (item->>'name' = 'manifest.json' OR EXISTS (
                        SELECT 1 FROM jsonb_array_elements(trends.payload->'originals') AS paired
                        WHERE paired->>'name' = CASE
                          WHEN item->>'name' LIKE '%.receipt.json'
                            THEN replace(item->>'name', '.receipt.json', '.json')
                          ELSE replace(item->>'name', '.json', '.receipt.json') END
                      ))
                    ) IS NOT TRUE
                  )
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(trends.payload->'originals') AS item
                    WHERE item->>'name' = 'manifest.json'
                      AND item->>'content_hash' = trends.payload->>'manifest_content_hash'
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
        raise RuntimeError("current trends migration requires the expected ownership guard")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    connection = op.get_bind()
    existing = connection.execute(
        text(
            "SELECT id::text, key, version, schema_version, payload FROM public.diagnostic_policies "
            "WHERE id=:id OR (key='alpaca_current_trends' AND version='alpaca_current_trends_v1')"
        ),
        {"id": _PURPOSE_ID},
    ).all()
    if existing and (
        len(existing) != 1
        or tuple(existing[0])
        != (
            _PURPOSE_ID,
            "alpaca_current_trends",
            "alpaca_current_trends_v1",
            "1",
            _PURPOSE,
        )
    ):
        raise RuntimeError("current trends purpose conflicts with the published policy")
    if not existing:
        connection.execute(
            text(
                "INSERT INTO public.diagnostic_policies (id, key, version, schema_version, payload) "
                "VALUES (:id, 'alpaca_current_trends', 'alpaca_current_trends_v1', '1', CAST(:payload AS jsonb))"
            ),
            {"id": _PURPOSE_ID, "payload": json.dumps(_PURPOSE)},
        )
    _replace(_OLD, _NEW)


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM public.data_snapshots WHERE source='alpaca_current_trends') "
            "OR EXISTS (SELECT 1 FROM public.agent_data_snapshot_evidence WHERE source='alpaca_current_trends')"
        )
    ):
        raise RuntimeError("cannot remove ownership support for persisted current trends")
    _replace(_NEW, _OLD)
    # The published policy is append-only, including across a schema rollback.
