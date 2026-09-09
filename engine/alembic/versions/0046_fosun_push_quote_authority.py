"""Admit authority-bound Fosun push quotes for display-only reads.

Revision ID: 0046_fosun_push_quote_authority
Revises: 0045_adjusted_diagnostic_prices
"""

import json

from alembic import op
from sqlalchemy import text

revision = "0046_fosun_push_quote_authority"
down_revision = "0045_adjusted_diagnostic_prices"
branch_labels = None
depends_on = None

_POLICY_ID = "ef11e044-5b1f-53cd-94b7-2ef8c4ac3cda"
_POLICY_PAYLOAD = {
    "selected_source": "fosun",
    "fallback": "disabled",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["unknown", "real_time", "delayed"],
        "artifact_schema_operations": {
            "fosun_gateway_market_snapshot_v1": "fosun_get_quotes",
            "fosun_push_quote_archive_v1": "fosun_subscribe_quotes",
        },
        "market_scope": "US",
    },
    "alpaca": {"mode": "disabled"},
    "purpose": "display_only_last_price",
}
_DAILY_POLICY_ID = "f780fa7f-9135-5004-ab21-d665ae7be24b"
_QUOTE_POLICY_V2_ID = "15afb077-46c6-5eea-8307-a7d203d11c2d"
_INTRADAY_POLICY_ID = "9bd1eb52-5486-5f40-a3d8-06bd56f80b8d"
_DAILY_PAYLOAD = {
    "selected_source": "fosun",
    "fallback": "disabled",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["real_time", "delayed"],
        "adjustment": "raw",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {"mode": "disabled"},
    "purpose": "agent_price_and_daily_bars",
}
_QUOTE_V2_PAYLOAD = {
    "selected_source": "fosun",
    "fallback": "disabled",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["unknown", "real_time", "delayed"],
        "operation": "fosun_get_quotes",
        "market_scope": "US",
    },
    "alpaca": {"mode": "disabled"},
    "purpose": "display_only_last_price",
}
_INTRADAY_PAYLOAD = {
    "selected_source": "fosun",
    "fallback": "disabled",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["unknown", "real_time", "delayed"],
        "adjustment": "raw",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {"mode": "disabled"},
    "purpose": "display_only_intraday_monitor",
}


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (CAST(:id AS uuid),'market_quote','quote_source_policy_v3','3',"
            "CAST(:payload AS jsonb))"
        ).bindparams(
            id=_POLICY_ID,
            payload=json.dumps(_POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
        )
    )
    _replace_market_binding_guard(use_push_policy=True)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.data_artifacts, public.integration_status, "
        "public.latest_quotes, public.price_series_manifests, "
        "public.provider_capabilities, public.source_policies "
        "IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM latest_quotes AS quote "
            "JOIN data_artifacts AS artifact ON artifact.id=quote.data_artifact_id "
            "WHERE artifact.source='fosun' "
            "AND artifact.schema_version='fosun_push_quote_archive_v1' "
            "AND quote.provider_capability_id IS NOT NULL)"
        )
    ):
        raise RuntimeError("Fosun push quote facts prevent downgrade")
    _replace_market_binding_guard(use_push_policy=False)
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(
        text(
            "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
            "AND key='market_quote' AND version='quote_source_policy_v3' "
            "AND schema_version='3' AND payload=CAST(:payload AS jsonb)"
        ),
        {
            "id": _POLICY_ID,
            "payload": json.dumps(_POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
        },
    )
    if deleted.rowcount != 1:
        raise RuntimeError("cannot downgrade a missing or altered v3 quote policy")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")


def _replace_market_binding_guard(*, use_push_policy: bool) -> None:
    daily_payload = json.dumps(_DAILY_PAYLOAD, sort_keys=True, separators=(",", ":"))
    intraday_payload = json.dumps(
        _INTRADAY_PAYLOAD, sort_keys=True, separators=(",", ":")
    )
    quote_payload = json.dumps(
        _POLICY_PAYLOAD if use_push_policy else _QUOTE_V2_PAYLOAD,
        sort_keys=True,
        separators=(",", ":"),
    )
    quote_policy_id = _POLICY_ID if use_push_policy else _QUOTE_POLICY_V2_ID
    quote_policy_version = (
        "quote_source_policy_v3" if use_push_policy else "quote_source_policy_v2"
    )
    policy_schema_declaration = (
        "\n          required_policy_schema text;" if use_push_policy else ""
    )
    daily_policy_schema = (
        "\n            required_policy_schema := '2';" if use_push_policy else ""
    )
    policy_schema_check = (
        "                AND policy.schema_version = required_policy_schema"
        if use_push_policy
        else "                AND policy.schema_version = '2'"
    )
    if use_push_policy:
        quote_branch = f"""ELSIF TG_TABLE_NAME = 'latest_quotes' THEN
            expected_operation := CASE artifact.schema_version
              WHEN 'fosun_gateway_market_snapshot_v1' THEN 'fosun_get_quotes'
              WHEN 'fosun_push_quote_archive_v1' THEN 'fosun_subscribe_quotes'
              ELSE NULL
            END;
            required_artifact_schema := artifact.schema_version;
            required_policy_id := '{quote_policy_id}'::uuid;
            required_policy_key := 'market_quote';
            required_policy_version := '{quote_policy_version}';
            required_policy_schema := '3';
            required_policy_payload := '{quote_payload}'::jsonb;
            IF NEW.feed <> 'l1'
               OR NEW.event_kind <> 'snapshot'
               OR NEW.last IS NULL
               OR NEW.bid IS NOT NULL
               OR NEW.ask IS NOT NULL
               OR NEW.evidence_status <> 'verified'
               OR NEW.available_at <> NEW.received_at
               OR NEW.observed_at > NEW.available_at
               OR (
                 artifact.schema_version = 'fosun_gateway_market_snapshot_v1'
                 AND NEW.observed_at <> NEW.received_at
               )
               OR NOT (
                 artifact.schema_version = 'fosun_gateway_market_snapshot_v1'
                 OR artifact.schema_version = 'fosun_push_quote_archive_v1'
               )
               OR jsonb_typeof(NEW.quality_flags) IS DISTINCT FROM 'array'
               OR NOT NEW.quality_flags ? 'FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE'
               OR NOT NEW.quality_flags ? 'INTRADAY_BID_ASK_UNAVAILABLE'
               OR NOT (
                 NEW.revision = artifact.sha256
                 OR right(NEW.revision, 65) = ':' || artifact.sha256
               )
               OR artifact.mime_type <> 'application/json'
               OR artifact.received_at <> NEW.received_at
            THEN
              RAISE EXCEPTION 'Fosun market authority is invalid'
                USING ERRCODE = '23514';
            END IF;"""
    else:
        quote_branch = f"""ELSIF TG_TABLE_NAME = 'latest_quotes' THEN
            expected_operation := 'fosun_get_quotes';
            required_artifact_schema := 'fosun_gateway_market_snapshot_v1';
            required_policy_id := '{quote_policy_id}'::uuid;
            required_policy_key := 'market_quote';
            required_policy_version := '{quote_policy_version}';
            required_policy_payload := '{quote_payload}'::jsonb;
            IF NEW.feed <> 'l1'
               OR NEW.event_kind <> 'snapshot'
               OR NEW.last IS NULL
               OR NEW.bid IS NOT NULL
               OR NEW.ask IS NOT NULL
               OR NEW.evidence_status <> 'verified'
               OR NEW.observed_at <> NEW.received_at
               OR NEW.available_at <> NEW.received_at
               OR jsonb_typeof(NEW.quality_flags) IS DISTINCT FROM 'array'
               OR NOT NEW.quality_flags ? 'FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE'
               OR NOT NEW.quality_flags ? 'INTRADAY_BID_ASK_UNAVAILABLE'
               OR NOT (
                 NEW.revision = artifact.sha256
                 OR right(NEW.revision, 65) = ':' || artifact.sha256
               )
               OR artifact.mime_type <> 'application/json'
               OR artifact.received_at <> NEW.received_at
            THEN
              RAISE EXCEPTION 'Fosun market authority is invalid'
                USING ERRCODE = '23514';
            END IF;"""
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_fosun_market_binding()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          capability provider_capabilities%ROWTYPE;
          provider_status integration_status%ROWTYPE;
          artifact data_artifacts%ROWTYPE;
          expected_operation text;
          required_artifact_schema text;
          required_policy_id uuid;
          required_policy_key text;
          required_policy_version text;{policy_schema_declaration}
          required_policy_payload jsonb;
          checked_at timestamptz;
        BEGIN
          IF NEW.source <> 'fosun' THEN
            IF num_nonnulls(
              NEW.provider_capability_id, NEW.provider_status_id,
              NEW.provider_status_version, NEW.provider_contract_hash,
              NEW.provider_authorized_at
            ) <> 0 THEN
              RAISE EXCEPTION 'Fosun market authority is invalid'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF num_nonnulls(
            NEW.provider_capability_id, NEW.provider_status_id,
            NEW.provider_status_version, NEW.provider_contract_hash,
            NEW.provider_authorized_at
          ) <> 5 THEN
            RAISE EXCEPTION 'Fosun market authority is invalid'
              USING ERRCODE = '23514';
          END IF;

          PERFORM pg_advisory_xact_lock(hashtextextended(
            'fosun-integration-status:' || NEW.provider_status_id::text, 0
          ));
          PERFORM pg_advisory_xact_lock(hashtextextended(
            'fosun-data-artifact:' || NEW.data_artifact_id::text, 0
          ));
          SELECT * INTO provider_status FROM integration_status
          WHERE id = NEW.provider_status_id;
          SELECT * INTO capability FROM provider_capabilities
          WHERE id = NEW.provider_capability_id;
          SELECT * INTO artifact FROM data_artifacts
          WHERE id = NEW.data_artifact_id;
          checked_at := clock_timestamp();

          IF TG_TABLE_NAME = 'price_series_manifests' THEN
            expected_operation := 'fosun_get_bars';
            required_artifact_schema := CASE
              WHEN NEW.interval = '1Min' THEN 'intraday_price_bars_v1'
              ELSE 'price_bars_v1'
            END;
            required_policy_id := CASE
              WHEN NEW.interval = '1Min' THEN '{_INTRADAY_POLICY_ID}'::uuid
              ELSE '{_DAILY_POLICY_ID}'::uuid
            END;
            required_policy_key := CASE
              WHEN NEW.interval = '1Min' THEN 'intraday_monitor'
              ELSE 'market_price'
            END;
            required_policy_version := CASE
              WHEN NEW.interval = '1Min' THEN 'intraday_monitor_policy_v2'
              ELSE 'source_policy_v2'
            END;{daily_policy_schema}
            required_policy_payload := CASE
              WHEN NEW.interval = '1Min' THEN '{intraday_payload}'::jsonb
              ELSE '{daily_payload}'::jsonb
            END;
            IF NEW.feed <> 'l1'
               OR NEW.adjustment <> 'raw'
               OR NEW.interval NOT IN ('1Min','1Day')
               OR artifact.mime_type <> 'application/vnd.apache.parquet' THEN
              RAISE EXCEPTION 'Fosun market authority is invalid'
                USING ERRCODE = '23514';
            END IF;
          {quote_branch}
          ELSE
            RAISE EXCEPTION 'Fosun market authority is invalid'
              USING ERRCODE = '23514';
          END IF;

          IF (
            provider_status.id = NEW.provider_status_id
            AND provider_status.provider = 'fosun'
            AND provider_status.status = 'read_only'
            AND provider_status.version = NEW.provider_status_version
            AND provider_status.admitted_contract_hash = NEW.provider_contract_hash
            AND provider_status.mode_changed_at <= NEW.provider_authorized_at
            AND capability.id = NEW.provider_capability_id
            AND capability.provider = 'fosun'
            AND capability.capability = 'READ_ONLY'
            AND capability.schema_version = '1'
            AND capability.contract_hash = NEW.provider_contract_hash
            AND capability.payload ->> 'operation' = expected_operation
            AND capability.payload ->> 'market_scope' = 'US'
            AND capability.payload ->> 'delivery_mode' = NEW.delivery_mode
            AND NEW.delivery_mode IN ('unknown','real_time','delayed')
            AND capability.created_at <= NEW.provider_authorized_at
            AND NEW.provider_authorized_at < capability.expires_at
            AND NEW.provider_authorized_at <= checked_at
            AND checked_at < capability.expires_at
            AND (
              provider_status.capability_invalidated_at IS NULL
              OR capability.created_at > provider_status.capability_invalidated_at
            )
            AND artifact.id = NEW.data_artifact_id
            AND artifact.source = 'fosun'
            AND artifact.schema_version = required_artifact_schema
            AND artifact.sha256 ~ '^[0-9a-f]{{64}}$'
            AND artifact.byte_size > 0
            AND artifact.observed_at <= artifact.available_at
            AND artifact.available_at <= artifact.received_at
            AND artifact.received_at <= NEW.provider_authorized_at
            AND EXISTS (
              SELECT 1 FROM source_policies AS policy
              WHERE policy.id = required_policy_id
                AND policy.key = required_policy_key
                AND policy.version = required_policy_version
{policy_schema_check}
                AND policy.payload = required_policy_payload
                AND policy.created_at <= NEW.provider_authorized_at
            )
          ) IS NOT TRUE THEN
            RAISE EXCEPTION 'Fosun market authority is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
