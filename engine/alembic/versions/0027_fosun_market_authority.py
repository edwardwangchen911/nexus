"""Bind Fosun market facts to immutable point-in-time provider authority.

Revision ID: 0027_fosun_market_authority
Revises: 0026_intraday_review_hardening
Create Date: 2026-08-29
"""

from collections.abc import Sequence
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0027_fosun_market_authority"
down_revision: str | Sequence[str] | None = "0026_intraday_review_hardening"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_BINDING_COLUMNS = (
    "provider_capability_id",
    "provider_status_id",
    "provider_status_version",
    "provider_contract_hash",
    "provider_authorized_at",
)
_POLICIES = (
    (
        "f780fa7f-9135-5004-ab21-d665ae7be24b",
        "market_price",
        "source_policy_v2",
        {
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
        },
    ),
    (
        "15afb077-46c6-5eea-8307-a7d203d11c2d",
        "market_quote",
        "quote_source_policy_v2",
        {
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
        },
    ),
    (
        "9bd1eb52-5486-5f40-a3d8-06bd56f80b8d",
        "intraday_monitor",
        "intraday_monitor_policy_v2",
        {
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
        },
    ),
)


def upgrade() -> None:
    """Add immutable authority bindings without rewriting legacy facts."""
    for table in ("price_series_manifests", "latest_quotes"):
        _add_binding_columns(table)
    _insert_v2_policies()
    _upgrade_authority_v2()
    _create_provider_capability_immutability()
    _create_market_binding_guard()
    _create_binding_immutability()


def downgrade() -> None:
    """Remove v2 only while no bound fact or v2 authority exists."""
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM price_series_manifests
            WHERE provider_capability_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM latest_quotes
            WHERE provider_capability_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM price_bar_set_authorities
            WHERE hash_version = 'price_bar_set_sha256_v2'
          ) OR EXISTS (
            SELECT 1 FROM intraday_bar_set_authorities
            WHERE hash_version = 'intraday_bar_set_sha256_v2'
          ) THEN
            RAISE EXCEPTION 'Fosun market authority prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    for table in ("price_series_manifests", "latest_quotes"):
        op.execute(f"DROP TRIGGER trg_{table}_provider_binding_immutable ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_fosun_market_binding ON {table}")
    op.execute("DROP TRIGGER trg_data_artifacts_fosun_binding ON data_artifacts")
    op.execute("DROP FUNCTION nexus_prevent_fosun_market_binding_change()")
    op.execute("DROP FUNCTION nexus_validate_fosun_market_binding()")
    op.execute(
        "DROP TRIGGER trg_provider_capabilities_append_only ON provider_capabilities"
    )
    op.execute("DROP FUNCTION nexus_prevent_provider_capability_change()")
    _downgrade_authority_v2()
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    for policy_id, key, version, payload in reversed(_POLICIES):
        deleted = op.get_bind().execute(
            sa.text(
                "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) AND key=:key "
                "AND version=:version AND schema_version='2' "
                "AND payload=CAST(:payload AS jsonb)"
            ),
            {
                "id": policy_id,
                "key": key,
                "version": version,
                "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
        )
        if deleted.rowcount != 1:
            raise RuntimeError("cannot downgrade a missing or altered v2 source policy")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
    for table in ("latest_quotes", "price_series_manifests"):
        _drop_binding_columns(table)


def _add_binding_columns(table: str) -> None:
    op.add_column(table, sa.Column("provider_capability_id", _UUID, nullable=True))
    op.add_column(table, sa.Column("provider_status_id", _UUID, nullable=True))
    op.add_column(table, sa.Column("provider_status_version", sa.Integer(), nullable=True))
    op.add_column(table, sa.Column("provider_contract_hash", sa.String(64), nullable=True))
    op.add_column(
        table,
        sa.Column("provider_authorized_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        f"fk_{table}_provider_capability",
        table,
        "provider_capabilities",
        ["provider_capability_id"],
        ["id"],
    )
    op.create_foreign_key(
        f"fk_{table}_provider_status",
        table,
        "integration_status",
        ["provider_status_id"],
        ["id"],
    )
    binding = ",".join(_BINDING_COLUMNS)
    op.create_check_constraint(
        op.f(f"ck_{table}_provider_binding_complete"),
        table,
        f"num_nonnulls({binding}) IN (0,5)",
    )
    op.create_check_constraint(
        op.f(f"ck_{table}_provider_binding_fosun_only"),
        table,
        f"source = 'fosun' OR num_nonnulls({binding}) = 0",
    )
    op.create_check_constraint(
        op.f(f"ck_{table}_provider_status_version_positive"),
        table,
        "provider_status_version IS NULL OR provider_status_version > 0",
    )
    op.create_check_constraint(
        op.f(f"ck_{table}_provider_contract_hash_canonical"),
        table,
        "provider_contract_hash IS NULL "
        "OR provider_contract_hash ~ '^[0-9a-f]{64}$'",
    )
    op.create_index(
        f"ix_{table}_provider_binding",
        table,
        ["provider_capability_id", "provider_status_id", "provider_authorized_at"],
    )


def _drop_binding_columns(table: str) -> None:
    op.drop_index(f"ix_{table}_provider_binding", table_name=table)
    for name in (
        "provider_contract_hash_canonical",
        "provider_status_version_positive",
        "provider_binding_fosun_only",
        "provider_binding_complete",
    ):
        op.drop_constraint(op.f(f"ck_{table}_{name}"), table, type_="check")
    op.drop_constraint(f"fk_{table}_provider_status", table, type_="foreignkey")
    op.drop_constraint(f"fk_{table}_provider_capability", table, type_="foreignkey")
    for column in reversed(_BINDING_COLUMNS):
        op.drop_column(table, column)


def _insert_v2_policies() -> None:
    for policy_id, key, version, payload in _POLICIES:
        op.execute(
            sa.text(
                "INSERT INTO source_policies (id,key,version,schema_version,payload) "
                "VALUES (CAST(:id AS uuid),:key,:version,'2',CAST(:payload AS jsonb))"
            ).bindparams(
                id=policy_id,
                key=key,
                version=version,
                payload=json.dumps(payload, sort_keys=True, separators=(",", ":")),
            )
        )


def _upgrade_authority_v2() -> None:
    _replace_authority_version_constraints(allow_v2=True)
    _create_price_v2_functions()
    _create_intraday_v2_functions()
    _create_price_validator(allow_v2=True)
    _create_intraday_validator(allow_v2=True)
    _create_agent_snapshot_validator(allow_v2=True)


def _downgrade_authority_v2() -> None:
    _create_agent_snapshot_validator(allow_v2=False)
    _create_intraday_validator(allow_v2=False)
    _create_price_validator(allow_v2=False)
    op.execute("DROP FUNCTION nexus_prepare_intraday_bar_set_v2(uuid)")
    op.execute("DROP FUNCTION nexus_intraday_bar_set_canonical_v2(uuid)")
    op.execute("DROP FUNCTION nexus_prepare_price_bar_set_v2(uuid)")
    op.execute("DROP FUNCTION nexus_price_bar_set_canonical_v2(uuid)")
    _replace_authority_version_constraints(allow_v2=False)


def _replace_authority_version_constraints(*, allow_v2: bool) -> None:
    tables = (
        (
            "price_bar_set_authorities",
            "price_bar_set_sha256_v1",
            "agent_price_authority_v1",
            "price_bar_set_sha256_v2",
            "agent_price_authority_v2",
        ),
        (
            "intraday_bar_set_authorities",
            "intraday_bar_set_sha256_v1",
            "intraday_monitor_authority_v1",
            "intraday_bar_set_sha256_v2",
            "intraday_monitor_authority_v2",
        ),
    )
    for table, hash_v1, policy_v1, hash_v2, policy_v2 in tables:
        if allow_v2:
            op.drop_constraint(
                op.f(f"ck_{table}_hash_version_v1"), table, type_="check"
            )
            op.drop_constraint(
                op.f(f"ck_{table}_policy_version_v1"), table, type_="check"
            )
            op.create_check_constraint(
                op.f(f"ck_{table}_hash_policy_version_tuple"),
                table,
                f"(hash_version = '{hash_v1}' AND policy_version = '{policy_v1}') "
                f"OR (hash_version = '{hash_v2}' AND policy_version = '{policy_v2}')",
            )
        else:
            op.drop_constraint(
                op.f(f"ck_{table}_hash_policy_version_tuple"),
                table,
                type_="check",
            )
            op.create_check_constraint(
                op.f(f"ck_{table}_hash_version_v1"),
                table,
                f"hash_version = '{hash_v1}'",
            )
            op.create_check_constraint(
                op.f(f"ck_{table}_policy_version_v1"),
                table,
                f"policy_version = '{policy_v1}'",
            )


def _create_price_v2_functions() -> None:
    daily_payload = json.dumps(_POLICIES[0][3], sort_keys=True, separators=(",", ":"))
    op.execute(
        """
        CREATE FUNCTION nexus_price_bar_set_canonical_v2(p_manifest_id uuid)
        RETURNS text LANGUAGE sql STABLE AS $$
          SELECT jsonb_build_object(
            'schema', 'price_bar_set_sha256_v2',
            'manifest', jsonb_build_object(
              'instrument', manifest.instrument,
              'source', manifest.source,
              'feed', manifest.feed,
              'delivery_mode', manifest.delivery_mode,
              'adjustment', manifest.adjustment,
              'interval', manifest.interval,
              'provider_capability_id', manifest.provider_capability_id::text,
              'provider_status_id', manifest.provider_status_id::text,
              'provider_status_version', manifest.provider_status_version,
              'provider_contract_hash', manifest.provider_contract_hash,
              'provider_authorized_at', to_char(
                manifest.provider_authorized_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
              )
            ),
            'bars', COALESCE((
              SELECT jsonb_agg(
                jsonb_build_array(
                  bar.instrument_id::text,
                  to_char(bar.session, 'YYYY-MM-DD'),
                  bar.open, bar.high, bar.low, bar.close, bar.volume,
                  to_char(bar.observed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                  to_char(bar.available_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                  to_char(bar.received_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                  bar.revision,
                  COALESCE((
                    SELECT jsonb_agg(flag.value ORDER BY flag.value)
                    FROM jsonb_array_elements_text(bar.quality_flags) AS flag(value)
                  ), '[]'::jsonb)
                ) ORDER BY bar.session, bar.observed_at, bar.revision, bar.instrument_id
              )
              FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
            ), '[]'::jsonb)
          )::text
          FROM price_series_manifests AS manifest
          WHERE manifest.id = p_manifest_id
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION nexus_prepare_price_bar_set_v2(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text,
          byte_length bigint,
          bar_count integer,
          first_session date,
          last_session date,
          available_at timestamptz
        ) LANGUAGE plpgsql AS $$
        DECLARE
          manifest price_series_manifests%ROWTYPE;
          actual_count bigint;
          first_observed timestamptz;
          last_observed timestamptz;
          distinct_session_count bigint;
          prepared_first_session date;
          prepared_last_session date;
          max_received timestamptz;
          canonical_text text;
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('price-series-manifest:' || p_manifest_id::text, 0)
          );
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND THEN RETURN; END IF;
          SELECT count(*), min(bar.observed_at), max(bar.observed_at),
                 count(DISTINCT bar.session), min(bar.session), max(bar.session),
                 max(bar.received_at)
          INTO actual_count, first_observed, last_observed, distinct_session_count,
               prepared_first_session, prepared_last_session, max_received
          FROM price_bar_records AS bar
          WHERE bar.price_series_manifest_id = manifest.id;

          IF NOT (
            manifest.source = 'fosun'
            AND manifest.feed = 'l1'
            AND manifest.delivery_mode IN ('real_time','delayed')
            AND manifest.adjustment = 'raw'
            AND manifest.interval = '1Day'
            AND num_nonnulls(
              manifest.provider_capability_id, manifest.provider_status_id,
              manifest.provider_status_version, manifest.provider_contract_hash,
              manifest.provider_authorized_at
            ) = 5
            AND actual_count BETWEEN 1 AND 10000
            AND actual_count = manifest.row_count
            AND first_observed = manifest.start_at
            AND last_observed = manifest.end_at
            AND actual_count = distinct_session_count
            AND max_received <= manifest.provider_authorized_at
            AND EXISTS (
              SELECT 1 FROM provider_capabilities AS capability
              WHERE capability.id = manifest.provider_capability_id
                AND capability.provider = 'fosun'
                AND capability.capability = 'READ_ONLY'
                AND capability.schema_version = '1'
                AND capability.contract_hash = manifest.provider_contract_hash
                AND capability.payload ->> 'operation' = 'fosun_get_bars'
                AND capability.payload ->> 'market_scope' = 'US'
                AND capability.payload ->> 'delivery_mode' = manifest.delivery_mode
                AND capability.created_at <= manifest.provider_authorized_at
                AND manifest.provider_authorized_at < capability.expires_at
            )
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id AND (
                bar.instrument_id::text <> manifest.instrument
                OR bar.observed_at > bar.available_at
                OR bar.available_at > bar.received_at
                OR jsonb_typeof(bar.quality_flags) <> 'array'
                OR bar.quality_flags <> '[]'::jsonb
              )
            )
            AND EXISTS (
              SELECT 1 FROM source_policies AS policy
              WHERE policy.id = '{_POLICIES[0][0]}'::uuid
                AND policy.key = 'market_price'
                AND policy.version = 'source_policy_v2'
                AND policy.schema_version = '2'
                AND policy.payload = '{daily_payload}'::jsonb
                AND policy.created_at <= manifest.provider_authorized_at
            )
          ) THEN RETURN; END IF;

          canonical_text := nexus_price_bar_set_canonical_v2(p_manifest_id);
          IF canonical_text IS NULL THEN RETURN; END IF;
          RETURN QUERY SELECT
            encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex'),
            octet_length(convert_to(canonical_text, 'UTF8'))::bigint,
            actual_count::integer,
            prepared_first_session,
            prepared_last_session,
            greatest(max_received, manifest.provider_authorized_at);
        END;
        $$
        """
    )


def _create_intraday_v2_functions() -> None:
    intraday_payload = json.dumps(_POLICIES[2][3], sort_keys=True, separators=(",", ":"))
    op.execute(
        """
        CREATE FUNCTION nexus_intraday_bar_set_canonical_v2(p_manifest_id uuid)
        RETURNS text LANGUAGE sql STABLE AS $$
          SELECT jsonb_build_object(
            'schema', 'intraday_bar_set_sha256_v2',
            'manifest', jsonb_build_object(
              'instrument', manifest.instrument,
              'source', manifest.source,
              'feed', manifest.feed,
              'delivery_mode', manifest.delivery_mode,
              'adjustment', manifest.adjustment,
              'interval', manifest.interval,
              'provider_capability_id', manifest.provider_capability_id::text,
              'provider_status_id', manifest.provider_status_id::text,
              'provider_status_version', manifest.provider_status_version,
              'provider_contract_hash', manifest.provider_contract_hash,
              'provider_authorized_at', to_char(
                manifest.provider_authorized_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
              )
            ),
            'bars', COALESCE((
              SELECT jsonb_agg(jsonb_build_array(
                bar.instrument_id::text,
                to_char(bar.session, 'YYYY-MM-DD'),
                bar.open, bar.high, bar.low, bar.close, bar.volume,
                to_char(bar.observed_at AT TIME ZONE 'UTC',
                  'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                to_char(bar.available_at AT TIME ZONE 'UTC',
                  'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                to_char(bar.received_at AT TIME ZONE 'UTC',
                  'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                bar.revision,
                COALESCE((SELECT jsonb_agg(flag.value ORDER BY flag.value)
                  FROM jsonb_array_elements_text(bar.quality_flags) AS flag(value)),
                  '[]'::jsonb)
              ) ORDER BY bar.observed_at, bar.revision, bar.instrument_id)
              FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
            ), '[]'::jsonb)
          )::text
          FROM price_series_manifests AS manifest
          WHERE manifest.id = p_manifest_id
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION nexus_prepare_intraday_bar_set_v2(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text,
          byte_length bigint,
          bar_count integer,
          first_observed_at timestamptz,
          last_observed_at timestamptz,
          available_at timestamptz
        ) LANGUAGE plpgsql AS $$
        DECLARE
          manifest price_series_manifests%ROWTYPE;
          actual_count bigint;
          distinct_observed_count bigint;
          prepared_first_observed timestamptz;
          prepared_last_observed timestamptz;
          max_received timestamptz;
          canonical_text text;
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('intraday-price-series-manifest:' || p_manifest_id::text, 0)
          );
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND THEN RETURN; END IF;
          SELECT count(*), count(DISTINCT observed_at), min(observed_at),
                 max(observed_at), max(received_at)
          INTO actual_count, distinct_observed_count, prepared_first_observed,
               prepared_last_observed, max_received
          FROM price_bar_records WHERE price_series_manifest_id = manifest.id;

          IF NOT (
            manifest.source = 'fosun'
            AND manifest.feed = 'l1'
            AND manifest.delivery_mode IN ('unknown','real_time','delayed')
            AND manifest.adjustment = 'raw'
            AND manifest.interval = '1Min'
            AND num_nonnulls(
              manifest.provider_capability_id, manifest.provider_status_id,
              manifest.provider_status_version, manifest.provider_contract_hash,
              manifest.provider_authorized_at
            ) = 5
            AND actual_count BETWEEN 1 AND 90
            AND actual_count = manifest.row_count
            AND actual_count = distinct_observed_count
            AND prepared_first_observed = manifest.start_at
            AND prepared_last_observed = manifest.end_at
            AND max_received <= manifest.provider_authorized_at
            AND EXISTS (
              SELECT 1 FROM provider_capabilities AS capability
              WHERE capability.id = manifest.provider_capability_id
                AND capability.provider = 'fosun'
                AND capability.capability = 'READ_ONLY'
                AND capability.schema_version = '1'
                AND capability.contract_hash = manifest.provider_contract_hash
                AND capability.payload ->> 'operation' = 'fosun_get_bars'
                AND capability.payload ->> 'market_scope' = 'US'
                AND capability.payload ->> 'delivery_mode' = manifest.delivery_mode
                AND capability.created_at <= manifest.provider_authorized_at
                AND manifest.provider_authorized_at < capability.expires_at
            )
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id AND (
                bar.instrument_id::text <> manifest.instrument
                OR bar.observed_at > bar.available_at
                OR bar.available_at > bar.received_at
                OR bar.received_at < bar.observed_at + interval '1 minute'
                OR jsonb_typeof(bar.quality_flags) IS DISTINCT FROM 'array'
                OR CASE
                  WHEN jsonb_typeof(bar.quality_flags) = 'array' THEN
                    jsonb_array_length(bar.quality_flags) > 16
                    OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(bar.quality_flags) AS flag(value)
                      WHERE jsonb_typeof(flag.value) <> 'string'
                        OR (flag.value #>> '{{}}') !~ '^[A-Z][A-Z0-9_]{{0,99}}$'
                    )
                  ELSE FALSE
                END
              )
            )
            AND EXISTS (
              SELECT 1 FROM source_policies AS policy
              WHERE policy.id = '{_POLICIES[2][0]}'::uuid
                AND policy.key = 'intraday_monitor'
                AND policy.version = 'intraday_monitor_policy_v2'
                AND policy.schema_version = '2'
                AND policy.payload = '{intraday_payload}'::jsonb
                AND policy.created_at <= manifest.provider_authorized_at
            )
          ) THEN RETURN; END IF;

          canonical_text := nexus_intraday_bar_set_canonical_v2(p_manifest_id);
          IF canonical_text IS NULL THEN RETURN; END IF;
          RETURN QUERY SELECT
            encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex'),
            octet_length(convert_to(canonical_text, 'UTF8'))::bigint,
            actual_count::integer,
            prepared_first_observed,
            prepared_last_observed,
            greatest(max_received, manifest.provider_authorized_at);
        END;
        $$
        """
    )


def _create_price_validator(*, allow_v2: bool) -> None:
    dispatch = """
          IF NEW.hash_version = 'price_bar_set_sha256_v1'
             AND NEW.policy_version = 'agent_price_authority_v1' THEN
            SELECT * INTO prepared
            FROM nexus_prepare_price_bar_set(NEW.price_series_manifest_id);
            expected_policy_id := '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid;
    """
    if allow_v2:
        dispatch += f"""
          ELSIF NEW.hash_version = 'price_bar_set_sha256_v2'
                AND NEW.policy_version = 'agent_price_authority_v2' THEN
            SELECT * INTO prepared
            FROM nexus_prepare_price_bar_set_v2(NEW.price_series_manifest_id);
            expected_policy_id := '{_POLICIES[0][0]}'::uuid;
        """
    dispatch += """
          ELSE
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
    """
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_price_bar_set_authority()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          prepared record;
          manifest price_series_manifests%ROWTYPE;
          evidence research_evidence_refs%ROWTYPE;
          expected_policy_id uuid;
        BEGIN
          {dispatch}
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = NEW.price_series_manifest_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO evidence FROM research_evidence_refs
          WHERE id = NEW.evidence_ref_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          IF (
            NEW.source_policy_id = expected_policy_id
            AND NEW.source = manifest.source
            AND NEW.provider_object_id = 'price-bar-set:' || manifest.id::text
            AND NEW.revision = prepared.content_hash
            AND NEW.content_hash = prepared.content_hash
            AND NEW.byte_length = prepared.byte_length
            AND NEW.bar_count = prepared.bar_count
            AND NEW.first_session = prepared.first_session
            AND NEW.last_session = prepared.last_session
            AND NEW.available_at = prepared.available_at
            AND evidence.source = NEW.source
            AND evidence.provider_object_id = NEW.provider_object_id
            AND evidence.revision = NEW.revision
            AND evidence.available_at = NEW.available_at
            AND evidence.content_hash = prepared.content_hash
            AND evidence.byte_length = prepared.byte_length
            AND evidence.location = 'db://price-bar-set/' || manifest.id::text
              || '@sha256:' || prepared.content_hash
          ) IS NOT TRUE THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def _create_intraday_validator(*, allow_v2: bool) -> None:
    dispatch = """
          IF NEW.hash_version = 'intraday_bar_set_sha256_v1'
             AND NEW.policy_version = 'intraday_monitor_authority_v1' THEN
            SELECT * INTO prepared
            FROM nexus_prepare_intraday_bar_set(NEW.price_series_manifest_id);
            expected_policy_id := '8fab80c9-5b6a-5b2b-8f36-3b22319b54ef'::uuid;
    """
    if allow_v2:
        dispatch += f"""
          ELSIF NEW.hash_version = 'intraday_bar_set_sha256_v2'
                AND NEW.policy_version = 'intraday_monitor_authority_v2' THEN
            SELECT * INTO prepared
            FROM nexus_prepare_intraday_bar_set_v2(NEW.price_series_manifest_id);
            expected_policy_id := '{_POLICIES[2][0]}'::uuid;
        """
    dispatch += """
          ELSE
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
    """
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_intraday_bar_set_authority()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          prepared record;
          manifest price_series_manifests%ROWTYPE;
          evidence research_evidence_refs%ROWTYPE;
          expected_policy_id uuid;
        BEGIN
          {dispatch}
          IF NOT FOUND THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = NEW.price_series_manifest_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO evidence FROM research_evidence_refs
          WHERE id = NEW.evidence_ref_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          IF (
            NEW.source_policy_id = expected_policy_id
            AND NEW.source = manifest.source
            AND NEW.provider_object_id = 'intraday-bar-set:' || manifest.id::text
            AND NEW.revision = prepared.content_hash
            AND NEW.content_hash = prepared.content_hash
            AND NEW.byte_length = prepared.byte_length
            AND NEW.bar_count = prepared.bar_count
            AND NEW.first_observed_at = prepared.first_observed_at
            AND NEW.last_observed_at = prepared.last_observed_at
            AND NEW.available_at = prepared.available_at
            AND evidence.source = NEW.source
            AND evidence.provider_object_id = NEW.provider_object_id
            AND evidence.revision = NEW.revision
            AND evidence.available_at = NEW.available_at
            AND evidence.content_hash = prepared.content_hash
            AND evidence.byte_length = prepared.byte_length
            AND evidence.location = 'db://price-bar-set/' || manifest.id::text
              || '@sha256:' || prepared.content_hash
          ) IS NOT TRUE THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def _create_agent_snapshot_validator(*, allow_v2: bool) -> None:
    v1_payload = (
        '{"priority":["fosun","alpaca","fixture"],'
        '"fosun":{"mode":"read_only","operation":"fosun_get_bars",'
        '"market_scope":"US"},'
        '"alpaca":{"feed":"sip","modes":["delayed","fixture"]},'
        '"fixture":{"mode":"fixture"}}'
    )
    authority_clause = f"""
      (
        bar_set.source_policy_id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
        AND bar_set.hash_version = 'price_bar_set_sha256_v1'
        AND bar_set.policy_version = 'agent_price_authority_v1'
        AND policy.key = 'market_price'
        AND policy.version = 'source_policy_v1'
        AND policy.schema_version = '1'
        AND policy.payload = '{v1_payload}'::jsonb
        AND (
          (manifest.source = 'alpaca' AND manifest.feed = 'sip'
            AND manifest.delivery_mode = 'delayed')
          OR (manifest.source = 'fixture' AND manifest.feed <> ''
            AND manifest.delivery_mode = 'fixture')
        )
      )
    """
    if allow_v2:
        daily_payload = json.dumps(_POLICIES[0][3], sort_keys=True, separators=(",", ":"))
        authority_clause += f"""
      OR (
        bar_set.source_policy_id = '{_POLICIES[0][0]}'::uuid
        AND bar_set.hash_version = 'price_bar_set_sha256_v2'
        AND bar_set.policy_version = 'agent_price_authority_v2'
        AND policy.key = 'market_price'
        AND policy.version = 'source_policy_v2'
        AND policy.schema_version = '2'
        AND policy.payload = '{daily_payload}'::jsonb
        AND policy.created_at <= manifest.provider_authorized_at
        AND policy.created_at <= NEW.selected_at
        AND manifest.source = 'fosun'
        AND manifest.feed = 'l1'
        AND manifest.delivery_mode IN ('real_time','delayed')
        AND manifest.adjustment = 'raw'
        AND manifest.interval = '1Day'
        AND num_nonnulls(
          manifest.provider_capability_id, manifest.provider_status_id,
          manifest.provider_status_version, manifest.provider_contract_hash,
          manifest.provider_authorized_at
        ) = 5
        AND EXISTS (
          SELECT 1 FROM nexus_prepare_price_bar_set_v2(manifest.id) AS prepared
          WHERE prepared.content_hash = bar_set.content_hash
            AND prepared.byte_length = bar_set.byte_length
            AND prepared.bar_count = bar_set.bar_count
            AND prepared.first_session = bar_set.first_session
            AND prepared.last_session = bar_set.last_session
            AND prepared.available_at = bar_set.available_at
        )
      )
        """
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_agent_snapshot_evidence()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          authority research_evidence_refs%ROWTYPE;
          snapshot_time timestamptz;
        BEGIN
          SELECT * INTO authority FROM research_evidence_refs
          WHERE id = NEW.evidence_ref_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'Agent snapshot evidence authority is missing'
              USING ERRCODE = '23503';
          END IF;
          IF authority.source <> NEW.source
             OR authority.provider_object_id <> NEW.provider_object_id
             OR authority.revision <> NEW.revision
             OR authority.available_at <> NEW.available_at
             OR authority.location <> NEW.location
             OR authority.content_hash <> NEW.content_hash THEN
            RAISE EXCEPTION 'Agent snapshot evidence authority mismatch'
              USING ERRCODE = '23514';
          END IF;
          SELECT available_at INTO snapshot_time FROM data_snapshots
          WHERE id = NEW.data_snapshot_id;
          IF snapshot_time IS NULL THEN
            RAISE EXCEPTION 'Agent snapshot authority is missing'
              USING ERRCODE = '23503';
          END IF;
          IF NEW.authority_kind = 'price' THEN
            IF NEW.selected_at > snapshot_time OR NOT EXISTS (
              SELECT 1 FROM price_bar_set_authorities AS bar_set
              JOIN price_series_manifests AS manifest
                ON manifest.id = bar_set.price_series_manifest_id
              JOIN research_evidence_refs AS evidence
                ON evidence.id = bar_set.evidence_ref_id
              JOIN source_policies AS policy ON policy.id = bar_set.source_policy_id
              WHERE bar_set.evidence_ref_id = NEW.evidence_ref_id
                AND manifest.instrument = NEW.instrument_id::text
                AND manifest.interval = '1Day'
                AND bar_set.source = manifest.source
                AND bar_set.provider_object_id = 'price-bar-set:' || manifest.id::text
                AND bar_set.revision = bar_set.content_hash
                AND bar_set.available_at = NEW.available_at
                AND bar_set.available_at <= NEW.selected_at
                AND NEW.selected_at <= snapshot_time
                AND evidence.source = bar_set.source
                AND evidence.provider_object_id = bar_set.provider_object_id
                AND evidence.revision = bar_set.revision
                AND evidence.available_at = bar_set.available_at
                AND evidence.location = 'db://price-bar-set/' || manifest.id::text
                  || '@sha256:' || bar_set.content_hash
                AND evidence.content_hash = bar_set.content_hash
                AND evidence.byte_length = bar_set.byte_length
                AND ({authority_clause})
            ) THEN
              RAISE EXCEPTION 'Agent price evidence authority is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF NEW.authority_kind = 'research' THEN
            IF NEW.provider_object_id LIKE 'price-bar-set:%'
               OR NEW.location LIKE 'db://price-bar-set/%'
               OR EXISTS (
                 SELECT 1 FROM price_series_manifests
                 WHERE evidence_ref_id = NEW.evidence_ref_id
               ) THEN
              RAISE EXCEPTION 'Agent price evidence requires price authority'
                USING ERRCODE = '23514';
            END IF;
            IF NEW.source <> 'fixture' AND NOT (
              EXISTS (
                SELECT 1 FROM filings
                WHERE evidence_ref_id = NEW.evidence_ref_id
                  AND instrument_id = NEW.instrument_id
              ) OR EXISTS (
                SELECT 1 FROM financial_facts
                WHERE evidence_ref_id = NEW.evidence_ref_id
                  AND instrument_id = NEW.instrument_id
              ) OR EXISTS (
                SELECT 1 FROM corporate_actions
                WHERE evidence_ref_id = NEW.evidence_ref_id
                  AND instrument_id = NEW.instrument_id
              ) OR EXISTS (
                SELECT 1 FROM macro_observations
                WHERE authority_version = 'research_macro_v1'
                  AND (evidence_ref_id = NEW.evidence_ref_id
                    OR calendar_evidence_ref_id = NEW.evidence_ref_id)
              )
            ) THEN
              RAISE EXCEPTION 'Agent snapshot evidence is not owned by the instrument'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'Agent snapshot evidence kind is invalid'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.available_at > snapshot_time THEN
            RAISE EXCEPTION 'future evidence cannot bind Agent snapshot'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def _create_provider_capability_immutability() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_provider_capability_change()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'provider capability is append-only' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_provider_capabilities_append_only BEFORE UPDATE OR DELETE "
        "ON provider_capabilities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_provider_capability_change()"
    )


def _create_market_binding_guard() -> None:
    daily_payload = json.dumps(_POLICIES[0][3], sort_keys=True, separators=(",", ":"))
    quote_payload = json.dumps(_POLICIES[1][3], sort_keys=True, separators=(",", ":"))
    intraday_payload = json.dumps(_POLICIES[2][3], sort_keys=True, separators=(",", ":"))
    op.execute(
        f"""
        CREATE FUNCTION nexus_validate_fosun_market_binding()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          capability provider_capabilities%ROWTYPE;
          provider_status integration_status%ROWTYPE;
          artifact data_artifacts%ROWTYPE;
          expected_operation text;
          required_artifact_schema text;
          required_policy_id uuid;
          required_policy_key text;
          required_policy_version text;
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
              WHEN NEW.interval = '1Min' THEN '{_POLICIES[2][0]}'::uuid
              ELSE '{_POLICIES[0][0]}'::uuid
            END;
            required_policy_key := CASE
              WHEN NEW.interval = '1Min' THEN 'intraday_monitor'
              ELSE 'market_price'
            END;
            required_policy_version := CASE
              WHEN NEW.interval = '1Min' THEN 'intraday_monitor_policy_v2'
              ELSE 'source_policy_v2'
            END;
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
          ELSIF TG_TABLE_NAME = 'latest_quotes' THEN
            expected_operation := 'fosun_get_quotes';
            required_artifact_schema := 'fosun_gateway_market_snapshot_v1';
            required_policy_id := '{_POLICIES[1][0]}'::uuid;
            required_policy_key := 'market_quote';
            required_policy_version := 'quote_source_policy_v2';
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
            END IF;
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
                AND policy.schema_version = '2'
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
    for table in ("price_series_manifests", "latest_quotes"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_fosun_market_binding BEFORE INSERT ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_validate_fosun_market_binding()"
        )


def _create_binding_immutability() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_fosun_market_binding_change()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_TABLE_NAME = 'data_artifacts' THEN
            IF EXISTS (
              SELECT 1 FROM price_series_manifests
              WHERE data_artifact_id = OLD.id AND provider_capability_id IS NOT NULL
            ) OR EXISTS (
              SELECT 1 FROM latest_quotes
              WHERE data_artifact_id = OLD.id AND provider_capability_id IS NOT NULL
            ) THEN
              RAISE EXCEPTION 'Fosun market binding is immutable'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.provider_capability_id IS NOT NULL THEN
            IF TG_OP = 'DELETE' THEN
              RAISE EXCEPTION 'Fosun market binding is immutable'
                USING ERRCODE = '23514';
            END IF;
            IF ROW(
                NEW.provider_capability_id, NEW.provider_status_id,
                NEW.provider_status_version, NEW.provider_contract_hash,
                NEW.provider_authorized_at
              ) IS DISTINCT FROM ROW(
                OLD.provider_capability_id, OLD.provider_status_id,
                OLD.provider_status_version, OLD.provider_contract_hash,
                OLD.provider_authorized_at
              ) THEN
              RAISE EXCEPTION 'Fosun market binding is immutable'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    for table in ("price_series_manifests", "latest_quotes"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_provider_binding_immutable "
            f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_fosun_market_binding_change()"
        )
    op.execute(
        "CREATE TRIGGER trg_data_artifacts_fosun_binding BEFORE UPDATE OR DELETE "
        "ON data_artifacts FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_fosun_market_binding_change()"
    )
