"""Add the display-only intraday market monitor authority.

Revision ID: 0025_intraday_market_monitor
Revises: 0024_broker_simulation
Create Date: 2026-08-28
"""

from collections.abc import Sequence
import json

from alembic import op
import sqlalchemy as sa


revision: str = "0025_intraday_market_monitor"
down_revision: str | Sequence[str] | None = "0024_broker_simulation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_POLICY_ID = "8fab80c9-5b6a-5b2b-8f36-3b22319b54ef"
_POLICY_PAYLOAD = {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {
        "mode": "read_only",
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "alpaca": {
        "feeds": ["iex", "sip", "delayed_sip"],
        "delivery_modes": ["real_time", "delayed"],
    },
    "fixture": {"delivery_mode": "fixture"},
    "purpose": "display_only_intraday_monitor",
}


def upgrade() -> None:
    """Upgrade schema without fabricating raw evidence for legacy quote rows."""
    op.add_column("latest_quotes", sa.Column("event_kind", sa.String(16), nullable=True))
    op.add_column("latest_quotes", sa.Column("content_hash", sa.String(64), nullable=True))
    op.add_column("latest_quotes", sa.Column("evidence_status", sa.String(24), nullable=True))
    op.add_column("latest_quotes", sa.Column("data_artifact_id", sa.UUID(), nullable=True))
    op.execute(
        """
        UPDATE latest_quotes
        SET event_kind = CASE WHEN source = 'fixture' THEN 'fixture' ELSE 'snapshot' END,
            evidence_status = CASE
              WHEN source = 'fixture' THEN 'fixture'
              ELSE 'legacy_unverified'
            END,
            content_hash = encode(digest(convert_to(concat_ws('|',
              id::text,
              instrument_id::text,
              source,
              feed,
              delivery_mode,
              COALESCE(bid::text, ''),
              COALESCE(ask::text, ''),
              COALESCE(last::text, ''),
              to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
              to_char(available_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
              to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
              revision,
              quality_flags::text
            ), 'UTF8'), 'sha256'), 'hex')
        """
    )
    op.alter_column("latest_quotes", "event_kind", nullable=False)
    op.alter_column("latest_quotes", "content_hash", nullable=False)
    op.alter_column("latest_quotes", "evidence_status", nullable=False)
    op.drop_constraint(
        "uq_latest_quotes_instrument_source_feed", "latest_quotes", type_="unique"
    )
    op.create_foreign_key(
        "fk_latest_quotes_data_artifact",
        "latest_quotes",
        "data_artifacts",
        ["data_artifact_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_latest_quotes_observation",
        "latest_quotes",
        ["instrument_id", "source", "feed", "event_kind", "observed_at", "content_hash"],
    )
    op.create_index(
        "ix_latest_quotes_observation_lookup",
        "latest_quotes",
        ["instrument_id", "source", "feed", "event_kind", "observed_at", "available_at"],
    )
    _create_quote_checks()
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_intraday_quote_change()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'intraday quote observations are immutable' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_latest_quotes_observation_immutable BEFORE UPDATE OR DELETE "
        "ON latest_quotes FOR EACH ROW EXECUTE FUNCTION nexus_prevent_intraday_quote_change()"
    )

    op.create_table(
        "intraday_bar_set_authorities",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("price_series_manifest_id", sa.UUID(), nullable=False),
        sa.Column("evidence_ref_id", sa.UUID(), nullable=False),
        sa.Column("source_policy_id", sa.UUID(), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("hash_version", sa.String(64), nullable=False),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("byte_length", sa.BigInteger(), nullable=False),
        sa.Column("bar_count", sa.Integer(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name="pk_intraday_bar_set_authorities"),
        sa.ForeignKeyConstraint(
            ["price_series_manifest_id"],
            ["price_series_manifests.id"],
            name="fk_intraday_bar_set_authorities_manifest",
        ),
        sa.ForeignKeyConstraint(
            ["source_policy_id"],
            ["source_policies.id"],
            name="fk_intraday_bar_set_authorities_source_policy",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_intraday_bar_set_authorities_evidence_owner",
        ),
        sa.UniqueConstraint(
            "price_series_manifest_id", name="uq_intraday_bar_set_authority_manifest"
        ),
        sa.UniqueConstraint("evidence_ref_id", name="uq_intraday_bar_set_authority_evidence"),
        sa.UniqueConstraint(
            "id", "source", "content_hash", name="uq_intraday_bar_set_authorities_id_source_hash"
        ),
        sa.CheckConstraint(
            "hash_version = 'intraday_bar_set_sha256_v1'",
            name="hash_version_v1",
        ),
        sa.CheckConstraint(
            "policy_version = 'intraday_monitor_authority_v1'",
            name="policy_version_v1",
        ),
        sa.CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'",
            name="content_hash_canonical",
        ),
        sa.CheckConstraint(
            "byte_length > 0", name="byte_length_positive"
        ),
        sa.CheckConstraint(
            "bar_count BETWEEN 1 AND 90",
            name="bar_count_bounded",
        ),
        sa.CheckConstraint(
            "first_observed_at <= last_observed_at",
            name="observation_time_ordered",
        ),
    )
    op.get_bind().execute(
        sa.text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (:id,'intraday_monitor','intraday_monitor_policy_v1','1',"
            "CAST(:payload AS jsonb))"
        ),
        {"id": _POLICY_ID, "payload": json.dumps(_POLICY_PAYLOAD, separators=(",", ":"))},
    )
    _create_intraday_authority_functions()
    op.execute(
        "CREATE TRIGGER trg_intraday_bar_set_authorities_created_at_immutable BEFORE UPDATE "
        "ON intraday_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_created_at_change()"
    )
    op.execute(
        "CREATE TRIGGER trg_intraday_bar_set_authorities_immutable BEFORE UPDATE OR DELETE "
        "ON intraday_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_intraday_quote_change()"
    )


def _create_quote_checks() -> None:
    constraints = (
        ("event_kind_allowed", "event_kind IN ('trade','quote','snapshot','fixture')"),
        ("content_hash_canonical", "content_hash ~ '^[0-9a-f]{64}$'"),
        (
            "evidence_status_allowed",
            "evidence_status IN ('verified','legacy_unverified','fixture')",
        ),
        (
            "time_order",
            "available_at >= observed_at AND received_at >= available_at",
        ),
        ("bid_positive", "bid IS NULL OR bid > 0"),
        ("ask_positive", "ask IS NULL OR ask > 0"),
        ("last_positive", "last IS NULL OR last > 0"),
        ("spread_order", "bid IS NULL OR ask IS NULL OR bid <= ask"),
        (
            "event_shape",
            "(event_kind = 'trade' AND last IS NOT NULL AND bid IS NULL AND ask IS NULL) OR "
            "(event_kind = 'quote' AND bid IS NOT NULL AND ask IS NOT NULL AND last IS NULL) "
            "OR (event_kind IN ('snapshot','fixture') AND "
            "(bid IS NOT NULL OR ask IS NOT NULL OR last IS NOT NULL))",
        ),
        (
            "evidence_state_consistent",
            "(source = 'fixture' AND evidence_status = 'fixture' "
            "AND data_artifact_id IS NULL) OR "
            "(source <> 'fixture' AND evidence_status = 'verified' "
            "AND data_artifact_id IS NOT NULL) OR "
            "(source <> 'fixture' AND evidence_status = 'legacy_unverified' "
            "AND data_artifact_id IS NULL)",
        ),
    )
    for name, condition in constraints:
        op.create_check_constraint(name, "latest_quotes", condition)


def _create_intraday_authority_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_intraday_bar_set_canonical_v1(p_manifest_id uuid)
        RETURNS text LANGUAGE sql STABLE AS $$
          SELECT jsonb_build_object(
            'schema', 'intraday_bar_set_sha256_v1',
            'manifest', jsonb_build_object(
              'instrument', manifest.instrument,
              'source', manifest.source,
              'feed', manifest.feed,
              'delivery_mode', manifest.delivery_mode,
              'adjustment', manifest.adjustment,
              'interval', manifest.interval
            ),
            'bars', COALESCE((
              SELECT jsonb_agg(jsonb_build_array(
                bar.instrument_id::text,
                to_char(bar.session, 'YYYY-MM-DD'),
                bar.open, bar.high, bar.low, bar.close, bar.volume,
                to_char(bar.observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                to_char(bar.available_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                to_char(bar.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
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
        CREATE FUNCTION nexus_prepare_intraday_bar_set(p_manifest_id uuid)
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
          SELECT * INTO manifest FROM price_series_manifests WHERE id = p_manifest_id;
          IF NOT FOUND THEN RETURN; END IF;

          SELECT count(*), count(DISTINCT observed_at), min(observed_at), max(observed_at),
                 max(received_at)
          INTO actual_count, distinct_observed_count, prepared_first_observed,
               prepared_last_observed, max_received
          FROM price_bar_records WHERE price_series_manifest_id = manifest.id;

          IF NOT (
            manifest.interval = '1Min'
            AND (
              (manifest.source = 'alpaca' AND (
                (manifest.feed IN ('iex','sip') AND manifest.delivery_mode = 'real_time')
                OR (manifest.feed = 'delayed_sip' AND manifest.delivery_mode = 'delayed')
              ))
              OR (manifest.source = 'fixture' AND manifest.delivery_mode = 'fixture')
            )
            AND actual_count BETWEEN 1 AND 90
            AND actual_count = manifest.row_count
            AND actual_count = distinct_observed_count
            AND prepared_first_observed = manifest.start_at
            AND prepared_last_observed = manifest.end_at
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id AND (
                bar.instrument_id::text <> manifest.instrument
                OR bar.observed_at > bar.available_at
                OR bar.available_at > bar.received_at
                OR jsonb_typeof(bar.quality_flags) <> 'array'
              )
            )
            AND EXISTS (
              SELECT 1 FROM source_policies AS policy
              WHERE policy.id = '{_POLICY_ID}'::uuid
                AND policy.key = 'intraday_monitor'
                AND policy.version = 'intraday_monitor_policy_v1'
                AND policy.schema_version = '1'
                AND policy.payload = '{json.dumps(_POLICY_PAYLOAD, separators=(",", ":"))}'::jsonb
            )
          ) THEN RETURN; END IF;

          canonical_text := nexus_intraday_bar_set_canonical_v1(p_manifest_id);
          IF canonical_text IS NULL THEN RETURN; END IF;
          RETURN QUERY SELECT
            encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex'),
            octet_length(convert_to(canonical_text, 'UTF8'))::bigint,
            actual_count::integer,
            prepared_first_observed,
            prepared_last_observed,
            max_received;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION nexus_validate_intraday_bar_set_authority()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          prepared record;
          manifest price_series_manifests%ROWTYPE;
          evidence research_evidence_refs%ROWTYPE;
        BEGIN
          SELECT * INTO prepared
          FROM nexus_prepare_intraday_bar_set(NEW.price_series_manifest_id);
          IF NOT FOUND THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = NEW.price_series_manifest_id;
          SELECT * INTO evidence FROM research_evidence_refs WHERE id = NEW.evidence_ref_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'intraday bar set authority is invalid' USING ERRCODE = '23514';
          END IF;
          IF (
            NEW.source_policy_id = '{_POLICY_ID}'::uuid
            AND NEW.hash_version = 'intraday_bar_set_sha256_v1'
            AND NEW.policy_version = 'intraday_monitor_authority_v1'
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
    op.execute(
        "CREATE TRIGGER trg_intraday_bar_set_authorities_validate BEFORE INSERT "
        "ON intraday_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_intraday_bar_set_authority()"
    )


def downgrade() -> None:
    """Downgrade only while no post-migration intraday facts exist."""
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM intraday_bar_set_authorities)
            OR EXISTS (
              SELECT 1 FROM latest_quotes
              WHERE evidence_status = 'verified' OR event_kind IN ('trade','quote')
            )
          THEN
            RAISE EXCEPTION 'intraday market facts prevent downgrade' USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        "DROP TRIGGER trg_intraday_bar_set_authorities_validate "
        "ON intraday_bar_set_authorities"
    )
    op.execute(
        "DROP TRIGGER trg_intraday_bar_set_authorities_created_at_immutable "
        "ON intraday_bar_set_authorities"
    )
    op.execute(
        "DROP TRIGGER trg_intraday_bar_set_authorities_immutable "
        "ON intraday_bar_set_authorities"
    )
    op.execute("DROP FUNCTION nexus_validate_intraday_bar_set_authority()")
    op.execute("DROP FUNCTION nexus_prepare_intraday_bar_set(uuid)")
    op.execute("DROP FUNCTION nexus_intraday_bar_set_canonical_v1(uuid)")
    op.drop_table("intraday_bar_set_authorities")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    op.execute(
        sa.text("DELETE FROM source_policies WHERE id = CAST(:id AS uuid)").bindparams(
            id=_POLICY_ID
        )
    )
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")

    op.execute(
        "DROP TRIGGER trg_latest_quotes_observation_immutable ON latest_quotes"
    )
    op.execute("DROP FUNCTION nexus_prevent_intraday_quote_change()")
    for name in (
        "event_kind_allowed",
        "content_hash_canonical",
        "evidence_status_allowed",
        "time_order",
        "bid_positive",
        "ask_positive",
        "last_positive",
        "spread_order",
        "event_shape",
        "evidence_state_consistent",
    ):
        op.drop_constraint(name, "latest_quotes", type_="check")
    op.drop_index("ix_latest_quotes_observation_lookup", table_name="latest_quotes")
    op.drop_constraint("uq_latest_quotes_observation", "latest_quotes", type_="unique")
    op.drop_constraint("fk_latest_quotes_data_artifact", "latest_quotes", type_="foreignkey")
    op.drop_column("latest_quotes", "data_artifact_id")
    op.drop_column("latest_quotes", "evidence_status")
    op.drop_column("latest_quotes", "content_hash")
    op.drop_column("latest_quotes", "event_kind")
    op.create_unique_constraint(
        "uq_latest_quotes_instrument_source_feed",
        "latest_quotes",
        ["instrument_id", "source", "feed"],
    )
