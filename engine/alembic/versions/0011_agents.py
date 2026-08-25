"""Add immutable, append-only research committee audit records."""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0011_agents"
down_revision: str | None = "0010_research_data"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)
_IMMUTABLE_TABLES = (
    "price_bar_records",
    "price_bar_set_authorities",
    "agent_prompt_versions",
    "agent_model_configs",
    "agent_research_events",
    "agent_research_outputs",
    "agent_research_output_evidence",
    "agent_data_snapshot_evidence",
)


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.drop_constraint(
        op.f("ck_research_evidence_refs_location_content_addressed"),
        "research_evidence_refs",
        type_="check",
    )
    op.create_check_constraint(
        "location_content_addressed",
        "research_evidence_refs",
        "(location LIKE 'memory://sha256/' || content_hash || '%') OR "
        "(location LIKE 'memory://normalized/market/%/' || content_hash || '.parquet') OR "
        "(location LIKE 'package://%' AND location LIKE '%@sha256:' || content_hash || '%') OR "
        "(location LIKE 's3://%' AND (location LIKE '%/sha256/' || content_hash || '%' "
        "OR location ~ '[?&]versionId=[^&#]+')) OR "
        "(location ~ ('^db://price-bar-set/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        "[0-9a-f]{4}-[0-9a-f]{12}@sha256:' || content_hash || '$'))",
    )
    op.add_column(
        "price_series_manifests",
        sa.Column("evidence_ref_id", _UUID, nullable=True),
    )
    op.add_column(
        "price_series_manifests",
        sa.Column("interval", sa.String(8), nullable=False, server_default="1Day"),
    )
    op.create_foreign_key(
        op.f("fk_price_series_evidence_ref"),
        "price_series_manifests",
        "research_evidence_refs",
        ["evidence_ref_id"],
        ["id"],
    )
    op.create_unique_constraint(
        op.f("uq_price_series_manifests_evidence_ref_id"),
        "price_series_manifests",
        ["evidence_ref_id"],
    )
    op.create_check_constraint(
        "interval_allowed",
        "price_series_manifests",
        "interval IN ('1Min', '1Day')",
    )
    op.create_table(
        "price_bar_records",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("price_series_manifest_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("session", sa.Date(), nullable=False),
        sa.Column("open", _NUMERIC, nullable=False),
        sa.Column("high", _NUMERIC, nullable=False),
        sa.Column("low", _NUMERIC, nullable=False),
        sa.Column("close", _NUMERIC, nullable=False),
        sa.Column("volume", _NUMERIC, nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("quality_flags", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["price_series_manifest_id"],
            ["price_series_manifests.id"],
            name="fk_price_bar_records_manifest",
        ),
        sa.UniqueConstraint(
            "price_series_manifest_id",
            "observed_at",
            "revision",
            name="uq_price_bar_records_manifest_observation_revision",
        ),
        sa.CheckConstraint("high >= open AND high >= close AND high >= low", name="high_valid"),
        sa.CheckConstraint("low <= open AND low <= close AND low <= high", name="low_valid"),
        sa.CheckConstraint(
            "open > 0 AND high > 0 AND low > 0 AND close > 0", name="price_positive"
        ),
        sa.CheckConstraint("volume >= 0", name="volume_nonnegative"),
        sa.CheckConstraint("available_at >= observed_at", name="available_after_observed"),
        sa.CheckConstraint("received_at >= available_at", name="received_after_available"),
    )
    op.create_table(
        "price_bar_set_authorities",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("price_series_manifest_id", _UUID, nullable=False),
        sa.Column("evidence_ref_id", _UUID, nullable=False),
        sa.Column("source_policy_id", _UUID, nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column(
            "hash_version",
            sa.String(64),
            nullable=False,
            server_default="price_bar_set_sha256_v1",
        ),
        sa.Column(
            "policy_version",
            sa.String(64),
            nullable=False,
            server_default="agent_price_authority_v1",
        ),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("byte_length", sa.BigInteger(), nullable=False),
        sa.Column("bar_count", sa.Integer(), nullable=False),
        sa.Column("first_session", sa.Date(), nullable=False),
        sa.Column("last_session", sa.Date(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["price_series_manifest_id"],
            ["price_series_manifests.id"],
            name="fk_price_bar_set_authorities_manifest",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_price_bar_set_authorities_evidence_owner",
        ),
        sa.ForeignKeyConstraint(
            ["source_policy_id"],
            ["source_policies.id"],
            name="fk_price_bar_set_authorities_source_policy",
        ),
        sa.UniqueConstraint("price_series_manifest_id", name="uq_price_bar_set_authority_manifest"),
        sa.UniqueConstraint("evidence_ref_id", name="uq_price_bar_set_authority_evidence"),
        sa.CheckConstraint("hash_version = 'price_bar_set_sha256_v1'", name="hash_version_v1"),
        sa.CheckConstraint("policy_version = 'agent_price_authority_v1'", name="policy_version_v1"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint("byte_length > 0", name="byte_length_positive"),
        sa.CheckConstraint("bar_count BETWEEN 1 AND 10000", name="bar_count_bounded"),
        sa.CheckConstraint("first_session <= last_session", name="session_ordered"),
    )
    op.create_table(
        "agent_prompt_versions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("role", "version", name="uq_agent_prompt_versions_role_version"),
        sa.CheckConstraint(
            "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio')",
            name="role_allowed",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )
    op.create_table(
        "agent_model_configs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("model_id", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("max_input_tokens", sa.Integer(), nullable=False),
        sa.Column("max_tool_calls", sa.Integer(), nullable=False),
        sa.Column("max_output_tokens", sa.Integer(), nullable=False),
        sa.Column("input_usd_per_million", _NUMERIC, nullable=False),
        sa.Column("output_usd_per_million", _NUMERIC, nullable=False),
        sa.Column("pricing_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pricing_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("model_id", "version", name="uq_agent_model_configs_identity"),
        sa.CheckConstraint("config_hash ~ '^[0-9a-f]{64}$'", name="config_hash_canonical"),
        sa.CheckConstraint(
            "max_input_tokens BETWEEN 1 AND 1048576", name="max_input_tokens_bounded"
        ),
        sa.CheckConstraint("max_tool_calls BETWEEN 0 AND 8", name="max_tool_calls_bounded"),
        sa.CheckConstraint(
            "max_output_tokens BETWEEN 1 AND 16384", name="max_output_tokens_bounded"
        ),
        sa.CheckConstraint("input_usd_per_million >= 0", name="input_price_nonnegative"),
        sa.CheckConstraint("output_usd_per_million >= 0", name="output_price_nonnegative"),
        sa.CheckConstraint(
            "pricing_expires_at > pricing_available_at", name="pricing_time_ordered"
        ),
    )
    op.create_table(
        "agent_research_runs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("prompt_version_id", _UUID, nullable=False),
        sa.Column("model_config_id", _UUID, nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_id", sa.String(100), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="prepared"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("schema_hash", sa.String(64), nullable=False),
        sa.Column("model_hash", sa.String(64), nullable=False),
        sa.Column("prompt_hash", sa.String(64), nullable=False),
        sa.Column("prompt_manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence_hash", sa.String(64), nullable=False),
        sa.Column("evidence_manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("tool_hash", sa.String(64), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("reserved_cost_usd", _NUMERIC, nullable=False),
        sa.Column("settled_cost_usd", _NUMERIC, nullable=True),
        sa.Column("pricing_version", sa.String(100), nullable=False, server_default="configured"),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_agent_runs_instrument"
        ),
        sa.ForeignKeyConstraint(
            ["data_snapshot_id"], ["data_snapshots.id"], name="fk_agent_runs_data_snapshot"
        ),
        sa.ForeignKeyConstraint(
            ["prompt_version_id"], ["agent_prompt_versions.id"], name="fk_agent_runs_prompt_version"
        ),
        sa.ForeignKeyConstraint(
            ["model_config_id"], ["agent_model_configs.id"], name="fk_agent_runs_model_config"
        ),
        sa.UniqueConstraint("actor_id", "idempotency_key", name="uq_agent_runs_actor_idempotency"),
        sa.CheckConstraint(
            "status IN ('prepared','running','validated','audited','synthesized','completed','failed','abstained')",
            name="status_allowed",
        ),
        sa.CheckConstraint("retry_count BETWEEN 0 AND 1", name="retry_count_bounded"),
        sa.CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        sa.CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd",
            name="settled_cost_bounded",
        ),
        sa.CheckConstraint("schema_hash ~ '^[0-9a-f]{64}$'", name="schema_hash_canonical"),
        sa.CheckConstraint("model_hash ~ '^[0-9a-f]{64}$'", name="model_hash_canonical"),
        sa.CheckConstraint("prompt_hash ~ '^[0-9a-f]{64}$'", name="prompt_hash_canonical"),
        sa.CheckConstraint("evidence_hash ~ '^[0-9a-f]{64}$'", name="evidence_hash_canonical"),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_manifest) = 'array' AND "
            "jsonb_array_length(evidence_manifest) BETWEEN 1 AND 100",
            name="evidence_manifest_bounded",
        ),
        sa.CheckConstraint("tool_hash ~ '^[0-9a-f]{64}$'", name="tool_hash_canonical"),
        sa.CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash_canonical"),
    )
    op.create_table(
        "agent_data_snapshot_evidence",
        sa.Column("data_snapshot_id", _UUID, primary_key=True),
        sa.Column("instrument_id", _UUID, primary_key=True),
        sa.Column("evidence_ref_id", _UUID, primary_key=True),
        sa.Column("authority_kind", sa.String(16), nullable=False, server_default="research"),
        sa.Column("selected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("location", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["data_snapshot_id"],
            ["data_snapshots.id"],
            name="fk_agent_snapshot_evidence_snapshot",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_agent_snapshot_evidence_instrument"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_agent_snapshot_evidence_authority",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint(
            "(authority_kind = 'research' AND selected_at IS NULL) OR "
            "(authority_kind = 'price' AND selected_at IS NOT NULL)",
            name="authority_kind_valid",
        ),
    )
    op.create_index(
        "uq_agent_snapshot_price_authority",
        "agent_data_snapshot_evidence",
        ["data_snapshot_id", "instrument_id"],
        unique=True,
        postgresql_where=sa.text("authority_kind = 'price'"),
    )
    op.create_table(
        "agent_research_events",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["run_id"], ["agent_research_runs.id"], name="fk_agent_events_run"),
        sa.UniqueConstraint("run_id", "sequence", name="uq_agent_research_events_run_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "state IN ('prepared','running','validated','audited','synthesized','completed',"
            "'failed','abstained')",
            name="state_allowed",
        ),
        sa.CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_canonical"),
    )
    op.create_table(
        "agent_research_outputs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("role", sa.String(32), nullable=True),
        sa.Column("prompt_hash", sa.String(64), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("intent", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["agent_research_runs.id"], name="fk_agent_outputs_run"
        ),
        sa.UniqueConstraint("run_id", "sequence", name="uq_agent_research_outputs_run_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "status IN ('running','validated','audited','synthesized','completed','failed',"
            "'abstained')",
            name="status_allowed",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint(
            "(role IS NULL AND prompt_hash IS NULL) OR "
            "(role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio') AND prompt_hash ~ '^[0-9a-f]{64}$')",
            name="role_prompt_bound",
        ),
    )
    op.create_table(
        "agent_daily_budget_reservations",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("pricing_version", sa.String(100), nullable=False),
        sa.Column("reserved_usd", _NUMERIC, nullable=False),
        sa.Column("settled_usd", _NUMERIC, nullable=True),
        sa.Column("state", sa.String(16), nullable=False, server_default="reserved"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["run_id"], ["agent_research_runs.id"], name="fk_agent_budget_run"),
        sa.UniqueConstraint("run_id", name="uq_agent_budget_reservations_run"),
        sa.CheckConstraint("reserved_usd >= 0", name="reserved_nonnegative"),
        sa.CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd", name="settled_bounded"
        ),
        sa.CheckConstraint("state IN ('reserved','settled','released')", name="state_allowed"),
    )
    op.create_table(
        "agent_research_output_evidence",
        sa.Column("output_id", _UUID, primary_key=True),
        sa.Column("evidence_ref_id", _UUID, primary_key=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("location", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["output_id"], ["agent_research_outputs.id"], name="fk_agent_output_evidence_output"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_agent_output_evidence_authority",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )
    op.execute(
        """
        CREATE FUNCTION nexus_price_bar_set_canonical_v1(p_manifest_id uuid)
        RETURNS text
        LANGUAGE sql
        STABLE
        AS $$
          SELECT jsonb_build_object(
            'schema', 'price_bar_set_sha256_v1',
            'manifest', jsonb_build_object(
              'instrument', manifest.instrument,
              'source', manifest.source,
              'feed', manifest.feed,
              'delivery_mode', manifest.delivery_mode,
              'adjustment', manifest.adjustment,
              'interval', manifest.interval
            ),
            'bars', COALESCE((
              SELECT jsonb_agg(
                jsonb_build_array(
                  bar.instrument_id::text,
                  to_char(bar.session, 'YYYY-MM-DD'),
                  bar.open, bar.high, bar.low, bar.close, bar.volume,
                  to_char(
                    bar.observed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                  ),
                  to_char(
                    bar.available_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                  ),
                  to_char(
                    bar.received_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                  ),
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
        """
        CREATE FUNCTION nexus_prepare_price_bar_set(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text,
          byte_length bigint,
          bar_count integer,
          first_session date,
          last_session date,
          available_at timestamptz
        )
        LANGUAGE plpgsql
        AS $$
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

          SELECT * INTO manifest
          FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND THEN
            RETURN;
          END IF;

          SELECT
            count(*),
            min(bar.observed_at),
            max(bar.observed_at),
            count(DISTINCT bar.session),
            min(bar.session),
            max(bar.session),
            max(bar.received_at)
          INTO
            actual_count,
            first_observed,
            last_observed,
            distinct_session_count,
            prepared_first_session,
            prepared_last_session,
            max_received
          FROM price_bar_records AS bar
          WHERE bar.price_series_manifest_id = manifest.id;

          IF NOT (
            manifest.interval = '1Day'
            AND (
              (
                manifest.source = 'alpaca'
                AND manifest.feed = 'sip'
                AND manifest.delivery_mode = 'delayed'
              )
              OR (
                manifest.source = 'fixture'
                AND manifest.feed <> ''
                AND manifest.delivery_mode = 'fixture'
              )
            )
            AND actual_count BETWEEN 1 AND 10000
            AND actual_count = manifest.row_count
            AND first_observed = manifest.start_at
            AND last_observed = manifest.end_at
            AND actual_count = distinct_session_count
            AND NOT EXISTS (
              SELECT 1
              FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
                AND (
                  bar.instrument_id::text <> manifest.instrument
                  OR bar.observed_at > bar.available_at
                  OR bar.available_at > bar.received_at
                  OR jsonb_typeof(bar.quality_flags) <> 'array'
                  OR bar.quality_flags <> '[]'::jsonb
                )
            )
            AND EXISTS (
              SELECT 1
              FROM source_policies AS policy
              WHERE policy.id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
                AND policy.key = 'market_price'
                AND policy.version = 'source_policy_v1'
                AND policy.schema_version = '1'
                AND policy.payload = '{
                  "priority":["fosun","alpaca","fixture"],
                  "fosun":{"mode":"read_only","operation":"fosun_get_bars","market_scope":"US"},
                  "alpaca":{"feed":"sip","modes":["delayed","fixture"]},
                  "fixture":{"mode":"fixture"}
                }'::jsonb
            )
          ) THEN
            RETURN;
          END IF;

          canonical_text := nexus_price_bar_set_canonical_v1(p_manifest_id);
          IF canonical_text IS NULL THEN
            RETURN;
          END IF;

          RETURN QUERY SELECT
            encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex'),
            octet_length(convert_to(canonical_text, 'UTF8'))::bigint,
            actual_count::integer,
            prepared_first_session,
            prepared_last_session,
            max_received;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION nexus_validate_price_bar_set_authority()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          prepared record;
          manifest price_series_manifests%ROWTYPE;
          evidence research_evidence_refs%ROWTYPE;
        BEGIN
          SELECT * INTO prepared
          FROM nexus_prepare_price_bar_set(NEW.price_series_manifest_id);
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;

          SELECT * INTO manifest
          FROM price_series_manifests
          WHERE id = NEW.price_series_manifest_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;

          SELECT * INTO evidence
          FROM research_evidence_refs
          WHERE id = NEW.evidence_ref_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE = '23514';
          END IF;

          IF (
            NEW.source_policy_id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
            AND NEW.hash_version = 'price_bar_set_sha256_v1'
            AND NEW.policy_version = 'agent_price_authority_v1'
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
    op.execute(
        "CREATE TRIGGER trg_price_bar_set_authorities_validate BEFORE INSERT "
        "ON price_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_price_bar_set_authority()"
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_agent_immutable_change() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN RAISE EXCEPTION 'agent audit records are append-only' USING ERRCODE = '23514'; END; $$"
    )
    for table in (
        "price_bar_records",
        "price_bar_set_authorities",
        "agent_prompt_versions",
        "agent_model_configs",
        "agent_research_runs",
        "agent_research_events",
        "agent_research_outputs",
        "agent_research_output_evidence",
        "agent_data_snapshot_evidence",
        "agent_daily_budget_reservations",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    for table in _IMMUTABLE_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_agent_immutable_change()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_price_bar_set_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          manifest_id uuid;
        BEGIN
          manifest_id := CASE
            WHEN TG_OP = 'INSERT' THEN NEW.price_series_manifest_id
            ELSE OLD.price_series_manifest_id
          END;
          PERFORM pg_advisory_xact_lock(
            hashtextextended('price-series-manifest:' || manifest_id::text, 0)
          );
          IF TG_OP = 'INSERT' AND EXISTS (
            SELECT 1
            FROM price_bar_set_authorities AS authority
            WHERE authority.price_series_manifest_id = manifest_id
          ) THEN
            RAISE EXCEPTION 'frozen price series cannot grow' USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'DELETE' THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_00_price_bar_records_authority_guard "
        "BEFORE INSERT OR UPDATE OR DELETE ON price_bar_records FOR EACH ROW "
        "EXECUTE FUNCTION nexus_guard_price_bar_set_mutation()"
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_frozen_price_authority_change() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM agent_data_snapshot_evidence snapshot_authority "
        "JOIN price_bar_set_authorities bar_set "
        "ON bar_set.evidence_ref_id = snapshot_authority.evidence_ref_id "
        "JOIN price_series_manifests manifest "
        "ON manifest.id = bar_set.price_series_manifest_id "
        "WHERE snapshot_authority.authority_kind = 'price' AND "
        "((TG_TABLE_NAME = 'price_series_manifests' AND manifest.id = OLD.id) OR "
        "(TG_TABLE_NAME = 'data_artifacts' AND manifest.data_artifact_id = OLD.id))) THEN "
        "RAISE EXCEPTION 'frozen price authority is immutable' USING ERRCODE = '23514'; "
        "END IF; IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END; $$"
    )
    for table in ("price_series_manifests", "data_artifacts"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_frozen_authority BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_frozen_price_authority_change()"
        )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_run_transition() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN "
        "IF (to_jsonb(NEW) - ARRAY['status','retry_count','settled_cost_usd','input_tokens','output_tokens']) "
        "IS DISTINCT FROM "
        "(to_jsonb(OLD) - ARRAY['status','retry_count','settled_cost_usd','input_tokens','output_tokens']) "
        "THEN RAISE EXCEPTION 'agent run authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF OLD.status IN ('completed','failed','abstained') THEN RAISE EXCEPTION 'terminal agent run is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status = OLD.status AND "
        "(NEW.retry_count,NEW.settled_cost_usd,NEW.input_tokens,NEW.output_tokens) "
        "IS DISTINCT FROM "
        "(OLD.retry_count,OLD.settled_cost_usd,OLD.input_tokens,OLD.output_tokens) "
        "THEN RETURN NEW; END IF; "
        "IF NEW.status IN ('failed','abstained') THEN RETURN NEW; END IF; "
        "IF (OLD.status = 'prepared' AND NEW.status = 'running') OR "
        "(OLD.status = 'running' AND NEW.status = 'validated') OR "
        "(OLD.status = 'validated' AND NEW.status = 'audited') OR "
        "(OLD.status = 'audited' AND NEW.status = 'synthesized') OR "
        "(OLD.status = 'synthesized' AND NEW.status = 'completed') THEN RETURN NEW; END IF; "
        "RAISE EXCEPTION 'invalid agent run transition' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_research_runs_transition BEFORE UPDATE ON agent_research_runs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_run_transition()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_snapshot_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "DECLARE authority research_evidence_refs%ROWTYPE; snapshot_time timestamptz; BEGIN "
        "SELECT * INTO authority FROM research_evidence_refs WHERE id = NEW.evidence_ref_id; "
        "IF NOT FOUND THEN RAISE EXCEPTION 'Agent snapshot evidence authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF authority.source <> NEW.source OR authority.provider_object_id <> NEW.provider_object_id "
        "OR authority.revision <> NEW.revision OR authority.available_at <> NEW.available_at "
        "OR authority.location <> NEW.location OR authority.content_hash <> NEW.content_hash THEN "
        "RAISE EXCEPTION 'Agent snapshot evidence authority mismatch' USING ERRCODE = '23514'; END IF; "
        "SELECT available_at INTO snapshot_time FROM data_snapshots WHERE id = NEW.data_snapshot_id; "
        "IF snapshot_time IS NULL THEN RAISE EXCEPTION 'Agent snapshot authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF NEW.authority_kind = 'price' THEN "
        "IF NEW.selected_at > snapshot_time OR NOT EXISTS ("
        "SELECT 1 FROM price_bar_set_authorities bar_set "
        "JOIN price_series_manifests manifest "
        "ON manifest.id = bar_set.price_series_manifest_id "
        "JOIN research_evidence_refs evidence ON evidence.id = bar_set.evidence_ref_id "
        "JOIN source_policies policy ON policy.id = bar_set.source_policy_id "
        "WHERE bar_set.evidence_ref_id = NEW.evidence_ref_id "
        "AND bar_set.source_policy_id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid "
        "AND bar_set.hash_version = 'price_bar_set_sha256_v1' "
        "AND bar_set.policy_version = 'agent_price_authority_v1' "
        "AND manifest.instrument = NEW.instrument_id::text "
        "AND manifest.interval = '1Day' AND bar_set.source = manifest.source "
        "AND bar_set.provider_object_id = 'price-bar-set:' || manifest.id::text "
        "AND bar_set.revision = bar_set.content_hash "
        "AND bar_set.available_at = NEW.available_at "
        "AND bar_set.available_at <= NEW.selected_at AND NEW.selected_at <= snapshot_time "
        "AND evidence.source = bar_set.source "
        "AND evidence.provider_object_id = bar_set.provider_object_id "
        "AND evidence.revision = bar_set.revision "
        "AND evidence.available_at = bar_set.available_at "
        "AND evidence.location = 'db://price-bar-set/' || manifest.id::text || "
        "'@sha256:' || bar_set.content_hash "
        "AND evidence.content_hash = bar_set.content_hash "
        "AND evidence.byte_length = bar_set.byte_length "
        "AND policy.key = 'market_price' AND policy.version = 'source_policy_v1' "
        "AND policy.schema_version = '1' "
        "AND policy.payload = '{"
        "\"priority\":[\"fosun\",\"alpaca\",\"fixture\"],"
        "\"fosun\":{\"mode\":\"read_only\",\"operation\":\"fosun_get_bars\",\"market_scope\":\"US\"},"
        "\"alpaca\":{\"feed\":\"sip\",\"modes\":[\"delayed\",\"fixture\"]},"
        "\"fixture\":{\"mode\":\"fixture\"}}'::jsonb "
        "AND ((manifest.source = 'alpaca' AND manifest.feed = 'sip' "
        "AND manifest.delivery_mode = 'delayed') OR "
        "(manifest.source = 'fixture' AND manifest.feed <> '' "
        "AND manifest.delivery_mode = 'fixture'))) THEN "
        "RAISE EXCEPTION 'Agent price evidence authority is invalid' USING ERRCODE = '23514'; END IF; "
        "ELSIF NEW.authority_kind = 'research' THEN "
        "IF NEW.provider_object_id LIKE 'price-bar-set:%' "
        "OR NEW.location LIKE 'db://price-bar-set/%' "
        "OR EXISTS (SELECT 1 FROM price_series_manifests "
        "WHERE evidence_ref_id = NEW.evidence_ref_id) THEN "
        "RAISE EXCEPTION 'Agent price evidence requires price authority' USING ERRCODE = '23514'; END IF; "
        "IF NEW.source <> 'fixture' AND NOT ("
        "EXISTS (SELECT 1 FROM filings WHERE evidence_ref_id = NEW.evidence_ref_id AND instrument_id = NEW.instrument_id) OR "
        "EXISTS (SELECT 1 FROM financial_facts WHERE evidence_ref_id = NEW.evidence_ref_id AND instrument_id = NEW.instrument_id) OR "
        "EXISTS (SELECT 1 FROM corporate_actions WHERE evidence_ref_id = NEW.evidence_ref_id AND instrument_id = NEW.instrument_id) OR "
        "EXISTS (SELECT 1 FROM macro_observations WHERE authority_version = 'research_macro_v1' AND "
        "(evidence_ref_id = NEW.evidence_ref_id OR calendar_evidence_ref_id = NEW.evidence_ref_id))) THEN "
        "RAISE EXCEPTION 'Agent snapshot evidence is not owned by the instrument' USING ERRCODE = '23514'; END IF; "
        "ELSE RAISE EXCEPTION 'Agent snapshot evidence kind is invalid' USING ERRCODE = '23514'; END IF; "
        "IF NEW.available_at > snapshot_time THEN RAISE EXCEPTION 'future evidence cannot bind Agent snapshot' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_snapshot_evidence_validate BEFORE INSERT OR UPDATE "
        "ON agent_data_snapshot_evidence FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_snapshot_evidence()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_output_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "DECLARE authority research_evidence_refs%ROWTYPE; run_decision_time timestamptz; BEGIN "
        "SELECT * INTO authority FROM research_evidence_refs WHERE id = NEW.evidence_ref_id; "
        "IF NOT FOUND THEN RAISE EXCEPTION 'Agent evidence authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF authority.source <> NEW.source OR authority.provider_object_id <> NEW.provider_object_id "
        "OR authority.revision <> NEW.revision OR authority.available_at <> NEW.available_at "
        "OR authority.location <> NEW.location OR authority.content_hash <> NEW.content_hash THEN "
        "RAISE EXCEPTION 'Agent evidence authority mismatch' USING ERRCODE = '23514'; END IF; "
        "SELECT run.decision_time INTO run_decision_time FROM agent_research_outputs output "
        "JOIN agent_research_runs run ON run.id = output.run_id WHERE output.id = NEW.output_id; "
        "IF run_decision_time IS NULL THEN RAISE EXCEPTION 'Agent output authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF NEW.available_at > run_decision_time THEN "
        "RAISE EXCEPTION 'future evidence cannot bind Agent output' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_output_evidence_validate BEFORE INSERT OR UPDATE "
        "ON agent_research_output_evidence FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_output_evidence()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_budget_transition() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF OLD.state <> 'reserved' THEN "
        "RAISE EXCEPTION 'terminal Agent budget reservation is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.state NOT IN ('settled','released') THEN "
        "RAISE EXCEPTION 'invalid Agent budget transition' USING ERRCODE = '23514'; END IF; "
        "IF NEW.run_id <> OLD.run_id OR NEW.business_date <> OLD.business_date "
        "OR NEW.pricing_version <> OLD.pricing_version OR NEW.reserved_usd <> OLD.reserved_usd THEN "
        "RAISE EXCEPTION 'Agent budget authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.state = 'settled' AND NEW.settled_usd IS NULL THEN "
        "RAISE EXCEPTION 'settled Agent budget requires usage' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_budget_transition BEFORE UPDATE "
        "ON agent_daily_budget_reservations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_budget_transition()"
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_agent_budget_delete() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'agent budget reservation is durable' "
        "USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_budget_delete BEFORE DELETE "
        "ON agent_daily_budget_reservations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_agent_budget_delete()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_job_change() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN "
        "IF OLD.schema_version <> 'agent_research_job_v1' THEN "
        "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END IF; "
        "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'agent job is durable' "
        "USING ERRCODE = '23514'; END IF; "
        "IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN "
        "RAISE EXCEPTION 'agent job authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF (OLD.status = 'QUEUED' AND NEW.status = 'RUNNING') OR "
        "(OLD.status = 'RUNNING' AND NEW.status IN ('COMPLETED','FAILED','ABSTAINED')) "
        "THEN RETURN NEW; END IF; "
        "RAISE EXCEPTION 'invalid agent job transition' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_jobs_change BEFORE UPDATE OR DELETE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_job_change()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_agent_jobs_change ON jobs")
    op.execute("DROP TRIGGER trg_price_series_manifests_frozen_authority ON price_series_manifests")
    op.execute("DROP TRIGGER trg_data_artifacts_frozen_authority ON data_artifacts")
    op.execute("DROP TRIGGER trg_00_price_bar_records_authority_guard ON price_bar_records")
    op.execute("DROP TRIGGER trg_price_bar_set_authorities_validate ON price_bar_set_authorities")
    op.execute("DROP TRIGGER trg_price_bar_set_authorities_immutable ON price_bar_set_authorities")
    op.execute("DROP TRIGGER trg_agent_budget_delete ON agent_daily_budget_reservations")
    op.execute("DROP TRIGGER trg_agent_budget_transition ON agent_daily_budget_reservations")
    op.execute("DROP TRIGGER trg_agent_output_evidence_validate ON agent_research_output_evidence")
    op.execute("DROP TRIGGER trg_agent_snapshot_evidence_validate ON agent_data_snapshot_evidence")
    op.execute("DROP FUNCTION nexus_prevent_frozen_price_authority_change()")
    op.execute("DROP FUNCTION nexus_validate_agent_snapshot_evidence()")
    for table in (
        "agent_daily_budget_reservations",
        "agent_research_output_evidence",
        "agent_data_snapshot_evidence",
        "agent_research_outputs",
        "agent_research_events",
        "agent_research_runs",
        "agent_model_configs",
        "agent_prompt_versions",
        "price_bar_set_authorities",
        "price_bar_records",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_validate_price_bar_set_authority()")
    op.execute("DROP FUNCTION nexus_guard_price_bar_set_mutation()")
    op.execute("DROP FUNCTION nexus_prepare_price_bar_set(uuid)")
    op.execute("DROP FUNCTION nexus_price_bar_set_canonical_v1(uuid)")
    op.drop_table("agent_daily_budget_reservations")
    op.drop_table("agent_research_output_evidence")
    op.drop_table("agent_research_outputs")
    op.drop_table("agent_research_events")
    op.drop_table("agent_data_snapshot_evidence")
    op.drop_table("agent_research_runs")
    op.drop_table("agent_model_configs")
    op.drop_table("agent_prompt_versions")
    op.drop_table("price_bar_set_authorities")
    op.drop_table("price_bar_records")
    op.drop_constraint(
        op.f("ck_price_series_manifests_interval_allowed"),
        "price_series_manifests",
        type_="check",
    )
    op.drop_constraint(
        op.f("uq_price_series_manifests_evidence_ref_id"),
        "price_series_manifests",
        type_="unique",
    )
    op.drop_constraint(
        op.f("fk_price_series_evidence_ref"),
        "price_series_manifests",
        type_="foreignkey",
    )
    op.drop_column("price_series_manifests", "interval")
    op.drop_column("price_series_manifests", "evidence_ref_id")
    op.drop_constraint(
        op.f("ck_research_evidence_refs_location_content_addressed"),
        "research_evidence_refs",
        type_="check",
    )
    op.create_check_constraint(
        "location_content_addressed",
        "research_evidence_refs",
        "(location LIKE 'memory://sha256/' || content_hash || '%') OR "
        "(location LIKE 'package://%' AND location LIKE '%@sha256:' || content_hash || '%') OR "
        "(location LIKE 's3://%' AND (location LIKE '%/sha256/' || content_hash || '%' "
        "OR location ~ '[?&]versionId=[^&#]+'))",
    )
    op.execute("DROP FUNCTION nexus_validate_agent_run_transition()")
    op.execute("DROP FUNCTION nexus_validate_agent_budget_transition()")
    op.execute("DROP FUNCTION nexus_prevent_agent_budget_delete()")
    op.execute("DROP FUNCTION nexus_validate_agent_job_change()")
    op.execute("DROP FUNCTION nexus_validate_agent_output_evidence()")
    op.execute("DROP FUNCTION nexus_prevent_agent_immutable_change()")
