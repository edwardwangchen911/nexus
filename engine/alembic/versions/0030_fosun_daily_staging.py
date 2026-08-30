"""Add append-only raw staging for rate-limited Fosun daily shards.

Revision ID: 0030_fosun_daily_staging
Revises: 0029_fosun_bar_lineage
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0030_fosun_daily_staging"
down_revision: str | Sequence[str] | None = "0029_fosun_bar_lineage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the durable reservation/fetched boundary used by daily shards."""
    # PostgreSQL executes same-event triggers by name. Put the status/normalized
    # binding guard before the raw/calendar transport guard so every valid
    # publication observes one lock order: status -> normalized -> raw -> calendar.
    op.execute(
        "ALTER TRIGGER trg_price_series_fosun_transport "
        "ON price_series_manifests RENAME TO "
        "trg_price_series_manifests_z_fosun_transport"
    )
    op.create_table(
        "fosun_daily_bar_staging",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("root_job_id", sa.UUID(), nullable=False),
        sa.Column("fetch_job_id", sa.UUID(), nullable=False),
        sa.Column("instrument_id", sa.UUID(), nullable=False),
        sa.Column("record_kind", sa.String(20), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("child_input_hash", sa.String(64), nullable=False),
        sa.Column("calendar_authority_id", sa.UUID(), nullable=False),
        sa.Column("universe_snapshot_id", sa.UUID(), nullable=False),
        sa.Column("universe_content_hash", sa.String(64), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("total_instruments", sa.Integer(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.clock_timestamp(),
        ),
        sa.Column("request_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_artifact_id", sa.UUID(), nullable=True),
        sa.Column("raw_artifact_identity_sha256", sa.String(64), nullable=True),
        sa.Column("gateway_request_id", sa.UUID(), nullable=True),
        sa.Column("request_body_sha256", sa.String(64), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_capability_id", sa.UUID(), nullable=True),
        sa.Column("provider_status_id", sa.UUID(), nullable=True),
        sa.Column("provider_status_version", sa.Integer(), nullable=True),
        sa.Column("provider_contract_hash", sa.String(64), nullable=True),
        sa.Column("provider_authorized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_mode", sa.String(20), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_fosun_daily_bar_staging"),
        sa.ForeignKeyConstraint(
            ["root_job_id"], ["jobs.id"], name="fk_fosun_daily_staging_root_job"
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_fosun_daily_staging_instrument",
        ),
        sa.ForeignKeyConstraint(
            ["calendar_authority_id"],
            ["exchange_calendar_authorities.id"],
            name="fk_fosun_daily_staging_calendar",
        ),
        sa.ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name="fk_fosun_daily_staging_universe",
        ),
        sa.ForeignKeyConstraint(
            ["raw_artifact_id"],
            ["data_artifacts.id"],
            name="fk_fosun_daily_staging_raw_artifact",
        ),
        sa.ForeignKeyConstraint(
            ["provider_capability_id"],
            ["provider_capabilities.id"],
            name="fk_fosun_daily_staging_provider_capability",
        ),
        sa.ForeignKeyConstraint(
            ["provider_status_id"],
            ["integration_status.id"],
            name="fk_fosun_daily_staging_provider_status",
        ),
        sa.UniqueConstraint(
            "fetch_job_id",
            "record_kind",
            name="uq_fosun_daily_staging_fetch_kind",
        ),
        sa.UniqueConstraint(
            "root_job_id",
            "instrument_id",
            "record_kind",
            name="uq_fosun_daily_staging_root_instrument_kind",
        ),
        sa.CheckConstraint(
            "record_kind IN ('reservation','fetched')",
            name="record_kind_allowed",
        ),
        sa.CheckConstraint(
            "schema_version = 'fosun_daily_staging_v1'",
            name="schema_version_allowed",
        ),
        sa.CheckConstraint(
            "child_input_hash ~ '^[0-9a-f]{64}$' AND universe_content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint(
            "ordinal >= 0 AND total_instruments BETWEEN 1 AND 49 AND ordinal < total_instruments",
            name="shard_bounds",
        ),
        sa.CheckConstraint(
            "request_start_at < request_end_at",
            name="request_window_order",
        ),
        sa.CheckConstraint(
            "(record_kind = 'reservation' AND "
            "num_nonnulls(raw_artifact_id,raw_artifact_identity_sha256,gateway_request_id,request_body_sha256,"
            "sent_at,received_at,provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at,"
            "delivery_mode) = 0) OR "
            "(record_kind = 'fetched' AND "
            "num_nonnulls(raw_artifact_id,raw_artifact_identity_sha256,gateway_request_id,request_body_sha256,"
            "sent_at,received_at,provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at,"
            "delivery_mode) = 12)",
            name="record_shape",
        ),
        sa.CheckConstraint(
            "raw_artifact_identity_sha256 IS NULL OR "
            "raw_artifact_identity_sha256 ~ '^[0-9a-f]{64}$'",
            name="raw_artifact_identity_hash_canonical",
        ),
        sa.CheckConstraint(
            "request_body_sha256 IS NULL OR request_body_sha256 ~ '^[0-9a-f]{64}$'",
            name="request_hash_canonical",
        ),
        sa.CheckConstraint(
            "provider_contract_hash IS NULL OR provider_contract_hash ~ '^[0-9a-f]{64}$'",
            name="contract_hash_canonical",
        ),
        sa.CheckConstraint(
            "provider_status_version IS NULL OR provider_status_version > 0",
            name="status_version_positive",
        ),
        sa.CheckConstraint(
            "delivery_mode IS NULL OR delivery_mode IN ('unknown','real_time','delayed')",
            name="delivery_mode_allowed",
        ),
        sa.CheckConstraint(
            "sent_at IS NULL OR (received_at >= sent_at AND provider_authorized_at >= received_at)",
            name="transport_time_order",
        ),
    )
    op.create_index(
        "ix_fosun_daily_staging_root",
        "fosun_daily_bar_staging",
        ["root_job_id", "record_kind"],
    )
    op.create_index(
        "ix_fosun_daily_staging_artifact",
        "fosun_daily_bar_staging",
        ["raw_artifact_id"],
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_fosun_daily_staging_change() "
        "RETURNS trigger LANGUAGE plpgsql "
        "SET search_path TO pg_catalog, public, pg_temp AS $$ BEGIN "
        "RAISE EXCEPTION 'Fosun daily staging is append-only' USING ERRCODE = '23514'; "
        "END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_fosun_daily_bar_staging_immutable "
        "BEFORE UPDATE OR DELETE ON fosun_daily_bar_staging FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_fosun_daily_staging_change()"
    )
    op.execute(
        "CREATE TRIGGER trg_fosun_daily_bar_staging_created_at_immutable "
        "BEFORE UPDATE ON fosun_daily_bar_staging FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_created_at_change()"
    )


def downgrade() -> None:
    """Refuse to discard staging evidence or an installed runtime grant."""
    # Serialize the empty/grant checks with every concurrent INSERT and keep the
    # lock until DROP TABLE. An old 0030 runtime or the schema owner therefore
    # cannot commit evidence between the guard and destructive DDL.
    op.execute("LOCK TABLE public.fosun_daily_bar_staging IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM fosun_daily_bar_staging)
             OR EXISTS (
               SELECT 1 FROM pg_catalog.pg_roles AS role
               WHERE role.rolname <> current_user
                 AND NOT role.rolsuper
                 AND role.rolcanlogin
                 AND has_table_privilege(
                   role.oid, 'public.fosun_daily_bar_staging',
                   'SELECT,INSERT,UPDATE,DELETE'
                 )
             ) THEN
            RAISE EXCEPTION 'Fosun daily staging prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        "DROP TRIGGER trg_fosun_daily_bar_staging_created_at_immutable ON fosun_daily_bar_staging"
    )
    op.execute("DROP TRIGGER trg_fosun_daily_bar_staging_immutable ON fosun_daily_bar_staging")
    op.execute("DROP FUNCTION nexus_prevent_fosun_daily_staging_change()")
    op.drop_index(
        "ix_fosun_daily_staging_artifact",
        table_name="fosun_daily_bar_staging",
    )
    op.drop_index(
        "ix_fosun_daily_staging_root",
        table_name="fosun_daily_bar_staging",
    )
    op.drop_table("fosun_daily_bar_staging")
    op.execute(
        "ALTER TRIGGER trg_price_series_manifests_z_fosun_transport "
        "ON price_series_manifests RENAME TO trg_price_series_fosun_transport"
    )
