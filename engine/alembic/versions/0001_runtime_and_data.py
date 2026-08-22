"""Create the Phase 0–1 runtime and data metadata tables.

Revision ID: 0001_runtime_and_data
Revises:
Create Date: 2026-08-22
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0001_runtime_and_data"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
UTC = sa.DateTime(timezone=True)
CREATED_AT = sa.Column("created_at", UTC, nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"))


def _id() -> sa.Column[object]:
    return sa.Column("id", UUID, nullable=False, primary_key=True)


def upgrade() -> None:
    op.create_table(
        "provider_runs",
        _id(),
        sa.Column("provider", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("started_at", UTC, nullable=False),
        sa.Column("completed_at", UTC),
        CREATED_AT,
        sa.UniqueConstraint("provider", "provider_object_id", "revision", name="uq_provider_runs_object"),
    )
    op.create_table(
        "provider_capabilities",
        _id(),
        sa.Column("provider", sa.String(100), nullable=False),
        sa.Column("capability", sa.String(100), nullable=False),
        sa.Column("contract_hash", sa.String(64), nullable=False),
        sa.Column("expires_at", UTC, nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("provider", "capability", "contract_hash", name="uq_provider_capabilities_contract"),
    )
    op.create_table(
        "source_policies",
        _id(),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("key", "version", name="uq_source_policies_key_version"),
    )
    op.create_table(
        "data_snapshots",
        _id(),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("observed_at", UTC, nullable=False),
        sa.Column("available_at", UTC, nullable=False),
        sa.Column("received_at", UTC, nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("source", "provider_object_id", "revision", name="uq_data_snapshots_source_object_revision"),
    )
    op.create_table(
        "quality_reports",
        _id(),
        sa.Column("data_snapshot_id", UUID, nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"], name="fk_quality_reports_snapshot"),
    )
    op.create_table(
        "data_artifacts",
        _id(),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("mime_type", sa.String(255), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("observed_at", UTC, nullable=False),
        sa.Column("available_at", UTC, nullable=False),
        sa.Column("received_at", UTC, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("sha256", name="uq_data_artifacts_sha256"),
    )
    op.create_table(
        "price_series_manifests",
        _id(),
        sa.Column("data_artifact_id", UUID, nullable=False),
        sa.Column("instrument", sa.String(100), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("feed", sa.String(100), nullable=False),
        sa.Column("adjustment", sa.String(100), nullable=False),
        sa.Column("start_at", UTC, nullable=False),
        sa.Column("end_at", UTC, nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        CREATED_AT,
        sa.ForeignKeyConstraint(["data_artifact_id"], ["data_artifacts.id"], name="fk_price_series_manifests_artifact"),
    )
    op.create_table(
        "macro_observations",
        _id(),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("series", sa.String(255), nullable=False),
        sa.Column("observed_at", UTC, nullable=False),
        sa.Column("available_at", UTC, nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("value", sa.Numeric(38, 12), nullable=False),
        sa.Column("unit", sa.String(100), nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("source", "series", "observed_at", "revision", name="uq_macro_observations_revision"),
    )
    op.create_table(
        "integration_status",
        _id(),
        sa.Column("provider", sa.String(100), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("last_success_at", UTC),
        CREATED_AT,
        sa.UniqueConstraint("provider", name="uq_integration_status_provider"),
    )
    op.create_table(
        "jobs",
        _id(),
        sa.Column("job_key", sa.String(255), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("job_key", name="uq_jobs_job_key"),
    )
    op.create_table(
        "job_leases",
        _id(),
        sa.Column("job_id", UUID, nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("expires_at", UTC, nullable=False),
        CREATED_AT,
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name="fk_job_leases_job"),
        sa.UniqueConstraint("job_id", name="uq_job_leases_job_id"),
    )
    op.create_table(
        "outbox_events",
        _id(),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("aggregate_id", UUID, nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("published_at", UTC),
        CREATED_AT,
    )
    op.create_table(
        "idempotency_records",
        _id(),
        sa.Column("actor_id", sa.String(255), nullable=False),
        sa.Column("route", sa.String(255), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("response_payload", JSONB, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("actor_id", "route", "idempotency_key", name="uq_idempotency_records_actor_route_key"),
    )
    op.create_table(
        "audit_events",
        _id(),
        sa.Column("actor_id", sa.String(255), nullable=False),
        sa.Column("action", sa.String(255), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        CREATED_AT,
    )
    op.create_table(
        "used_nonces",
        _id(),
        sa.Column("key_id", sa.String(255), nullable=False),
        sa.Column("nonce", sa.String(255), nullable=False),
        sa.Column("used_at", UTC, nullable=False),
        CREATED_AT,
        sa.UniqueConstraint("key_id", "nonce", name="uq_used_nonces_key_nonce"),
    )


def downgrade() -> None:
    op.drop_table("used_nonces")
    op.drop_table("audit_events")
    op.drop_table("idempotency_records")
    op.drop_table("outbox_events")
    op.drop_table("job_leases")
    op.drop_table("jobs")
    op.drop_table("integration_status")
    op.drop_table("macro_observations")
    op.drop_table("price_series_manifests")
    op.drop_table("data_artifacts")
    op.drop_table("quality_reports")
    op.drop_table("data_snapshots")
    op.drop_table("source_policies")
    op.drop_table("provider_capabilities")
    op.drop_table("provider_runs")
