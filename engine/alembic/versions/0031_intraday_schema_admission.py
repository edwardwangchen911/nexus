"""Admit only source-explicit v2 intraday Jobs after the provider cutover.

Revision ID: 0031_intraday_schema_admission
Revises: 0030_fosun_daily_staging
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0031_intraday_schema_admission"
down_revision: str | Sequence[str] | None = "0030_fosun_daily_staging"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Close the legacy scheduler gate-to-claim insertion race in PostgreSQL."""
    op.execute(
        "CREATE UNIQUE INDEX uq_jobs_intraday_v2_bucket "
        "ON public.jobs (effective_at) "
        "WHERE job_type = 'collect.intraday_snapshot' "
        "AND schema_version = 'intraday_snapshot_job_v2'"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_intraday_job_schema_admission()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF TG_LEVEL <> 'ROW'
             OR TG_WHEN <> 'BEFORE'
             OR TG_TABLE_SCHEMA <> 'public'
             OR TG_RELID <> 'public.jobs'::pg_catalog.regclass
             OR TG_OP NOT IN ('INSERT', 'UPDATE')
          THEN
            RAISE EXCEPTION 'unsupported intraday Job admission trigger context'
              USING ERRCODE = '23514';
          END IF;

          IF TG_OP = 'INSERT'
             AND NEW.job_type = 'collect.intraday_snapshot'
          THEN
            IF (
               NEW.schema_version IS DISTINCT FROM 'intraday_snapshot_job_v2'
               OR NEW.payload ->> 'schema'
                  IS DISTINCT FROM 'intraday_snapshot_job_v2'
               OR NOT COALESCE(
                 (NEW.payload ->> 'source' = 'fosun'
                  AND NEW.payload ->> 'feed' = 'l1')
                 OR
                 (NEW.payload ->> 'source' = 'alpaca'
                  AND NEW.payload ->> 'feed' IN ('iex', 'sip', 'delayed_sip')),
                 FALSE
               )
               OR NEW.retry_policy IS DISTINCT FROM 'never'
               OR NEW.max_attempts IS DISTINCT FROM 1
               OR NOT pg_catalog.pg_input_is_valid(
                    COALESCE(NEW.payload ->> 'bucket_start', ''),
                    'timestamp with time zone'
                  )
               OR NOT pg_catalog.pg_input_is_valid(
                    COALESCE(NEW.payload ->> 'bucket_end', ''),
                    'timestamp with time zone'
                  )
             )
            THEN
              RAISE EXCEPTION 'intraday Job schema is not admitted'
                USING ERRCODE = '23514';
            END IF;

            IF (
               NEW.effective_at IS DISTINCT FROM
                  (NEW.payload ->> 'bucket_start')::pg_catalog.timestamptz
               OR (NEW.payload ->> 'bucket_end')::pg_catalog.timestamptz
                  IS DISTINCT FROM NEW.effective_at + INTERVAL '15 seconds'
               OR pg_catalog.mod(
                    EXTRACT(EPOCH FROM NEW.effective_at),
                    15
                  ) <> 0
             )
            THEN
              RAISE EXCEPTION 'intraday Job schema is not admitted'
                USING ERRCODE = '23514';
            END IF;
          END IF;

          IF TG_OP = 'UPDATE'
             AND OLD.job_type = 'collect.intraday_snapshot'
             AND OLD.status IN ('failed', 'dead_letter')
             AND NEW.status = 'queued'
          THEN
            IF (
               OLD.schema_version IS DISTINCT FROM 'intraday_snapshot_job_v2'
               OR OLD.payload ->> 'schema'
                  IS DISTINCT FROM 'intraday_snapshot_job_v2'
               OR NOT COALESCE(
                 (OLD.payload ->> 'source' = 'fosun'
                  AND OLD.payload ->> 'feed' = 'l1')
                 OR
                 (OLD.payload ->> 'source' = 'alpaca'
                  AND OLD.payload ->> 'feed' IN ('iex', 'sip', 'delayed_sip')),
                 FALSE
               )
               OR OLD.retry_policy IS DISTINCT FROM 'never'
               OR OLD.max_attempts IS DISTINCT FROM 1
               OR NOT pg_catalog.pg_input_is_valid(
                    COALESCE(OLD.payload ->> 'bucket_start', ''),
                    'timestamp with time zone'
                  )
               OR NOT pg_catalog.pg_input_is_valid(
                    COALESCE(OLD.payload ->> 'bucket_end', ''),
                    'timestamp with time zone'
                  )
             )
            THEN
              RAISE EXCEPTION 'legacy intraday Job retry is not admitted'
                USING ERRCODE = '23514';
            END IF;

            IF (
               OLD.effective_at IS DISTINCT FROM
                  (OLD.payload ->> 'bucket_start')::pg_catalog.timestamptz
               OR (OLD.payload ->> 'bucket_end')::pg_catalog.timestamptz
                  IS DISTINCT FROM OLD.effective_at + INTERVAL '15 seconds'
               OR pg_catalog.mod(
                    EXTRACT(EPOCH FROM OLD.effective_at),
                    15
                  ) <> 0
             )
            THEN
              RAISE EXCEPTION 'legacy intraday Job retry is not admitted'
                USING ERRCODE = '23514';
            END IF;
          END IF;

          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_jobs_intraday_schema_admission "
        "BEFORE INSERT OR UPDATE ON jobs FOR EACH ROW "
        "EXECUTE FUNCTION nexus_guard_intraday_job_schema_admission()"
    )


def downgrade() -> None:
    """Remove only the admission guard during a synchronized code rollback."""
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended('nexus:intraday-schema-admission:v1', 0))"
    )
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1
            FROM public.worker_heartbeats
            WHERE queue = 'provider'
              AND migration_head = '0031_intraday_schema_admission'
              AND seen_at >= pg_catalog.clock_timestamp() - INTERVAL '2 minutes'
          ) OR EXISTS (
            SELECT 1
            FROM public.jobs
            WHERE job_type = 'collect.intraday_snapshot'
              AND schema_version = 'intraday_snapshot_job_v2'
              AND payload ->> 'schema' = 'intraday_snapshot_job_v2'
              AND status IN ('queued', 'running', 'failed')
          ) THEN
            RAISE EXCEPTION 'intraday admission prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER trg_jobs_intraday_schema_admission ON jobs")
    op.execute("DROP FUNCTION nexus_guard_intraday_job_schema_admission()")
    op.execute("DROP INDEX public.uq_jobs_intraday_v2_bucket")
