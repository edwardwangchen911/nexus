"""Admit source-explicit v3 intraday Jobs including Schwab Level One.

Revision ID: 0055_schwab_intraday_job_v3
Revises: 0054_schwab_stream_policy
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0055_schwab_intraday_job_v3"
down_revision: str | Sequence[str] | None = "0054_schwab_stream_policy"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOCK_KEY = "nexus:intraday-schema-admission:v1"


def _source_feed_predicate(row: str, *, version: str) -> str:
    existing = (
        f"({row}.payload ->> 'source' = 'fosun' "
        f"AND {row}.payload ->> 'feed' = 'l1') OR "
        f"({row}.payload ->> 'source' = 'alpaca' "
        f"AND {row}.payload ->> 'feed' IN ('iex', 'sip', 'delayed_sip'))"
    )
    if version == "v2":
        return existing
    return (
        f"{existing} OR "
        f"({row}.payload ->> 'source' = 'schwab' "
        f"AND {row}.payload ->> 'feed' = 'l1' "
        f"AND {row}.payload ->> 'delivery_mode' = 'real_time' "
        f"AND {row}.payload ->> 'include_minute_bars' = 'false' "
        f"AND {row}.payload -> 'quote_authority' = 'null'::pg_catalog.jsonb "
        f"AND {row}.payload -> 'bars_authority' = 'null'::pg_catalog.jsonb)"
    )


def _create_admission(*, version: str) -> None:
    schema = f"intraday_snapshot_job_{version}"
    op.execute(
        f"CREATE UNIQUE INDEX uq_jobs_intraday_{version}_bucket "
        "ON public.jobs (effective_at) "
        "WHERE job_type = 'collect.intraday_snapshot' "
        f"AND schema_version = '{schema}'"
    )
    new_source_feed = _source_feed_predicate("NEW", version=version)
    old_source_feed = _source_feed_predicate("OLD", version=version)
    op.execute(
        f"""
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
               NEW.schema_version IS DISTINCT FROM '{schema}'
               OR NEW.payload ->> 'schema' IS DISTINCT FROM '{schema}'
               OR NOT COALESCE(({new_source_feed}), FALSE)
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
               OR pg_catalog.mod(EXTRACT(EPOCH FROM NEW.effective_at), 15) <> 0
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
               OLD.schema_version IS DISTINCT FROM '{schema}'
               OR OLD.payload ->> 'schema' IS DISTINCT FROM '{schema}'
               OR NOT COALESCE(({old_source_feed}), FALSE)
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
               OR pg_catalog.mod(EXTRACT(EPOCH FROM OLD.effective_at), 15) <> 0
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


def upgrade() -> None:
    """Fence actionable v2 work before admitting only v3 Jobs."""
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        f"pg_catalog.hashtextextended('{_LOCK_KEY}', 0))"
    )
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.jobs
            WHERE job_type = 'collect.intraday_snapshot'
              AND schema_version = 'intraday_snapshot_job_v2'
              AND status IN ('queued', 'running', 'failed')
          ) THEN
            RAISE EXCEPTION 'intraday v2 Jobs must drain before v3 admission'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER trg_jobs_intraday_schema_admission ON public.jobs")
    op.execute("DROP FUNCTION public.nexus_guard_intraday_job_schema_admission()")
    op.execute("DROP INDEX public.uq_jobs_intraday_v2_bucket")
    _create_admission(version="v3")


def downgrade() -> None:
    """Fence actionable v3 work and restore the exact v2 admission shape."""
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        f"pg_catalog.hashtextextended('{_LOCK_KEY}', 0))"
    )
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.worker_heartbeats
            WHERE queue = 'provider'
              AND migration_head = '0055_schwab_intraday_job_v3'
              AND seen_at >= pg_catalog.clock_timestamp() - INTERVAL '2 minutes'
          ) OR EXISTS (
            SELECT 1 FROM public.jobs
            WHERE job_type = 'collect.intraday_snapshot'
              AND schema_version = 'intraday_snapshot_job_v3'
              AND status IN ('queued', 'running', 'failed')
          ) THEN
            RAISE EXCEPTION 'intraday v3 admission prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER trg_jobs_intraday_schema_admission ON public.jobs")
    op.execute("DROP FUNCTION public.nexus_guard_intraday_job_schema_admission()")
    op.execute("DROP INDEX public.uq_jobs_intraday_v3_bucket")
    _create_admission(version="v2")
