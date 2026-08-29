"""Harden intraday provenance pairs and frozen bar dependencies.

Revision ID: 0026_intraday_review_hardening
Revises: 0025_intraday_market_monitor
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0026_intraday_review_hardening"
down_revision: str | Sequence[str] | None = "0025_intraday_market_monitor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INTRADAY_DELIVERY_PAIR = (
    "event_kind NOT IN ('trade','quote') OR "
    "(source = 'alpaca' AND evidence_status = 'verified' AND "
    "(((feed = 'iex' OR feed = 'sip') AND delivery_mode = 'real_time') OR "
    "(feed = 'delayed_sip' AND delivery_mode = 'delayed'))) OR "
    "(source = 'fixture' AND evidence_status = 'fixture' "
    "AND feed = 'fixture' AND delivery_mode = 'fixture') OR "
    "(source NOT IN ('alpaca','fixture') AND evidence_status = 'verified')"
)


def upgrade() -> None:
    """Reject contradictory quote provenance and seal intraday bar members."""
    op.drop_constraint("jobs_queue_allowed", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_queue_allowed",
        "jobs",
        "queue IN ('general','provider','broker','strategy_rnd')",
    )
    op.drop_constraint("queue_allowed", "worker_heartbeats", type_="check")
    op.create_check_constraint(
        "queue_allowed",
        "worker_heartbeats",
        "queue IN ('general','provider','broker','strategy_rnd')",
    )
    _move_provider_jobs(source_queue="general", target_queue="provider")
    op.create_check_constraint(
        "intraday_delivery_pair",
        "latest_quotes",
        _INTRADAY_DELIVERY_PAIR,
    )
    _create_quote_artifact_guard()
    _create_intraday_authority_guard()
    _replace_bar_mutation_guard(include_intraday=True)


def downgrade() -> None:
    """Restore the pre-hardening daily-only append guard."""
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM intraday_bar_set_authorities)
            OR EXISTS (
              SELECT 1 FROM latest_quotes
              WHERE evidence_status = 'verified' OR event_kind IN ('trade','quote')
            )
          THEN
            RAISE EXCEPTION 'intraday review facts prevent downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM worker_heartbeats WHERE queue = 'provider')
             OR EXISTS (
               SELECT 1 FROM jobs
               WHERE queue = 'provider' AND status <> 'queued'
             )
          THEN
            RAISE EXCEPTION 'provider runtime history prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    _move_provider_jobs(source_queue="provider", target_queue="general")
    op.drop_constraint("jobs_queue_allowed", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_queue_allowed",
        "jobs",
        "queue IN ('general','broker','strategy_rnd')",
    )
    _replace_bar_mutation_guard(include_intraday=False)
    op.execute(
        "DROP TRIGGER trg_intraday_bar_set_authorities_review "
        "ON intraday_bar_set_authorities"
    )
    op.execute("DROP FUNCTION nexus_validate_intraday_review_authority()")
    op.execute("DROP TRIGGER trg_latest_quotes_raw_artifact ON latest_quotes")
    op.execute("DROP FUNCTION nexus_validate_latest_quote_raw_artifact()")
    op.drop_constraint(
        "intraday_delivery_pair",
        "latest_quotes",
        type_="check",
    )
    op.drop_constraint("queue_allowed", "worker_heartbeats", type_="check")
    op.create_check_constraint(
        "queue_allowed",
        "worker_heartbeats",
        "queue IN ('general','broker','strategy_rnd')",
    )


def _move_provider_jobs(*, source_queue: str, target_queue: str) -> None:
    op.execute(
        f"""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM jobs
            WHERE queue = '{source_queue}'
              AND job_type IN (
                'collect.market','collect.sec_macro','collect.intraday_snapshot'
              )
              AND status = 'running'
          ) THEN
            RAISE EXCEPTION 'provider jobs must be drained before queue migration'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    _set_existing_job_triggers(enabled=False)
    op.execute(
        f"""
        UPDATE jobs SET queue = '{target_queue}'
        WHERE queue = '{source_queue}'
          AND status = 'queued'
          AND job_type IN (
            'collect.market','collect.sec_macro','collect.intraday_snapshot'
          )
        """
    )
    _set_existing_job_triggers(enabled=True)


def _set_existing_job_triggers(*, enabled: bool) -> None:
    action = "ENABLE" if enabled else "DISABLE"
    op.execute(
        f"""
        DO $$
        DECLARE
          trigger_name text;
        BEGIN
          FOREACH trigger_name IN ARRAY ARRAY['trg_jobs_state', 'trg_agent_jobs_change']
          LOOP
            IF EXISTS (
              SELECT 1 FROM pg_trigger
              WHERE tgrelid = 'jobs'::regclass
                AND tgname = trigger_name
                AND NOT tgisinternal
            ) THEN
              EXECUTE format('ALTER TABLE jobs {action} TRIGGER %I', trigger_name);
            END IF;
          END LOOP;
        END $$
        """
    )


def _create_quote_artifact_guard() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1
            FROM latest_quotes AS quote
            LEFT JOIN data_artifacts AS artifact ON artifact.id = quote.data_artifact_id
            WHERE quote.evidence_status = 'verified'
              AND quote.source = 'alpaca'
              AND quote.event_kind IN ('trade','quote')
              AND (
              artifact.id IS NULL
              OR artifact.source <> quote.source
              OR NOT (
                quote.revision = artifact.sha256
                OR right(quote.revision, 65) = ':' || artifact.sha256
              )
            )
          ) THEN
            RAISE EXCEPTION 'verified quote raw artifact is invalid'
              USING ERRCODE = '23514';
          END IF;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION nexus_validate_latest_quote_raw_artifact()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          artifact data_artifacts%ROWTYPE;
        BEGIN
          IF NEW.evidence_status <> 'verified'
             OR NEW.source <> 'alpaca'
             OR NEW.event_kind NOT IN ('trade','quote')
          THEN
            RETURN NEW;
          END IF;
          SELECT * INTO artifact FROM data_artifacts WHERE id = NEW.data_artifact_id;
          IF NOT FOUND
             OR artifact.source <> NEW.source
             OR NOT (
               NEW.revision = artifact.sha256
               OR right(NEW.revision, 65) = ':' || artifact.sha256
             )
          THEN
            RAISE EXCEPTION 'verified quote raw artifact is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_latest_quotes_raw_artifact BEFORE INSERT ON latest_quotes "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_latest_quote_raw_artifact()"
    )


def _create_intraday_authority_guard() -> None:
    invalid = (
        "bar.received_at < bar.observed_at + interval '1 minute' "
        "OR jsonb_typeof(bar.quality_flags) IS DISTINCT FROM 'array' "
        "OR CASE WHEN jsonb_typeof(bar.quality_flags) = 'array' THEN ("
        "jsonb_array_length(bar.quality_flags) > 16 "
        "OR EXISTS (SELECT 1 FROM jsonb_array_elements(bar.quality_flags) AS flag(value) "
        "WHERE jsonb_typeof(flag.value) <> 'string' "
        "OR (flag.value #>> '{}') !~ '^[A-Z][A-Z0-9_]{0,99}$')) "
        "ELSE FALSE END"
    )
    op.execute(
        f"""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1
            FROM intraday_bar_set_authorities AS authority
            JOIN price_bar_records AS bar
              ON bar.price_series_manifest_id = authority.price_series_manifest_id
            WHERE {invalid}
          ) THEN
            RAISE EXCEPTION 'intraday bar review authority is invalid'
              USING ERRCODE = '23514';
          END IF;
        END $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION nexus_validate_intraday_review_authority()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM price_bar_records AS bar
            WHERE bar.price_series_manifest_id = NEW.price_series_manifest_id
              AND ({invalid})
          ) THEN
            RAISE EXCEPTION 'intraday bar review authority is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_intraday_bar_set_authorities_review BEFORE INSERT "
        "ON intraday_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_intraday_review_authority()"
    )


def _replace_bar_mutation_guard(*, include_intraday: bool) -> None:
    intraday_lock = (
        "PERFORM pg_advisory_xact_lock("
        "hashtextextended('intraday-price-series-manifest:' || manifest_id::text, 0));"
        if include_intraday
        else ""
    )
    intraday_authority = (
        " OR EXISTS (SELECT 1 FROM intraday_bar_set_authorities AS authority "
        "WHERE authority.price_series_manifest_id = manifest_id)"
        if include_intraday
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_guard_price_bar_set_mutation()
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
          {intraday_lock}
          IF TG_OP = 'INSERT' AND (
            EXISTS (
              SELECT 1 FROM price_bar_set_authorities AS authority
              WHERE authority.price_series_manifest_id = manifest_id
            ){intraday_authority}
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
