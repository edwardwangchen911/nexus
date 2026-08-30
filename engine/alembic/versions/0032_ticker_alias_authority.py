"""Make ticker identity bijective at every point in time.

Revision ID: 0032_ticker_alias_authority
Revises: 0031_intraday_schema_admission
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0032_ticker_alias_authority"
down_revision: str | Sequence[str] | None = "0031_intraday_schema_admission"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Reject ambiguous or concurrently introduced ticker authority."""
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute("LOCK TABLE public.instrument_aliases IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1
            FROM public.instrument_aliases AS left_alias
            JOIN public.instrument_aliases AS right_alias
              ON left_alias.id < right_alias.id
             AND left_alias.alias_type = 'ticker'
             AND right_alias.alias_type = 'ticker'
             AND (
               left_alias.instrument_id = right_alias.instrument_id
               OR pg_catalog.upper(left_alias.symbol)
                  = pg_catalog.upper(right_alias.symbol)
             )
             AND pg_catalog.daterange(
                   left_alias.valid_from, left_alias.valid_to, '[]'
                 ) && pg_catalog.daterange(
                   right_alias.valid_from, right_alias.valid_to, '[]'
                 )
          ) THEN
            RAISE EXCEPTION 'ambiguous ticker authority prevents upgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        """
        ALTER TABLE public.instrument_aliases
        ADD CONSTRAINT ex_instrument_aliases_ticker_instrument_period
        EXCLUDE USING gist (
          instrument_id WITH =,
          pg_catalog.daterange(valid_from, valid_to, '[]') WITH &&
        ) WHERE (alias_type = 'ticker')
        NOT DEFERRABLE INITIALLY IMMEDIATE
        """
    )
    op.execute(
        """
        ALTER TABLE public.instrument_aliases
        ADD CONSTRAINT ex_instrument_aliases_ticker_symbol_period
        EXCLUDE USING gist (
          (pg_catalog.upper(symbol)) WITH =,
          pg_catalog.daterange(valid_from, valid_to, '[]') WITH &&
        ) WHERE (alias_type = 'ticker')
        NOT DEFERRABLE INITIALLY IMMEDIATE
        """
    )


def downgrade() -> None:
    """Remove ticker exclusion only while the current Provider is stopped."""
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended('nexus:intraday-schema-admission:v1', 0))"
    )
    op.execute("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.instrument_aliases IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1
            FROM public.worker_heartbeats
            WHERE queue IN ('general', 'provider', 'broker')
              AND migration_head = '0032_ticker_alias_authority'
              AND seen_at >= pg_catalog.clock_timestamp() - INTERVAL '2 minutes'
          ) OR EXISTS (
            SELECT 1
            FROM public.jobs
            WHERE job_type IN (
              'daily.pipeline',
              'collect.market',
              'collect.fosun_daily_instrument',
              'publish.fosun_daily',
              'collect.intraday_snapshot',
              'broker.simulation.dispatch'
            )
              AND status IN ('queued', 'running', 'failed')
          ) THEN
            RAISE EXCEPTION 'ticker alias authority prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    op.execute(
        "ALTER TABLE public.instrument_aliases "
        "DROP CONSTRAINT ex_instrument_aliases_ticker_symbol_period"
    )
    op.execute(
        "ALTER TABLE public.instrument_aliases "
        "DROP CONSTRAINT ex_instrument_aliases_ticker_instrument_period"
    )
