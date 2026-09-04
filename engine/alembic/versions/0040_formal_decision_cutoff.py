"""Separate formal optimization evidence cutoff from portfolio valuation.

Revision ID: 0040_formal_decision_cutoff
Revises: 0039_issuer_nav_history

Legacy rows remain NULL and are never rewritten. This is not Shadow admission.
"""

from alembic import op
import sqlalchemy as sa

revision = "0040_formal_decision_cutoff"
down_revision = "0039_issuer_nav_history"
branch_labels = None
depends_on = None

_TABLES = (
    ("optimization_admission_authorities", "trg_optimization_admission_formal_clock"),
    ("optimization_proposal_groups", "trg_optimization_groups_formal_clock"),
)

_CLOCK_GUARD = r"""
CREATE FUNCTION public.nexus_validate_formal_optimization_clock()
RETURNS trigger LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
DECLARE
    manifest public.data_snapshots%ROWTYPE;
    bound_portfolio_snapshot_id uuid;
    is_formal boolean;
    manifest_valuation timestamptz;
    manifest_cutoff timestamptz;
    materialized timestamptz;
    clock_key text;
BEGIN
    IF TG_TABLE_NAME = 'optimization_proposal_groups' THEN
        IF TG_OP = 'UPDATE'
           AND NEW.decision_cutoff_at IS DISTINCT FROM OLD.decision_cutoff_at THEN
            RAISE EXCEPTION 'formal optimization clock is immutable' USING ERRCODE = '23514';
        END IF;
        bound_portfolio_snapshot_id := NEW.baseline_portfolio_snapshot_id;
    ELSE
        bound_portfolio_snapshot_id := NEW.portfolio_snapshot_id;
    END IF;

    SELECT * INTO manifest FROM public.data_snapshots WHERE id = NEW.data_snapshot_id;
    is_formal := manifest.source = 'nexus_optimization_formal'
                 OR manifest.schema_version = 'optimization_formal_inputs_v2';
    IF NEW.decision_cutoff_at IS NULL THEN
        IF is_formal THEN
            RAISE EXCEPTION 'formal optimization clock binding is invalid' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF manifest.source IS DISTINCT FROM 'nexus_optimization_formal'
       OR manifest.schema_version IS DISTINCT FROM 'optimization_formal_inputs_v2'
       OR manifest.provider_object_id IS DISTINCT FROM bound_portfolio_snapshot_id::text
       OR manifest.payload->>'schema_version' IS DISTINCT FROM 'optimization_formal_inputs_v2'
       OR manifest.payload->>'portfolio_snapshot_id' IS DISTINCT FROM bound_portfolio_snapshot_id::text
       OR manifest.observed_at IS DISTINCT FROM NEW.valuation_time
       OR NOT isfinite(NEW.valuation_time) OR NOT isfinite(NEW.decision_cutoff_at)
       OR NEW.decision_cutoff_at < NEW.valuation_time
       OR NOT EXISTS (
           SELECT 1 FROM public.portfolio_snapshots p
           WHERE p.id = bound_portfolio_snapshot_id AND p.as_of = NEW.valuation_time
       ) THEN
        RAISE EXCEPTION 'formal optimization clock binding is invalid' USING ERRCODE = '23514';
    END IF;

    FOREACH clock_key IN ARRAY ARRAY['valuation_time', 'decision_cutoff_at', 'materialized_at'] LOOP
        IF jsonb_typeof(manifest.payload->clock_key) IS DISTINCT FROM 'string'
           OR (manifest.payload->>clock_key) !~
              '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|\+00:00)$' THEN
            RAISE EXCEPTION 'formal optimization clock binding is invalid' USING ERRCODE = '23514';
        END IF;
    END LOOP;
    BEGIN
        manifest_valuation := (manifest.payload->>'valuation_time')::timestamptz;
        manifest_cutoff := (manifest.payload->>'decision_cutoff_at')::timestamptz;
        materialized := (manifest.payload->>'materialized_at')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION 'formal optimization clock binding is invalid' USING ERRCODE = '23514';
    END;
    IF manifest_valuation IS DISTINCT FROM NEW.valuation_time
       OR manifest_cutoff IS DISTINCT FROM NEW.decision_cutoff_at
       OR materialized < manifest_cutoff
       OR manifest.available_at IS DISTINCT FROM materialized
       OR manifest.received_at IS DISTINCT FROM materialized THEN
        RAISE EXCEPTION 'formal optimization clock binding is invalid' USING ERRCODE = '23514';
    END IF;

    IF TG_TABLE_NAME = 'optimization_proposal_groups' THEN
        IF NEW.status IN ('shadowing', 'validated') THEN
            RAISE EXCEPTION 'PROPOSAL_NOT_EXECUTABLE' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$
"""


def upgrade() -> None:
    for table, _ in _TABLES:
        op.add_column(table, sa.Column("decision_cutoff_at", sa.DateTime(timezone=True)))
        op.create_check_constraint(
            "decision_cutoff_after_valuation",
            table,
            "decision_cutoff_at IS NULL OR decision_cutoff_at >= valuation_time",
        )
    # Keep all historical selection, immutability, and lifecycle functions intact.
    op.execute(_CLOCK_GUARD)
    for table, trigger in _TABLES:
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE ON public.{table} "
            "FOR EACH ROW EXECUTE FUNCTION public.nexus_validate_formal_optimization_clock()"
        )


def downgrade() -> None:
    # Check after waiting for existing writers; checking before DDL locking could
    # miss an uncommitted formal row and then silently drop its evidence clock.
    op.execute(
        "LOCK TABLE public.optimization_admission_authorities, "
        "public.optimization_proposal_groups IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM public.optimization_admission_authorities
                       WHERE decision_cutoff_at IS NOT NULL)
               OR EXISTS (SELECT 1 FROM public.optimization_proposal_groups
                          WHERE decision_cutoff_at IS NOT NULL) THEN
                RAISE EXCEPTION 'formal optimization evidence prevents downgrade'
                    USING ERRCODE = '23514';
            END IF;
        END $$
    """)
    for table, trigger in _TABLES:
        op.execute(f"DROP TRIGGER {trigger} ON public.{table}")
    op.execute("DROP FUNCTION public.nexus_validate_formal_optimization_clock()")
    for table, _ in _TABLES:
        op.drop_constraint("decision_cutoff_after_valuation", table, type_="check")
        op.drop_column(table, "decision_cutoff_at")
