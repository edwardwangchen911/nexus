"""Separate fixed price and disclosure identities, retaining all v1 price contracts.

Revision ID: 0093_monitor_kinds
Revises: 0092_monitor_daily_receipts
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0093_monitor_kinds"
down_revision = "0092_monitor_daily_receipts"
branch_labels = None
depends_on = None

_P = "research_monitor_plans"
_C = "research_monitor_checks"
_T = "research_monitor_triggers"
_R = "research_monitor_coverage"

# Patch only known, exact seams; never rewrite already applied migrations.
_PATCHES = (
    (
        "nexus_monitor_same_condition_ancestor(uuid,uuid)",
        (
            (
                "FROM public.research_monitor_plans WHERE id=current_id",
                "FROM public.research_monitor_plans WHERE id=current_id AND kind='price'",
            ),
            (
                "WHERE p.generation=c.generation-1 AND p.account_id=c.account_id",
                "WHERE p.kind='price' AND p.generation=c.generation-1 AND p.account_id=c.account_id",
            ),
        ),
    ),
    (
        "nexus_validate_agent_snapshot_evidence()",
        (
            (
                "AND trigger.account_id = plan.account_id AND checked.result = 'true'",
                "AND trigger.account_id = plan.account_id AND checked.result = 'true' "
                "AND trigger.kind='price' AND checked.kind='price' AND source_plan.kind='price' AND plan.kind='price'",
            ),
        ),
    ),
    (
        "nexus_monitor_validate_frozen_members(uuid)",
        (
            (
                "WHERE p.id::text=context->>'plan_id'",
                "WHERE p.kind='price' AND p.id::text=context->>'plan_id'",
            ),
            (
                "WHERE t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'",
                "WHERE t.kind='price' AND c.kind='price' AND p.kind='price' "
                "AND t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'",
            ),
        ),
    ),
    (
        "nexus_validate_daily_monitor_context(uuid)",
        (
            (
                "WHERE p.id::text=parent->>'plan_id'",
                "WHERE p.kind='price' AND p.id::text=parent->>'plan_id'",
            ),
            (
                "WHERE t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'",
                "WHERE t.kind='price' AND c.kind='price' AND p.kind='price' "
                "AND t.id::text=item->>'trigger_id' AND c.id::text=item->>'check_id'",
            ),
        ),
    ),
    (
        "nexus_daily_monitor_receipt_guard()",
        (
            (
                "IF NOT FOUND OR trigger_row.account_id<>NEW.account_id",
                "IF NOT FOUND OR trigger_row.kind<>'price' OR trigger_row.account_id<>NEW.account_id",
            ),
        ),
    ),
)


def _patch(reverse=False):
    for name, pairs in _PATCHES:
        # Works in online and offline Alembic mode, and fails on contract drift.
        for before, after in pairs:
            old, new = (after, before) if reverse else (before, after)
            op.execute(f"""DO $migration$ DECLARE definition text; BEGIN
              SELECT pg_get_functiondef('public.{name}'::regprocedure) INTO definition;
              IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$) <> 1
              THEN RAISE EXCEPTION 'MONITOR_KIND_GUARD_UNEXPECTED'; END IF;
              EXECUTE replace(definition,$old${old}$old$,$new${new}$new$);
            END $migration$;""")


def upgrade():
    for table in (_P, _C, _T, _R):
        op.add_column(
            table, sa.Column("kind", sa.String(16), nullable=False, server_default="price")
        )
    for table in (_C, _T):
        op.add_column(
            table, sa.Column("event_key", sa.String(128), nullable=False, server_default="")
        )
    op.add_column(
        _P, sa.Column("baseline_snapshot_id", pg.UUID(), sa.ForeignKey("data_snapshots.id"))
    )
    op.add_column(_P, sa.Column("baseline_as_of", sa.DateTime(timezone=True)))
    op.drop_constraint(f"uq_{_P}_account_id", _P, type_="unique")
    op.drop_constraint("uq_monitor_account_output", _P, type_="unique")
    op.create_unique_constraint(
        f"uq_{_P}_account_id", _P, ["account_id", "instrument_id", "kind", "generation"]
    )
    op.create_unique_constraint(
        "uq_monitor_account_output", _P, ["account_id", "source_output_id", "kind"]
    )
    op.create_unique_constraint(
        "uq_monitor_plan_scope_kind", _P, ["id", "account_id", "instrument_id", "kind"]
    )
    op.create_check_constraint("kind_allowed", _P, "kind IN ('price','disclosure')")
    op.create_check_constraint(
        "baseline_kind",
        _P,
        "(kind='price' AND baseline_snapshot_id IS NULL AND baseline_as_of IS NULL) OR "
        "(kind='disclosure' AND baseline_snapshot_id IS NOT NULL AND baseline_as_of IS NOT NULL AND baseline_as_of<=effective_at)",
    )
    op.create_foreign_key(
        "fk_monitor_plan_predecessor_kind",
        _P,
        _P,
        ["predecessor_id", "account_id", "instrument_id", "kind"],
        ["id", "account_id", "instrument_id", "kind"],
    )
    op.drop_constraint(f"uq_{_C}_account_id", _C, type_="unique")
    op.create_unique_constraint(
        f"uq_{_C}_account_id",
        _C,
        ["account_id", "instrument_id", "kind", "scheduled_at", "event_key"],
    )
    op.create_unique_constraint(
        "uq_monitor_check_scope_kind",
        _C,
        ["id", "account_id", "instrument_id", "kind", "event_key"],
    )
    op.create_foreign_key(
        "fk_monitor_check_plan_kind",
        _C,
        _P,
        ["plan_id", "account_id", "instrument_id", "kind"],
        ["id", "account_id", "instrument_id", "kind"],
    )
    op.create_check_constraint(
        "kind_event",
        _C,
        "(kind='price' AND event_key='') OR (kind='disclosure' AND length(event_key)>0)",
    )
    op.drop_constraint(f"uq_{_T}_account_id", _T, type_="unique")
    op.drop_constraint(op.f(f"ck_{_T}_episode_positive"), _T, type_="check")
    op.alter_column(_T, "episode", nullable=True)
    op.create_index(
        "uq_monitor_price_episode",
        _T,
        ["account_id", "instrument_id", "condition_hash", "episode"],
        unique=True,
        postgresql_where=sa.text("kind='price'"),
    )
    op.create_index(
        "uq_monitor_disclosure_event",
        _T,
        ["account_id", "instrument_id", "event_key"],
        unique=True,
        postgresql_where=sa.text("kind='disclosure'"),
    )
    op.create_check_constraint(
        "kind_episode",
        _T,
        "(kind='price' AND event_key='' AND episode IS NOT NULL "
        "AND episode>0 AND cause IN ('initial_match','crossed')) OR "
        "(kind='disclosure' AND event_key ~ '^sec:[0-9]{10}:[0-9]{10}-[0-9]{2}-[0-9]{6}$' "
        "AND episode IS NULL AND cause='disclosure_added')",
    )
    op.create_foreign_key(
        "fk_monitor_trigger_check_kind",
        _T,
        _C,
        ["check_id", "account_id", "instrument_id", "kind", "event_key"],
        ["id", "account_id", "instrument_id", "kind", "event_key"],
    )
    op.drop_constraint(f"uq_{_R}_account_id", _R, type_="unique")
    op.create_unique_constraint(f"uq_{_R}_account_id", _R, ["account_id", "run_id", "kind"])
    op.create_check_constraint("kind_allowed", _R, "kind IN ('price','disclosure')")
    for column, name in (("prior_plan_id", "prior"), ("active_plan_id", "active")):
        op.create_foreign_key(
            f"fk_monitor_coverage_{name}_kind",
            _R,
            _P,
            [column, "account_id", "instrument_id", "kind"],
            ["id", "account_id", "instrument_id", "kind"],
        )
    op.execute("""CREATE FUNCTION public.nexus_monitor_disclosure_baseline_guard()
      RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN
        IF NEW.kind='disclosure' AND NEW.baseline_as_of IS NOT NULL
          AND NEW.baseline_snapshot_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.agent_research_runs r
          JOIN public.data_snapshots b ON b.id=NEW.baseline_snapshot_id
          WHERE r.id=NEW.source_run_id AND r.instrument_id=NEW.instrument_id
            AND r.decision_time=NEW.baseline_as_of
            AND b.source='sec_disclosure_index' AND b.schema_version='sec_disclosure_index_v1'
            AND b.payload->'index'->>'instrument_id'=NEW.instrument_id::text
            AND greatest(b.created_at,b.observed_at,b.available_at,b.received_at)<=NEW.baseline_as_of
        ) THEN RAISE EXCEPTION 'MONITOR_DISCLOSURE_BASELINE_INVALID'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER trg_monitor_disclosure_baseline BEFORE INSERT
      ON research_monitor_plans FOR EACH ROW
      EXECUTE FUNCTION public.nexus_monitor_disclosure_baseline_guard()""")
    _patch()


def downgrade():
    op.execute(f"LOCK TABLE {_P},{_C},{_T},{_R} IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS(SELECT 1 FROM {_P} WHERE kind<>'price')
      OR EXISTS(SELECT 1 FROM {_C} WHERE kind<>'price')
      OR EXISTS(SELECT 1 FROM {_T} WHERE kind<>'price')
      OR EXISTS(SELECT 1 FROM {_R} WHERE kind<>'price')
      THEN RAISE EXCEPTION 'MONITOR_KIND_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    _patch(reverse=True)
    op.execute("DROP TRIGGER trg_monitor_disclosure_baseline ON research_monitor_plans")
    op.execute("DROP FUNCTION public.nexus_monitor_disclosure_baseline_guard()")
    for name in ("fk_monitor_coverage_prior_kind", "fk_monitor_coverage_active_kind"):
        op.drop_constraint(name, _R, type_="foreignkey")
    op.drop_constraint(op.f(f"ck_{_R}_kind_allowed"), _R, type_="check")
    op.drop_constraint(f"uq_{_R}_account_id", _R, type_="unique")
    op.create_unique_constraint(f"uq_{_R}_account_id", _R, ["account_id", "run_id"])
    op.drop_constraint("fk_monitor_trigger_check_kind", _T, type_="foreignkey")
    op.drop_constraint(op.f(f"ck_{_T}_kind_episode"), _T, type_="check")
    op.drop_index("uq_monitor_price_episode", table_name=_T)
    op.drop_index("uq_monitor_disclosure_event", table_name=_T)
    op.alter_column(_T, "episode", nullable=False)
    op.create_check_constraint("episode_positive", _T, "episode>0")
    op.create_unique_constraint(
        f"uq_{_T}_account_id", _T, ["account_id", "instrument_id", "condition_hash", "episode"]
    )
    op.drop_constraint("fk_monitor_check_plan_kind", _C, type_="foreignkey")
    op.drop_constraint("uq_monitor_check_scope_kind", _C, type_="unique")
    op.drop_constraint(op.f(f"ck_{_C}_kind_event"), _C, type_="check")
    op.drop_constraint(f"uq_{_C}_account_id", _C, type_="unique")
    op.create_unique_constraint(
        f"uq_{_C}_account_id", _C, ["account_id", "instrument_id", "scheduled_at"]
    )
    op.drop_constraint("fk_monitor_plan_predecessor_kind", _P, type_="foreignkey")
    op.drop_constraint("uq_monitor_plan_scope_kind", _P, type_="unique")
    op.drop_constraint(op.f(f"ck_{_P}_baseline_kind"), _P, type_="check")
    op.drop_constraint(op.f(f"ck_{_P}_kind_allowed"), _P, type_="check")
    op.drop_constraint(f"uq_{_P}_account_id", _P, type_="unique")
    op.drop_constraint("uq_monitor_account_output", _P, type_="unique")
    op.create_unique_constraint(
        f"uq_{_P}_account_id", _P, ["account_id", "instrument_id", "generation"]
    )
    op.create_unique_constraint("uq_monitor_account_output", _P, ["account_id", "source_output_id"])
    for column in ("baseline_snapshot_id", "baseline_as_of"):
        op.drop_column(_P, column)
    for table in (_C, _T):
        op.drop_column(table, "event_key")
    for table in (_P, _C, _T, _R):
        op.drop_column(table, "kind")
