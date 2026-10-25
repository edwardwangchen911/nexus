"""Seal stock observations and bind one complete notification to all new triggers.

Revision ID: 0094_monitor_batches
Revises: 0093_monitor_kinds
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "0094_monitor_batches"
down_revision = "0093_monitor_kinds"
branch_labels = None
depends_on = None

_VALIDATOR = r"""
CREATE FUNCTION public.nexus_monitor_validate_batch(batch_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE b public.research_monitor_batches%ROWTYPE; payload jsonb; actual jsonb; events jsonb;
  sources jsonb; n integer;
BEGIN
 SELECT * INTO b FROM public.research_monitor_batches WHERE id=batch_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'MONITOR_BATCH_INVALID'; END IF;
 PERFORM 1 FROM public.outbox_events WHERE id=b.outbox_id FOR UPDATE;
 IF jsonb_typeof(b.plan_ids) IS DISTINCT FROM 'array'
  OR jsonb_typeof(b.check_ids) IS DISTINCT FROM 'array'
  OR jsonb_typeof(b.trigger_ids) IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'MONITOR_BATCH_INVALID'; END IF;
 n:=jsonb_array_length(b.trigger_ids);
 IF n>1001 OR jsonb_array_length(b.plan_ids) NOT BETWEEN 1 AND 2
  OR jsonb_array_length(b.check_ids)>1002
  OR (SELECT count(DISTINCT x) FROM jsonb_array_elements_text(b.plan_ids) x)<>jsonb_array_length(b.plan_ids)
  OR (SELECT count(DISTINCT x) FROM jsonb_array_elements_text(b.check_ids) x)<>jsonb_array_length(b.check_ids)
  OR (SELECT count(DISTINCT x) FROM jsonb_array_elements_text(b.trigger_ids) x)<>n
  OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(b.plan_ids) x WHERE NOT EXISTS(
    SELECT 1 FROM public.research_monitor_plans p WHERE p.id::text=x
      AND p.account_id=b.account_id AND p.instrument_id=b.instrument_id))
  OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(b.check_ids) x WHERE NOT EXISTS(
    SELECT 1 FROM public.research_monitor_checks c WHERE c.id::text=x
      AND c.account_id=b.account_id AND c.instrument_id=b.instrument_id
      AND b.plan_ids ? c.plan_id::text AND c.checked_at<=b.checked_at AND c.created_at<=b.sealed_at))
 THEN RAISE EXCEPTION 'MONITOR_BATCH_INVALID'; END IF;
 SELECT coalesce(jsonb_agg(t.id::text ORDER BY t.id::text),'[]'::jsonb),
        coalesce(jsonb_agg(CASE WHEN t.kind='price' THEN 'price:'||t.id::text ELSE t.event_key END
          ORDER BY CASE WHEN t.kind='price' THEN 'price:'||t.id::text ELSE t.event_key END),'[]'::jsonb)
 INTO actual,events FROM public.research_monitor_triggers t WHERE t.batch_id=b.id;
 IF actual<>b.trigger_ids OR EXISTS(
   SELECT 1 FROM public.research_monitor_triggers t JOIN public.research_monitor_checks c ON c.id=t.check_id
   WHERE (t.batch_id=b.id OR t.outbox_id=b.outbox_id) AND (t.batch_id IS DISTINCT FROM b.id OR t.account_id<>b.account_id OR t.instrument_id<>b.instrument_id
    OR t.outbox_id IS DISTINCT FROM b.outbox_id OR NOT(b.check_ids ? c.id::text)
    OR c.result<>'true' OR t.created_at>b.sealed_at))
 THEN RAISE EXCEPTION 'MONITOR_BATCH_MEMBERS_INVALID'; END IF;
 IF n=0 THEN
   IF b.outbox_id IS NOT NULL THEN RAISE EXCEPTION 'MONITOR_BATCH_OUTBOX_INVALID'; END IF;
   RETURN;
 END IF;
 SELECT jsonb_agg(x.id ORDER BY x.id) INTO sources FROM (
   SELECT DISTINCT p.source_run_id::text id FROM public.research_monitor_triggers t
   JOIN public.research_monitor_checks c ON c.id=t.check_id
   JOIN public.research_monitor_plans p ON p.id=c.plan_id WHERE t.batch_id=b.id) x;
 SELECT o.payload INTO payload FROM public.outbox_events o WHERE o.id=b.outbox_id
   AND o.aggregate_id=b.id AND o.event_type='research.monitor.batch_triggered'
   AND o.schema_version='research_monitor_batch_v1';
 IF NOT FOUND OR (payload-'occurred_at') IS DISTINCT FROM jsonb_build_object(
   'schema_version','research_monitor_batch_v1','batch_id',b.id::text,'instrument_id',b.instrument_id::text,
   'trigger_ids',actual,'event_ids',events,'source_run_ids',sources)
   OR (payload->>'occurred_at')::timestamptz IS DISTINCT FROM b.checked_at
 THEN RAISE EXCEPTION 'MONITOR_BATCH_OUTBOX_INVALID'; END IF;
END $$;
CREATE FUNCTION public.nexus_monitor_batch_guard() RETURNS trigger LANGUAGE plpgsql
 SET search_path=pg_catalog,public AS $$ BEGIN
 IF TG_TABLE_NAME='research_monitor_triggers' THEN
   PERFORM 1 FROM public.outbox_events WHERE id=NEW.outbox_id FOR UPDATE;
 END IF;
 IF TG_TABLE_NAME='research_monitor_batches' THEN PERFORM public.nexus_monitor_validate_batch(NEW.id);
 ELSIF NEW.batch_id IS NULL AND EXISTS (
   SELECT 1 FROM public.research_monitor_batches b WHERE b.outbox_id=NEW.outbox_id
 ) THEN RAISE EXCEPTION 'MONITOR_BATCH_MEMBERS_INVALID';
 ELSIF NEW.batch_id IS NOT NULL AND NOT EXISTS (
   SELECT 1 FROM public.research_monitor_batches b WHERE b.id=NEW.batch_id
    AND b.account_id=NEW.account_id AND b.instrument_id=NEW.instrument_id
    AND b.outbox_id=NEW.outbox_id AND b.trigger_ids ? NEW.id::text
    AND b.check_ids ? NEW.check_id::text
 ) THEN RAISE EXCEPTION 'MONITOR_BATCH_MEMBERS_INVALID'; END IF;
 RETURN NULL; END $$;
"""

_PATCHES = (
    (
        "nexus_validate_agent_snapshot_evidence()",
        "AND trigger.kind='price'",
        "AND trigger.batch_id IS NULL AND trigger.kind='price'",
    ),
    (
        "nexus_monitor_validate_frozen_members(uuid)",
        "WHERE t.kind='price'",
        "WHERE t.batch_id IS NULL AND t.kind='price'",
    ),
    (
        "nexus_validate_daily_monitor_context(uuid)",
        "WHERE t.kind='price'",
        "WHERE t.batch_id IS NULL AND t.kind='price'",
    ),
    (
        "nexus_daily_monitor_receipt_guard()",
        "OR trigger_row.kind<>'price'",
        "OR trigger_row.batch_id IS NOT NULL OR trigger_row.kind<>'price'",
    ),
)


def _patch(reverse=False):
    for name, before, after in _PATCHES:
        old, new = (after, before) if reverse else (before, after)
        op.execute(f"""DO $migration$ DECLARE definition text; BEGIN
    SELECT pg_get_functiondef('public.{name}'::regprocedure) INTO definition;
    IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$)<>1
    THEN RAISE EXCEPTION 'MONITOR_BATCH_GUARD_UNEXPECTED'; END IF;
    EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")


def upgrade():
    op.create_table(
        "research_monitor_batches",
        sa.Column("id", pg.UUID(), primary_key=True),
        sa.Column("account_id", pg.UUID(), sa.ForeignKey("paper_accounts.id"), nullable=False),
        sa.Column("instrument_id", pg.UUID(), sa.ForeignKey("instruments.id"), nullable=False),
        *(
            sa.Column(name, sa.DateTime(timezone=True), nullable=False)
            for name in ("scheduled_at", "checked_at", "sealed_at")
        ),
        sa.Column("observation_hash", sa.String(64), nullable=False),
        *(
            sa.Column(name, pg.JSONB(), nullable=False)
            for name in ("plan_ids", "check_ids", "trigger_ids")
        ),
        sa.Column("outbox_id", pg.UUID(), sa.ForeignKey("outbox_events.id"), unique=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("account_id", "instrument_id", "scheduled_at", "observation_hash"),
        sa.UniqueConstraint(
            "id", "account_id", "instrument_id", "outbox_id", name="uq_monitor_batch_scope"
        ),
        sa.CheckConstraint("observation_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
        sa.CheckConstraint("scheduled_at<=checked_at AND checked_at<=sealed_at", name="time_order"),
    )
    op.add_column(
        "research_monitor_triggers",
        sa.Column(
            "batch_id",
            pg.UUID(),
            sa.ForeignKey("research_monitor_batches.id", deferrable=True, initially="DEFERRED"),
        ),
    )
    op.create_index(
        "ix_research_monitor_triggers_batch_id", "research_monitor_triggers", ["batch_id"]
    )
    op.drop_constraint(
        "uq_research_monitor_triggers_outbox_id", "research_monitor_triggers", type_="unique"
    )
    op.create_index(
        "uq_monitor_legacy_outbox",
        "research_monitor_triggers",
        ["outbox_id"],
        unique=True,
        postgresql_where=sa.text("batch_id IS NULL"),
    )
    op.create_foreign_key(
        "fk_monitor_trigger_batch_scope",
        "research_monitor_triggers",
        "research_monitor_batches",
        ["batch_id", "account_id", "instrument_id", "outbox_id"],
        ["id", "account_id", "instrument_id", "outbox_id"],
        deferrable=True,
        initially="DEFERRED",
    )
    op.execute(
        "ALTER TABLE research_monitor_triggers ALTER CONSTRAINT fk_research_monitor_triggers_outbox_id_outbox_events DEFERRABLE INITIALLY DEFERRED"
    )
    first, second = _VALIDATOR.split("CREATE FUNCTION public.nexus_monitor_batch_guard()", 1)
    op.execute(first)
    op.execute("CREATE FUNCTION public.nexus_monitor_batch_guard()" + second)
    op.execute(
        "CREATE TRIGGER trg_monitor_batches_immutable BEFORE UPDATE OR DELETE ON research_monitor_batches FOR EACH ROW EXECUTE FUNCTION nexus_monitor_immutable()"
    )
    for table in ("research_monitor_batches", "research_monitor_triggers"):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_{table}_batch AFTER INSERT OR UPDATE ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_monitor_batch_guard()"
        )
    _patch()


def downgrade():
    op.execute(
        "LOCK TABLE research_monitor_batches,research_monitor_triggers IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_monitor_batches) OR EXISTS(SELECT 1 FROM research_monitor_triggers WHERE batch_id IS NOT NULL) THEN RAISE EXCEPTION 'MONITOR_BATCH_HISTORY_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    _patch(reverse=True)
    for table in ("research_monitor_batches", "research_monitor_triggers"):
        op.execute(f"DROP TRIGGER trg_{table}_batch ON {table}")
    op.execute("DROP TRIGGER trg_monitor_batches_immutable ON research_monitor_batches")
    op.execute("DROP FUNCTION public.nexus_monitor_batch_guard()")
    op.execute("DROP FUNCTION public.nexus_monitor_validate_batch(uuid)")
    op.drop_constraint(
        "fk_monitor_trigger_batch_scope", "research_monitor_triggers", type_="foreignkey"
    )
    op.drop_index("uq_monitor_legacy_outbox", table_name="research_monitor_triggers")
    op.create_unique_constraint(
        "uq_research_monitor_triggers_outbox_id", "research_monitor_triggers", ["outbox_id"]
    )
    op.execute(
        "ALTER TABLE research_monitor_triggers ALTER CONSTRAINT fk_research_monitor_triggers_outbox_id_outbox_events NOT DEFERRABLE INITIALLY IMMEDIATE"
    )
    op.drop_column("research_monitor_triggers", "batch_id")
    op.drop_table("research_monitor_batches")
