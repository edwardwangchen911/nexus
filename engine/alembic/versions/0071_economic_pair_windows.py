"""Freeze first common economic windows without changing historical business facts."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0071_economic_pair_windows"
down_revision = "0070_economic_pair_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    stamp = sa.DateTime(timezone=True)
    op.create_table(
        "economic_common_windows",
        sa.Column("id", uuid, primary_key=True),
        sa.Column("pair_slot_id", uuid, sa.ForeignKey("economic_pair_slots.id"), nullable=False),
        sa.Column("window_seq", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        *(
            sa.Column(name, stamp, nullable=False)
            for name in ("eligible_at", "window_start", "window_end", "frozen_at")
        ),
        sa.Column("qualification", postgresql.JSONB(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("created_at", stamp, server_default=sa.func.clock_timestamp(), nullable=False),
        sa.UniqueConstraint("pair_slot_id", "window_seq"),
        sa.CheckConstraint("window_seq=1", name="first_window_only"),
        sa.CheckConstraint("status IN ('frozen','missed')", name="status_allowed"),
        sa.CheckConstraint(
            "window_end=window_start+interval '5 minutes' AND eligible_at<window_start",
            name="bounds_exact",
        ),
        sa.CheckConstraint(
            "status<>'frozen' OR frozen_at<window_start", name="frozen_before_start"
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )
    op.create_table(
        "economic_window_orders",
        sa.Column("id", uuid, primary_key=True),
        sa.Column("window_id", uuid, sa.ForeignKey("economic_common_windows.id"), nullable=False),
        sa.Column("lane", sa.String(16), nullable=False),
        sa.Column("order_id", uuid, nullable=False),
        sa.Column("account_id", uuid, nullable=False),
        sa.Column("instrument_id", uuid, sa.ForeignKey("instruments.id"), nullable=False),
        sa.Column("created_at", stamp, server_default=sa.func.clock_timestamp(), nullable=False),
        sa.UniqueConstraint("window_id", "order_id"),
        sa.ForeignKeyConstraint(
            ["order_id", "account_id"], ["paper_orders.id", "paper_orders.account_id"]
        ),
        sa.CheckConstraint("lane IN ('incumbent','challenger')", name="lane_allowed"),
    )
    op.execute("""CREATE FUNCTION nexus_economic_window_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF jsonb_typeof(NEW.qualification) IS DISTINCT FROM 'object'
        OR jsonb_typeof(NEW.qualification->'lanes') IS DISTINCT FROM 'array'
        OR jsonb_typeof(NEW.qualification->'orders') IS DISTINCT FROM 'array'
        OR (NEW.qualification->>'window_seq')::int IS DISTINCT FROM NEW.window_seq
        OR (NEW.qualification->>'eligible_at')::timestamptz IS DISTINCT FROM NEW.eligible_at
        OR (NEW.qualification->>'window_start')::timestamptz IS DISTINCT FROM NEW.window_start
        OR (NEW.qualification->>'window_end')::timestamptz IS DISTINCT FROM NEW.window_end
        OR (NEW.qualification->>'frozen_at')::timestamptz IS DISTINCT FROM NEW.frozen_at
        OR NEW.window_start<>date_trunc('minute',NEW.eligible_at)+interval '1 minute'
        OR NEW.frozen_at<NEW.eligible_at
        OR (NEW.status='missed' AND NEW.frozen_at<NEW.window_start)
        OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.qualification->'lanes') a
          JOIN economic_lane_cycles l ON l.pair_slot_id=NEW.pair_slot_id AND l.lane='incumbent'
          WHERE a->>'lane'='incumbent' AND a->>'cycle_id'=l.cycle_id::text)
        OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.qualification->'lanes') a
          JOIN economic_lane_cycles l ON l.pair_slot_id=NEW.pair_slot_id AND l.lane='challenger'
          WHERE a->>'lane'='challenger' AND a->>'cycle_id'=l.cycle_id::text)
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.qualification->'lanes') a
          LEFT JOIN economic_lane_cycles l ON l.pair_slot_id=NEW.pair_slot_id
            AND l.lane=a->>'lane' AND l.cycle_id::text=a->>'cycle_id'
          LEFT JOIN agent_paper_cycle_events e ON e.cycle_id=l.cycle_id
            AND e.sequence=(a->>'event_sequence')::integer
          WHERE e.cycle_id IS NULL OR e.event_hash IS DISTINCT FROM a->>'event_hash'
            OR CASE WHEN EXISTS (SELECT 1 FROM paper_orders o JOIN strategy_decisions d
                ON d.id=o.strategy_decision_id WHERE d.agent_paper_cycle_id=l.cycle_id
                AND NEW.qualification->'orders' @> jsonb_build_array(o.id::text))
              THEN e.to_status IS DISTINCT FROM 'awaiting_market'
              ELSE e.to_status IS DISTINCT FROM 'completed'
                OR e.result_code IS DISTINCT FROM 'NO_ORDER_REQUIRED' END)
        OR NEW.content_hash <> encode(sha256(convert_to(nexus_economic_json_text(NEW.qualification,false),'UTF8')),'hex')
        OR NEW.qualification->>'pair_slot_id' IS DISTINCT FROM NEW.pair_slot_id::text
        OR NEW.qualification->>'status' IS DISTINCT FROM NEW.status
        OR jsonb_array_length(NEW.qualification->'lanes') <> 2
        OR NOT EXISTS (SELECT 1 FROM economic_pair_slots s WHERE s.id=NEW.pair_slot_id
          AND NEW.window_end<=s.market_close_at AND NEW.eligible_at>=s.decision_at)
      THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute("""CREATE TRIGGER economic_window_guard BEFORE INSERT ON economic_common_windows
      FOR EACH ROW EXECUTE FUNCTION nexus_economic_window_guard()""")
    op.execute("""CREATE FUNCTION nexus_economic_window_order_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM economic_common_windows w
        JOIN economic_lane_cycles l ON l.pair_slot_id=w.pair_slot_id AND l.lane=NEW.lane
        JOIN strategy_decisions d ON d.agent_paper_cycle_id=l.cycle_id AND d.account_id=l.account_id
        JOIN paper_orders o ON o.strategy_decision_id=d.id AND o.account_id=l.account_id
        WHERE w.id=NEW.window_id AND NEW.order_id=o.id AND NEW.account_id=o.account_id
          AND NEW.instrument_id=o.instrument_id AND EXISTS (SELECT 1 FROM paper_order_events e
            WHERE e.order_id=o.id AND e.to_status='submitted')
          AND w.qualification->'orders' @> jsonb_build_array(o.id::text))
      THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_ORDER_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute("""CREATE TRIGGER economic_window_order_guard BEFORE INSERT ON economic_window_orders
      FOR EACH ROW EXECUTE FUNCTION nexus_economic_window_order_guard()""")

    op.execute("""CREATE FUNCTION nexus_economic_window_complete() RETURNS trigger
      LANGUAGE plpgsql AS $$ DECLARE window_ref uuid; wanted jsonb; actual jsonb;
      BEGIN
        IF TG_TABLE_NAME='economic_common_windows' THEN window_ref:=NEW.id;
        ELSE window_ref:=NEW.window_id; END IF;
        SELECT qualification->'orders' INTO wanted FROM economic_common_windows WHERE id=window_ref;
        SELECT coalesce(jsonb_agg(order_id::text ORDER BY order_id::text),'[]'::jsonb)
          INTO actual FROM economic_window_orders WHERE window_id=window_ref;
        IF wanted IS DISTINCT FROM actual OR jsonb_array_length(actual)=0
          THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_ORDER_SET_INVALID'; END IF;
        RETURN NEW;
      END $$""")
    for table in ("economic_common_windows", "economic_window_orders"):
        op.execute(f"""CREATE CONSTRAINT TRIGGER economic_window_complete
          AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION nexus_economic_window_complete()""")
    for table in ("economic_common_windows", "economic_window_orders"):
        op.execute(
            f"CREATE TRIGGER economic_window_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_reject_economic_pair_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER economic_window_no_truncate BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION nexus_reject_economic_pair_mutation()"
        )


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM economic_common_windows) OR EXISTS(SELECT 1 FROM economic_window_orders)
      THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    op.drop_table("economic_window_orders")
    op.drop_table("economic_common_windows")
    op.execute("DROP FUNCTION nexus_economic_window_complete()")
    op.execute("DROP FUNCTION nexus_economic_window_order_guard()")
    op.execute("DROP FUNCTION nexus_economic_window_guard()")
