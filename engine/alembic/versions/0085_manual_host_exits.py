"""Append-only local host exits; keep heartbeat and business occupancy audit."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0085_manual_host_exits"
down_revision = "0084_research_only_requests"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "manual_host_exits",
        sa.Column(
            "heartbeat_id",
            postgresql.UUID(),
            sa.ForeignKey("worker_heartbeats.id"),
            primary_key=True,
        ),
        sa.Column("queue", sa.String(20), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("process_version", sa.String(64), nullable=False),
        sa.Column("migration_head", sa.String(64), nullable=False),
        sa.Column("final_seen_at", sa.DateTime(timezone=True), nullable=False),
        # Filled exclusively by the INSERT guard using the real database clock.
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "started_at <= final_seen_at AND final_seen_at <= ended_at", name="time_order"
        ),
    )
    op.execute(r"""CREATE FUNCTION nexus_manual_host_exit_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE h worker_heartbeats;
    BEGIN
      IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'MANUAL_HOST_EXIT_IMMUTABLE'; END IF;
      SELECT * INTO h FROM worker_heartbeats WHERE id=NEW.heartbeat_id FOR UPDATE;
      IF NOT FOUND OR NEW.ended_at IS NOT NULL OR NEW.queue <> 'agent_paper'
        OR NEW.worker_id !~ '^agent-paper-loop-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:manual:[0-9a-f]{64}$'
        OR NEW.process_version !~ '^nexus-manual-paper-v1\+[0-9a-f]{40}$'
        OR (NEW.queue,NEW.worker_id,NEW.started_at,NEW.process_version,NEW.migration_head,NEW.final_seen_at)
          IS DISTINCT FROM (h.queue,h.worker_id,h.started_at,h.process_version,h.migration_head,h.seen_at)
        OR h.seen_at > clock_timestamp()
      THEN RAISE EXCEPTION 'MANUAL_HOST_EXIT_INVALID'; END IF;
      -- Keep every audit field unchanged, but advance the parent MVCC version:
      -- an old REPEATABLE READ snapshot must fail rather than revive this host.
      UPDATE worker_heartbeats SET seen_at=seen_at WHERE id=h.id;
      NEW.ended_at := clock_timestamp();
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER trg_manual_host_exit BEFORE INSERT OR UPDATE OR DELETE ON manual_host_exits FOR EACH ROW EXECUTE FUNCTION nexus_manual_host_exit_guard()"
    )
    # UPDATE already owns the parent row lock. This VOLATILE trigger's separate
    # query sees an exit committed while the UPDATE was waiting at READ COMMITTED.
    # Other queues never read the new table and need no additional privileges.
    op.execute("""CREATE FUNCTION nexus_guard_closed_manual_host() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF OLD.queue = 'agent_paper' THEN
        IF EXISTS(SELECT 1 FROM manual_host_exits WHERE heartbeat_id=OLD.id) THEN
          RAISE EXCEPTION 'MANUAL_HOST_ALREADY_EXITED';
        END IF;
      END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER trg_worker_heartbeats_closed_host BEFORE UPDATE ON worker_heartbeats FOR EACH ROW EXECUTE FUNCTION nexus_guard_closed_manual_host()"
    )


def downgrade() -> None:
    op.execute("LOCK TABLE worker_heartbeats, manual_host_exits IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM manual_host_exits) THEN RAISE EXCEPTION 'MANUAL_HOST_EXIT_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.execute("DROP TRIGGER trg_worker_heartbeats_closed_host ON worker_heartbeats")
    op.execute("DROP FUNCTION nexus_guard_closed_manual_host()")
    op.drop_table("manual_host_exits")
    op.execute("DROP FUNCTION nexus_manual_host_exit_guard()")
