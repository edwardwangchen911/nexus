"""Append target-plan child batches after actual sale settlement; preserve roots."""

import sqlalchemy as sa
from alembic import op

revision = "0073_portfolio_plan_continuation"
down_revision = "0072_economic_later_windows"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("paper_order_batches", sa.Column("previous_batch_id", sa.Uuid(), nullable=True))
    op.drop_constraint(
        "uq_paper_order_batches_account_decision", "paper_order_batches", type_="unique"
    )
    op.create_index(
        "uq_paper_order_batches_root",
        "paper_order_batches",
        ["account_id", "strategy_decision_id"],
        unique=True,
        postgresql_where=sa.text("previous_batch_id IS NULL"),
    )
    op.create_unique_constraint(
        "uq_paper_order_batches_previous", "paper_order_batches", ["previous_batch_id"]
    )
    op.create_foreign_key(
        "fk_paper_order_batches_previous",
        "paper_order_batches",
        "paper_order_batches",
        ["previous_batch_id", "account_id", "strategy_decision_id"],
        ["id", "account_id", "strategy_decision_id"],
    )
    op.create_check_constraint(
        op.f("ck_paper_order_batches_previous_target"),
        "paper_order_batches",
        "previous_batch_id IS NULL OR (target_version_id IS NOT NULL AND previous_batch_id<>id)",
    )
    op.execute("""CREATE FUNCTION nexus_paper_plan_continuation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE parent paper_order_batches;
    BEGIN
      IF NEW.previous_batch_id IS NULL THEN RETURN NEW; END IF;
      SELECT * INTO parent FROM paper_order_batches WHERE id=NEW.previous_batch_id;
      IF parent.id IS NULL OR parent.account_id<>NEW.account_id
        OR parent.strategy_decision_id<>NEW.strategy_decision_id
        OR parent.target_version_id IS DISTINCT FROM NEW.target_version_id
        OR NEW.target_version_id IS NULL OR NEW.evaluated_at<parent.evaluated_at
        OR NEW.target_version_id IS DISTINCT FROM (SELECT id FROM paper_target_versions WHERE account_id=NEW.account_id ORDER BY version_seq DESC LIMIT 1)
        OR NOT EXISTS(SELECT 1 FROM strategy_decisions WHERE id=NEW.strategy_decision_id AND strategy_version='agent_portfolio_proposal_v1')
        OR NOT EXISTS(SELECT 1 FROM paper_orders o JOIN LATERAL (SELECT * FROM paper_order_events WHERE order_id=o.id ORDER BY event_seq DESC LIMIT 1) e ON true JOIN paper_fills f ON f.id=e.source_fill_id AND f.order_id=o.id AND f.account_id=NEW.account_id WHERE o.batch_id=parent.id AND o.side='sell' AND e.to_status='filled' AND NEW.evaluated_at>=greatest(e.occurred_at,e.created_at,f.evaluated_at,f.created_at))
        OR EXISTS(SELECT 1 FROM paper_orders o LEFT JOIN LATERAL (SELECT * FROM paper_order_events WHERE order_id=o.id ORDER BY event_seq DESC LIMIT 1) e ON true WHERE o.batch_id=parent.id AND (e.id IS NULL OR NEW.evaluated_at<greatest(e.created_at,e.occurred_at) OR (o.risk_outcome='rejected' AND e.to_status<>'risk_rejected') OR (o.risk_outcome<>'rejected' AND (e.to_status<>'filled' OR e.source_fill_id IS NULL OR o.quantity IS DISTINCT FROM (SELECT sum(quantity) FROM paper_fills WHERE order_id=o.id)))))
      THEN RAISE EXCEPTION 'PORTFOLIO_PLAN_NOT_READY'; END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER paper_plan_continuation_guard BEFORE INSERT ON paper_order_batches FOR EACH ROW EXECUTE FUNCTION nexus_paper_plan_continuation_guard()"
    )


def downgrade():
    op.execute(
        """DO $$ BEGIN IF EXISTS(SELECT 1 FROM paper_order_batches WHERE previous_batch_id IS NOT NULL) THEN RAISE EXCEPTION 'PORTFOLIO_PLAN_DOWNGRADE_FORBIDDEN'; END IF; END $$"""
    )
    op.execute("DROP TRIGGER paper_plan_continuation_guard ON paper_order_batches")
    op.execute("DROP FUNCTION nexus_paper_plan_continuation_guard()")
    op.drop_constraint(
        op.f("ck_paper_order_batches_previous_target"), "paper_order_batches", type_="check"
    )
    op.drop_constraint("fk_paper_order_batches_previous", "paper_order_batches", type_="foreignkey")
    op.drop_constraint("uq_paper_order_batches_previous", "paper_order_batches", type_="unique")
    op.drop_index("uq_paper_order_batches_root", table_name="paper_order_batches")
    op.create_unique_constraint(
        "uq_paper_order_batches_account_decision",
        "paper_order_batches",
        ["account_id", "strategy_decision_id"],
    )
    op.drop_column("paper_order_batches", "previous_batch_id")
