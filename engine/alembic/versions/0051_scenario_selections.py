"""Add a narrow user selection projection for frozen portfolio scenarios.

Revision ID: 0051_scenario_selections
Revises: 0050_scenario_spread_cost_policy
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0051_scenario_selections"
down_revision: str | None = "0050_scenario_spread_cost_policy"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "portfolio_scenario_selections",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_id", sa.String(length=255), nullable=False),
        sa.Column("scenario_claim_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("selection_claim_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("portfolio_snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("diagnostic_report_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scenario_result_hash", sa.String(length=64), nullable=False),
        sa.Column("choice", sa.String(length=32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("selected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "choice IN ('accepted_for_analysis','rejected','watch')",
            name=op.f("ck_portfolio_scenario_selections_choice_allowed"),
        ),
        sa.CheckConstraint(
            "scenario_result_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_portfolio_scenario_selections_result_hash_canonical"),
        ),
        sa.CheckConstraint(
            "version >= 1",
            name=op.f("ck_portfolio_scenario_selections_version_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["diagnostic_report_id", "portfolio_snapshot_id"],
            [
                "portfolio_diagnostic_reports.id",
                "portfolio_diagnostic_reports.portfolio_snapshot_id",
            ],
            name=op.f("fk_scenario_selections_diagnostic_snapshot"),
        ),
        sa.ForeignKeyConstraint(
            ["portfolio_snapshot_id"],
            ["portfolio_snapshots.id"],
            name=op.f("fk_portfolio_scenario_selections_portfolio_snapshot_id_portfolio_snapshots"),
        ),
        sa.ForeignKeyConstraint(
            ["scenario_claim_id"],
            ["idempotency_records.id"],
            name=op.f("fk_portfolio_scenario_selections_scenario_claim_id_idempotency_records"),
        ),
        sa.ForeignKeyConstraint(
            ["selection_claim_id"],
            ["idempotency_records.id"],
            name=op.f("fk_portfolio_scenario_selections_selection_claim_id_idempotency_records"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_portfolio_scenario_selections")),
        sa.UniqueConstraint(
            "actor_id",
            "scenario_result_hash",
            name=op.f("uq_scenario_selections_actor_result"),
        ),
        sa.UniqueConstraint(
            "selection_claim_id",
            name=op.f("uq_scenario_selections_selection_claim"),
        ),
    )
    op.execute(
        """
        CREATE FUNCTION nexus_validate_portfolio_scenario_selection() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF TG_OP = 'INSERT' AND NEW.version <> 1 THEN
            RAISE EXCEPTION 'scenario selection must start at version one' USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'UPDATE' THEN
            IF NEW.actor_id IS DISTINCT FROM OLD.actor_id
              OR NEW.scenario_claim_id IS DISTINCT FROM OLD.scenario_claim_id
              OR NEW.portfolio_snapshot_id IS DISTINCT FROM OLD.portfolio_snapshot_id
              OR NEW.diagnostic_report_id IS DISTINCT FROM OLD.diagnostic_report_id
              OR NEW.scenario_result_hash IS DISTINCT FROM OLD.scenario_result_hash
              OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
              RAISE EXCEPTION 'scenario selection authority is immutable' USING ERRCODE = '23514';
            END IF;
            IF NEW.version <> OLD.version + 1 OR NEW.selected_at < OLD.selected_at THEN
              RAISE EXCEPTION 'scenario selection transition is invalid' USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END; $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_portfolio_scenario_selections_validate
        BEFORE INSERT OR UPDATE ON portfolio_scenario_selections
        FOR EACH ROW EXECUTE FUNCTION nexus_validate_portfolio_scenario_selection()
        """
    )


def downgrade() -> None:
    if op.get_bind().execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM portfolio_scenario_selections)")
    ).scalar():
        raise RuntimeError(
            "scenario selections exist; preserve or explicitly remove them before downgrade"
        )
    op.execute(
        "DROP TRIGGER trg_portfolio_scenario_selections_validate "
        "ON portfolio_scenario_selections"
    )
    op.execute("DROP FUNCTION nexus_validate_portfolio_scenario_selection()")
    op.drop_table("portfolio_scenario_selections")
