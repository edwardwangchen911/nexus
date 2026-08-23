"""Add immutable three-way optimization proposal persistence."""

from collections.abc import Sequence
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0009_optimization"
down_revision: str | None = "0008_diagnostics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)


def upgrade() -> None:
    op.create_table(
        "optimization_policies",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("key", "version", name="uq_optimization_policies_key_version"),
    )
    op.create_table(
        "optimization_proposal_groups",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("baseline_portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="draft"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("policy_id", _UUID, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["baseline_portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.ForeignKeyConstraint(["policy_id"], ["optimization_policies.id"]),
        sa.CheckConstraint(
            "status IN ('draft','compared','shadowing','validated','rejected','expired')",
            name="status_allowed",
        ),
    )
    op.create_table(
        "optimization_proposals",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_group_id", _UUID, nullable=False),
        sa.Column("variant", sa.String(30), nullable=False),
        sa.Column("baseline_portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("analysis_only", sa.Boolean(), nullable=False),
        sa.Column("strategy_evidence_complete", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_group_id"], ["optimization_proposal_groups.id"]),
        sa.ForeignKeyConstraint(["baseline_portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.UniqueConstraint(
            "proposal_group_id", "variant", name="uq_optimization_proposals_group_variant"
        ),
        sa.CheckConstraint(
            "variant IN ('hold_current','minimum_change','strategy_target')", name="variant_allowed"
        ),
    )
    op.create_table(
        "proposal_positions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("current_weight", _NUMERIC, nullable=False),
        sa.Column("target_weight", _NUMERIC, nullable=False),
        sa.Column("action", sa.String(20), nullable=False),
        sa.Column("reason_code", sa.String(100), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_id"], ["optimization_proposals.id"]),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.UniqueConstraint(
            "proposal_id", "instrument_id", name="uq_proposal_positions_proposal_instrument"
        ),
    )
    op.execute(
        sa.text(
            "INSERT INTO optimization_policies (id, key, version, schema_version, payload) VALUES (:id, 'optimizer', 'optimizer_v1', '1', '{}'::jsonb)"
        ).bindparams(id=UUID("e690b91d-1e24-5de3-b2b2-566a463fca25"))
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_optimization_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'optimization record is immutable' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE FUNCTION nexus_require_three_optimization_variants() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE group_ref uuid; variant_count integer; BEGIN IF TG_TABLE_NAME = 'optimization_proposal_groups' THEN group_ref := NEW.id; ELSE group_ref := COALESCE(NEW.proposal_group_id, OLD.proposal_group_id); END IF; SELECT count(*) INTO variant_count FROM optimization_proposals WHERE proposal_group_id = group_ref; IF variant_count <> 3 THEN RAISE EXCEPTION 'optimization group requires exactly three variants' USING ERRCODE = '23514'; END IF; RETURN NULL; END; $$"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_groups_three_variants AFTER INSERT ON optimization_proposal_groups DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_proposals_three_variants AFTER INSERT OR DELETE ON optimization_proposals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    for table in (
        "optimization_policies",
        "optimization_proposal_groups",
        "optimization_proposals",
        "proposal_positions",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    for table in ("optimization_policies", "optimization_proposals", "proposal_positions"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_optimization_change()"
        )


def downgrade() -> None:
    for table in (
        "proposal_positions",
        "optimization_proposals",
        "optimization_proposal_groups",
        "optimization_policies",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.execute("DROP TRIGGER trg_optimization_proposals_three_variants ON optimization_proposals")
    op.execute(
        "DROP TRIGGER trg_optimization_groups_three_variants ON optimization_proposal_groups"
    )
    op.execute("DROP FUNCTION nexus_require_three_optimization_variants()")
    for table in ("proposal_positions", "optimization_proposals", "optimization_policies"):
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_optimization_change()")
    op.drop_table("proposal_positions")
    op.drop_table("optimization_proposals")
    op.drop_table("optimization_proposal_groups")
    op.drop_table("optimization_policies")
