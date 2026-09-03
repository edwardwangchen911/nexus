"""Store append-only rank evidence with forward decisions.

Revision ID: 0037_strategy_rank_checkpoints
Revises: 0036_strategy_rnd_usage_unknown
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0037_strategy_rank_checkpoints"
down_revision = "0036_strategy_rnd_usage_unknown"
branch_labels = None
depends_on = None
_TABLE = "strategy_rank_checkpoints"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("decision_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("previous_decision_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("schema_version", sa.String(30), nullable=False),
        sa.Column("input_snapshot_hash", sa.String(64), nullable=False),
        sa.Column("observations", postgresql.JSONB(), nullable=False),
        sa.Column("checkpoint_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("decision_id"),
        sa.ForeignKeyConstraint(["decision_id"], ["strategy_decisions.id"]),
        sa.ForeignKeyConstraint(["previous_decision_id"], [f"{_TABLE}.decision_id"]),
        sa.CheckConstraint("schema_version = 'rank_checkpoint_v1'", name="schema_version"),
        sa.CheckConstraint(
            "input_snapshot_hash ~ '^[0-9a-f]{64}$' AND checkpoint_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint("jsonb_typeof(observations) = 'object'", name="observations_object"),
        sa.CheckConstraint(
            "previous_decision_id IS NULL OR previous_decision_id <> decision_id",
            name="parent_distinct",
        ),
    )
    op.execute(
        f"CREATE TRIGGER trg_{_TABLE}_immutable BEFORE UPDATE OR DELETE ON {_TABLE} "
        "FOR EACH ROW EXECUTE FUNCTION nexus_reject_paper_fact_mutation()"
    )


def downgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE})")):
        raise RuntimeError("rank checkpoint history prevents downgrade")
    op.drop_table(_TABLE)
