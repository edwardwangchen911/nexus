"""Add immutable, append-only research committee audit records."""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0011_agents"
down_revision: str | None = "0010_research_data"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)
_IMMUTABLE_TABLES = (
    "agent_prompt_versions",
    "agent_model_configs",
    "agent_research_outputs",
)


def upgrade() -> None:
    op.create_table(
        "agent_prompt_versions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("version", name="uq_agent_prompt_versions_version"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )
    op.create_table(
        "agent_model_configs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("model_id", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("max_tool_calls", sa.Integer(), nullable=False),
        sa.Column("max_output_tokens", sa.Integer(), nullable=False),
        sa.Column("max_cost_usd", _NUMERIC, nullable=False),
        sa.Column("pricing_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pricing_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("model_id", "version", name="uq_agent_model_configs_identity"),
        sa.CheckConstraint("config_hash ~ '^[0-9a-f]{64}$'", name="config_hash_canonical"),
        sa.CheckConstraint("max_tool_calls BETWEEN 0 AND 8", name="max_tool_calls_bounded"),
        sa.CheckConstraint("max_output_tokens BETWEEN 1 AND 16384", name="max_output_tokens_bounded"),
        sa.CheckConstraint("max_cost_usd >= 0", name="max_cost_nonnegative"),
        sa.CheckConstraint("pricing_expires_at > pricing_available_at", name="pricing_time_ordered"),
    )
    op.create_table(
        "agent_research_runs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("prompt_version_id", _UUID, nullable=False),
        sa.Column("model_config_id", _UUID, nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="prepared"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("schema_hash", sa.String(64), nullable=False),
        sa.Column("model_hash", sa.String(64), nullable=False),
        sa.Column("prompt_hash", sa.String(64), nullable=False),
        sa.Column("tool_hash", sa.String(64), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("reserved_cost_usd", _NUMERIC, nullable=False),
        sa.Column("settled_cost_usd", _NUMERIC, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"], name="fk_agent_runs_instrument"),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"], name="fk_agent_runs_data_snapshot"),
        sa.ForeignKeyConstraint(["prompt_version_id"], ["agent_prompt_versions.id"], name="fk_agent_runs_prompt_version"),
        sa.ForeignKeyConstraint(["model_config_id"], ["agent_model_configs.id"], name="fk_agent_runs_model_config"),
        sa.CheckConstraint("status IN ('prepared','running','validated','audited','synthesized','completed','failed','abstained')", name="status_allowed"),
        sa.CheckConstraint("retry_count BETWEEN 0 AND 1", name="retry_count_bounded"),
        sa.CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        sa.CheckConstraint("settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd", name="settled_cost_bounded"),
        sa.CheckConstraint("schema_hash ~ '^[0-9a-f]{64}$'", name="schema_hash_canonical"),
        sa.CheckConstraint("model_hash ~ '^[0-9a-f]{64}$'", name="model_hash_canonical"),
        sa.CheckConstraint("prompt_hash ~ '^[0-9a-f]{64}$'", name="prompt_hash_canonical"),
        sa.CheckConstraint("tool_hash ~ '^[0-9a-f]{64}$'", name="tool_hash_canonical"),
        sa.CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash_canonical"),
    )
    op.create_table(
        "agent_research_outputs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("intent", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["agent_research_runs.id"], name="fk_agent_outputs_run"),
        sa.UniqueConstraint("run_id", "sequence", name="uq_agent_research_outputs_run_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint("status IN ('validated','audited','synthesized','completed','failed','abstained')", name="status_allowed"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_agent_immutable_change() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN RAISE EXCEPTION 'agent audit records are append-only' USING ERRCODE = '23514'; END; $$"
    )
    for table in (
        "agent_prompt_versions",
        "agent_model_configs",
        "agent_research_runs",
        "agent_research_outputs",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    for table in _IMMUTABLE_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_agent_immutable_change()"
        )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_run_transition() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN "
        "IF OLD.status IN ('completed','failed','abstained') THEN RAISE EXCEPTION 'terminal agent run is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status IN ('failed','abstained') THEN RETURN NEW; END IF; "
        "IF (OLD.status = 'prepared' AND NEW.status = 'running') OR "
        "(OLD.status = 'running' AND NEW.status = 'validated') OR "
        "(OLD.status = 'validated' AND NEW.status = 'audited') OR "
        "(OLD.status = 'audited' AND NEW.status = 'synthesized') OR "
        "(OLD.status = 'synthesized' AND NEW.status = 'completed') THEN RETURN NEW; END IF; "
        "RAISE EXCEPTION 'invalid agent run transition' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_research_runs_transition BEFORE UPDATE ON agent_research_runs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_run_transition()"
    )


def downgrade() -> None:
    for table in (
        "agent_research_outputs",
        "agent_research_runs",
        "agent_model_configs",
        "agent_prompt_versions",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.drop_table("agent_research_outputs")
    op.drop_table("agent_research_runs")
    op.drop_table("agent_model_configs")
    op.drop_table("agent_prompt_versions")
    op.execute("DROP FUNCTION nexus_validate_agent_run_transition()")
    op.execute("DROP FUNCTION nexus_prevent_agent_immutable_change()")
