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
    "agent_research_events",
    "agent_research_outputs",
    "agent_research_output_evidence",
)


def upgrade() -> None:
    op.create_table(
        "agent_prompt_versions",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("role", "version", name="uq_agent_prompt_versions_role_version"),
        sa.CheckConstraint(
            "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio')",
            name="role_allowed",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )
    op.create_table(
        "agent_model_configs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("model_id", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("max_input_tokens", sa.Integer(), nullable=False),
        sa.Column("max_tool_calls", sa.Integer(), nullable=False),
        sa.Column("max_output_tokens", sa.Integer(), nullable=False),
        sa.Column("input_usd_per_million", _NUMERIC, nullable=False),
        sa.Column("output_usd_per_million", _NUMERIC, nullable=False),
        sa.Column("pricing_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pricing_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("model_id", "version", name="uq_agent_model_configs_identity"),
        sa.CheckConstraint("config_hash ~ '^[0-9a-f]{64}$'", name="config_hash_canonical"),
        sa.CheckConstraint(
            "max_input_tokens BETWEEN 1 AND 1048576", name="max_input_tokens_bounded"
        ),
        sa.CheckConstraint("max_tool_calls BETWEEN 0 AND 8", name="max_tool_calls_bounded"),
        sa.CheckConstraint(
            "max_output_tokens BETWEEN 1 AND 16384", name="max_output_tokens_bounded"
        ),
        sa.CheckConstraint("input_usd_per_million >= 0", name="input_price_nonnegative"),
        sa.CheckConstraint("output_usd_per_million >= 0", name="output_price_nonnegative"),
        sa.CheckConstraint(
            "pricing_expires_at > pricing_available_at", name="pricing_time_ordered"
        ),
    )
    op.create_table(
        "agent_research_runs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("prompt_version_id", _UUID, nullable=False),
        sa.Column("model_config_id", _UUID, nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_id", sa.String(100), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="prepared"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("schema_hash", sa.String(64), nullable=False),
        sa.Column("model_hash", sa.String(64), nullable=False),
        sa.Column("prompt_hash", sa.String(64), nullable=False),
        sa.Column("tool_hash", sa.String(64), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("reserved_cost_usd", _NUMERIC, nullable=False),
        sa.Column("settled_cost_usd", _NUMERIC, nullable=True),
        sa.Column("pricing_version", sa.String(100), nullable=False, server_default="configured"),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_agent_runs_instrument"
        ),
        sa.ForeignKeyConstraint(
            ["data_snapshot_id"], ["data_snapshots.id"], name="fk_agent_runs_data_snapshot"
        ),
        sa.ForeignKeyConstraint(
            ["prompt_version_id"], ["agent_prompt_versions.id"], name="fk_agent_runs_prompt_version"
        ),
        sa.ForeignKeyConstraint(
            ["model_config_id"], ["agent_model_configs.id"], name="fk_agent_runs_model_config"
        ),
        sa.UniqueConstraint("actor_id", "idempotency_key", name="uq_agent_runs_actor_idempotency"),
        sa.CheckConstraint(
            "status IN ('prepared','running','validated','audited','synthesized','completed','failed','abstained')",
            name="status_allowed",
        ),
        sa.CheckConstraint("retry_count BETWEEN 0 AND 1", name="retry_count_bounded"),
        sa.CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        sa.CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd",
            name="settled_cost_bounded",
        ),
        sa.CheckConstraint("schema_hash ~ '^[0-9a-f]{64}$'", name="schema_hash_canonical"),
        sa.CheckConstraint("model_hash ~ '^[0-9a-f]{64}$'", name="model_hash_canonical"),
        sa.CheckConstraint("prompt_hash ~ '^[0-9a-f]{64}$'", name="prompt_hash_canonical"),
        sa.CheckConstraint("tool_hash ~ '^[0-9a-f]{64}$'", name="tool_hash_canonical"),
        sa.CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash_canonical"),
    )
    op.create_table(
        "agent_research_events",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["run_id"], ["agent_research_runs.id"], name="fk_agent_events_run"),
        sa.UniqueConstraint("run_id", "sequence", name="uq_agent_research_events_run_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "state IN ('prepared','running','validated','audited','synthesized','completed',"
            "'failed','abstained')",
            name="state_allowed",
        ),
        sa.CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_canonical"),
    )
    op.create_table(
        "agent_research_outputs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("role", sa.String(32), nullable=True),
        sa.Column("prompt_hash", sa.String(64), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("intent", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["agent_research_runs.id"], name="fk_agent_outputs_run"
        ),
        sa.UniqueConstraint("run_id", "sequence", name="uq_agent_research_outputs_run_sequence"),
        sa.CheckConstraint("sequence >= 1", name="sequence_positive"),
        sa.CheckConstraint(
            "status IN ('running','validated','audited','synthesized','completed','failed',"
            "'abstained')",
            name="status_allowed",
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint(
            "(role IS NULL AND prompt_hash IS NULL) OR "
            "(role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio') AND prompt_hash ~ '^[0-9a-f]{64}$')",
            name="role_prompt_bound",
        ),
    )
    op.create_table(
        "agent_daily_budget_reservations",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("pricing_version", sa.String(100), nullable=False),
        sa.Column("reserved_usd", _NUMERIC, nullable=False),
        sa.Column("settled_usd", _NUMERIC, nullable=True),
        sa.Column("state", sa.String(16), nullable=False, server_default="reserved"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["run_id"], ["agent_research_runs.id"], name="fk_agent_budget_run"),
        sa.UniqueConstraint("run_id", name="uq_agent_budget_reservations_run"),
        sa.CheckConstraint("reserved_usd >= 0", name="reserved_nonnegative"),
        sa.CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd", name="settled_bounded"
        ),
        sa.CheckConstraint("state IN ('reserved','settled','released')", name="state_allowed"),
    )
    op.create_table(
        "agent_research_output_evidence",
        sa.Column("output_id", _UUID, primary_key=True),
        sa.Column("evidence_ref_id", _UUID, primary_key=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("location", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["output_id"], ["agent_research_outputs.id"], name="fk_agent_output_evidence_output"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_agent_output_evidence_authority",
        ),
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
        "agent_research_events",
        "agent_research_outputs",
        "agent_research_output_evidence",
        "agent_daily_budget_reservations",
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
        "IF (to_jsonb(NEW) - ARRAY['status','retry_count','settled_cost_usd','input_tokens','output_tokens']) "
        "IS DISTINCT FROM "
        "(to_jsonb(OLD) - ARRAY['status','retry_count','settled_cost_usd','input_tokens','output_tokens']) "
        "THEN RAISE EXCEPTION 'agent run authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF OLD.status IN ('completed','failed','abstained') THEN RAISE EXCEPTION 'terminal agent run is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status = OLD.status AND "
        "(NEW.retry_count,NEW.settled_cost_usd,NEW.input_tokens,NEW.output_tokens) "
        "IS DISTINCT FROM "
        "(OLD.retry_count,OLD.settled_cost_usd,OLD.input_tokens,OLD.output_tokens) "
        "THEN RETURN NEW; END IF; "
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
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_output_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "DECLARE authority research_evidence_refs%ROWTYPE; run_decision_time timestamptz; BEGIN "
        "SELECT * INTO authority FROM research_evidence_refs WHERE id = NEW.evidence_ref_id; "
        "IF NOT FOUND THEN RAISE EXCEPTION 'Agent evidence authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF authority.source <> NEW.source OR authority.provider_object_id <> NEW.provider_object_id "
        "OR authority.revision <> NEW.revision OR authority.available_at <> NEW.available_at "
        "OR authority.location <> NEW.location OR authority.content_hash <> NEW.content_hash THEN "
        "RAISE EXCEPTION 'Agent evidence authority mismatch' USING ERRCODE = '23514'; END IF; "
        "SELECT run.decision_time INTO run_decision_time FROM agent_research_outputs output "
        "JOIN agent_research_runs run ON run.id = output.run_id WHERE output.id = NEW.output_id; "
        "IF run_decision_time IS NULL THEN RAISE EXCEPTION 'Agent output authority is missing' USING ERRCODE = '23503'; END IF; "
        "IF NEW.available_at > run_decision_time THEN "
        "RAISE EXCEPTION 'future evidence cannot bind Agent output' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_output_evidence_validate BEFORE INSERT OR UPDATE "
        "ON agent_research_output_evidence FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_output_evidence()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_budget_transition() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF OLD.state <> 'reserved' THEN "
        "RAISE EXCEPTION 'terminal Agent budget reservation is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.state NOT IN ('settled','released') THEN "
        "RAISE EXCEPTION 'invalid Agent budget transition' USING ERRCODE = '23514'; END IF; "
        "IF NEW.run_id <> OLD.run_id OR NEW.business_date <> OLD.business_date "
        "OR NEW.pricing_version <> OLD.pricing_version OR NEW.reserved_usd <> OLD.reserved_usd THEN "
        "RAISE EXCEPTION 'Agent budget authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.state = 'settled' AND NEW.settled_usd IS NULL THEN "
        "RAISE EXCEPTION 'settled Agent budget requires usage' USING ERRCODE = '23514'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_budget_transition BEFORE UPDATE "
        "ON agent_daily_budget_reservations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_budget_transition()"
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_agent_budget_delete() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'agent budget reservation is durable' "
        "USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_budget_delete BEFORE DELETE "
        "ON agent_daily_budget_reservations FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_agent_budget_delete()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_agent_job_change() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN "
        "IF OLD.schema_version <> 'agent_research_job_v1' THEN "
        "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END IF; "
        "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'agent job is durable' "
        "USING ERRCODE = '23514'; END IF; "
        "IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN "
        "RAISE EXCEPTION 'agent job authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF (OLD.status = 'QUEUED' AND NEW.status = 'RUNNING') OR "
        "(OLD.status = 'RUNNING' AND NEW.status IN ('COMPLETED','FAILED','ABSTAINED')) "
        "THEN RETURN NEW; END IF; "
        "RAISE EXCEPTION 'invalid agent job transition' USING ERRCODE = '23514'; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_jobs_change BEFORE UPDATE OR DELETE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_job_change()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_agent_jobs_change ON jobs")
    op.execute("DROP TRIGGER trg_agent_budget_delete ON agent_daily_budget_reservations")
    op.execute("DROP TRIGGER trg_agent_budget_transition ON agent_daily_budget_reservations")
    op.execute("DROP TRIGGER trg_agent_output_evidence_validate ON agent_research_output_evidence")
    for table in (
        "agent_daily_budget_reservations",
        "agent_research_output_evidence",
        "agent_research_outputs",
        "agent_research_events",
        "agent_research_runs",
        "agent_model_configs",
        "agent_prompt_versions",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.drop_table("agent_daily_budget_reservations")
    op.drop_table("agent_research_output_evidence")
    op.drop_table("agent_research_outputs")
    op.drop_table("agent_research_events")
    op.drop_table("agent_research_runs")
    op.drop_table("agent_model_configs")
    op.drop_table("agent_prompt_versions")
    op.execute("DROP FUNCTION nexus_validate_agent_run_transition()")
    op.execute("DROP FUNCTION nexus_validate_agent_budget_transition()")
    op.execute("DROP FUNCTION nexus_prevent_agent_budget_delete()")
    op.execute("DROP FUNCTION nexus_validate_agent_job_change()")
    op.execute("DROP FUNCTION nexus_validate_agent_output_evidence()")
    op.execute("DROP FUNCTION nexus_prevent_agent_immutable_change()")
