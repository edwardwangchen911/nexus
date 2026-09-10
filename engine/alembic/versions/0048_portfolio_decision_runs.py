"""Add durable, read-only portfolio decision runs.

Revision ID: 0048_portfolio_decision_runs
Revises: 0047_alpaca_market_stream
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0048_portfolio_decision_runs"
down_revision: str | None = "0047_alpaca_market_stream"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_NUMERIC = sa.Numeric(38, 12)
_TABLES = (
    "portfolio_decision_runs",
    "portfolio_decision_outputs",
    "portfolio_decision_budget_reservations",
)


def upgrade() -> None:
    op.drop_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"),
        "agent_prompt_versions",
        type_="check",
    )
    op.create_check_constraint(
        "role_allowed",
        "agent_prompt_versions",
        "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
        "'evidence_audit','portfolio','strategy_rnd','portfolio_decision')",
    )
    op.create_unique_constraint(
        "uq_diag_reports_id_snapshot",
        "portfolio_diagnostic_reports",
        ["id", "portfolio_snapshot_id"],
    )
    op.create_table(
        "portfolio_decision_runs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_id", _UUID, nullable=False),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("diagnostic_report_id", _UUID, nullable=False),
        sa.Column("prompt_version_id", _UUID, nullable=False),
        sa.Column("model_config_id", _UUID, nullable=False),
        sa.Column("actor_id", sa.String(100), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="prepared"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("schema_hash", sa.String(64), nullable=False),
        sa.Column("frozen_input", postgresql.JSONB(), nullable=False),
        sa.Column("prompt_hash", sa.String(64), nullable=False),
        sa.Column("model_hash", sa.String(64), nullable=False),
        sa.Column("tool_hash", sa.String(64), nullable=False),
        sa.Column("evidence_hash", sa.String(64), nullable=False),
        sa.Column("evidence_manifest", postgresql.JSONB(), nullable=False),
        sa.Column("tool_result_hash", sa.String(64), nullable=True),
        sa.Column("tool_projection", postgresql.JSONB(), nullable=True),
        sa.Column("provider_phase", sa.String(24), nullable=False, server_default="none"),
        sa.Column("reserved_cost_usd", _NUMERIC, nullable=False),
        sa.Column("settled_cost_usd", _NUMERIC, nullable=True),
        sa.Column("pricing_version", sa.String(100), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.id"]),
        sa.ForeignKeyConstraint(["prompt_version_id"], ["agent_prompt_versions.id"]),
        sa.ForeignKeyConstraint(["model_config_id"], ["agent_model_configs.id"]),
        sa.ForeignKeyConstraint(
            ["portfolio_snapshot_id", "portfolio_id"],
            ["portfolio_snapshots.id", "portfolio_snapshots.portfolio_id"],
            name="fk_portfolio_decision_runs_snapshot_portfolio",
        ),
        sa.ForeignKeyConstraint(
            ["diagnostic_report_id", "portfolio_snapshot_id"],
            [
                "portfolio_diagnostic_reports.id",
                "portfolio_diagnostic_reports.portfolio_snapshot_id",
            ],
            name="fk_portfolio_decision_runs_diagnostic_snapshot",
        ),
        sa.UniqueConstraint(
            "actor_id", "idempotency_key", name="uq_portfolio_decision_runs_actor_idempotency"
        ),
        sa.CheckConstraint(
            "status IN ('prepared','running','completed','abstained','failed')",
            name="status_allowed",
        ),
        sa.CheckConstraint("retry_count = 0", name="retry_count_zero"),
        sa.CheckConstraint(
            "provider_phase IN ('none','first_requested','tool_recorded','second_requested')",
            name="provider_phase_allowed",
        ),
        *[
            sa.CheckConstraint(f"{name} ~ '^[0-9a-f]{{64}}$'", name=f"{name}_canonical")
            for name in (
                "request_hash",
                "input_hash",
                "schema_hash",
                "model_hash",
                "prompt_hash",
                "tool_hash",
                "evidence_hash",
            )
        ],
        sa.CheckConstraint(
            "tool_result_hash IS NULL OR tool_result_hash ~ '^[0-9a-f]{64}$'",
            name="tool_result_hash_canonical",
        ),
        sa.CheckConstraint("jsonb_typeof(frozen_input) = 'object'", name="frozen_input_object"),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_manifest) = 'array' AND "
            "jsonb_array_length(evidence_manifest) BETWEEN 1 AND 100",
            name="evidence_manifest_bounded",
        ),
        sa.CheckConstraint(
            "tool_projection IS NULL OR jsonb_typeof(tool_projection) = 'object'",
            name="tool_projection_object",
        ),
        sa.CheckConstraint(
            "(tool_result_hash IS NULL) = (tool_projection IS NULL)",
            name="tool_result_projection_bound",
        ),
        sa.CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        sa.CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd",
            name="settled_cost_bounded",
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0", name="input_tokens_nonnegative"
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0", name="output_tokens_nonnegative"
        ),
    )
    op.create_table(
        "portfolio_decision_outputs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("scenario_result_hash", sa.String(64), nullable=True),
        sa.Column("intent", postgresql.JSONB(), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["run_id"], ["portfolio_decision_runs.id"]),
        sa.UniqueConstraint("run_id", name="uq_portfolio_decision_outputs_run"),
        sa.CheckConstraint("status IN ('completed','abstained','failed')", name="status_allowed"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        sa.CheckConstraint(
            "scenario_result_hash IS NULL OR scenario_result_hash ~ '^[0-9a-f]{64}$'",
            name="scenario_result_hash_canonical",
        ),
        sa.CheckConstraint(
            "intent IS NULL OR jsonb_typeof(intent) = 'object'", name="intent_object"
        ),
        sa.CheckConstraint("jsonb_typeof(details) = 'object'", name="details_object"),
        sa.CheckConstraint(
            "(status = 'completed' AND intent IS NOT NULL AND scenario_result_hash IS NOT NULL) OR "
            "(status IN ('abstained','failed') AND intent IS NULL)",
            name="status_payload_bound",
        ),
    )
    op.create_table(
        "portfolio_decision_budget_reservations",
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
        sa.ForeignKeyConstraint(["run_id"], ["portfolio_decision_runs.id"]),
        sa.UniqueConstraint("run_id", name="uq_portfolio_decision_budget_reservations_run"),
        sa.CheckConstraint("reserved_usd >= 0", name="reserved_nonnegative"),
        sa.CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd",
            name="settled_bounded",
        ),
        sa.CheckConstraint("state IN ('reserved','settled','released')", name="state_allowed"),
    )
    for table_name in _TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable BEFORE UPDATE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    op.execute(
        "CREATE TRIGGER trg_portfolio_decision_outputs_immutable BEFORE UPDATE OR DELETE "
        "ON portfolio_decision_outputs FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_agent_immutable_change()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_validate_portfolio_decision_transition()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF (to_jsonb(NEW) - ARRAY['status','provider_phase','tool_result_hash',
              'tool_projection','settled_cost_usd','input_tokens','output_tokens'])
             IS DISTINCT FROM
             (to_jsonb(OLD) - ARRAY['status','provider_phase','tool_result_hash',
              'tool_projection','settled_cost_usd','input_tokens','output_tokens']) THEN
            RAISE EXCEPTION 'portfolio decision authority is immutable' USING ERRCODE = '23514';
          END IF;
          IF OLD.status IN ('completed','abstained','failed') THEN
            RAISE EXCEPTION 'terminal portfolio decision is immutable' USING ERRCODE = '23514';
          END IF;
          IF NEW.provider_phase <> OLD.provider_phase AND NOT (
            (OLD.provider_phase = 'none' AND NEW.provider_phase = 'first_requested') OR
            (OLD.provider_phase = 'first_requested' AND NEW.provider_phase = 'tool_recorded') OR
            (OLD.provider_phase = 'tool_recorded' AND NEW.provider_phase = 'second_requested')
          ) THEN
            RAISE EXCEPTION 'invalid portfolio decision provider phase' USING ERRCODE = '23514';
          END IF;
          IF NEW.status <> OLD.status AND NOT (
            (OLD.status = 'prepared' AND NEW.status IN ('running','failed')) OR
            (OLD.status = 'running' AND NEW.status IN ('completed','abstained','failed'))
          ) THEN
            RAISE EXCEPTION 'invalid portfolio decision transition' USING ERRCODE = '23514';
          END IF;
          IF OLD.status = 'prepared' AND NEW.status = 'failed'
             AND NEW.provider_phase <> 'none' THEN
            RAISE EXCEPTION 'prepared failure cannot follow provider request' USING ERRCODE = '23514';
          END IF;
          IF NEW.status IN ('completed','abstained')
             AND NEW.provider_phase <> 'second_requested' THEN
            RAISE EXCEPTION 'portfolio decision terminal output requires second turn'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_portfolio_decision_runs_transition BEFORE UPDATE "
        "ON portfolio_decision_runs FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_portfolio_decision_transition()"
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE portfolio_decision_budget_reservations, portfolio_decision_outputs, "
        "portfolio_decision_runs IN ACCESS EXCLUSIVE MODE"
    )
    connection = op.get_bind()
    if connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM portfolio_decision_runs "
            "WHERE status IN ('prepared','running'))"
        )
    ):
        raise RuntimeError("active portfolio decision runs prevent downgrade")
    if connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM portfolio_decision_budget_reservations "
            "WHERE state = 'reserved')"
        )
    ):
        raise RuntimeError("reserved portfolio decision budget prevents downgrade")
    if connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM portfolio_decision_budget_reservations "
            "WHERE business_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date)"
        )
    ):
        raise RuntimeError("current UTC budget day prevents portfolio decision downgrade")
    if connection.scalar(
        text("SELECT EXISTS (SELECT 1 FROM agent_prompt_versions WHERE role='portfolio_decision')")
    ):
        raise RuntimeError("portfolio decision prompts prevent downgrade")

    op.execute("DROP FUNCTION nexus_validate_portfolio_decision_transition() CASCADE")
    for table_name in reversed(_TABLES):
        op.drop_table(table_name)
    op.drop_constraint(
        op.f("uq_diag_reports_id_snapshot"),
        "portfolio_diagnostic_reports",
        type_="unique",
    )
    op.drop_constraint(
        op.f("ck_agent_prompt_versions_role_allowed"),
        "agent_prompt_versions",
        type_="check",
    )
    op.create_check_constraint(
        "role_allowed",
        "agent_prompt_versions",
        "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
        "'evidence_audit','portfolio','strategy_rnd')",
    )
