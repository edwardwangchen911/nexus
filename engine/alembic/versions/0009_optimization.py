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
        "optimization_admission_authorities",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("diagnostic_report_id", _UUID, nullable=False),
        sa.Column("data_snapshot_id", _UUID, nullable=False),
        sa.Column("source_policy_id", _UUID, nullable=False),
        sa.Column("optimizer_policy_id", _UUID, nullable=False),
        sa.Column("cost_model_version", sa.String(100), nullable=False),
        sa.Column("diagnostic_input_hash", sa.String(64), nullable=False),
        sa.Column("valuation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False),
        sa.Column("diagnostic_complete", sa.Boolean(), nullable=False),
        sa.Column("source_conflict", sa.Boolean(), nullable=False),
        sa.Column("partial", sa.Boolean(), nullable=False),
        sa.Column("requires_fx", sa.Boolean(), nullable=False),
        sa.Column("cost_complete", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.ForeignKeyConstraint(["diagnostic_report_id"], ["portfolio_diagnostic_reports.id"]),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"]),
        sa.ForeignKeyConstraint(["source_policy_id"], ["source_policies.id"]),
        sa.ForeignKeyConstraint(["optimizer_policy_id"], ["optimization_policies.id"]),
        sa.UniqueConstraint(
            "portfolio_snapshot_id",
            "data_snapshot_id",
            "cost_model_version",
            name="uq_optimization_admission_authorities_binding",
        ),
    )
    op.create_table(
        "optimization_proposal_groups",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("baseline_portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="draft"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("policy_id", _UUID, nullable=False),
        sa.Column("diagnostic_report_id", _UUID, nullable=True),
        sa.Column("data_snapshot_id", _UUID, nullable=True),
        sa.Column("source_policy_id", _UUID, nullable=True),
        sa.Column("cost_model_version", sa.String(100), nullable=True),
        sa.Column("diagnostic_input_hash", sa.String(64), nullable=True),
        sa.Column("valuation_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("diagnostic_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source_conflict", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("partial", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("requires_fx", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("cost_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["baseline_portfolio_snapshot_id"], ["portfolio_snapshots.id"]),
        sa.ForeignKeyConstraint(["policy_id"], ["optimization_policies.id"]),
        sa.ForeignKeyConstraint(["diagnostic_report_id"], ["portfolio_diagnostic_reports.id"]),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.id"]),
        sa.ForeignKeyConstraint(["source_policy_id"], ["source_policies.id"]),
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
        "proposal_comparisons",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("proposal_id", _UUID, nullable=False),
        sa.Column("metrics", postgresql.JSONB(), nullable=False),
        sa.Column("tax_impact", sa.String(20), nullable=False, server_default="unknown"),
        sa.Column("requires_fx", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("non_executable", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("cost_complete", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["proposal_id"], ["optimization_proposals.id"]),
        sa.UniqueConstraint("proposal_id", name="uq_proposal_comparisons_proposal"),
        sa.CheckConstraint("tax_impact = 'unknown'", name="tax_impact_unknown"),
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
        "CREATE FUNCTION nexus_optimization_group_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "IF NEW.baseline_portfolio_snapshot_id IS DISTINCT FROM OLD.baseline_portfolio_snapshot_id OR NEW.policy_id IS DISTINCT FROM OLD.policy_id OR NEW.diagnostic_report_id IS DISTINCT FROM OLD.diagnostic_report_id OR NEW.data_snapshot_id IS DISTINCT FROM OLD.data_snapshot_id OR NEW.source_policy_id IS DISTINCT FROM OLD.source_policy_id OR NEW.cost_model_version IS DISTINCT FROM OLD.cost_model_version OR NEW.diagnostic_input_hash IS DISTINCT FROM OLD.diagnostic_input_hash OR NEW.valuation_time IS DISTINCT FROM OLD.valuation_time OR NEW.baseline_ready IS DISTINCT FROM OLD.baseline_ready OR NEW.diagnostic_complete IS DISTINCT FROM OLD.diagnostic_complete OR NEW.source_conflict IS DISTINCT FROM OLD.source_conflict OR NEW.partial IS DISTINCT FROM OLD.partial OR NEW.requires_fx IS DISTINCT FROM OLD.requires_fx OR NEW.cost_complete IS DISTINCT FROM OLD.cost_complete THEN RAISE EXCEPTION 'optimization group authority is immutable' USING ERRCODE = '23514'; END IF; "
        "IF NEW.version <> OLD.version + 1 THEN RAISE EXCEPTION 'optimization group version conflict' USING ERRCODE = '40001'; END IF; "
        "IF (OLD.status = 'draft' AND NEW.status NOT IN ('compared','rejected','expired')) OR (OLD.status = 'compared' AND NEW.status NOT IN ('shadowing','rejected','expired')) OR (OLD.status = 'shadowing' AND NEW.status NOT IN ('validated','rejected','expired')) OR (OLD.status IN ('validated','rejected','expired')) THEN RAISE EXCEPTION 'optimization lifecycle transition invalid' USING ERRCODE = '23514'; END IF; "
        "IF NEW.status = 'shadowing' AND (NOT OLD.baseline_ready OR NOT OLD.diagnostic_complete OR OLD.source_conflict OR OLD.partial OR OLD.requires_fx OR NOT OLD.cost_complete OR NOT EXISTS (SELECT 1 FROM optimization_proposals p WHERE p.proposal_group_id = OLD.id AND p.variant = 'strategy_target' AND p.strategy_evidence_complete AND NOT p.analysis_only) OR (SELECT count(*) FROM proposal_comparisons c JOIN optimization_proposals p ON p.id = c.proposal_id WHERE p.proposal_group_id = OLD.id AND NOT c.non_executable AND NOT c.requires_fx AND c.cost_complete AND COALESCE((c.metrics->>'ranking_eligible')::boolean, false) AND COALESCE((c.metrics->>'constraint_complete')::boolean, false) AND COALESCE((c.metrics->>'eligibility_complete')::boolean, false) AND COALESCE((c.metrics->>'evidence_complete')::boolean, false) AND NOT COALESCE((c.metrics->>'stale')::boolean, true) AND COALESCE(c.metrics->'unknown_metrics', 'null'::jsonb) = '[]'::jsonb AND c.metrics->>'cost' IS NOT NULL) <> 3) THEN RAISE EXCEPTION 'PROPOSAL_NOT_EXECUTABLE' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE FUNCTION nexus_require_three_optimization_variants() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE group_ref uuid; variant_count integer; comparison_count integer; BEGIN IF TG_TABLE_NAME = 'optimization_proposal_groups' THEN group_ref := NEW.id; ELSIF TG_TABLE_NAME = 'proposal_comparisons' THEN SELECT p.proposal_group_id INTO group_ref FROM optimization_proposals p WHERE p.id = COALESCE(NEW.proposal_id, OLD.proposal_id); ELSE group_ref := COALESCE(NEW.proposal_group_id, OLD.proposal_group_id); END IF; SELECT count(*) INTO variant_count FROM optimization_proposals WHERE proposal_group_id = group_ref; SELECT count(*) INTO comparison_count FROM proposal_comparisons c JOIN optimization_proposals p ON p.id = c.proposal_id WHERE p.proposal_group_id = group_ref; IF variant_count <> 3 OR comparison_count <> 3 THEN RAISE EXCEPTION 'optimization group requires exactly three variants and comparisons' USING ERRCODE = '23514'; END IF; RETURN NULL; END; $$"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_groups_three_variants AFTER INSERT ON optimization_proposal_groups DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_optimization_proposals_three_variants AFTER INSERT OR DELETE ON optimization_proposals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_proposal_comparisons_three_variants AFTER INSERT OR DELETE ON proposal_comparisons DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION nexus_require_three_optimization_variants()"
    )
    op.execute(
        "CREATE TRIGGER trg_optimization_groups_lifecycle BEFORE UPDATE ON optimization_proposal_groups FOR EACH ROW EXECUTE FUNCTION nexus_optimization_group_lifecycle()"
    )
    for table in (
        "optimization_policies",
        "optimization_admission_authorities",
        "optimization_proposal_groups",
        "optimization_proposals",
        "proposal_positions",
        "proposal_comparisons",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    for table in (
        "optimization_policies",
        "optimization_admission_authorities",
        "optimization_proposals",
        "proposal_positions",
        "proposal_comparisons",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_optimization_change()"
        )


def downgrade() -> None:
    for table in (
        "proposal_comparisons",
        "proposal_positions",
        "optimization_proposals",
        "optimization_proposal_groups",
        "optimization_admission_authorities",
        "optimization_policies",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
    op.execute("DROP TRIGGER trg_proposal_comparisons_three_variants ON proposal_comparisons")
    op.execute("DROP TRIGGER trg_optimization_proposals_three_variants ON optimization_proposals")
    op.execute("DROP TRIGGER trg_optimization_groups_lifecycle ON optimization_proposal_groups")
    op.execute(
        "DROP TRIGGER trg_optimization_groups_three_variants ON optimization_proposal_groups"
    )
    op.execute("DROP FUNCTION nexus_require_three_optimization_variants()")
    op.execute("DROP FUNCTION nexus_optimization_group_lifecycle()")
    for table in (
        "proposal_comparisons",
        "proposal_positions",
        "optimization_proposals",
        "optimization_admission_authorities",
        "optimization_policies",
    ):
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_optimization_change()")
    op.drop_table("proposal_comparisons")
    op.drop_table("proposal_positions")
    op.drop_table("optimization_proposals")
    op.drop_table("optimization_proposal_groups")
    op.drop_table("optimization_admission_authorities")
    op.drop_table("optimization_policies")
