"""Add immutable real-advisory policy and portfolio diagnostic projections."""

from collections.abc import Sequence
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0008_diagnostics"
down_revision: str | None = "0007_portfolio_snapshots"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

POLICY_ID = UUID("801f00b8-f963-5164-a6e7-34c4d9955d31")
POLICY_PAYLOAD = '{"single_name_warning":"0.15","single_name_critical":"0.30","top3_warning":"0.60","sector_warning":"0.30","sector_critical":"0.50","currency_warning":"0.80","correlation_cluster_warning":"0.50","cash_minimum":"0.10","drawdown_warning":"0.08","drawdown_critical":"0.15","days_to_exit_warning":"2","days_to_exit_critical":"5"}'
_UUID = postgresql.UUID(as_uuid=True)
_TIMESTAMP = sa.DateTime(timezone=True)
_NUMERIC = sa.Numeric(38, 12)


def upgrade() -> None:
    op.create_table("diagnostic_policies", sa.Column("id", _UUID, primary_key=True), sa.Column("key", sa.String(100), nullable=False), sa.Column("version", sa.String(100), nullable=False), sa.Column("schema_version", sa.String(32), nullable=False), sa.Column("payload", postgresql.JSONB(), nullable=False), sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()), sa.UniqueConstraint("key", "version", name="uq_diagnostic_policies_key_version"))
    op.create_table("portfolio_diagnostic_reports", sa.Column("id", _UUID, primary_key=True), sa.Column("portfolio_id", _UUID, nullable=False), sa.Column("portfolio_snapshot_id", _UUID, nullable=False), sa.Column("snapshot_as_of", _TIMESTAMP, nullable=False), sa.Column("trading_day", sa.Date()), sa.Column("policy_id", _UUID, nullable=False), sa.Column("policy_version", sa.String(100), nullable=False), sa.Column("optimization_need", sa.String(40), nullable=False), sa.Column("diagnostic_input_hash", sa.String(64), nullable=False), sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()), sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.id"], name="fk_diag_reports_portfolio"), sa.ForeignKeyConstraint(["portfolio_snapshot_id"], ["portfolio_snapshots.id"], name="fk_diag_reports_snapshot"), sa.ForeignKeyConstraint(["policy_id"], ["diagnostic_policies.id"], name="fk_diag_reports_policy"), sa.CheckConstraint("optimization_need IN ('BLOCKED', 'RESEARCH_REQUIRED', 'OPTIMIZATION_CANDIDATE', 'NO_ACTION')", name="optimization_need_allowed"))
    op.create_unique_constraint("uq_diag_reports_snapshot_policy", "portfolio_diagnostic_reports", ["portfolio_snapshot_id", "policy_id"])
    op.create_foreign_key("fk_diag_reports_snapshot_same_portfolio", "portfolio_diagnostic_reports", "portfolio_snapshots", ["portfolio_snapshot_id", "portfolio_id"], ["id", "portfolio_id"])
    op.create_table("portfolio_diagnostic_items", sa.Column("id", _UUID, primary_key=True), sa.Column("report_id", _UUID, nullable=False), sa.Column("diagnostic_code", sa.String(200), nullable=False), sa.Column("metric_value", _NUMERIC), sa.Column("unit", sa.String(100), nullable=False), sa.Column("reference_limit", _NUMERIC), sa.Column("policy_scope", sa.String(200), nullable=False), sa.Column("severity", sa.String(20), nullable=False), sa.Column("actionability", sa.String(20), nullable=False), sa.Column("unknown_reason", sa.String(200)), sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()), sa.ForeignKeyConstraint(["report_id"], ["portfolio_diagnostic_reports.id"], name="fk_diag_items_report"), sa.CheckConstraint("severity IN ('info', 'warning', 'critical')", name="severity_allowed"), sa.CheckConstraint("actionability IN ('monitor', 'research', 'reduce_only', 'blocked')", name="actionability_allowed"))
    op.create_table("portfolio_diagnostic_evidence_refs", sa.Column("id", _UUID, primary_key=True), sa.Column("item_id", _UUID, nullable=False), sa.Column("source", sa.String(100), nullable=False), sa.Column("object_id", sa.String(255), nullable=False), sa.Column("available_at", _TIMESTAMP, nullable=False), sa.Column("location", sa.String(1000), nullable=False), sa.Column("content_hash", sa.String(64), nullable=False), sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()), sa.ForeignKeyConstraint(["item_id"], ["portfolio_diagnostic_items.id"], name="fk_diag_evidence_item"))
    op.execute(sa.text("INSERT INTO diagnostic_policies (id, key, version, schema_version, payload) VALUES (:id, 'real_advisory', 'real_advisory_policy_v1', '1', CAST(:payload AS jsonb))").bindparams(id=POLICY_ID, payload=POLICY_PAYLOAD))
    op.execute("CREATE FUNCTION nexus_prevent_diagnostic_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'diagnostic record is immutable' USING ERRCODE = '23514'; END; $$")
    for table in ("diagnostic_policies", "portfolio_diagnostic_reports", "portfolio_diagnostic_items", "portfolio_diagnostic_evidence_refs"):
        op.execute(f"CREATE TRIGGER trg_{table}_created_at_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_created_at_change()")
        op.execute(f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_diagnostic_change()")
    op.execute("CREATE FUNCTION nexus_prevent_diagnostic_authority_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'diagnostic authority record is immutable' USING ERRCODE = '23514'; END; $$")
    for table in ("data_artifacts", "price_series_manifests", "instruments", "instrument_aliases"):
        op.execute(f"CREATE TRIGGER trg_{table}_z_diagnostic_authority_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_diagnostic_authority_change()")


def downgrade() -> None:
    for table in ("instrument_aliases", "instruments", "price_series_manifests", "data_artifacts"):
        op.execute(f"DROP TRIGGER trg_{table}_z_diagnostic_authority_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_diagnostic_authority_change()")
    op.drop_constraint("fk_diag_reports_snapshot_same_portfolio", "portfolio_diagnostic_reports", type_="foreignkey")
    op.drop_constraint("uq_diag_reports_snapshot_policy", "portfolio_diagnostic_reports", type_="unique")
    for table in ("portfolio_diagnostic_evidence_refs", "portfolio_diagnostic_items", "portfolio_diagnostic_reports", "diagnostic_policies"):
        op.execute(f"DROP TRIGGER trg_{table}_created_at_immutable ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION nexus_prevent_diagnostic_change()")
    op.drop_table("portfolio_diagnostic_evidence_refs")
    op.drop_table("portfolio_diagnostic_items")
    op.drop_table("portfolio_diagnostic_reports")
    op.drop_table("diagnostic_policies")
