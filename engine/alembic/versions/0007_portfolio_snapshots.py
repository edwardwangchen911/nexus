"""Add immutable broker portfolios, sync runs, snapshots, and cash flows.

Revision ID: 0007_portfolio_snapshots
Revises: 0006_market_delivery_authority
Create Date: 2026-08-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0007_portfolio_snapshots"
down_revision: str | None = "0006_market_delivery_authority"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_TIMESTAMP = sa.DateTime(timezone=True)
_NUMERIC = sa.Numeric(38, 12)

def _fk(column: str, target: str, name: str) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint([column], [target], name=name)

def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'DISABLED')",
            name=op.f("ck_accounts_status_allowed"),
        ),
    )
    op.create_table(
        "account_external_id_envelopes",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("field_context", sa.String(100), nullable=False),
        sa.Column("envelope_version", sa.Integer(), nullable=False),
        sa.Column("aad_version", sa.String(50), nullable=False),
        sa.Column("key_version", sa.String(100), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "envelope_version >= 1",
            name=op.f("ck_account_external_id_envelopes_envelope_version_positive"),
        ),
        sa.CheckConstraint(
            "aad_version = 'external_id_aad_v2'",
            name=op.f("ck_account_external_id_envelopes_aad_version_allowed"),
        ),
        _fk("account_id", "accounts.id", "fk_account_external_id_envelopes_account_id_accounts"),
        sa.UniqueConstraint(
            "account_id",
            "field_context",
            "envelope_version",
            name="uq_account_external_id_envelopes_version",
        ),
    )
    op.create_table(
        "portfolios",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        _fk("account_id", "accounts.id", "fk_portfolios_account_id_accounts"),
        sa.UniqueConstraint("account_id", name="uq_portfolios_account_id"),
    )
    op.create_table(
        "broker_sync_runs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("portfolio_id", _UUID, nullable=False),
        sa.Column("trigger_kind", sa.String(20), nullable=False),
        sa.Column("actor_id", sa.String(255), nullable=False),
        sa.Column("route", sa.String(255), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("accepted_at", _TIMESTAMP, nullable=False),
        sa.Column("completed_at", _TIMESTAMP),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("failure_reason", sa.String(255)),
        sa.Column("cash_flow_quality", sa.String(20), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "trigger_kind IN ('MANUAL', 'AUTOMATIC')",
            name=op.f("ck_broker_sync_runs_trigger_kind_allowed"),
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'READY', 'PARTIAL', 'STALE', "
            "'RECONCILIATION_REQUIRED', 'FAILED')",
            name=op.f("ck_broker_sync_runs_status_allowed"),
        ),
        sa.CheckConstraint(
            "cash_flow_quality IN ('PASS', 'PARTIAL', 'NOT_RUN')",
            name=op.f("ck_broker_sync_runs_cash_flow_quality_allowed"),
        ),
        _fk("account_id", "accounts.id", "fk_broker_sync_runs_account_id_accounts"),
        _fk("portfolio_id", "portfolios.id", "fk_broker_sync_runs_portfolio_id_portfolios"),
        sa.UniqueConstraint(
            "actor_id",
            "route",
            "idempotency_key",
            name="uq_broker_sync_runs_actor_route_key",
        ),
    )
    op.create_index(
        "uq_broker_sync_runs_active_account",
        "broker_sync_runs",
        ["account_id"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )
    op.create_table(
        "broker_sync_requests",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("sync_run_id", _UUID, nullable=False),
        sa.Column("idempotency_record_id", _UUID, nullable=False),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("trigger_kind", sa.String(20), nullable=False),
        sa.Column("accepted_at", _TIMESTAMP, nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "trigger_kind IN ('MANUAL', 'AUTOMATIC')",
            name=op.f("ck_broker_sync_requests_trigger_kind_allowed"),
        ),
        _fk("sync_run_id", "broker_sync_runs.id", "fk_broker_sync_requests_sync_run_id_runs"),
        _fk("idempotency_record_id", "idempotency_records.id", "fk_broker_sync_requests_idempotency_record_id_records"),
        _fk("account_id", "accounts.id", "fk_broker_sync_requests_account_id_accounts"),
        sa.UniqueConstraint(
            "idempotency_record_id",
            name="uq_broker_sync_requests_idempotency_record_id",
        ),
    )
    op.create_table(
        "broker_sync_attempts",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("sync_run_id", _UUID, nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("phase", sa.String(50), nullable=False),
        sa.Column("completed", sa.Boolean(), nullable=False),
        sa.Column("reason", sa.String(255), nullable=False),
        sa.Column("source_request_ids", postgresql.JSONB(), nullable=False),
        sa.Column("source_observations", postgresql.JSONB(), nullable=False),
        sa.Column("raw_facts", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        _fk("sync_run_id", "broker_sync_runs.id", "fk_broker_sync_attempts_sync_run_id_runs"),
        sa.UniqueConstraint(
            "sync_run_id",
            "attempt",
            name="uq_broker_sync_attempts_run_attempt",
        ),
    )
    op.create_table(
        "portfolio_snapshots",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_id", _UUID, nullable=False),
        sa.Column("sync_run_id", _UUID, nullable=False),
        sa.Column("stale_from_snapshot_id", _UUID),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("as_of", _TIMESTAMP),
        sa.Column("position_hash_version", sa.String(50), nullable=False),
        sa.Column("position_hash", sa.String(64), nullable=False),
        sa.Column("order_hash_version", sa.String(50), nullable=False),
        sa.Column("order_hash", sa.String(64), nullable=False),
        sa.Column("source_request_ids", postgresql.JSONB(), nullable=False),
        sa.Column("source_observations", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('READY', 'PARTIAL', 'STALE', 'RECONCILIATION_REQUIRED')",
            name=op.f("ck_portfolio_snapshots_status_allowed"),
        ),
        sa.CheckConstraint(
            "(status = 'STALE' AND stale_from_snapshot_id IS NOT NULL) OR "
            "(status <> 'STALE' AND stale_from_snapshot_id IS NULL)",
            name=op.f("ck_portfolio_snapshots_stale_source_required"),
        ),
        _fk("portfolio_id", "portfolios.id", "fk_portfolio_snapshots_portfolio_id_portfolios"),
        _fk("sync_run_id", "broker_sync_runs.id", "fk_portfolio_snapshots_sync_run_id_runs"),
        sa.ForeignKeyConstraint(
            ["stale_from_snapshot_id", "portfolio_id"],
            ["portfolio_snapshots.id", "portfolio_snapshots.portfolio_id"],
            name="fk_portfolio_snapshots_stale_source_same_portfolio",
        ),
        sa.UniqueConstraint("sync_run_id", name="uq_portfolio_snapshots_sync_run_id"),
        sa.UniqueConstraint("id", "portfolio_id", name="uq_portfolio_snapshots_id_portfolio"),
    )
    op.create_table(
        "cash_snapshots",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("cash_balance", _NUMERIC, nullable=False),
        sa.Column("available_cash", _NUMERIC, nullable=False),
        sa.Column("provider_as_of", _TIMESTAMP),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        _fk("portfolio_snapshot_id", "portfolio_snapshots.id", "fk_cash_snapshots_portfolio_snapshot_id_snapshots"),
        sa.UniqueConstraint(
            "portfolio_snapshot_id",
            "currency",
            name="uq_cash_snapshots_snapshot_currency",
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_cash_snapshots_currency_canonical"),
        ),
    )
    op.create_table(
        "position_snapshots",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("quantity", _NUMERIC, nullable=False),
        sa.Column("available_quantity", _NUMERIC, nullable=False),
        sa.Column("frozen_quantity", _NUMERIC, nullable=False),
        sa.Column("avg_cost", _NUMERIC),
        sa.Column("diluted_cost", _NUMERIC),
        sa.Column("current_price", _NUMERIC),
        sa.Column("provider_as_of", _TIMESTAMP),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        _fk("portfolio_snapshot_id", "portfolio_snapshots.id", "fk_position_snapshots_portfolio_snapshot_id_snapshots"),
        _fk("instrument_id", "instruments.id", "fk_position_snapshots_instrument_id_instruments"),
        sa.UniqueConstraint(
            "portfolio_snapshot_id",
            "instrument_id",
            name="uq_position_snapshots_snapshot_instrument",
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_position_snapshots_currency_canonical"),
        ),
    )
    op.create_table(
        "broker_order_snapshots",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("portfolio_snapshot_id", _UUID, nullable=False),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("provider_order_id", sa.String(255), nullable=False),
        sa.Column("instrument_id", _UUID, nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("quantity", _NUMERIC, nullable=False),
        sa.Column("filled_quantity", _NUMERIC, nullable=False),
        sa.Column("price", _NUMERIC),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("updated_at", _TIMESTAMP),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        _fk("portfolio_snapshot_id", "portfolio_snapshots.id", "fk_broker_order_snapshots_portfolio_snapshot_id_snapshots"),
        _fk("instrument_id", "instruments.id", "fk_broker_order_snapshots_instrument_id_instruments"),
        sa.UniqueConstraint(
            "portfolio_snapshot_id",
            "provider_order_id",
            name="uq_broker_order_snapshots_snapshot_provider_order",
        ),
    )
    op.create_table(
        "broker_cash_flows",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("sync_run_id", _UUID, nullable=False),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("provider_cash_flow_id", sa.String(255), nullable=False),
        sa.Column("identity_version", sa.String(50), nullable=False),
        sa.Column("identity_quality", sa.String(30), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("amount", _NUMERIC, nullable=False),
        sa.Column("occurred_at", _TIMESTAMP, nullable=False),
        sa.Column("provider_business_date", sa.Date(), nullable=False),
        sa.Column("kind", sa.String(100), nullable=False),
        sa.Column("source_request_id", sa.String(128), nullable=False),
        sa.Column("response_as_of", _TIMESTAMP, nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "identity_quality IN ('STABLE', 'FALLBACK_PARTIAL')",
            name=op.f("ck_broker_cash_flows_identity_quality_allowed"),
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_broker_cash_flows_currency_canonical"),
        ),
        _fk("account_id", "accounts.id", "fk_broker_cash_flows_account_id_accounts"),
        _fk("sync_run_id", "broker_sync_runs.id", "fk_broker_cash_flows_sync_run_id_runs"),
        sa.UniqueConstraint(
            "provider",
            "provider_cash_flow_id",
            name="uq_broker_cash_flows_provider_id",
        ),
    )
    op.create_table(
        "broker_cash_flow_watermarks",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column("account_id", _UUID, nullable=False),
        sa.Column("provider_business_date", sa.Date(), nullable=False),
        sa.Column("identity_quality", sa.String(30), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "identity_quality IN ('STABLE', 'FALLBACK_PARTIAL')",
            name=op.f("ck_broker_cash_flow_watermarks_identity_quality_allowed"),
        ),
        sa.CheckConstraint(
            "version >= 1",
            name=op.f("ck_broker_cash_flow_watermarks_version_positive"),
        ),
        _fk("account_id", "accounts.id", "fk_cash_flow_watermarks_account_id_accounts"),
        sa.UniqueConstraint("account_id", name="uq_cash_flow_watermarks_account_id"),
    )
    op.execute(
        "CREATE FUNCTION nexus_lock_provider_capability_scope() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended("
        "'provider-capability-scope:v1:' || NEW.provider || ':' || NEW.capability || ':' || "
        "COALESCE(NEW.payload ->> 'operation', '') || ':' || "
        "COALESCE(NEW.payload ->> 'market_scope', ''), 0)); RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_provider_capabilities_scope_lock BEFORE INSERT "
        "ON provider_capabilities FOR EACH ROW EXECUTE FUNCTION "
        "nexus_lock_provider_capability_scope()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_external_id_envelope() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ DECLARE prior account_external_id_envelopes%ROWTYPE; "
        "BEGIN PERFORM pg_advisory_xact_lock(hashtextextended("
        "'external-envelope-account:' || NEW.account_id::text || ':' || NEW.field_context, 0)); "
        "PERFORM pg_advisory_xact_lock(hashtextextended("
        "'external-envelope-fingerprint:' || NEW.provider || ':' || NEW.field_context || ':' "
        "|| NEW.fingerprint, 0)); "
        "IF NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id = NEW.account_id "
        "AND a.provider = NEW.provider) THEN RAISE EXCEPTION "
        "'external ID envelope provider mismatch'; END IF; "
        "IF EXISTS (SELECT 1 FROM account_external_id_envelopes e WHERE "
        "e.provider = NEW.provider AND e.field_context = NEW.field_context "
        "AND e.fingerprint = NEW.fingerprint AND e.account_id <> NEW.account_id) "
        "THEN RAISE EXCEPTION 'external ID fingerprint belongs to another account'; END IF; "
        "SELECT e.* INTO prior FROM account_external_id_envelopes e WHERE "
        "e.account_id = NEW.account_id AND e.field_context = NEW.field_context "
        "ORDER BY e.envelope_version DESC LIMIT 1; "
        "IF prior.id IS NULL THEN IF NEW.envelope_version <> 1 THEN RAISE EXCEPTION "
        "'first external ID envelope version must be one'; END IF; "
        "ELSIF NEW.envelope_version <> prior.envelope_version + 1 THEN RAISE EXCEPTION "
        "'external ID envelope version is not append-only'; "
        "ELSIF NEW.fingerprint <> prior.fingerprint THEN RAISE EXCEPTION "
        "'external ID fingerprint identity changed'; END IF; RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_account_external_id_envelopes_validate BEFORE INSERT "
        "ON account_external_id_envelopes FOR EACH ROW EXECUTE FUNCTION "
        "nexus_validate_external_id_envelope()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_stale_snapshot() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'STALE' AND NOT EXISTS "
        "(SELECT 1 FROM portfolio_snapshots p WHERE p.id = NEW.stale_from_snapshot_id "
        "AND p.portfolio_id = NEW.portfolio_id AND p.status = 'READY') THEN "
        "RAISE EXCEPTION 'STALE source must be same-portfolio READY'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_portfolio_snapshots_stale_ready BEFORE INSERT "
        "ON portfolio_snapshots FOR EACH ROW EXECUTE FUNCTION nexus_validate_stale_snapshot()"
    )
    op.execute(
        "CREATE FUNCTION nexus_validate_cash_flow_watermark_update() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'cash-flow watermark deletion is forbidden'; "
        "END IF; IF NEW.provider_business_date < OLD.provider_business_date OR "
        "NEW.version <> OLD.version + 1 OR "
        "(OLD.identity_quality = 'FALLBACK_PARTIAL' AND NEW.identity_quality = 'STABLE') "
        "THEN RAISE EXCEPTION 'cash-flow watermark regression'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_broker_cash_flow_watermarks_monotonic BEFORE UPDATE OR DELETE "
        "ON broker_cash_flow_watermarks FOR EACH ROW EXECUTE FUNCTION "
        "nexus_validate_cash_flow_watermark_update()"
    )
    op.execute(
        "CREATE FUNCTION nexus_prevent_broker_fact_change() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'broker fact is immutable'; END; $$"
    )
    created_at_tables = (
        "accounts",
        "account_external_id_envelopes",
        "portfolios",
        "broker_sync_runs",
        "broker_sync_requests",
        "broker_sync_attempts",
        "portfolio_snapshots",
        "cash_snapshots",
        "position_snapshots",
        "broker_order_snapshots",
        "broker_cash_flows",
        "broker_cash_flow_watermarks",
    )
    for table_name in created_at_tables:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW EXECUTE FUNCTION "
            "nexus_prevent_created_at_change()"
        )
    for table_name in (
        "accounts",
        "account_external_id_envelopes",
        "portfolios",
        "broker_sync_requests",
        "broker_sync_attempts",
        "portfolio_snapshots",
        "cash_snapshots",
        "position_snapshots",
        "broker_order_snapshots",
        "broker_cash_flows",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION "
            "nexus_prevent_broker_fact_change()"
        )
    op.execute(
        "CREATE FUNCTION nexus_protect_terminal_broker_sync() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "IF TG_OP = 'DELETE' OR OLD.status <> 'ACTIVE' THEN "
        "RAISE EXCEPTION 'terminal broker sync run is immutable'; END IF; "
        "RETURN NEW; END; $$"
    )
    op.execute(
        "CREATE TRIGGER trg_broker_sync_runs_terminal_immutable BEFORE UPDATE OR DELETE "
        "ON broker_sync_runs FOR EACH ROW EXECUTE FUNCTION "
        "nexus_protect_terminal_broker_sync()"
    )


def downgrade() -> None:
    for table_name in (
        "broker_cash_flow_watermarks",
        "broker_cash_flows",
        "broker_order_snapshots",
        "position_snapshots",
        "cash_snapshots",
        "portfolio_snapshots",
        "broker_sync_attempts",
        "broker_sync_requests",
        "broker_sync_runs",
        "portfolios",
        "account_external_id_envelopes",
        "accounts",
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute("DROP TRIGGER trg_broker_sync_runs_terminal_immutable ON broker_sync_runs")
    op.execute("DROP FUNCTION nexus_protect_terminal_broker_sync()")
    op.execute(
        "DROP TRIGGER trg_broker_cash_flow_watermarks_monotonic "
        "ON broker_cash_flow_watermarks"
    )
    op.execute("DROP FUNCTION nexus_validate_cash_flow_watermark_update()")
    op.execute("DROP TRIGGER trg_portfolio_snapshots_stale_ready ON portfolio_snapshots")
    op.execute("DROP FUNCTION nexus_validate_stale_snapshot()")
    op.execute(
        "DROP TRIGGER trg_account_external_id_envelopes_validate "
        "ON account_external_id_envelopes"
    )
    op.execute("DROP FUNCTION nexus_validate_external_id_envelope()")
    op.execute("DROP TRIGGER trg_provider_capabilities_scope_lock ON provider_capabilities")
    op.execute("DROP FUNCTION nexus_lock_provider_capability_scope()")
    for table_name in (
        "broker_cash_flows",
        "broker_order_snapshots",
        "position_snapshots",
        "cash_snapshots",
        "portfolio_snapshots",
        "broker_sync_attempts",
        "broker_sync_requests",
        "portfolios",
        "account_external_id_envelopes",
        "accounts",
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
    op.execute("DROP FUNCTION nexus_prevent_broker_fact_change()")
    op.drop_table("broker_cash_flow_watermarks")
    op.drop_table("broker_cash_flows")
    op.drop_table("broker_order_snapshots")
    op.drop_table("position_snapshots")
    op.drop_table("cash_snapshots")
    op.drop_table("portfolio_snapshots")
    op.drop_table("broker_sync_attempts")
    op.drop_table("broker_sync_requests")
    op.drop_index("uq_broker_sync_runs_active_account", table_name="broker_sync_runs")
    op.drop_table("broker_sync_runs")
    op.drop_table("portfolios")
    op.drop_table("account_external_id_envelopes")
    op.drop_table("accounts")
