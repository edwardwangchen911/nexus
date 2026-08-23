"""Add immutable broker portfolio storage.

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
        "CREATE FUNCTION nexus_prevent_broker_fact_change() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'broker fact is immutable'; END; $$"
    )
    for table_name in ("accounts", "account_external_id_envelopes"):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW EXECUTE FUNCTION "
            "nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION nexus_prevent_broker_fact_change()"
        )


def downgrade() -> None:
    for table_name in ("account_external_id_envelopes", "accounts"):
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
    op.execute(
        "DROP TRIGGER trg_account_external_id_envelopes_validate "
        "ON account_external_id_envelopes"
    )
    op.execute("DROP FUNCTION nexus_validate_external_id_envelope()")
    op.execute("DROP FUNCTION nexus_prevent_broker_fact_change()")
    op.drop_table("account_external_id_envelopes")
    op.drop_table("accounts")
