"""Preserve market capability scope and explicit quote delivery authority.

Revision ID: 0006_market_delivery_authority
Revises: 0005_market_integrity
Create Date: 2026-08-23
"""

from collections.abc import Sequence
from uuid import UUID

from alembic import op
import sqlalchemy as sa

revision: str = "0006_market_delivery_authority"
down_revision: str | None = "0005_market_integrity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

QUOTE_SOURCE_POLICY_ID = UUID("6d243c22-a020-5d1d-b884-e6240cc2d8bb")
QUOTE_SOURCE_POLICY_PAYLOAD = """{"priority":["fosun","alpaca","fixture"],"fosun":{"mode":"read_only","operation":"fosun_get_quotes","market_scope":"US"},"alpaca":{"feed":"iex","delivery_modes":["real_time","delayed","unknown"]},"fixture":{"delivery_mode":"fixture"}}"""


def upgrade() -> None:
    op.drop_constraint(
        "uq_provider_capabilities_contract",
        "provider_capabilities",
        type_="unique",
    )
    op.create_index(
        "uq_provider_capabilities_contract_scope",
        "provider_capabilities",
        [
            "provider",
            "capability",
            "contract_hash",
            sa.text("COALESCE(payload ->> 'operation', '')"),
            sa.text("COALESCE(payload ->> 'market_scope', '')"),
        ],
        unique=True,
    )
    op.add_column(
        "latest_quotes",
        sa.Column(
            "delivery_mode",
            sa.String(20),
            nullable=False,
            server_default="unknown",
        ),
    )
    op.execute(
        "UPDATE latest_quotes SET delivery_mode = 'fixture' WHERE source = 'fixture'"
    )
    op.create_check_constraint(
        op.f("ck_latest_quotes_delivery_mode_allowed"),
        "latest_quotes",
        "delivery_mode IN ('real_time', 'delayed', 'fixture', 'unknown')",
    )
    op.create_check_constraint(
        op.f("ck_latest_quotes_fixture_delivery_mode"),
        "latest_quotes",
        "source <> 'fixture' OR delivery_mode = 'fixture'",
    )
    op.create_check_constraint(
        op.f("ck_latest_quotes_fosun_delivery_mode"),
        "latest_quotes",
        "source <> 'fosun' OR delivery_mode <> 'fixture'",
    )
    op.alter_column("latest_quotes", "delivery_mode", server_default=None)
    op.add_column(
        "price_series_manifests",
        sa.Column(
            "delivery_mode",
            sa.String(20),
            nullable=False,
            server_default="unknown",
        ),
    )
    op.execute(
        "UPDATE price_series_manifests SET delivery_mode = 'fixture' "
        "WHERE source = 'fixture'"
    )
    op.create_check_constraint(
        op.f("ck_price_series_manifests_delivery_mode_allowed"),
        "price_series_manifests",
        "delivery_mode IN ('real_time', 'delayed', 'fixture', 'unknown')",
    )
    op.create_check_constraint(
        op.f("ck_price_series_manifests_fixture_delivery_mode"),
        "price_series_manifests",
        "source <> 'fixture' OR delivery_mode = 'fixture'",
    )
    op.create_check_constraint(
        op.f("ck_price_series_manifests_fosun_delivery_mode"),
        "price_series_manifests",
        "source <> 'fosun' OR delivery_mode <> 'fixture'",
    )
    op.alter_column("price_series_manifests", "delivery_mode", server_default=None)
    op.execute(
        sa.text(
            "INSERT INTO source_policies (id, key, version, schema_version, payload) "
            "VALUES (:id, 'market_quote', 'quote_source_policy_v1', '1', "
            "CAST(:payload AS jsonb))"
        ).bindparams(id=QUOTE_SOURCE_POLICY_ID, payload=QUOTE_SOURCE_POLICY_PAYLOAD)
    )


def downgrade() -> None:
    duplicate_contract_scope = op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM provider_capabilities "
            "GROUP BY provider, capability, contract_hash "
            "HAVING COUNT(*) > 1"
            ")"
        )
    )
    if duplicate_contract_scope:
        raise RuntimeError(
            "cannot downgrade provider capability scope while scoped duplicates exist"
        )
    bind = op.get_bind()
    op.execute("DROP TRIGGER trg_source_policies_immutable ON source_policies")
    deleted_policy = bind.execute(
        sa.text(
            "DELETE FROM source_policies WHERE id = :id "
            "AND key = 'market_quote' AND version = 'quote_source_policy_v1' "
            "AND schema_version = '1' AND payload = CAST(:payload AS jsonb)"
        ),
        {"id": QUOTE_SOURCE_POLICY_ID, "payload": QUOTE_SOURCE_POLICY_PAYLOAD},
    )
    if deleted_policy.rowcount != 1:
        raise RuntimeError("cannot downgrade a missing or altered quote source policy")
    op.execute(
        "CREATE TRIGGER trg_source_policies_immutable BEFORE UPDATE OR DELETE "
        "ON source_policies FOR EACH ROW EXECUTE FUNCTION "
        "nexus_prevent_source_policy_change()"
    )
    op.drop_constraint(
        op.f("ck_price_series_manifests_fosun_delivery_mode"),
        "price_series_manifests",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_price_series_manifests_fixture_delivery_mode"),
        "price_series_manifests",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_price_series_manifests_delivery_mode_allowed"),
        "price_series_manifests",
        type_="check",
    )
    op.drop_column("price_series_manifests", "delivery_mode")
    op.drop_constraint(
        op.f("ck_latest_quotes_fosun_delivery_mode"),
        "latest_quotes",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_latest_quotes_fixture_delivery_mode"),
        "latest_quotes",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_latest_quotes_delivery_mode_allowed"),
        "latest_quotes",
        type_="check",
    )
    op.drop_column("latest_quotes", "delivery_mode")
    op.drop_index(
        "uq_provider_capabilities_contract_scope",
        table_name="provider_capabilities",
    )
    op.create_unique_constraint(
        "uq_provider_capabilities_contract",
        "provider_capabilities",
        ["provider", "capability", "contract_hash"],
    )
