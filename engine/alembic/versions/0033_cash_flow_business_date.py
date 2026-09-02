"""Permit provider-date cash flows without inventing an event timezone.

Revision ID: 0033_cash_flow_business_date
Revises: 0032_ticker_alias_authority
"""

from alembic import op

revision = "0033_cash_flow_business_date"
down_revision = "0032_ticker_alias_authority"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("broker_cash_flows", "occurred_at", nullable=True)
    op.create_check_constraint(
        op.f("ck_broker_cash_flows_date_only_identity_stable"),
        "broker_cash_flows",
        "occurred_at IS NOT NULL OR identity_quality = 'STABLE'",
    )


def downgrade() -> None:
    # SET NOT NULL refuses rollback with date-only facts; never fabricate or delete them.
    op.alter_column("broker_cash_flows", "occurred_at", nullable=False)
    op.drop_constraint(
        op.f("ck_broker_cash_flows_date_only_identity_stable"),
        "broker_cash_flows",
        type_="check",
    )
