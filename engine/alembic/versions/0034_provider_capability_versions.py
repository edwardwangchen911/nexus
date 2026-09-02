"""Allow immutable versions of the same provider capability scope.

Revision ID: 0034_capability_versions
Revises: 0033_cash_flow_business_date
"""

from alembic import op

revision = "0034_capability_versions"
down_revision = "0033_cash_flow_business_date"
branch_labels = None
depends_on = None

_SCOPE = """
    provider, capability, contract_hash,
    COALESCE(payload ->> 'operation', ''),
    COALESCE(payload ->> 'market_scope', '')
"""


def upgrade() -> None:
    op.execute("LOCK TABLE public.provider_capabilities IN ACCESS EXCLUSIVE MODE")
    op.drop_index("uq_provider_capabilities_contract_scope", table_name="provider_capabilities")
    op.execute(
        f"CREATE UNIQUE INDEX uq_provider_capabilities_contract_scope "
        f"ON public.provider_capabilities ({_SCOPE}, created_at)"
    )


def downgrade() -> None:
    op.execute("LOCK TABLE public.provider_capabilities IN ACCESS EXCLUSIVE MODE")
    # Restore the old unique index first. Existing versions make this fail
    # transactionally; never delete immutable capability/fact history to roll back.
    op.execute(
        f"CREATE UNIQUE INDEX uq_provider_capabilities_scope_rollback "
        f"ON public.provider_capabilities ({_SCOPE})"
    )
    op.drop_index("uq_provider_capabilities_contract_scope", table_name="provider_capabilities")
    op.execute(
        "ALTER INDEX public.uq_provider_capabilities_scope_rollback "
        "RENAME TO uq_provider_capabilities_contract_scope"
    )
