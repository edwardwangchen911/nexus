"""Admit exact as-disclosed cash-flow components; no historical rows change.

New projections use their actual creation time and stay invisible to old cutoffs.
A compatible application rollback retains this schema and its new facts.
"""

from alembic import op
import sqlalchemy as sa

revision = "0121_sec_cash_flow_components"
down_revision = "0120_sol_formal_output_binding"
branch_labels = None
depends_on = None

_OLD = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cash_flow",
    "capital_expenditure",
    "total_assets",
    "total_liabilities",
    "stockholders_equity",
    "diluted_shares",
    "diluted_eps",
)
_NEW = _OLD + (
    "depreciation_depletion_amortization",
    "share_based_compensation",
    "deferred_income_tax_expense_benefit",
    "investment_gain_loss",
    "other_noncash_income_expense",
    "cash_flow_change_accounts_receivable",
    "cash_flow_change_inventories",
    "cash_flow_change_accounts_payable",
    "cash_flow_change_accrued_operating_liabilities",
    "cash_flow_change_prepaid_other_assets",
    "cash_flow_change_other_noncurrent_liabilities",
)


def _constraint(fields):
    return "canonical_fact IN (" + ", ".join("'" + f + "'" for f in fields) + ")"


def _replace(fields):
    op.drop_constraint(
        op.f("ck_financial_facts_canonical_fact_allowed"), "financial_facts", type_="check"
    )
    op.create_check_constraint(
        op.f("ck_financial_facts_canonical_fact_allowed"), "financial_facts", _constraint(fields)
    )


def upgrade():
    _replace(_NEW)


def downgrade():
    op.execute("LOCK TABLE financial_facts IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM financial_facts WHERE NOT (" + _constraint(_OLD) + "))"
        )
    ):
        raise RuntimeError("SEC_COMPONENT_DOWNGRADE_FORBIDDEN")
    _replace(_OLD)
