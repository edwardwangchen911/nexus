"""Join formal budget pins and formal research output gates without changing either.

Revision ID: 0069_formal_budget_output_merge
Revises: 0067_formal_budget_binding, 0068_formal_output_binding
"""

revision = "0069_formal_budget_output_merge"
down_revision = ("0067_formal_budget_binding", "0068_formal_output_binding")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Both parent migrations own their DDL; this revision only joins history."""
    pass


def downgrade() -> None:
    """Restore two version heads without removing either parent's facts or guards."""
    pass
