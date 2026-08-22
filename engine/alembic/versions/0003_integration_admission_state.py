"""Add integration admission invalidation state.

Revision ID: 0003_integration_admission_state
Revises: 0002_integration_status_version
Create Date: 2026-08-22
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0003_integration_admission_state"
down_revision: str | None = "0002_integration_status_version"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "integration_status",
        sa.Column("mode_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "integration_status",
        sa.Column("capability_invalidated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "integration_status",
        sa.Column("admitted_contract_hash", sa.String(length=64), nullable=True),
    )
    op.execute("UPDATE integration_status SET mode_changed_at = created_at")
    op.execute(
        "UPDATE integration_status SET capability_invalidated_at = CURRENT_TIMESTAMP "
        "WHERE status = 'disabled'"
    )
    op.alter_column(
        "integration_status",
        "mode_changed_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )


def downgrade() -> None:
    op.drop_column("integration_status", "admitted_contract_hash")
    op.drop_column("integration_status", "capability_invalidated_at")
    op.drop_column("integration_status", "mode_changed_at")
