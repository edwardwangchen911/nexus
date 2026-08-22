"""Add versioned integration mode state.

Revision ID: 0002_integration_status_version
Revises: 0001_runtime_and_data
Create Date: 2026-08-22
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0002_integration_status_version"
down_revision: str | None = "0001_runtime_and_data"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "integration_status",
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.add_column(
        "integration_status",
        sa.Column("mode_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute("UPDATE integration_status SET mode_changed_at = created_at")
    op.alter_column(
        "integration_status",
        "mode_changed_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    op.add_column(
        "integration_status",
        sa.Column("admitted_contract_hash", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("integration_status", "admitted_contract_hash")
    op.drop_column("integration_status", "mode_changed_at")
    op.drop_column("integration_status", "version")
