"""Add optimistic-lock versions to integration status.

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


def downgrade() -> None:
    op.drop_column("integration_status", "version")
