"""Keep evaluation challengers out of the active Agent prompt authority.

Revision ID: 0064_prompt_release_channel
Revises: 0063_agent_version_pair_context
Create Date: 2026-09-17
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "0064_prompt_release_channel"
down_revision: str | Sequence[str] | None = "0063_agent_version_pair_context"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM agent_version_evaluations) THEN
            RAISE EXCEPTION
              'existing agent version evaluations require explicit release-channel migration';
          END IF;
        END
        $$
        """
    )
    op.add_column(
        "agent_prompt_versions",
        sa.Column(
            "release_channel",
            sa.String(length=16),
            nullable=False,
            server_default="active",
        ),
    )
    op.create_check_constraint(
        "agent_prompt_versions_release_channel_allowed",
        "agent_prompt_versions",
        "release_channel IN ('active','challenger')",
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
              SELECT 1 FROM agent_prompt_versions
               WHERE release_channel = 'challenger'
          ) THEN
            RAISE EXCEPTION
              'cannot remove prompt release channel while challenger prompts exist';
          END IF;
        END
        $$
        """
    )
    op.drop_constraint(
        "agent_prompt_versions_release_channel_allowed",
        "agent_prompt_versions",
        type_="check",
    )
    op.drop_column("agent_prompt_versions", "release_channel")
