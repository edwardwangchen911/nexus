"""Qualify frozen validator lookups without changing data or policy.

Downgrading the marker retains the compatible lookup fix. Reintroducing the
restore failure is not a rollback strategy; no new data shape is introduced.
"""

from alembic import op
from nexus.persistence.platform_restore import PLATFORM_LOOKUP_REPAIR

revision = "0108_platform_restore_lookup"
down_revision = "0107_platform_price_renewal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(PLATFORM_LOOKUP_REPAIR)


def downgrade() -> None:
    op.execute(PLATFORM_LOOKUP_REPAIR)
