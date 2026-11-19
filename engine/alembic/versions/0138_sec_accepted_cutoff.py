"""Require supported acceptance evidence semantics on future foundation checks."""

from alembic import op

revision = "0138_sec_accepted_cutoff"
down_revision = "0137_linear_json_canonical"
branch_labels = None
depends_on = None

_CONSTRAINT = "ck_sec_discovery_checks_accepted_cutoff_v1"


def upgrade() -> None:
    # NOT VALID preserves existing immutable, audited observations. Every new
    # insert is checked, including one made by an old writer or with a backdated clock.
    op.execute(
        f"""
        ALTER TABLE public.sec_discovery_checks ADD CONSTRAINT {_CONSTRAINT}
        CHECK (
          observation->>'evaluation_version' IS NOT DISTINCT FROM 'accepted_cutoff_v1'
          AND observation->'accepted_complete_through_at' IS NOT DISTINCT FROM 'null'::jsonb
          AND NOT (observation ? 'coverage_proof')
          AND coverage <> 'complete'
          AND result <> 'false'
        ) NOT VALID
        """
    )


def downgrade() -> None:
    # Operational rollback keeps the constraint until the old producer is fenced.
    op.execute(f"ALTER TABLE public.sec_discovery_checks DROP CONSTRAINT {_CONSTRAINT}")
