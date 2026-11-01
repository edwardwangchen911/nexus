"""One separately authorized qualification; keep consumed purposes and price v2 frozen.

Only the explicit new purpose is admitted. No rows or prices are rewritten.
After a new manifest exists, use a compatible reader rather than downgrade.
"""

from alembic import op
from sqlalchemy import text
from nexus.persistence.platform_restore import PLATFORM_LOOKUP_REPAIR

revision = "0109_platform_third_round"
down_revision = "0108_platform_restore_lookup"
branch_labels = None
depends_on = None

_OLD = "'platform-gifted-terra-v1-20261031','platform-terra-v2-20261101'"
_NEW = _OLD + ",'platform-terra-v3-20261101'"


def _purposes(reverse: bool = False) -> None:
    old, new = (_NEW, _OLD) if reverse else (_OLD, _NEW)
    for signature in (
        "research_comparison_binding(uuid,uuid,uuid)",
        "research_platform_stage_fact_guard()",
        "research_platform_position_guard()",
        "research_platform_dispatch_guard()",
        "nexus_validate_agent_snapshot_evidence()",
    ):
        definition = op.get_bind().scalar(
            text("SELECT pg_get_functiondef(CAST(:s AS regprocedure))"), {"s": signature}
        )
        if not isinstance(definition, str) or old not in definition:
            raise RuntimeError("PLATFORM_THIRD_PURPOSE_UNEXPECTED")
        op.get_bind().exec_driver_sql(definition.replace(old, new))
    op.execute(f"""ALTER TABLE research_comparison_manifest
      DROP CONSTRAINT research_comparison_manifest_purpose_check,
      ADD CONSTRAINT research_comparison_manifest_purpose_check
      CHECK(purpose IN ('direct-responses-terra-v11-20261030',{new}))""")


def _drop_contract() -> None:
    op.execute(
        "ALTER TABLE research_comparison_manifest DROP CONSTRAINT research_platform_manifest_contract"
    )


def _add_contract() -> None:
    op.execute("""ALTER TABLE research_comparison_manifest
      ADD CONSTRAINT research_platform_manifest_contract CHECK(
        purpose='direct-responses-terra-v11-20261030' OR
        (payload->>'platform_contract'=purpose AND public.research_platform_manifest_valid(payload)))""")


def upgrade() -> None:
    op.execute(PLATFORM_LOOKUP_REPAIR)
    _drop_contract()
    op.execute(
        "ALTER FUNCTION public.research_platform_manifest_valid(jsonb) RENAME TO research_platform_manifest_v2_valid"
    )
    op.execute("""CREATE FUNCTION public.research_platform_manifest_valid(p jsonb)
      RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(
        public.research_platform_manifest_v2_valid(p) OR (
          p->>'platform_contract'='platform-terra-v3-20261101' AND
          public.research_platform_manifest_v2_valid(jsonb_set(p,'{platform_contract}',
            '"platform-terra-v2-20261101"'::jsonb))),false) $$""")
    _add_contract()
    _purposes()


def downgrade() -> None:
    op.execute("LOCK TABLE research_comparison_manifest IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM research_comparison_manifest WHERE purpose='platform-terra-v3-20261101')"
        )
    ):
        raise RuntimeError("PLATFORM_THIRD_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    _purposes(reverse=True)
    _drop_contract()
    op.execute("DROP FUNCTION public.research_platform_manifest_valid(jsonb)")
    op.execute(
        "ALTER FUNCTION public.research_platform_manifest_v2_valid(jsonb) RENAME TO research_platform_manifest_valid"
    )
    _add_contract()
