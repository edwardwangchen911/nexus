"""Admit versioned API schemas without rewriting historical allocations or fees.

The same price, purpose, budget and execution guards remain authoritative.
Once v2 bytes are registered, rollback requires a compatible reader image.
"""

from alembic import op
from sqlalchemy import text

revision = "0110_platform_structured_schema"
down_revision = "0109_platform_third_round"
branch_labels = None
depends_on = None

_OLD = "a->>'units_version'='platform-full-http-utf8-plus8192-v1'"
_NEW = """a->>'units_version' IN ('platform-full-http-utf8-plus8192-v1',
       'platform-structured-http-utf8-plus8192-v2')
     AND a->>'units_version'=(p->'allocations'->0->>'units_version')"""


def _replace(reverse: bool = False) -> None:
    old, new = (_NEW, _OLD) if reverse else (_OLD, _NEW)
    definition = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.research_platform_manifest_v1_valid(jsonb)'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("PLATFORM_STRUCTURED_SCHEMA_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    _replace()


def downgrade() -> None:
    op.execute("LOCK TABLE research_comparison_manifest IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        text("""SELECT EXISTS(
      SELECT 1 FROM research_comparison_manifest m,
        jsonb_array_elements(m.payload->'allocations') a
      WHERE a->>'units_version'='platform-structured-http-utf8-plus8192-v2')""")
    ):
        raise RuntimeError("PLATFORM_STRUCTURED_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    _replace(reverse=True)
