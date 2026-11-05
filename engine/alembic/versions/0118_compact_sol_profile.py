"""Freeze compact output in the existing immutable native model configuration.

No data rows are rewritten. Old profiles and queued roots remain v2. A compact
profile is a new hash under the same model, pricing, budget and owner contracts.
Keep this reader schema once a compact profile exists; maintenance rollback
uses compatible code, not deletion of the new interpretation.
"""

from alembic import op
from sqlalchemy import text

revision = "0118_compact_sol_profile"
down_revision = "0117_budgeted_single_sol"
branch_labels = None
depends_on = None

_V2 = "platform-structured-http-utf8-plus8192-v2"
_V3 = "platform-compact-json-http-utf8-plus8192-v3"
_OLD_SHAPE = "OR (SELECT count(*) FROM jsonb_object_keys(p))<>7"
_NEW_SHAPE = f"""OR (((NOT (p ? 'output_units_version')
           AND (SELECT count(*) FROM jsonb_object_keys(p))=7)
         OR (p->>'output_units_version'='{_V3}'
           AND (SELECT count(*) FROM jsonb_object_keys(p))=8)) IS NOT TRUE)"""
_OLD_ROUTE = f"AND a->>'units_version'='{_V2}'"
_NEW_ROUTE = (
    f"AND a->>'units_version'=COALESCE(m.platform_profile->>'output_units_version','{_V2}')"
)


def _replace(signature: str, before: str, after: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:signature))"),
        {"signature": "public." + signature},
    )
    if definition is None or definition.count(before) != 1:
        raise RuntimeError("COMPACT_SOL_PREDECESSOR_INVALID")
    op.execute(definition.replace(before, after))


def upgrade() -> None:
    _replace("nexus_sol_model_profile_valid(agent_model_configs)", _OLD_SHAPE, _NEW_SHAPE)
    _replace("nexus_sol_native_route_valid(research_stage_route)", _OLD_ROUTE, _NEW_ROUTE)


def downgrade() -> None:
    # Fence normal profile INSERTs before the READ COMMITTED existence check.
    # Keep this transaction lock through function replacement and schema commit;
    # an in-flight writer makes maintenance fail safely rather than wait.
    op.execute("LOCK TABLE agent_model_configs IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    op.execute(f"""DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM agent_model_configs
                WHERE platform_profile->>'output_units_version'='{_V3}') THEN
        RAISE EXCEPTION 'COMPACT_SOL_DOWNGRADE_FORBIDDEN';
      END IF;
    END $$""")
    _replace("nexus_sol_native_route_valid(research_stage_route)", _NEW_ROUTE, _OLD_ROUTE)
    _replace("nexus_sol_model_profile_valid(agent_model_configs)", _NEW_SHAPE, _OLD_SHAPE)
