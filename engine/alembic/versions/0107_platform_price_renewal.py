"""Add one exact renewed Platform identity, retaining every historical contract.

No historical row is rewritten. Once v2 facts exist, rollback requires a reader
supporting both versions; a database downgrade must not erase their protection.
"""

from alembic import op
from sqlalchemy import text

revision = "0107_platform_price_renewal"
down_revision = "0106_platform_qualification"
branch_labels = None
depends_on = None

_OLD_VERSION = "platform-terra-default-implicit30m-20261031-v1"
_NEW_VERSION = "platform-terra-default-implicit30m-20261101-v2"
_OLD_HASH = "64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a"
_NEW_HASH = "e5a3e02ebd9b5276352224056c95880cef99e6230ed7219eef617551f6642f5d"
_OLD_PURPOSE = "platform-gifted-terra-v1-20261031"
_NEW_PURPOSE = "platform-terra-v2-20261101"
_OLD_BINDING = f"model.config_hash='{_OLD_HASH}'\n            AND model.version='{_OLD_VERSION}'"
_NEW_BINDING = (
    f"(model.config_hash,model.version) IN (('{_OLD_HASH}','{_OLD_VERSION}'),"
    f"('{_NEW_HASH}','{_NEW_VERSION}'))"
)
_OLD_STAGE = f"NEW.platform_binding->>'price_version' IS DISTINCT FROM '{_OLD_VERSION}'"
_NEW_STAGE = (
    "(NEW.platform_binding->>'price_version' IS DISTINCT FROM "
    "(SELECT pricing_version FROM agent_research_runs WHERE id=NEW.run_id) "
    f"OR NEW.platform_binding->>'price_version' NOT IN ('{_OLD_VERSION}','{_NEW_VERSION}'))"
)


def _replace_function(signature: str, old: str, new: str) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
        {"signature": signature},
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("PLATFORM_RENEWAL_FUNCTION_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))


def _purpose_functions(*, reverse: bool = False) -> None:
    pairs = (
        (f"m.purpose='{_OLD_PURPOSE}'", f"m.purpose IN ('{_OLD_PURPOSE}','{_NEW_PURPOSE}')"),
        (
            f"('direct-responses-terra-v11-20261030','{_OLD_PURPOSE}')",
            f"('direct-responses-terra-v11-20261030','{_OLD_PURPOSE}','{_NEW_PURPOSE}')",
        ),
    )
    for signature in (
        "research_comparison_binding(uuid,uuid,uuid)",
        "research_platform_stage_fact_guard()",
        "research_platform_position_guard()",
        "research_platform_dispatch_guard()",
        "nexus_validate_agent_snapshot_evidence()",
    ):
        definition = op.get_bind().scalar(
            text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
            {"signature": signature},
        )
        updated = definition
        for old, new in pairs:
            updated = updated.replace(new, old) if reverse else updated.replace(old, new)
        if updated == definition:
            raise RuntimeError("PLATFORM_RENEWAL_PURPOSE_UNEXPECTED")
        op.get_bind().exec_driver_sql(updated)


def _constraint(*, renewed: bool = True) -> None:
    match = "payload->>'platform_contract'=purpose AND " if renewed else ""
    op.execute(f"""ALTER TABLE research_comparison_manifest
      ADD CONSTRAINT research_platform_manifest_contract CHECK(
        purpose='direct-responses-terra-v11-20261030' OR ({match}research_platform_manifest_valid(payload)))""")
    purposes = f"'direct-responses-terra-v11-20261030','{_OLD_PURPOSE}'"
    if renewed:
        purposes += f",'{_NEW_PURPOSE}'"
    op.execute(f"""ALTER TABLE research_comparison_manifest
      DROP CONSTRAINT research_comparison_manifest_purpose_check,
      ADD CONSTRAINT research_comparison_manifest_purpose_check CHECK(purpose IN ({purposes}))""")


def upgrade() -> None:
    op.execute(
        "ALTER TABLE research_comparison_manifest DROP CONSTRAINT research_platform_manifest_contract"
    )
    op.execute(
        "ALTER FUNCTION research_platform_manifest_valid(jsonb) RENAME TO research_platform_manifest_v1_valid"
    )
    # Only the purpose and price identity fields differ. Project a local argument onto the
    # frozen v1 validator; no stored payload is normalized or updated.
    op.execute(f"""CREATE FUNCTION research_platform_manifest_valid(p jsonb) RETURNS boolean
    LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(
      research_platform_manifest_v1_valid(p) OR (
        p->>'platform_contract'='{_NEW_PURPOSE}' AND p->>'api_config_hash'='{_NEW_HASH}'
        AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'allocations') a
          WHERE a->'profile'->>'price_version' IS DISTINCT FROM '{_NEW_VERSION}')
        AND research_platform_manifest_v1_valid(
          jsonb_set(jsonb_set(jsonb_set(p,'{{platform_contract}}','"{_OLD_PURPOSE}"'::jsonb),
            '{{api_config_hash}}','"{_OLD_HASH}"'::jsonb),
            '{{allocations}}',(SELECT jsonb_agg(
              jsonb_set(a,'{{profile,price_version}}','"{_OLD_VERSION}"'::jsonb) ORDER BY n)
              FROM jsonb_array_elements(p->'allocations') WITH ORDINALITY item(a,n))))
      ),false) $$""")
    _constraint()
    _replace_function("research_comparison_binding(uuid,uuid,uuid)", _OLD_BINDING, _NEW_BINDING)
    _replace_function("research_platform_stage_fact_guard()", _OLD_STAGE, _NEW_STAGE)
    _purpose_functions()


def downgrade() -> None:
    op.execute("""LOCK TABLE research_comparison_manifest,agent_model_configs,
      research_stage_authorization,jobs IN ACCESS EXCLUSIVE MODE NOWAIT""")
    if op.get_bind().scalar(
        text(f"""SELECT
      EXISTS(SELECT 1 FROM agent_model_configs WHERE version='{_NEW_VERSION}') OR
      EXISTS(SELECT 1 FROM research_comparison_manifest WHERE payload->>'api_config_hash'='{_NEW_HASH}') OR
      EXISTS(SELECT 1 FROM research_stage_authorization WHERE platform_binding->>'price_version'='{_NEW_VERSION}')""")
    ):
        raise RuntimeError("PLATFORM_RENEWAL_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    _replace_function("research_comparison_binding(uuid,uuid,uuid)", _NEW_BINDING, _OLD_BINDING)
    _replace_function("research_platform_stage_fact_guard()", _NEW_STAGE, _OLD_STAGE)
    _purpose_functions(reverse=True)
    op.execute(
        "ALTER TABLE research_comparison_manifest DROP CONSTRAINT research_platform_manifest_contract"
    )
    op.execute("DROP FUNCTION research_platform_manifest_valid(jsonb)")
    op.execute(
        "ALTER FUNCTION research_platform_manifest_v1_valid(jsonb) RENAME TO research_platform_manifest_valid"
    )
    _constraint(renewed=False)
