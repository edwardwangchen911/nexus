"""Opt-in medium diagnostic; retain exact low contracts and compatible rollback.

No historical row, price, prompt, limit or default is changed. A medium profile
is supported only by the stable qualification protocol, never a business run.
"""

from alembic import op
from sqlalchemy import text

revision = "0112_platform_medium_profile"
down_revision = "0111_platform_round_registry"
branch_labels = None
depends_on = None

_V1 = "platform-terra-default-implicit30m-20261031-v1"
_LOW = "platform-terra-default-implicit30m-20261101-v2"
_MEDIUM = "platform-terra-medium-implicit30m-20261101-v1"
_V1_HASH = "64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a"
_LOW_HASH = "e5a3e02ebd9b5276352224056c95880cef99e6230ed7219eef617551f6642f5d"
_MEDIUM_HASH = "bccfe955523567268db8daa6a9d56a187db776066927e3b5075763cbeaa12099"
_OLD_BINDING = (
    f"(model.config_hash,model.version) IN (('{_V1_HASH}','{_V1}'),('{_LOW_HASH}','{_LOW}'))"
)
_NEW_BINDING = _OLD_BINDING[:-1] + f",('{_MEDIUM_HASH}','{_MEDIUM}'))"
_OLD_STAGE = f"NOT IN ('{_V1}','{_LOW}')"
_NEW_STAGE = f"NOT IN ('{_V1}','{_LOW}','{_MEDIUM}')"


def _routes(reverse: bool = False) -> None:
    for signature, old, new in (
        ("research_comparison_binding(uuid,uuid,uuid)", _OLD_BINDING, _NEW_BINDING),
        ("research_platform_stage_fact_guard()", _OLD_STAGE, _NEW_STAGE),
    ):
        if reverse:
            old, new = new, old
        definition = op.get_bind().scalar(
            text("SELECT pg_get_functiondef(CAST(:s AS regprocedure))"), {"s": signature}
        )
        if not isinstance(definition, str) or definition.count(old) != 1:
            raise RuntimeError("PLATFORM_MEDIUM_FUNCTION_UNEXPECTED")
        op.get_bind().exec_driver_sql(definition.replace(old, new))


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
    _drop_contract()
    op.execute(
        "ALTER FUNCTION public.research_platform_manifest_valid(jsonb) RENAME TO research_platform_manifest_low_valid"
    )
    # Project only this explicit new configuration onto the unchanged low validator.
    # This changes a function argument, never a persisted manifest or allocation.
    op.execute(f"""CREATE FUNCTION public.research_platform_manifest_valid(p jsonb)
      RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(
        public.research_platform_manifest_low_valid(p) OR (
          p->>'platform_contract'='platform-qualification-v1'
          AND p->>'api_config_hash'='{_MEDIUM_HASH}'
          AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'allocations') a
            WHERE a->'profile'->>'price_version' IS DISTINCT FROM '{_MEDIUM}'
               OR a->'profile'->>'effort' IS DISTINCT FROM 'medium'
               OR a->>'units_version' IS DISTINCT FROM 'platform-structured-http-utf8-plus8192-v2')
          AND public.research_platform_manifest_low_valid(
            jsonb_set(jsonb_set(p,'{{api_config_hash}}','"{_LOW_HASH}"'::jsonb),
              '{{allocations}}',(SELECT jsonb_agg(
                jsonb_set(jsonb_set(a,'{{profile,price_version}}','"{_LOW}"'::jsonb),
                  '{{profile,effort}}','"low"'::jsonb) ORDER BY n)
                FROM jsonb_array_elements(p->'allocations') WITH ORDINALITY item(a,n))))
        ),false) $$""")
    _add_contract()
    _routes()


def downgrade() -> None:
    op.execute("""LOCK TABLE research_comparison_manifest,agent_model_configs,
      research_stage_authorization,jobs IN ACCESS EXCLUSIVE MODE NOWAIT""")
    if op.get_bind().scalar(
        text(f"""SELECT
      EXISTS(SELECT 1 FROM agent_model_configs WHERE version='{_MEDIUM}') OR
      EXISTS(SELECT 1 FROM research_comparison_manifest WHERE payload->>'api_config_hash'='{_MEDIUM_HASH}') OR
      EXISTS(SELECT 1 FROM research_stage_authorization WHERE platform_binding->>'price_version'='{_MEDIUM}')""")
    ):
        raise RuntimeError("PLATFORM_MEDIUM_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    _routes(reverse=True)
    _drop_contract()
    op.execute("DROP FUNCTION public.research_platform_manifest_valid(jsonb)")
    op.execute(
        "ALTER FUNCTION public.research_platform_manifest_low_valid(jsonb) RENAME TO research_platform_manifest_valid"
    )
    _add_contract()
