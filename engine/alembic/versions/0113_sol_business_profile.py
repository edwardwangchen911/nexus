"""Renewable Sol prices in the existing immutable model registry.

No existing row or comparison authority changes. This does not authorize a
normal producer/stage yet. Once registered, retain this schema for rollback.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0113_sol_business_profile"
down_revision = "0112_platform_medium_profile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_model_configs", sa.Column("platform_profile", JSONB(), nullable=True))
    op.execute("""
    CREATE FUNCTION public.nexus_sol_model_profile_valid(m public.agent_model_configs)
    RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
    DECLARE p jsonb:=m.platform_profile; r jsonb:=p->'price_record'; w jsonb:=p->'wire_profile';
      h text; price_hash text; field_name text;
    BEGIN
      IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR jsonb_typeof(r) IS DISTINCT FROM 'object'
         OR (SELECT count(*) FROM jsonb_object_keys(p))<>7
         OR (SELECT count(*) FROM jsonb_object_keys(r))<>14 THEN RETURN false; END IF;
      FOREACH field_name IN ARRAY ARRAY['ordinary_input_usd_per_million','cache_read_usd_per_million',
        'cache_write_30m_usd_per_million','output_usd_per_million'] LOOP
        IF jsonb_typeof(r->field_name) IS DISTINCT FROM 'string'
           OR (r->>field_name) !~ '^(0|[1-9][0-9]{0,11})(\\.[0-9]{1,6})?$'
          THEN RETURN false; END IF;
      END LOOP;
      price_hash:=encode(sha256(convert_to(public.nexus_economic_json_text(r,true),'UTF8')),'hex');
      h:=encode(sha256(convert_to(public.nexus_economic_json_text(p,true),'UTF8')),'hex');
      RETURN COALESCE(
        p->>'contract'='platform-sol-business-v1'
        AND p->'business_consumption_allowed'='true'::jsonb
        AND p->'max_input_tokens'='816000'::jsonb
        AND p->'max_output_tokens'='8192'::jsonb AND p->'max_tool_calls'='0'::jsonb
        AND r->>'schema_version'='platform-sol-pricing-v1'
        AND r->>'model_id'='gpt-6.1-sol' AND r->>'service_tier'='default'
        AND r->>'cache_mode'='implicit' AND r->>'cache_ttl'='30m'
        AND r->>'currency'='USD'
        AND r->>'source_url'='https://developers.openai.com/api/docs/pricing?latest-pricing=standard'
        AND r->>'source_evidence_sha256' ~ '^[0-9a-f]{64}$'
        AND r->>'verified_at' ~ 'Z$' AND r->>'expires_at' ~ 'Z$'
        AND (r->>'expires_at')::timestamptz>(r->>'verified_at')::timestamptz
        AND (r->>'expires_at')::timestamptz<=(r->>'verified_at')::timestamptz+interval '168 hours'
        AND m.model_id='gpt-6.1-sol' AND m.version='platform-sol-business-v1-'||h
        AND m.config_hash=h AND m.max_input_tokens=816000 AND m.max_output_tokens=8192
        AND m.max_tool_calls=0
        AND m.input_usd_per_million=greatest((r->>'ordinary_input_usd_per_million')::numeric,
          (r->>'cache_read_usd_per_million')::numeric,(r->>'cache_write_30m_usd_per_million')::numeric)
        AND m.output_usd_per_million=(r->>'output_usd_per_million')::numeric
        AND m.pricing_available_at=(r->>'verified_at')::timestamptz
        AND m.pricing_expires_at=(r->>'expires_at')::timestamptz
        AND w=jsonb_build_object('credential_kind','platform_project_key',
          'endpoint','https://api.openai.com/v1/responses','model','gpt-6.1-sol','effort','medium',
          'service_tier','default','cache_mode','implicit','cache_ttl','30m','stream',true,'store',false,
          'price_version','platform-sol-price-v1-'||price_hash,
          'base_instructions_hash','9c3ae749ff048f6828963ad24fb9eee3229af795fe98cb4610f74a55116cc5e5',
          'developer_instructions_hash','47a7cddaf28462c2d54edd7f5e1ff3dd28da50d42f120847bee6aaf58f120f60'),
        false);
    EXCEPTION WHEN OTHERS THEN RETURN false;
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.nexus_sol_model_profile_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN
      IF NEW.platform_profile IS NOT NULL AND NOT public.nexus_sol_model_profile_valid(NEW) THEN
        RAISE EXCEPTION 'PLATFORM_SOL_PROFILE_INVALID' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;
    """)
    op.execute("""
    CREATE TRIGGER trg_sol_model_profile BEFORE INSERT ON public.agent_model_configs
      FOR EACH ROW EXECUTE FUNCTION public.nexus_sol_model_profile_guard();
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE agent_model_configs IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM agent_model_configs WHERE platform_profile IS NOT NULL)"
        )
    ):
        raise RuntimeError("PLATFORM_SOL_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("DROP TRIGGER trg_sol_model_profile ON agent_model_configs")
    op.execute("DROP FUNCTION public.nexus_sol_model_profile_guard()")
    op.execute("DROP FUNCTION public.nexus_sol_model_profile_valid(public.agent_model_configs)")
    op.drop_column("agent_model_configs", "platform_profile")
