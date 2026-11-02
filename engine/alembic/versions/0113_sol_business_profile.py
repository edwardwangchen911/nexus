"""Renewable Sol prices and normal preparation in the existing immutable route.

No existing row or comparison authority changes. Native stage dispatch still
requires its matching version-2 proof. Retain this schema once Sol is registered.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0113_sol_business_profile"
down_revision = "0112_platform_medium_profile"
branch_labels = None
depends_on = None


_AUTH_START = "IF TG_TABLE_NAME='research_stage_authorization' THEN"
_NATIVE_AUTH = """
          IF EXISTS(SELECT 1 FROM public.research_stage_route WHERE run_id=NEW.run_id
                    AND platform_binding IS NOT NULL) THEN
            IF NOT public.nexus_sol_native_authorization_valid(NEW) THEN
              RAISE EXCEPTION 'PLATFORM_NATIVE_AUTHORIZATION_INVALID' USING ERRCODE='23514';
            END IF;
            RETURN NEW;
          END IF;
"""


def _stage_dispatch(*, reverse: bool = False) -> None:
    old, new = _AUTH_START, _AUTH_START + _NATIVE_AUTH
    if reverse:
        old, new = new, old
    definition = op.get_bind().scalar(
        sa.text("SELECT pg_get_functiondef('research_platform_stage_fact_guard()'::regprocedure)")
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("PLATFORM_NATIVE_STAGE_FUNCTION_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))


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
    op.add_column("research_stage_route", sa.Column("platform_binding", JSONB(), nullable=True))
    op.execute("""
    CREATE FUNCTION public.nexus_sol_native_route_valid(v public.research_stage_route)
    RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
    DECLARE m public.agent_model_configs; r public.agent_research_runs;
      b jsonb:=v.platform_binding; a jsonb:=b->'allocation'; price_hash text; item jsonb;
    BEGIN
      SELECT * INTO r FROM public.agent_research_runs WHERE id=v.run_id;
      SELECT * INTO m FROM public.agent_model_configs WHERE id=r.model_config_id;
      IF m.id IS NULL THEN RETURN false; END IF;
      IF m.platform_profile IS NULL THEN RETURN b IS NULL; END IF;
      IF NOT public.nexus_sol_model_profile_valid(m) OR v.kind NOT IN ('formal','independent','increment')
        OR jsonb_typeof(b) IS DISTINCT FROM 'object' OR jsonb_typeof(a) IS DISTINCT FROM 'object'
        OR (SELECT count(*) FROM jsonb_object_keys(b))<>9
        OR (SELECT count(*) FROM jsonb_object_keys(a))<>8
        OR jsonb_typeof(a->'stage_input_upper_bounds') IS DISTINCT FROM 'array'
        OR jsonb_array_length(a->'stage_input_upper_bounds')<>3
        OR jsonb_typeof(a->'stage_schema_envelope_hashes') IS DISTINCT FROM 'array'
        OR jsonb_array_length(a->'stage_schema_envelope_hashes')<>3 THEN RETURN false; END IF;
      FOR item IN SELECT value FROM jsonb_array_elements(a->'stage_input_upper_bounds') LOOP
        IF jsonb_typeof(item)<>'number' OR item::text !~ '^[0-9]+$'
          OR item::text::numeric<=8192 OR item::text::numeric>272000 THEN RETURN false; END IF;
      END LOOP;
      FOR item IN SELECT value FROM jsonb_array_elements(a->'stage_schema_envelope_hashes') LOOP
        IF jsonb_typeof(item)<>'string' OR (item#>>'{}') !~ '^[0-9a-f]{64}$'
          THEN RETURN false; END IF;
      END LOOP;
      price_hash:=encode(sha256(convert_to(public.nexus_economic_json_text(
        m.platform_profile->'price_record',true),'UTF8')),'hex');
      RETURN COALESCE(b->'version'='1'::jsonb AND b->>'transport'='platform_responses'
        AND b->>'model_config_id'=m.id::text AND b->>'model_hash'=m.config_hash
        AND r.model_hash=m.config_hash AND r.pricing_version=m.version
        AND r.reserved_cost_usd=(m.max_input_tokens*m.input_usd_per_million+
          m.max_output_tokens*m.output_usd_per_million)/1000000
        AND b->>'price_record_hash'=price_hash
        AND b->>'price_version'='platform-sol-price-v1-'||price_hash
        AND b->>'credential_binding_hash' ~ '^[0-9a-f]{64}$'
        AND b->>'allocation_hash'=encode(sha256(convert_to(
          public.nexus_economic_json_text(a,false),'UTF8')),'hex')
        AND a->>'input_hash' ~ '^[0-9a-f]{64}$'
        AND a->'profile'=m.platform_profile->'wire_profile'
        AND a->'stage_order'='["research","critique","synthesis"]'::jsonb
        AND a->'output_limit'='8192'::jsonb AND a->'framing_units'='8192'::jsonb
        AND a->>'units_version'='platform-structured-http-utf8-plus8192-v2',false);
    EXCEPTION WHEN OTHERS THEN RETURN false;
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.nexus_sol_native_route_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN
      IF NOT public.nexus_sol_native_route_valid(NEW) OR
        (NEW.platform_binding IS NOT NULL AND NOT EXISTS(
          SELECT 1 FROM public.agent_research_runs r JOIN public.agent_model_configs m
            ON m.id=r.model_config_id JOIN public.agent_daily_budget_reservations b ON b.run_id=r.id
          WHERE r.id=NEW.run_id AND b.state='reserved' AND b.reserved_usd=r.reserved_cost_usd
            AND m.pricing_available_at<=clock_timestamp()
            AND m.pricing_expires_at>=clock_timestamp()+interval '952 seconds')) THEN
        RAISE EXCEPTION 'PLATFORM_NATIVE_ROUTE_INVALID' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$;
    """)
    op.execute("""CREATE TRIGGER trg_sol_native_route BEFORE INSERT ON public.research_stage_route
      FOR EACH ROW EXECUTE FUNCTION public.nexus_sol_native_route_guard()""")
    op.execute("""
    CREATE FUNCTION public.nexus_sol_native_transport_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN
      IF EXISTS(SELECT 1 FROM public.research_stage_route WHERE run_id=NEW.run_id
          AND platform_binding IS NOT NULL) AND
         (NEW.platform_binding IS NULL OR NEW.platform_binding->'version' IS DISTINCT FROM '2'::jsonb)
        THEN RAISE EXCEPTION 'PLATFORM_NATIVE_TRANSPORT_REQUIRED' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$;
    """)
    op.execute("""CREATE TRIGGER trg_sol_native_transport BEFORE INSERT ON public.research_stage_authorization
      FOR EACH ROW EXECUTE FUNCTION public.nexus_sol_native_transport_guard()""")

    op.execute("""
    CREATE FUNCTION public.nexus_sol_native_authorization_valid(v research_stage_authorization)
    RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
    DECLARE r public.research_stage_route; b jsonb:=v.platform_binding; n jsonb; a jsonb;
    BEGIN
      SELECT * INTO r FROM public.research_stage_route WHERE run_id=v.run_id;
      n:=r.platform_binding; a:=n->'allocation';
      IF n IS NULL OR NOT public.nexus_sol_native_route_valid(r)
        OR jsonb_typeof(b) IS DISTINCT FROM 'object'
        OR (SELECT count(*) FROM jsonb_object_keys(b))<>13 OR v.stage NOT BETWEEN 1 AND 3
        THEN RETURN false; END IF;
      RETURN COALESCE(b->'version'='2'::jsonb AND b->>'body_sha256'=v.wire_hash
        AND b->>'price_version'=n->>'price_version'
        AND b->>'price_record_hash'=n->>'price_record_hash'
        AND b->>'model_config_id'=n->>'model_config_id' AND b->>'model_hash'=n->>'model_hash'
        AND b->>'credential_binding_hash'=n->>'credential_binding_hash'
        AND b->>'allocation_hash'=n->>'allocation_hash' AND b->>'input_hash'=a->>'input_hash'
        AND b->'input_upper_bound'=a->'stage_input_upper_bounds'->(v.stage-1)
        AND b->>'cache_mode'='implicit' AND b->>'cache_ttl'='30m'
        AND b->>'binding_hash'=encode(sha256(convert_to(public.nexus_economic_json_text(
          jsonb_build_object('allocation_hash',n->>'allocation_hash',
            'stage',a->'stage_order'->>(v.stage-1),'body_sha256',v.wire_hash),false),'UTF8')),'hex'),false);
    EXCEPTION WHEN OTHERS THEN RETURN false;
    END $$;
    """)
    _stage_dispatch()


def downgrade() -> None:
    op.execute("LOCK TABLE agent_model_configs IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM agent_model_configs WHERE platform_profile IS NOT NULL)"
        )
    ):
        raise RuntimeError("PLATFORM_SOL_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    _stage_dispatch(reverse=True)
    op.execute(
        "DROP FUNCTION public.nexus_sol_native_authorization_valid(public.research_stage_authorization)"
    )
    op.execute("DROP TRIGGER trg_sol_native_transport ON research_stage_authorization")
    op.execute("DROP FUNCTION public.nexus_sol_native_transport_guard()")
    op.execute("DROP TRIGGER trg_sol_native_route ON research_stage_route")
    op.execute("DROP FUNCTION public.nexus_sol_native_route_guard()")
    op.execute("DROP FUNCTION public.nexus_sol_native_route_valid(public.research_stage_route)")
    op.drop_column("research_stage_route", "platform_binding")
    op.execute("DROP TRIGGER trg_sol_model_profile ON agent_model_configs")
    op.execute("DROP FUNCTION public.nexus_sol_model_profile_guard()")
    op.execute("DROP FUNCTION public.nexus_sol_model_profile_valid(public.agent_model_configs)")
    op.drop_column("agent_model_configs", "platform_profile")
