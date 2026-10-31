"""Fixed three-position Platform qualification; old purpose and native ledger remain intact.

No new ledger or business dispatch. Populated Platform facts require compatible
reader rollback; downgrade never erases a consumed purpose or held reservation.
"""

from alembic import op
from sqlalchemy import text

revision = "0106_platform_qualification"
down_revision = "0105_comparison_preclaim"
branch_labels = None
depends_on = None

_OLD_BINDING = """CREATE OR REPLACE FUNCTION research_comparison_binding(run uuid,job uuid,experiment uuid)
      RETURNS boolean LANGUAGE sql STABLE AS $$
      SELECT EXISTS(
        SELECT 1 FROM research_comparison_chain c
        JOIN research_comparison_manifest m ON m.id=c.experiment_id
        JOIN agent_research_runs r ON r.id=c.run_id
        JOIN jobs j ON j.id=c.job_id
        JOIN agent_model_configs model ON model.id=r.model_config_id
        JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id AND b.run_id=r.id
        JOIN data_snapshots s ON s.id=r.data_snapshot_id
        CROSS JOIN LATERAL (SELECT (ARRAY[0,0,1,1,2,2,2,2,1,1,0,0])[c.ordinal] AS i) slot
        WHERE c.run_id=run AND c.job_id=job AND c.experiment_id=experiment
          AND m.purpose='direct-responses-terra-v11-20261030'
          AND m.payload->>'experiment_id'=m.id::text
          AND r.actor_id='research-comparison:direct-responses-terra-v11'
          AND c.input_hash=r.input_hash
          AND m.payload->'input_hashes'->>slot.i=c.input_hash
          AND m.payload->'instrument_ids'->>slot.i=r.instrument_id::text
          AND r.model_hash=model.config_hash AND r.pricing_version=model.version
          AND model.config_hash=m.payload->>CASE WHEN c.ordinal IN (1,3,5,8,10,12)
            THEN 'sdk_config_hash' ELSE 'api_config_hash' END
          AND model.model_id='gpt-5.6-terra' AND model.max_input_tokens=96000
          AND model.max_output_tokens=8192 AND model.max_tool_calls=0
          AND b.pricing_version=r.pricing_version AND b.reserved_usd=r.reserved_cost_usd
          AND r.idempotency_key='comparison:' || m.id::text || ':' || c.ordinal::text
          AND j.job_key=r.idempotency_key
          AND j.job_type='agent.research.comparison' AND j.schema_version='comparison-v1'
          AND j.queue='strategy_rnd' AND j.scope_kind='instrument'
          AND j.scope_id=r.instrument_id::text AND j.effective_at=r.decision_time
          AND j.payload=jsonb_build_object('run_id',r.id::text)
          AND j.retry_policy='never' AND j.max_attempts=1 AND j.manual_retry_count=0
          AND r.retry_count=0 AND s.source='nexus_research_comparison'
          AND s.schema_version='comparison-input-v1' AND s.payload->>'purpose'=m.purpose
          AND s.payload->>'experiment_id'=m.id::text
          AND s.payload->>'input_hash'=c.input_hash
          AND s.payload->>'source_manifest_hash'=m.payload->>'source_manifest_hash'
          AND s.payload->>'execution_locked'='true'
          AND s.payload->>'formal_consumption_allowed'='false'
          AND s.payload->'frozen'->'request'->>'data_snapshot_id'=s.id::text
          AND s.payload->'frozen'->'request'->>'instrument_id'=r.instrument_id::text
          AND (s.payload->'frozen'->'request'->>'decision_time')::timestamptz=r.decision_time
      ) $$"""

_NEW_BINDING = """CREATE OR REPLACE FUNCTION research_comparison_binding(run uuid,job uuid,experiment uuid)
      RETURNS boolean LANGUAGE sql STABLE AS $$
      SELECT EXISTS(
        SELECT 1 FROM research_comparison_chain c
        JOIN research_comparison_manifest m ON m.id=c.experiment_id
        JOIN agent_research_runs r ON r.id=c.run_id
        JOIN jobs j ON j.id=c.job_id
        JOIN agent_model_configs model ON model.id=r.model_config_id
        JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id AND b.run_id=r.id
        JOIN data_snapshots s ON s.id=r.data_snapshot_id
        CROSS JOIN LATERAL (SELECT CASE WHEN m.purpose='platform-gifted-terra-v1-20261031' THEN c.ordinal-1 ELSE (ARRAY[0,0,1,1,2,2,2,2,1,1,0,0])[c.ordinal] END AS i) slot
        WHERE c.run_id=run AND c.job_id=job AND c.experiment_id=experiment
          AND m.purpose IN ('direct-responses-terra-v11-20261030','platform-gifted-terra-v1-20261031')
          AND m.payload->>'experiment_id'=m.id::text
          AND r.actor_id=CASE WHEN m.purpose='platform-gifted-terra-v1-20261031' THEN 'research-comparison:platform-gifted-terra-v1' ELSE 'research-comparison:direct-responses-terra-v11' END

          AND (m.purpose='direct-responses-terra-v11-20261030' OR (
            c.ordinal BETWEEN 1 AND 3 AND research_platform_manifest_valid(m.payload)
            AND model.config_hash='64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a'
            AND model.version='platform-terra-default-implicit30m-20261031-v1'
            AND model.input_usd_per_million=2.5 AND model.output_usd_per_million=12
            AND r.reserved_cost_usd=(
              SELECT sum(value::text::numeric)*0.0000025+8192*0.000012
              FROM jsonb_array_elements(m.payload->'allocations'->slot.i->'stage_input_upper_bounds'))
          ))
          AND c.input_hash=r.input_hash
          AND m.payload->'input_hashes'->>slot.i=c.input_hash
          AND m.payload->'instrument_ids'->>slot.i=r.instrument_id::text
          AND r.model_hash=model.config_hash AND r.pricing_version=model.version
          AND model.config_hash=m.payload->>CASE WHEN m.purpose='direct-responses-terra-v11-20261030' AND c.ordinal IN (1,3,5,8,10,12)
            THEN 'sdk_config_hash' ELSE 'api_config_hash' END
          AND model.model_id='gpt-5.6-terra' AND model.max_input_tokens=CASE WHEN m.purpose='platform-gifted-terra-v1-20261031' THEN 816000 ELSE 96000 END
          AND model.max_output_tokens=8192 AND model.max_tool_calls=0
          AND b.pricing_version=r.pricing_version AND b.reserved_usd=r.reserved_cost_usd
          AND r.idempotency_key='comparison:' || m.id::text || ':' || c.ordinal::text
          AND j.job_key=r.idempotency_key
          AND j.job_type='agent.research.comparison' AND j.schema_version='comparison-v1'
          AND j.queue='strategy_rnd' AND j.scope_kind='instrument'
          AND j.scope_id=r.instrument_id::text AND j.effective_at=r.decision_time
          AND j.payload=jsonb_build_object('run_id',r.id::text)
          AND j.retry_policy='never' AND j.max_attempts=1 AND j.manual_retry_count=0
          AND r.retry_count=0 AND s.source='nexus_research_comparison'
          AND s.schema_version='comparison-input-v1' AND s.payload->>'purpose'=m.purpose
          AND s.payload->>'experiment_id'=m.id::text
          AND s.payload->>'input_hash'=c.input_hash
          AND s.payload->>'source_manifest_hash'=m.payload->>'source_manifest_hash'
          AND s.payload->>'execution_locked'='true'
          AND s.payload->>'formal_consumption_allowed'='false'
          AND s.payload->'frozen'->'request'->>'data_snapshot_id'=s.id::text
          AND s.payload->'frozen'->'request'->>'instrument_id'=r.instrument_id::text
          AND (s.payload->'frozen'->'request'->>'decision_time')::timestamptz=r.decision_time
      ) $$"""

_VALID = """CREATE FUNCTION research_platform_manifest_valid(p jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(
 p->>'platform_contract'='platform-gifted-terra-v1-20261031'
 AND p->>'api_config_hash'='64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a'
 AND p->>'sdk_config_hash'='cdf5e7d3b0cfd2a3e6fda267f74bc9b3d135f28e33f965431f17175b9f2a8720'
 AND p->>'funding_proof_hash' ~ '^[0-9a-f]{64}$'
 AND p->>'credential_binding_hash' ~ '^[0-9a-f]{64}$'
 AND jsonb_array_length(p->'allocations')=3
 AND jsonb_array_length(p->'input_hashes')=3
 AND jsonb_array_length(p->'instrument_ids')=3
 AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'allocations') WITH ORDINALITY item(a,n)
   WHERE (a->>'input_hash'=p->'input_hashes'->>(n::int-1)
     AND a->'profile'='{"base_instructions_hash": "9c3ae749ff048f6828963ad24fb9eee3229af795fe98cb4610f74a55116cc5e5", "cache_mode": "implicit", "cache_ttl": "30m", "credential_kind": "platform_project_key", "developer_instructions_hash": "47a7cddaf28462c2d54edd7f5e1ff3dd28da50d42f120847bee6aaf58f120f60", "effort": "low", "endpoint": "https://api.openai.com/v1/responses", "model": "gpt-5.6-terra", "price_version": "platform-terra-default-implicit30m-20261031-v1", "service_tier": "default", "store": false, "stream": true}'::jsonb
     AND a->'stage_order'='["research","critique","synthesis"]'::jsonb
     AND a->>'output_limit'='8192' AND a->>'framing_units'='8192'
     AND a->>'units_version'='platform-full-http-utf8-plus8192-v1'
     AND jsonb_array_length(a->'stage_input_upper_bounds')=3
     AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(a->'stage_input_upper_bounds') v
       WHERE jsonb_typeof(v)<>'number' OR v::text !~ '^[0-9]+$'
         OR v::text::numeric NOT BETWEEN 8192 AND 272000)
     AND jsonb_array_length(a->'stage_schema_envelope_hashes')=3
     AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(a->'stage_schema_envelope_hashes') h
       WHERE h !~ '^[0-9a-f]{64}$')) IS DISTINCT FROM true),false) $$"""
_OLD_PURPOSE = "AND m.purpose='direct-responses-terra-v11-20261030'"
_NEW_PURPOSE = (
    "AND m.purpose IN ('direct-responses-terra-v11-20261030','platform-gifted-terra-v1-20261031')"
)


def _source_guard(old: str, new: str) -> None:
    op.execute(f"""DO $migration$ DECLARE definition text;
    BEGIN SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure) INTO definition;
    IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$)<>1
    THEN RAISE EXCEPTION 'PLATFORM_SOURCE_GUARD_UNEXPECTED'; END IF;
    EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")


def upgrade() -> None:
    op.execute("ALTER TABLE research_stage_authorization ADD COLUMN platform_binding jsonb")
    op.execute("ALTER TABLE research_stage_observation ADD COLUMN platform_response text")
    op.execute("""CREATE FUNCTION research_platform_stage_fact_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ DECLARE is_platform boolean; binding jsonb; BEGIN
        IF TG_TABLE_NAME='research_stage_authorization' THEN
          SELECT EXISTS(SELECT 1 FROM research_comparison_chain c
            JOIN research_comparison_manifest m ON m.id=c.experiment_id
            WHERE c.run_id=NEW.run_id AND m.purpose='platform-gifted-terra-v1-20261031') INTO is_platform;
          IF is_platform != (NEW.platform_binding IS NOT NULL) THEN
            RAISE EXCEPTION 'PLATFORM_STAGE_BINDING_REQUIRED'; END IF;
          IF is_platform AND (NEW.platform_binding->>'body_sha256' IS DISTINCT FROM NEW.wire_hash
            OR NEW.platform_binding->>'version' IS DISTINCT FROM '1'
            OR NEW.platform_binding->>'price_version' IS DISTINCT FROM 'platform-terra-default-implicit30m-20261031-v1')
          THEN RAISE EXCEPTION 'PLATFORM_STAGE_BINDING_INVALID'; END IF;
        ELSE
          SELECT platform_binding INTO binding FROM research_stage_authorization
            WHERE run_id=NEW.run_id AND stage=NEW.stage;
          IF (binding IS NOT NULL) != (NEW.platform_response IS NOT NULL) THEN
            RAISE EXCEPTION 'PLATFORM_RAW_RESPONSE_REQUIRED'; END IF;
          IF binding IS NOT NULL AND (octet_length(NEW.platform_response)>2097152
            OR encode(sha256(convert_to(NEW.platform_response,'UTF8')),'hex') != NEW.response_hash)
          THEN RAISE EXCEPTION 'PLATFORM_RAW_RESPONSE_INVALID'; END IF;
        END IF;
        RETURN NEW; END $$""")
    for table in ("authorization", "observation"):
        op.execute(
            f"CREATE TRIGGER research_platform_stage_fact_guard BEFORE INSERT ON research_stage_{table} "
            "FOR EACH ROW EXECUTE FUNCTION research_platform_stage_fact_guard()"
        )
    op.execute(_VALID)
    op.execute("""ALTER TABLE research_comparison_manifest
      DROP CONSTRAINT research_comparison_manifest_purpose_check,
      ADD CONSTRAINT research_comparison_manifest_purpose_check
        CHECK(purpose IN ('direct-responses-terra-v11-20261030','platform-gifted-terra-v1-20261031')),
      ADD CONSTRAINT research_platform_manifest_contract CHECK(
        purpose='direct-responses-terra-v11-20261030' OR research_platform_manifest_valid(payload))""")
    op.execute(_NEW_BINDING)
    _source_guard(_OLD_PURPOSE, _NEW_PURPOSE)
    op.execute("""CREATE FUNCTION research_platform_position_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF EXISTS(SELECT 1 FROM research_comparison_manifest m WHERE m.id=NEW.experiment_id
          AND m.purpose='platform-gifted-terra-v1-20261031') AND (
            NEW.ordinal NOT BETWEEN 1 AND 3 OR NOT research_comparison_binding(NEW.run_id,NEW.job_id,NEW.experiment_id))
        THEN RAISE EXCEPTION 'PLATFORM_POSITION_BINDING_INVALID'; END IF;
        RETURN NEW; END $$""")
    # AFTER insertion: the shared binding function must see this exact chain row.
    op.execute("""CREATE TRIGGER research_platform_position_guard AFTER INSERT ON research_comparison_chain
      FOR EACH ROW EXECUTE FUNCTION research_platform_position_guard()""")
    # Only the normal native claim may enable this exact position, after fresh funding checks.
    # The CLI execution gate remains closed until the entire executor is integrated.
    op.execute("""CREATE FUNCTION research_platform_dispatch_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.job_type='agent.research.comparison' AND NEW.attempt_count>OLD.attempt_count AND EXISTS(
          SELECT 1 FROM research_comparison_chain c JOIN research_comparison_manifest m ON m.id=c.experiment_id
          WHERE c.job_id=NEW.id AND m.purpose='platform-gifted-terra-v1-20261031'
          AND current_setting('nexus.platform_execution',true) IS DISTINCT FROM (c.experiment_id::text||':'||c.ordinal::text))
        THEN RAISE EXCEPTION 'PLATFORM_NATIVE_EXECUTION_NOT_READY'; END IF;
        RETURN NEW; END $$""")
    op.execute("""CREATE TRIGGER research_platform_dispatch_guard BEFORE UPDATE ON jobs
      FOR EACH ROW EXECUTE FUNCTION research_platform_dispatch_guard()""")


def downgrade() -> None:
    query = text(
        "SELECT EXISTS(SELECT 1 FROM research_comparison_manifest WHERE purpose='platform-gifted-terra-v1-20261031')"
    )
    if op.get_bind().scalar(query):
        raise RuntimeError("PLATFORM_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute(
        "LOCK TABLE research_comparison_manifest, research_comparison_chain, jobs IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    if op.get_bind().scalar(query):
        raise RuntimeError("PLATFORM_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    for table in ("authorization", "observation"):
        op.execute(f"DROP TRIGGER research_platform_stage_fact_guard ON research_stage_{table}")
    op.execute("DROP FUNCTION research_platform_stage_fact_guard()")
    op.execute("ALTER TABLE research_stage_observation DROP COLUMN platform_response")
    op.execute("ALTER TABLE research_stage_authorization DROP COLUMN platform_binding")
    op.execute("DROP TRIGGER research_platform_dispatch_guard ON jobs")
    op.execute("DROP FUNCTION research_platform_dispatch_guard()")
    op.execute("DROP TRIGGER research_platform_position_guard ON research_comparison_chain")
    op.execute("DROP FUNCTION research_platform_position_guard()")
    _source_guard(_NEW_PURPOSE, _OLD_PURPOSE)
    op.execute(_OLD_BINDING)
    op.execute("""ALTER TABLE research_comparison_manifest DROP CONSTRAINT research_platform_manifest_contract,
      DROP CONSTRAINT research_comparison_manifest_purpose_check,
      ADD CONSTRAINT research_comparison_manifest_purpose_check CHECK(purpose='direct-responses-terra-v11-20261030')""")
    op.execute("DROP FUNCTION research_platform_manifest_valid(jsonb)")
