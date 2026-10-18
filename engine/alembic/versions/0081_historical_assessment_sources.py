"""Record immutable, isolated three-stock historical assessment sources.

0080 supplies the deferred closeout. This revision deliberately does not change
any formal model/output allowlist. Sources must be registered before projected
Run/Output creation; their deterministic IDs are reserved by the importer.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0081_historical_sources"
down_revision = "0080_partial_assessment_closeout"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "historical_assessment_sources",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cycle_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_cycle_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("archive_hash", sa.CHAR(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("content_hash", sa.CHAR(64), nullable=False),
        sa.Column("evidence_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cycle_id"),
        sa.UniqueConstraint("archive_hash"),
        sa.ForeignKeyConstraint(["cycle_id"], ["agent_paper_cycles.id"]),
        sa.ForeignKeyConstraint(["source_cycle_id"], ["agent_paper_cycles.id"]),
        sa.CheckConstraint("cycle_id <> source_cycle_id", name="cycles_distinct"),
        sa.CheckConstraint(
            "archive_hash ~ '^[0-9a-f]{64}$' AND content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        sa.CheckConstraint(
            "evidence_at <= observed_started_at AND observed_started_at <= observed_finished_at "
            "AND observed_finished_at <= imported_at",
            name="times_ordered",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' "
            "AND jsonb_typeof(payload->'archive') IS NOT DISTINCT FROM 'object' "
            "AND jsonb_typeof(payload->'projection') IS NOT DISTINCT FROM 'object' "
            "AND jsonb_typeof(payload->'projection'->'research') IS NOT DISTINCT FROM 'array'",
            name="payload_shape",
        ),
    )
    op.execute("""
    CREATE FUNCTION nexus_historical_assessment_source_valid() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE account_ref uuid; member_row jsonb; research jsonb; seen_runs uuid[] := '{}';
            seen_outputs uuid[] := '{}'; run_ref uuid; output_ref uuid;
    BEGIN
      IF current_database() !~ '^nexus_(test|recovery)_' THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_ISOLATION_REQUIRED' USING ERRCODE='23514';
      END IF;
      SELECT account_id INTO account_ref FROM public.agent_paper_cycles WHERE id=NEW.cycle_id;
      IF account_ref IS NULL THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_CYCLE_REQUIRED' USING ERRCODE='23514';
      END IF;
      PERFORM pg_advisory_xact_lock(
        hashtextextended('nexus:agent-paper-cycle:' || account_ref::text, 0));
      research := NEW.payload->'projection'->'research';
      IF jsonb_typeof(research) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RESEARCH_INVALID' USING ERRCODE='23514';
      END IF;
      IF jsonb_array_length(research) <> 3 THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_FULL3_REQUIRED' USING ERRCODE='23514';
      END IF;
      FOR member_row IN SELECT value FROM jsonb_array_elements(research) LOOP
        IF jsonb_typeof(member_row) IS DISTINCT FROM 'object'
           OR jsonb_typeof(member_row->'run_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(member_row->'output_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(member_row->'source_run_id') IS DISTINCT FROM 'string'
           OR jsonb_typeof(member_row->'source_output_id') IS DISTINCT FROM 'string' THEN
          RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RESEARCH_INVALID' USING ERRCODE='23514';
        END IF;
        run_ref := (member_row->>'run_id')::uuid;
        output_ref := (member_row->>'output_id')::uuid;
        IF run_ref = ANY(seen_runs) OR output_ref = ANY(seen_outputs)
           OR run_ref = (member_row->>'source_run_id')::uuid
           OR output_ref = (member_row->>'source_output_id')::uuid THEN
          RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RESEARCH_IDENTITY_INVALID'
            USING ERRCODE='23514';
        END IF;
        seen_runs := array_append(seen_runs,run_ref);
        seen_outputs := array_append(seen_outputs,output_ref);
      END LOOP;
      -- Registration and binding share one output identity lock even when the
      -- source row is not yet visible. Keep acquisition deterministic for full3.
      FOR output_ref IN SELECT value FROM unnest(seen_outputs) value ORDER BY value LOOP
        PERFORM pg_advisory_xact_lock(
          hashtextextended('nexus:historical-assessment-output:' || output_ref::text, 0));
      END LOOP;
      -- Distinct observations must not reserve one Run with different Output IDs.
      FOR run_ref IN SELECT value FROM unnest(seen_runs) value ORDER BY value LOOP
        PERFORM pg_advisory_xact_lock(
          hashtextextended('nexus:historical-assessment-run:' || run_ref::text, 0));
      END LOOP;
      IF EXISTS (SELECT 1 FROM public.agent_research_runs WHERE id = ANY(seen_runs))
         OR EXISTS (SELECT 1 FROM public.agent_research_outputs WHERE id = ANY(seen_outputs))
         OR EXISTS (
           SELECT 1 FROM public.historical_assessment_sources source
           CROSS JOIN LATERAL jsonb_array_elements(source.payload->'projection'->'research') item
           WHERE (item->>'run_id')::uuid = ANY(seen_runs)
              OR (item->>'output_id')::uuid = ANY(seen_outputs)
         ) THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RESEARCH_IDENTITY_OCCUPIED'
          USING ERRCODE='23514';
      END IF;
      IF EXISTS (SELECT 1 FROM public.strategy_decisions
                 WHERE agent_paper_cycle_id=NEW.cycle_id)
         OR EXISTS (SELECT 1 FROM public.strategy_decision_agent_outputs
                    WHERE agent_research_output_id = ANY(seen_outputs)) THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_EXECUTION_FORBIDDEN' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER historical_assessment_source_valid BEFORE INSERT
    ON historical_assessment_sources FOR EACH ROW
    EXECUTE FUNCTION nexus_historical_assessment_source_valid()
    """)
    op.execute("""
    CREATE TRIGGER historical_assessment_source_immutable BEFORE UPDATE OR DELETE
    ON historical_assessment_sources FOR EACH ROW
    EXECUTE FUNCTION nexus_prevent_agent_immutable_change()
    """)
    op.execute("""
    CREATE FUNCTION nexus_historical_assessment_execution_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE account_ref uuid;
    BEGIN
      SELECT account_id INTO account_ref FROM public.agent_paper_cycles
      WHERE id=NEW.agent_paper_cycle_id;
      IF account_ref IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(
          hashtextextended('nexus:agent-paper-cycle:' || account_ref::text, 0));
      END IF;
      IF EXISTS (SELECT 1 FROM public.historical_assessment_sources
                 WHERE cycle_id=NEW.agent_paper_cycle_id) THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_EXECUTION_FORBIDDEN' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER aa_historical_assessment_execution_guard BEFORE INSERT ON strategy_decisions
    FOR EACH ROW EXECUTE FUNCTION nexus_historical_assessment_execution_guard()
    """)
    op.execute("""
    CREATE FUNCTION nexus_historical_assessment_output_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE run_ref uuid;
    BEGIN
      -- Do not acquire the source account after this lock: the decision already
      -- owns its account lock, while source registration takes account -> identity.
      PERFORM pg_advisory_xact_lock(
        hashtextextended(
          'nexus:historical-assessment-output:' || NEW.agent_research_output_id::text, 0));
      SELECT run_id INTO run_ref FROM public.agent_research_outputs
        WHERE id=NEW.agent_research_output_id;
      IF run_ref IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(
          hashtextextended('nexus:historical-assessment-run:' || run_ref::text, 0));
      END IF;
      IF EXISTS (
        SELECT 1 FROM public.historical_assessment_sources source
        CROSS JOIN LATERAL jsonb_array_elements(source.payload->'projection'->'research') item
        WHERE (item->>'output_id')::uuid=NEW.agent_research_output_id
           OR (item->>'run_id')::uuid=run_ref
      ) THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_OUTPUT_EXECUTION_FORBIDDEN' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER aa_historical_assessment_output_guard BEFORE INSERT
    ON strategy_decision_agent_outputs FOR EACH ROW
    EXECUTE FUNCTION nexus_historical_assessment_output_guard()
    """)

    op.execute("""
    CREATE FUNCTION nexus_historical_assessment_run_insert_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE reserved record; member_row jsonb; model_version text; model_digest text;
    BEGIN
      -- Also serialize normal inserts: a reserved ID cannot be claimed using
      -- an ordinary model while registration is waiting in another transaction.
      PERFORM pg_advisory_xact_lock(
        hashtextextended('nexus:historical-assessment-run:' || NEW.id::text, 0));
      SELECT version,config_hash INTO model_version,model_digest
        FROM public.agent_model_configs WHERE id=NEW.model_config_id;
      SELECT item.value AS projection, source.payload AS source_payload,
             source.evidence_at,source.imported_at INTO reserved
      FROM public.historical_assessment_sources source
      CROSS JOIN LATERAL jsonb_array_elements(source.payload->'projection'->'research') item
      WHERE (item.value->>'run_id')::uuid=NEW.id;
      IF NOT FOUND THEN
        IF NEW.actor_id='historical-archive-consumer-v1'
           OR NEW.pricing_version='historical-archive-consumer-v1'
           OR model_version='historical-archive-consumer-v1' THEN
          RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RUN_NOT_RESERVED' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END IF;
      SELECT member.value INTO member_row
      FROM jsonb_array_elements(reserved.source_payload->'archive'->'case'->'members') member
      WHERE member.value->>'source_run_id'=reserved.projection->>'source_run_id';
      IF NOT FOUND
         OR current_database() !~ '^nexus_(test|recovery)_'
         OR NEW.actor_id IS DISTINCT FROM 'historical-archive-consumer-v1'
         OR NEW.pricing_version IS DISTINCT FROM 'historical-archive-consumer-v1'
         OR model_version IS DISTINCT FROM 'historical-archive-consumer-v1'
         OR model_digest IS DISTINCT FROM
            '41de5bc89e16356388c49448a046e1b9f165e5d23c1af896318e14256f7e2338'
         OR NEW.model_hash IS DISTINCT FROM model_digest
         OR NEW.model_config_id IS DISTINCT FROM
            (reserved.source_payload->'projection'->'synthesis'->'frozen_input'->>'model_config_id')::uuid
         OR NEW.instrument_id IS DISTINCT FROM (reserved.projection->>'instrument_id')::uuid
         OR NEW.data_snapshot_id IS DISTINCT FROM
            (member_row->'request'->>'data_snapshot_id')::uuid
         OR NEW.decision_time IS DISTINCT FROM reserved.evidence_at
         OR NEW.created_at < reserved.imported_at
         OR NEW.status IS DISTINCT FROM 'prepared' OR NEW.retry_count IS DISTINCT FROM 0
         OR NEW.reserved_cost_usd IS DISTINCT FROM 0 OR NEW.settled_cost_usd IS NOT NULL
         OR NEW.input_tokens IS NOT NULL OR NEW.output_tokens IS NOT NULL THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_RUN_BINDING_INVALID' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER aa_historical_assessment_run_insert_guard BEFORE INSERT ON agent_research_runs
    FOR EACH ROW EXECUTE FUNCTION nexus_historical_assessment_run_insert_guard()
    """)
    op.execute("""
    CREATE FUNCTION nexus_historical_assessment_output_insert_guard() RETURNS trigger
    LANGUAGE plpgsql SET search_path TO pg_catalog, public, pg_temp AS $$
    DECLARE reserved record; run_profile text; pricing_profile text;
    BEGIN
      PERFORM pg_advisory_xact_lock(
        hashtextextended('nexus:historical-assessment-output:' || NEW.id::text, 0));
      PERFORM pg_advisory_xact_lock(
        hashtextextended('nexus:historical-assessment-run:' || NEW.run_id::text, 0));
      SELECT item.value AS projection,source.imported_at INTO reserved
      FROM public.historical_assessment_sources source
      CROSS JOIN LATERAL jsonb_array_elements(source.payload->'projection'->'research') item
      WHERE (item.value->>'run_id')::uuid=NEW.run_id
         OR (item.value->>'output_id')::uuid=NEW.id;
      IF NOT FOUND THEN
        SELECT actor_id,pricing_version INTO run_profile,pricing_profile
          FROM public.agent_research_runs WHERE id=NEW.run_id;
        IF run_profile='historical-archive-consumer-v1'
           OR pricing_profile='historical-archive-consumer-v1' THEN
          RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_OUTPUT_NOT_RESERVED' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END IF;
      IF current_database() !~ '^nexus_(test|recovery)_'
         OR NEW.id IS DISTINCT FROM (reserved.projection->>'output_id')::uuid
         OR NEW.run_id IS DISTINCT FROM (reserved.projection->>'run_id')::uuid
         OR NEW.sequence IS DISTINCT FROM 1 OR NEW.status IS DISTINCT FROM 'completed'
         OR NEW.role IS NOT NULL OR NEW.prompt_hash IS NOT NULL
         OR NEW.payload IS DISTINCT FROM reserved.projection->'payload'
         OR NEW.intent IS DISTINCT FROM reserved.projection->'intent'
         OR NEW.content_hash IS DISTINCT FROM reserved.projection->>'content_hash'
         OR NEW.created_at < reserved.imported_at THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_OUTPUT_BINDING_INVALID' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$
    """)
    op.execute("""
    CREATE TRIGGER aa_historical_assessment_output_insert_guard
    BEFORE INSERT ON agent_research_outputs FOR EACH ROW
    EXECUTE FUNCTION nexus_historical_assessment_output_insert_guard()
    """)


def downgrade() -> None:
    op.execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM public.historical_assessment_sources) THEN
        RAISE EXCEPTION 'HISTORICAL_ASSESSMENT_FACTS_EXIST';
      END IF;
    END $$
    """)
    op.execute(
        "DROP TRIGGER aa_historical_assessment_output_guard ON strategy_decision_agent_outputs"
    )
    op.execute("DROP FUNCTION nexus_historical_assessment_output_guard()")
    op.execute(
        "DROP TRIGGER aa_historical_assessment_output_insert_guard ON agent_research_outputs"
    )
    op.execute("DROP FUNCTION nexus_historical_assessment_output_insert_guard()")
    op.execute("DROP TRIGGER aa_historical_assessment_run_insert_guard ON agent_research_runs")
    op.execute("DROP FUNCTION nexus_historical_assessment_run_insert_guard()")
    op.execute("DROP TRIGGER aa_historical_assessment_execution_guard ON strategy_decisions")
    op.execute("DROP FUNCTION nexus_historical_assessment_execution_guard()")
    op.execute("DROP TABLE historical_assessment_sources")
    op.execute("DROP FUNCTION nexus_historical_assessment_source_valid()")
