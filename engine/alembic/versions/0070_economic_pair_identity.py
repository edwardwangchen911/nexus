"""Add immutable economic identity links; no accounts, slots or runs are activated.

Revision ID: 0070_economic_pair_identity
Revises: 0069_formal_budget_output_merge
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0070_economic_pair_identity"
down_revision = "0069_formal_budget_output_merge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    op.create_table(
        "economic_evaluations",
        sa.Column("id", uuid, primary_key=True),
        sa.Column("protocol_event_id", uuid, sa.ForeignKey("audit_events.id"), nullable=False),
        sa.Column("binding_event_id", uuid, sa.ForeignKey("audit_events.id"), nullable=False),
        sa.Column("protocol_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("binding_hash", sa.String(64), nullable=False),
        sa.Column("incumbent_account_id", uuid, nullable=False),
        sa.Column("incumbent_portfolio_id", uuid, nullable=False),
        sa.Column("incumbent_opening_id", uuid, nullable=False),
        sa.Column("challenger_account_id", uuid, nullable=False),
        sa.Column("challenger_portfolio_id", uuid, nullable=False),
        sa.Column("challenger_opening_id", uuid, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.clock_timestamp(),
            nullable=False,
        ),
        sa.UniqueConstraint("binding_event_id"),
        sa.ForeignKeyConstraint(
            ["incumbent_account_id", "incumbent_portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
        ),
        sa.ForeignKeyConstraint(
            ["challenger_account_id", "challenger_portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
        ),
        sa.ForeignKeyConstraint(
            ["incumbent_opening_id", "incumbent_account_id"],
            ["paper_ledger_transactions.id", "paper_ledger_transactions.account_id"],
        ),
        sa.ForeignKeyConstraint(
            ["challenger_opening_id", "challenger_account_id"],
            ["paper_ledger_transactions.id", "paper_ledger_transactions.account_id"],
        ),
        sa.CheckConstraint(
            "incumbent_account_id <> challenger_account_id AND incumbent_portfolio_id <> challenger_portfolio_id",
            name="lanes_distinct",
        ),
        sa.CheckConstraint(
            "protocol_hash ~ '^[0-9a-f]{64}$' AND binding_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )
    op.create_table(
        "economic_pair_slots",
        sa.Column("id", uuid, primary_key=True),
        sa.Column("evaluation_id", uuid, sa.ForeignKey("economic_evaluations.id"), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("session", sa.Date(), nullable=False),
        sa.Column("decision_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_close_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_locked", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.clock_timestamp(),
            nullable=False,
        ),
        sa.UniqueConstraint("evaluation_id", "ordinal", name="uq_economic_pair_slot_ordinal"),
        sa.UniqueConstraint("evaluation_id", "decision_at", name="uq_economic_pair_slot_time"),
        sa.CheckConstraint("ordinal BETWEEN 1 AND 24", name="ordinal_bounded"),
        sa.CheckConstraint(
            "cutoff_at = decision_at AND market_close_at > decision_at", name="times_ordered"
        ),
        sa.CheckConstraint("execution_locked", name="execution_locked"),
    )
    op.create_table(
        "economic_lane_cycles",
        sa.Column("id", uuid, primary_key=True),
        sa.Column("pair_slot_id", uuid, sa.ForeignKey("economic_pair_slots.id"), nullable=False),
        sa.Column("lane", sa.String(16), nullable=False),
        sa.Column("account_id", uuid, nullable=False),
        sa.Column("portfolio_id", uuid, nullable=False),
        sa.Column("cycle_id", uuid, sa.ForeignKey("agent_paper_cycles.id"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.clock_timestamp(),
            nullable=False,
        ),
        sa.UniqueConstraint("pair_slot_id", "lane"),
        sa.UniqueConstraint("cycle_id"),
        sa.ForeignKeyConstraint(
            ["account_id", "portfolio_id"], ["paper_accounts.id", "paper_accounts.portfolio_id"]
        ),
        sa.CheckConstraint("lane IN ('incumbent','challenger')", name="lane_allowed"),
    )
    # Exact economic document encoding; preserve strings, escapes and the two hash modes.
    op.execute("""
        CREATE FUNCTION nexus_economic_json_text(document jsonb, ascii_only boolean)
        RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
        DECLARE item record; fragments text[] := ARRAY[]::text[]; encoded text;
            rendered text := ''; ch text; cp integer; i integer;
        BEGIN
            CASE jsonb_typeof(document)
            WHEN 'object' THEN
                FOR item IN SELECT key,value FROM jsonb_each(document) ORDER BY key COLLATE "C" LOOP
                    encoded := nexus_economic_json_text(item.value,ascii_only);
                    IF encoded IS NULL THEN RETURN NULL; END IF;
                    fragments := array_append(fragments,
                        nexus_economic_json_text(to_jsonb(item.key),ascii_only) || ':' || encoded);
                END LOOP;
                RETURN '{' || array_to_string(fragments,',') || '}';
            WHEN 'array' THEN
                FOR item IN SELECT value FROM jsonb_array_elements(document) LOOP
                    encoded := nexus_economic_json_text(item.value,ascii_only);
                    IF encoded IS NULL THEN RETURN NULL; END IF;
                    fragments := array_append(fragments,encoded);
                END LOOP;
                RETURN '[' || array_to_string(fragments,',') || ']';
            WHEN 'string' THEN
                encoded := document::text;
                IF NOT ascii_only THEN RETURN encoded; END IF;
                FOR i IN 1..char_length(encoded) LOOP
                    ch := substr(encoded,i,1); cp := ascii(ch);
                    IF cp <= 126 THEN rendered := rendered || ch;
                    ELSIF cp <= 65535 THEN
                        rendered := rendered || chr(92) || 'u' || lpad(to_hex(cp),4,'0');
                    ELSE
                        rendered := rendered || chr(92) || 'u'
                            || lpad(to_hex(55296+(cp-65536)/1024),4,'0')
                            || chr(92) || 'u' || lpad(to_hex(56320+(cp-65536)%1024),4,'0');
                    END IF;
                END LOOP;
                RETURN rendered;
            WHEN 'boolean' THEN RETURN document::text;
            WHEN 'null' THEN RETURN 'null';
            WHEN 'number' THEN
                IF document::text !~ '^-?[0-9]+$'
                    OR abs((document::text)::numeric)>9007199254740991 THEN RETURN NULL; END IF;
                RETURN document::text;
            ELSE RETURN NULL;
            END CASE;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_economic_audit_valid(event_id uuid)
        RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT canonical_event=nexus_economic_json_text(jsonb_build_object(
                'action',action,'actor_id',actor_id,'event_seq',event_seq,
                'occurred_at',to_char(occurred_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'payload',payload,'schema_version',schema_version),false)
                AND event_hash=encode(sha256(convert_to(previous_hash || canonical_event,'UTF8')),'hex')
                AND chain_version='audit_jcs_sha256_v1'
            FROM audit_events WHERE id=event_id
        $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_economic_timestamp_text(value timestamptz)
        RETURNS text LANGUAGE sql IMMUTABLE AS $$
            SELECT to_char(value AT TIME ZONE 'UTC',CASE
                WHEN extract(microseconds FROM value)::bigint % 1000000 = 0
                THEN 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                ELSE 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"' END)
        $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_economic_decimal_text_valid(value text)
        RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
        DECLARE parts text[];
        BEGIN
            IF value ~ '^(0|[1-9][0-9]*)([.][0-9]+)?$' THEN
                RETURN value !~ '^0[.]0{6}[0-9]';
            END IF;
            parts := regexp_match(value,'^([1-9]([.][0-9]+)?|0)E([+-][1-9][0-9]*)$');
            IF parts IS NULL THEN RETURN false; END IF;
            IF parts[3]::integer < 0 THEN RETURN parts[3]::integer <= -7; END IF;
            RETURN parts[3]::integer > length(split_part(parts[1],'.',2));
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_validate_economic_identity() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE protocol jsonb; binding jsonb;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtextextended('economic-identity-registry-v1',2026));
            SELECT payload INTO protocol FROM audit_events WHERE id=NEW.protocol_event_id
                AND action='portfolio.economic.protocol_registered'
                AND actor_id='local-economic-protocol-cli' AND schema_version='portfolio_economic_protocol_v1';
            SELECT payload INTO binding FROM audit_events WHERE id=NEW.binding_event_id
                AND action='portfolio.economic.versions_bound'
                AND actor_id='local-economic-version-binding-cli' AND schema_version='portfolio_economic_binding_v1';
            IF protocol IS NULL OR binding IS NULL OR
                protocol->'schema_version' IS DISTINCT FROM '"portfolio_economic_preflight_v1"'::jsonb OR
                protocol->'status' IS DISTINCT FROM '"compiled_not_registered"'::jsonb OR
                protocol->'economic_status' IS DISTINCT FROM '"not_evaluated"'::jsonb OR
                protocol->'execution_locked' IS DISTINCT FROM 'true'::jsonb OR
                protocol->>'content_hash' IS DISTINCT FROM NEW.protocol_hash OR
                binding->>'content_hash' IS DISTINCT FROM NEW.binding_hash OR
                binding->>'protocol_hash' IS DISTINCT FROM NEW.protocol_hash OR
                jsonb_array_length(protocol->'slots') IS DISTINCT FROM 24 OR
                NOT coalesce(nexus_economic_audit_valid(NEW.protocol_event_id),false) OR
                NOT coalesce(nexus_economic_audit_valid(NEW.binding_event_id),false) OR
                encode(sha256(convert_to(nexus_economic_json_text(protocol - ARRAY[
                    'schema_version','status','economic_status','execution_locked','content_hash'],true),
                    'UTF8')),'hex') IS DISTINCT FROM NEW.protocol_hash OR
                encode(sha256(convert_to(nexus_economic_json_text(binding-'content_hash',true),
                    'UTF8')),'hex') IS DISTINCT FROM NEW.binding_hash OR
                binding->>'schema_version' IS DISTINCT FROM 'portfolio_economic_binding_v1' OR
                binding->>'status' IS DISTINCT FROM 'versions_bound_not_activated' OR
                binding->'activation_permitted' IS DISTINCT FROM 'false'::jsonb OR
                binding->>'economic_status' IS DISTINCT FROM 'not_evaluated' OR
                (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(binding) key)
                    IS DISTINCT FROM ARRAY['activation_permitted','challenger_prompt_hash',
                    'challenger_prompt_id','configured_cost_usd','content_hash','economic_status',
                    'incumbent_prompt_hash','incumbent_prompt_id','pricing_available_at','pricing_expires_at',
                    'proposal_hash','protocol_hash','publication_event_hash','schema_version',
                    'shared_model_hash','shared_model_id','shared_model_version','status']::text[] OR
                EXISTS (SELECT 1 FROM jsonb_each(binding) WHERE key<>'activation_permitted'
                    AND jsonb_typeof(value)<>'string') OR
                EXISTS (SELECT 1 FROM jsonb_each_text(binding) WHERE key IN ('protocol_hash',
                    'proposal_hash','publication_event_hash','shared_model_hash','incumbent_prompt_hash',
                    'challenger_prompt_hash','content_hash') AND value !~ '^[0-9a-f]{64}$') OR
                EXISTS (SELECT 1 FROM jsonb_each_text(binding) WHERE key IN ('shared_model_id',
                    'incumbent_prompt_id','challenger_prompt_id') AND
                    value IS DISTINCT FROM nexus_economic_cycle_uuid(value)::text) OR
                binding->>'incumbent_prompt_id' = binding->>'challenger_prompt_id' OR
                binding->>'incumbent_prompt_hash' = binding->>'challenger_prompt_hash' OR
                NOT coalesce(nexus_economic_decimal_text_valid(binding->>'configured_cost_usd'),false)
                THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_IDENTITY_INVALID';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM agent_model_configs m
                JOIN agent_prompt_versions i ON i.id=nexus_economic_cycle_uuid(binding->>'incumbent_prompt_id')
                JOIN agent_prompt_versions c ON c.id=nexus_economic_cycle_uuid(binding->>'challenger_prompt_id')
                JOIN audit_events b ON b.id=NEW.binding_event_id
                JOIN audit_events p ON p.event_hash=binding->>'publication_event_hash'
                JOIN audit_events proto ON proto.id=NEW.protocol_event_id
                WHERE m.id=nexus_economic_cycle_uuid(binding->>'shared_model_id')
                AND m.version=binding->>'shared_model_version'
                AND m.version='codex-sdk-chatgpt-v6-stage-time-budget'
                AND m.config_hash=binding->>'shared_model_hash'
                AND i.role='portfolio_decision' AND i.release_channel='active'
                AND c.role='portfolio_decision' AND c.release_channel='challenger'
                AND i.content_hash=binding->>'incumbent_prompt_hash'
                AND c.content_hash=binding->>'challenger_prompt_hash'
                AND p.action='portfolio.challenger.published'
                AND p.payload->>'proposal_hash'=binding->>'proposal_hash'
                AND p.payload->>'incumbent_prompt_version_id'=binding->>'incumbent_prompt_id'
                AND p.payload->>'challenger_prompt_version_id'=binding->>'challenger_prompt_id'
                AND p.payload->>'challenger_prompt_hash'=binding->>'challenger_prompt_hash'
                AND p.payload->>'challenger_model_config_id'=binding->>'shared_model_id'
                AND nexus_economic_audit_valid(p.id) AND p.occurred_at<=b.occurred_at
                AND p.created_at<=b.occurred_at AND proto.occurred_at<=b.occurred_at
                AND b.occurred_at<(protocol->'slots'->0->>'decision_at')::timestamptz
                AND b.occurred_at>=m.pricing_available_at AND b.occurred_at<m.pricing_expires_at
                AND binding->>'pricing_available_at'=nexus_economic_timestamp_text(m.pricing_available_at)
                AND binding->>'pricing_expires_at'=nexus_economic_timestamp_text(m.pricing_expires_at)
                AND (binding->>'configured_cost_usd')::numeric=(
                    m.max_input_tokens*m.input_usd_per_million + m.max_output_tokens*m.output_usd_per_million
                    )/1000000
                AND (protocol->'request'->>'research_cost_usd')::numeric=(binding->>'configured_cost_usd')::numeric
                AND (protocol->'request'->>'synthesis_cost_usd')::numeric=(binding->>'configured_cost_usd')::numeric
            ) THEN RAISE EXCEPTION 'ECONOMIC_PAIR_IDENTITY_INVALID'; END IF;
            IF EXISTS (SELECT 1 FROM economic_evaluations WHERE
                incumbent_account_id IN (NEW.incumbent_account_id,NEW.challenger_account_id) OR
                challenger_account_id IN (NEW.incumbent_account_id,NEW.challenger_account_id)) OR
                EXISTS (SELECT 1 FROM agent_paper_cycles WHERE account_id IN
                    (NEW.incumbent_account_id,NEW.challenger_account_id)) OR
                EXISTS (SELECT 1 FROM paper_orders WHERE account_id IN
                    (NEW.incumbent_account_id,NEW.challenger_account_id)) THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_ACCOUNT_REUSED';
            END IF;
            IF (SELECT count(*) FROM paper_accounts WHERE id IN
                (NEW.incumbent_account_id,NEW.challenger_account_id) AND initial_capital=10000
                AND capability='simulation_write_internal') <> 2 OR
                (SELECT count(*) FROM paper_ledger_transactions WHERE id IN
                (NEW.incumbent_opening_id,NEW.challenger_opening_id) AND transaction_type='funding'
                AND reference_type='initial_funding' AND order_id IS NULL) <> 2 OR
                (SELECT count(*) FROM paper_ledger_transactions t WHERE t.id IN
                    (NEW.incumbent_opening_id,NEW.challenger_opening_id)
                    AND (SELECT count(*) FROM paper_ledger_entries x
                         WHERE x.transaction_id=t.id AND x.account_id=t.account_id) = 2
                    AND EXISTS (SELECT 1 FROM paper_ledger_entries x
                         JOIN paper_ledger_accounts a ON a.id=x.ledger_account_id
                         WHERE x.transaction_id=t.id AND x.account_id=t.account_id
                         AND a.paper_account_id=t.account_id AND a.normal_side='debit'
                         AND a.code='asset.cash.available.usd' AND a.currency='USD'
                         AND x.account_code=a.code AND x.currency='USD'
                         AND x.debit_amount=10000 AND x.credit_amount=0)
                    AND EXISTS (SELECT 1 FROM paper_ledger_entries x
                         JOIN paper_ledger_accounts a ON a.id=x.ledger_account_id
                         WHERE x.transaction_id=t.id AND x.account_id=t.account_id
                         AND a.paper_account_id=t.account_id AND a.normal_side='credit'
                         AND a.code='equity.contributed_capital.usd' AND a.currency='USD'
                         AND x.account_code=a.code AND x.currency='USD'
                         AND x.credit_amount=10000 AND x.debit_amount=0)) <> 2 THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_OPENING_INVALID';
            END IF;
            RETURN NEW;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_validate_economic_slot() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE planned jsonb;
        BEGIN
            SELECT a.payload->'slots'->(NEW.ordinal-1) INTO planned FROM economic_evaluations e
                JOIN audit_events a ON a.id=e.protocol_event_id WHERE e.id=NEW.evaluation_id;
            IF planned IS NULL OR (planned->>'ordinal')::integer IS DISTINCT FROM NEW.ordinal OR
                (planned->>'session')::date IS DISTINCT FROM NEW.session OR
                (planned->>'decision_at')::timestamptz IS DISTINCT FROM NEW.decision_at OR
                NEW.cutoff_at IS DISTINCT FROM NEW.decision_at OR
                (planned->>'market_close_at')::timestamptz IS DISTINCT FROM NEW.market_close_at THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_SLOT_INVALID';
            END IF;
            RETURN NEW;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_economic_cycle_uuid(reference text)
        RETURNS uuid LANGUAGE plpgsql IMMUTABLE AS $$
        BEGIN
            RETURN reference::uuid;
        EXCEPTION WHEN invalid_text_representation THEN RETURN NULL;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_economic_cycle_manifest_valid(cycle_id uuid, protocol jsonb)
        RETURNS boolean LANGUAGE plpgsql STABLE AS $$
        DECLARE c agent_paper_cycles%ROWTYPE; b agent_paper_candidate_batches%ROWTYPE;
            entry jsonb; canonical_manifest jsonb; wanted jsonb; actual jsonb; wanted_ids uuid[];
        BEGIN
            SELECT * INTO c FROM agent_paper_cycles WHERE id=cycle_id;
            SELECT * INTO b FROM agent_paper_candidate_batches WHERE id=c.candidate_batch_id;
            IF c.id IS NULL OR b.id IS NULL OR jsonb_typeof(c.candidate_manifest)<>'array'
                OR jsonb_array_length(c.candidate_manifest)<>2 THEN RETURN false; END IF;
            FOR entry IN SELECT value FROM jsonb_array_elements(c.candidate_manifest) LOOP
                IF jsonb_typeof(entry)<>'object' THEN RETURN false; END IF;
                IF (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(entry) key)
                    IS DISTINCT FROM ARRAY['decision_artifact_id','decision_evidence_hash',
                    'decision_quote_id','decision_trade_id','inclusion_reason','instrument_id',
                    'prior_daily_price_authority_id','research_snapshot_id','risk_query_authority_ids','symbol']::text[]
                    THEN RETURN false; END IF;
                IF EXISTS (SELECT 1 FROM jsonb_each(entry) WHERE key NOT IN
                    ('decision_trade_id','risk_query_authority_ids') AND jsonb_typeof(value)<>'string')
                    OR entry->>'decision_evidence_hash' !~ '^[0-9a-f]{64}$'
                    OR EXISTS (SELECT 1 FROM jsonb_each_text(entry) WHERE key IN
                    ('instrument_id','research_snapshot_id','prior_daily_price_authority_id',
                    'decision_quote_id','decision_artifact_id') AND
                    value IS DISTINCT FROM nexus_economic_cycle_uuid(value)::text)
                    OR (entry->'decision_trade_id'<>'null'::jsonb AND
                        (jsonb_typeof(entry->'decision_trade_id')<>'string' OR
                        entry->>'decision_trade_id' IS DISTINCT FROM
                        nexus_economic_cycle_uuid(entry->>'decision_trade_id')::text))
                    THEN RETURN false; END IF;
                IF jsonb_typeof(entry->'risk_query_authority_ids')<>'array' THEN RETURN false; END IF;
                IF jsonb_array_length(entry->'risk_query_authority_ids')<>3 OR
                    EXISTS (SELECT 1 FROM jsonb_array_elements(entry->'risk_query_authority_ids') value
                        WHERE jsonb_typeof(value)<>'string' OR value #>> '{}'
                        IS DISTINCT FROM nexus_economic_cycle_uuid(value #>> '{}')::text)
                    THEN RETURN false; END IF;
            END LOOP;
            SELECT jsonb_agg(value ORDER BY nexus_economic_cycle_uuid(value->>'instrument_id')),
                jsonb_agg(jsonb_build_object('instrument_id',value->>'instrument_id','symbol',value->>'symbol')
                    ORDER BY nexus_economic_cycle_uuid(value->>'instrument_id'))
                INTO canonical_manifest,actual FROM jsonb_array_elements(c.candidate_manifest);
            SELECT jsonb_agg(jsonb_build_object('instrument_id',value->>'instrument_id','symbol',value->>'symbol')
                    ORDER BY nexus_economic_cycle_uuid(value->>'instrument_id')),
                array_agg(nexus_economic_cycle_uuid(value->>'instrument_id')
                    ORDER BY nexus_economic_cycle_uuid(value->>'instrument_id'))
                INTO wanted,wanted_ids FROM jsonb_array_elements(protocol->'request'->'instruments');
            RETURN actual=wanted AND b.candidate_instrument_ids=wanted_ids
                AND b.batch_hash=encode(sha256(convert_to(nexus_economic_json_text(jsonb_build_object(
                    'account_id',b.account_id::text,'universe_snapshot_id',b.universe_snapshot_id::text,
                    'schema','agent_paper_candidate_batch_v1','source','explicit_research_scope',
                    'candidate_instrument_ids',to_jsonb(b.candidate_instrument_ids)),true),'UTF8')),'hex')
                AND c.candidate_hash=encode(sha256(convert_to(
                    nexus_economic_json_text(canonical_manifest,true),'UTF8')),'hex');
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_validate_economic_lane_cycle() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            -- Native producer writes use this same row lock. No research may commit first.
            PERFORM 1 FROM agent_paper_cycles WHERE id=NEW.cycle_id FOR UPDATE;
            IF NOT EXISTS (SELECT 1 FROM economic_pair_slots s
                JOIN economic_evaluations e ON e.id=s.evaluation_id
                JOIN audit_events protocol ON protocol.id=e.protocol_event_id
                JOIN agent_paper_cycles c ON c.id=NEW.cycle_id
                WHERE s.id=NEW.pair_slot_id AND c.account_id=NEW.account_id
                  AND c.portfolio_id=NEW.portfolio_id AND c.decision_time=s.decision_at
                  AND c.decision_slot_at=s.decision_at AND c.decision_session=s.session
                  AND c.execution_session=s.session AND c.execution_locked
                  AND c.candidate_batch_id IS NOT NULL AND c.formal_budget_binding_id IS NULL
                  AND nexus_economic_cycle_manifest_valid(c.id,protocol.payload)
                  AND NEW.account_id=CASE NEW.lane WHEN 'incumbent' THEN e.incumbent_account_id
                        WHEN 'challenger' THEN e.challenger_account_id END
                  AND NEW.portfolio_id=CASE NEW.lane WHEN 'incumbent' THEN e.incumbent_portfolio_id
                        WHEN 'challenger' THEN e.challenger_portfolio_id END) THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_CYCLE_INVALID';
            END IF;
            IF EXISTS (SELECT 1 FROM jobs WHERE nexus_economic_cycle_uuid(payload->>'cycle_id')=NEW.cycle_id)
               OR EXISTS (SELECT 1 FROM agent_research_runs r JOIN data_snapshots d
                    ON d.id=r.data_snapshot_id
                    WHERE nexus_economic_cycle_uuid(d.payload->'agent_paper_context'->>'cycle_id')=NEW.cycle_id)
               OR EXISTS (SELECT 1 FROM agent_portfolio_synthesis_runs WHERE cycle_id=NEW.cycle_id)
               OR EXISTS (SELECT 1 FROM strategy_decisions WHERE agent_paper_cycle_id=NEW.cycle_id) THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_CYCLE_LINK_TOO_LATE';
            END IF;
            RETURN NEW;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_require_economic_cycle_bound(cycle_ref text)
        RETURNS void LANGUAGE plpgsql AS $$
        DECLARE c agent_paper_cycles%ROWTYPE;
        BEGIN
            IF cycle_ref IS NOT NULL AND nexus_economic_cycle_uuid(cycle_ref) IS NULL THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_CYCLE_REFERENCE_INVALID';
            END IF;
            SELECT * INTO c FROM agent_paper_cycles
                WHERE id=nexus_economic_cycle_uuid(cycle_ref) FOR UPDATE;
            IF c.id IS NOT NULL AND EXISTS (SELECT 1 FROM economic_evaluations e
                WHERE c.account_id IN (e.incumbent_account_id,e.challenger_account_id))
                AND NOT EXISTS (SELECT 1 FROM economic_lane_cycles l
                    WHERE l.cycle_id=c.id AND l.account_id=c.account_id
                    AND l.portfolio_id=c.portfolio_id) THEN
                RAISE EXCEPTION 'ECONOMIC_PAIR_CYCLE_NOT_BOUND';
            END IF;
        END $$;
    """)
    op.execute("""
        CREATE FUNCTION nexus_guard_economic_native_producer()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE cycle_ref text;
        BEGIN
            IF TG_TABLE_NAME='agent_research_runs' THEN
                SELECT payload->'agent_paper_context'->>'cycle_id' INTO cycle_ref
                    FROM data_snapshots WHERE id=NEW.data_snapshot_id;
                PERFORM nexus_require_economic_cycle_bound(cycle_ref);
            ELSIF TG_TABLE_NAME='agent_portfolio_synthesis_runs' THEN
                PERFORM nexus_require_economic_cycle_bound(NEW.cycle_id::text);
            ELSIF TG_TABLE_NAME='strategy_decisions' THEN
                PERFORM nexus_require_economic_cycle_bound(NEW.agent_paper_cycle_id::text);
            ELSIF TG_TABLE_NAME='jobs' THEN
                -- Actual native Jobs carry run_id, not the admission request's cycle_id.
                -- Check every reference, so a supplied ordinary cycle cannot mask an economic Run.
                IF (NEW.job_type='agent.paper.research' AND NOT EXISTS
                    (SELECT 1 FROM agent_research_runs
                        WHERE id=nexus_economic_cycle_uuid(NEW.payload->>'run_id')))
                    OR (NEW.job_type='agent.portfolio_synthesis' AND NOT EXISTS
                    (SELECT 1 FROM agent_portfolio_synthesis_runs
                        WHERE id=nexus_economic_cycle_uuid(NEW.payload->>'run_id'))) THEN
                    RAISE EXCEPTION 'ECONOMIC_PAIR_JOB_RUN_UNAVAILABLE';
                END IF;
                FOR cycle_ref IN
                    SELECT c.id::text FROM agent_paper_cycles c
                    WHERE c.id=nexus_economic_cycle_uuid(NEW.payload->>'cycle_id')
                    OR c.id IN (SELECT r.cycle_id FROM agent_portfolio_synthesis_runs r
                        WHERE r.id=nexus_economic_cycle_uuid(NEW.payload->>'run_id'))
                    OR c.id IN (SELECT nexus_economic_cycle_uuid(
                        d.payload->'agent_paper_context'->>'cycle_id')
                        FROM agent_research_runs r JOIN data_snapshots d ON d.id=r.data_snapshot_id
                        WHERE r.id=nexus_economic_cycle_uuid(NEW.payload->>'run_id'))
                    ORDER BY c.id
                LOOP
                    PERFORM nexus_require_economic_cycle_bound(cycle_ref);
                END LOOP;
            END IF;
            RETURN NEW;
        END $$;
    """)
    for table, columns in (
        ("jobs", "payload"),
        ("agent_research_runs", "data_snapshot_id"),
        ("agent_portfolio_synthesis_runs", "cycle_id"),
        ("strategy_decisions", "agent_paper_cycle_id"),
    ):
        op.execute(f"""CREATE TRIGGER economic_native_cycle_bound
            BEFORE INSERT OR UPDATE OF {columns} ON {table}
            FOR EACH ROW EXECUTE FUNCTION nexus_guard_economic_native_producer()""")
    op.execute(
        "CREATE TRIGGER economic_identity_valid BEFORE INSERT ON economic_evaluations FOR EACH ROW EXECUTE FUNCTION nexus_validate_economic_identity()"
    )
    op.execute(
        "CREATE TRIGGER economic_slot_valid BEFORE INSERT ON economic_pair_slots FOR EACH ROW EXECUTE FUNCTION nexus_validate_economic_slot()"
    )
    op.execute(
        "CREATE TRIGGER economic_lane_cycle_valid BEFORE INSERT ON economic_lane_cycles FOR EACH ROW EXECUTE FUNCTION nexus_validate_economic_lane_cycle()"
    )
    op.execute("""
        CREATE FUNCTION nexus_reject_economic_pair_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'ECONOMIC_PAIR_IMMUTABLE'; END $$;
    """)
    for table in ("economic_evaluations", "economic_pair_slots", "economic_lane_cycles"):
        op.execute(f"""CREATE TRIGGER economic_pair_immutable BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION nexus_reject_economic_pair_mutation()""")
        op.execute(f"""CREATE TRIGGER economic_pair_no_truncate BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION nexus_reject_economic_pair_mutation()""")


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM economic_evaluations) OR
           EXISTS (SELECT 1 FROM economic_pair_slots) OR
           EXISTS (SELECT 1 FROM economic_lane_cycles) THEN
            RAISE EXCEPTION 'ECONOMIC_PAIR_ROLLBACK_REQUIRES_EMPTY';
        END IF;
    END $$;""")
    for table in (
        "jobs",
        "agent_research_runs",
        "agent_portfolio_synthesis_runs",
        "strategy_decisions",
    ):
        op.execute(f"DROP TRIGGER economic_native_cycle_bound ON {table}")
    op.execute("DROP FUNCTION nexus_guard_economic_native_producer()")
    op.execute("DROP FUNCTION nexus_require_economic_cycle_bound(text)")
    for table in ("economic_lane_cycles", "economic_pair_slots", "economic_evaluations"):
        op.drop_table(table)
    for function in (
        "nexus_reject_economic_pair_mutation",
        "nexus_validate_economic_lane_cycle",
        "nexus_validate_economic_slot",
        "nexus_validate_economic_identity",
    ):
        signature = "text" if function == "nexus_economic_cycle_uuid" else ""
        op.execute(f"DROP FUNCTION {function}({signature})")
    op.execute("DROP FUNCTION nexus_economic_cycle_manifest_valid(uuid,jsonb)")
    op.execute("DROP FUNCTION nexus_economic_cycle_uuid(text)")
    op.execute("DROP FUNCTION nexus_economic_audit_valid(uuid)")
    op.execute("DROP FUNCTION nexus_economic_timestamp_text(timestamptz)")
    op.execute("DROP FUNCTION nexus_economic_decimal_text_valid(text)")
    op.execute("DROP FUNCTION nexus_economic_json_text(jsonb,boolean)")
