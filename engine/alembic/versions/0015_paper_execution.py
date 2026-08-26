"""Add immutable evidence for deterministic internal paper execution.

Revision ID: 0015_paper_execution
Revises: 0014_paper
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015_paper_execution"
down_revision: str | Sequence[str] | None = "0014_paper"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EXECUTION_TABLES = (
    "paper_execution_models",
    "paper_execution_window_authorities",
    "paper_execution_window_members",
    "paper_request_idempotency",
    "paper_performance_attributions",
)


def _replace_price_bar_preparer(*, allow_intraday: bool) -> None:
    interval_clause = (
        "manifest.interval IN ('1Day','1Min')"
        if allow_intraday
        else "manifest.interval = '1Day'"
    )
    observation_clause = (
        "AND ((manifest.interval = '1Day' AND "
        "nexus_daily_price_revisions_valid(manifest.id, actual_count, "
        "distinct_session_count)) OR (manifest.interval = '1Min' "
        "AND actual_count = distinct_observation_count))"
        if allow_intraday
        else (
            "AND nexus_daily_price_revisions_valid(manifest.id, actual_count, "
            "distinct_session_count)"
        )
    )
    intraday_time_clause = (
        "OR (manifest.interval = '1Min' AND "
        "(bar.observed_at AT TIME ZONE 'America/New_York')::date <> bar.session)"
        if allow_intraday
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_prepare_price_bar_set(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text,
          byte_length bigint,
          bar_count integer,
          first_session date,
          last_session date,
          available_at timestamptz
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
          manifest price_series_manifests%ROWTYPE;
          actual_count bigint;
          first_observed timestamptz;
          last_observed timestamptz;
          distinct_session_count bigint;
          distinct_observation_count bigint;
          prepared_first_session date;
          prepared_last_session date;
          max_received timestamptz;
          canonical_text text;
        BEGIN
          PERFORM pg_advisory_xact_lock(
            hashtextextended('price-series-manifest:' || p_manifest_id::text, 0)
          );
          SELECT * INTO manifest
          FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND THEN
            RETURN;
          END IF;
          SELECT count(*), min(bar.observed_at), max(bar.observed_at),
                 count(DISTINCT bar.session), count(DISTINCT bar.observed_at),
                 min(bar.session), max(bar.session), max(bar.received_at)
            INTO actual_count, first_observed, last_observed,
                 distinct_session_count, distinct_observation_count,
                 prepared_first_session, prepared_last_session, max_received
          FROM price_bar_records AS bar
          WHERE bar.price_series_manifest_id = manifest.id;
          IF NOT (
            {interval_clause}
            AND (
              (manifest.source = 'alpaca' AND manifest.feed = 'sip'
                AND manifest.delivery_mode = 'delayed')
              OR (manifest.source = 'fixture' AND manifest.feed <> ''
                AND manifest.delivery_mode = 'fixture')
            )
            AND actual_count BETWEEN 1 AND 10000
            AND actual_count = manifest.row_count
            AND first_observed = manifest.start_at
            AND last_observed = manifest.end_at
            {observation_clause}
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
                AND (
                  bar.instrument_id::text <> manifest.instrument
                  OR bar.observed_at > bar.available_at
                  OR bar.available_at > bar.received_at
                  OR jsonb_typeof(bar.quality_flags) <> 'array'
                  OR bar.quality_flags <> '[]'::jsonb
                  {intraday_time_clause}
                )
            )
            AND EXISTS (
              SELECT 1 FROM source_policies AS policy
              WHERE policy.id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
                AND policy.key = 'market_price'
                AND policy.version = 'source_policy_v1'
                AND policy.schema_version = '1'
                AND policy.payload = '{{
                  "priority":["fosun","alpaca","fixture"],
                  "fosun":{{"mode":"read_only","operation":"fosun_get_bars","market_scope":"US"}},
                  "alpaca":{{"feed":"sip","modes":["delayed","fixture"]}},
                  "fixture":{{"mode":"fixture"}}
                }}'::jsonb
            )
          ) THEN
            RETURN;
          END IF;
          canonical_text := nexus_price_bar_set_canonical_v1(p_manifest_id);
          IF canonical_text IS NULL THEN
            RETURN;
          END IF;
          RETURN QUERY SELECT
            encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex'),
            octet_length(convert_to(canonical_text, 'UTF8'))::bigint,
            actual_count::integer,
            prepared_first_session,
            prepared_last_session,
            max_received;
        END;
        $$
        """
    )


def _extend_calendar_sessions() -> None:
    op.execute(
        "ALTER TABLE exchange_calendar_sessions "
        "ADD COLUMN market_open_at timestamptz, "
        "ADD COLUMN market_close_at timestamptz"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_calendar_session_times_v1()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          authority_source varchar(255);
        BEGIN
          IF NEW.market_open_at IS NULL OR NEW.market_close_at IS NULL THEN
            SELECT source INTO authority_source
            FROM exchange_calendar_authorities
            WHERE id = NEW.calendar_authority_id;
            IF authority_source IS DISTINCT FROM 'fixture' THEN
              RAISE EXCEPTION 'calendar session requires authoritative market bounds'
                USING ERRCODE = '23514';
            END IF;
            NEW.market_open_at := COALESCE(
              NEW.market_open_at,
              (NEW.session::timestamp + time '09:30')
                AT TIME ZONE 'America/New_York'
            );
            NEW.market_close_at := COALESCE(
              NEW.market_close_at,
              (NEW.session::timestamp + time '16:00')
                AT TIME ZONE 'America/New_York'
            );
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_exchange_calendar_session_times_v1 "
        "BEFORE INSERT ON exchange_calendar_sessions FOR EACH ROW "
        "EXECUTE FUNCTION nexus_calendar_session_times_v1()"
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER "
        "trg_exchange_calendar_sessions_created_at_immutable"
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER "
        "trg_exchange_calendar_sessions_immutable"
    )
    op.execute(
        """
        UPDATE exchange_calendar_sessions session_row
        SET market_open_at =
              (session_row.session::timestamp + time '09:30')
                AT TIME ZONE 'America/New_York',
            market_close_at =
              (session_row.session::timestamp + time '16:00')
                AT TIME ZONE 'America/New_York'
        FROM exchange_calendar_authorities authority
        WHERE authority.id = session_row.calendar_authority_id
          AND authority.source = 'fixture'
          AND (session_row.market_open_at IS NULL OR session_row.market_close_at IS NULL)
        """
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER "
        "trg_exchange_calendar_sessions_immutable"
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER "
        "trg_exchange_calendar_sessions_created_at_immutable"
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM exchange_calendar_sessions
            WHERE market_open_at IS NULL OR market_close_at IS NULL
          ) THEN
            RAISE EXCEPTION 'existing calendar sessions require authoritative market bounds';
          END IF;
        END;
        $$
        """
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions "
        "ALTER COLUMN market_open_at SET NOT NULL, "
        "ALTER COLUMN market_close_at SET NOT NULL"
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions ADD CONSTRAINT "
        "ck_exchange_calendar_sessions_market_bounds CHECK "
        "(market_open_at < market_close_at AND market_close_at <= decision_at)"
    )


def _create_execution_tables() -> None:
    op.execute(
        """
        CREATE TABLE paper_execution_models (
          id uuid NOT NULL,
          version varchar(100) NOT NULL,
          schema_version varchar(100) NOT NULL,
          content_hash varchar(64) NOT NULL,
          document jsonb NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_execution_models PRIMARY KEY (id),
          CONSTRAINT uq_paper_execution_models_version UNIQUE (version),
          CONSTRAINT uq_paper_execution_models_hash UNIQUE (content_hash),
          CONSTRAINT uq_paper_execution_models_id_hash UNIQUE (id, content_hash),
          CONSTRAINT ck_paper_execution_models_version_v1
            CHECK (version = 'execution_model_v1'),
          CONSTRAINT ck_paper_execution_models_schema_v1
            CHECK (schema_version = 'paper_execution_model_v1'),
          CONSTRAINT ck_paper_execution_models_hash
            CHECK (content_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_paper_execution_models_document_object
            CHECK (jsonb_typeof(document) = 'object')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_execution_window_authorities (
          id uuid NOT NULL,
          order_id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          window_seq integer NOT NULL,
          window_start_at timestamptz NOT NULL,
          window_end_at timestamptz NOT NULL,
          evaluated_at timestamptz NOT NULL,
          data_available_at timestamptz NOT NULL,
          source_authority_id uuid NOT NULL,
          source varchar(100) NOT NULL,
          source_content_hash varchar(64) NOT NULL,
          feed varchar(100) NOT NULL,
          delivery_mode varchar(20) NOT NULL,
          vwap numeric(38,12) NOT NULL,
          volume numeric(38,12) NOT NULL,
          capacity numeric(38,12) NOT NULL,
          quote_id uuid,
          quote_revision varchar(255),
          bid numeric(38,12),
          ask numeric(38,12),
          half_spread numeric(38,12) NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_execution_window_authorities PRIMARY KEY (id),
          CONSTRAINT uq_paper_execution_windows_order_seq UNIQUE (order_id, window_seq),
          CONSTRAINT uq_paper_execution_windows_order_bounds
            UNIQUE (order_id, window_start_at, window_end_at),
          CONSTRAINT uq_paper_execution_windows_fill_scope
            UNIQUE (id, order_id, account_id, instrument_id, source_authority_id),
          CONSTRAINT fk_paper_execution_windows_order
            FOREIGN KEY (order_id, account_id, instrument_id)
            REFERENCES paper_orders(id, account_id, instrument_id),
          CONSTRAINT fk_paper_execution_windows_authority
            FOREIGN KEY (source_authority_id, source, source_content_hash)
            REFERENCES price_bar_set_authorities(id, source, content_hash),
          CONSTRAINT fk_paper_execution_windows_quote
            FOREIGN KEY (quote_id) REFERENCES latest_quotes(id),
          CONSTRAINT ck_paper_execution_window_authorities_seq CHECK (window_seq >= 1),
          CONSTRAINT ck_paper_execution_window_authorities_bounds CHECK (
            window_end_at = window_start_at + interval '5 minutes'
            AND data_available_at <= evaluated_at
          ),
          CONSTRAINT ck_paper_execution_window_authorities_source CHECK (
            delivery_mode IN ('real_time','delayed','fixture','unknown')
            AND btrim(source) <> '' AND btrim(feed) <> ''
          ),
          CONSTRAINT ck_paper_execution_window_authorities_values CHECK (
            vwap > 0 AND volume > 0 AND capacity >= 0
            AND trunc(capacity) = capacity AND half_spread >= 0
          ),
          CONSTRAINT ck_paper_execution_window_authorities_quote_copy CHECK (
            (quote_id IS NULL AND quote_revision IS NULL AND bid IS NULL
              AND ask IS NULL AND half_spread = 0)
            OR (quote_id IS NOT NULL AND quote_revision IS NOT NULL
              AND bid > 0 AND ask >= bid AND half_spread = (ask - bid) / 2)
          ),
          CONSTRAINT ck_paper_execution_window_authorities_hashes CHECK (
            source_content_hash ~ '^[0-9a-f]{64}$'
            AND content_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_execution_window_members (
          window_authority_id uuid NOT NULL,
          bar_id uuid NOT NULL,
          member_seq integer NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_execution_window_members
            PRIMARY KEY (window_authority_id, bar_id),
          CONSTRAINT uq_paper_execution_window_member_seq
            UNIQUE (window_authority_id, member_seq),
          CONSTRAINT fk_paper_execution_window_members_window
            FOREIGN KEY (window_authority_id)
            REFERENCES paper_execution_window_authorities(id),
          CONSTRAINT fk_paper_execution_window_members_bar
            FOREIGN KEY (bar_id) REFERENCES price_bar_records(id),
          CONSTRAINT ck_paper_execution_window_members_seq
            CHECK (member_seq BETWEEN 1 AND 5)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_request_idempotency (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          order_id uuid NOT NULL,
          operation varchar(30) NOT NULL,
          idempotency_key varchar(255) NOT NULL,
          request_hash varchar(64) NOT NULL,
          result_event_seq integer,
          result_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_request_idempotency PRIMARY KEY (id),
          CONSTRAINT uq_paper_request_idempotency_scope
            UNIQUE (account_id, operation, idempotency_key),
          CONSTRAINT fk_paper_request_idempotency_order
            FOREIGN KEY (order_id, account_id)
            REFERENCES paper_orders(id, account_id),
          CONSTRAINT ck_paper_request_idempotency_operation
            CHECK (operation IN ('advance','cancel')),
          CONSTRAINT ck_paper_request_idempotency_key
            CHECK (length(idempotency_key) BETWEEN 1 AND 255),
          CONSTRAINT ck_paper_request_idempotency_seq
            CHECK (result_event_seq IS NULL OR result_event_seq >= 1),
          CONSTRAINT ck_paper_request_idempotency_hashes CHECK (
            request_hash ~ '^[0-9a-f]{64}$'
            AND result_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.create_unique_constraint(
        "uq_paper_performance_snapshots_id_account",
        "paper_performance_snapshots",
        ["id", "account_id"],
    )
    op.execute(
        """
        CREATE TABLE paper_performance_attributions (
          id uuid NOT NULL,
          performance_snapshot_id uuid NOT NULL,
          account_id uuid NOT NULL,
          dimension varchar(20) NOT NULL,
          dimension_key varchar(100) NOT NULL,
          market_value numeric(38,12) NOT NULL,
          realized_pnl numeric(38,12) NOT NULL,
          unrealized_pnl numeric(38,12) NOT NULL,
          fees numeric(38,12) NOT NULL,
          slippage numeric(38,12) NOT NULL,
          completeness varchar(10) NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_performance_attributions PRIMARY KEY (id),
          CONSTRAINT uq_paper_performance_attribution_dimension
            UNIQUE (performance_snapshot_id, dimension, dimension_key),
          CONSTRAINT fk_paper_performance_attributions_snapshot
            FOREIGN KEY (performance_snapshot_id, account_id)
            REFERENCES paper_performance_snapshots(id, account_id),
          CONSTRAINT ck_paper_performance_attributions_dimension
            CHECK (dimension IN ('cash','instrument','sector')),
          CONSTRAINT ck_paper_performance_attributions_values
            CHECK (market_value >= 0 AND fees >= 0 AND slippage >= 0),
          CONSTRAINT ck_paper_performance_attributions_completeness
            CHECK (completeness IN ('FULL','PARTIAL')),
          CONSTRAINT ck_paper_performance_attributions_hash
            CHECK (content_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _extend_fill_contract() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM paper_fills) THEN
            RAISE EXCEPTION 'Task 19 requires an empty pre-release paper fill table';
          END IF;
        END;
        $$
        """
    )
    op.execute(
        "ALTER TABLE paper_fills "
        "ADD COLUMN execution_model_id uuid NOT NULL, "
        "ADD COLUMN execution_window_authority_id uuid NOT NULL, "
        "ADD COLUMN ledger_transaction_id uuid NOT NULL, "
        "ADD COLUMN window_vwap numeric(38,12) NOT NULL, "
        "ADD COLUMN half_spread numeric(38,12) NOT NULL, "
        "ADD COLUMN slippage_bps integer NOT NULL, "
        "ADD COLUMN slippage_amount numeric(38,12) NOT NULL, "
        "ADD COLUMN commission numeric(38,12) NOT NULL, "
        "ADD COLUMN regulatory_fee numeric(38,12) NOT NULL, "
        "ADD COLUMN evaluated_at timestamptz NOT NULL"
    )
    op.create_unique_constraint(
        "uq_paper_fills_execution_window",
        "paper_fills",
        ["execution_window_authority_id"],
    )
    op.create_unique_constraint(
        "uq_paper_fills_ledger_transaction",
        "paper_fills",
        ["ledger_transaction_id"],
    )
    op.create_foreign_key(
        "fk_paper_fills_execution_model",
        "paper_fills",
        "paper_execution_models",
        ["execution_model_id", "execution_model_hash"],
        ["id", "content_hash"],
    )
    op.create_foreign_key(
        "fk_paper_fills_execution_window",
        "paper_fills",
        "paper_execution_window_authorities",
        [
            "execution_window_authority_id",
            "order_id",
            "account_id",
            "instrument_id",
            "source_authority_id",
        ],
        ["id", "order_id", "account_id", "instrument_id", "source_authority_id"],
    )
    op.create_foreign_key(
        "fk_paper_fills_ledger_transaction",
        "paper_fills",
        "paper_ledger_transactions",
        ["ledger_transaction_id", "account_id", "order_id"],
        ["id", "account_id", "order_id"],
    )
    op.execute(
        "ALTER TABLE paper_fills ADD CONSTRAINT ck_paper_fills_execution_values CHECK ("
        "window_vwap > 0 AND half_spread >= 0 "
        "AND slippage_bps IN (10,25) AND slippage_amount >= 0 "
        "AND commission >= 0 AND regulatory_fee >= 0 "
        "AND fee = commission + regulatory_fee)"
    )
    op.execute(
        "ALTER TABLE paper_reservation_events ADD COLUMN source_fill_id uuid"
    )
    op.create_unique_constraint(
        "uq_paper_reservation_events_source_fill",
        "paper_reservation_events",
        ["source_fill_id"],
    )
    op.create_foreign_key(
        "fk_paper_reservation_events_source_fill",
        "paper_reservation_events",
        "paper_fills",
        ["source_fill_id", "account_id"],
        ["id", "account_id"],
    )
    op.execute(
        "ALTER TABLE paper_reservation_events ADD CONSTRAINT "
        "ck_paper_reservation_events_fill_binding CHECK ("
        "(event_type = 'consumed' AND source_fill_id IS NOT NULL) OR "
        "(event_type IN ('reserved','released') AND source_fill_id IS NULL))"
    )


def _create_execution_guards() -> None:
    for table_name in _EXECUTION_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_reject_paper_fact_mutation()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_paper_execution_window_complete()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          window_ref uuid;
          root paper_execution_window_authorities%ROWTYPE;
          member_count integer;
          max_sequence integer;
          computed_volume numeric(38,12);
          computed_vwap numeric(38,12);
          computed_available_at timestamptz;
          scope_valid boolean;
          quote_valid boolean;
        BEGIN
          IF TG_TABLE_NAME = 'paper_execution_window_authorities' THEN
            window_ref := NEW.id;
          ELSE
            window_ref := NEW.window_authority_id;
          END IF;
          SELECT * INTO root
          FROM paper_execution_window_authorities WHERE id = window_ref;
          SELECT count(*), max(member.member_seq), sum(bar.volume),
                 round(sum(bar.close * bar.volume) / NULLIF(sum(bar.volume), 0), 12),
                 GREATEST(max(bar.available_at), max(authority.available_at)),
                 bool_and(
                   member.member_seq BETWEEN 1 AND 5
                   AND bar.observed_at = root.window_start_at
                     + make_interval(mins => member.member_seq - 1)
                   AND bar.instrument_id = root.instrument_id
                   AND bar.available_at <= root.evaluated_at
                   AND bar.price_series_manifest_id = authority.price_series_manifest_id
                   AND manifest.source = root.source
                   AND manifest.feed = root.feed
                   AND manifest.delivery_mode = root.delivery_mode
                   AND manifest.interval = '1Min'
                 )
            INTO member_count, max_sequence, computed_volume, computed_vwap,
                 computed_available_at, scope_valid
          FROM paper_execution_window_members member
          JOIN price_bar_records bar ON bar.id = member.bar_id
          JOIN price_bar_set_authorities authority
            ON authority.id = root.source_authority_id
          JOIN price_series_manifests manifest
            ON manifest.id = bar.price_series_manifest_id
          WHERE member.window_authority_id = window_ref;
          IF member_count <> 5 OR max_sequence <> 5 OR NOT COALESCE(scope_valid, false)
             OR computed_volume IS DISTINCT FROM root.volume
             OR computed_vwap IS DISTINCT FROM root.vwap
             OR trunc(computed_volume * 0.01) IS DISTINCT FROM root.capacity
             OR computed_available_at IS DISTINCT FROM root.data_available_at THEN
            RAISE EXCEPTION 'paper execution window evidence is incomplete or inconsistent'
              USING ERRCODE = '23514';
          END IF;
          IF root.quote_id IS NOT NULL THEN
            SELECT quote.instrument_id = root.instrument_id
                   AND quote.source = root.source AND quote.feed = root.feed
                   AND quote.observed_at >= root.window_start_at
                   AND quote.observed_at < root.window_end_at
                   AND quote.available_at <= root.evaluated_at
                   AND quote.bid = root.bid AND quote.ask = root.ask
                   AND quote.revision = root.quote_revision
                   AND quote.bid > 0 AND quote.ask >= quote.bid
                   AND quote.quality_flags = '[]'::jsonb
              INTO quote_valid
            FROM latest_quotes quote WHERE quote.id = root.quote_id;
            IF NOT COALESCE(quote_valid, false) THEN
              RAISE EXCEPTION 'paper execution quote evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_execution_windows_complete "
        "AFTER INSERT ON paper_execution_window_authorities "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_paper_execution_window_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_execution_members_complete "
        "AFTER INSERT ON paper_execution_window_members "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_paper_execution_window_complete()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_paper_fill_execution_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          order_quantity numeric(38,12);
          filled_quantity numeric(38,12);
          window_capacity numeric(38,12);
          window_quote_id uuid;
        BEGIN
          SELECT quantity INTO order_quantity FROM paper_orders WHERE id = NEW.order_id;
          SELECT COALESCE(sum(quantity), 0) INTO filled_quantity
          FROM paper_fills WHERE order_id = NEW.order_id;
          SELECT capacity, quote_id INTO window_capacity, window_quote_id
          FROM paper_execution_window_authorities
          WHERE id = NEW.execution_window_authority_id;
          IF filled_quantity > order_quantity OR NEW.quantity > window_capacity
             OR (window_quote_id IS NULL AND NEW.slippage_bps <> 25)
             OR (window_quote_id IS NOT NULL AND NEW.slippage_bps <> 10) THEN
            RAISE EXCEPTION 'paper fill exceeds its deterministic execution authority'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_fills_execution_valid "
        "AFTER INSERT ON paper_fills DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_fill_execution_valid()"
    )


def _seed_execution_model() -> None:
    op.execute(
        """
        INSERT INTO paper_execution_models (
          id, version, schema_version, content_hash, document
        ) VALUES (
          'c67dd7d7-d793-5b72-9c05-150634463e89',
          'execution_model_v1', 'paper_execution_model_v1',
          '18a4f8c7b9111e668baf011f33210df3f24ffc02ae15a448891b91898e169433',
          jsonb_build_object(
            'bar_interval', '1Min',
            'cash_reserve_markup_bps', 100,
            'commission_bps', 0,
            'first_evaluation_offset_minutes', 25,
            'missing_data_cancel_offset_minutes', 45,
            'price_method', 'close_volume_vwap_v1',
            'quoted_slippage_bps', 10,
            'regulatory_fee_policy', 'zero_v1',
            'remainder_cancel', 'calendar_market_close',
            'schema_version', 'paper_execution_model_v1',
            'unquoted_slippage_bps', 25,
            'version', 'execution_model_v1',
            'volume_participation', '0.01',
            'window_minutes', 5
          )
        )
        """
    )


def upgrade() -> None:
    """Upgrade schema."""
    _replace_price_bar_preparer(allow_intraday=True)
    _extend_calendar_sessions()
    _create_execution_tables()
    _extend_fill_contract()
    _create_execution_guards()
    _seed_execution_model()


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TRIGGER trg_paper_fills_execution_valid ON paper_fills")
    op.execute("DROP FUNCTION nexus_paper_fill_execution_valid()")
    op.execute(
        "DROP TRIGGER trg_paper_execution_members_complete "
        "ON paper_execution_window_members"
    )
    op.execute(
        "DROP TRIGGER trg_paper_execution_windows_complete "
        "ON paper_execution_window_authorities"
    )
    op.execute("DROP FUNCTION nexus_paper_execution_window_complete()")
    for table_name in reversed(_EXECUTION_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute(
        "ALTER TABLE paper_reservation_events DROP CONSTRAINT "
        "ck_paper_reservation_events_fill_binding"
    )
    op.drop_constraint(
        "fk_paper_reservation_events_source_fill",
        "paper_reservation_events",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_paper_reservation_events_source_fill",
        "paper_reservation_events",
        type_="unique",
    )
    op.drop_column("paper_reservation_events", "source_fill_id")
    op.execute(
        "ALTER TABLE paper_fills DROP CONSTRAINT ck_paper_fills_execution_values"
    )
    op.drop_constraint(
        "fk_paper_fills_ledger_transaction", "paper_fills", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_paper_fills_execution_window", "paper_fills", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_paper_fills_execution_model", "paper_fills", type_="foreignkey"
    )
    op.drop_constraint(
        "uq_paper_fills_ledger_transaction", "paper_fills", type_="unique"
    )
    op.drop_constraint(
        "uq_paper_fills_execution_window", "paper_fills", type_="unique"
    )
    for column_name in (
        "evaluated_at",
        "regulatory_fee",
        "commission",
        "slippage_amount",
        "slippage_bps",
        "half_spread",
        "window_vwap",
        "ledger_transaction_id",
        "execution_window_authority_id",
        "execution_model_id",
    ):
        op.drop_column("paper_fills", column_name)
    for table_name in reversed(_EXECUTION_TABLES):
        op.drop_table(table_name)
    op.drop_constraint(
        "uq_paper_performance_snapshots_id_account",
        "paper_performance_snapshots",
        type_="unique",
    )
    op.execute(
        "ALTER TABLE exchange_calendar_sessions DROP CONSTRAINT "
        "ck_exchange_calendar_sessions_market_bounds"
    )
    op.execute(
        "DROP TRIGGER trg_exchange_calendar_session_times_v1 "
        "ON exchange_calendar_sessions"
    )
    op.execute("DROP FUNCTION nexus_calendar_session_times_v1()")
    op.drop_column("exchange_calendar_sessions", "market_close_at")
    op.drop_column("exchange_calendar_sessions", "market_open_at")
    _replace_price_bar_preparer(allow_intraday=False)
