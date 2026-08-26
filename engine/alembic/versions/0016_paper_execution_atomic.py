"""Bind each internal-paper fill to one complete atomic fact root.

Revision ID: 0016_paper_execution_atomic
Revises: 0015_paper_execution
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016_paper_execution_atomic"
down_revision: str | Sequence[str] | None = "0015_paper_execution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _replace_order_chain(*, allow_repeated_partial: bool) -> None:
    partial_targets = (
        "('partially_filled','filled','cancelled','rejected','reconciling')"
        if allow_repeated_partial
        else "('filled','cancelled','rejected','reconciling')"
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_paper_order_event_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          order_ref uuid := NEW.order_id;
          event_count integer;
          max_sequence integer;
        BEGIN
          SELECT count(*), max(event_seq)
            INTO event_count, max_sequence
          FROM paper_order_events WHERE order_id = order_ref;
          IF event_count = 0 OR max_sequence <> event_count THEN
            RAISE EXCEPTION 'paper order event sequence is not continuous'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1
            FROM (
              SELECT event_seq, from_status, to_status, actor_kind,
                     lag(to_status) OVER (ORDER BY event_seq) AS prior_status
              FROM paper_order_events WHERE order_id = order_ref
            ) ordered
            WHERE
              (event_seq = 1 AND (
                from_status IS NOT NULL OR to_status <> 'proposed'
                OR actor_kind <> 'task18'
              ))
              OR (event_seq > 1 AND from_status IS DISTINCT FROM prior_status)
              OR (event_seq > 1 AND NOT (
                (from_status = 'proposed' AND to_status IN ('risk_rejected','risk_approved'))
                OR (from_status = 'risk_approved'
                    AND to_status IN ('submitted','awaiting_approval'))
                OR (from_status = 'awaiting_approval'
                    AND to_status IN ('submitted','cancelled','rejected'))
                OR (from_status = 'submitted'
                    AND to_status IN
                      ('partially_filled','filled','cancelled','rejected','reconciling'))
                OR (from_status = 'partially_filled'
                    AND to_status IN {partial_targets})
                OR (from_status = 'reconciling'
                    AND to_status IN
                      ('submitted','partially_filled','filled','cancelled','rejected'))
              ))
              OR (actor_kind = 'task18' AND NOT (
                (event_seq = 1 AND to_status = 'proposed')
                OR (from_status = 'proposed'
                    AND to_status IN ('risk_rejected','risk_approved'))
                OR (from_status = 'risk_approved' AND to_status = 'submitted')
              ))
              OR (actor_kind = 'task19' AND from_status NOT IN
                ('submitted','partially_filled','reconciling'))
          ) THEN
            RAISE EXCEPTION 'paper order event transition is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )


def _replace_window_guard(*, include_quote_watermark: bool) -> None:
    watermark = (
        "GREATEST(max(bar.available_at), max(authority.available_at), max(quote.available_at))"
        if include_quote_watermark
        else "GREATEST(max(bar.available_at), max(authority.available_at))"
    )
    quote_join = (
        "LEFT JOIN latest_quotes quote ON quote.id = root.quote_id"
        if include_quote_watermark
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_paper_execution_window_complete()
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
                 {watermark},
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
          {quote_join}
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


def _add_fill_bindings() -> None:
    op.execute("ALTER TABLE paper_order_events ADD COLUMN source_fill_id uuid")
    op.create_unique_constraint(
        "uq_paper_order_events_source_fill",
        "paper_order_events",
        ["source_fill_id"],
    )
    op.create_foreign_key(
        "fk_paper_order_events_source_fill",
        "paper_order_events",
        "paper_fills",
        ["source_fill_id", "account_id"],
        ["id", "account_id"],
    )
    op.execute(
        "ALTER TABLE paper_order_events ADD CONSTRAINT "
        "ck_paper_order_events_fill_binding CHECK ("
        "(actor_kind = 'task19' AND to_status IN ('partially_filled','filled') "
        "AND source_fill_id IS NOT NULL) OR "
        "(NOT (actor_kind = 'task19' AND "
        "to_status IN ('partially_filled','filled')) AND source_fill_id IS NULL))"
    )
    op.create_unique_constraint(
        "uq_paper_position_adjustments_source_fill",
        "paper_position_adjustment_events",
        ["source_fill_id"],
    )


def _create_atomic_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_paper_execution_source_frozen()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF (
            SELECT count(DISTINCT (source, feed, delivery_mode))
            FROM paper_execution_window_authorities
            WHERE order_id = NEW.order_id
          ) > 1 THEN
            RAISE EXCEPTION 'paper execution source scope cannot switch'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_execution_source_frozen "
        "AFTER INSERT ON paper_execution_window_authorities "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION nexus_paper_execution_source_frozen()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_paper_fill_atomic_root_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          fill_ref uuid;
          fill_row paper_fills%ROWTYPE;
          order_side varchar(10);
          consumed_count integer;
          consumed_amount numeric(38,12);
          position_count integer;
          position_event paper_position_adjustment_events%ROWTYPE;
          prior_quantity numeric(38,12);
          prior_cost numeric(38,12);
          allocated_cost numeric(38,12);
          transaction_row paper_ledger_transactions%ROWTYPE;
          entry_count integer;
          position_debit numeric(38,12);
          position_credit numeric(38,12);
          reserved_credit numeric(38,12);
          available_debit numeric(38,12);
          fee_debit numeric(38,12);
          realized_net numeric(38,12);
          event_count integer;
          source_scope_count integer;
          tick numeric(38,12);
          raw_price numeric;
          expected_price numeric(38,12);
          directional_cost numeric(38,12);
          expected_entry_count integer;
        BEGIN
          IF TG_TABLE_NAME = 'paper_fills' THEN
            fill_ref := NEW.id;
          ELSIF TG_TABLE_NAME = 'paper_reservation_events' THEN
            fill_ref := NEW.source_fill_id;
          ELSIF TG_TABLE_NAME = 'paper_position_adjustment_events' THEN
            fill_ref := NEW.source_fill_id;
          ELSIF TG_TABLE_NAME = 'paper_order_events' THEN
            fill_ref := NEW.source_fill_id;
          ELSIF TG_TABLE_NAME = 'paper_ledger_entries' THEN
            SELECT fill.id INTO fill_ref
            FROM paper_fills fill
            WHERE fill.ledger_transaction_id = NEW.transaction_id;
          END IF;
          IF fill_ref IS NULL THEN
            RETURN NULL;
          END IF;
          SELECT * INTO fill_row FROM paper_fills WHERE id = fill_ref;
          IF NOT FOUND THEN
            RETURN NULL;
          END IF;
          SELECT side INTO order_side FROM paper_orders WHERE id = fill_row.order_id;

          SELECT count(*), max(event.amount)
            INTO consumed_count, consumed_amount
          FROM paper_reservation_events event
          JOIN paper_reservations reservation ON reservation.id = event.reservation_id
          WHERE event.source_fill_id = fill_ref
            AND event.event_type = 'consumed'
            AND reservation.order_id = fill_row.order_id
            AND event.account_id = fill_row.account_id;
          IF consumed_count <> 1 OR consumed_amount IS DISTINCT FROM (
             CASE WHEN order_side = 'buy' THEN fill_row.notional + fill_row.fee
                  ELSE fill_row.quantity END
          ) THEN
            RAISE EXCEPTION 'paper fill reservation root is incomplete'
              USING ERRCODE = '23514';
          END IF;

          SELECT count(*), max(event.event_seq)
            INTO position_count, event_count
          FROM paper_position_adjustment_events event
          WHERE event.source_fill_id = fill_ref
            AND event.account_id = fill_row.account_id
            AND event.instrument_id = fill_row.instrument_id;
          IF position_count <> 1 THEN
            RAISE EXCEPTION 'paper fill position root is incomplete'
              USING ERRCODE = '23514';
          END IF;
          SELECT * INTO position_event
          FROM paper_position_adjustment_events WHERE source_fill_id = fill_ref;
          IF order_side = 'buy' THEN
            IF position_event.quantity_delta IS DISTINCT FROM fill_row.quantity
               OR position_event.cost_delta IS DISTINCT FROM fill_row.notional THEN
              RAISE EXCEPTION 'paper buy fill position root is inconsistent'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            SELECT COALESCE(sum(quantity_delta), 0), COALESCE(sum(cost_delta), 0)
              INTO prior_quantity, prior_cost
            FROM paper_position_adjustment_events
            WHERE account_id = fill_row.account_id
              AND instrument_id = fill_row.instrument_id
              AND event_seq < position_event.event_seq;
            IF prior_quantity < fill_row.quantity OR prior_cost <= 0 THEN
              RAISE EXCEPTION 'paper sell fill has insufficient prior position'
                USING ERRCODE = '23514';
            END IF;
            allocated_cost := CASE
              WHEN prior_quantity = fill_row.quantity THEN prior_cost
              ELSE round(prior_cost * fill_row.quantity / prior_quantity, 12)
            END;
            IF position_event.quantity_delta IS DISTINCT FROM -fill_row.quantity
               OR position_event.cost_delta IS DISTINCT FROM -allocated_cost THEN
              RAISE EXCEPTION 'paper sell fill position root is inconsistent'
                USING ERRCODE = '23514';
            END IF;
          END IF;

          SELECT * INTO transaction_row FROM paper_ledger_transactions
          WHERE id = fill_row.ledger_transaction_id;
          SELECT count(*),
                 COALESCE(sum(debit_amount) FILTER (
                   WHERE account_code = 'asset.position_cost.usd'), 0),
                 COALESCE(sum(credit_amount) FILTER (
                   WHERE account_code = 'asset.position_cost.usd'), 0),
                 COALESCE(sum(credit_amount) FILTER (
                   WHERE account_code = 'asset.cash.reserved.usd'), 0),
                 COALESCE(sum(debit_amount) FILTER (
                   WHERE account_code = 'asset.cash.available.usd'), 0),
                 COALESCE(sum(debit_amount) FILTER (
                   WHERE account_code = 'expense.fees.usd'), 0),
                 COALESCE(sum(credit_amount - debit_amount) FILTER (
                   WHERE account_code = 'income.realized_pnl.usd'), 0)
            INTO entry_count, position_debit, position_credit, reserved_credit,
                 available_debit, fee_debit, realized_net
          FROM paper_ledger_entries WHERE transaction_id = fill_row.ledger_transaction_id;
          IF transaction_row.transaction_type IS DISTINCT FROM 'fill'
             OR transaction_row.reference_type IS DISTINCT FROM 'fill'
             OR transaction_row.reference_hash IS DISTINCT FROM fill_row.payload_hash
             OR transaction_row.order_id IS DISTINCT FROM fill_row.order_id THEN
            RAISE EXCEPTION 'paper fill ledger root is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          IF order_side = 'buy' THEN
            expected_entry_count := 2 + CASE WHEN fill_row.fee > 0 THEN 1 ELSE 0 END;
            IF entry_count <> expected_entry_count
               OR position_debit IS DISTINCT FROM fill_row.notional
               OR position_credit <> 0 OR reserved_credit IS DISTINCT FROM
                  fill_row.notional + fill_row.fee
               OR available_debit <> 0 OR fee_debit IS DISTINCT FROM fill_row.fee
               OR realized_net <> 0 THEN
              RAISE EXCEPTION 'paper buy fill ledger entries are inconsistent'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            allocated_cost := -position_event.cost_delta;
            expected_entry_count := 2
              + CASE WHEN fill_row.fee > 0 THEN 1 ELSE 0 END
              + CASE WHEN fill_row.notional <> allocated_cost THEN 1 ELSE 0 END;
            IF entry_count <> expected_entry_count OR position_debit <> 0
               OR position_credit IS DISTINCT FROM allocated_cost
               OR reserved_credit <> 0
               OR available_debit IS DISTINCT FROM fill_row.notional - fill_row.fee
               OR fee_debit IS DISTINCT FROM fill_row.fee
               OR realized_net IS DISTINCT FROM fill_row.notional - allocated_cost THEN
              RAISE EXCEPTION 'paper sell fill ledger entries are inconsistent'
                USING ERRCODE = '23514';
            END IF;
          END IF;

          SELECT count(*) INTO event_count FROM paper_order_events
          WHERE source_fill_id = fill_ref AND order_id = fill_row.order_id
            AND account_id = fill_row.account_id AND actor_kind = 'task19'
            AND to_status IN ('partially_filled','filled');
          IF event_count <> 1 THEN
            RAISE EXCEPTION 'paper fill order event root is incomplete'
              USING ERRCODE = '23514';
          END IF;

          SELECT count(DISTINCT ROW(source, feed, delivery_mode))
            INTO source_scope_count
          FROM paper_execution_window_authorities
          WHERE order_id = fill_row.order_id;
          IF source_scope_count <> 1 THEN
            RAISE EXCEPTION 'paper fill execution source scope is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          SELECT CASE WHEN instrument.tick_size > 0 THEN instrument.tick_size
                      ELSE 0.000000000001 END,
                 window_root.half_spread
                   + window_root.vwap * fill_row.slippage_bps / 10000
            INTO tick, directional_cost
          FROM instruments instrument
          JOIN paper_execution_window_authorities window_root
            ON window_root.id = fill_row.execution_window_authority_id
          WHERE instrument.id = fill_row.instrument_id;
          SELECT CASE WHEN order_side = 'buy'
                      THEN window_root.vwap + directional_cost
                      ELSE window_root.vwap - directional_cost END
            INTO raw_price
          FROM paper_execution_window_authorities window_root
          WHERE window_root.id = fill_row.execution_window_authority_id;
          expected_price := round(
            CASE WHEN order_side = 'buy' THEN ceil(raw_price / tick) * tick
                 ELSE floor(raw_price / tick) * tick END,
            12
          );
          IF fill_row.price IS DISTINCT FROM expected_price
             OR fill_row.slippage_amount IS DISTINCT FROM
                round(fill_row.quantity * directional_cost, 12) THEN
            RAISE EXCEPTION 'paper fill price is inconsistent with execution authority'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    for table_name in (
        "paper_fills",
        "paper_reservation_events",
        "paper_position_adjustment_events",
        "paper_order_events",
        "paper_ledger_entries",
    ):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_{table_name}_fill_atomic_root "
            f"AFTER INSERT ON {table_name} DEFERRABLE INITIALLY DEFERRED "
            "FOR EACH ROW EXECUTE FUNCTION nexus_paper_fill_atomic_root_valid()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_paper_terminal_reservation_closed()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          order_ref uuid;
          reservation_ref uuid;
          root_amount numeric(38,12);
          closed_amount numeric(38,12);
          terminal_status varchar(30);
        BEGIN
          IF TG_TABLE_NAME = 'paper_order_events' THEN
            order_ref := NEW.order_id;
          ELSE
            SELECT order_id INTO order_ref FROM paper_reservations
            WHERE id = NEW.reservation_id;
          END IF;
          SELECT to_status INTO terminal_status FROM paper_order_events
          WHERE order_id = order_ref ORDER BY event_seq DESC LIMIT 1;
          IF terminal_status NOT IN ('filled','cancelled','rejected','risk_rejected') THEN
            RETURN NULL;
          END IF;
          SELECT id, amount INTO reservation_ref, root_amount
          FROM paper_reservations WHERE order_id = order_ref;
          IF reservation_ref IS NULL THEN
            RETURN NULL;
          END IF;
          SELECT COALESCE(sum(amount), 0) INTO closed_amount
          FROM paper_reservation_events
          WHERE reservation_id = reservation_ref
            AND event_type IN ('consumed','released');
          IF closed_amount IS DISTINCT FROM root_amount THEN
            RAISE EXCEPTION 'terminal paper order reservation must be fully closed'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    for table_name in ("paper_order_events", "paper_reservation_events"):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_{table_name}_terminal_reservation "
            f"AFTER INSERT ON {table_name} DEFERRABLE INITIALLY DEFERRED "
            "FOR EACH ROW EXECUTE FUNCTION nexus_paper_terminal_reservation_closed()"
        )


def upgrade() -> None:
    """Upgrade schema."""
    _add_fill_bindings()
    _replace_order_chain(allow_repeated_partial=True)
    _replace_window_guard(include_quote_watermark=True)
    _create_atomic_guards()


def downgrade() -> None:
    """Downgrade schema."""
    for table_name in ("paper_reservation_events", "paper_order_events"):
        op.execute(f"DROP TRIGGER trg_{table_name}_terminal_reservation ON {table_name}")
    op.execute("DROP FUNCTION nexus_paper_terminal_reservation_closed()")
    for table_name in reversed(
        (
            "paper_fills",
            "paper_reservation_events",
            "paper_position_adjustment_events",
            "paper_order_events",
            "paper_ledger_entries",
        )
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_fill_atomic_root ON {table_name}")
    op.execute("DROP FUNCTION nexus_paper_fill_atomic_root_valid()")
    op.execute(
        "DROP TRIGGER trg_paper_execution_source_frozen ON paper_execution_window_authorities"
    )
    op.execute("DROP FUNCTION nexus_paper_execution_source_frozen()")
    _replace_window_guard(include_quote_watermark=False)
    _replace_order_chain(allow_repeated_partial=False)
    op.drop_constraint(
        "uq_paper_position_adjustments_source_fill",
        "paper_position_adjustment_events",
        type_="unique",
    )
    op.execute("ALTER TABLE paper_order_events DROP CONSTRAINT ck_paper_order_events_fill_binding")
    op.drop_constraint(
        "fk_paper_order_events_source_fill",
        "paper_order_events",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_paper_order_events_source_fill",
        "paper_order_events",
        type_="unique",
    )
    op.drop_column("paper_order_events", "source_fill_id")
