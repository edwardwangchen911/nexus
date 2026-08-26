"""Bind paper performance projections to their deterministic source facts.

Revision ID: 0019_paper_performance_root
Revises: 0018_paper_performance_chain
Create Date: 2026-08-27
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0019_paper_performance_root"
down_revision: str | Sequence[str] | None = "0018_paper_performance_chain"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Require ledger, position, price, and attribution reconciliation at commit."""
    op.execute(
        """
        CREATE FUNCTION nexus_paper_performance_root_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          snapshot_ref uuid;
          root paper_performance_snapshots%ROWTYPE;
          ledger_cash numeric(38,12);
          ledger_realized numeric(38,12);
          ledger_fees numeric(38,12);
          position_market numeric(38,12);
          position_unrealized numeric(38,12);
          cash_count integer;
          cash_market numeric(38,12);
          instrument_market numeric(38,12);
          instrument_realized numeric(38,12);
          instrument_unrealized numeric(38,12);
          instrument_fees numeric(38,12);
          instrument_slippage numeric(38,12);
          sector_market numeric(38,12);
          sector_realized numeric(38,12);
          sector_unrealized numeric(38,12);
          sector_fees numeric(38,12);
          sector_slippage numeric(38,12);
        BEGIN
          IF TG_TABLE_NAME = 'paper_performance_snapshots' THEN
            snapshot_ref := NEW.id;
          ELSIF TG_TABLE_NAME = 'paper_position_snapshots' THEN
            SELECT id INTO snapshot_ref
            FROM paper_performance_snapshots
            WHERE account_id = NEW.account_id AND as_of = NEW.as_of;
          ELSE
            snapshot_ref := NEW.performance_snapshot_id;
          END IF;

          SELECT * INTO root
          FROM paper_performance_snapshots WHERE id = snapshot_ref;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'paper performance root is inconsistent'
              USING ERRCODE = '23514';
          END IF;

          SELECT
            COALESCE(sum(CASE
              WHEN entry.account_code IN
                ('asset.cash.available.usd','asset.cash.reserved.usd')
              THEN entry.debit_amount - entry.credit_amount ELSE 0 END), 0),
            COALESCE(sum(CASE
              WHEN entry.account_code = 'income.realized_pnl.usd'
              THEN entry.credit_amount - entry.debit_amount ELSE 0 END), 0),
            COALESCE(sum(CASE
              WHEN entry.account_code = 'expense.fees.usd'
              THEN entry.debit_amount - entry.credit_amount ELSE 0 END), 0)
            INTO ledger_cash, ledger_realized, ledger_fees
          FROM paper_ledger_transactions ledger_transaction
          JOIN paper_ledger_entries entry
            ON entry.transaction_id = ledger_transaction.id
          WHERE ledger_transaction.account_id = root.account_id
            AND ledger_transaction.occurred_at <= root.as_of;

          SELECT
            COALESCE(sum(position.quantity * position.reference_close), 0),
            COALESCE(sum(
              position.quantity * position.reference_close - position.cost_basis
            ), 0)
            INTO position_market, position_unrealized
          FROM paper_position_snapshots position
          WHERE position.account_id = root.account_id
            AND position.as_of = root.as_of;

          IF EXISTS (
            SELECT 1
            FROM paper_position_snapshots position
            WHERE position.account_id = root.account_id
              AND position.as_of = root.as_of
              AND (
                COALESCE((
                  SELECT sum(event.quantity_delta)
                  FROM paper_position_adjustment_events event
                  WHERE event.account_id = position.account_id
                    AND event.instrument_id = position.instrument_id
                    AND event.occurred_at <= root.as_of
                ), 0) IS DISTINCT FROM position.quantity
                OR COALESCE((
                  SELECT sum(event.cost_delta)
                  FROM paper_position_adjustment_events event
                  WHERE event.account_id = position.account_id
                    AND event.instrument_id = position.instrument_id
                    AND event.occurred_at <= root.as_of
                ), 0) IS DISTINCT FROM position.cost_basis
                OR COALESCE((
                  SELECT sum(CASE
                    WHEN event.event_type = 'reserved' THEN event.amount
                    ELSE -event.amount END)
                  FROM paper_reservations reservation
                  JOIN paper_reservation_events event
                    ON event.reservation_id = reservation.id
                  WHERE reservation.account_id = position.account_id
                    AND reservation.reservation_type = 'position'
                    AND reservation.instrument_id = position.instrument_id
                    AND event.occurred_at <= root.as_of
                ), 0) IS DISTINCT FROM position.reserved_quantity
                OR NOT EXISTS (
                  SELECT 1
                  FROM price_bar_set_authorities authority
                  JOIN price_series_manifests manifest
                    ON manifest.id = authority.price_series_manifest_id
                  JOIN price_bar_records bar
                    ON bar.price_series_manifest_id = manifest.id
                  JOIN exchange_calendar_sessions calendar
                    ON calendar.session = bar.session
                   AND calendar.market_close_at = root.as_of
                  WHERE authority.id = position.price_authority_id
                    AND authority.available_at <= root.as_of
                    AND manifest.instrument = position.instrument_id::text
                    AND manifest.interval = '1Day'
                    AND bar.instrument_id = position.instrument_id
                    AND bar.available_at <= root.as_of
                    AND bar.received_at <= root.as_of
                    AND bar.close = position.reference_close
                )
              )
          ) THEN
            RAISE EXCEPTION 'paper performance root is inconsistent'
              USING ERRCODE = '23514';
          END IF;

          IF EXISTS (
            WITH position_metrics AS (
              SELECT
                position.instrument_id,
                position.quantity * position.reference_close AS market_value,
                position.quantity * position.reference_close - position.cost_basis
                  AS unrealized_pnl
              FROM paper_position_snapshots position
              WHERE position.account_id = root.account_id
                AND position.as_of = root.as_of
            ),
            fill_metrics AS (
              SELECT
                fill.instrument_id,
                COALESCE(sum(CASE WHEN paper_order.side = 'sell'
                  THEN fill.notional + adjustment.cost_delta ELSE 0 END), 0)
                  AS realized_pnl,
                COALESCE(sum(fill.fee), 0) AS fees,
                COALESCE(sum(fill.slippage_amount), 0) AS slippage
              FROM paper_fills fill
              JOIN paper_orders paper_order ON paper_order.id = fill.order_id
              JOIN paper_position_adjustment_events adjustment
                ON adjustment.source_fill_id = fill.id
              WHERE fill.account_id = root.account_id
                AND fill.occurred_at <= root.as_of
              GROUP BY fill.instrument_id
            ),
            instrument_ids AS (
              SELECT instrument_id FROM position_metrics
              UNION
              SELECT instrument_id FROM fill_metrics
            ),
            expected AS (
              SELECT
                instrument.instrument_id,
                COALESCE(position.market_value, 0) AS market_value,
                COALESCE(fill.realized_pnl, 0) AS realized_pnl,
                COALESCE(position.unrealized_pnl, 0) AS unrealized_pnl,
                COALESCE(fill.fees, 0) AS fees,
                COALESCE(fill.slippage, 0) AS slippage
              FROM instrument_ids instrument
              LEFT JOIN position_metrics position
                ON position.instrument_id = instrument.instrument_id
              LEFT JOIN fill_metrics fill
                ON fill.instrument_id = instrument.instrument_id
            ),
            actual AS (
              SELECT attribution.*
              FROM paper_performance_attributions attribution
              WHERE attribution.performance_snapshot_id = root.id
                AND attribution.account_id = root.account_id
                AND attribution.dimension = 'instrument'
            )
            SELECT 1
            FROM expected
            FULL OUTER JOIN actual
              ON actual.dimension_key = expected.instrument_id::text
            WHERE expected.instrument_id IS NULL
               OR actual.id IS NULL
               OR actual.market_value IS DISTINCT FROM expected.market_value
               OR actual.realized_pnl IS DISTINCT FROM expected.realized_pnl
               OR actual.unrealized_pnl IS DISTINCT FROM expected.unrealized_pnl
               OR actual.fees IS DISTINCT FROM expected.fees
               OR actual.slippage IS DISTINCT FROM expected.slippage
               OR actual.completeness <> 'FULL'
          ) THEN
            RAISE EXCEPTION 'paper performance root is inconsistent'
              USING ERRCODE = '23514';
          END IF;

          IF EXISTS (
            WITH instrument_ids AS (
              SELECT position.instrument_id
              FROM paper_position_snapshots position
              WHERE position.account_id = root.account_id
                AND position.as_of = root.as_of
              UNION
              SELECT fill.instrument_id
              FROM paper_fills fill
              WHERE fill.account_id = root.account_id
                AND fill.occurred_at <= root.as_of
            ),
            exposure_totals AS (
              SELECT
                instrument.instrument_id,
                COALESCE(sum(exposure.weight), 0) AS total_weight
              FROM instrument_ids instrument
              LEFT JOIN (
                SELECT DISTINCT session
                FROM exchange_calendar_sessions
                WHERE market_close_at = root.as_of
              ) calendar ON TRUE
              LEFT JOIN sector_exposures exposure
                ON exposure.instrument_id = instrument.instrument_id
               AND exposure.as_of = calendar.session
               AND exposure.created_at <= root.as_of
               AND exposure.reviewed
               AND NOT exposure.synthetic
              GROUP BY instrument.instrument_id
            ),
            expected_keys AS (
              SELECT DISTINCT exposure.sector_code AS dimension_key
              FROM exposure_totals total
              JOIN (
                SELECT DISTINCT session
                FROM exchange_calendar_sessions
                WHERE market_close_at = root.as_of
              ) calendar ON TRUE
              JOIN sector_exposures exposure
                ON exposure.instrument_id = total.instrument_id
               AND exposure.as_of = calendar.session
               AND exposure.created_at <= root.as_of
               AND exposure.reviewed
               AND NOT exposure.synthetic
              WHERE total.total_weight = 1
              UNION
              SELECT 'unclassified'
              WHERE EXISTS (
                SELECT 1 FROM exposure_totals WHERE total_weight <> 1
              )
            ),
            actual_keys AS (
              SELECT attribution.dimension_key, attribution.completeness
              FROM paper_performance_attributions attribution
              WHERE attribution.performance_snapshot_id = root.id
                AND attribution.account_id = root.account_id
                AND attribution.dimension = 'sector'
            )
            SELECT 1
            FROM expected_keys
            FULL OUTER JOIN actual_keys USING (dimension_key)
            WHERE expected_keys.dimension_key IS NULL
               OR actual_keys.dimension_key IS NULL
               OR actual_keys.completeness <> CASE
                    WHEN EXISTS (
                      SELECT 1 FROM exposure_totals WHERE total_weight <> 1
                    ) THEN 'PARTIAL' ELSE 'FULL' END
          ) THEN
            RAISE EXCEPTION 'paper performance root is inconsistent'
              USING ERRCODE = '23514';
          END IF;

          SELECT
            count(*) FILTER (WHERE attribution.dimension = 'cash'),
            COALESCE(sum(attribution.market_value)
              FILTER (WHERE attribution.dimension = 'cash'), 0),
            COALESCE(sum(attribution.market_value)
              FILTER (WHERE attribution.dimension = 'instrument'), 0),
            COALESCE(sum(attribution.realized_pnl)
              FILTER (WHERE attribution.dimension = 'instrument'), 0),
            COALESCE(sum(attribution.unrealized_pnl)
              FILTER (WHERE attribution.dimension = 'instrument'), 0),
            COALESCE(sum(attribution.fees)
              FILTER (WHERE attribution.dimension = 'instrument'), 0),
            COALESCE(sum(attribution.slippage)
              FILTER (WHERE attribution.dimension = 'instrument'), 0),
            COALESCE(sum(attribution.market_value)
              FILTER (WHERE attribution.dimension = 'sector'), 0),
            COALESCE(sum(attribution.realized_pnl)
              FILTER (WHERE attribution.dimension = 'sector'), 0),
            COALESCE(sum(attribution.unrealized_pnl)
              FILTER (WHERE attribution.dimension = 'sector'), 0),
            COALESCE(sum(attribution.fees)
              FILTER (WHERE attribution.dimension = 'sector'), 0),
            COALESCE(sum(attribution.slippage)
              FILTER (WHERE attribution.dimension = 'sector'), 0)
            INTO cash_count, cash_market,
                 instrument_market, instrument_realized, instrument_unrealized,
                 instrument_fees, instrument_slippage,
                 sector_market, sector_realized, sector_unrealized,
                 sector_fees, sector_slippage
          FROM paper_performance_attributions attribution
          WHERE attribution.performance_snapshot_id = root.id
            AND attribution.account_id = root.account_id;

          IF cash_count <> 1 OR EXISTS (
            SELECT 1 FROM paper_performance_attributions attribution
            WHERE attribution.performance_snapshot_id = root.id
              AND attribution.dimension = 'cash'
              AND (
                attribution.dimension_key <> 'USD'
                OR attribution.market_value IS DISTINCT FROM root.cash
                OR attribution.realized_pnl <> 0
                OR attribution.unrealized_pnl <> 0
                OR attribution.fees <> 0
                OR attribution.slippage <> 0
                OR attribution.completeness <> 'FULL'
              )
          ) OR root.cash IS DISTINCT FROM ledger_cash
             OR root.cash IS DISTINCT FROM cash_market
             OR root.realized_pnl IS DISTINCT FROM ledger_realized
             OR root.realized_pnl IS DISTINCT FROM instrument_realized
             OR root.unrealized_pnl IS DISTINCT FROM position_unrealized
             OR root.unrealized_pnl IS DISTINCT FROM instrument_unrealized
             OR root.fees IS DISTINCT FROM ledger_fees
             OR root.fees IS DISTINCT FROM instrument_fees
             OR root.nav IS DISTINCT FROM root.cash + position_market
             OR position_market IS DISTINCT FROM instrument_market
             OR sector_market IS DISTINCT FROM instrument_market
             OR sector_realized IS DISTINCT FROM instrument_realized
             OR sector_unrealized IS DISTINCT FROM instrument_unrealized
             OR sector_fees IS DISTINCT FROM instrument_fees
             OR sector_slippage IS DISTINCT FROM instrument_slippage THEN
            RAISE EXCEPTION 'paper performance root is inconsistent'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    for table_name in (
        "paper_performance_snapshots",
        "paper_position_snapshots",
        "paper_performance_attributions",
    ):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER trg_{table_name}_performance_root "
            f"AFTER INSERT ON {table_name} DEFERRABLE INITIALLY DEFERRED "
            "FOR EACH ROW EXECUTE FUNCTION nexus_paper_performance_root_valid()"
        )


def downgrade() -> None:
    """Remove the Task 19 corrective performance-root guards."""
    for table_name in reversed(
        (
            "paper_performance_snapshots",
            "paper_position_snapshots",
            "paper_performance_attributions",
        )
    ):
        op.execute(f"DROP TRIGGER trg_{table_name}_performance_root ON {table_name}")
    op.execute("DROP FUNCTION nexus_paper_performance_root_valid()")
