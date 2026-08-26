"""Add deterministic hard risk and append-only internal paper accounting.

Revision ID: 0014_paper
Revises: 0013_shadow
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0014_paper"
down_revision: str | Sequence[str] | None = "0013_shadow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PAPER_TABLES = (
    "risk_policies",
    "paper_accounts",
    "strategy_decisions",
    "strategy_decision_targets",
    "paper_drawdown_lock_events",
    "paper_order_batches",
    "paper_orders",
    "paper_order_events",
    "risk_evaluations",
    "risk_evaluation_rules",
    "risk_evaluation_price_inputs",
    "risk_evaluation_sector_inputs",
    "risk_query_authorities",
    "risk_evaluation_query_proofs",
    "paper_ledger_accounts",
    "paper_ledger_transactions",
    "paper_ledger_entries",
    "paper_reservations",
    "paper_reservation_events",
    "paper_fills",
    "paper_position_adjustment_events",
    "paper_position_snapshots",
    "paper_performance_snapshots",
)


def _create_identity_policy_and_decisions() -> None:
    op.execute(
        """
        CREATE TABLE paper_accounts (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          portfolio_id uuid NOT NULL,
          code varchar(50) NOT NULL,
          currency varchar(3) NOT NULL,
          account_model varchar(20) NOT NULL,
          position_model varchar(20) NOT NULL,
          initial_capital numeric(38,12) NOT NULL,
          capability varchar(50) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_accounts PRIMARY KEY (id),
          CONSTRAINT uq_paper_accounts_account UNIQUE (account_id),
          CONSTRAINT uq_paper_accounts_portfolio UNIQUE (portfolio_id),
          CONSTRAINT uq_paper_accounts_code UNIQUE (code),
          CONSTRAINT uq_paper_accounts_id_account UNIQUE (id, account_id),
          CONSTRAINT uq_paper_accounts_id_portfolio UNIQUE (id, portfolio_id),
          CONSTRAINT fk_paper_accounts_account FOREIGN KEY (account_id)
            REFERENCES accounts(id),
          CONSTRAINT fk_paper_accounts_portfolio FOREIGN KEY (portfolio_id)
            REFERENCES portfolios(id),
          CONSTRAINT ck_paper_accounts_currency_usd CHECK (currency = 'USD'),
          CONSTRAINT ck_paper_accounts_model_fixed
            CHECK (account_model = 'cash' AND position_model = 'long_only'),
          CONSTRAINT ck_paper_accounts_capital_positive CHECK (initial_capital > 0),
          CONSTRAINT ck_paper_accounts_capability_internal
            CHECK (capability = 'simulation_write_internal'),
          CONSTRAINT ck_paper_accounts_code_nonempty CHECK (btrim(code) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_policies (
          id uuid NOT NULL,
          version varchar(50) NOT NULL,
          active boolean NOT NULL,
          single_name_limit numeric(38,12) NOT NULL,
          sector_limit numeric(38,12) NOT NULL,
          minimum_cash_ratio numeric(38,12) NOT NULL,
          drawdown_lock_ratio numeric(38,12) NOT NULL,
          drawdown_recovery_ratio numeric(38,12) NOT NULL,
          minimum_buy_notional numeric(38,12) NOT NULL,
          buy_reserve_markup numeric(38,12) NOT NULL,
          commission numeric(38,12) NOT NULL,
          supported_market varchar(10) NOT NULL,
          supported_currency varchar(3) NOT NULL,
          supported_asset_types varchar(30)[] NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_policies PRIMARY KEY (id),
          CONSTRAINT uq_risk_policies_version UNIQUE (version),
          CONSTRAINT ck_risk_policies_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_risk_policies_ratio_bounds CHECK (
            single_name_limit > 0 AND single_name_limit <= 1
            AND sector_limit > 0 AND sector_limit <= 1
            AND minimum_cash_ratio > 0 AND minimum_cash_ratio <= 1
            AND drawdown_lock_ratio > 0 AND drawdown_lock_ratio <= 1
            AND drawdown_recovery_ratio > 0 AND drawdown_recovery_ratio < drawdown_lock_ratio
            AND single_name_limit <= sector_limit
          ),
          CONSTRAINT ck_risk_policies_amount_bounds CHECK (
            minimum_buy_notional > 0
            AND buy_reserve_markup >= 0 AND buy_reserve_markup <= 0.10
            AND commission >= 0
          ),
          CONSTRAINT ck_risk_policies_scope_fixed CHECK (
            supported_market = 'US' AND supported_currency = 'USD'
            AND supported_asset_types = ARRAY['ETF','common_stock']::varchar[]
          )
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_risk_policies_active ON risk_policies ((active)) WHERE active"
    )
    op.execute(
        """
        CREATE TABLE strategy_decisions (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          portfolio_id uuid NOT NULL,
          strategy_id varchar(50) NOT NULL,
          strategy_version varchar(50) NOT NULL,
          decision_session date NOT NULL,
          decision_time timestamptz NOT NULL,
          execution_session date NOT NULL,
          source_policy_id uuid NOT NULL,
          calendar_authority_id uuid NOT NULL,
          universe_snapshot_id uuid NOT NULL,
          quality_report_id uuid NOT NULL,
          portfolio_projection_hash varchar(64) NOT NULL,
          authority_hash varchar(64) NOT NULL,
          strategy_parameters_hash varchar(64) NOT NULL,
          decision_hash varchar(64) NOT NULL,
          cash_weight numeric(38,12) NOT NULL,
          reason varchar(30) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_strategy_decisions PRIMARY KEY (id),
          CONSTRAINT uq_strategy_decisions_account_version_session
            UNIQUE (account_id, strategy_version, decision_session),
          CONSTRAINT uq_strategy_decisions_id_account UNIQUE (id, account_id),
          CONSTRAINT uq_strategy_decisions_hash UNIQUE (decision_hash),
          CONSTRAINT fk_strategy_decisions_paper_account
            FOREIGN KEY (account_id, portfolio_id)
            REFERENCES paper_accounts(id, portfolio_id),
          CONSTRAINT fk_strategy_decisions_source_policy FOREIGN KEY (source_policy_id)
            REFERENCES source_policies(id),
          CONSTRAINT fk_strategy_decisions_calendar FOREIGN KEY (calendar_authority_id)
            REFERENCES exchange_calendar_authorities(id),
          CONSTRAINT fk_strategy_decisions_universe FOREIGN KEY (universe_snapshot_id)
            REFERENCES universe_snapshots(id),
          CONSTRAINT fk_strategy_decisions_quality FOREIGN KEY (quality_report_id)
            REFERENCES quality_reports(id),
          CONSTRAINT ck_strategy_decisions_strategy_fixed
            CHECK (strategy_id = 'baseline' AND strategy_version = 'baseline_v1'),
          CONSTRAINT ck_strategy_decisions_sessions_ordered
            CHECK (execution_session > decision_session),
          CONSTRAINT ck_strategy_decisions_reason_weekly
            CHECK (reason = 'WEEKLY_REBALANCE'),
          CONSTRAINT ck_strategy_decisions_cash_weight
            CHECK (cash_weight >= 0 AND cash_weight <= 1),
          CONSTRAINT ck_strategy_decisions_hashes_canonical CHECK (
            portfolio_projection_hash ~ '^[0-9a-f]{64}$'
            AND authority_hash ~ '^[0-9a-f]{64}$'
            AND strategy_parameters_hash ~ '^[0-9a-f]{64}$'
            AND decision_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE strategy_decision_targets (
          id uuid NOT NULL,
          strategy_decision_id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          target_weight numeric(38,12) NOT NULL,
          exit boolean NOT NULL,
          strategy_priority integer NOT NULL,
          reason varchar(100) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_strategy_decision_targets PRIMARY KEY (id),
          CONSTRAINT uq_strategy_decision_targets_decision_instrument
            UNIQUE (strategy_decision_id, instrument_id),
          CONSTRAINT fk_strategy_decision_targets_decision
            FOREIGN KEY (strategy_decision_id, account_id)
            REFERENCES strategy_decisions(id, account_id),
          CONSTRAINT fk_strategy_decision_targets_instrument
            FOREIGN KEY (instrument_id) REFERENCES instruments(id),
          CONSTRAINT ck_strategy_decision_targets_weight
            CHECK (target_weight >= 0 AND target_weight <= 1),
          CONSTRAINT ck_strategy_decision_targets_priority
            CHECK (strategy_priority BETWEEN 0 AND 2),
          CONSTRAINT ck_strategy_decision_targets_exit_binding CHECK (
            (exit AND target_weight = 0 AND strategy_priority = 0)
            OR (NOT exit AND strategy_priority IN (1,2))
          ),
          CONSTRAINT ck_strategy_decision_targets_reason_nonempty
            CHECK (btrim(reason) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_drawdown_lock_events (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          event_seq integer NOT NULL,
          from_state varchar(30),
          to_state varchar(30) NOT NULL,
          drawdown_ratio numeric(38,12) NOT NULL,
          user_acknowledged boolean NOT NULL,
          reason_code varchar(100) NOT NULL,
          occurred_at timestamptz NOT NULL,
          payload_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_drawdown_lock_events PRIMARY KEY (id),
          CONSTRAINT uq_paper_drawdown_events_account_seq UNIQUE (account_id, event_seq),
          CONSTRAINT fk_paper_drawdown_events_account FOREIGN KEY (account_id)
            REFERENCES paper_accounts(id),
          CONSTRAINT ck_paper_drawdown_lock_events_seq_positive CHECK (event_seq >= 1),
          CONSTRAINT ck_paper_drawdown_lock_events_states_allowed CHECK (
            (from_state IS NULL OR from_state IN ('unlocked','locked','recovery_eligible'))
            AND to_state IN ('unlocked','locked','recovery_eligible')
          ),
          CONSTRAINT ck_paper_drawdown_lock_events_ratio
            CHECK (drawdown_ratio >= 0 AND drawdown_ratio <= 1),
          CONSTRAINT ck_paper_drawdown_lock_events_reason_nonempty CHECK (btrim(reason_code) <> ''),
          CONSTRAINT ck_paper_drawdown_lock_events_hash_canonical
            CHECK (payload_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _create_orders_and_risk() -> None:
    op.execute(
        """
        CREATE TABLE paper_order_batches (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          strategy_decision_id uuid NOT NULL,
          risk_policy_id uuid NOT NULL,
          idempotency_record_id uuid NOT NULL,
          request_hash varchar(64) NOT NULL,
          projection_hash varchar(64) NOT NULL,
          risk_input_hash varchar(64) NOT NULL,
          result_hash varchar(64) NOT NULL,
          batch_hash varchar(64) NOT NULL,
          evaluated_at timestamptz NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_order_batches PRIMARY KEY (id),
          CONSTRAINT uq_paper_order_batches_account_decision
            UNIQUE (account_id, strategy_decision_id),
          CONSTRAINT uq_paper_order_batches_idempotency UNIQUE (idempotency_record_id),
          CONSTRAINT uq_paper_order_batches_hash UNIQUE (batch_hash),
          CONSTRAINT uq_paper_order_batches_id_account_decision
            UNIQUE (id, account_id, strategy_decision_id),
          CONSTRAINT fk_paper_order_batches_decision
            FOREIGN KEY (strategy_decision_id, account_id)
            REFERENCES strategy_decisions(id, account_id),
          CONSTRAINT fk_paper_order_batches_policy FOREIGN KEY (risk_policy_id)
            REFERENCES risk_policies(id),
          CONSTRAINT fk_paper_order_batches_idempotency FOREIGN KEY (idempotency_record_id)
            REFERENCES idempotency_records(id),
          CONSTRAINT ck_paper_order_batches_hashes_canonical CHECK (
            request_hash ~ '^[0-9a-f]{64}$'
            AND projection_hash ~ '^[0-9a-f]{64}$'
            AND risk_input_hash ~ '^[0-9a-f]{64}$'
            AND result_hash ~ '^[0-9a-f]{64}$'
            AND batch_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_orders (
          id uuid NOT NULL,
          batch_id uuid NOT NULL,
          account_id uuid NOT NULL,
          strategy_decision_id uuid NOT NULL,
          client_order_id varchar(100) NOT NULL,
          instrument_id uuid NOT NULL,
          side varchar(10) NOT NULL,
          quantity numeric(38,12) NOT NULL,
          target_weight numeric(38,12) NOT NULL,
          reference_close numeric(38,12) NOT NULL,
          worst_price numeric(38,12) NOT NULL,
          risk_outcome varchar(20) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_orders PRIMARY KEY (id),
          CONSTRAINT uq_paper_orders_account_client UNIQUE (account_id, client_order_id),
          CONSTRAINT uq_paper_orders_id_account UNIQUE (id, account_id),
          CONSTRAINT fk_paper_orders_batch
            FOREIGN KEY (batch_id, account_id, strategy_decision_id)
            REFERENCES paper_order_batches(id, account_id, strategy_decision_id),
          CONSTRAINT fk_paper_orders_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_paper_orders_side_allowed CHECK (side IN ('buy','sell')),
          CONSTRAINT ck_paper_orders_quantity_whole_positive
            CHECK (quantity > 0 AND trunc(quantity) = quantity),
          CONSTRAINT ck_paper_orders_weight
            CHECK (target_weight >= 0 AND target_weight <= 1),
          CONSTRAINT ck_paper_orders_prices_positive
            CHECK (reference_close > 0 AND worst_price > 0),
          CONSTRAINT ck_paper_orders_outcome_allowed
            CHECK (risk_outcome IN ('approved','reduced','rejected'))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_order_events (
          id uuid NOT NULL,
          order_id uuid NOT NULL,
          account_id uuid NOT NULL,
          event_seq integer NOT NULL,
          from_status varchar(30),
          to_status varchar(30) NOT NULL,
          actor_kind varchar(30) NOT NULL,
          reason_code varchar(100) NOT NULL,
          occurred_at timestamptz NOT NULL,
          payload_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_order_events PRIMARY KEY (id),
          CONSTRAINT uq_paper_order_events_order_seq UNIQUE (order_id, event_seq),
          CONSTRAINT fk_paper_order_events_order
            FOREIGN KEY (order_id, account_id) REFERENCES paper_orders(id, account_id),
          CONSTRAINT ck_paper_order_events_seq_positive CHECK (event_seq >= 1),
          CONSTRAINT ck_paper_order_events_status_allowed CHECK (
            (from_status IS NULL OR from_status IN
              ('proposed','risk_rejected','risk_approved','awaiting_approval','submitted',
               'partially_filled','filled','cancelled','rejected','reconciling'))
            AND to_status IN
              ('proposed','risk_rejected','risk_approved','awaiting_approval','submitted',
               'partially_filled','filled','cancelled','rejected','reconciling')
          ),
          CONSTRAINT ck_paper_order_events_actor_allowed
            CHECK (actor_kind IN ('task18','task19','user','reconciliation')),
          CONSTRAINT ck_paper_order_events_reason_nonempty CHECK (btrim(reason_code) <> ''),
          CONSTRAINT ck_paper_order_events_hash_canonical
            CHECK (payload_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_evaluations (
          id uuid NOT NULL,
          order_id uuid NOT NULL,
          batch_id uuid NOT NULL,
          account_id uuid NOT NULL,
          risk_policy_id uuid NOT NULL,
          outcome varchar(20) NOT NULL,
          desired_quantity numeric(38,12) NOT NULL,
          approved_quantity numeric(38,12) NOT NULL,
          target_weight numeric(38,12) NOT NULL,
          strategy_priority integer NOT NULL,
          reference_close numeric(38,12) NOT NULL,
          worst_price numeric(38,12) NOT NULL,
          reserve_amount numeric(38,12) NOT NULL,
          projected_available_cash numeric(38,12) NOT NULL,
          projected_cash_ratio numeric(38,12) NOT NULL,
          projected_name_ratio numeric(38,12) NOT NULL,
          input_hash varchar(64) NOT NULL,
          result_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_evaluations PRIMARY KEY (id),
          CONSTRAINT uq_risk_evaluations_order UNIQUE (order_id),
          CONSTRAINT uq_risk_evaluations_id_account UNIQUE (id, account_id),
          CONSTRAINT fk_risk_evaluations_order
            FOREIGN KEY (order_id, account_id) REFERENCES paper_orders(id, account_id),
          CONSTRAINT fk_risk_evaluations_batch FOREIGN KEY (batch_id)
            REFERENCES paper_order_batches(id),
          CONSTRAINT fk_risk_evaluations_policy FOREIGN KEY (risk_policy_id)
            REFERENCES risk_policies(id),
          CONSTRAINT ck_risk_evaluations_outcome_allowed
            CHECK (outcome IN ('approved','reduced','rejected')),
          CONSTRAINT ck_risk_evaluations_quantities CHECK (
            desired_quantity > 0 AND trunc(desired_quantity) = desired_quantity
            AND approved_quantity >= 0 AND trunc(approved_quantity) = approved_quantity
            AND approved_quantity <= desired_quantity
          ),
          CONSTRAINT ck_risk_evaluations_amounts CHECK (
            reference_close > 0 AND worst_price > 0
            AND reserve_amount >= 0 AND projected_available_cash >= 0
          ),
          CONSTRAINT ck_risk_evaluations_hashes_canonical CHECK (
            input_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_evaluation_rules (
          id uuid NOT NULL,
          risk_evaluation_id uuid NOT NULL,
          account_id uuid NOT NULL,
          rule_seq integer NOT NULL,
          rule_code varchar(100) NOT NULL,
          passed boolean NOT NULL,
          limit_value numeric(38,12),
          observed_value numeric(38,12),
          remaining_value numeric(38,12),
          authority_ids uuid[] NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_evaluation_rules PRIMARY KEY (id),
          CONSTRAINT uq_risk_evaluation_rules_evaluation_seq
            UNIQUE (risk_evaluation_id, rule_seq),
          CONSTRAINT fk_risk_evaluation_rules_evaluation
            FOREIGN KEY (risk_evaluation_id, account_id)
            REFERENCES risk_evaluations(id, account_id),
          CONSTRAINT ck_risk_evaluation_rules_seq_positive CHECK (rule_seq >= 1),
          CONSTRAINT ck_risk_evaluation_rules_code_nonempty CHECK (btrim(rule_code) <> ''),
          CONSTRAINT ck_risk_evaluation_rules_authorities_nonnull
            CHECK (array_position(authority_ids, NULL) IS NULL)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_evaluation_price_inputs (
          id uuid NOT NULL,
          risk_evaluation_id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          price_authority_id uuid NOT NULL,
          close numeric(38,12) NOT NULL,
          source varchar(100) NOT NULL,
          delivery_mode varchar(50) NOT NULL,
          available_at timestamptz NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_evaluation_price_inputs PRIMARY KEY (id),
          CONSTRAINT uq_risk_price_inputs_evaluation_instrument
            UNIQUE (risk_evaluation_id, instrument_id),
          CONSTRAINT fk_risk_price_inputs_evaluation
            FOREIGN KEY (risk_evaluation_id, account_id)
            REFERENCES risk_evaluations(id, account_id),
          CONSTRAINT fk_risk_price_inputs_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT fk_risk_price_inputs_authority FOREIGN KEY (price_authority_id)
            REFERENCES price_bar_set_authorities(id),
          CONSTRAINT ck_risk_evaluation_price_inputs_close_positive CHECK (close > 0),
          CONSTRAINT ck_risk_evaluation_price_inputs_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_evaluation_sector_inputs (
          id uuid NOT NULL,
          risk_evaluation_id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          sector_exposure_id uuid NOT NULL,
          sector varchar(100) NOT NULL,
          weight numeric(38,12) NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_evaluation_sector_inputs PRIMARY KEY (id),
          CONSTRAINT uq_risk_sector_inputs_evaluation_instrument_sector
            UNIQUE (risk_evaluation_id, instrument_id, sector),
          CONSTRAINT fk_risk_sector_inputs_evaluation
            FOREIGN KEY (risk_evaluation_id, account_id)
            REFERENCES risk_evaluations(id, account_id),
          CONSTRAINT fk_risk_sector_inputs_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT fk_risk_sector_inputs_exposure FOREIGN KEY (sector_exposure_id)
            REFERENCES sector_exposures(id),
          CONSTRAINT ck_risk_evaluation_sector_inputs_weight
            CHECK (weight > 0 AND weight <= 1),
          CONSTRAINT ck_risk_evaluation_sector_inputs_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_query_authorities (
          id uuid NOT NULL,
          source_policy_id uuid NOT NULL,
          universe_snapshot_id uuid NOT NULL,
          decision_session date NOT NULL,
          kind varchar(30) NOT NULL,
          required_instrument_ids uuid[] NOT NULL,
          blocked_instrument_ids uuid[] NOT NULL,
          complete boolean NOT NULL,
          source_watermark_hash varchar(64) NOT NULL,
          available_at timestamptz NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_query_authorities PRIMARY KEY (id),
          CONSTRAINT uq_risk_query_authorities_scope
            UNIQUE (source_policy_id, universe_snapshot_id, decision_session, kind),
          CONSTRAINT fk_risk_query_authorities_source_policy
            FOREIGN KEY (source_policy_id) REFERENCES source_policies(id),
          CONSTRAINT fk_risk_query_authorities_universe
            FOREIGN KEY (universe_snapshot_id) REFERENCES universe_snapshots(id),
          CONSTRAINT ck_risk_query_authorities_kind_allowed
            CHECK (kind IN ('corporate_action','suspension','delisting')),
          CONSTRAINT ck_risk_query_authorities_required_nonempty
            CHECK (cardinality(required_instrument_ids) > 0),
          CONSTRAINT ck_risk_query_authorities_arrays_nonnull CHECK (
            array_position(required_instrument_ids, NULL) IS NULL
            AND array_position(blocked_instrument_ids, NULL) IS NULL
          ),
          CONSTRAINT ck_risk_query_authorities_blocked_subset
            CHECK (blocked_instrument_ids <@ required_instrument_ids),
          CONSTRAINT ck_risk_query_authorities_complete CHECK (complete),
          CONSTRAINT ck_risk_query_authorities_hashes_canonical CHECK (
            source_watermark_hash ~ '^[0-9a-f]{64}$'
            AND content_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE risk_evaluation_query_proofs (
          id uuid NOT NULL,
          risk_evaluation_id uuid NOT NULL,
          account_id uuid NOT NULL,
          kind varchar(30) NOT NULL,
          authority_id uuid NOT NULL,
          complete boolean NOT NULL,
          blocked boolean NOT NULL,
          available_at timestamptz NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_risk_evaluation_query_proofs PRIMARY KEY (id),
          CONSTRAINT uq_risk_query_proofs_evaluation_kind
            UNIQUE (risk_evaluation_id, kind),
          CONSTRAINT fk_risk_query_proofs_evaluation
            FOREIGN KEY (risk_evaluation_id, account_id)
            REFERENCES risk_evaluations(id, account_id),
          CONSTRAINT fk_risk_query_proofs_authority
            FOREIGN KEY (authority_id) REFERENCES risk_query_authorities(id),
          CONSTRAINT ck_risk_evaluation_query_proofs_kind_allowed
            CHECK (kind IN ('corporate_action','suspension','delisting')),
          CONSTRAINT ck_risk_evaluation_query_proofs_complete CHECK (complete),
          CONSTRAINT ck_risk_evaluation_query_proofs_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _create_ledger_and_reservations() -> None:
    op.execute(
        """
        CREATE TABLE paper_ledger_accounts (
          id uuid NOT NULL,
          paper_account_id uuid NOT NULL,
          code varchar(100) NOT NULL,
          normal_side varchar(10) NOT NULL,
          currency varchar(3) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_ledger_accounts PRIMARY KEY (id),
          CONSTRAINT uq_paper_ledger_accounts_account_code
            UNIQUE (paper_account_id, code),
          CONSTRAINT uq_paper_ledger_accounts_id_account_code
            UNIQUE (id, paper_account_id, code),
          CONSTRAINT fk_paper_ledger_accounts_paper_account
            FOREIGN KEY (paper_account_id) REFERENCES paper_accounts(id),
          CONSTRAINT ck_paper_ledger_accounts_normal_side
            CHECK (normal_side IN ('debit','credit')),
          CONSTRAINT ck_paper_ledger_accounts_currency_usd CHECK (currency = 'USD'),
          CONSTRAINT ck_paper_ledger_accounts_code_nonempty CHECK (btrim(code) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_ledger_transactions (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          order_id uuid,
          transaction_type varchar(30) NOT NULL,
          reference_type varchar(30) NOT NULL,
          reference_hash varchar(64) NOT NULL,
          occurred_at timestamptz NOT NULL,
          transaction_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_ledger_transactions PRIMARY KEY (id),
          CONSTRAINT uq_paper_ledger_transactions_account_hash
            UNIQUE (account_id, transaction_hash),
          CONSTRAINT uq_paper_ledger_transactions_id_account
            UNIQUE (id, account_id),
          CONSTRAINT fk_paper_ledger_transactions_account FOREIGN KEY (account_id)
            REFERENCES paper_accounts(id),
          CONSTRAINT fk_paper_ledger_transactions_order
            FOREIGN KEY (order_id, account_id) REFERENCES paper_orders(id, account_id),
          CONSTRAINT ck_paper_ledger_transactions_type_allowed
            CHECK (transaction_type IN ('funding','cash_reservation','fill','release','adjustment')),
          CONSTRAINT ck_paper_ledger_transactions_reference_allowed
            CHECK (reference_type IN
              ('initial_funding','cash_reservation','fill','release','adjustment')),
          CONSTRAINT ck_paper_ledger_transactions_order_binding CHECK (
            (transaction_type = 'funding' AND order_id IS NULL
              AND reference_type = 'initial_funding')
            OR (transaction_type <> 'funding' AND order_id IS NOT NULL)
          ),
          CONSTRAINT ck_paper_ledger_transactions_hashes_canonical CHECK (
            reference_hash ~ '^[0-9a-f]{64}$'
            AND transaction_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_ledger_entries (
          id uuid NOT NULL,
          transaction_id uuid NOT NULL,
          account_id uuid NOT NULL,
          entry_seq integer NOT NULL,
          ledger_account_id uuid NOT NULL,
          account_code varchar(100) NOT NULL,
          currency varchar(3) NOT NULL,
          debit_amount numeric(38,12) NOT NULL,
          credit_amount numeric(38,12) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_ledger_entries PRIMARY KEY (id),
          CONSTRAINT uq_paper_ledger_entries_transaction_seq
            UNIQUE (transaction_id, entry_seq),
          CONSTRAINT fk_paper_ledger_entries_transaction
            FOREIGN KEY (transaction_id, account_id)
            REFERENCES paper_ledger_transactions(id, account_id),
          CONSTRAINT fk_paper_ledger_entries_account
            FOREIGN KEY (ledger_account_id, account_id, account_code)
            REFERENCES paper_ledger_accounts(id, paper_account_id, code),
          CONSTRAINT ck_paper_ledger_entries_seq_positive CHECK (entry_seq >= 1),
          CONSTRAINT ck_paper_ledger_entries_currency_usd CHECK (currency = 'USD'),
          CONSTRAINT ck_paper_ledger_entries_one_side CHECK (
            (debit_amount > 0 AND credit_amount = 0)
            OR (credit_amount > 0 AND debit_amount = 0)
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_reservations (
          id uuid NOT NULL,
          order_id uuid NOT NULL,
          account_id uuid NOT NULL,
          ledger_transaction_id uuid,
          reservation_type varchar(20) NOT NULL,
          currency varchar(3),
          instrument_id uuid,
          amount numeric(38,12) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_reservations PRIMARY KEY (id),
          CONSTRAINT uq_paper_reservations_order UNIQUE (order_id),
          CONSTRAINT uq_paper_reservations_id_account UNIQUE (id, account_id),
          CONSTRAINT uq_paper_reservations_ledger_transaction UNIQUE (ledger_transaction_id),
          CONSTRAINT fk_paper_reservations_order
            FOREIGN KEY (order_id, account_id) REFERENCES paper_orders(id, account_id),
          CONSTRAINT fk_paper_reservations_ledger_transaction
            FOREIGN KEY (ledger_transaction_id, account_id)
            REFERENCES paper_ledger_transactions(id, account_id),
          CONSTRAINT fk_paper_reservations_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_paper_reservations_amount_positive CHECK (amount > 0),
          CONSTRAINT ck_paper_reservations_type_binding CHECK (
            (reservation_type = 'cash' AND currency = 'USD'
              AND instrument_id IS NULL AND ledger_transaction_id IS NOT NULL)
            OR (reservation_type = 'position' AND currency IS NULL
              AND instrument_id IS NOT NULL AND ledger_transaction_id IS NULL
              AND trunc(amount) = amount)
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_reservation_events (
          id uuid NOT NULL,
          reservation_id uuid NOT NULL,
          account_id uuid NOT NULL,
          event_seq integer NOT NULL,
          event_type varchar(20) NOT NULL,
          amount numeric(38,12) NOT NULL,
          occurred_at timestamptz NOT NULL,
          payload_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_reservation_events PRIMARY KEY (id),
          CONSTRAINT uq_paper_reservation_events_reservation_seq
            UNIQUE (reservation_id, event_seq),
          CONSTRAINT fk_paper_reservation_events_reservation
            FOREIGN KEY (reservation_id, account_id)
            REFERENCES paper_reservations(id, account_id),
          CONSTRAINT ck_paper_reservation_events_seq_positive CHECK (event_seq >= 1),
          CONSTRAINT ck_paper_reservation_events_type_allowed
            CHECK (event_type IN ('reserved','consumed','released')),
          CONSTRAINT ck_paper_reservation_events_amount_positive CHECK (amount > 0),
          CONSTRAINT ck_paper_reservation_events_hash_canonical
            CHECK (payload_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _create_task19_contract_tables() -> None:
    op.execute(
        """
        CREATE TABLE paper_fills (
          id uuid NOT NULL,
          order_id uuid NOT NULL,
          account_id uuid NOT NULL,
          fill_seq integer NOT NULL,
          instrument_id uuid NOT NULL,
          quantity numeric(38,12) NOT NULL,
          price numeric(38,12) NOT NULL,
          notional numeric(38,12) NOT NULL,
          fee numeric(38,12) NOT NULL,
          occurred_at timestamptz NOT NULL,
          source_authority_id uuid NOT NULL,
          execution_model_hash varchar(64) NOT NULL,
          payload_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_fills PRIMARY KEY (id),
          CONSTRAINT uq_paper_fills_order_seq UNIQUE (order_id, fill_seq),
          CONSTRAINT uq_paper_fills_id_account UNIQUE (id, account_id),
          CONSTRAINT fk_paper_fills_order
            FOREIGN KEY (order_id, account_id) REFERENCES paper_orders(id, account_id),
          CONSTRAINT fk_paper_fills_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_paper_fills_seq_positive CHECK (fill_seq >= 1),
          CONSTRAINT ck_paper_fills_values_positive CHECK (
            quantity > 0 AND trunc(quantity) = quantity
            AND price > 0 AND notional > 0 AND fee >= 0
          ),
          CONSTRAINT ck_paper_fills_hashes_canonical CHECK (
            execution_model_hash ~ '^[0-9a-f]{64}$'
            AND payload_hash ~ '^[0-9a-f]{64}$'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_position_adjustment_events (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          event_seq integer NOT NULL,
          event_type varchar(30) NOT NULL,
          quantity_delta numeric(38,12) NOT NULL,
          cost_delta numeric(38,12) NOT NULL,
          source_fill_id uuid,
          occurred_at timestamptz NOT NULL,
          payload_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_position_adjustment_events PRIMARY KEY (id),
          CONSTRAINT uq_paper_position_events_account_instrument_seq
            UNIQUE (account_id, instrument_id, event_seq),
          CONSTRAINT fk_paper_position_events_account FOREIGN KEY (account_id)
            REFERENCES paper_accounts(id),
          CONSTRAINT fk_paper_position_events_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT fk_paper_position_events_fill
            FOREIGN KEY (source_fill_id, account_id) REFERENCES paper_fills(id, account_id),
          CONSTRAINT ck_paper_position_adjustment_events_seq_positive CHECK (event_seq >= 1),
          CONSTRAINT ck_paper_position_adjustment_events_type_allowed
            CHECK (event_type IN
              ('fill','split','dividend','delisting','correction')),
          CONSTRAINT ck_paper_position_adjustment_events_quantity_whole
            CHECK (trunc(quantity_delta) = quantity_delta),
          CONSTRAINT ck_paper_position_adjustment_events_hash_canonical
            CHECK (payload_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_position_snapshots (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          as_of timestamptz NOT NULL,
          quantity numeric(38,12) NOT NULL,
          reserved_quantity numeric(38,12) NOT NULL,
          cost_basis numeric(38,12) NOT NULL,
          reference_close numeric(38,12) NOT NULL,
          price_authority_id uuid NOT NULL,
          projection_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_position_snapshots PRIMARY KEY (id),
          CONSTRAINT uq_paper_position_snapshots_account_asof_instrument
            UNIQUE (account_id, as_of, instrument_id),
          CONSTRAINT fk_paper_position_snapshots_account FOREIGN KEY (account_id)
            REFERENCES paper_accounts(id),
          CONSTRAINT fk_paper_position_snapshots_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT fk_paper_position_snapshots_price_authority
            FOREIGN KEY (price_authority_id) REFERENCES price_bar_set_authorities(id),
          CONSTRAINT ck_paper_position_snapshots_values CHECK (
            quantity >= 0 AND trunc(quantity) = quantity
            AND reserved_quantity >= 0 AND trunc(reserved_quantity) = reserved_quantity
            AND reserved_quantity <= quantity
            AND cost_basis >= 0 AND reference_close > 0
          ),
          CONSTRAINT ck_paper_position_snapshots_hash_canonical
            CHECK (projection_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE paper_performance_snapshots (
          id uuid NOT NULL,
          account_id uuid NOT NULL,
          as_of timestamptz NOT NULL,
          nav numeric(38,12) NOT NULL,
          peak_nav numeric(38,12) NOT NULL,
          cash numeric(38,12) NOT NULL,
          realized_pnl numeric(38,12) NOT NULL,
          unrealized_pnl numeric(38,12) NOT NULL,
          fees numeric(38,12) NOT NULL,
          drawdown_ratio numeric(38,12) NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_paper_performance_snapshots PRIMARY KEY (id),
          CONSTRAINT uq_paper_performance_snapshots_account_asof
            UNIQUE (account_id, as_of),
          CONSTRAINT fk_paper_performance_snapshots_account FOREIGN KEY (account_id)
            REFERENCES paper_accounts(id),
          CONSTRAINT ck_paper_performance_snapshots_values CHECK (
            nav > 0 AND peak_nav >= nav AND cash >= 0 AND fees >= 0
            AND drawdown_ratio >= 0 AND drawdown_ratio <= 1
          ),
          CONSTRAINT ck_paper_performance_snapshots_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _create_database_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_reject_paper_fact_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RAISE EXCEPTION 'paper fact is append-only' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    for table_name in _PAPER_TABLES:
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
        CREATE FUNCTION nexus_paper_order_event_chain_valid()
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
                    AND to_status IN ('filled','cancelled','rejected','reconciling'))
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
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_order_events_chain "
        "AFTER INSERT ON paper_order_events DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_order_event_chain_valid()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_paper_reservation_event_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          reservation_ref uuid := NEW.reservation_id;
          root_amount numeric(38,12);
          event_count integer;
          max_sequence integer;
          reserved_total numeric(38,12);
          consumed_total numeric(38,12);
        BEGIN
          SELECT amount INTO root_amount
          FROM paper_reservations WHERE id = reservation_ref;
          SELECT count(*), max(event_seq),
                 sum(amount) FILTER (WHERE event_type = 'reserved'),
                 sum(amount) FILTER (WHERE event_type IN ('consumed','released'))
            INTO event_count, max_sequence, reserved_total, consumed_total
          FROM paper_reservation_events WHERE reservation_id = reservation_ref;
          IF event_count = 0 OR max_sequence <> event_count
             OR NOT EXISTS (
               SELECT 1 FROM paper_reservation_events
               WHERE reservation_id = reservation_ref AND event_seq = 1
                 AND event_type = 'reserved' AND amount = root_amount
             )
             OR EXISTS (
               SELECT 1 FROM paper_reservation_events
               WHERE reservation_id = reservation_ref
                 AND event_seq > 1 AND event_type = 'reserved'
             )
             OR reserved_total IS DISTINCT FROM root_amount
             OR COALESCE(consumed_total, 0) > root_amount THEN
            RAISE EXCEPTION 'paper reservation event chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_reservation_events_chain "
        "AFTER INSERT ON paper_reservation_events DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_reservation_event_chain_valid()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_paper_ledger_transaction_balanced()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          transaction_ref uuid;
          account_ref uuid;
          entry_count integer;
          max_sequence integer;
          debit_total numeric(38,12);
          credit_total numeric(38,12);
        BEGIN
          IF TG_TABLE_NAME = 'paper_ledger_transactions' THEN
            transaction_ref := NEW.id;
            account_ref := NEW.account_id;
          ELSE
            transaction_ref := NEW.transaction_id;
            account_ref := NEW.account_id;
          END IF;
          SELECT count(*), max(entry_seq), sum(debit_amount), sum(credit_amount)
            INTO entry_count, max_sequence, debit_total, credit_total
          FROM paper_ledger_entries WHERE transaction_id = transaction_ref;
          IF entry_count = 0 OR max_sequence <> entry_count
             OR debit_total <= 0 OR debit_total IS DISTINCT FROM credit_total THEN
            RAISE EXCEPTION 'paper ledger transaction is not balanced'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1
            FROM paper_ledger_accounts account
            LEFT JOIN paper_ledger_entries entry
              ON entry.ledger_account_id = account.id
            WHERE account.paper_account_id = account_ref
            GROUP BY account.id, account.normal_side
            HAVING CASE account.normal_side
              WHEN 'debit' THEN
                COALESCE(sum(entry.debit_amount), 0)
                - COALESCE(sum(entry.credit_amount), 0) < 0
              ELSE
                COALESCE(sum(entry.credit_amount), 0)
                - COALESCE(sum(entry.debit_amount), 0) < 0
            END
          ) THEN
            RAISE EXCEPTION 'paper ledger account balance is negative'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_ledger_transactions_balanced "
        "AFTER INSERT ON paper_ledger_transactions DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_ledger_transaction_balanced()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_ledger_entries_balanced "
        "AFTER INSERT ON paper_ledger_entries DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_ledger_transaction_balanced()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_paper_drawdown_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          event_count integer;
          max_sequence integer;
        BEGIN
          SELECT count(*), max(event_seq) INTO event_count, max_sequence
          FROM paper_drawdown_lock_events WHERE account_id = NEW.account_id;
          IF max_sequence <> event_count OR EXISTS (
            SELECT 1
            FROM (
              SELECT event_seq, from_state, to_state, drawdown_ratio,
                     user_acknowledged,
                     lag(to_state) OVER (ORDER BY event_seq) AS prior_state
              FROM paper_drawdown_lock_events WHERE account_id = NEW.account_id
            ) ordered
            WHERE
              (event_seq = 1 AND (
                from_state IS NOT NULL OR to_state <> 'unlocked'
                OR drawdown_ratio <> 0 OR user_acknowledged
              ))
              OR (event_seq > 1 AND from_state IS DISTINCT FROM prior_state)
              OR (event_seq > 1 AND NOT (
                (from_state = 'unlocked' AND to_state = 'locked'
                  AND drawdown_ratio >= 0.08 AND NOT user_acknowledged)
                OR (from_state = 'locked' AND to_state = 'recovery_eligible'
                  AND drawdown_ratio < 0.06 AND NOT user_acknowledged)
                OR (from_state = 'recovery_eligible' AND to_state = 'locked'
                  AND drawdown_ratio >= 0.06 AND NOT user_acknowledged)
                OR (from_state = 'recovery_eligible' AND to_state = 'unlocked'
                  AND drawdown_ratio < 0.06 AND user_acknowledged)
              ))
          ) THEN
            RAISE EXCEPTION 'paper drawdown event chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_paper_drawdown_events_chain "
        "AFTER INSERT ON paper_drawdown_lock_events DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_paper_drawdown_chain_valid()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_strategy_decision_weights_complete()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          decision_ref uuid;
          total_weight numeric(38,12);
        BEGIN
          IF TG_TABLE_NAME = 'strategy_decisions' THEN
            decision_ref := NEW.id;
          ELSE
            decision_ref := NEW.strategy_decision_id;
          END IF;
          SELECT decision.cash_weight + COALESCE(sum(target.target_weight), 0)
            INTO total_weight
          FROM strategy_decisions decision
          LEFT JOIN strategy_decision_targets target
            ON target.strategy_decision_id = decision.id
          WHERE decision.id = decision_ref
          GROUP BY decision.cash_weight;
          IF total_weight IS DISTINCT FROM 1.000000000000 THEN
            RAISE EXCEPTION 'strategy decision weights must equal one'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_strategy_decisions_weights "
        "AFTER INSERT ON strategy_decisions DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_strategy_decision_weights_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_strategy_decision_targets_weights "
        "AFTER INSERT ON strategy_decision_targets DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_strategy_decision_weights_complete()"
    )


def _seed_internal_paper_account() -> None:
    op.execute(
        """
        INSERT INTO accounts (id, provider, status)
        VALUES ('877caec6-5092-5945-a268-544f5e78a40c', 'internal', 'ACTIVE')
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO portfolios (id, account_id, name)
        VALUES (
          'ee2ff673-eaf6-5076-bbab-f4c9cedfc74e',
          '877caec6-5092-5945-a268-544f5e78a40c',
          'NEXUS Internal USD Paper'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO paper_accounts (
          id, account_id, portfolio_id, code, currency, account_model,
          position_model, initial_capital, capability
        ) VALUES (
          'd31672c0-3922-5ebd-8fdb-cd3174266731',
          '877caec6-5092-5945-a268-544f5e78a40c',
          'ee2ff673-eaf6-5076-bbab-f4c9cedfc74e',
          'internal_usd_v1', 'USD', 'cash', 'long_only',
          10000.000000000000, 'simulation_write_internal'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO risk_policies (
          id, version, active, single_name_limit, sector_limit,
          minimum_cash_ratio, drawdown_lock_ratio, drawdown_recovery_ratio,
          minimum_buy_notional, buy_reserve_markup, commission,
          supported_market, supported_currency, supported_asset_types, content_hash
        ) VALUES (
          '2a3bf2d3-85c2-524b-abe3-d37112685caf',
          'risk_policy_v1', true,
          0.150000000000, 0.300000000000, 0.100000000000,
          0.080000000000, 0.060000000000, 100.000000000000,
          0.010000000000, 0.000000000000, 'US', 'USD',
          ARRAY['ETF','common_stock']::varchar[],
          '70f395ac13218d3e19b65e1717b3e2fdcbc31eaa5614ff60613f1c509dd7280b'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    ledger_accounts = (
        (
            "065e4d75-50c0-5ed3-b24c-723855334a58",
            "asset.cash.available.usd",
            "debit",
        ),
        (
            "96993de2-2715-5945-8e83-72128e4f69a8",
            "asset.cash.reserved.usd",
            "debit",
        ),
        (
            "8bdde654-ef47-5190-b90c-c97431ec4511",
            "asset.position_cost.usd",
            "debit",
        ),
        (
            "2865b289-4871-5d60-a4f7-5a219122f18c",
            "equity.contributed_capital.usd",
            "credit",
        ),
        (
            "c4fb3dba-a2f7-5e92-a19e-9cfa0c89866a",
            "income.realized_pnl.usd",
            "credit",
        ),
        (
            "3a8d685d-d828-5d74-ab07-81d5c0126cdf",
            "income.dividend.usd",
            "credit",
        ),
        (
            "cffafff0-9ca1-5bfc-ba8f-5b8df3545a79",
            "expense.fees.usd",
            "debit",
        ),
    )
    for account_id, code, normal_side in ledger_accounts:
        op.execute(
            f"""
            INSERT INTO paper_ledger_accounts (
              id, paper_account_id, code, normal_side, currency
            ) VALUES (
              '{account_id}', 'd31672c0-3922-5ebd-8fdb-cd3174266731',
              '{code}', '{normal_side}', 'USD'
            )
            ON CONFLICT (id) DO NOTHING
            """
        )
    op.execute(
        """
        INSERT INTO paper_ledger_transactions (
          id, account_id, order_id, transaction_type, reference_type,
          reference_hash, occurred_at, transaction_hash
        ) VALUES (
          '308bc9dc-e3b8-5208-ad1e-32ec2dbb4ffc',
          'd31672c0-3922-5ebd-8fdb-cd3174266731',
          NULL, 'funding', 'initial_funding',
          'eac2dd058f1d1a05c7cda6f21e176b06f084588f6a78c7d0dc38ec717e24625c',
          '2026-08-26T00:00:00Z',
          '92c32846d2a224e07ac7505a417b2eb286f401cd9765205a2d84c2e4c2256234'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO paper_ledger_entries (
          id, transaction_id, account_id, entry_seq, ledger_account_id,
          account_code, currency, debit_amount, credit_amount
        ) VALUES
        (
          'c5238982-ab47-5075-a9cc-cbc89a412f32',
          '308bc9dc-e3b8-5208-ad1e-32ec2dbb4ffc',
          'd31672c0-3922-5ebd-8fdb-cd3174266731',
          1, '065e4d75-50c0-5ed3-b24c-723855334a58',
          'asset.cash.available.usd', 'USD', 10000.000000000000, 0
        ),
        (
          'd0dc4f48-105f-5093-ad30-ca9b67cea084',
          '308bc9dc-e3b8-5208-ad1e-32ec2dbb4ffc',
          'd31672c0-3922-5ebd-8fdb-cd3174266731',
          2, '2865b289-4871-5d60-a4f7-5a219122f18c',
          'equity.contributed_capital.usd', 'USD', 0, 10000.000000000000
        )
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO paper_drawdown_lock_events (
          id, account_id, event_seq, from_state, to_state, drawdown_ratio,
          user_acknowledged, reason_code, occurred_at, payload_hash
        ) VALUES (
          'b5dfffb0-3552-577e-8cee-bc0d60d1b1bf',
          'd31672c0-3922-5ebd-8fdb-cd3174266731',
          1, NULL, 'unlocked', 0.000000000000, false,
          'PAPER_ACCOUNT_INITIALIZED', '2026-08-26T00:00:00Z',
          '54553c5d5273377b17a25f38289375f2c52061cb6c23beeee899bab98205caba'
        )
        ON CONFLICT (id) DO NOTHING
        """
    )


def upgrade() -> None:
    """Upgrade schema."""
    _create_identity_policy_and_decisions()
    _create_orders_and_risk()
    _create_ledger_and_reservations()
    _create_task19_contract_tables()
    _create_database_guards()
    _seed_internal_paper_account()


def downgrade() -> None:
    """Downgrade schema without mutating pre-existing account or portfolio roots."""
    op.execute("DROP TRIGGER trg_strategy_decision_targets_weights ON strategy_decision_targets")
    op.execute("DROP TRIGGER trg_strategy_decisions_weights ON strategy_decisions")
    op.execute("DROP FUNCTION nexus_strategy_decision_weights_complete()")
    op.execute("DROP TRIGGER trg_paper_drawdown_events_chain ON paper_drawdown_lock_events")
    op.execute("DROP FUNCTION nexus_paper_drawdown_chain_valid()")
    op.execute("DROP TRIGGER trg_paper_ledger_entries_balanced ON paper_ledger_entries")
    op.execute("DROP TRIGGER trg_paper_ledger_transactions_balanced ON paper_ledger_transactions")
    op.execute("DROP FUNCTION nexus_paper_ledger_transaction_balanced()")
    op.execute("DROP TRIGGER trg_paper_reservation_events_chain ON paper_reservation_events")
    op.execute("DROP FUNCTION nexus_paper_reservation_event_chain_valid()")
    op.execute("DROP TRIGGER trg_paper_order_events_chain ON paper_order_events")
    op.execute("DROP FUNCTION nexus_paper_order_event_chain_valid()")
    for table_name in reversed(_PAPER_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute("DROP FUNCTION nexus_reject_paper_fact_mutation()")
    for table_name in reversed(_PAPER_TABLES):
        op.drop_table(table_name)
