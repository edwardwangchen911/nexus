"""Add append-only proposal Shadow validation facts.

Revision ID: 0013_shadow
Revises: 0012_backtests
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013_shadow"
down_revision: str | Sequence[str] | None = "0012_backtests"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SHADOW_TABLES = (
    "shadow_groups",
    "shadow_portfolios",
    "shadow_advance_attempts",
    "shadow_session_authorities",
    "shadow_session_price_inputs",
    "shadow_session_event_inputs",
    "shadow_portfolio_states",
    "shadow_instrument_states",
    "shadow_receivable_states",
    "shadow_simulated_outcomes",
    "shadow_simulated_fills",
    "shadow_checkpoints",
    "shadow_checkpoint_metrics",
    "shadow_seals",
)

_SEALED_CHILD_TABLES = _SHADOW_TABLES[1:]

_LIFECYCLE_SHADOW_GUARD = """
IF OLD.status = 'compared' AND NEW.status = 'shadowing' AND (
  (SELECT count(*) FROM shadow_groups g WHERE g.proposal_group_id = OLD.id) <> 1
  OR (SELECT count(*) FROM shadow_portfolios p JOIN shadow_groups g
      ON g.id = p.shadow_group_id WHERE g.proposal_group_id = OLD.id) <> 3
  OR (SELECT array_agg(DISTINCT p.variant ORDER BY p.variant)
      FROM shadow_portfolios p JOIN shadow_groups g ON g.id = p.shadow_group_id
      WHERE g.proposal_group_id = OLD.id)
     IS DISTINCT FROM ARRAY['hold_current','minimum_change','strategy_target']::varchar[]
) THEN
  RAISE EXCEPTION 'PROPOSAL_SHADOW_INCOMPLETE' USING ERRCODE = '23514';
END IF;
IF OLD.status = 'shadowing' AND NEW.status = 'validated' AND (
  NOT EXISTS (
    SELECT 1 FROM shadow_seals s JOIN shadow_groups g ON g.id = s.shadow_group_id
    WHERE g.proposal_group_id = OLD.id AND s.reason = 'validated'
  )
  OR (SELECT count(*) FROM shadow_session_authorities a JOIN shadow_groups g
      ON g.id = a.shadow_group_id WHERE g.proposal_group_id = OLD.id) <> 63
  OR (SELECT count(*) FROM shadow_portfolio_states st JOIN shadow_groups g
      ON g.id = st.shadow_group_id WHERE g.proposal_group_id = OLD.id) <> 189
  OR (SELECT count(*) FROM shadow_checkpoints c JOIN shadow_groups g
      ON g.id = c.shadow_group_id WHERE g.proposal_group_id = OLD.id
      AND c.status = 'complete' AND c.ordinal IN (5,21,63)) <> 3
  OR (SELECT count(*) FROM shadow_checkpoint_metrics m JOIN shadow_groups g
      ON g.id = m.shadow_group_id WHERE g.proposal_group_id = OLD.id) <> 9
  OR EXISTS (
    SELECT 1 FROM shadow_simulated_outcomes o JOIN shadow_groups g
      ON g.id = o.shadow_group_id
    WHERE g.proposal_group_id = OLD.id AND o.status <> 'filled'
  )
  OR EXISTS (
    SELECT 1 FROM shadow_simulated_outcomes o JOIN shadow_groups g
      ON g.id = o.shadow_group_id
    WHERE g.proposal_group_id = OLD.id AND o.status = 'filled'
      AND NOT EXISTS (SELECT 1 FROM shadow_simulated_fills f WHERE f.outcome_id = o.id)
  )
  OR EXISTS (
    SELECT 1
    FROM shadow_portfolios sp
    JOIN shadow_groups sg ON sg.id = sp.shadow_group_id
    JOIN proposal_positions pp ON pp.proposal_id = sp.proposal_id
    WHERE sg.proposal_group_id = OLD.id
      AND CASE WHEN sp.variant = 'hold_current'
        THEN pp.current_weight ELSE pp.target_weight END > 0
      AND NOT EXISTS (
        SELECT 1 FROM shadow_simulated_outcomes o
        WHERE o.shadow_portfolio_id = sp.id
          AND o.instrument_id = pp.instrument_id
          AND o.status = 'filled'
      )
  )
) THEN
  RAISE EXCEPTION 'PROPOSAL_SHADOW_VALIDATION_INCOMPLETE' USING ERRCODE = '23514';
END IF;
IF OLD.status = 'shadowing' AND NEW.status IN ('rejected','expired') AND NOT EXISTS (
  SELECT 1 FROM shadow_seals s JOIN shadow_groups g ON g.id = s.shadow_group_id
  WHERE g.proposal_group_id = OLD.id AND s.reason = NEW.status
) THEN
  RAISE EXCEPTION 'PROPOSAL_SHADOW_SEAL_REQUIRED' USING ERRCODE = '23514';
END IF;
""".strip()


def _create_tables() -> None:
    op.execute(
        """
        CREATE TABLE shadow_groups (
          id uuid NOT NULL,
          proposal_group_id uuid NOT NULL,
          baseline_portfolio_snapshot_id uuid NOT NULL,
          proposal_bundle_hash varchar(64) NOT NULL,
          source_policy_id uuid NOT NULL,
          cost_policy_id uuid NOT NULL,
          calendar_authority_id uuid NOT NULL,
          calendar_version varchar(100) NOT NULL,
          calendar_hash varchar(64) NOT NULL,
          signal_session date NOT NULL,
          started_at timestamptz NOT NULL,
          start_session date NOT NULL,
          evaluation_end_session date NOT NULL,
          grace_end_session date NOT NULL,
          evaluation_sessions date[] NOT NULL,
          grace_sessions date[] NOT NULL,
          initial_nav numeric(38,12) NOT NULL,
          schema_version varchar(32) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_groups PRIMARY KEY (id),
          CONSTRAINT uq_shadow_groups_proposal_group UNIQUE (proposal_group_id),
          CONSTRAINT uq_shadow_groups_id_proposal_group UNIQUE (id, proposal_group_id),
          CONSTRAINT uq_shadow_groups_authority_binding UNIQUE
            (id, calendar_authority_id, source_policy_id, cost_policy_id),
          CONSTRAINT fk_shadow_groups_proposal_group FOREIGN KEY (proposal_group_id)
            REFERENCES optimization_proposal_groups(id),
          CONSTRAINT fk_shadow_groups_baseline_snapshot
            FOREIGN KEY (baseline_portfolio_snapshot_id) REFERENCES portfolio_snapshots(id),
          CONSTRAINT fk_shadow_groups_source_policy FOREIGN KEY (source_policy_id)
            REFERENCES source_policies(id),
          CONSTRAINT fk_shadow_groups_cost_policy FOREIGN KEY (cost_policy_id)
            REFERENCES cost_policies(id),
          CONSTRAINT fk_shadow_groups_calendar_authority FOREIGN KEY (calendar_authority_id)
            REFERENCES exchange_calendar_authorities(id),
          CONSTRAINT ck_shadow_groups_proposal_bundle_hash_canonical
            CHECK (proposal_bundle_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_groups_calendar_hash_canonical
            CHECK (calendar_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_groups_evaluation_session_count
            CHECK (cardinality(evaluation_sessions) = 63),
          CONSTRAINT ck_shadow_groups_grace_session_count
            CHECK (cardinality(grace_sessions) = 5),
          CONSTRAINT ck_shadow_groups_session_arrays_nonnull CHECK (
            array_position(evaluation_sessions, NULL) IS NULL
            AND array_position(grace_sessions, NULL) IS NULL
          ),
          CONSTRAINT ck_shadow_groups_session_arrays_ordered CHECK (
            nexus_date_array_strictly_increasing(evaluation_sessions)
            AND nexus_date_array_strictly_increasing(grace_sessions)
          ),
          CONSTRAINT ck_shadow_groups_session_boundaries_bound CHECK (
            evaluation_sessions[1] = start_session
            AND evaluation_sessions[63] = evaluation_end_session
            AND grace_sessions[1] > evaluation_end_session
            AND grace_sessions[5] = grace_end_session
          ),
          CONSTRAINT ck_shadow_groups_session_boundaries_ordered CHECK (
            signal_session < start_session
            AND start_session <= evaluation_end_session
            AND evaluation_end_session < grace_end_session
          ),
          CONSTRAINT ck_shadow_groups_initial_nav_fixed
            CHECK (initial_nav = 100.000000000000),
          CONSTRAINT ck_shadow_groups_schema_version_v1
            CHECK (schema_version = 'proposal_shadow_v1')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_portfolios (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          proposal_group_id uuid NOT NULL,
          proposal_id uuid NOT NULL,
          variant varchar(30) NOT NULL,
          target_weights_hash varchar(64) NOT NULL,
          initial_nav numeric(38,12) NOT NULL,
          initial_cash numeric(38,12) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_portfolios PRIMARY KEY (id),
          CONSTRAINT uq_shadow_portfolios_group_variant UNIQUE (shadow_group_id, variant),
          CONSTRAINT uq_shadow_portfolios_proposal UNIQUE (proposal_id),
          CONSTRAINT uq_shadow_portfolios_id_group UNIQUE (id, shadow_group_id),
          CONSTRAINT fk_shadow_portfolios_group_root
            FOREIGN KEY (shadow_group_id, proposal_group_id)
            REFERENCES shadow_groups(id, proposal_group_id),
          CONSTRAINT fk_shadow_portfolios_proposal_root
            FOREIGN KEY (proposal_id, proposal_group_id, variant)
            REFERENCES optimization_proposals(id, proposal_group_id, variant),
          CONSTRAINT ck_shadow_portfolios_variant_allowed CHECK (
            variant IN ('hold_current','minimum_change','strategy_target')
          ),
          CONSTRAINT ck_shadow_portfolios_initial_anchor_fixed CHECK (
            initial_nav = 100.000000000000 AND initial_cash = 100.000000000000
          ),
          CONSTRAINT ck_shadow_portfolios_target_weights_hash_canonical
            CHECK (target_weights_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_advance_attempts (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          requested_session date NOT NULL,
          expected_session date,
          observed_at timestamptz NOT NULL,
          result varchar(32) NOT NULL,
          reason_code varchar(100) NOT NULL,
          authority_hash varchar(64),
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_advance_attempts PRIMARY KEY (id),
          CONSTRAINT fk_shadow_advance_attempts_group FOREIGN KEY (shadow_group_id)
            REFERENCES shadow_groups(id),
          CONSTRAINT ck_shadow_advance_attempts_result_allowed CHECK (
            result IN ('applied','data_partial','data_stale','source_conflict','session_gap')
          ),
          CONSTRAINT ck_shadow_advance_attempts_authority_hash_canonical CHECK (
            authority_hash IS NULL OR authority_hash ~ '^[0-9a-f]{64}$'
          ),
          CONSTRAINT ck_shadow_advance_attempts_reason_code_nonempty
            CHECK (btrim(reason_code) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_session_authorities (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          session date NOT NULL,
          ordinal integer NOT NULL,
          calendar_authority_id uuid NOT NULL,
          source_policy_id uuid NOT NULL,
          cost_policy_id uuid NOT NULL,
          content_hash varchar(64) NOT NULL,
          available_at timestamptz NOT NULL,
          observed_at timestamptz NOT NULL,
          late_backfill boolean NOT NULL,
          query_hash varchar(64) NOT NULL,
          source_summary jsonb NOT NULL,
          delivery_mode_summary jsonb NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_session_authorities PRIMARY KEY (id),
          CONSTRAINT uq_shadow_authorities_group_session UNIQUE (shadow_group_id, session),
          CONSTRAINT uq_shadow_authorities_group_ordinal UNIQUE (shadow_group_id, ordinal),
          CONSTRAINT uq_shadow_authorities_state_root
            UNIQUE (id, shadow_group_id, session, ordinal),
          CONSTRAINT uq_shadow_authorities_id_group UNIQUE (id, shadow_group_id),
          CONSTRAINT fk_shadow_authorities_group_binding
            FOREIGN KEY (shadow_group_id, calendar_authority_id, source_policy_id, cost_policy_id)
            REFERENCES shadow_groups
              (id, calendar_authority_id, source_policy_id, cost_policy_id),
          CONSTRAINT ck_shadow_session_authorities_ordinal_bounded
            CHECK (ordinal BETWEEN 1 AND 63),
          CONSTRAINT ck_shadow_session_authorities_content_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_session_authorities_query_hash_canonical
            CHECK (query_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_session_authorities_available_before_observed
            CHECK (available_at <= observed_at),
          CONSTRAINT ck_shadow_session_authorities_summaries_are_objects CHECK (
            jsonb_typeof(source_summary) = 'object'
            AND jsonb_typeof(delivery_mode_summary) = 'object'
          )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_session_price_inputs (
          id uuid NOT NULL,
          session_authority_id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          price_bar_record_id uuid NOT NULL,
          price_bar_set_authority_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          usage varchar(32) NOT NULL,
          source varchar(100) NOT NULL,
          delivery_mode varchar(20) NOT NULL,
          revision varchar(255) NOT NULL,
          available_at timestamptz NOT NULL,
          content_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_session_price_inputs PRIMARY KEY (id),
          CONSTRAINT uq_shadow_price_inputs_session_instrument
            UNIQUE (session_authority_id, instrument_id),
          CONSTRAINT fk_shadow_price_inputs_session_root
            FOREIGN KEY (session_authority_id, shadow_group_id)
            REFERENCES shadow_session_authorities(id, shadow_group_id),
          CONSTRAINT fk_shadow_price_inputs_bar FOREIGN KEY (price_bar_record_id)
            REFERENCES price_bar_records(id),
          CONSTRAINT fk_shadow_price_inputs_bar_set FOREIGN KEY (price_bar_set_authority_id)
            REFERENCES price_bar_set_authorities(id),
          CONSTRAINT fk_shadow_price_inputs_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_shadow_session_price_inputs_usage_allowed
            CHECK (usage IN ('execution_and_mark','mark')),
          CONSTRAINT ck_shadow_session_price_inputs_delivery_mode_allowed CHECK (
            delivery_mode IN ('real_time','delayed','fixture','unknown')
          ),
          CONSTRAINT ck_shadow_session_price_inputs_content_hash_canonical
            CHECK (content_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_session_price_inputs_identity_nonempty
            CHECK (btrim(source) <> '' AND btrim(revision) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_session_event_inputs (
          id uuid NOT NULL,
          session_authority_id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          corporate_action_id uuid NOT NULL,
          event_kind varchar(20) NOT NULL,
          provider_event_id varchar(255) NOT NULL,
          revision varchar(255) NOT NULL,
          available_at timestamptz NOT NULL,
          evidence_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_session_event_inputs PRIMARY KEY (id),
          CONSTRAINT uq_shadow_event_inputs_session_action
            UNIQUE (session_authority_id, corporate_action_id),
          CONSTRAINT fk_shadow_event_inputs_session_root
            FOREIGN KEY (session_authority_id, shadow_group_id)
            REFERENCES shadow_session_authorities(id, shadow_group_id),
          CONSTRAINT fk_shadow_event_inputs_action FOREIGN KEY (corporate_action_id)
            REFERENCES corporate_actions(id),
          CONSTRAINT ck_shadow_session_event_inputs_event_kind_allowed
            CHECK (event_kind IN ('split','dividend','delisting')),
          CONSTRAINT ck_shadow_session_event_inputs_evidence_hash_canonical
            CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_shadow_session_event_inputs_identity_nonempty
            CHECK (btrim(provider_event_id) <> '' AND btrim(revision) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_portfolio_states (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          session_authority_id uuid NOT NULL,
          session date NOT NULL,
          ordinal integer NOT NULL,
          nav numeric(38,12) NOT NULL,
          cash numeric(38,12) NOT NULL,
          cumulative_traded_notional numeric(38,12) NOT NULL,
          cumulative_fees numeric(38,12) NOT NULL,
          cumulative_realized_pnl numeric(38,12) NOT NULL,
          state_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_portfolio_states PRIMARY KEY (id),
          CONSTRAINT uq_shadow_states_portfolio_session
            UNIQUE (shadow_portfolio_id, session),
          CONSTRAINT uq_shadow_states_portfolio_ordinal
            UNIQUE (shadow_portfolio_id, ordinal),
          CONSTRAINT uq_shadow_states_ledger_root
            UNIQUE (id, shadow_portfolio_id, shadow_group_id),
          CONSTRAINT fk_shadow_states_portfolio_root
            FOREIGN KEY (shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_portfolios(id, shadow_group_id),
          CONSTRAINT fk_shadow_states_session_root
            FOREIGN KEY (session_authority_id, shadow_group_id, session, ordinal)
            REFERENCES shadow_session_authorities(id, shadow_group_id, session, ordinal),
          CONSTRAINT ck_shadow_portfolio_states_ordinal_bounded
            CHECK (ordinal BETWEEN 1 AND 63),
          CONSTRAINT ck_shadow_portfolio_states_nav_cash_nonnegative
            CHECK (nav >= 0 AND cash >= 0),
          CONSTRAINT ck_shadow_portfolio_states_cumulative_costs_nonnegative
            CHECK (cumulative_traded_notional >= 0 AND cumulative_fees >= 0),
          CONSTRAINT ck_shadow_portfolio_states_state_hash_canonical
            CHECK (state_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_instrument_states (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          portfolio_state_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          quantity numeric(38,12) NOT NULL,
          cost_basis numeric(38,12) NOT NULL,
          realized_pnl numeric(38,12) NOT NULL,
          cumulative_cost numeric(38,12) NOT NULL,
          last_close numeric(38,12),
          untradable boolean NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_instrument_states PRIMARY KEY (id),
          CONSTRAINT uq_shadow_instruments_state_instrument
            UNIQUE (portfolio_state_id, instrument_id),
          CONSTRAINT fk_shadow_instruments_state_root
            FOREIGN KEY (portfolio_state_id, shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_portfolio_states(id, shadow_portfolio_id, shadow_group_id),
          CONSTRAINT fk_shadow_instruments_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_shadow_instrument_states_amounts_nonnegative
            CHECK (quantity >= 0 AND cost_basis >= 0 AND cumulative_cost >= 0),
          CONSTRAINT ck_shadow_instrument_states_last_close_positive
            CHECK (last_close IS NULL OR last_close > 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_receivable_states (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          portfolio_state_id uuid NOT NULL,
          corporate_action_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          amount numeric(38,12) NOT NULL,
          payable_session date NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_receivable_states PRIMARY KEY (id),
          CONSTRAINT uq_shadow_receivables_state_action
            UNIQUE (portfolio_state_id, corporate_action_id, instrument_id, payable_session),
          CONSTRAINT fk_shadow_receivables_state_root
            FOREIGN KEY (portfolio_state_id, shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_portfolio_states(id, shadow_portfolio_id, shadow_group_id),
          CONSTRAINT fk_shadow_receivables_action FOREIGN KEY (corporate_action_id)
            REFERENCES corporate_actions(id),
          CONSTRAINT fk_shadow_receivables_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_shadow_receivable_states_amount_nonnegative CHECK (amount >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_simulated_outcomes (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          session_authority_id uuid NOT NULL,
          instrument_id uuid NOT NULL,
          signal_session date NOT NULL,
          target_session date NOT NULL,
          ordinal integer NOT NULL,
          side varchar(16) NOT NULL,
          status varchar(16) NOT NULL,
          reason varchar(255) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_simulated_outcomes PRIMARY KEY (id),
          CONSTRAINT uq_shadow_outcomes_portfolio_instrument
            UNIQUE (shadow_portfolio_id, instrument_id),
          CONSTRAINT uq_shadow_outcomes_fill_root
            UNIQUE (id, shadow_portfolio_id, shadow_group_id),
          CONSTRAINT fk_shadow_outcomes_portfolio_root
            FOREIGN KEY (shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_portfolios(id, shadow_group_id),
          CONSTRAINT fk_shadow_outcomes_session_root
            FOREIGN KEY (session_authority_id, shadow_group_id, target_session, ordinal)
            REFERENCES shadow_session_authorities(id, shadow_group_id, session, ordinal),
          CONSTRAINT fk_shadow_outcomes_instrument FOREIGN KEY (instrument_id)
            REFERENCES instruments(id),
          CONSTRAINT ck_shadow_simulated_outcomes_first_session_only CHECK (ordinal = 1),
          CONSTRAINT ck_shadow_simulated_outcomes_target_after_signal
            CHECK (signal_session < target_session),
          CONSTRAINT ck_shadow_simulated_outcomes_side_allowed
            CHECK (side IN ('buy','sell','rebalance')),
          CONSTRAINT ck_shadow_simulated_outcomes_status_allowed
            CHECK (status IN ('filled','missing_bar','untradable')),
          CONSTRAINT ck_shadow_simulated_outcomes_reason_nonempty CHECK (btrim(reason) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_simulated_fills (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          outcome_id uuid NOT NULL,
          side varchar(4) NOT NULL,
          quantity numeric(38,12) NOT NULL,
          price numeric(38,12) NOT NULL,
          notional numeric(38,12) NOT NULL,
          cost numeric(38,12) NOT NULL,
          execution_price_kind varchar(16) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_simulated_fills PRIMARY KEY (id),
          CONSTRAINT uq_shadow_fills_outcome UNIQUE (outcome_id),
          CONSTRAINT fk_shadow_fills_outcome_root
            FOREIGN KEY (outcome_id, shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_simulated_outcomes(id, shadow_portfolio_id, shadow_group_id),
          CONSTRAINT ck_shadow_simulated_fills_side_allowed CHECK (side IN ('buy','sell')),
          CONSTRAINT ck_shadow_simulated_fills_fill_amounts_valid
            CHECK (quantity > 0 AND price > 0 AND notional > 0 AND cost >= 0),
          CONSTRAINT ck_shadow_simulated_fills_execution_price_open
            CHECK (execution_price_kind = 'open')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_checkpoints (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          session_authority_id uuid,
          session date NOT NULL,
          ordinal integer NOT NULL,
          status varchar(16) NOT NULL,
          coverage_count integer NOT NULL,
          reason varchar(100),
          metrics_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_checkpoints PRIMARY KEY (id),
          CONSTRAINT uq_shadow_checkpoints_group_ordinal UNIQUE (shadow_group_id, ordinal),
          CONSTRAINT uq_shadow_checkpoints_id_group UNIQUE (id, shadow_group_id),
          CONSTRAINT fk_shadow_checkpoints_group FOREIGN KEY (shadow_group_id)
            REFERENCES shadow_groups(id),
          CONSTRAINT fk_shadow_checkpoints_session_root
            FOREIGN KEY (session_authority_id, shadow_group_id, session, ordinal)
            REFERENCES shadow_session_authorities(id, shadow_group_id, session, ordinal),
          CONSTRAINT ck_shadow_checkpoints_ordinal_allowed CHECK (ordinal IN (5,21,63)),
          CONSTRAINT ck_shadow_checkpoints_status_allowed
            CHECK (status IN ('complete','incomplete')),
          CONSTRAINT ck_shadow_checkpoints_coverage_bounded
            CHECK (coverage_count BETWEEN 0 AND ordinal),
          CONSTRAINT ck_shadow_checkpoints_status_binding CHECK (
            (status = 'complete' AND session_authority_id IS NOT NULL
              AND coverage_count = ordinal AND reason IS NULL)
            OR (status = 'incomplete' AND session_authority_id IS NULL
              AND reason IS NOT NULL AND btrim(reason) <> '')
          ),
          CONSTRAINT ck_shadow_checkpoints_metrics_hash_canonical
            CHECK (metrics_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_checkpoint_metrics (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          checkpoint_id uuid NOT NULL,
          shadow_portfolio_id uuid NOT NULL,
          variant varchar(30) NOT NULL,
          metric_status varchar(16) NOT NULL,
          unknown_reason varchar(100),
          net_cumulative_return numeric(38,12),
          return_delta_vs_hold numeric(38,12),
          max_drawdown numeric(38,12),
          drawdown_improvement numeric(38,12),
          annualized_volatility numeric(38,12),
          volatility_improvement numeric(38,12),
          cumulative_turnover numeric(38,12),
          fee_drag_index_points numeric(38,12),
          worst_sessions jsonb,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_checkpoint_metrics PRIMARY KEY (id),
          CONSTRAINT uq_shadow_metrics_checkpoint_variant UNIQUE (checkpoint_id, variant),
          CONSTRAINT uq_shadow_metrics_checkpoint_portfolio
            UNIQUE (checkpoint_id, shadow_portfolio_id),
          CONSTRAINT fk_shadow_metrics_checkpoint_root
            FOREIGN KEY (checkpoint_id, shadow_group_id)
            REFERENCES shadow_checkpoints(id, shadow_group_id),
          CONSTRAINT fk_shadow_metrics_portfolio_root
            FOREIGN KEY (shadow_portfolio_id, shadow_group_id)
            REFERENCES shadow_portfolios(id, shadow_group_id),
          CONSTRAINT ck_shadow_checkpoint_metrics_variant_allowed CHECK (
            variant IN ('hold_current','minimum_change','strategy_target')
          ),
          CONSTRAINT ck_shadow_checkpoint_metrics_status_allowed
            CHECK (metric_status IN ('known','unknown')),
          CONSTRAINT ck_shadow_checkpoint_metrics_known_unknown_binding CHECK (
            (metric_status = 'unknown' AND unknown_reason IS NOT NULL
              AND btrim(unknown_reason) <> ''
              AND net_cumulative_return IS NULL AND return_delta_vs_hold IS NULL
              AND max_drawdown IS NULL AND drawdown_improvement IS NULL
              AND annualized_volatility IS NULL AND volatility_improvement IS NULL
              AND cumulative_turnover IS NULL AND fee_drag_index_points IS NULL
              AND worst_sessions IS NULL)
            OR (metric_status = 'known' AND unknown_reason IS NULL
              AND net_cumulative_return IS NOT NULL AND return_delta_vs_hold IS NOT NULL
              AND max_drawdown IS NOT NULL AND drawdown_improvement IS NOT NULL
              AND annualized_volatility IS NOT NULL AND volatility_improvement IS NOT NULL
              AND cumulative_turnover IS NOT NULL AND fee_drag_index_points IS NOT NULL
              AND jsonb_typeof(worst_sessions) = 'array')
          ),
          CONSTRAINT ck_shadow_checkpoint_metrics_drawdown_bounded
            CHECK (max_drawdown IS NULL OR max_drawdown BETWEEN -1 AND 0),
          CONSTRAINT ck_shadow_checkpoint_metrics_volatility_nonnegative
            CHECK (annualized_volatility IS NULL OR annualized_volatility >= 0),
          CONSTRAINT ck_shadow_checkpoint_metrics_turnover_nonnegative
            CHECK (cumulative_turnover IS NULL OR cumulative_turnover >= 0),
          CONSTRAINT ck_shadow_checkpoint_metrics_fees_nonnegative
            CHECK (fee_drag_index_points IS NULL OR fee_drag_index_points >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shadow_seals (
          id uuid NOT NULL,
          shadow_group_id uuid NOT NULL,
          reason varchar(16) NOT NULL,
          reason_code varchar(100) NOT NULL,
          sealed_at timestamptz NOT NULL,
          final_coverage integer NOT NULL,
          final_authority_hash varchar(64) NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_shadow_seals PRIMARY KEY (id),
          CONSTRAINT uq_shadow_seals_group UNIQUE (shadow_group_id),
          CONSTRAINT fk_shadow_seals_group FOREIGN KEY (shadow_group_id)
            REFERENCES shadow_groups(id),
          CONSTRAINT ck_shadow_seals_reason_allowed
            CHECK (reason IN ('validated','rejected','expired')),
          CONSTRAINT ck_shadow_seals_reason_code_binding CHECK (
            (reason = 'validated' AND reason_code = 'SHADOW_VALIDATED')
            OR (reason = 'rejected' AND reason_code = 'USER_REJECTED')
            OR (reason = 'expired' AND reason_code IN
              ('SHADOW_DATA_INCOMPLETE','SHADOW_INITIAL_EXECUTION_INCOMPLETE'))
          ),
          CONSTRAINT ck_shadow_seals_reason_code_nonempty CHECK (btrim(reason_code) <> ''),
          CONSTRAINT ck_shadow_seals_coverage_bounded CHECK (final_coverage BETWEEN 0 AND 63),
          CONSTRAINT ck_shadow_seals_final_authority_hash_canonical
            CHECK (final_authority_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )


def _create_database_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_reject_shadow_fact_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RAISE EXCEPTION 'shadow fact is append-only' USING ERRCODE = '23514';
        END;
        $$
        """
    )
    for table_name in _SHADOW_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_reject_shadow_fact_mutation()"
        )

    op.execute(
        """
        CREATE FUNCTION nexus_shadow_unsealed()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          outcome_status text;
          outcome_side text;
        BEGIN
          PERFORM id FROM shadow_groups WHERE id = NEW.shadow_group_id FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'shadow root is missing' USING ERRCODE = '23503';
          END IF;
          IF EXISTS (
            SELECT 1 FROM shadow_seals WHERE shadow_group_id = NEW.shadow_group_id
          ) THEN
            RAISE EXCEPTION 'shadow is sealed' USING ERRCODE = '23514';
          END IF;
          IF TG_TABLE_NAME = 'shadow_simulated_fills' THEN
            SELECT status, side INTO outcome_status, outcome_side
            FROM shadow_simulated_outcomes
            WHERE id = NEW.outcome_id
              AND shadow_group_id = NEW.shadow_group_id
              AND shadow_portfolio_id = NEW.shadow_portfolio_id;
            IF outcome_status IS DISTINCT FROM 'filled'
               OR outcome_side IS DISTINCT FROM NEW.side THEN
              RAISE EXCEPTION 'shadow fill must match a filled outcome'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    for table_name in _SEALED_CHILD_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_unsealed_insert "
            f"BEFORE INSERT ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_shadow_unsealed()"
        )

    op.execute(
        """
        CREATE FUNCTION nexus_shadow_group_complete()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          group_ref uuid;
          authority_ref uuid;
          portfolio_count integer;
          state_count integer;
          variants varchar[];
        BEGIN
          IF TG_TABLE_NAME = 'shadow_groups' THEN
            group_ref := NEW.id;
          ELSIF TG_TABLE_NAME = 'shadow_portfolios' THEN
            group_ref := NEW.shadow_group_id;
          ELSIF TG_TABLE_NAME = 'shadow_session_authorities' THEN
            group_ref := NEW.shadow_group_id;
            authority_ref := NEW.id;
          ELSE
            group_ref := NEW.shadow_group_id;
            authority_ref := NEW.session_authority_id;
          END IF;

          IF TG_TABLE_NAME IN ('shadow_groups','shadow_portfolios') THEN
            SELECT count(*), array_agg(DISTINCT variant ORDER BY variant)
              INTO portfolio_count, variants
            FROM shadow_portfolios WHERE shadow_group_id = group_ref;
            IF portfolio_count <> 3 OR variants IS DISTINCT FROM
              ARRAY['hold_current','minimum_change','strategy_target']::varchar[] THEN
              RAISE EXCEPTION 'shadow group requires exact fixed variants'
                USING ERRCODE = '23514';
            END IF;
            IF NOT EXISTS (
              SELECT 1
              FROM shadow_groups g
              JOIN optimization_proposal_groups pg ON pg.id = g.proposal_group_id
              JOIN exchange_calendar_authorities ca ON ca.id = g.calendar_authority_id
              WHERE g.id = group_ref
                AND pg.baseline_portfolio_snapshot_id = g.baseline_portfolio_snapshot_id
                AND pg.source_policy_id = g.source_policy_id
                AND pg.cost_policy_id = g.cost_policy_id
                AND pg.status IN ('shadowing','validated','rejected','expired')
                AND ca.version = g.calendar_version
                AND ca.content_hash = g.calendar_hash
                AND ca.completeness_status = 'complete'
                AND ca.range_start <= g.start_session
                AND ca.range_end >= g.grace_end_session
            ) THEN
              RAISE EXCEPTION 'shadow group authority root is invalid'
                USING ERRCODE = '23514';
            END IF;
            IF (
              SELECT count(*)
              FROM exchange_calendar_sessions cs
              JOIN shadow_groups g ON g.calendar_authority_id = cs.calendar_authority_id
              WHERE g.id = group_ref
                AND cs.session = ANY(g.evaluation_sessions || g.grace_sessions)
            ) <> 68 THEN
              RAISE EXCEPTION 'shadow group calendar members are incomplete'
                USING ERRCODE = '23514';
            END IF;
            IF (
              SELECT count(*)
              FROM shadow_portfolios sp
              JOIN shadow_groups g ON g.id = sp.shadow_group_id
              JOIN optimization_proposals p ON p.id = sp.proposal_id
              WHERE sp.shadow_group_id = group_ref
                AND p.proposal_group_id = g.proposal_group_id
                AND p.variant = sp.variant
            ) <> 3 THEN
              RAISE EXCEPTION 'shadow portfolio proposal ownership is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            IF NOT EXISTS (
              SELECT 1
              FROM shadow_session_authorities a
              JOIN shadow_groups g ON g.id = a.shadow_group_id
              WHERE a.id = authority_ref
                AND a.shadow_group_id = group_ref
                AND g.evaluation_sessions[a.ordinal] = a.session
            ) THEN
              RAISE EXCEPTION 'shadow session is outside the frozen sequence'
                USING ERRCODE = '23514';
            END IF;
            SELECT count(*), array_agg(DISTINCT p.variant ORDER BY p.variant)
              INTO state_count, variants
            FROM shadow_portfolio_states s
            JOIN shadow_portfolios p ON p.id = s.shadow_portfolio_id
            WHERE s.session_authority_id = authority_ref
              AND s.shadow_group_id = group_ref;
            IF state_count <> 3 OR variants IS DISTINCT FROM
              ARRAY['hold_current','minimum_change','strategy_target']::varchar[] THEN
              RAISE EXCEPTION 'shadow session requires exact portfolio states'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_groups_complete "
        "AFTER INSERT ON shadow_groups DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_group_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_portfolios_group_complete "
        "AFTER INSERT ON shadow_portfolios DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_group_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_authorities_states_complete "
        "AFTER INSERT ON shadow_session_authorities DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_group_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_states_session_complete "
        "AFTER INSERT ON shadow_portfolio_states DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_group_complete()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_shadow_checkpoint_complete()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          checkpoint_ref uuid;
          checkpoint_status text;
          metric_count integer;
          variants varchar[];
          status_count integer;
        BEGIN
          IF TG_TABLE_NAME = 'shadow_checkpoints' THEN
            checkpoint_ref := NEW.id;
          ELSE
            checkpoint_ref := NEW.checkpoint_id;
          END IF;
          SELECT status INTO checkpoint_status
          FROM shadow_checkpoints WHERE id = checkpoint_ref;
          SELECT count(*), array_agg(DISTINCT m.variant ORDER BY m.variant),
                 count(*) FILTER (
                   WHERE (checkpoint_status = 'complete' AND m.metric_status = 'known')
                      OR (checkpoint_status = 'incomplete' AND m.metric_status = 'unknown')
                 )
            INTO metric_count, variants, status_count
          FROM shadow_checkpoint_metrics m
          JOIN shadow_checkpoints c ON c.id = m.checkpoint_id
          JOIN shadow_portfolios p ON p.id = m.shadow_portfolio_id
          WHERE m.checkpoint_id = checkpoint_ref
            AND m.shadow_group_id = c.shadow_group_id
            AND p.shadow_group_id = c.shadow_group_id
            AND p.variant = m.variant;
          IF metric_count <> 3 OR status_count <> 3 OR variants IS DISTINCT FROM
            ARRAY['hold_current','minimum_change','strategy_target']::varchar[] THEN
            RAISE EXCEPTION 'shadow checkpoint requires exact variant metrics'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_checkpoints_metrics_complete "
        "AFTER INSERT ON shadow_checkpoints DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_checkpoint_complete()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_metrics_checkpoint_complete "
        "AFTER INSERT ON shadow_checkpoint_metrics DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_checkpoint_complete()"
    )

    op.execute(
        """
        CREATE FUNCTION nexus_shadow_seal_matches_lifecycle()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          proposal_status text;
          actual_coverage integer;
          grace_decision_at timestamptz;
        BEGIN
          SELECT pg.status INTO proposal_status
          FROM shadow_groups g
          JOIN optimization_proposal_groups pg ON pg.id = g.proposal_group_id
          WHERE g.id = NEW.shadow_group_id;
          IF proposal_status IS DISTINCT FROM NEW.reason THEN
            RAISE EXCEPTION 'shadow seal does not match proposal lifecycle'
              USING ERRCODE = '23514';
          END IF;
          SELECT count(*) INTO actual_coverage
          FROM shadow_session_authorities
          WHERE shadow_group_id = NEW.shadow_group_id;
          IF NEW.final_coverage <> actual_coverage THEN
            RAISE EXCEPTION 'shadow seal coverage does not match authorities'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.reason = 'validated' AND NEW.final_coverage <> 63 THEN
            RAISE EXCEPTION 'validated shadow seal requires full coverage'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.reason = 'expired'
             AND NEW.reason_code = 'SHADOW_INITIAL_EXECUTION_INCOMPLETE'
             AND NEW.final_coverage <> 63 THEN
            RAISE EXCEPTION 'initial-execution seal requires full coverage'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.reason = 'expired' AND NEW.reason_code = 'SHADOW_DATA_INCOMPLETE' THEN
            SELECT cs.decision_at INTO grace_decision_at
            FROM shadow_groups g
            JOIN exchange_calendar_sessions cs
              ON cs.calendar_authority_id = g.calendar_authority_id
             AND cs.session = g.grace_end_session
            WHERE g.id = NEW.shadow_group_id;
            IF grace_decision_at IS NULL OR NEW.sealed_at < grace_decision_at THEN
              RAISE EXCEPTION 'shadow data-incomplete seal precedes grace deadline'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_shadow_seals_lifecycle "
        "AFTER INSERT ON shadow_seals DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_shadow_seal_matches_lifecycle()"
    )


def _replace_lifecycle_with_shadow_guard() -> None:
    op.execute(
        f"""
        DO $migration$
        DECLARE
          function_definition text;
          shadow_guard text := $shadow_guard${_LIFECYCLE_SHADOW_GUARD}$shadow_guard$;
        BEGIN
          SELECT pg_get_functiondef(
            'nexus_optimization_group_lifecycle()'::regprocedure
          ) INTO function_definition;
          IF position('shadow_groups' IN function_definition) <> 0
             OR position('RETURN NEW; END;' IN function_definition) = 0 THEN
            RAISE EXCEPTION 'unexpected pre-shadow lifecycle function';
          END IF;
          EXECUTE replace(
            function_definition,
            'RETURN NEW; END;',
            shadow_guard || ' RETURN NEW; END;'
          );
        END;
        $migration$
        """
    )


def _restore_pre_shadow_lifecycle() -> None:
    op.execute(
        f"""
        DO $migration$
        DECLARE
          function_definition text;
          shadow_guard text := $shadow_guard${_LIFECYCLE_SHADOW_GUARD}$shadow_guard$;
        BEGIN
          SELECT pg_get_functiondef(
            'nexus_optimization_group_lifecycle()'::regprocedure
          ) INTO function_definition;
          IF position(shadow_guard IN function_definition) = 0 THEN
            RAISE EXCEPTION 'unexpected shadow lifecycle function';
          END IF;
          EXECUTE replace(
            function_definition,
            shadow_guard || ' RETURN NEW; END;',
            'RETURN NEW; END;'
          );
        END;
        $migration$
        """
    )


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        CREATE FUNCTION nexus_date_array_strictly_increasing(date_values date[])
        RETURNS boolean
        LANGUAGE sql
        IMMUTABLE
        STRICT
        AS $$
          SELECT COALESCE(
              bool_and(date_values[index_value] < date_values[index_value + 1]),
            true
          )
          FROM generate_subscripts(date_values, 1) AS index_value
          WHERE index_value < cardinality(date_values)
        $$
        """
    )
    op.create_unique_constraint(
        "uq_optimization_proposals_shadow_root",
        "optimization_proposals",
        ["id", "proposal_group_id", "variant"],
    )
    _create_tables()
    _create_database_guards()
    _replace_lifecycle_with_shadow_guard()


def downgrade() -> None:
    """Downgrade schema."""
    _restore_pre_shadow_lifecycle()

    op.execute("DROP TRIGGER trg_shadow_seals_lifecycle ON shadow_seals")
    op.execute("DROP FUNCTION nexus_shadow_seal_matches_lifecycle()")
    op.execute(
        "DROP TRIGGER trg_shadow_metrics_checkpoint_complete "
        "ON shadow_checkpoint_metrics"
    )
    op.execute(
        "DROP TRIGGER trg_shadow_checkpoints_metrics_complete ON shadow_checkpoints"
    )
    op.execute("DROP FUNCTION nexus_shadow_checkpoint_complete()")
    op.execute("DROP TRIGGER trg_shadow_states_session_complete ON shadow_portfolio_states")
    op.execute(
        "DROP TRIGGER trg_shadow_authorities_states_complete "
        "ON shadow_session_authorities"
    )
    op.execute("DROP TRIGGER trg_shadow_portfolios_group_complete ON shadow_portfolios")
    op.execute("DROP TRIGGER trg_shadow_groups_complete ON shadow_groups")
    op.execute("DROP FUNCTION nexus_shadow_group_complete()")

    for table_name in reversed(_SEALED_CHILD_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_unsealed_insert ON {table_name}")
    op.execute("DROP FUNCTION nexus_shadow_unsealed()")
    for table_name in reversed(_SHADOW_TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_immutable ON {table_name}")
        op.execute(f"DROP TRIGGER trg_{table_name}_created_at_immutable ON {table_name}")
    op.execute("DROP FUNCTION nexus_reject_shadow_fact_mutation()")

    for table_name in reversed(_SHADOW_TABLES):
        op.drop_table(table_name)
    op.drop_constraint(
        "uq_optimization_proposals_shadow_root",
        "optimization_proposals",
        type_="unique",
    )
    op.execute("DROP FUNCTION nexus_date_array_strictly_increasing(date[])")
