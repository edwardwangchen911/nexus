# ruff: noqa: E501
"""Append-only persistence records for deterministic proposal Shadow validation."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import (
    DECIMAL_NUMERIC,
    Base,
    ImmutableCreatedAt,
    UTCDateTime,
)


class ShadowGroupRecord(ImmutableCreatedAt, Base):
    """One immutable three-portfolio forward-validation root."""

    __tablename__ = "shadow_groups"
    __table_args__ = (
        UniqueConstraint("proposal_group_id", name="uq_shadow_groups_proposal_group"),
        UniqueConstraint(
            "id", "proposal_group_id", name="uq_shadow_groups_id_proposal_group"
        ),
        UniqueConstraint(
            "id",
            "calendar_authority_id",
            "source_policy_id",
            "cost_policy_id",
            name="uq_shadow_groups_authority_binding",
        ),
        ForeignKeyConstraint(
            ["proposal_group_id"],
            ["optimization_proposal_groups.id"],
            name="fk_shadow_groups_proposal_group",
        ),
        ForeignKeyConstraint(
            ["baseline_portfolio_snapshot_id"],
            ["portfolio_snapshots.id"],
            name="fk_shadow_groups_baseline_snapshot",
        ),
        ForeignKeyConstraint(
            ["source_policy_id"],
            ["source_policies.id"],
            name="fk_shadow_groups_source_policy",
        ),
        ForeignKeyConstraint(
            ["cost_policy_id"],
            ["cost_policies.id"],
            name="fk_shadow_groups_cost_policy",
        ),
        ForeignKeyConstraint(
            ["calendar_authority_id"],
            ["exchange_calendar_authorities.id"],
            name="fk_shadow_groups_calendar_authority",
        ),
        CheckConstraint(
            "proposal_bundle_hash ~ '^[0-9a-f]{64}$'",
            name="proposal_bundle_hash_canonical",
        ),
        CheckConstraint(
            "calendar_hash ~ '^[0-9a-f]{64}$'", name="calendar_hash_canonical"
        ),
        CheckConstraint(
            "cardinality(evaluation_sessions) = 63",
            name="evaluation_session_count",
        ),
        CheckConstraint(
            "cardinality(grace_sessions) = 5", name="grace_session_count"
        ),
        CheckConstraint(
            "array_position(evaluation_sessions, NULL) IS NULL "
            "AND array_position(grace_sessions, NULL) IS NULL",
            name="session_arrays_nonnull",
        ),
        CheckConstraint(
            "nexus_date_array_strictly_increasing(evaluation_sessions) "
            "AND nexus_date_array_strictly_increasing(grace_sessions)",
            name="session_arrays_ordered",
        ),
        CheckConstraint(
            "evaluation_sessions[1] = start_session "
            "AND evaluation_sessions[63] = evaluation_end_session "
            "AND grace_sessions[1] > evaluation_end_session "
            "AND grace_sessions[5] = grace_end_session",
            name="session_boundaries_bound",
        ),
        CheckConstraint(
            "signal_session < start_session "
            "AND start_session <= evaluation_end_session "
            "AND evaluation_end_session < grace_end_session",
            name="session_boundaries_ordered",
        ),
        CheckConstraint(
            "initial_nav = 100.000000000000", name="initial_nav_fixed"
        ),
        CheckConstraint(
            "schema_version = 'proposal_shadow_v1'", name="schema_version_v1"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    proposal_group_id: Mapped[UUID]
    baseline_portfolio_snapshot_id: Mapped[UUID]
    proposal_bundle_hash: Mapped[str] = mapped_column(String(64))
    source_policy_id: Mapped[UUID]
    cost_policy_id: Mapped[UUID]
    calendar_authority_id: Mapped[UUID]
    calendar_version: Mapped[str] = mapped_column(String(100))
    calendar_hash: Mapped[str] = mapped_column(String(64))
    signal_session: Mapped[date] = mapped_column(Date)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    start_session: Mapped[date] = mapped_column(Date)
    evaluation_end_session: Mapped[date] = mapped_column(Date)
    grace_end_session: Mapped[date] = mapped_column(Date)
    evaluation_sessions: Mapped[list[date]] = mapped_column(ARRAY(Date))
    grace_sessions: Mapped[list[date]] = mapped_column(ARRAY(Date))
    initial_nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    schema_version: Mapped[str] = mapped_column(String(32))


class ShadowPortfolioRecord(ImmutableCreatedAt, Base):
    """One of the fixed hold/minimum-change/strategy-target variants."""

    __tablename__ = "shadow_portfolios"
    __table_args__ = (
        UniqueConstraint(
            "shadow_group_id", "variant", name="uq_shadow_portfolios_group_variant"
        ),
        UniqueConstraint("proposal_id", name="uq_shadow_portfolios_proposal"),
        UniqueConstraint(
            "id", "shadow_group_id", name="uq_shadow_portfolios_id_group"
        ),
        ForeignKeyConstraint(
            ["shadow_group_id", "proposal_group_id"],
            ["shadow_groups.id", "shadow_groups.proposal_group_id"],
            name="fk_shadow_portfolios_group_root",
        ),
        ForeignKeyConstraint(
            ["proposal_id", "proposal_group_id", "variant"],
            [
                "optimization_proposals.id",
                "optimization_proposals.proposal_group_id",
                "optimization_proposals.variant",
            ],
            name="fk_shadow_portfolios_proposal_root",
        ),
        CheckConstraint(
            "variant IN ('hold_current','minimum_change','strategy_target')",
            name="variant_allowed",
        ),
        CheckConstraint(
            "initial_nav = 100.000000000000 "
            "AND initial_cash = 100.000000000000",
            name="initial_anchor_fixed",
        ),
        CheckConstraint(
            "target_weights_hash ~ '^[0-9a-f]{64}$'",
            name="target_weights_hash_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    proposal_group_id: Mapped[UUID]
    proposal_id: Mapped[UUID]
    variant: Mapped[str] = mapped_column(String(30))
    target_weights_hash: Mapped[str] = mapped_column(String(64))
    initial_nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    initial_cash: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)


class ShadowAdvanceAttemptRecord(ImmutableCreatedAt, Base):
    """One applied or failed attempt to advance the fixed session sequence."""

    __tablename__ = "shadow_advance_attempts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["shadow_group_id"],
            ["shadow_groups.id"],
            name="fk_shadow_advance_attempts_group",
        ),
        CheckConstraint(
            "result IN ('applied','data_partial','data_stale','source_conflict','session_gap')",
            name="result_allowed",
        ),
        CheckConstraint(
            "authority_hash IS NULL OR authority_hash ~ '^[0-9a-f]{64}$'",
            name="authority_hash_canonical",
        ),
        CheckConstraint("btrim(reason_code) <> ''", name="reason_code_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    requested_session: Mapped[date] = mapped_column(Date)
    expected_session: Mapped[date | None] = mapped_column(Date)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    result: Mapped[str] = mapped_column(String(32))
    reason_code: Mapped[str] = mapped_column(String(100))
    authority_hash: Mapped[str | None] = mapped_column(String(64))


class ShadowSessionAuthorityRecord(ImmutableCreatedAt, Base):
    """The exact point-in-time market/event authority for one successful session."""

    __tablename__ = "shadow_session_authorities"
    __table_args__ = (
        UniqueConstraint(
            "shadow_group_id", "session", name="uq_shadow_authorities_group_session"
        ),
        UniqueConstraint(
            "shadow_group_id", "ordinal", name="uq_shadow_authorities_group_ordinal"
        ),
        UniqueConstraint(
            "id",
            "shadow_group_id",
            "session",
            "ordinal",
            name="uq_shadow_authorities_state_root",
        ),
        UniqueConstraint(
            "id", "shadow_group_id", name="uq_shadow_authorities_id_group"
        ),
        ForeignKeyConstraint(
            [
                "shadow_group_id",
                "calendar_authority_id",
                "source_policy_id",
                "cost_policy_id",
            ],
            [
                "shadow_groups.id",
                "shadow_groups.calendar_authority_id",
                "shadow_groups.source_policy_id",
                "shadow_groups.cost_policy_id",
            ],
            name="fk_shadow_authorities_group_binding",
        ),
        CheckConstraint("ordinal BETWEEN 1 AND 63", name="ordinal_bounded"),
        CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"
        ),
        CheckConstraint(
            "query_hash ~ '^[0-9a-f]{64}$'", name="query_hash_canonical"
        ),
        CheckConstraint("available_at <= observed_at", name="available_before_observed"),
        CheckConstraint(
            "jsonb_typeof(source_summary) = 'object' "
            "AND jsonb_typeof(delivery_mode_summary) = 'object'",
            name="summaries_are_objects",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    session: Mapped[date] = mapped_column(Date)
    ordinal: Mapped[int] = mapped_column(Integer)
    calendar_authority_id: Mapped[UUID]
    source_policy_id: Mapped[UUID]
    cost_policy_id: Mapped[UUID]
    content_hash: Mapped[str] = mapped_column(String(64))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    late_backfill: Mapped[bool] = mapped_column(Boolean)
    query_hash: Mapped[str] = mapped_column(String(64))
    source_summary: Mapped[dict[str, object]] = mapped_column(JSONB)
    delivery_mode_summary: Mapped[dict[str, object]] = mapped_column(JSONB)


class ShadowSessionPriceInputRecord(ImmutableCreatedAt, Base):
    """One immutable raw Bar member consumed by a session authority."""

    __tablename__ = "shadow_session_price_inputs"
    __table_args__ = (
        UniqueConstraint(
            "session_authority_id",
            "instrument_id",
            name="uq_shadow_price_inputs_session_instrument",
        ),
        ForeignKeyConstraint(
            ["session_authority_id", "shadow_group_id"],
            ["shadow_session_authorities.id", "shadow_session_authorities.shadow_group_id"],
            name="fk_shadow_price_inputs_session_root",
        ),
        ForeignKeyConstraint(
            ["price_bar_record_id"],
            ["price_bar_records.id"],
            name="fk_shadow_price_inputs_bar",
        ),
        ForeignKeyConstraint(
            ["price_bar_set_authority_id"],
            ["price_bar_set_authorities.id"],
            name="fk_shadow_price_inputs_bar_set",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_shadow_price_inputs_instrument",
        ),
        CheckConstraint(
            "usage IN ('execution_and_mark','mark')", name="usage_allowed"
        ),
        CheckConstraint(
            "delivery_mode IN ('real_time','delayed','fixture','unknown')",
            name="delivery_mode_allowed",
        ),
        CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"
        ),
        CheckConstraint("btrim(source) <> '' AND btrim(revision) <> ''", name="identity_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    session_authority_id: Mapped[UUID]
    shadow_group_id: Mapped[UUID]
    price_bar_record_id: Mapped[UUID]
    price_bar_set_authority_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    usage: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    revision: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    content_hash: Mapped[str] = mapped_column(String(64))


class ShadowSessionEventInputRecord(ImmutableCreatedAt, Base):
    """One corporate-action or delisting member consumed by a session."""

    __tablename__ = "shadow_session_event_inputs"
    __table_args__ = (
        UniqueConstraint(
            "session_authority_id",
            "corporate_action_id",
            name="uq_shadow_event_inputs_session_action",
        ),
        ForeignKeyConstraint(
            ["session_authority_id", "shadow_group_id"],
            ["shadow_session_authorities.id", "shadow_session_authorities.shadow_group_id"],
            name="fk_shadow_event_inputs_session_root",
        ),
        ForeignKeyConstraint(
            ["corporate_action_id"],
            ["corporate_actions.id"],
            name="fk_shadow_event_inputs_action",
        ),
        CheckConstraint(
            "event_kind IN ('split','dividend','delisting')", name="event_kind_allowed"
        ),
        CheckConstraint(
            "evidence_hash ~ '^[0-9a-f]{64}$'", name="evidence_hash_canonical"
        ),
        CheckConstraint(
            "btrim(provider_event_id) <> '' AND btrim(revision) <> ''",
            name="identity_nonempty",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    session_authority_id: Mapped[UUID]
    shadow_group_id: Mapped[UUID]
    corporate_action_id: Mapped[UUID]
    event_kind: Mapped[str] = mapped_column(String(20))
    provider_event_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    evidence_hash: Mapped[str] = mapped_column(String(64))


class ShadowPortfolioStateRecord(ImmutableCreatedAt, Base):
    """Complete reconstructable ledger state for one portfolio and session."""

    __tablename__ = "shadow_portfolio_states"
    __table_args__ = (
        UniqueConstraint(
            "shadow_portfolio_id", "session", name="uq_shadow_states_portfolio_session"
        ),
        UniqueConstraint(
            "shadow_portfolio_id", "ordinal", name="uq_shadow_states_portfolio_ordinal"
        ),
        UniqueConstraint(
            "id",
            "shadow_portfolio_id",
            "shadow_group_id",
            name="uq_shadow_states_ledger_root",
        ),
        ForeignKeyConstraint(
            ["shadow_portfolio_id", "shadow_group_id"],
            ["shadow_portfolios.id", "shadow_portfolios.shadow_group_id"],
            name="fk_shadow_states_portfolio_root",
        ),
        ForeignKeyConstraint(
            ["session_authority_id", "shadow_group_id", "session", "ordinal"],
            [
                "shadow_session_authorities.id",
                "shadow_session_authorities.shadow_group_id",
                "shadow_session_authorities.session",
                "shadow_session_authorities.ordinal",
            ],
            name="fk_shadow_states_session_root",
        ),
        CheckConstraint("ordinal BETWEEN 1 AND 63", name="ordinal_bounded"),
        CheckConstraint("nav >= 0 AND cash >= 0", name="nav_cash_nonnegative"),
        CheckConstraint(
            "cumulative_traded_notional >= 0 AND cumulative_fees >= 0",
            name="cumulative_costs_nonnegative",
        ),
        CheckConstraint("state_hash ~ '^[0-9a-f]{64}$'", name="state_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    session_authority_id: Mapped[UUID]
    session: Mapped[date] = mapped_column(Date)
    ordinal: Mapped[int] = mapped_column(Integer)
    nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cash: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cumulative_traded_notional: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cumulative_fees: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cumulative_realized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    state_hash: Mapped[str] = mapped_column(String(64))


class ShadowInstrumentStateRecord(ImmutableCreatedAt, Base):
    """One touched instrument in a complete portfolio-state snapshot."""

    __tablename__ = "shadow_instrument_states"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_state_id",
            "instrument_id",
            name="uq_shadow_instruments_state_instrument",
        ),
        ForeignKeyConstraint(
            ["portfolio_state_id", "shadow_portfolio_id", "shadow_group_id"],
            [
                "shadow_portfolio_states.id",
                "shadow_portfolio_states.shadow_portfolio_id",
                "shadow_portfolio_states.shadow_group_id",
            ],
            name="fk_shadow_instruments_state_root",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_shadow_instruments_instrument",
        ),
        CheckConstraint(
            "quantity >= 0 AND cost_basis >= 0 AND cumulative_cost >= 0",
            name="amounts_nonnegative",
        ),
        CheckConstraint("last_close IS NULL OR last_close > 0", name="last_close_positive"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    portfolio_state_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cost_basis: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    realized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cumulative_cost: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    last_close: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    untradable: Mapped[bool] = mapped_column(Boolean)


class ShadowReceivableStateRecord(ImmutableCreatedAt, Base):
    """One unsettled dividend receivable in a complete ledger snapshot."""

    __tablename__ = "shadow_receivable_states"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_state_id",
            "corporate_action_id",
            "instrument_id",
            "payable_session",
            name="uq_shadow_receivables_state_action",
        ),
        ForeignKeyConstraint(
            ["portfolio_state_id", "shadow_portfolio_id", "shadow_group_id"],
            [
                "shadow_portfolio_states.id",
                "shadow_portfolio_states.shadow_portfolio_id",
                "shadow_portfolio_states.shadow_group_id",
            ],
            name="fk_shadow_receivables_state_root",
        ),
        ForeignKeyConstraint(
            ["corporate_action_id"],
            ["corporate_actions.id"],
            name="fk_shadow_receivables_action",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_shadow_receivables_instrument",
        ),
        CheckConstraint("amount >= 0", name="amount_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    portfolio_state_id: Mapped[UUID]
    corporate_action_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    payable_session: Mapped[date] = mapped_column(Date)


class ShadowSimulatedOutcomeRecord(ImmutableCreatedAt, Base):
    """One deterministic first-session target intent outcome."""

    __tablename__ = "shadow_simulated_outcomes"
    __table_args__ = (
        UniqueConstraint(
            "shadow_portfolio_id",
            "instrument_id",
            name="uq_shadow_outcomes_portfolio_instrument",
        ),
        UniqueConstraint(
            "id",
            "shadow_portfolio_id",
            "shadow_group_id",
            name="uq_shadow_outcomes_fill_root",
        ),
        ForeignKeyConstraint(
            ["shadow_portfolio_id", "shadow_group_id"],
            ["shadow_portfolios.id", "shadow_portfolios.shadow_group_id"],
            name="fk_shadow_outcomes_portfolio_root",
        ),
        ForeignKeyConstraint(
            ["session_authority_id", "shadow_group_id", "target_session", "ordinal"],
            [
                "shadow_session_authorities.id",
                "shadow_session_authorities.shadow_group_id",
                "shadow_session_authorities.session",
                "shadow_session_authorities.ordinal",
            ],
            name="fk_shadow_outcomes_session_root",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_shadow_outcomes_instrument",
        ),
        CheckConstraint("ordinal = 1", name="first_session_only"),
        CheckConstraint("signal_session < target_session", name="target_after_signal"),
        CheckConstraint("side IN ('buy','sell','rebalance')", name="side_allowed"),
        CheckConstraint(
            "status IN ('filled','missing_bar','untradable')", name="status_allowed"
        ),
        CheckConstraint("btrim(reason) <> ''", name="reason_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    session_authority_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    signal_session: Mapped[date] = mapped_column(Date)
    target_session: Mapped[date] = mapped_column(Date)
    ordinal: Mapped[int] = mapped_column(Integer)
    side: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str] = mapped_column(String(255))


class ShadowSimulatedFillRecord(ImmutableCreatedAt, Base):
    """One exact first-session open fill for a filled Shadow outcome."""

    __tablename__ = "shadow_simulated_fills"
    __table_args__ = (
        UniqueConstraint("outcome_id", name="uq_shadow_fills_outcome"),
        ForeignKeyConstraint(
            ["outcome_id", "shadow_portfolio_id", "shadow_group_id"],
            [
                "shadow_simulated_outcomes.id",
                "shadow_simulated_outcomes.shadow_portfolio_id",
                "shadow_simulated_outcomes.shadow_group_id",
            ],
            name="fk_shadow_fills_outcome_root",
        ),
        CheckConstraint("side IN ('buy','sell')", name="side_allowed"),
        CheckConstraint(
            "quantity > 0 AND price > 0 AND notional > 0 AND cost >= 0",
            name="fill_amounts_valid",
        ),
        CheckConstraint("execution_price_kind = 'open'", name="execution_price_open"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    outcome_id: Mapped[UUID]
    side: Mapped[str] = mapped_column(String(4))
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    notional: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cost: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    execution_price_kind: Mapped[str] = mapped_column(String(16))


class ShadowCheckpointRecord(ImmutableCreatedAt, Base):
    """One immutable observation or terminal checkpoint."""

    __tablename__ = "shadow_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "shadow_group_id", "ordinal", name="uq_shadow_checkpoints_group_ordinal"
        ),
        UniqueConstraint("id", "shadow_group_id", name="uq_shadow_checkpoints_id_group"),
        ForeignKeyConstraint(
            ["shadow_group_id"],
            ["shadow_groups.id"],
            name="fk_shadow_checkpoints_group",
        ),
        ForeignKeyConstraint(
            ["session_authority_id", "shadow_group_id", "session", "ordinal"],
            [
                "shadow_session_authorities.id",
                "shadow_session_authorities.shadow_group_id",
                "shadow_session_authorities.session",
                "shadow_session_authorities.ordinal",
            ],
            name="fk_shadow_checkpoints_session_root",
        ),
        CheckConstraint("ordinal IN (5,21,63)", name="ordinal_allowed"),
        CheckConstraint("status IN ('complete','incomplete')", name="status_allowed"),
        CheckConstraint(
            "coverage_count BETWEEN 0 AND ordinal", name="coverage_bounded"
        ),
        CheckConstraint(
            "(status = 'complete' AND session_authority_id IS NOT NULL "
            "AND coverage_count = ordinal AND reason IS NULL) "
            "OR (status = 'incomplete' AND session_authority_id IS NULL "
            "AND reason IS NOT NULL AND btrim(reason) <> '')",
            name="status_binding",
        ),
        CheckConstraint(
            "metrics_hash ~ '^[0-9a-f]{64}$'", name="metrics_hash_canonical"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    session_authority_id: Mapped[UUID | None]
    session: Mapped[date] = mapped_column(Date)
    ordinal: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    coverage_count: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str | None] = mapped_column(String(100))
    metrics_hash: Mapped[str] = mapped_column(String(64))


class ShadowCheckpointMetricRecord(ImmutableCreatedAt, Base):
    """One exact metric row for a checkpoint and fixed variant."""

    __tablename__ = "shadow_checkpoint_metrics"
    __table_args__ = (
        UniqueConstraint(
            "checkpoint_id", "variant", name="uq_shadow_metrics_checkpoint_variant"
        ),
        UniqueConstraint(
            "checkpoint_id",
            "shadow_portfolio_id",
            name="uq_shadow_metrics_checkpoint_portfolio",
        ),
        ForeignKeyConstraint(
            ["checkpoint_id", "shadow_group_id"],
            ["shadow_checkpoints.id", "shadow_checkpoints.shadow_group_id"],
            name="fk_shadow_metrics_checkpoint_root",
        ),
        ForeignKeyConstraint(
            ["shadow_portfolio_id", "shadow_group_id"],
            ["shadow_portfolios.id", "shadow_portfolios.shadow_group_id"],
            name="fk_shadow_metrics_portfolio_root",
        ),
        CheckConstraint(
            "variant IN ('hold_current','minimum_change','strategy_target')",
            name="variant_allowed",
        ),
        CheckConstraint("metric_status IN ('known','unknown')", name="status_allowed"),
        CheckConstraint(
            "(metric_status = 'unknown' AND unknown_reason IS NOT NULL "
            "AND btrim(unknown_reason) <> '' "
            "AND net_cumulative_return IS NULL AND return_delta_vs_hold IS NULL "
            "AND max_drawdown IS NULL AND drawdown_improvement IS NULL "
            "AND annualized_volatility IS NULL AND volatility_improvement IS NULL "
            "AND cumulative_turnover IS NULL AND fee_drag_index_points IS NULL "
            "AND worst_sessions IS NULL) "
            "OR (metric_status = 'known' AND unknown_reason IS NULL "
            "AND net_cumulative_return IS NOT NULL AND return_delta_vs_hold IS NOT NULL "
            "AND max_drawdown IS NOT NULL AND drawdown_improvement IS NOT NULL "
            "AND annualized_volatility IS NOT NULL AND volatility_improvement IS NOT NULL "
            "AND cumulative_turnover IS NOT NULL AND fee_drag_index_points IS NOT NULL "
            "AND jsonb_typeof(worst_sessions) = 'array')",
            name="known_unknown_binding",
        ),
        CheckConstraint(
            "max_drawdown IS NULL OR max_drawdown BETWEEN -1 AND 0",
            name="drawdown_bounded",
        ),
        CheckConstraint(
            "annualized_volatility IS NULL OR annualized_volatility >= 0",
            name="volatility_nonnegative",
        ),
        CheckConstraint(
            "cumulative_turnover IS NULL OR cumulative_turnover >= 0",
            name="turnover_nonnegative",
        ),
        CheckConstraint(
            "fee_drag_index_points IS NULL OR fee_drag_index_points >= 0",
            name="fees_nonnegative",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    checkpoint_id: Mapped[UUID]
    shadow_portfolio_id: Mapped[UUID]
    variant: Mapped[str] = mapped_column(String(30))
    metric_status: Mapped[str] = mapped_column(String(16))
    unknown_reason: Mapped[str | None] = mapped_column(String(100))
    net_cumulative_return: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    return_delta_vs_hold: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    max_drawdown: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    drawdown_improvement: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    annualized_volatility: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    volatility_improvement: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    cumulative_turnover: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    fee_drag_index_points: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    worst_sessions: Mapped[list[dict[str, object]] | None] = mapped_column(
        JSONB(none_as_null=True)
    )


class ShadowSealRecord(ImmutableCreatedAt, Base):
    """The final append-only seal that blocks all further Shadow facts."""

    __tablename__ = "shadow_seals"
    __table_args__ = (
        UniqueConstraint("shadow_group_id", name="uq_shadow_seals_group"),
        ForeignKeyConstraint(
            ["shadow_group_id"],
            ["shadow_groups.id"],
            name="fk_shadow_seals_group",
        ),
        CheckConstraint(
            "reason IN ('validated','rejected','expired')", name="reason_allowed"
        ),
        CheckConstraint(
            "(reason = 'validated' AND reason_code = 'SHADOW_VALIDATED') "
            "OR (reason = 'rejected' AND reason_code = 'USER_REJECTED') "
            "OR (reason = 'expired' AND reason_code IN "
            "('SHADOW_DATA_INCOMPLETE','SHADOW_INITIAL_EXECUTION_INCOMPLETE'))",
            name="reason_code_binding",
        ),
        CheckConstraint("btrim(reason_code) <> ''", name="reason_code_nonempty"),
        CheckConstraint("final_coverage BETWEEN 0 AND 63", name="coverage_bounded"),
        CheckConstraint(
            "final_authority_hash ~ '^[0-9a-f]{64}$'",
            name="final_authority_hash_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    shadow_group_id: Mapped[UUID]
    reason: Mapped[str] = mapped_column(String(16))
    reason_code: Mapped[str] = mapped_column(String(100))
    sealed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    final_coverage: Mapped[int] = mapped_column(Integer)
    final_authority_hash: Mapped[str] = mapped_column(String(64))
