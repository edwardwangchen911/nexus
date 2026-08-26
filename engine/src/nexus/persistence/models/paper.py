# ruff: noqa: E501
"""Append-only persistence records for deterministic risk and paper accounting."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import DECIMAL_NUMERIC, Base, ImmutableCreatedAt, UTCDateTime


class PaperAccountRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_accounts"
    __table_args__ = (
        UniqueConstraint("account_id", name="uq_paper_accounts_account"),
        UniqueConstraint("portfolio_id", name="uq_paper_accounts_portfolio"),
        UniqueConstraint("code", name="uq_paper_accounts_code"),
        UniqueConstraint("id", "account_id", name="uq_paper_accounts_id_account"),
        UniqueConstraint("id", "portfolio_id", name="uq_paper_accounts_id_portfolio"),
        ForeignKeyConstraint(["account_id"], ["accounts.id"], name="fk_paper_accounts_account"),
        ForeignKeyConstraint(
            ["portfolio_id", "account_id"],
            ["portfolios.id", "portfolios.account_id"],
            name="fk_paper_accounts_portfolio",
        ),
        CheckConstraint("currency = 'USD'", name="currency_usd"),
        CheckConstraint(
            "account_model = 'cash' AND position_model = 'long_only'",
            name="model_fixed",
        ),
        CheckConstraint("initial_capital > 0", name="capital_positive"),
        CheckConstraint("capability = 'simulation_write_internal'", name="capability_internal"),
        CheckConstraint("btrim(code) <> ''", name="code_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    portfolio_id: Mapped[UUID]
    code: Mapped[str] = mapped_column(String(50))
    currency: Mapped[str] = mapped_column(String(3))
    account_model: Mapped[str] = mapped_column(String(20))
    position_model: Mapped[str] = mapped_column(String(20))
    initial_capital: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    capability: Mapped[str] = mapped_column(String(50))


class RiskPolicyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_policies"
    __table_args__ = (
        UniqueConstraint("version", name="uq_risk_policies_version"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
        CheckConstraint(
            "single_name_limit > 0 AND single_name_limit <= 1 "
            "AND sector_limit > 0 AND sector_limit <= 1 "
            "AND minimum_cash_ratio > 0 AND minimum_cash_ratio <= 1 "
            "AND drawdown_lock_ratio > 0 AND drawdown_lock_ratio <= 1 "
            "AND drawdown_recovery_ratio > 0 "
            "AND drawdown_recovery_ratio < drawdown_lock_ratio "
            "AND single_name_limit <= sector_limit",
            name="ratio_bounds",
        ),
        CheckConstraint(
            "minimum_buy_notional > 0 "
            "AND buy_reserve_markup >= 0 AND buy_reserve_markup <= 0.10 "
            "AND commission >= 0",
            name="amount_bounds",
        ),
        CheckConstraint(
            "supported_market = 'US' AND supported_currency = 'USD' "
            "AND supported_asset_types = ARRAY['ETF','common_stock']::varchar[]",
            name="scope_fixed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    version: Mapped[str] = mapped_column(String(50))
    active: Mapped[bool] = mapped_column(Boolean)
    single_name_limit: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    sector_limit: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    minimum_cash_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    drawdown_lock_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    drawdown_recovery_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    minimum_buy_notional: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    buy_reserve_markup: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    commission: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    supported_market: Mapped[str] = mapped_column(String(10))
    supported_currency: Mapped[str] = mapped_column(String(3))
    supported_asset_types: Mapped[list[str]] = mapped_column(ARRAY(String(30)))
    content_hash: Mapped[str] = mapped_column(String(64))


Index(
    "uq_risk_policies_active",
    RiskPolicyRecord.active,
    unique=True,
    postgresql_where=text("active"),
)


class StrategyDecisionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_decisions"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "strategy_version",
            "decision_session",
            name="uq_strategy_decisions_account_version_session",
        ),
        UniqueConstraint("id", "account_id", name="uq_strategy_decisions_id_account"),
        UniqueConstraint("decision_hash", name="uq_strategy_decisions_hash"),
        ForeignKeyConstraint(
            ["account_id", "portfolio_id"],
            ["paper_accounts.id", "paper_accounts.portfolio_id"],
            name="fk_strategy_decisions_paper_account",
        ),
        ForeignKeyConstraint(
            ["source_policy_id"],
            ["source_policies.id"],
            name="fk_strategy_decisions_source_policy",
        ),
        ForeignKeyConstraint(
            ["calendar_authority_id"],
            ["exchange_calendar_authorities.id"],
            name="fk_strategy_decisions_calendar",
        ),
        ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name="fk_strategy_decisions_universe",
        ),
        ForeignKeyConstraint(
            ["quality_report_id"],
            ["quality_reports.id"],
            name="fk_strategy_decisions_quality",
        ),
        CheckConstraint(
            "strategy_id = 'baseline' AND strategy_version = 'baseline_v1'",
            name="strategy_fixed",
        ),
        CheckConstraint("execution_session > decision_session", name="sessions_ordered"),
        CheckConstraint("reason = 'WEEKLY_REBALANCE'", name="reason_weekly"),
        CheckConstraint("cash_weight >= 0 AND cash_weight <= 1", name="cash_weight"),
        CheckConstraint(
            "portfolio_projection_hash ~ '^[0-9a-f]{64}$' "
            "AND authority_hash ~ '^[0-9a-f]{64}$' "
            "AND strategy_parameters_hash ~ '^[0-9a-f]{64}$' "
            "AND decision_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    portfolio_id: Mapped[UUID]
    strategy_id: Mapped[str] = mapped_column(String(50))
    strategy_version: Mapped[str] = mapped_column(String(50))
    decision_session: Mapped[date] = mapped_column(Date)
    decision_time: Mapped[datetime] = mapped_column(UTCDateTime())
    execution_session: Mapped[date] = mapped_column(Date)
    source_policy_id: Mapped[UUID]
    calendar_authority_id: Mapped[UUID]
    universe_snapshot_id: Mapped[UUID]
    quality_report_id: Mapped[UUID]
    portfolio_projection_hash: Mapped[str] = mapped_column(String(64))
    authority_hash: Mapped[str] = mapped_column(String(64))
    strategy_parameters_hash: Mapped[str] = mapped_column(String(64))
    decision_hash: Mapped[str] = mapped_column(String(64))
    cash_weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    reason: Mapped[str] = mapped_column(String(30))


class StrategyDecisionTargetRecord(ImmutableCreatedAt, Base):
    __tablename__ = "strategy_decision_targets"
    __table_args__ = (
        UniqueConstraint(
            "strategy_decision_id",
            "instrument_id",
            name="uq_strategy_decision_targets_decision_instrument",
        ),
        ForeignKeyConstraint(
            ["strategy_decision_id", "account_id"],
            ["strategy_decisions.id", "strategy_decisions.account_id"],
            name="fk_strategy_decision_targets_decision",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_strategy_decision_targets_instrument",
        ),
        CheckConstraint("target_weight >= 0 AND target_weight <= 1", name="weight"),
        CheckConstraint("strategy_priority BETWEEN 0 AND 2", name="priority"),
        CheckConstraint(
            "(exit AND target_weight = 0 AND strategy_priority = 0) "
            "OR (NOT exit AND strategy_priority IN (1,2))",
            name="exit_binding",
        ),
        CheckConstraint("btrim(reason) <> ''", name="reason_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    strategy_decision_id: Mapped[UUID]
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    target_weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    exit: Mapped[bool] = mapped_column(Boolean)
    strategy_priority: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str] = mapped_column(String(100))


class PaperDrawdownLockEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_drawdown_lock_events"
    __table_args__ = (
        UniqueConstraint("account_id", "event_seq", name="uq_paper_drawdown_events_account_seq"),
        ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name="fk_paper_drawdown_events_account",
        ),
        CheckConstraint("event_seq >= 1", name="seq_positive"),
        CheckConstraint(
            "(from_state IS NULL OR from_state IN "
            "('unlocked','locked','recovery_eligible')) "
            "AND to_state IN ('unlocked','locked','recovery_eligible')",
            name="states_allowed",
        ),
        CheckConstraint("drawdown_ratio >= 0 AND drawdown_ratio <= 1", name="ratio"),
        CheckConstraint("btrim(reason_code) <> ''", name="reason_nonempty"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    event_seq: Mapped[int] = mapped_column(Integer)
    from_state: Mapped[str | None] = mapped_column(String(30))
    to_state: Mapped[str] = mapped_column(String(30))
    drawdown_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    user_acknowledged: Mapped[bool] = mapped_column(Boolean)
    reason_code: Mapped[str] = mapped_column(String(100))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload_hash: Mapped[str] = mapped_column(String(64))


class PaperOrderBatchRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_order_batches"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "strategy_decision_id",
            name="uq_paper_order_batches_account_decision",
        ),
        UniqueConstraint("idempotency_record_id", name="uq_paper_order_batches_idempotency"),
        UniqueConstraint("batch_hash", name="uq_paper_order_batches_hash"),
        UniqueConstraint(
            "id",
            "account_id",
            "strategy_decision_id",
            name="uq_paper_order_batches_id_account_decision",
        ),
        UniqueConstraint(
            "id",
            "account_id",
            "risk_policy_id",
            name="uq_paper_order_batches_id_account_policy",
        ),
        ForeignKeyConstraint(
            ["strategy_decision_id", "account_id"],
            ["strategy_decisions.id", "strategy_decisions.account_id"],
            name="fk_paper_order_batches_decision",
        ),
        ForeignKeyConstraint(
            ["risk_policy_id"],
            ["risk_policies.id"],
            name="fk_paper_order_batches_policy",
        ),
        ForeignKeyConstraint(
            ["idempotency_record_id"],
            ["idempotency_records.id"],
            name="fk_paper_order_batches_idempotency",
        ),
        CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$' "
            "AND projection_hash ~ '^[0-9a-f]{64}$' "
            "AND risk_input_hash ~ '^[0-9a-f]{64}$' "
            "AND result_hash ~ '^[0-9a-f]{64}$' "
            "AND batch_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    strategy_decision_id: Mapped[UUID]
    risk_policy_id: Mapped[UUID]
    idempotency_record_id: Mapped[UUID]
    request_hash: Mapped[str] = mapped_column(String(64))
    projection_hash: Mapped[str] = mapped_column(String(64))
    risk_input_hash: Mapped[str] = mapped_column(String(64))
    result_hash: Mapped[str] = mapped_column(String(64))
    batch_hash: Mapped[str] = mapped_column(String(64))
    evaluated_at: Mapped[datetime] = mapped_column(UTCDateTime())


class RiskNoChangeEvaluationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_no_change_evaluations"
    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "instrument_id",
            name="uq_risk_no_change_batch_instrument",
        ),
        ForeignKeyConstraint(
            ["batch_id", "account_id", "strategy_decision_id"],
            [
                "paper_order_batches.id",
                "paper_order_batches.account_id",
                "paper_order_batches.strategy_decision_id",
            ],
            name="fk_risk_no_change_batch",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_risk_no_change_instrument",
        ),
        ForeignKeyConstraint(
            ["price_authority_id"],
            ["price_bar_set_authorities.id"],
            name="fk_risk_no_change_price_authority",
        ),
        CheckConstraint(
            "current_quantity >= 0 AND trunc(current_quantity) = current_quantity "
            "AND target_quantity = current_quantity",
            name="quantity_equal_whole",
        ),
        CheckConstraint("target_weight >= 0 AND target_weight <= 1", name="weight"),
        CheckConstraint("strategy_priority BETWEEN 0 AND 2", name="priority"),
        CheckConstraint("reference_close > 0", name="price_positive"),
        CheckConstraint("reason_code = 'NO_CHANGE'", name="reason_fixed"),
        CheckConstraint(
            "cardinality(authority_ids) > 0 AND array_position(authority_ids, NULL) IS NULL",
            name="authorities_nonempty",
        ),
        CheckConstraint(
            "input_hash ~ '^[0-9a-f]{64}$' AND evidence_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    batch_id: Mapped[UUID]
    account_id: Mapped[UUID]
    strategy_decision_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    current_quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    target_quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    target_weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    strategy_priority: Mapped[int] = mapped_column(Integer)
    reference_close: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    price_authority_id: Mapped[UUID]
    authority_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PostgreSQLUUID(as_uuid=True)))
    reason_code: Mapped[str] = mapped_column(String(100))
    input_hash: Mapped[str] = mapped_column(String(64))
    evidence_hash: Mapped[str] = mapped_column(String(64))


class PaperOrderRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_orders"
    __table_args__ = (
        UniqueConstraint("account_id", "client_order_id", name="uq_paper_orders_account_client"),
        UniqueConstraint("id", "account_id", name="uq_paper_orders_id_account"),
        UniqueConstraint("id", "batch_id", "account_id", name="uq_paper_orders_id_batch_account"),
        UniqueConstraint(
            "id",
            "account_id",
            "instrument_id",
            name="uq_paper_orders_id_account_instrument",
        ),
        ForeignKeyConstraint(
            ["batch_id", "account_id", "strategy_decision_id"],
            [
                "paper_order_batches.id",
                "paper_order_batches.account_id",
                "paper_order_batches.strategy_decision_id",
            ],
            name="fk_paper_orders_batch",
        ),
        ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_paper_orders_instrument"
        ),
        CheckConstraint("side IN ('buy','sell')", name="side_allowed"),
        CheckConstraint(
            "quantity > 0 AND trunc(quantity) = quantity",
            name="quantity_whole_positive",
        ),
        CheckConstraint("target_weight >= 0 AND target_weight <= 1", name="weight"),
        CheckConstraint("reference_close > 0 AND worst_price > 0", name="prices_positive"),
        CheckConstraint(
            "risk_outcome IN ('approved','reduced','rejected')",
            name="outcome_allowed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    batch_id: Mapped[UUID]
    account_id: Mapped[UUID]
    strategy_decision_id: Mapped[UUID]
    client_order_id: Mapped[str] = mapped_column(String(100))
    instrument_id: Mapped[UUID]
    side: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    target_weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    reference_close: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    worst_price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    risk_outcome: Mapped[str] = mapped_column(String(20))


class PaperOrderEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_order_events"
    __table_args__ = (
        UniqueConstraint("order_id", "event_seq", name="uq_paper_order_events_order_seq"),
        ForeignKeyConstraint(
            ["order_id", "account_id"],
            ["paper_orders.id", "paper_orders.account_id"],
            name="fk_paper_order_events_order",
        ),
        CheckConstraint("event_seq >= 1", name="seq_positive"),
        CheckConstraint(
            "(from_status IS NULL OR from_status IN "
            "('proposed','risk_rejected','risk_approved','awaiting_approval',"
            "'submitted','partially_filled','filled','cancelled','rejected','reconciling')) "
            "AND to_status IN "
            "('proposed','risk_rejected','risk_approved','awaiting_approval',"
            "'submitted','partially_filled','filled','cancelled','rejected','reconciling')",
            name="status_allowed",
        ),
        CheckConstraint(
            "actor_kind IN ('task18','task19','user','reconciliation')",
            name="actor_allowed",
        ),
        CheckConstraint("btrim(reason_code) <> ''", name="reason_nonempty"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
        UniqueConstraint("source_fill_id", name="uq_paper_order_events_source_fill"),
        ForeignKeyConstraint(
            ["source_fill_id", "account_id"],
            ["paper_fills.id", "paper_fills.account_id"],
            name="fk_paper_order_events_source_fill",
        ),
        CheckConstraint(
            "(actor_kind = 'task19' AND to_status IN ('partially_filled','filled') "
            "AND source_fill_id IS NOT NULL) OR "
            "(NOT (actor_kind = 'task19' AND "
            "to_status IN ('partially_filled','filled')) AND source_fill_id IS NULL)",
            name="fill_binding",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID]
    account_id: Mapped[UUID]
    event_seq: Mapped[int] = mapped_column(Integer)
    from_status: Mapped[str | None] = mapped_column(String(30))
    to_status: Mapped[str] = mapped_column(String(30))
    actor_kind: Mapped[str] = mapped_column(String(30))
    reason_code: Mapped[str] = mapped_column(String(100))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload_hash: Mapped[str] = mapped_column(String(64))
    source_fill_id: Mapped[UUID | None]


class RiskEvaluationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_evaluations"
    __table_args__ = (
        UniqueConstraint("order_id", name="uq_risk_evaluations_order"),
        UniqueConstraint("id", "account_id", name="uq_risk_evaluations_id_account"),
        ForeignKeyConstraint(
            ["order_id", "batch_id", "account_id"],
            ["paper_orders.id", "paper_orders.batch_id", "paper_orders.account_id"],
            name="fk_risk_evaluations_order",
        ),
        ForeignKeyConstraint(
            ["batch_id", "account_id", "risk_policy_id"],
            [
                "paper_order_batches.id",
                "paper_order_batches.account_id",
                "paper_order_batches.risk_policy_id",
            ],
            name="fk_risk_evaluations_batch",
        ),
        CheckConstraint("outcome IN ('approved','reduced','rejected')", name="outcome_allowed"),
        CheckConstraint(
            "desired_quantity > 0 AND trunc(desired_quantity) = desired_quantity "
            "AND approved_quantity >= 0 "
            "AND trunc(approved_quantity) = approved_quantity "
            "AND approved_quantity <= desired_quantity",
            name="quantities",
        ),
        CheckConstraint(
            "(outcome = 'approved' AND approved_quantity = desired_quantity) "
            "OR (outcome = 'reduced' AND approved_quantity > 0 "
            "AND approved_quantity < desired_quantity) "
            "OR (outcome = 'rejected' AND approved_quantity = 0)",
            name="outcome_quantity_binding",
        ),
        CheckConstraint(
            "reference_close > 0 AND worst_price > 0 "
            "AND reserve_amount >= 0 AND projected_available_cash >= 0",
            name="amounts",
        ),
        CheckConstraint(
            "jsonb_typeof(projected_sector_ratios) = 'object'",
            name="sector_ratios_object",
        ),
        CheckConstraint(
            "input_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID]
    batch_id: Mapped[UUID]
    account_id: Mapped[UUID]
    risk_policy_id: Mapped[UUID]
    outcome: Mapped[str] = mapped_column(String(20))
    desired_quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    approved_quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    target_weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    strategy_priority: Mapped[int] = mapped_column(Integer)
    reference_close: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    worst_price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    reserve_amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    projected_available_cash: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    projected_cash_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    projected_name_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    projected_sector_ratios: Mapped[dict[str, str]] = mapped_column(JSONB)
    input_hash: Mapped[str] = mapped_column(String(64))
    result_hash: Mapped[str] = mapped_column(String(64))


class RiskEvaluationRuleRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_evaluation_rules"
    __table_args__ = (
        UniqueConstraint(
            "risk_evaluation_id",
            "rule_seq",
            name="uq_risk_evaluation_rules_evaluation_seq",
        ),
        ForeignKeyConstraint(
            ["risk_evaluation_id", "account_id"],
            ["risk_evaluations.id", "risk_evaluations.account_id"],
            name="fk_risk_evaluation_rules_evaluation",
        ),
        CheckConstraint("rule_seq >= 1", name="seq_positive"),
        CheckConstraint("btrim(rule_code) <> ''", name="code_nonempty"),
        CheckConstraint(
            "array_position(authority_ids, NULL) IS NULL",
            name="authorities_nonnull",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    risk_evaluation_id: Mapped[UUID]
    account_id: Mapped[UUID]
    rule_seq: Mapped[int] = mapped_column(Integer)
    rule_code: Mapped[str] = mapped_column(String(100))
    passed: Mapped[bool] = mapped_column(Boolean)
    limit_value: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    observed_value: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    remaining_value: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    authority_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PostgreSQLUUID(as_uuid=True)))


class RiskEvaluationPriceInputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_evaluation_price_inputs"
    __table_args__ = (
        UniqueConstraint(
            "risk_evaluation_id",
            "instrument_id",
            name="uq_risk_price_inputs_evaluation_instrument",
        ),
        ForeignKeyConstraint(
            ["risk_evaluation_id", "account_id"],
            ["risk_evaluations.id", "risk_evaluations.account_id"],
            name="fk_risk_price_inputs_evaluation",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_risk_price_inputs_instrument",
        ),
        ForeignKeyConstraint(
            ["price_authority_id", "source", "content_hash"],
            [
                "price_bar_set_authorities.id",
                "price_bar_set_authorities.source",
                "price_bar_set_authorities.content_hash",
            ],
            name="fk_risk_price_inputs_authority",
        ),
        CheckConstraint("close > 0", name="close_positive"),
        CheckConstraint(
            "market = 'US' AND currency = 'USD' AND asset_type IN ('ETF','common_stock')",
            name="instrument_scope",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    risk_evaluation_id: Mapped[UUID]
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    price_authority_id: Mapped[UUID]
    market: Mapped[str] = mapped_column(String(10))
    currency: Mapped[str] = mapped_column(String(3))
    asset_type: Mapped[str] = mapped_column(String(30))
    close: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    source: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(50))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    content_hash: Mapped[str] = mapped_column(String(64))


class RiskEvaluationSectorInputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_evaluation_sector_inputs"
    __table_args__ = (
        UniqueConstraint(
            "risk_evaluation_id",
            "instrument_id",
            "sector",
            name="uq_risk_sector_inputs_evaluation_instrument_sector",
        ),
        ForeignKeyConstraint(
            ["risk_evaluation_id", "account_id"],
            ["risk_evaluations.id", "risk_evaluations.account_id"],
            name="fk_risk_sector_inputs_evaluation",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_risk_sector_inputs_instrument",
        ),
        ForeignKeyConstraint(
            ["sector_exposure_id", "instrument_id", "sector", "weight", "content_hash"],
            [
                "sector_exposures.id",
                "sector_exposures.instrument_id",
                "sector_exposures.sector_code",
                "sector_exposures.weight",
                "sector_exposures.content_hash",
            ],
            name="fk_risk_sector_inputs_exposure",
        ),
        CheckConstraint("weight > 0 AND weight <= 1", name="weight"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    risk_evaluation_id: Mapped[UUID]
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    sector_exposure_id: Mapped[UUID]
    sector: Mapped[str] = mapped_column(String(100))
    weight: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    content_hash: Mapped[str] = mapped_column(String(64))


class RiskQueryAuthorityRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_query_authorities"
    __table_args__ = (
        UniqueConstraint(
            "source_policy_id",
            "universe_snapshot_id",
            "decision_session",
            "kind",
            name="uq_risk_query_authorities_scope",
        ),
        UniqueConstraint(
            "id",
            "kind",
            "complete",
            "available_at",
            "content_hash",
            name="uq_risk_query_authorities_proof_binding",
        ),
        ForeignKeyConstraint(
            ["source_policy_id"],
            ["source_policies.id"],
            name="fk_risk_query_authorities_source_policy",
        ),
        ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name="fk_risk_query_authorities_universe",
        ),
        CheckConstraint(
            "kind IN ('corporate_action','suspension','delisting')",
            name="kind_allowed",
        ),
        CheckConstraint(
            "cardinality(required_instrument_ids) > 0",
            name="required_nonempty",
        ),
        CheckConstraint(
            "array_position(required_instrument_ids, NULL) IS NULL "
            "AND array_position(blocked_instrument_ids, NULL) IS NULL",
            name="arrays_nonnull",
        ),
        CheckConstraint(
            "blocked_instrument_ids <@ required_instrument_ids",
            name="blocked_subset",
        ),
        CheckConstraint("complete", name="complete"),
        CheckConstraint(
            "source_watermark_hash ~ '^[0-9a-f]{64}$' AND content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    source_policy_id: Mapped[UUID]
    universe_snapshot_id: Mapped[UUID]
    decision_session: Mapped[date] = mapped_column(Date)
    kind: Mapped[str] = mapped_column(String(30))
    required_instrument_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PostgreSQLUUID(as_uuid=True)))
    blocked_instrument_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PostgreSQLUUID(as_uuid=True)))
    complete: Mapped[bool] = mapped_column(Boolean)
    source_watermark_hash: Mapped[str] = mapped_column(String(64))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    content_hash: Mapped[str] = mapped_column(String(64))


class RiskEvaluationQueryProofRecord(ImmutableCreatedAt, Base):
    __tablename__ = "risk_evaluation_query_proofs"
    __table_args__ = (
        UniqueConstraint("risk_evaluation_id", "kind", name="uq_risk_query_proofs_evaluation_kind"),
        ForeignKeyConstraint(
            ["risk_evaluation_id", "account_id"],
            ["risk_evaluations.id", "risk_evaluations.account_id"],
            name="fk_risk_query_proofs_evaluation",
        ),
        ForeignKeyConstraint(
            ["authority_id", "kind", "complete", "available_at", "content_hash"],
            [
                "risk_query_authorities.id",
                "risk_query_authorities.kind",
                "risk_query_authorities.complete",
                "risk_query_authorities.available_at",
                "risk_query_authorities.content_hash",
            ],
            name="fk_risk_query_proofs_authority",
        ),
        CheckConstraint(
            "kind IN ('corporate_action','suspension','delisting')",
            name="kind_allowed",
        ),
        CheckConstraint("complete", name="complete"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    risk_evaluation_id: Mapped[UUID]
    account_id: Mapped[UUID]
    kind: Mapped[str] = mapped_column(String(30))
    authority_id: Mapped[UUID]
    complete: Mapped[bool] = mapped_column(Boolean)
    blocked: Mapped[bool] = mapped_column(Boolean)
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    content_hash: Mapped[str] = mapped_column(String(64))


class PaperLedgerAccountRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_ledger_accounts"
    __table_args__ = (
        UniqueConstraint("paper_account_id", "code", name="uq_paper_ledger_accounts_account_code"),
        UniqueConstraint(
            "id",
            "paper_account_id",
            "code",
            name="uq_paper_ledger_accounts_id_account_code",
        ),
        ForeignKeyConstraint(
            ["paper_account_id"],
            ["paper_accounts.id"],
            name="fk_paper_ledger_accounts_paper_account",
        ),
        CheckConstraint("normal_side IN ('debit','credit')", name="normal_side"),
        CheckConstraint("currency = 'USD'", name="currency_usd"),
        CheckConstraint("btrim(code) <> ''", name="code_nonempty"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    paper_account_id: Mapped[UUID]
    code: Mapped[str] = mapped_column(String(100))
    normal_side: Mapped[str] = mapped_column(String(10))
    currency: Mapped[str] = mapped_column(String(3))


class PaperLedgerTransactionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_ledger_transactions"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "transaction_hash",
            name="uq_paper_ledger_transactions_account_hash",
        ),
        UniqueConstraint("id", "account_id", name="uq_paper_ledger_transactions_id_account"),
        UniqueConstraint(
            "id",
            "account_id",
            "order_id",
            name="uq_paper_ledger_transactions_id_account_order",
        ),
        ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name="fk_paper_ledger_transactions_account",
        ),
        ForeignKeyConstraint(
            ["order_id", "account_id"],
            ["paper_orders.id", "paper_orders.account_id"],
            name="fk_paper_ledger_transactions_order",
        ),
        CheckConstraint(
            "transaction_type IN ('funding','cash_reservation','fill','release','adjustment')",
            name="type_allowed",
        ),
        CheckConstraint(
            "reference_type IN "
            "('initial_funding','cash_reservation','fill','release','adjustment')",
            name="reference_allowed",
        ),
        CheckConstraint(
            "(transaction_type = 'funding' AND order_id IS NULL "
            "AND reference_type = 'initial_funding') "
            "OR (transaction_type <> 'funding' AND order_id IS NOT NULL)",
            name="order_binding",
        ),
        CheckConstraint(
            "reference_hash ~ '^[0-9a-f]{64}$' AND transaction_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    order_id: Mapped[UUID | None]
    transaction_type: Mapped[str] = mapped_column(String(30))
    reference_type: Mapped[str] = mapped_column(String(30))
    reference_hash: Mapped[str] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    transaction_hash: Mapped[str] = mapped_column(String(64))


class PaperLedgerEntryRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_ledger_entries"
    __table_args__ = (
        UniqueConstraint(
            "transaction_id",
            "entry_seq",
            name="uq_paper_ledger_entries_transaction_seq",
        ),
        ForeignKeyConstraint(
            ["transaction_id", "account_id"],
            ["paper_ledger_transactions.id", "paper_ledger_transactions.account_id"],
            name="fk_paper_ledger_entries_transaction",
        ),
        ForeignKeyConstraint(
            ["ledger_account_id", "account_id", "account_code"],
            [
                "paper_ledger_accounts.id",
                "paper_ledger_accounts.paper_account_id",
                "paper_ledger_accounts.code",
            ],
            name="fk_paper_ledger_entries_account",
        ),
        CheckConstraint("entry_seq >= 1", name="seq_positive"),
        CheckConstraint("currency = 'USD'", name="currency_usd"),
        CheckConstraint(
            "(debit_amount > 0 AND credit_amount = 0) OR (credit_amount > 0 AND debit_amount = 0)",
            name="one_side",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    transaction_id: Mapped[UUID]
    account_id: Mapped[UUID]
    entry_seq: Mapped[int] = mapped_column(Integer)
    ledger_account_id: Mapped[UUID]
    account_code: Mapped[str] = mapped_column(String(100))
    currency: Mapped[str] = mapped_column(String(3))
    debit_amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    credit_amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)


class PaperReservationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_reservations"
    __table_args__ = (
        UniqueConstraint("order_id", name="uq_paper_reservations_order"),
        UniqueConstraint("id", "account_id", name="uq_paper_reservations_id_account"),
        UniqueConstraint(
            "ledger_transaction_id",
            name="uq_paper_reservations_ledger_transaction",
        ),
        ForeignKeyConstraint(
            ["order_id", "account_id"],
            ["paper_orders.id", "paper_orders.account_id"],
            name="fk_paper_reservations_order",
        ),
        ForeignKeyConstraint(
            ["ledger_transaction_id", "account_id", "order_id"],
            [
                "paper_ledger_transactions.id",
                "paper_ledger_transactions.account_id",
                "paper_ledger_transactions.order_id",
            ],
            name="fk_paper_reservations_ledger_transaction",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_paper_reservations_instrument",
        ),
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(
            "(reservation_type = 'cash' AND currency = 'USD' "
            "AND instrument_id IS NULL AND ledger_transaction_id IS NOT NULL) "
            "OR (reservation_type = 'position' AND currency IS NULL "
            "AND instrument_id IS NOT NULL AND ledger_transaction_id IS NULL "
            "AND trunc(amount) = amount)",
            name="type_binding",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID]
    account_id: Mapped[UUID]
    ledger_transaction_id: Mapped[UUID | None]
    reservation_type: Mapped[str] = mapped_column(String(20))
    currency: Mapped[str | None] = mapped_column(String(3))
    instrument_id: Mapped[UUID | None]
    amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)


class PaperReservationEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_reservation_events"
    __table_args__ = (
        UniqueConstraint(
            "reservation_id",
            "event_seq",
            name="uq_paper_reservation_events_reservation_seq",
        ),
        ForeignKeyConstraint(
            ["reservation_id", "account_id"],
            ["paper_reservations.id", "paper_reservations.account_id"],
            name="fk_paper_reservation_events_reservation",
        ),
        CheckConstraint("event_seq >= 1", name="seq_positive"),
        CheckConstraint("event_type IN ('reserved','consumed','released')", name="type_allowed"),
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
        UniqueConstraint("source_fill_id", name="uq_paper_reservation_events_source_fill"),
        ForeignKeyConstraint(
            ["source_fill_id", "account_id"],
            ["paper_fills.id", "paper_fills.account_id"],
            name="fk_paper_reservation_events_source_fill",
        ),
        CheckConstraint(
            "(event_type = 'consumed' AND source_fill_id IS NOT NULL) "
            "OR (event_type IN ('reserved','released') AND source_fill_id IS NULL)",
            name="fill_binding",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    reservation_id: Mapped[UUID]
    account_id: Mapped[UUID]
    event_seq: Mapped[int] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(20))
    amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload_hash: Mapped[str] = mapped_column(String(64))
    source_fill_id: Mapped[UUID | None]


class PaperExecutionModelRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_execution_models"
    __table_args__ = (
        UniqueConstraint("version", name="uq_paper_execution_models_version"),
        UniqueConstraint("content_hash", name="uq_paper_execution_models_hash"),
        UniqueConstraint("id", "content_hash", name="uq_paper_execution_models_id_hash"),
        CheckConstraint("version = 'execution_model_v1'", name="version_v1"),
        CheckConstraint("schema_version = 'paper_execution_model_v1'", name="schema_v1"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash"),
        CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(100))
    content_hash: Mapped[str] = mapped_column(String(64))
    document: Mapped[dict[str, object]] = mapped_column(JSONB)


class PaperExecutionWindowAuthorityRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_execution_window_authorities"
    __table_args__ = (
        UniqueConstraint("order_id", "window_seq", name="uq_paper_execution_windows_order_seq"),
        UniqueConstraint(
            "order_id",
            "window_start_at",
            "window_end_at",
            name="uq_paper_execution_windows_order_bounds",
        ),
        UniqueConstraint(
            "id",
            "order_id",
            "account_id",
            "instrument_id",
            "source_authority_id",
            name="uq_paper_execution_windows_fill_scope",
        ),
        ForeignKeyConstraint(
            ["order_id", "account_id", "instrument_id"],
            ["paper_orders.id", "paper_orders.account_id", "paper_orders.instrument_id"],
            name="fk_paper_execution_windows_order",
        ),
        ForeignKeyConstraint(
            ["source_authority_id", "source", "source_content_hash"],
            [
                "price_bar_set_authorities.id",
                "price_bar_set_authorities.source",
                "price_bar_set_authorities.content_hash",
            ],
            name="fk_paper_execution_windows_authority",
        ),
        ForeignKeyConstraint(
            ["quote_id"], ["latest_quotes.id"], name="fk_paper_execution_windows_quote"
        ),
        CheckConstraint("window_seq >= 1", name="seq"),
        CheckConstraint(
            "window_end_at = window_start_at + interval '5 minutes' "
            "AND data_available_at <= evaluated_at",
            name="bounds",
        ),
        CheckConstraint(
            "delivery_mode IN ('real_time','delayed','fixture','unknown') "
            "AND btrim(source) <> '' AND btrim(feed) <> ''",
            name="source",
        ),
        CheckConstraint(
            "vwap > 0 AND volume > 0 AND capacity >= 0 "
            "AND trunc(capacity) = capacity AND half_spread >= 0",
            name="values",
        ),
        CheckConstraint(
            "(quote_id IS NULL AND quote_revision IS NULL AND bid IS NULL "
            "AND ask IS NULL AND half_spread = 0) "
            "OR (quote_id IS NOT NULL AND quote_revision IS NOT NULL "
            "AND bid > 0 AND ask >= bid AND half_spread = (ask - bid) / 2)",
            name="quote_copy",
        ),
        CheckConstraint(
            "source_content_hash ~ '^[0-9a-f]{64}$' AND content_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID]
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    window_seq: Mapped[int] = mapped_column(Integer)
    window_start_at: Mapped[datetime] = mapped_column(UTCDateTime())
    window_end_at: Mapped[datetime] = mapped_column(UTCDateTime())
    evaluated_at: Mapped[datetime] = mapped_column(UTCDateTime())
    data_available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    source_authority_id: Mapped[UUID]
    source: Mapped[str] = mapped_column(String(100))
    source_content_hash: Mapped[str] = mapped_column(String(64))
    feed: Mapped[str] = mapped_column(String(100))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    vwap: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    volume: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    capacity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    quote_id: Mapped[UUID | None]
    quote_revision: Mapped[str | None] = mapped_column(String(255))
    bid: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    ask: Mapped[Decimal | None] = mapped_column(DECIMAL_NUMERIC)
    half_spread: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    content_hash: Mapped[str] = mapped_column(String(64))


class PaperExecutionWindowMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_execution_window_members"
    __table_args__ = (
        UniqueConstraint(
            "window_authority_id",
            "member_seq",
            name="uq_paper_execution_window_member_seq",
        ),
        ForeignKeyConstraint(
            ["window_authority_id"],
            ["paper_execution_window_authorities.id"],
            name="fk_paper_execution_window_members_window",
        ),
        ForeignKeyConstraint(
            ["bar_id"], ["price_bar_records.id"], name="fk_paper_execution_window_members_bar"
        ),
        CheckConstraint("member_seq BETWEEN 1 AND 5", name="seq"),
    )

    window_authority_id: Mapped[UUID] = mapped_column(primary_key=True)
    bar_id: Mapped[UUID] = mapped_column(primary_key=True)
    member_seq: Mapped[int] = mapped_column(Integer)


class PaperRequestIdempotencyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_request_idempotency"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "operation",
            "idempotency_key",
            name="uq_paper_request_idempotency_scope",
        ),
        ForeignKeyConstraint(
            ["order_id", "account_id"],
            ["paper_orders.id", "paper_orders.account_id"],
            name="fk_paper_request_idempotency_order",
        ),
        CheckConstraint("operation IN ('advance','cancel')", name="operation"),
        CheckConstraint("length(idempotency_key) BETWEEN 1 AND 255", name="key"),
        CheckConstraint("result_event_seq IS NULL OR result_event_seq >= 1", name="seq"),
        CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    order_id: Mapped[UUID]
    operation: Mapped[str] = mapped_column(String(30))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    result_event_seq: Mapped[int | None] = mapped_column(Integer)
    result_hash: Mapped[str] = mapped_column(String(64))


class PaperFillRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_fills"
    __table_args__ = (
        UniqueConstraint("order_id", "fill_seq", name="uq_paper_fills_order_seq"),
        UniqueConstraint("id", "account_id", name="uq_paper_fills_id_account"),
        UniqueConstraint("execution_window_authority_id", name="uq_paper_fills_execution_window"),
        UniqueConstraint("ledger_transaction_id", name="uq_paper_fills_ledger_transaction"),
        UniqueConstraint(
            "id",
            "account_id",
            "instrument_id",
            name="uq_paper_fills_id_account_instrument",
        ),
        ForeignKeyConstraint(
            ["order_id", "account_id", "instrument_id"],
            [
                "paper_orders.id",
                "paper_orders.account_id",
                "paper_orders.instrument_id",
            ],
            name="fk_paper_fills_order",
        ),
        ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_paper_fills_instrument"
        ),
        ForeignKeyConstraint(
            ["source_authority_id"],
            ["price_bar_set_authorities.id"],
            name="fk_paper_fills_source_authority",
        ),
        ForeignKeyConstraint(
            ["execution_model_id", "execution_model_hash"],
            ["paper_execution_models.id", "paper_execution_models.content_hash"],
            name="fk_paper_fills_execution_model",
        ),
        ForeignKeyConstraint(
            [
                "execution_window_authority_id",
                "order_id",
                "account_id",
                "instrument_id",
                "source_authority_id",
            ],
            [
                "paper_execution_window_authorities.id",
                "paper_execution_window_authorities.order_id",
                "paper_execution_window_authorities.account_id",
                "paper_execution_window_authorities.instrument_id",
                "paper_execution_window_authorities.source_authority_id",
            ],
            name="fk_paper_fills_execution_window",
        ),
        ForeignKeyConstraint(
            ["ledger_transaction_id", "account_id", "order_id"],
            [
                "paper_ledger_transactions.id",
                "paper_ledger_transactions.account_id",
                "paper_ledger_transactions.order_id",
            ],
            name="fk_paper_fills_ledger_transaction",
        ),
        CheckConstraint("fill_seq >= 1", name="seq_positive"),
        CheckConstraint(
            "quantity > 0 AND trunc(quantity) = quantity "
            "AND price > 0 AND notional > 0 AND notional = quantity * price AND fee >= 0",
            name="values_positive",
        ),
        CheckConstraint(
            "execution_model_hash ~ '^[0-9a-f]{64}$' AND payload_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        CheckConstraint(
            "window_vwap > 0 AND half_spread >= 0 "
            "AND slippage_bps IN (10,25) AND slippage_amount >= 0 "
            "AND commission >= 0 AND regulatory_fee >= 0 "
            "AND fee = commission + regulatory_fee",
            name="execution_values",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    order_id: Mapped[UUID]
    account_id: Mapped[UUID]
    fill_seq: Mapped[int] = mapped_column(Integer)
    instrument_id: Mapped[UUID]
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    notional: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    fee: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    source_authority_id: Mapped[UUID]
    execution_model_hash: Mapped[str] = mapped_column(String(64))
    payload_hash: Mapped[str] = mapped_column(String(64))
    execution_model_id: Mapped[UUID]
    execution_window_authority_id: Mapped[UUID]
    ledger_transaction_id: Mapped[UUID]
    window_vwap: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    half_spread: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    slippage_bps: Mapped[int] = mapped_column(Integer)
    slippage_amount: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    commission: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    regulatory_fee: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    evaluated_at: Mapped[datetime] = mapped_column(UTCDateTime())


class PaperPositionAdjustmentEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_position_adjustment_events"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "instrument_id",
            "event_seq",
            name="uq_paper_position_events_account_instrument_seq",
        ),
        UniqueConstraint(
            "source_fill_id",
            name="uq_paper_position_adjustments_source_fill",
        ),
        ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name="fk_paper_position_events_account",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_paper_position_events_instrument",
        ),
        ForeignKeyConstraint(
            ["source_fill_id", "account_id", "instrument_id"],
            [
                "paper_fills.id",
                "paper_fills.account_id",
                "paper_fills.instrument_id",
            ],
            name="fk_paper_position_events_fill",
        ),
        CheckConstraint("event_seq >= 1", name="seq_positive"),
        CheckConstraint(
            "event_type IN ('fill','split','dividend','delisting','correction')",
            name="type_allowed",
        ),
        CheckConstraint(
            "(event_type = 'fill' AND source_fill_id IS NOT NULL) "
            "OR (event_type IN ('split','dividend','delisting') "
            "AND source_fill_id IS NULL) "
            "OR event_type = 'correction'",
            name="fill_source_binding",
        ),
        CheckConstraint("trunc(quantity_delta) = quantity_delta", name="quantity_whole"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    event_seq: Mapped[int] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(30))
    quantity_delta: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cost_delta: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    source_fill_id: Mapped[UUID | None]
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload_hash: Mapped[str] = mapped_column(String(64))


class PaperPositionSnapshotRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_position_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "as_of",
            "instrument_id",
            name="uq_paper_position_snapshots_account_asof_instrument",
        ),
        ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name="fk_paper_position_snapshots_account",
        ),
        ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_paper_position_snapshots_instrument",
        ),
        ForeignKeyConstraint(
            ["price_authority_id"],
            ["price_bar_set_authorities.id"],
            name="fk_paper_position_snapshots_price_authority",
        ),
        CheckConstraint(
            "quantity >= 0 AND trunc(quantity) = quantity "
            "AND reserved_quantity >= 0 "
            "AND trunc(reserved_quantity) = reserved_quantity "
            "AND reserved_quantity <= quantity "
            "AND cost_basis >= 0 AND reference_close > 0",
            name="values",
        ),
        CheckConstraint("projection_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    instrument_id: Mapped[UUID]
    as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    reserved_quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cost_basis: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    reference_close: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    price_authority_id: Mapped[UUID]
    projection_hash: Mapped[str] = mapped_column(String(64))


class PaperPerformanceSnapshotRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_performance_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "as_of",
            name="uq_paper_performance_snapshots_account_asof",
        ),
        ForeignKeyConstraint(
            ["account_id"],
            ["paper_accounts.id"],
            name="fk_paper_performance_snapshots_account",
        ),
        UniqueConstraint("id", "account_id", name="uq_paper_performance_snapshots_id_account"),
        CheckConstraint(
            "nav > 0 AND peak_nav >= nav AND cash >= 0 AND fees >= 0 "
            "AND drawdown_ratio >= 0 AND drawdown_ratio <= 1",
            name="values",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID]
    as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    peak_nav: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    cash: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    realized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    unrealized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    fees: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    drawdown_ratio: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    content_hash: Mapped[str] = mapped_column(String(64))


class PaperPerformanceAttributionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "paper_performance_attributions"
    __table_args__ = (
        UniqueConstraint(
            "performance_snapshot_id",
            "dimension",
            "dimension_key",
            name="uq_paper_performance_attribution_dimension",
        ),
        ForeignKeyConstraint(
            ["performance_snapshot_id", "account_id"],
            ["paper_performance_snapshots.id", "paper_performance_snapshots.account_id"],
            name="fk_paper_performance_attributions_snapshot",
        ),
        CheckConstraint("dimension IN ('cash','instrument','sector')", name="dimension"),
        CheckConstraint("market_value >= 0 AND fees >= 0 AND slippage >= 0", name="values"),
        CheckConstraint("completeness IN ('FULL','PARTIAL')", name="completeness"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    performance_snapshot_id: Mapped[UUID]
    account_id: Mapped[UUID]
    dimension: Mapped[str] = mapped_column(String(20))
    dimension_key: Mapped[str] = mapped_column(String(100))
    market_value: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    realized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    unrealized_pnl: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    fees: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    slippage: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    completeness: Mapped[str] = mapped_column(String(10))
    content_hash: Mapped[str] = mapped_column(String(64))
