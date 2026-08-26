"""Immutable persistence records for deterministic strategy backtests."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class StrategyDefinitionRecord(ImmutableCreatedAt, Base):
    """One published, content-addressed declarative strategy definition."""

    __tablename__ = "strategy_definitions"
    __table_args__ = (
        UniqueConstraint("strategy_version", name="uq_strategy_definitions_version"),
        CheckConstraint("expression_hash ~ '^[0-9a-f]{64}$'", name="expression_hash_canonical"),
        CheckConstraint(
            "lifecycle IN ('champion','challenger','retired')", name="lifecycle_allowed"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    strategy_version: Mapped[str] = mapped_column(String(100))
    schema_version: Mapped[str] = mapped_column(String(32))
    expression_hash: Mapped[str] = mapped_column(String(64))
    document: Mapped[dict[str, object]] = mapped_column(JSONB)
    lifecycle: Mapped[str] = mapped_column(String(16))
    published_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BacktestAuthorityPublicationMarkRecord(ImmutableCreatedAt, Base):
    """Database-ingestion watermark coordinated with exact-set finalization."""

    __tablename__ = "backtest_authority_publication_marks"
    __table_args__ = (
        CheckConstraint(
            "domain IN ('universe','sector','corporate_action','price','instrument_alias')",
            name="domain_allowed",
        ),
    )

    domain: Mapped[str] = mapped_column(String(32), primary_key=True)
    record_id: Mapped[UUID] = mapped_column(primary_key=True)


class ExchangeCalendarAuthorityRecord(ImmutableCreatedAt, Base):
    """One complete, content-addressed exchange-session calendar."""

    __tablename__ = "exchange_calendar_authorities"
    __table_args__ = (
        UniqueConstraint(
            "exchange",
            "version",
            "range_start",
            "range_end",
            name="uq_exchange_calendar_version_range",
        ),
        CheckConstraint("range_start <= range_end", name="range_ordered"),
        CheckConstraint("completeness_status = 'complete'", name="must_be_complete"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    exchange: Mapped[str] = mapped_column(String(20))
    version: Mapped[str] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(255))
    range_start: Mapped[date]
    range_end: Mapped[date]
    query_parameters: Mapped[dict[str, object]] = mapped_column(JSONB)
    completeness_status: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    published_at: Mapped[datetime] = mapped_column(UTCDateTime())


class ExchangeCalendarSessionRecord(ImmutableCreatedAt, Base):
    """One session belonging to a frozen complete calendar authority."""

    __tablename__ = "exchange_calendar_sessions"
    __table_args__ = (
        CheckConstraint(
            "market_open_at < market_close_at AND market_close_at <= decision_at",
            name="market_bounds",
        ),
    )

    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id"), primary_key=True
    )
    session: Mapped[date] = mapped_column(primary_key=True)
    decision_at: Mapped[datetime] = mapped_column(UTCDateTime())
    market_open_at: Mapped[datetime] = mapped_column(UTCDateTime())
    market_close_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BacktestAuthorityCollectionRecord(ImmutableCreatedAt, Base):
    """A finalized exact-set query result for one historical authority kind."""

    __tablename__ = "backtest_authority_collections"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('universe','sector','corporate_action')", name="kind_allowed"
        ),
        CheckConstraint("range_start <= range_end", name="range_ordered"),
        CheckConstraint("completeness_status = 'complete'", name="must_be_complete"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    kind: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(255))
    range_start: Mapped[date]
    range_end: Mapped[date]
    query_parameters: Mapped[dict[str, object]] = mapped_column(JSONB)
    completeness_status: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    completed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BacktestUniverseAuthorityMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "backtest_universe_authority_members"

    collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id"), primary_key=True
    )
    universe_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("universe_snapshots.id"), primary_key=True
    )


class BacktestSectorAuthorityMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "backtest_sector_authority_members"

    collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id"), primary_key=True
    )
    sector_exposure_id: Mapped[UUID] = mapped_column(
        ForeignKey("sector_exposures.id"), primary_key=True
    )


class BacktestCorporateActionAuthorityMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "backtest_corporate_action_authority_members"

    collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id"), primary_key=True
    )
    corporate_action_id: Mapped[UUID] = mapped_column(
        ForeignKey("corporate_actions.id"), primary_key=True
    )


class BacktestAuthorityBundleRecord(ImmutableCreatedAt, Base):
    """The immutable root that owns every authority used by one backtest."""

    __tablename__ = "backtest_authority_bundles"
    __table_args__ = (
        UniqueConstraint("data_snapshot_id", name="uq_backtest_bundle_data_snapshot"),
        CheckConstraint("range_start <= range_end", name="range_ordered"),
        CheckConstraint("completeness_status = 'complete'", name="must_be_complete"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    source_policy_id: Mapped[UUID] = mapped_column(ForeignKey("source_policies.id"))
    cost_policy_id: Mapped[UUID] = mapped_column(ForeignKey("cost_policies.id"))
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    universe_collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id")
    )
    sector_collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id")
    )
    corporate_action_collection_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_collections.id")
    )
    spy_instrument_alias_id: Mapped[UUID] = mapped_column(
        ForeignKey("instrument_aliases.id")
    )
    range_start: Mapped[date]
    range_end: Mapped[date]
    completeness_status: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    source_selection_reports: Mapped[list[dict[str, object]]] = mapped_column(JSONB)
    completed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BacktestPriceAuthorityMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "backtest_price_authority_members"

    bundle_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_bundles.id"), primary_key=True
    )
    price_bar_set_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_bar_set_authorities.id"), primary_key=True
    )


class BacktestInstrumentAuthorityMemberRecord(ImmutableCreatedAt, Base):
    __tablename__ = "backtest_instrument_authority_members"

    bundle_id: Mapped[UUID] = mapped_column(
        ForeignKey("backtest_authority_bundles.id"), primary_key=True
    )
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"), primary_key=True)


class BacktestRunRecord(ImmutableCreatedAt, Base):
    """Frozen authority and result identity for one completed historical run."""

    __tablename__ = "backtest_runs"
    __table_args__ = (
        CheckConstraint("start_session <= end_session", name="session_range_ordered"),
        CheckConstraint("initial_nav > 0", name="initial_nav_positive"),
        CheckConstraint("oos_window_count >= 0", name="oos_window_count_nonnegative"),
        CheckConstraint(
            "NOT promotable OR oos_window_count >= 8", name="promotion_requires_eight_windows"
        ),
        CheckConstraint("historical_agent_overlay = false", name="historical_agent_excluded"),
        CheckConstraint("strategy_data_complete = true", name="strategy_data_must_be_complete"),
        CheckConstraint(
            "benchmark_comparison_complete = true",
            name="benchmark_comparisons_must_be_complete",
        ),
        CheckConstraint("promotable = false", name="phase01_promotion_locked"),
        CheckConstraint("random_seed = 0", name="random_seed_fixed"),
        CheckConstraint("protocol_hash ~ '^[0-9a-f]{64}$'", name="protocol_hash_canonical"),
        CheckConstraint("data_hash ~ '^[0-9a-f]{64}$'", name="data_hash_canonical"),
        CheckConstraint("code_hash ~ '^[0-9a-f]{64}$'", name="code_hash_canonical"),
        CheckConstraint("result_hash ~ '^[0-9a-f]{64}$'", name="result_hash_canonical"),
        CheckConstraint("authority_hash ~ '^[0-9a-f]{64}$'", name="authority_hash_canonical"),
        UniqueConstraint("result_hash", name="uq_backtest_runs_result_hash"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    strategy_definition_id: Mapped[UUID] = mapped_column(ForeignKey("strategy_definitions.id"))
    authority_bundle_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_authority_bundles.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    source_policy_id: Mapped[UUID] = mapped_column(ForeignKey("source_policies.id"))
    cost_policy_id: Mapped[UUID] = mapped_column(ForeignKey("cost_policies.id"))
    start_session: Mapped[date]
    end_session: Mapped[date]
    initial_nav: Mapped[Decimal]
    protocol: Mapped[dict[str, object]] = mapped_column(JSONB)
    result_payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    protocol_hash: Mapped[str] = mapped_column(String(64))
    authority_hash: Mapped[str] = mapped_column(String(64))
    data_hash: Mapped[str] = mapped_column(String(64))
    code_hash: Mapped[str] = mapped_column(String(64))
    result_hash: Mapped[str] = mapped_column(String(64))
    random_seed: Mapped[int] = mapped_column(Integer, default=0)
    oos_window_count: Mapped[int] = mapped_column(Integer)
    strategy_data_complete: Mapped[bool] = mapped_column(Boolean)
    benchmark_comparison_complete: Mapped[bool] = mapped_column(Boolean)
    history_requirement_met: Mapped[bool] = mapped_column(Boolean)
    promotable: Mapped[bool] = mapped_column(Boolean, default=False)
    historical_agent_overlay: Mapped[bool] = mapped_column(Boolean, default=False)
    graph_sealed: Mapped[bool] = mapped_column(Boolean, default=False)
    authority_frozen_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BacktestWindowRecord(ImmutableCreatedAt, Base):
    """One train/validation/out-of-sample walk-forward window."""

    __tablename__ = "backtest_windows"
    __table_args__ = (
        UniqueConstraint("backtest_run_id", "ordinal", name="uq_backtest_windows_run_ordinal"),
        CheckConstraint("ordinal >= 1", name="ordinal_positive"),
        CheckConstraint(
            "train_start <= train_end AND train_end <= validation_start "
            "AND validation_start <= validation_end "
            "AND validation_end <= out_of_sample_start "
            "AND out_of_sample_start <= out_of_sample_end",
            name="window_dates_ordered",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    ordinal: Mapped[int] = mapped_column(Integer)
    train_start: Mapped[date]
    train_end: Mapped[date]
    validation_start: Mapped[date]
    validation_end: Mapped[date]
    out_of_sample_start: Mapped[date]
    out_of_sample_end: Mapped[date]
    session_coverage: Mapped[Decimal]
    valid: Mapped[bool] = mapped_column(Boolean)
    invalid_reasons: Mapped[list[str]] = mapped_column(JSONB)
    metrics: Mapped[dict[str, object]] = mapped_column(JSONB)


class BacktestNavPointRecord(ImmutableCreatedAt, Base):
    """Exact Decimal NAV and cash for one evaluated market session."""

    __tablename__ = "backtest_nav_points"
    __table_args__ = (
        UniqueConstraint("backtest_run_id", "session", name="uq_backtest_nav_run_session"),
        CheckConstraint("nav >= 0", name="nav_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    session: Mapped[date]
    nav: Mapped[Decimal]
    cash: Mapped[Decimal]


class BacktestFillRecord(ImmutableCreatedAt, Base):
    """One deterministic next-session simulated fill and its exact cost."""

    __tablename__ = "backtest_fills"
    __table_args__ = (
        CheckConstraint("side IN ('buy','sell')", name="side_allowed"),
        CheckConstraint("signal_session < fill_session", name="next_session_fill_required"),
        CheckConstraint("quantity > 0 AND price > 0 AND notional > 0", name="fill_positive"),
        CheckConstraint("cost >= 0", name="cost_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    signal_session: Mapped[date]
    fill_session: Mapped[date]
    side: Mapped[str] = mapped_column(String(4))
    quantity: Mapped[Decimal]
    price: Mapped[Decimal]
    notional: Mapped[Decimal]
    cost: Mapped[Decimal]


class BacktestOrderOutcomeRecord(ImmutableCreatedAt, Base):
    """One immutable filled or deterministically terminated simulated intent."""

    __tablename__ = "backtest_order_outcomes"
    __table_args__ = (
        UniqueConstraint(
            "backtest_run_id",
            "instrument_id",
            "signal_session",
            "target_session",
            name="uq_backtest_order_outcomes_identity",
        ),
        CheckConstraint("signal_session < target_session", name="target_after_signal"),
        CheckConstraint("side IN ('buy','sell','rebalance')", name="side_allowed"),
        CheckConstraint("status IN ('filled','missing_bar','untradable')", name="status_allowed"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    signal_session: Mapped[date]
    target_session: Mapped[date]
    side: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str] = mapped_column(String(255))


class BacktestMetricRecord(ImmutableCreatedAt, Base):
    """A comparable metric for strategy, benchmark, or sensitivity scenario."""

    __tablename__ = "backtest_metrics"
    __table_args__ = (
        UniqueConstraint(
            "backtest_run_id", "comparison_key", "metric_name", name="uq_backtest_metrics_key"
        ),
        CheckConstraint(
            "comparison_key IN ('strategy','spy','equal_weight','champion',"
            "'cost_half','cost_double','parameter_adv40','parameter_adv60',"
            "'execution_close')",
            name="comparison_key_allowed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    backtest_run_id: Mapped[UUID] = mapped_column(ForeignKey("backtest_runs.id"))
    comparison_key: Mapped[str] = mapped_column(String(32))
    metric_name: Mapped[str] = mapped_column(String(64))
    value: Mapped[Decimal]
    unit: Mapped[str] = mapped_column(String(32))
