"""Immutable broker account, synchronization, snapshot, and cash-flow facts."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class Account(ImmutableCreatedAt, Base):
    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint("status IN ('ACTIVE', 'DISABLED')", name="status_allowed"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(20))

    def __repr__(self) -> str:
        return f"Account(id={self.id!r}, provider={self.provider!r}, status={self.status!r})"


class AccountExternalIdEnvelope(ImmutableCreatedAt, Base):
    __tablename__ = "account_external_id_envelopes"
    __table_args__ = (
        CheckConstraint("envelope_version >= 1", name="envelope_version_positive"),
        CheckConstraint(
            "aad_version = 'external_id_aad_v2'",
            name="aad_version_allowed",
        ),
        UniqueConstraint(
            "account_id",
            "field_context",
            "envelope_version",
            name="uq_account_external_id_envelopes_version",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_account_external_id_envelopes_account_id_accounts")
    )
    provider: Mapped[str] = mapped_column(String(50))
    field_context: Mapped[str] = mapped_column(String(100))
    envelope_version: Mapped[int] = mapped_column(Integer)
    aad_version: Mapped[str] = mapped_column(String(50))
    key_version: Mapped[str] = mapped_column(String(100))
    nonce: Mapped[bytes] = mapped_column(LargeBinary)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    fingerprint: Mapped[str] = mapped_column(String(64))


class Portfolio(ImmutableCreatedAt, Base):
    __tablename__ = "portfolios"
    __table_args__ = (
        UniqueConstraint("account_id", name="uq_portfolios_account_id"),
        UniqueConstraint("id", "account_id", name="uq_portfolios_id_account"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_portfolios_account_id_accounts")
    )
    name: Mapped[str] = mapped_column(String(100))


class BrokerSyncRun(ImmutableCreatedAt, Base):
    __tablename__ = "broker_sync_runs"
    __table_args__ = (
        CheckConstraint(
            "trigger_kind IN ('MANUAL', 'AUTOMATIC')",
            name="trigger_kind_allowed",
        ),
        CheckConstraint(
            "status IN ('ACTIVE', 'READY', 'PARTIAL', 'STALE', "
            "'RECONCILIATION_REQUIRED', 'FAILED')",
            name="status_allowed",
        ),
        CheckConstraint(
            "cash_flow_quality IN ('PASS', 'PARTIAL', 'NOT_RUN')",
            name="cash_flow_quality_allowed",
        ),
        UniqueConstraint(
            "actor_id",
            "route",
            "idempotency_key",
            name="uq_broker_sync_runs_actor_route_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_broker_sync_runs_account_id_accounts")
    )
    portfolio_id: Mapped[UUID] = mapped_column(
        ForeignKey("portfolios.id", name="fk_broker_sync_runs_portfolio_id_portfolios")
    )
    trigger_kind: Mapped[str] = mapped_column(String(20))
    actor_id: Mapped[str] = mapped_column(String(255))
    route: Mapped[str] = mapped_column(String(255))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(40), default="ACTIVE")
    accepted_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_reason: Mapped[str | None] = mapped_column(String(255))
    cash_flow_quality: Mapped[str] = mapped_column(String(20), default="NOT_RUN")


Index(
    "uq_broker_sync_runs_active_account",
    BrokerSyncRun.account_id,
    unique=True,
    postgresql_where=text("status = 'ACTIVE'"),
)


class BrokerSyncRequest(ImmutableCreatedAt, Base):
    __tablename__ = "broker_sync_requests"
    __table_args__ = (
        CheckConstraint(
            "trigger_kind IN ('MANUAL', 'AUTOMATIC')",
            name="trigger_kind_allowed",
        ),
        UniqueConstraint(
            "idempotency_record_id",
            name="uq_broker_sync_requests_idempotency_record_id",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    sync_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_sync_runs.id", name="fk_broker_sync_requests_sync_run_id_runs")
    )
    idempotency_record_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "idempotency_records.id",
            name="fk_broker_sync_requests_idempotency_record_id_records",
        )
    )
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_broker_sync_requests_account_id_accounts")
    )
    trigger_kind: Mapped[str] = mapped_column(String(20))
    accepted_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BrokerSyncAttempt(ImmutableCreatedAt, Base):
    __tablename__ = "broker_sync_attempts"
    __table_args__ = (
        UniqueConstraint("sync_run_id", "attempt", name="uq_broker_sync_attempts_run_attempt"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    sync_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_sync_runs.id", name="fk_broker_sync_attempts_sync_run_id_runs")
    )
    attempt: Mapped[int] = mapped_column(Integer)
    phase: Mapped[str] = mapped_column(String(50))
    completed: Mapped[bool]
    reason: Mapped[str] = mapped_column(String(255))
    source_request_ids: Mapped[list[str]] = mapped_column(JSONB)
    source_observations: Mapped[list[dict[str, object]]] = mapped_column(JSONB)
    raw_facts: Mapped[dict[str, object]] = mapped_column(JSONB)


class PortfolioSnapshotRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        CheckConstraint(
            "status IN ('READY', 'PARTIAL', 'STALE', 'RECONCILIATION_REQUIRED')",
            name="status_allowed",
        ),
        CheckConstraint(
            "(status = 'STALE' AND stale_from_snapshot_id IS NOT NULL) OR "
            "(status <> 'STALE' AND stale_from_snapshot_id IS NULL)",
            name="stale_source_required",
        ),
        UniqueConstraint("sync_run_id", name="uq_portfolio_snapshots_sync_run_id"),
        UniqueConstraint("id", "portfolio_id", name="uq_portfolio_snapshots_id_portfolio"),
        ForeignKeyConstraint(
            ["stale_from_snapshot_id", "portfolio_id"],
            ["portfolio_snapshots.id", "portfolio_snapshots.portfolio_id"],
            name="fk_portfolio_snapshots_stale_source_same_portfolio",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_id: Mapped[UUID] = mapped_column(
        ForeignKey("portfolios.id", name="fk_portfolio_snapshots_portfolio_id_portfolios")
    )
    sync_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_sync_runs.id", name="fk_portfolio_snapshots_sync_run_id_runs")
    )
    stale_from_snapshot_id: Mapped[UUID | None]
    status: Mapped[str] = mapped_column(String(40))
    as_of: Mapped[datetime | None] = mapped_column(UTCDateTime())
    position_hash_version: Mapped[str] = mapped_column(String(50))
    position_hash: Mapped[str] = mapped_column(String(64))
    order_hash_version: Mapped[str] = mapped_column(String(50))
    order_hash: Mapped[str] = mapped_column(String(64))
    source_request_ids: Mapped[list[str]] = mapped_column(JSONB)
    source_observations: Mapped[list[dict[str, object]]] = mapped_column(JSONB)


class CashSnapshot(ImmutableCreatedAt, Base):
    __tablename__ = "cash_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_snapshot_id",
            "currency",
            name="uq_cash_snapshots_snapshot_currency",
        ),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "portfolio_snapshots.id",
            name="fk_cash_snapshots_portfolio_snapshot_id_snapshots",
        )
    )
    currency: Mapped[str] = mapped_column(String(3))
    cash_balance: Mapped[Decimal]
    available_cash: Mapped[Decimal]
    provider_as_of: Mapped[datetime | None] = mapped_column(UTCDateTime())


class PositionSnapshot(ImmutableCreatedAt, Base):
    __tablename__ = "position_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_snapshot_id",
            "instrument_id",
            name="uq_position_snapshots_snapshot_instrument",
        ),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "portfolio_snapshots.id",
            name="fk_position_snapshots_portfolio_snapshot_id_snapshots",
        )
    )
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_position_snapshots_instrument_id_instruments")
    )
    currency: Mapped[str] = mapped_column(String(3))
    quantity: Mapped[Decimal]
    available_quantity: Mapped[Decimal]
    frozen_quantity: Mapped[Decimal]
    avg_cost: Mapped[Decimal | None]
    diluted_cost: Mapped[Decimal | None]
    current_price: Mapped[Decimal | None]
    provider_as_of: Mapped[datetime | None] = mapped_column(UTCDateTime())


class BrokerOrderSnapshot(ImmutableCreatedAt, Base):
    __tablename__ = "broker_order_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_snapshot_id",
            "provider_order_id",
            name="uq_broker_order_snapshots_snapshot_provider_order",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            "portfolio_snapshots.id",
            name="fk_broker_order_snapshots_portfolio_snapshot_id_snapshots",
        )
    )
    provider: Mapped[str] = mapped_column(String(50))
    provider_order_id: Mapped[str] = mapped_column(String(255))
    instrument_id: Mapped[UUID] = mapped_column(
        ForeignKey("instruments.id", name="fk_broker_order_snapshots_instrument_id_instruments")
    )
    side: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[Decimal]
    filled_quantity: Mapped[Decimal]
    price: Mapped[Decimal | None]
    status: Mapped[str] = mapped_column(String(30))
    updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class BrokerCashFlow(ImmutableCreatedAt, Base):
    __tablename__ = "broker_cash_flows"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_cash_flow_id",
            name="uq_broker_cash_flows_provider_id",
        ),
        CheckConstraint(
            "identity_quality IN ('STABLE', 'FALLBACK_PARTIAL')",
            name="identity_quality_allowed",
        ),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_canonical"),
        CheckConstraint(
            "occurred_at IS NOT NULL OR identity_quality = 'STABLE'",
            name="date_only_identity_stable",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_broker_cash_flows_account_id_accounts")
    )
    sync_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_sync_runs.id", name="fk_broker_cash_flows_sync_run_id_runs")
    )
    provider: Mapped[str] = mapped_column(String(50))
    provider_cash_flow_id: Mapped[str] = mapped_column(String(255))
    identity_version: Mapped[str] = mapped_column(String(50))
    identity_quality: Mapped[str] = mapped_column(String(30))
    currency: Mapped[str] = mapped_column(String(3))
    amount: Mapped[Decimal]
    occurred_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    provider_business_date: Mapped[date] = mapped_column(Date)
    kind: Mapped[str] = mapped_column(String(100))
    source_request_id: Mapped[str] = mapped_column(String(128))
    response_as_of: Mapped[datetime] = mapped_column(UTCDateTime())


class BrokerCashFlowWatermark(ImmutableCreatedAt, Base):
    __tablename__ = "broker_cash_flow_watermarks"
    __table_args__ = (
        UniqueConstraint("account_id", name="uq_cash_flow_watermarks_account_id"),
        CheckConstraint(
            "identity_quality IN ('STABLE', 'FALLBACK_PARTIAL')",
            name="identity_quality_allowed",
        ),
        CheckConstraint("version >= 1", name="version_positive"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_cash_flow_watermarks_account_id_accounts")
    )
    provider_business_date: Mapped[date] = mapped_column(Date)
    identity_quality: Mapped[str] = mapped_column(String(30))
    version: Mapped[int] = mapped_column(Integer, default=1)
