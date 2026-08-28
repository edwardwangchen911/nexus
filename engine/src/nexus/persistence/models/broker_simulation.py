"""Persistence records for the isolated broker-simulation authority."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import DECIMAL_NUMERIC, Base, ImmutableCreatedAt, UTCDateTime

_HASH_CHECK = " ~ '^[0-9a-f]{64}$'"


class BrokerAccountBindingRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_account_bindings"
    __table_args__ = (
        UniqueConstraint(
            "external_account_id_digest", name="uq_broker_account_bindings_external_digest"
        ),
        CheckConstraint(
            "provider = 'fosun' AND environment = 'simulation' "
            "AND account_type = 'simulation'",
            name="simulation_only",
        ),
        CheckConstraint(
            f"external_account_id_digest{_HASH_CHECK} AND "
            f"snapshot_identity_hash{_HASH_CHECK}",
            name="hashes",
        ),
        CheckConstraint(
            "reconciliation_status IN "
            "('pending','running','matched','conflict','acknowledged')",
            name="reconciliation_status",
        ),
        CheckConstraint("version >= 1", name="version"),
        CheckConstraint("btrim(redacted_label) <> ''", name="label"),
        CheckConstraint(
            "(NOT locked AND lock_reason IS NULL) OR "
            "(locked AND btrim(lock_reason) <> '')",
            name="lock_reason",
        ),
        CheckConstraint(
            "reconciliation_status <> 'conflict' OR locked",
            name="conflict_locked",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(20))
    environment: Mapped[str] = mapped_column(String(20))
    account_type: Mapped[str] = mapped_column(String(20))
    redacted_label: Mapped[str] = mapped_column(String(100))
    external_account_id_digest: Mapped[str] = mapped_column(String(64))
    snapshot_identity_hash: Mapped[str] = mapped_column(String(64))
    capability_subject: Mapped[str] = mapped_column(String(255))
    capability_expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    reconciliation_status: Mapped[str] = mapped_column(String(24))
    locked: Mapped[bool] = mapped_column(Boolean)
    lock_reason: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(BigInteger)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class BrokerExecutionIntentRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_execution_intents"
    __table_args__ = (
        UniqueConstraint("body_hash", name="uq_broker_execution_intents_body_hash"),
        CheckConstraint(
            "status IN ('draft','prepared','approved','rejected','expired','dispatching',"
            "'completed','partially_completed','failed','submission_unknown',"
            "'reconciling','reconciled')",
            name="status",
        ),
        CheckConstraint("version >= 1 AND total_notional > 0", name="values"),
        CheckConstraint(
            f"body_hash{_HASH_CHECK} AND risk_policy_hash{_HASH_CHECK} "
            f"AND execution_model_hash{_HASH_CHECK}",
            name="hashes",
        ),
        CheckConstraint(
            "cardinality(evidence_hashes) > 0 AND expires_at > prepared_at",
            name="authority_window",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_binding_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_account_bindings.id")
    )
    proposal_group_id: Mapped[UUID]
    strategy_decision_id: Mapped[UUID]
    portfolio_snapshot_id: Mapped[UUID]
    evidence_hashes: Mapped[list[str]] = mapped_column(ARRAY(String(64)))
    risk_policy_hash: Mapped[str] = mapped_column(String(64))
    execution_model_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(BigInteger)
    body_hash: Mapped[str] = mapped_column(String(64))
    total_notional: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    prepared_at: Mapped[datetime] = mapped_column(UTCDateTime())
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BrokerExecutionIntentOrderRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_execution_intent_orders"
    __table_args__ = (
        UniqueConstraint(
            "intent_id", "ordinal", name="uq_broker_execution_intent_orders_ordinal"
        ),
        UniqueConstraint(
            "intent_id",
            "instrument_id",
            name="uq_broker_execution_intent_orders_instrument",
        ),
        UniqueConstraint("order_hash", name="uq_broker_execution_intent_orders_hash"),
        CheckConstraint("ordinal >= 1 AND quantity > 0", name="values"),
        CheckConstraint(
            "side IN ('buy','sell') AND currency = 'USD' AND order_type = 'limit' "
            "AND time_in_force = 'DAY' AND session = 'regular'",
            name="scope",
        ),
        CheckConstraint(
            "status IN ('not_sent','acknowledged','working','partially_filled','filled',"
            "'cancelled','rejected','expired','reconciling')",
            name="status",
        ),
        CheckConstraint(
            f"order_hash{_HASH_CHECK} AND limit_price > 0 "
            "AND quantity = trunc(quantity)",
            name="order",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    intent_id: Mapped[UUID] = mapped_column(ForeignKey("broker_execution_intents.id"))
    ordinal: Mapped[int] = mapped_column(Integer)
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    side: Mapped[str] = mapped_column(String(4))
    quantity: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    limit_price: Mapped[Decimal] = mapped_column(DECIMAL_NUMERIC)
    currency: Mapped[str] = mapped_column(String(3))
    order_type: Mapped[str] = mapped_column(String(10))
    time_in_force: Mapped[str] = mapped_column(String(10))
    session: Mapped[str] = mapped_column(String(10))
    status: Mapped[str] = mapped_column(String(24))
    order_hash: Mapped[str] = mapped_column(String(64))


class BrokerBatchApprovalRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_batch_approvals"
    __table_args__ = (
        UniqueConstraint(
            "intent_id",
            "intent_version",
            "intent_body_hash",
            name="uq_broker_batch_approvals_authority",
        ),
        UniqueConstraint(
            "actor_id", "idempotency_key", name="uq_broker_batch_approvals_idempotency"
        ),
        CheckConstraint("intent_version >= 1 AND expires_at > approved_at", name="values"),
        CheckConstraint(
            f"intent_body_hash{_HASH_CHECK} AND request_body_hash{_HASH_CHECK}",
            name="hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    intent_id: Mapped[UUID] = mapped_column(ForeignKey("broker_execution_intents.id"))
    intent_version: Mapped[int] = mapped_column(BigInteger)
    intent_body_hash: Mapped[str] = mapped_column(String(64))
    request_body_hash: Mapped[str] = mapped_column(String(64))
    actor_id: Mapped[str] = mapped_column(String(255))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    approved_at: Mapped[datetime] = mapped_column(UTCDateTime())
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())


class BrokerCommandRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_commands"
    __table_args__ = (
        UniqueConstraint("command_hash", name="uq_broker_commands_hash"),
        UniqueConstraint("idempotency_key", name="uq_broker_commands_idempotency"),
        CheckConstraint("kind IN ('create','modify','cancel')", name="kind"),
        CheckConstraint(
            "status IN ('prepared','sending','acknowledged','rejected','transport_failed',"
            "'result_unknown','reconciled','reconciliation_required')",
            name="status",
        ),
        CheckConstraint("version >= 1 AND expected_event_sequence >= 0", name="values"),
        CheckConstraint(f"command_hash{_HASH_CHECK}", name="hash"),
        CheckConstraint(
            "(lease_token IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="lease_pair",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    intent_order_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_execution_intent_orders.id")
    )
    account_binding_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_account_bindings.id")
    )
    approval_id: Mapped[UUID] = mapped_column(ForeignKey("broker_batch_approvals.id"))
    kind: Mapped[str] = mapped_column(String(10))
    status: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(BigInteger)
    expected_event_sequence: Mapped[int] = mapped_column(BigInteger)
    command_hash: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    lease_token: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class BrokerCommandAttemptRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_command_attempts"
    __table_args__ = (
        UniqueConstraint("command_id", name="uq_broker_command_attempts_command"),
        UniqueConstraint(
            "command_id", "attempt_number", name="uq_broker_command_attempts_number"
        ),
        CheckConstraint("attempt_number = 1", name="single_attempt"),
        CheckConstraint(
            "safe_outcome_code IS NULL OR safe_outcome_code IN "
            "('acknowledged','rejected','transport_failed','result_unknown')",
            name="outcome",
        ),
        CheckConstraint(
            f"result_hash IS NULL OR result_hash{_HASH_CHECK}", name="result_hash"
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at", name="time_order"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    command_id: Mapped[UUID] = mapped_column(ForeignKey("broker_commands.id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    safe_outcome_code: Mapped[str | None] = mapped_column(String(32))
    result_hash: Mapped[str | None] = mapped_column(String(64))


class BrokerOrderEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_order_events"
    __table_args__ = (
        UniqueConstraint(
            "account_binding_id", "event_sequence", name="uq_broker_order_events_sequence"
        ),
        UniqueConstraint("event_hash", name="uq_broker_order_events_hash"),
        CheckConstraint("event_sequence >= 1", name="sequence"),
        CheckConstraint(
            "source IN ('internal','gateway','poll','webhook_candidate','recovery')",
            name="source",
        ),
        CheckConstraint(
            "order_status IN ('not_sent','acknowledged','working','partially_filled',"
            "'filled','cancelled','rejected','expired','reconciling')",
            name="status",
        ),
        CheckConstraint(
            f"payload_hash{_HASH_CHECK} AND previous_hash{_HASH_CHECK} "
            f"AND event_hash{_HASH_CHECK}",
            name="hashes",
        ),
        CheckConstraint(
            f"provider_order_id_digest IS NULL OR provider_order_id_digest{_HASH_CHECK}",
            name="provider_digest",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_binding_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_account_bindings.id")
    )
    intent_order_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_execution_intent_orders.id")
    )
    command_id: Mapped[UUID | None] = mapped_column(ForeignKey("broker_commands.id"))
    event_sequence: Mapped[int] = mapped_column(BigInteger)
    source: Mapped[str] = mapped_column(String(24))
    order_status: Mapped[str] = mapped_column(String(24))
    provider_order_id_digest: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    payload_hash: Mapped[str] = mapped_column(String(64))
    previous_hash: Mapped[str] = mapped_column(String(64))
    event_hash: Mapped[str] = mapped_column(String(64))


Index(
    "uq_broker_order_events_provider_order",
    BrokerOrderEventRecord.provider_order_id_digest,
    unique=True,
    postgresql_where=text("provider_order_id_digest IS NOT NULL"),
)


class BrokerWebhookReceiptRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_webhook_receipts"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_broker_webhook_receipts_event_id"),
        CheckConstraint("body_length BETWEEN 0 AND 65536", name="body_length"),
        CheckConstraint(
            "authentication_status IN ('authenticated','rejected','not_admitted')",
            name="authentication",
        ),
        CheckConstraint(f"body_hash{_HASH_CHECK}", name="body_hash"),
        CheckConstraint("btrim(safe_code) <> ''", name="safe_code"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_binding_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("broker_account_bindings.id")
    )
    event_id: Mapped[str] = mapped_column(String(255))
    body_hash: Mapped[str] = mapped_column(String(64))
    body_length: Mapped[int] = mapped_column(Integer)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())
    authentication_status: Mapped[str] = mapped_column(String(20))
    safe_code: Mapped[str] = mapped_column(String(64))


class BrokerReconciliationRunRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_reconciliation_runs"
    __table_args__ = (
        UniqueConstraint("run_hash", name="uq_broker_reconciliation_runs_hash"),
        CheckConstraint(
            "status IN ('pending','running','matched','conflict','acknowledged')",
            name="status",
        ),
        CheckConstraint(
            f"run_hash{_HASH_CHECK} AND "
            f"(orders_hash IS NULL OR orders_hash{_HASH_CHECK}) AND "
            f"(positions_hash IS NULL OR positions_hash{_HASH_CHECK}) AND "
            f"(cash_hash IS NULL OR cash_hash{_HASH_CHECK}) AND "
            f"(cash_flows_hash IS NULL OR cash_flows_hash{_HASH_CHECK})",
            name="hashes",
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at", name="time_order"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_binding_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_account_bindings.id")
    )
    trigger_reason: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(24))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    orders_hash: Mapped[str | None] = mapped_column(String(64))
    positions_hash: Mapped[str | None] = mapped_column(String(64))
    cash_hash: Mapped[str | None] = mapped_column(String(64))
    cash_flows_hash: Mapped[str | None] = mapped_column(String(64))
    run_hash: Mapped[str] = mapped_column(String(64))
    acknowledged_by: Mapped[str | None] = mapped_column(String(255))
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class BrokerReconciliationDiffRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_reconciliation_diffs"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "diff_kind", "internal_hash", "broker_hash",
            name="uq_broker_reconciliation_diffs_fact",
        ),
        CheckConstraint(
            "diff_kind IN ('orders','positions','cash','cash_flows')", name="kind"
        ),
        CheckConstraint(
            f"internal_hash{_HASH_CHECK} AND broker_hash{_HASH_CHECK}", name="hashes"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("broker_reconciliation_runs.id"))
    diff_kind: Mapped[str] = mapped_column(String(20))
    internal_hash: Mapped[str] = mapped_column(String(64))
    broker_hash: Mapped[str] = mapped_column(String(64))
    details: Mapped[dict[str, object]] = mapped_column(JSONB)
    resolved: Mapped[bool] = mapped_column(Boolean)


class BrokerSimulationAdmissionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "broker_simulation_admissions"
    __table_args__ = (
        UniqueConstraint("subject", "manifest_hash", name="uq_broker_admissions_subject"),
        CheckConstraint(
            "capability_status IN ('OFFLINE_FIXTURE','READ_ONLY_ADMITTED',"
            "'READ_ONLY_ACTIVATED','SIMULATION_CONTRACT_ADMITTED',"
            "'SIMULATION_WRITE_ACTIVATED')",
            name="capability",
        ),
        CheckConstraint(
            f"manifest_hash{_HASH_CHECK} AND artifact_hash{_HASH_CHECK}", name="hashes"
        ),
        CheckConstraint("valid_until > valid_from", name="window"),
        CheckConstraint(
            "(NOT revoked AND revoked_at IS NULL) OR (revoked AND revoked_at IS NOT NULL)",
            name="revocation",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    subject: Mapped[str] = mapped_column(String(255))
    manifest_hash: Mapped[str] = mapped_column(String(64))
    artifact_hash: Mapped[str] = mapped_column(String(64))
    capability_status: Mapped[str] = mapped_column(String(40))
    valid_from: Mapped[datetime] = mapped_column(UTCDateTime())
    valid_until: Mapped[datetime] = mapped_column(UTCDateTime())
    revoked: Mapped[bool] = mapped_column(Boolean)
    revoked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


Index(
    "uq_broker_simulation_admissions_active",
    BrokerSimulationAdmissionRecord.subject,
    unique=True,
    postgresql_where=text("NOT revoked"),
)
