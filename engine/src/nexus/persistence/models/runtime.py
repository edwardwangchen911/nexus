"""Runtime coordination, idempotency, and audit mappings."""

from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class IntegrationStatus(ImmutableCreatedAt, Base):
    __tablename__ = "integration_status"
    __table_args__ = (UniqueConstraint("provider", name="uq_integration_status_provider"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50))
    last_success_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    mode_changed_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, server_default=func.now()
    )
    capability_invalidated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    admitted_contract_hash: Mapped[str | None] = mapped_column(String(64))


class Job(ImmutableCreatedAt, Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("job_key", name="uq_jobs_job_key"),
        CheckConstraint(
            "status IN ('queued','running','succeeded','failed','skipped','dead_letter')",
            name="jobs_status_allowed",
        ),
        CheckConstraint(
            "queue IN ('general','provider','broker','strategy_rnd')",
            name="jobs_queue_allowed",
        ),
        CheckConstraint(
            "scope_kind IN ('global','portfolio','instrument','provider') "
            "AND scope_id ~ '^[A-Za-z0-9._-]{1,100}$'",
            name="jobs_scope_allowed",
        ),
        CheckConstraint(
            "retry_policy IN ('automatic_safe','manual_safe','never') "
            "AND max_attempts BETWEEN 1 AND 3 "
            "AND attempt_count BETWEEN 0 AND max_attempts + manual_retry_count "
            "AND manual_retry_count BETWEEN 0 AND 2",
            name="jobs_retry_allowed",
        ),
        CheckConstraint(
            "input_hash ~ '^[0-9a-f]{64}$' AND jsonb_typeof(payload) = 'object'",
            name="jobs_hash_payload",
        ),
        CheckConstraint(
            "(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
            "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
            "(status IN ('succeeded','failed','skipped','dead_letter') "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL "
            "AND finished_at >= started_at)",
            name="jobs_time_shape",
        ),
        Index(
            "ix_jobs_claim",
            "queue",
            "status",
            "priority",
            "available_at",
            "created_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_key: Mapped[str] = mapped_column(String(512))
    input_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(50))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    job_type: Mapped[str] = mapped_column(String(50))
    queue: Mapped[str] = mapped_column(String(20))
    scope_kind: Mapped[str] = mapped_column(String(20))
    scope_id: Mapped[str] = mapped_column(String(100))
    effective_at: Mapped[datetime] = mapped_column(UTCDateTime())
    priority: Mapped[int] = mapped_column(Integer)
    retry_policy: Mapped[str] = mapped_column(String(20))
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default="3"
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    manual_retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    result_code: Mapped[str | None] = mapped_column(String(100))
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )


class JobLease(ImmutableCreatedAt, Base):
    __tablename__ = "job_leases"
    __table_args__ = (
        UniqueConstraint("job_id", name="uq_job_leases_job_id"),
        UniqueConstraint("lease_token", name="uq_job_leases_lease_token"),
        CheckConstraint(
            "fence_token >= 1 AND expires_at > heartbeat_at "
            "AND heartbeat_at >= acquired_at",
            name="job_leases_values",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", name="fk_job_leases_job_id_jobs"))
    worker_id: Mapped[str] = mapped_column(String(255))
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    lease_token: Mapped[UUID]
    fence_token: Mapped[int] = mapped_column(BigInteger)
    acquired_at: Mapped[datetime] = mapped_column(UTCDateTime())
    heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime())


class JobAttempt(ImmutableCreatedAt, Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_no", name="uq_job_attempts_job_no"),
        UniqueConstraint("lease_token", name="uq_job_attempts_lease_token"),
        CheckConstraint("attempt_no >= 1", name="attempt_no_positive"),
        CheckConstraint("fence_token >= 1", name="fence_positive"),
        CheckConstraint(
            "outcome IN ('running','succeeded','skipped','failed','lost')",
            name="outcome_allowed",
        ),
        CheckConstraint(
            "(outcome = 'running' AND finished_at IS NULL AND error_code IS NULL) OR "
            "(outcome IN ('succeeded','skipped') AND finished_at IS NOT NULL "
            "AND error_code IS NULL) OR "
            "(outcome IN ('failed','lost') AND finished_at IS NOT NULL "
            "AND error_code IS NOT NULL)",
            name="terminal_shape",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", name="fk_job_attempts_job")
    )
    attempt_no: Mapped[int] = mapped_column(Integer)
    lease_token: Mapped[UUID]
    fence_token: Mapped[int] = mapped_column(BigInteger)
    worker_id: Mapped[str] = mapped_column(String(255))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    outcome: Mapped[str] = mapped_column(String(20))
    error_code: Mapped[str | None] = mapped_column(String(100))


class JobDependency(ImmutableCreatedAt, Base):
    __tablename__ = "job_dependencies"
    __table_args__ = (
        UniqueConstraint("job_id", "depends_on_job_id", name="uq_job_dependencies_edge"),
        CheckConstraint("job_id <> depends_on_job_id", name="not_self"),
        CheckConstraint(
            "required_outcome IN ('succeeded','succeeded_or_skipped')",
            name="required_outcome_allowed",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", name="fk_job_dependencies_job")
    )
    depends_on_job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", name="fk_job_dependencies_parent")
    )
    required_outcome: Mapped[str] = mapped_column(String(30))


class WorkerHeartbeat(ImmutableCreatedAt, Base):
    __tablename__ = "worker_heartbeats"
    __table_args__ = (
        UniqueConstraint(
            "queue", "worker_id", name="uq_worker_heartbeats_queue_worker"
        ),
        CheckConstraint(
            "queue IN ('general','provider','broker','strategy_rnd')", name="queue_allowed"
        ),
        CheckConstraint("seen_at >= started_at", name="time_order"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    queue: Mapped[str] = mapped_column(String(20))
    worker_id: Mapped[str] = mapped_column(String(255))
    process_version: Mapped[str] = mapped_column(String(64))
    migration_head: Mapped[str] = mapped_column(String(64))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    seen_at: Mapped[datetime] = mapped_column(UTCDateTime())


class OutboxEvent(ImmutableCreatedAt, Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued','delivering','published','dead_letter')",
            name="outbox_status_allowed",
        ),
        CheckConstraint(
            "max_attempts BETWEEN 1 AND 10 AND attempt_count BETWEEN 0 AND max_attempts",
            name="outbox_attempts_bounded",
        ),
        CheckConstraint(
            "(status = 'published' AND published_at IS NOT NULL "
            "AND dead_lettered_at IS NULL) OR "
            "(status = 'dead_letter' AND published_at IS NULL "
            "AND dead_lettered_at IS NOT NULL) OR "
            "(status IN ('queued','delivering') AND published_at IS NULL "
            "AND dead_lettered_at IS NULL)",
            name="outbox_terminal_shape",
        ),
        Index(
            "ix_outbox_events_delivery",
            "status",
            "available_at",
            "created_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    event_type: Mapped[str] = mapped_column(String(255))
    aggregate_id: Mapped[UUID]
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="queued", server_default="queued"
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default="3"
    )
    available_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, server_default=func.now()
    )
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class OutboxDeliveryAttempt(ImmutableCreatedAt, Base):
    __tablename__ = "outbox_delivery_attempts"
    __table_args__ = (
        UniqueConstraint(
            "outbox_event_id",
            "attempt_no",
            name="uq_outbox_delivery_attempts_event_no",
        ),
        UniqueConstraint(
            "lease_token", name="uq_outbox_delivery_attempts_lease_token"
        ),
        CheckConstraint("attempt_no >= 1", name="attempt_no_positive"),
        CheckConstraint(
            "outcome IN ('delivering','published','failed','lost')",
            name="outcome_allowed",
        ),
        CheckConstraint(
            "(outcome = 'delivering' AND finished_at IS NULL AND error_code IS NULL) OR "
            "(outcome = 'published' AND finished_at IS NOT NULL AND error_code IS NULL) OR "
            "(outcome IN ('failed','lost') AND finished_at IS NOT NULL "
            "AND error_code IS NOT NULL)",
            name="terminal_shape",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    outbox_event_id: Mapped[UUID] = mapped_column(
        ForeignKey("outbox_events.id", name="fk_outbox_delivery_attempts_event")
    )
    attempt_no: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str] = mapped_column(String(255))
    lease_token: Mapped[UUID]
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    outcome: Mapped[str] = mapped_column(String(20))
    error_code: Mapped[str | None] = mapped_column(String(100))


class IdempotencyRecord(ImmutableCreatedAt, Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "actor_id",
            "route",
            "idempotency_key",
            name="uq_idempotency_records_actor_route_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    actor_id: Mapped[str] = mapped_column(String(255))
    route: Mapped[str] = mapped_column(String(255))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    schema_version: Mapped[str] = mapped_column(String(32))
    response_payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class AuditEvent(ImmutableCreatedAt, Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        UniqueConstraint("event_seq", name="uq_audit_events_event_seq"),
        UniqueConstraint("event_hash", name="uq_audit_events_event_hash"),
        CheckConstraint(
            "previous_hash ~ '^[0-9a-f]{64}$' AND "
            "event_hash ~ '^[0-9a-f]{64}$' "
            "AND chain_version = 'audit_jcs_sha256_v1'",
            name="audit_events_hashes",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    actor_id: Mapped[str] = mapped_column(String(255))
    action: Mapped[str] = mapped_column(String(255))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    event_seq: Mapped[int] = mapped_column(BigInteger)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    previous_hash: Mapped[str] = mapped_column(String(64))
    canonical_event: Mapped[str] = mapped_column(Text)
    event_hash: Mapped[str] = mapped_column(String(64))
    chain_version: Mapped[str] = mapped_column(String(32))


class AuditChainManifest(ImmutableCreatedAt, Base):
    __tablename__ = "audit_chain_manifests"
    __table_args__ = (
        UniqueConstraint("session", name="uq_audit_chain_manifests_session"),
        UniqueConstraint("head_hash", name="uq_audit_chain_manifests_head_hash"),
        CheckConstraint(
            "first_event_seq >= 1 AND last_event_seq >= first_event_seq",
            name="seq_bounds",
        ),
        CheckConstraint(
            "head_hash ~ '^[0-9a-f]{64}$' AND "
            "object_sha256 ~ '^[0-9a-f]{64}$'",
            name="hashes_canonical",
        ),
        CheckConstraint("byte_size > 0", name="byte_size_positive"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    session: Mapped[date] = mapped_column(Date)
    first_event_seq: Mapped[int] = mapped_column(BigInteger)
    last_event_seq: Mapped[int] = mapped_column(BigInteger)
    head_hash: Mapped[str] = mapped_column(String(64))
    object_uri: Mapped[str] = mapped_column(Text)
    object_sha256: Mapped[str] = mapped_column(String(64))
    byte_size: Mapped[int] = mapped_column(BigInteger)


class UsedNonce(ImmutableCreatedAt, Base):
    __tablename__ = "used_nonces"
    __table_args__ = (UniqueConstraint("key_id", "nonce", name="uq_used_nonces_key_nonce"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    key_id: Mapped[str] = mapped_column(String(255))
    nonce: Mapped[str] = mapped_column(String(255))
    used_at: Mapped[datetime] = mapped_column(UTCDateTime())
