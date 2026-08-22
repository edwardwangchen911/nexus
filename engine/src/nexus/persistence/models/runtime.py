"""Runtime coordination, idempotency, and audit mappings."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint, func
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
    __table_args__ = (UniqueConstraint("job_key", name="uq_jobs_job_key"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_key: Mapped[str] = mapped_column(String(255))
    input_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(50))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class JobLease(ImmutableCreatedAt, Base):
    __tablename__ = "job_leases"
    __table_args__ = (UniqueConstraint("job_id", name="uq_job_leases_job_id"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", name="fk_job_leases_job_id_jobs"))
    worker_id: Mapped[str] = mapped_column(String(255))
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())


class OutboxEvent(ImmutableCreatedAt, Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    event_type: Mapped[str] = mapped_column(String(255))
    aggregate_id: Mapped[UUID]
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


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

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    actor_id: Mapped[str] = mapped_column(String(255))
    action: Mapped[str] = mapped_column(String(255))
    schema_version: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)


class UsedNonce(ImmutableCreatedAt, Base):
    __tablename__ = "used_nonces"
    __table_args__ = (UniqueConstraint("key_id", "nonce", name="uq_used_nonces_key_nonce"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    key_id: Mapped[str] = mapped_column(String(255))
    nonce: Mapped[str] = mapped_column(String(255))
    used_at: Mapped[datetime] = mapped_column(UTCDateTime())
