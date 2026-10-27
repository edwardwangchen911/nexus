"""Immutable identity and append-only receipts for explicit manual Paper requests."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class ManualPaperRequest(ImmutableCreatedAt, Base):
    __tablename__ = "manual_paper_requests"
    __table_args__ = (
        UniqueConstraint("account_id", "key_hash", name="uq_manual_paper_request_key"),
        CheckConstraint(
            "mode IN ('research_only','research_and_paper','research_and_monitor')",
            name="mode_valid",
        ),
        CheckConstraint(
            "key_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$' "
            "AND batch_hash ~ '^[0-9a-f]{64}$' AND model_hash ~ '^[0-9a-f]{64}$'",
            name="hashes_valid",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    candidate_batch_id: Mapped[UUID] = mapped_column(ForeignKey("agent_paper_candidate_batches.id"))
    model_config_id: Mapped[UUID] = mapped_column(ForeignKey("agent_model_configs.id"))
    research_profile: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    contract_version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    mode: Mapped[str] = mapped_column(String(32))
    key_hash: Mapped[str] = mapped_column(String(64))
    request_hash: Mapped[str] = mapped_column(String(64))
    batch_hash: Mapped[str] = mapped_column(String(64))
    model_hash: Mapped[str] = mapped_column(String(64))
    requested_at: Mapped[datetime] = mapped_column(UTCDateTime())


class ManualPaperRequestEvent(ImmutableCreatedAt, Base):
    __tablename__ = "manual_paper_request_events"
    __table_args__ = (
        UniqueConstraint("request_id", "sequence", name="uq_manual_paper_event_sequence"),
        Index(
            "uq_manual_paper_active_cycle",
            "cycle_id",
            unique=True,
            postgresql_where=text("status='active'"),
        ),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        CheckConstraint(
            "status IN ('queued','preparing','active','completed','unavailable','unknown')",
            name="status_valid",
        ),
        CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_valid"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    request_id: Mapped[UUID] = mapped_column(ForeignKey("manual_paper_requests.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24))
    code: Mapped[str] = mapped_column(String(64))
    cycle_id: Mapped[UUID | None] = mapped_column(ForeignKey("agent_paper_cycles.id"))
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())
    event_hash: Mapped[str] = mapped_column(String(64))


class ResearchRequestManifest(ImmutableCreatedAt, Base):
    """One immutable complete candidate set, independent of a Paper cycle."""

    __tablename__ = "research_request_manifests"
    __table_args__ = (
        UniqueConstraint("request_id", name="uq_research_manifest_request"),
        CheckConstraint("member_count BETWEEN 1 AND 5", name="member_count_valid"),
        CheckConstraint("manifest_hash ~ '^[0-9a-f]{64}$'", name="manifest_hash_valid"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    request_id: Mapped[UUID] = mapped_column(ForeignKey("manual_paper_requests.id"))
    research_as_of: Mapped[datetime] = mapped_column(UTCDateTime())
    member_count: Mapped[int] = mapped_column(Integer)
    manifest_hash: Mapped[str] = mapped_column(String(64))


class ResearchRequestMember(ImmutableCreatedAt, Base):
    __tablename__ = "research_request_members"
    __table_args__ = (
        UniqueConstraint("manifest_id", "instrument_id", name="uq_research_member_instrument"),
        UniqueConstraint("manifest_id", "ordinal", name="uq_research_member_ordinal"),
        UniqueConstraint("run_id", name="uq_research_member_run"),
        UniqueConstraint("job_id", name="uq_research_member_job"),
        CheckConstraint("ordinal BETWEEN 0 AND 4", name="ordinal_valid"),
        CheckConstraint("member_hash ~ '^[0-9a-f]{64}$'", name="member_hash_valid"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    manifest_id: Mapped[UUID] = mapped_column(ForeignKey("research_request_manifests.id"))
    ordinal: Mapped[int] = mapped_column(Integer)
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id"))
    member_hash: Mapped[str] = mapped_column(String(64))
