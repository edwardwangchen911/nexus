"""Append-only persistence mappings for bounded, read-only Agent research."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class AgentPromptVersionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_prompt_versions"
    __table_args__ = (
        UniqueConstraint("version", name="uq_agent_prompt_versions_version"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    version: Mapped[str] = mapped_column(String(100))
    content_hash: Mapped[str] = mapped_column(String(64))
    content: Mapped[str] = mapped_column(Text)


class AgentModelConfigRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_model_configs"
    __table_args__ = (
        UniqueConstraint("model_id", "version", name="uq_agent_model_configs_identity"),
        CheckConstraint("config_hash ~ '^[0-9a-f]{64}$'", name="config_hash_canonical"),
        CheckConstraint("max_tool_calls BETWEEN 0 AND 8", name="max_tool_calls_bounded"),
        CheckConstraint("max_output_tokens BETWEEN 1 AND 16384", name="max_output_tokens_bounded"),
        CheckConstraint("max_cost_usd >= 0", name="max_cost_nonnegative"),
        CheckConstraint("pricing_expires_at > pricing_available_at", name="pricing_time_ordered"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    model_id: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    config_hash: Mapped[str] = mapped_column(String(64))
    max_tool_calls: Mapped[int] = mapped_column(Integer)
    max_output_tokens: Mapped[int] = mapped_column(Integer)
    max_cost_usd: Mapped[Decimal]
    pricing_available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    pricing_expires_at: Mapped[datetime] = mapped_column(UTCDateTime())


class AgentResearchRunRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_research_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('prepared','running','validated','audited','synthesized',"
            "'completed','failed','abstained')",
            name="status_allowed",
        ),
        CheckConstraint("retry_count BETWEEN 0 AND 1", name="retry_count_bounded"),
        CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd",
            name="settled_cost_bounded",
        ),
        CheckConstraint("schema_hash ~ '^[0-9a-f]{64}$'", name="schema_hash_canonical"),
        CheckConstraint("model_hash ~ '^[0-9a-f]{64}$'", name="model_hash_canonical"),
        CheckConstraint("prompt_hash ~ '^[0-9a-f]{64}$'", name="prompt_hash_canonical"),
        CheckConstraint("tool_hash ~ '^[0-9a-f]{64}$'", name="tool_hash_canonical"),
        CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash_canonical"),
        UniqueConstraint("actor_id", "idempotency_key", name="uq_agent_runs_actor_idempotency"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    data_snapshot_id: Mapped[UUID] = mapped_column(ForeignKey("data_snapshots.id"))
    prompt_version_id: Mapped[UUID] = mapped_column(ForeignKey("agent_prompt_versions.id"))
    model_config_id: Mapped[UUID] = mapped_column(ForeignKey("agent_model_configs.id"))
    decision_time: Mapped[datetime] = mapped_column(UTCDateTime())
    actor_id: Mapped[str] = mapped_column(String(100), default="service")
    idempotency_key: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16), default="prepared")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    schema_hash: Mapped[str] = mapped_column(String(64))
    model_hash: Mapped[str] = mapped_column(String(64))
    prompt_hash: Mapped[str] = mapped_column(String(64))
    tool_hash: Mapped[str] = mapped_column(String(64))
    input_hash: Mapped[str] = mapped_column(String(64))
    reserved_cost_usd: Mapped[Decimal]
    settled_cost_usd: Mapped[Decimal | None]
    pricing_version: Mapped[str] = mapped_column(String(100), default="configured")
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)


class AgentResearchOutputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_research_outputs"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_agent_research_outputs_run_sequence"),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        CheckConstraint(
            "status IN ('validated','audited','synthesized','completed','failed','abstained')",
            name="status_allowed",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    intent: Mapped[dict[str, object] | None] = mapped_column(JSONB)


class AgentDailyBudgetReservationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_daily_budget_reservations"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_agent_budget_reservations_run"),
        CheckConstraint("reserved_usd >= 0", name="reserved_nonnegative"),
        CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd",
            name="settled_bounded",
        ),
        CheckConstraint("state IN ('reserved','settled','released')", name="state_allowed"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    business_date: Mapped[date]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    pricing_version: Mapped[str] = mapped_column(String(100))
    reserved_usd: Mapped[Decimal]
    settled_usd: Mapped[Decimal | None]
    state: Mapped[str] = mapped_column(String(16), default="reserved")
