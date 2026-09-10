"""Append-only persistence mappings for bounded, read-only Agent research."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt, UTCDateTime


class AgentPromptVersionRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_prompt_versions"
    __table_args__ = (
        UniqueConstraint("role", "version", name="uq_agent_prompt_versions_role_version"),
        CheckConstraint(
            "role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio','strategy_rnd','portfolio_decision')",
            name="role_allowed",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    role: Mapped[str] = mapped_column(String(32))
    version: Mapped[str] = mapped_column(String(100))
    content_hash: Mapped[str] = mapped_column(String(64))
    content: Mapped[str] = mapped_column(Text)


class AgentModelConfigRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_model_configs"
    __table_args__ = (
        UniqueConstraint("model_id", "version", name="uq_agent_model_configs_identity"),
        CheckConstraint("config_hash ~ '^[0-9a-f]{64}$'", name="config_hash_canonical"),
        CheckConstraint("max_input_tokens BETWEEN 1 AND 1048576", name="max_input_tokens_bounded"),
        CheckConstraint("max_tool_calls BETWEEN 0 AND 8", name="max_tool_calls_bounded"),
        CheckConstraint("max_output_tokens BETWEEN 1 AND 16384", name="max_output_tokens_bounded"),
        CheckConstraint("input_usd_per_million >= 0", name="input_price_nonnegative"),
        CheckConstraint("output_usd_per_million >= 0", name="output_price_nonnegative"),
        CheckConstraint("pricing_expires_at > pricing_available_at", name="pricing_time_ordered"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    model_id: Mapped[str] = mapped_column(String(100))
    version: Mapped[str] = mapped_column(String(100))
    config_hash: Mapped[str] = mapped_column(String(64))
    max_input_tokens: Mapped[int] = mapped_column(Integer)
    max_tool_calls: Mapped[int] = mapped_column(Integer)
    max_output_tokens: Mapped[int] = mapped_column(Integer)
    input_usd_per_million: Mapped[Decimal]
    output_usd_per_million: Mapped[Decimal]
    pricing_available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    pricing_expires_at: Mapped[datetime] = mapped_column(UTCDateTime())

    @property
    def worst_case_cost_usd(self) -> Decimal:
        return (
            Decimal(self.max_input_tokens) * self.input_usd_per_million
            + Decimal(self.max_output_tokens) * self.output_usd_per_million
        ) / Decimal(1_000_000)


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
        CheckConstraint("evidence_hash ~ '^[0-9a-f]{64}$'", name="evidence_hash_canonical"),
        CheckConstraint(
            "jsonb_typeof(evidence_manifest) = 'array' AND "
            "jsonb_array_length(evidence_manifest) BETWEEN 1 AND 100",
            name="evidence_manifest_bounded",
        ),
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
    prompt_manifest: Mapped[dict[str, object]] = mapped_column(JSONB)
    evidence_hash: Mapped[str] = mapped_column(String(64))
    evidence_manifest: Mapped[list[dict[str, object]]] = mapped_column(JSONB)
    tool_hash: Mapped[str] = mapped_column(String(64))
    input_hash: Mapped[str] = mapped_column(String(64))
    reserved_cost_usd: Mapped[Decimal]
    settled_cost_usd: Mapped[Decimal | None]
    pricing_version: Mapped[str] = mapped_column(String(100), default="configured")
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)


class AgentDataSnapshotEvidenceRecord(ImmutableCreatedAt, Base):
    """Exact Task 14 evidence authority covered by one instrument snapshot."""

    __tablename__ = "agent_data_snapshot_evidence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["data_snapshot_id"],
            ["data_snapshots.id"],
            name="fk_agent_snapshot_evidence_snapshot",
        ),
        ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_agent_snapshot_evidence_instrument"
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_agent_snapshot_evidence_authority",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint(
            "(authority_kind = 'research' AND selected_at IS NULL) OR "
            "(authority_kind = 'price' AND selected_at IS NOT NULL)",
            name="authority_kind_valid",
        ),
        Index(
            "uq_agent_snapshot_price_authority",
            "data_snapshot_id",
            "instrument_id",
            unique=True,
            postgresql_where=text("authority_kind = 'price'"),
        ),
    )

    data_snapshot_id: Mapped[UUID] = mapped_column(primary_key=True)
    instrument_id: Mapped[UUID] = mapped_column(primary_key=True)
    evidence_ref_id: Mapped[UUID] = mapped_column(primary_key=True)
    authority_kind: Mapped[str] = mapped_column(String(16), default="research")
    selected_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    location: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))


class AgentResearchEventRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_research_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_agent_research_events_run_sequence"),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        CheckConstraint(
            "state IN ('prepared','running','validated','audited','synthesized','completed',"
            "'failed','abstained')",
            name="state_allowed",
        ),
        CheckConstraint("event_hash ~ '^[0-9a-f]{64}$'", name="event_hash_canonical"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(16))
    event_hash: Mapped[str] = mapped_column(String(64))
    details: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)


class AgentResearchOutputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_research_outputs"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_agent_research_outputs_run_sequence"),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        CheckConstraint(
            "status IN ('running','validated','audited','synthesized','completed','failed',"
            "'abstained')",
            name="status_allowed",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint(
            "(role IS NULL AND prompt_hash IS NULL) OR "
            "(role IN ('orchestrator','fundamental','market_state','macro_sector','bull','bear',"
            "'evidence_audit','portfolio') AND prompt_hash ~ '^[0-9a-f]{64}$')",
            name="role_prompt_bound",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("agent_research_runs.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    role: Mapped[str | None] = mapped_column(String(32))
    prompt_hash: Mapped[str | None] = mapped_column(String(64))
    content_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)
    intent: Mapped[dict[str, object] | None] = mapped_column(JSONB)


class AgentResearchOutputEvidenceRecord(ImmutableCreatedAt, Base):
    """Exact Task 14 evidence authority bound to one immutable Agent output."""

    __tablename__ = "agent_research_output_evidence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["output_id"],
            ["agent_research_outputs.id"],
            name="fk_agent_output_evidence_output",
        ),
        ForeignKeyConstraint(
            ["evidence_ref_id"],
            ["research_evidence_refs.id"],
            name="fk_agent_output_evidence_authority",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
    )

    output_id: Mapped[UUID] = mapped_column(primary_key=True)
    evidence_ref_id: Mapped[UUID] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    location: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))


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


class PortfolioDecisionRunRecord(ImmutableCreatedAt, Base):
    """Durable authority for one explicitly requested portfolio interpretation."""

    __tablename__ = "portfolio_decision_runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["portfolio_snapshot_id", "portfolio_id"],
            ["portfolio_snapshots.id", "portfolio_snapshots.portfolio_id"],
            name="fk_portfolio_decision_runs_snapshot_portfolio",
        ),
        ForeignKeyConstraint(
            ["diagnostic_report_id", "portfolio_snapshot_id"],
            [
                "portfolio_diagnostic_reports.id",
                "portfolio_diagnostic_reports.portfolio_snapshot_id",
            ],
            name="fk_portfolio_decision_runs_diagnostic_snapshot",
        ),
        UniqueConstraint(
            "actor_id", "idempotency_key", name="uq_portfolio_decision_runs_actor_idempotency"
        ),
        CheckConstraint(
            "status IN ('prepared','running','completed','abstained','failed')",
            name="status_allowed",
        ),
        CheckConstraint("retry_count = 0", name="retry_count_zero"),
        CheckConstraint(
            "provider_phase IN ('none','first_requested','tool_recorded','second_requested')",
            name="provider_phase_allowed",
        ),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="request_hash_canonical"),
        CheckConstraint("input_hash ~ '^[0-9a-f]{64}$'", name="input_hash_canonical"),
        CheckConstraint("schema_hash ~ '^[0-9a-f]{64}$'", name="schema_hash_canonical"),
        CheckConstraint("model_hash ~ '^[0-9a-f]{64}$'", name="model_hash_canonical"),
        CheckConstraint("prompt_hash ~ '^[0-9a-f]{64}$'", name="prompt_hash_canonical"),
        CheckConstraint("tool_hash ~ '^[0-9a-f]{64}$'", name="tool_hash_canonical"),
        CheckConstraint("evidence_hash ~ '^[0-9a-f]{64}$'", name="evidence_hash_canonical"),
        CheckConstraint(
            "tool_result_hash IS NULL OR tool_result_hash ~ '^[0-9a-f]{64}$'",
            name="tool_result_hash_canonical",
        ),
        CheckConstraint("jsonb_typeof(frozen_input) = 'object'", name="frozen_input_object"),
        CheckConstraint(
            "jsonb_typeof(evidence_manifest) = 'array' AND "
            "jsonb_array_length(evidence_manifest) BETWEEN 1 AND 100",
            name="evidence_manifest_bounded",
        ),
        CheckConstraint(
            "tool_projection IS NULL OR jsonb_typeof(tool_projection) = 'object'",
            name="tool_projection_object",
        ),
        CheckConstraint(
            "(tool_result_hash IS NULL) = (tool_projection IS NULL)",
            name="tool_result_projection_bound",
        ),
        CheckConstraint("reserved_cost_usd >= 0", name="reserved_cost_nonnegative"),
        CheckConstraint(
            "settled_cost_usd IS NULL OR settled_cost_usd BETWEEN 0 AND reserved_cost_usd",
            name="settled_cost_bounded",
        ),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0", name="input_tokens_nonnegative"
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0", name="output_tokens_nonnegative"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    portfolio_id: Mapped[UUID] = mapped_column(ForeignKey("portfolios.id"))
    portfolio_snapshot_id: Mapped[UUID]
    diagnostic_report_id: Mapped[UUID]
    prompt_version_id: Mapped[UUID] = mapped_column(ForeignKey("agent_prompt_versions.id"))
    model_config_id: Mapped[UUID] = mapped_column(ForeignKey("agent_model_configs.id"))
    actor_id: Mapped[str] = mapped_column(String(100))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="prepared")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    decision_time: Mapped[datetime] = mapped_column(UTCDateTime())
    input_hash: Mapped[str] = mapped_column(String(64))
    schema_hash: Mapped[str] = mapped_column(String(64))
    frozen_input: Mapped[dict[str, object]] = mapped_column(JSONB)
    prompt_hash: Mapped[str] = mapped_column(String(64))
    model_hash: Mapped[str] = mapped_column(String(64))
    tool_hash: Mapped[str] = mapped_column(String(64))
    evidence_hash: Mapped[str] = mapped_column(String(64))
    evidence_manifest: Mapped[list[dict[str, object]]] = mapped_column(JSONB)
    tool_result_hash: Mapped[str | None] = mapped_column(String(64))
    tool_projection: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    provider_phase: Mapped[str] = mapped_column(String(24), default="none")
    reserved_cost_usd: Mapped[Decimal]
    settled_cost_usd: Mapped[Decimal | None]
    pricing_version: Mapped[str] = mapped_column(String(100))
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)


class PortfolioDecisionOutputRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_decision_outputs"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_portfolio_decision_outputs_run"),
        CheckConstraint("status IN ('completed','abstained','failed')", name="status_allowed"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint(
            "scenario_result_hash IS NULL OR scenario_result_hash ~ '^[0-9a-f]{64}$'",
            name="scenario_result_hash_canonical",
        ),
        CheckConstraint("intent IS NULL OR jsonb_typeof(intent) = 'object'", name="intent_object"),
        CheckConstraint("jsonb_typeof(details) = 'object'", name="details_object"),
        CheckConstraint(
            "(status = 'completed' AND intent IS NOT NULL AND scenario_result_hash IS NOT NULL) OR "
            "(status IN ('abstained','failed') AND intent IS NULL)",
            name="status_payload_bound",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_decision_runs.id"))
    status: Mapped[str] = mapped_column(String(16))
    content_hash: Mapped[str] = mapped_column(String(64))
    scenario_result_hash: Mapped[str | None] = mapped_column(String(64))
    intent: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    details: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)


class PortfolioDecisionBudgetReservationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "portfolio_decision_budget_reservations"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_portfolio_decision_budget_reservations_run"),
        CheckConstraint("reserved_usd >= 0", name="reserved_nonnegative"),
        CheckConstraint(
            "settled_usd IS NULL OR settled_usd BETWEEN 0 AND reserved_usd",
            name="settled_bounded",
        ),
        CheckConstraint("state IN ('reserved','settled','released')", name="state_allowed"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    business_date: Mapped[date]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("portfolio_decision_runs.id"))
    pricing_version: Mapped[str] = mapped_column(String(100))
    reserved_usd: Mapped[Decimal]
    settled_usd: Mapped[Decimal | None]
    state: Mapped[str] = mapped_column(String(16), default="reserved")
