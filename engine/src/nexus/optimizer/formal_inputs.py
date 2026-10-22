"""Exact formal A/R/P bindings; later research never rewrites valuation history."""

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.synthesis import staged_intent_semantics_valid
from nexus.domain.time import require_utc
from nexus.optimizer.research_context import _intent
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import Job
from nexus.strategy.intents import ResearchIntent
from nexus.strategy.snapshot_reader import FrozenStrategyReferences

FORMAL_SCHEMA = "optimization_formal_inputs_v2"
FORMAL_SOURCE = "nexus_optimization_formal"


def canonical_hash(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class _Closed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class FormalResearchOutputReference(_Closed):
    run_id: UUID
    output_id: UUID


class FormalResearchBinding(FormalResearchOutputReference):
    instrument_id: UUID
    research_data_snapshot_id: UUID
    output_sequence: int = Field(ge=1)
    output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    actual_available_at: datetime
    portfolio_snapshot_id: UUID
    research_as_of: datetime

    @field_validator("actual_available_at", "research_as_of")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class FormalOptimizationInputs(_Closed):
    schema_version: Literal["optimization_formal_inputs_v2"] = "optimization_formal_inputs_v2"
    portfolio_snapshot_id: UUID
    valuation_time: datetime
    decision_cutoff_at: datetime
    materialized_at: datetime
    diagnostic_report_id: UUID
    diagnostic_input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    research_bindings: tuple[FormalResearchBinding, ...] = Field(min_length=1, max_length=100)
    strategy_references: FrozenStrategyReferences
    strategy_references_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("valuation_time", "decision_cutoff_at", "materialized_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _binding(self) -> "FormalOptimizationInputs":
        if (
            not self.valuation_time <= self.decision_cutoff_at <= self.materialized_at
            or not self.valuation_time
            <= self.strategy_references.market.decision_time
            <= self.decision_cutoff_at
            or self.strategy_references_hash != self.strategy_references.content_hash
            or len({r.instrument_id for r in self.research_bindings}) != len(self.research_bindings)
            or len({r.run_id for r in self.research_bindings}) != len(self.research_bindings)
            or any(
                r.portfolio_snapshot_id != self.portfolio_snapshot_id
                or not self.valuation_time
                <= r.research_as_of
                <= r.actual_available_at
                <= self.decision_cutoff_at
                for r in self.research_bindings
            )
        ):
            raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
        return self


class FormalOptimizationJobBinding(_Closed):
    schema_version: Literal["optimization_formal_binding_v2"] = "optimization_formal_binding_v2"
    admission_authority_id: UUID
    data_snapshot_id: UUID
    data_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_cutoff_at: datetime

    @field_validator("decision_cutoff_at", mode="before")
    @classmethod
    def _utc(cls, value: object) -> datetime:
        # The Job worker parses JSON in strict mode; explicitly preserve its UTC clock.
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("OPTIMIZATION_FORMAL_CLOCK_INVALID")
        return require_utc(value)


def read_formal_inputs(data: DataSnapshot) -> FormalOptimizationInputs:
    inputs = FormalOptimizationInputs.model_validate(data.payload)
    if (
        data.schema_version != FORMAL_SCHEMA
        or data.source != FORMAL_SOURCE
        or data.provider_object_id != str(inputs.portfolio_snapshot_id)
        or data.revision != canonical_hash(data.payload)
        or data.observed_at != inputs.valuation_time
        or data.available_at != inputs.materialized_at
        or data.received_at != inputs.materialized_at
    ):
        raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
    return inputs


async def freeze_formal_research(
    session: AsyncSession,
    reference: FormalResearchOutputReference,
    *,
    portfolio_snapshot_id: UUID,
    diagnostic_report_id: UUID,
    diagnostic_input_hash: str,
    valuation_time: datetime,
    decision_cutoff_at: datetime,
) -> tuple[FormalResearchBinding, ResearchIntent]:
    """Read one explicit final output, its original R and all original evidence."""
    run = await session.get(AgentResearchRunRecord, reference.run_id)
    if run is not None:
        from nexus.agents.research_only import require_formal_research

        await require_formal_research(session, run)
    output = await session.get(AgentResearchOutputRecord, reference.output_id)
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type == "agent.research",
                Job.payload["run_id"].astext == str(reference.run_id),
            )
        )
    )
    if (
        run is None
        or output is None
        or len(jobs) != 1
        or run.status != "completed"
        or output.status != "completed"
        or output.role is not None
        or output.run_id != run.id
        or jobs[0].status != "succeeded"
        or jobs[0].finished_at is None
    ):
        raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_NOT_READY")
    actual = max(run.created_at, output.created_at, jobs[0].finished_at)
    if actual > decision_cutoff_at or run.decision_time > decision_cutoff_at:
        raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_NOT_READY")
    content = {
        "status": output.status,
        "role": output.role,
        "payload": output.payload,
        "intent": output.intent,
    }
    if canonical_hash(content) != output.content_hash:
        raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_INVALID")
    research_data = await session.get(DataSnapshot, run.data_snapshot_id)
    if (
        research_data is None
        or max(research_data.created_at, research_data.available_at, research_data.received_at)
        > decision_cutoff_at
    ):
        raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_NOT_READY")
    intent = await _intent(session, run, output, decision_cutoff_at)
    if (
        intent.quality_status != "complete"
        or intent.evidence_audit != "complete"
        or intent.valid_until < decision_cutoff_at
        or not staged_intent_semantics_valid(intent, run.pricing_version)
    ):
        raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_NOT_READY")
    context = await read_portfolio_context(
        session,
        run.data_snapshot_id,
        run.instrument_id,
        run.decision_time,
    )
    if (
        context is None
        or context.portfolio_snapshot_id != portfolio_snapshot_id
        or context.snapshot_as_of != valuation_time
        or context.diagnostic_run_id != diagnostic_report_id
        or context.diagnostic_input_hash != diagnostic_input_hash
    ):
        raise ValueError("OPTIMIZATION_FORMAL_PORTFOLIO_MISMATCH")
    return FormalResearchBinding(
        run_id=run.id,
        output_id=output.id,
        output_sequence=output.sequence,
        output_hash=output.content_hash,
        evidence_hash=run.evidence_hash,
        instrument_id=run.instrument_id,
        research_data_snapshot_id=run.data_snapshot_id,
        actual_available_at=actual,
        portfolio_snapshot_id=portfolio_snapshot_id,
        research_as_of=run.decision_time,
    ), intent


async def load_formal_research(
    session: AsyncSession,
    inputs: FormalOptimizationInputs,
) -> tuple[ResearchIntent, ...]:
    intents = []
    for frozen in inputs.research_bindings:
        binding, intent = await freeze_formal_research(
            session,
            FormalResearchOutputReference(run_id=frozen.run_id, output_id=frozen.output_id),
            portfolio_snapshot_id=inputs.portfolio_snapshot_id,
            diagnostic_report_id=inputs.diagnostic_report_id,
            diagnostic_input_hash=inputs.diagnostic_input_hash,
            valuation_time=inputs.valuation_time,
            decision_cutoff_at=inputs.decision_cutoff_at,
        )
        if binding != frozen:
            raise ValueError("OPTIMIZATION_FORMAL_RESEARCH_BINDING_CHANGED")
        intents.append(intent)
    return tuple(intents)
