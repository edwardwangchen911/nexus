"""Immutable safe observations, independent of synthesis intent and settlement."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import InvocationKind, InvocationPhase, SdkFailureFacts, TimeoutSource
from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt

Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
LocalObservation = Literal[
    "cancelled",
    "exception",
    "validation",
    "authority_rejected",
    "orphan_prepared",
    "orphan_running",
    "diagnostic_unavailable",
    "evidence_expired",
]


class FailureObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    invocation_phase: InvocationPhase | None = None
    invocation_kind: InvocationKind | None = None
    timeout_source: TimeoutSource | None = None
    failure_facts: SdkFailureFacts | None = None
    local: LocalObservation | None = None


def freeze_failure_observation(value: object) -> FailureObservation:
    """Revalidate even constructed models; never retain validation input or exception text."""
    try:
        if isinstance(value, FailureObservation):
            value = value.model_dump(mode="json", warnings=False)
        if isinstance(value, dict):
            value = dict(value)
            facts = value.get("failure_facts")
            if isinstance(facts, SdkFailureFacts):
                value["failure_facts"] = facts.model_dump(mode="json", warnings=False)
        return FailureObservation.model_validate(value)
    except (ValueError, TypeError):
        return FailureObservation(local="diagnostic_unavailable")


class SynthesisFailureDiagnostic(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    schema_version: Literal["portfolio_synthesis_failure_v1"] = "portfolio_synthesis_failure_v1"
    run_id: UUID
    job_id: UUID
    input_hash: Hash
    output_id: UUID
    binding_hash: Hash
    attempt_id: UUID | None
    attempt_no: int | None = Field(ge=1, strict=True)
    fence_token: int | None = Field(ge=1, strict=True)
    observation: FailureObservation

    @model_validator(mode="after")
    def provenance(self) -> SynthesisFailureDiagnostic:
        values = (self.attempt_id, self.attempt_no, self.fence_token)
        if any(v is None for v in values) and not all(v is None for v in values):
            raise ValueError("SYNTHESIS_DIAGNOSTIC_PROVENANCE_INVALID")
        return self


class SynthesisFailureRead(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    state: Literal["valid", "missing", "invalid", "unsupported"]
    reason: Literal["DIAGNOSTIC_MISSING", "DIAGNOSTIC_INVALID", "DIAGNOSTIC_UNSUPPORTED"] | None = (
        None
    )
    diagnostic: SynthesisFailureDiagnostic | None = None


class SynthesisDiagnosticConflict(Exception):
    """Existing business facts match; reject only the new diagnostic, never retry."""

    def __init__(self) -> None:
        super().__init__("PORTFOLIO_SYNTHESIS_DIAGNOSTIC_CONFLICT")


async def bind_failure_diagnostic(
    session: AsyncSession,
    *,
    run: AgentPortfolioSynthesisRunRecord,
    output_id: UUID,
    binding_hash: str,
    observation: FailureObservation,
    job_id: UUID,
    attempt_no: int | None,
    fence_token: int | None,
) -> dict[str, object]:
    job = await session.get(Job, job_id)
    if (
        job is None
        or job.job_type != "agent.portfolio_synthesis"
        or job.payload != {"run_id": str(run.id)}
        or job.input_hash != run.input_hash
    ):
        raise ValueError("PORTFOLIO_SYNTHESIS_DIAGNOSTIC_BINDING_INVALID")
    attempt = None
    if attempt_no is not None:
        attempt = await session.scalar(
            select(JobAttempt).where(
                JobAttempt.job_id == job_id,
                JobAttempt.attempt_no == attempt_no,
                JobAttempt.fence_token == fence_token,
            )
        )
        if attempt is None:
            raise ValueError("PORTFOLIO_SYNTHESIS_DIAGNOSTIC_BINDING_INVALID")
    diagnostic = SynthesisFailureDiagnostic(
        run_id=run.id,
        job_id=job_id,
        input_hash=run.input_hash,
        output_id=output_id,
        binding_hash=binding_hash,
        attempt_id=None if attempt is None else attempt.id,
        attempt_no=attempt_no,
        fence_token=fence_token,
        observation=observation,
    ).model_dump(mode="json")
    return {
        "diagnostic": diagnostic,
        "diagnostic_hash": canonical_portfolio_synthesis_hash(diagnostic),
    }


async def read_synthesis_failure(session: AsyncSession, *, run_id: UUID) -> SynthesisFailureRead:
    """Normal exact-run readonly path; corrupt diagnostics never invalidate business facts."""
    output = await session.scalar(
        select(AgentPortfolioSynthesisOutputRecord).where(
            AgentPortfolioSynthesisOutputRecord.run_id == run_id
        )
    )
    if output is None or output.failure_diagnostic is None:
        return SynthesisFailureRead(state="missing", reason="DIAGNOSTIC_MISSING")
    value = output.failure_diagnostic
    invalid = SynthesisFailureRead(state="invalid", reason="DIAGNOSTIC_INVALID")
    if not isinstance(value, dict) or set(value) != {"diagnostic", "diagnostic_hash"}:
        return invalid
    raw = value["diagnostic"]
    if isinstance(raw, dict) and raw.get("schema_version") != "portfolio_synthesis_failure_v1":
        return SynthesisFailureRead(state="unsupported", reason="DIAGNOSTIC_UNSUPPORTED")
    try:
        diagnostic = SynthesisFailureDiagnostic.model_validate(raw)
    except (ValidationError, TypeError):
        return invalid
    if value["diagnostic_hash"] != canonical_portfolio_synthesis_hash(
        diagnostic.model_dump(mode="json")
    ):
        return invalid
    run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
    job = await session.get(Job, diagnostic.job_id)
    if (
        run is None
        or job is None
        or output.status != "failed"
        or diagnostic.run_id != run_id
        or diagnostic.output_id != output.id
        or diagnostic.binding_hash != output.binding_hash
        or diagnostic.input_hash != run.input_hash
        or job.job_type != "agent.portfolio_synthesis"
        or job.payload != {"run_id": str(run_id)}
        or job.input_hash != run.input_hash
    ):
        return invalid
    if diagnostic.attempt_id is not None:
        attempt = await session.get(JobAttempt, diagnostic.attempt_id)
        if (
            attempt is None
            or attempt.job_id != job.id
            or attempt.attempt_no != diagnostic.attempt_no
            or attempt.fence_token != diagnostic.fence_token
        ):
            return invalid
    return SynthesisFailureRead(state="valid", diagnostic=diagnostic)
