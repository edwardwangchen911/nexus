"""Exact, time-bounded research output for later read-only proposal context."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.synthesis import domain_evidence, staged_intent_semantics_valid
from nexus.jobs.daily_manual import ManualDailyRequest
from nexus.jobs.daily_research import resolve_daily_research_job
from nexus.persistence.models.agents import (
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchIntent

_INVALID = "DRAFT_RESEARCH_OUTPUT_INVALID"
_UNAVAILABLE = "DRAFT_RESEARCH_OUTPUT_UNAVAILABLE"
type _RunStatus = Literal["completed", "failed", "abstained"]
type _UnavailableReason = Literal[
    "RESEARCH_FAILED",
    "RESEARCH_ABSTAINED",
    "STAGED_ACTION_SEMANTICS_UNVERIFIED",
]


class FrozenDailyResearchContext(BaseModel):
    """Evidence for reading a historical baseline, never a selection authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["daily_research_context_v1"] = "daily_research_context_v1"
    mode: Literal["context_only"] = "context_only"
    execution_locked: Literal[True] = True
    root_id: UUID
    run_id: UUID
    output_id: UUID
    output_sequence: int = Field(ge=1)
    output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    research_data_snapshot_id: UUID
    instrument_id: UUID
    valuation_time: datetime
    research_as_of: datetime
    output_available_at: datetime
    context_as_of: datetime
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_status: _RunStatus
    quality_status: Literal["complete", "partial"]
    intent: ResearchIntent | None
    unavailable_reason: _UnavailableReason | None


def _binding(record: AgentResearchOutputEvidenceRecord | ResearchEvidenceRef) -> tuple[object, ...]:
    return (
        record.source,
        record.provider_object_id,
        record.revision,
        record.available_at,
        record.location,
        record.content_hash,
    )


async def _intent(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord,
    as_of: datetime,
) -> ResearchIntent:
    try:
        intent = ResearchIntent.model_validate(output.intent)
    except ValidationError as error:
        raise ValueError(_INVALID) from error
    if (
        intent.instrument_id != run.instrument_id
        or intent.data_snapshot_id != run.data_snapshot_id
        or intent.decision_time != run.decision_time
    ):
        raise ValueError(_INVALID)
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    bound = tuple(
        await session.scalars(
            select(AgentResearchOutputEvidenceRecord)
            .where(
                AgentResearchOutputEvidenceRecord.output_id == output.id,
            )
            .limit(101)
        )
    )
    by_id = {record.id: record for record in evidence}
    if (
        not evidence
        or set(by_id) != {record.evidence_ref_id for record in bound}
        or any(record.created_at > as_of for record in evidence)
        or any(record.created_at > as_of for record in bound)
        or any(_binding(record) != _binding(by_id[record.evidence_ref_id]) for record in bound)
    ):
        raise ValueError(_INVALID)
    expected = {domain_evidence(record) for record in evidence}
    if set(intent.evidence_refs) != expected:
        raise ValueError(_INVALID)
    nested = (
        ref
        for collection in (
            intent.theses,
            intent.counter_theses,
            intent.invalidation_conditions,
            intent.entry_conditions,
            intent.exit_conditions,
        )
        for claim in collection
        for ref in claim.evidence_refs
    )
    if any(ref not in expected for ref in nested):
        raise ValueError(_INVALID)
    return intent


async def freeze_daily_research_context(
    session: AsyncSession,
    *,
    root_id: UUID,
    run_id: UUID,
    context_as_of: datetime,
) -> FrozenDailyResearchContext:
    """Read exact output identities; no latest-run choice, writes, calls or promotion."""
    ManualDailyRequest._utc(context_as_of)
    if context_as_of > datetime.now(UTC):
        raise ValueError(_UNAVAILABLE)
    with session.no_autoflush:
        job = await resolve_daily_research_job(session, root_id=root_id, run_id=run_id)
        run = await session.get(AgentResearchRunRecord, run_id)
        assert run is not None
        if (
            run.status not in {"completed", "failed", "abstained"}
            or run.created_at > context_as_of
            or run.decision_time > context_as_of
            or job.status
            != {"completed": "succeeded", "failed": "failed", "abstained": "skipped"}.get(
                run.status
            )
            or job.finished_at is None
            or job.finished_at > context_as_of
        ):
            raise ValueError(_UNAVAILABLE)
        output = await session.scalar(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == run_id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == run.status,
                AgentResearchOutputRecord.created_at <= context_as_of,
            )
            .order_by(AgentResearchOutputRecord.sequence.desc())
            .limit(1)
        )
        if output is None:
            raise ValueError(_UNAVAILABLE)
        content = {
            "status": output.status,
            "role": output.role,
            "payload": output.payload,
            "intent": output.intent,
        }
        digest = hashlib.sha256(
            json.dumps(
                content,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode()
        ).hexdigest()
        if output.content_hash != digest:
            raise ValueError(_INVALID)
        intent = None
        reason: _UnavailableReason | None = None
        if run.status == "completed":
            intent = await _intent(session, run, output, context_as_of)
            if not staged_intent_semantics_valid(intent, run.pricing_version):
                intent = None
                reason = "STAGED_ACTION_SEMANTICS_UNVERIFIED"
        else:
            if output.intent is not None:
                raise ValueError(_INVALID)
            reason = "RESEARCH_FAILED" if run.status == "failed" else "RESEARCH_ABSTAINED"
        # The resolver already verified this exact context and its report binding.
        from nexus.agents.portfolio_context import read_portfolio_context

        portfolio = await read_portfolio_context(
            session,
            run.data_snapshot_id,
            run.instrument_id,
            run.decision_time,
        )
        assert portfolio is not None and portfolio.diagnostic_run_id is not None
        return FrozenDailyResearchContext(
            root_id=root_id,
            run_id=run_id,
            output_id=output.id,
            output_sequence=output.sequence,
            output_hash=output.content_hash,
            portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
            diagnostic_report_id=portfolio.diagnostic_run_id,
            research_data_snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            valuation_time=portfolio.snapshot_as_of,
            research_as_of=run.decision_time,
            output_available_at=max(output.created_at, job.finished_at),
            context_as_of=context_as_of,
            evidence_hash=run.evidence_hash,
            run_status=cast(_RunStatus, run.status),
            quality_status=intent.quality_status if intent is not None else "partial",
            intent=intent,
            unavailable_reason=reason,
        )
