"""Small read-only projections of exact frozen research; never admission authority."""

from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import _all_nested_evidence_bound, _evidence_identity
from nexus.agents.actionable_research import _hash as canonical_hash
from nexus.agents.concentration import condition_evaluations_match
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.observation_readback import ObservationReadback, readable_semantics
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.synthesis import domain_evidence
from nexus.market.current_identity import identity_parse_scope
from nexus.paper.agent_cycle_models import (
    CycleStatus,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    parse_candidate_manifest,
)
from nexus.paper.manual_request import ManualRequestView
from nexus.paper.research_request import read_research_manifest
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.strategy.intents import ResearchIntent


class WorkbenchLimitationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source: Literal["system", "model"]
    kind: str
    text: str = Field(max_length=1000)
    truncated: bool = False
    impact: str | None = Field(default=None, max_length=1000)


class WorkbenchCandidateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument_id: UUID
    symbol: str
    run_id: UUID | None = None
    snapshot_id: UUID | None = None
    research_as_of: datetime | None = None
    run_status: str | None = None
    job_status: str | None = None
    quality: str | None = None
    read_status: Literal["verified", "unavailable"] = "unavailable"
    reason_code: str | None = None
    href: str | None = None
    limitations: tuple[WorkbenchLimitationResponse, ...] = Field(default=(), max_length=20)
    has_more: bool = False


def _limitations(intent: ResearchIntent) -> tuple[tuple[WorkbenchLimitationResponse, ...], bool]:
    items = []
    assessment = intent.synthesis_assessment
    if assessment is not None:
        for review in assessment.reviews:
            for text in review.missing_evidence:
                items.append(
                    WorkbenchLimitationResponse(
                        source="model",
                        kind="missing_evidence",
                        text=text[:1000],
                        truncated=len(text) > 1000 or len(review.prevented_conclusion or "") > 1000,
                        impact=review.prevented_conclusion[:1000]
                        if review.prevented_conclusion
                        else None,
                    )
                )
            if review.prevented_conclusion and not review.missing_evidence:
                text = review.prevented_conclusion
                items.append(
                    WorkbenchLimitationResponse(
                        source="model",
                        kind="prevented_conclusion",
                        text=text[:1000],
                        truncated=len(text) > 1000,
                    )
                )
        uncertainties = (*assessment.uncertainties, *intent.uncertainties)
    else:
        uncertainties = intent.uncertainties
    for text in dict.fromkeys(uncertainties):
        items.append(
            WorkbenchLimitationResponse(
                source="model",
                kind="uncertainties",
                text=text[:1000],
                truncated=len(text) > 1000,
            )
        )
    return tuple(items[:20]), len(items) > 20


async def _validated_intent(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    outputs: tuple[AgentResearchOutputRecord, ...],
    reader: ObservationReadback,
    *,
    read_at: datetime,
) -> ResearchIntent | None:
    if run.status != "completed" or len(outputs) != 1:
        return None
    output = outputs[0]
    try:
        intent = ResearchIntent.model_validate(output.intent)
    except ValueError:
        return None
    if (
        output.run_id != run.id
        or output.role is not None
        or output.status != "completed"
        or max(run.created_at, output.created_at, run.decision_time) > read_at
        or intent.instrument_id != run.instrument_id
        or intent.data_snapshot_id != run.data_snapshot_id
        or intent.decision_time != run.decision_time
        or output.payload.get("quality_status") != intent.quality_status
        or output.intent != intent.model_dump(mode="json")
        or canonical_hash(
            {
                "status": output.status,
                "role": output.role,
                "payload": output.payload,
                "intent": output.intent,
            }
        )
        != output.content_hash
        or canonical_hash(run.prompt_manifest) != run.prompt_hash
    ):
        return None
    bindings = tuple(
        await session.scalars(
            select(AgentResearchOutputEvidenceRecord)
            .where(
                AgentResearchOutputEvidenceRecord.output_id == output.id,
            )
            .limit(101)
        )
    )
    # The observation verifier already checks the complete frozen identity,
    # prompts, evidence bindings and portfolio conditions. Do not hydrate that
    # same authority twice for the supported read-only observation exception.
    if await reader.verified(run, output, bindings):
        return intent
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    prompts = await load_frozen_prompts(session, run)
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    if (
        model is None
        or snapshot is None
        or prompts is None
        or not evidence
        or model.version != run.pricing_version
        or model.config_hash != run.model_hash
        or intent.model_version != model.model_id
        or intent.prompt_version != prompts["orchestrator"].version
        or intent.data_version != snapshot.revision
        or max(snapshot.available_at, snapshot.received_at) > run.decision_time
        or any(ref.created_at > read_at for ref in evidence)
    ):
        return None
    by_id = {ref.id: ref for ref in evidence}
    fields = (
        "source",
        "provider_object_id",
        "revision",
        "available_at",
        "location",
        "content_hash",
    )
    expected = {_evidence_identity(domain_evidence(ref)) for ref in evidence}
    if (
        len(bindings) != len(by_id)
        or {item.evidence_ref_id for item in bindings} != set(by_id)
        or any(item.created_at > read_at for item in bindings)
        or any(
            any(
                getattr(item, field) != getattr(by_id[item.evidence_ref_id], field)
                for field in fields
            )
            for item in bindings
        )
        or len(intent.evidence_refs) != len(expected)
        or {_evidence_identity(ref) for ref in intent.evidence_refs} != expected
        or not _all_nested_evidence_bound(intent, expected)
    ):
        return None
    context = await read_portfolio_context(
        session, run.data_snapshot_id, run.instrument_id, run.decision_time
    )
    if not condition_evaluations_match(intent, context) or not readable_semantics(
        intent,
        run.pricing_version,
        observation_verified=False,
    ):
        return None
    return intent


async def _project(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    symbol: str,
    run: AgentResearchRunRecord | None,
    jobs: tuple[Job, ...],
    reader: ObservationReadback,
    read_at: datetime,
) -> WorkbenchCandidateResponse:
    base = WorkbenchCandidateResponse(instrument_id=instrument_id, symbol=symbol)
    if run is None:
        return base.model_copy(update={"reason_code": "RESEARCH_RUN_UNAVAILABLE"})
    base = base.model_copy(
        update={
            "run_id": run.id,
            "snapshot_id": run.data_snapshot_id,
            "research_as_of": run.decision_time,
            "run_status": run.status,
            "href": f"/research/{instrument_id}?run_id={run.id}",
        }
    )
    if (
        len(jobs) != 1
        or jobs[0].payload != {"run_id": str(run.id)}
        or jobs[0].scope_id != str(instrument_id)
        or jobs[0].effective_at != run.decision_time
        or jobs[0].job_type not in {"agent.research", "agent.paper.research"}
        or jobs[0].created_at > read_at
    ):
        return base.model_copy(update={"reason_code": "RESEARCH_JOB_BINDING_INVALID"})
    job = jobs[0]
    status = (
        job.status
        if job.finished_at is not None and job.finished_at <= read_at
        else ("running" if job.started_at is not None and job.started_at <= read_at else "queued")
    )
    base = base.model_copy(update={"job_status": status})
    if run.status not in {"completed", "failed", "abstained"}:
        return base.model_copy(update={"read_status": "verified"})
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status.in_(("completed", "failed", "abstained")),
            )
            .limit(2)
        )
    )
    if run.status in {"failed", "abstained"}:
        safe_reasons = {
            "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN": "原研究执行结果仍未知。",
            "manual_unused_research_cancelled": "未使用的研究已按原取消记录收尾。",
        }
        reason = None
        if len(outputs) == 1:
            output = outputs[0]
            content = {
                "status": output.status,
                "role": output.role,
                "payload": output.payload,
                "intent": output.intent,
            }
            if (
                output.status == run.status
                and output.intent is None
                and output.created_at <= read_at
                and output.content_hash == canonical_hash(content)
            ):
                raw = output.payload.get("reason")
                if isinstance(raw, str) and raw in safe_reasons:
                    reason = raw
        if reason is None:
            return base.model_copy(update={"reason_code": "RESEARCH_FAILURE_DETAIL_UNVERIFIED"})
        return base.model_copy(
            update={
                "read_status": "verified",
                "reason_code": reason,
                "limitations": (
                    WorkbenchLimitationResponse(
                        source="system",
                        kind="runtime_result",
                        text=safe_reasons[reason],
                    ),
                ),
            }
        )
    try:
        with identity_parse_scope():
            intent = await _validated_intent(session, run, outputs, reader, read_at=read_at)
    except (ValueError, TypeError, KeyError):
        intent = None
    if intent is None:
        return base.model_copy(update={"reason_code": "RESEARCH_SUMMARY_UNVERIFIED"})
    items, more = _limitations(intent)
    return base.model_copy(
        update={
            "read_status": "verified",
            "quality": intent.quality_status,
            "limitations": items,
            "has_more": more,
        }
    )


async def cycle_candidates(
    session: AsyncSession,
    cycle: AgentPaperCycleRecord,
    *,
    read_at: datetime,
) -> tuple[WorkbenchCandidateResponse, ...]:
    manifest = parse_candidate_manifest(
        cycle.candidate_manifest,
        allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
    )
    if (
        len(manifest) > 5
        or len({c.instrument_id for c in manifest}) != len(manifest)
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
        or cycle.created_at > read_at
    ):
        raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id == cycle.id,
                AgentPaperCycleEventRecord.created_at <= read_at,
                AgentPaperCycleEventRecord.occurred_at <= read_at,
            )
            .order_by(AgentPaperCycleEventRecord.sequence)
            .limit(101)
        )
    )
    previous = None
    prepared = []
    for index, event in enumerate(events, 1):
        if (
            event.sequence != index
            or event.from_status != previous
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=cycle.id,
                sequence=event.sequence,
                from_status=cast(CycleStatus | None, event.from_status),
                to_status=cast(CycleStatus, event.to_status),
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
        previous = event.to_status
        if event.result_code == "RESEARCH_JOBS_PREPARED":
            prepared.append(event)
    if not events or len(events) > 100 or len(prepared) > 1:
        raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    run_ids: tuple[UUID, ...] = ()
    if prepared:
        raw = prepared[0].fact_ids.get("research_run_ids")
        if not isinstance(raw, list) or len(raw) != len(manifest):
            raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
        run_ids = tuple(UUID(str(value)) for value in raw)
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    runs = (
        tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(run_ids),
                    AgentResearchRunRecord.created_at <= read_at,
                )
            )
        )
        if run_ids
        else ()
    )
    jobs = (
        tuple(
            await session.scalars(
                select(Job)
                .where(
                    Job.payload["run_id"].as_string().in_([str(value) for value in run_ids]),
                    Job.created_at <= read_at,
                )
                .limit(11)
            )
        )
        if run_ids
        else ()
    )
    reader = ObservationReadback(session, read_at=read_at)
    await reader.prime(runs)
    result = []
    for candidate in manifest:
        matches = [
            run
            for run in runs
            if run.instrument_id == candidate.instrument_id
            and run.actor_id == "local-agent-paper-cycle"
            and run.idempotency_key == f"agent-paper:{cycle.id}:{candidate.instrument_id}"
            and run.decision_time == cycle.decision_time
        ]
        run = matches[0] if len(matches) == 1 else None
        snapshot_bound = run is not None and run.data_snapshot_id == candidate.research_snapshot_id
        if run is not None and cycle.execution_session == cycle.decision_session:
            try:
                context = await read_agent_paper_research_context(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=candidate.instrument_id,
                    as_of=run.decision_time,
                )
                snapshot_bound = context is not None and context[0].cycle_id == cycle.id
            except (ValueError, TypeError, KeyError):
                snapshot_bound = False
        if run is not None and not snapshot_bound:
            result.append(
                WorkbenchCandidateResponse(
                    instrument_id=candidate.instrument_id,
                    symbol=candidate.symbol,
                    run_id=run.id,
                    run_status=run.status,
                    reason_code="RESEARCH_RUN_BINDING_INVALID",
                )
            )
            continue
        matching_jobs = tuple(
            job for job in jobs if run is not None and job.payload.get("run_id") == str(run.id)
        )
        result.append(
            await _project(
                session,
                instrument_id=candidate.instrument_id,
                symbol=candidate.symbol,
                run=run,
                jobs=matching_jobs,
                reader=reader,
                read_at=read_at,
            )
        )
    return tuple(result)


async def research_candidates(
    session: AsyncSession,
    request: ManualPaperRequest,
    view: ManualRequestView,
    *,
    read_at: datetime,
) -> tuple[WorkbenchCandidateResponse, ...]:
    if (
        request.id != view.request_id
        or request.mode != "research_only"
        or request.requested_at > read_at
    ):
        raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    manifest = await read_research_manifest(session, request)
    if manifest != view.research or manifest is None:
        if manifest is None and view.research is None:
            return ()
        raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    if len(manifest.members) > 5 or manifest.research_as_of > read_at:
        raise ValueError("WORKBENCH_CANDIDATE_BINDING_INVALID")
    runs = tuple(
        await session.scalars(
            select(AgentResearchRunRecord).where(
                AgentResearchRunRecord.id.in_([m.run_id for m in manifest.members]),
                AgentResearchRunRecord.created_at <= read_at,
            )
        )
    )
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.id.in_([m.job_id for m in manifest.members]),
                Job.created_at <= read_at,
            )
        )
    )
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias)
            .where(
                InstrumentAlias.instrument_id.in_([m.instrument_id for m in manifest.members]),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= manifest.research_as_of.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= manifest.research_as_of.date(),
                ),
                InstrumentAlias.created_at <= read_at,
            )
            .limit(11)
        )
    )
    reader = ObservationReadback(session, read_at=read_at)
    await reader.prime(runs)
    result = []
    for member in manifest.members:
        run = next((run for run in runs if run.id == member.run_id), None)
        symbols = {alias.symbol for alias in aliases if alias.instrument_id == member.instrument_id}
        result.append(
            await _project(
                session,
                instrument_id=member.instrument_id,
                symbol=next(iter(symbols)) if len(symbols) == 1 else str(member.instrument_id),
                run=run,
                jobs=tuple(job for job in jobs if job.id == member.job_id),
                reader=reader,
                read_at=read_at,
            )
        )
    return tuple(result)
