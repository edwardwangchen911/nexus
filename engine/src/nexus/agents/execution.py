"""Durable lifecycle runner for the bounded eight-role research committee."""

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Final, Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.committee import (
    ROLE_ORDER,
    AgentRole,
    CommitteeModel,
    CommitteeRoleOutput,
    PromptAsset,
    StageFailureDiagnostic,
    StageInputIdentity,
    StageInvocationTiming,
)
from nexus.agents.execution_authority import (
    load_frozen_evidence,
    load_frozen_prompts,
    load_frozen_symbol,
)
from nexus.agents.models import ResearchRequest, ResearchRunResult
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.persistence import (
    release_daily_budget_if_reserved,
    require_reserved_daily_budget,
    settle_daily_budget,
)
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.price_authority import (
    FrozenPriceAuthorityStatus,
    revalidate_frozen_price_authority,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.synthesis import (
    citations_bound,
    domain_evidence,
    synthesize_intent,
    usage_cost,
)
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchEventRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef

LIFECYCLE: Final[tuple[str, ...]] = (
    "prepared",
    "running",
    "validated",
    "audited",
    "synthesized",
    "completed",
)
TERMINAL: Final[frozenset[str]] = frozenset({"completed", "failed", "abstained"})
SAFE_MODEL_FAILURES: Final[frozenset[str]] = frozenset(
    {
        "model_usage_uncertain",
        "model_research_stopped",
        "model_stage_allowance_exhausted",
        "model_stage_wire_bytes_exceeded",
        "model_stage_input_allowance_exhausted",
        "model_stage_output_allowance_exhausted",
        "model_stage_call_allowance_exhausted",
        "model_stage_deadline_missing",
        "model_stage_deadline_elapsed",
        "model_token_limit_exceeded",
        "model_stage_schema_invalid",
        "model_stage_output_invalid",
        "model_stage_roles_invalid",
        "model_stage_review_invalid",
        "model_stage_citation_invalid",
    }
)


async def execute_research_run(
    session: AsyncSession,
    *,
    run_id: UUID,
    committee: CommitteeModel,
    allow_fixture_evidence: bool = True,
    allow_fixture_price_authority: bool = False,
) -> ResearchRunResult:
    """Advance one prepared run through every durable state or a fail-closed terminal."""
    run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
    if run is None:
        raise ValueError("research run is missing")
    if run.status != "prepared":
        raise ValueError("only a prepared research run can execute")
    event_sequence = 1
    output_sequence = 1
    await _append_event(session, run, event_sequence, "prepared", {})
    evidence_records = await load_frozen_evidence(
        session, run, allow_fixture_evidence=allow_fixture_evidence
    )
    if not evidence_records:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="evidence_authority_missing",
        )
    price_authority_status = await revalidate_frozen_price_authority(
        session,
        run=run,
        evidence_records=evidence_records,
        allow_fixture=allow_fixture_price_authority,
    )
    paper_context_status = await revalidate_frozen_agent_paper_context(
        session,
        run=run,
        evidence_records=evidence_records,
    )
    from nexus.monitoring.context import FrozenMonitorContextStatus, revalidate_monitor_context

    monitor_context_status = await revalidate_monitor_context(
        session, run=run, evidence_records=evidence_records
    )
    if monitor_context_status is FrozenMonitorContextStatus.INVALID:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="monitor_context_invalid",
        )
    if (
        price_authority_status is FrozenPriceAuthorityStatus.INVALID
        or paper_context_status is FrozenAgentPaperContextStatus.INVALID
    ):
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="price_authority_invalid",
        )
    prompts = await load_frozen_prompts(session, run)
    if prompts is None:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="prompt_authority_missing",
        )
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    symbol = await load_frozen_symbol(session, run)
    if model is None or snapshot is None or symbol is None:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="run_authority_missing",
        )
    evidence_refs = tuple(domain_evidence(record) for record in evidence_records)
    request = ResearchRequest(
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
    )
    try:
        await require_reserved_daily_budget(session, run_id=run.id)
    except BudgetExhausted:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="failed",
            reason="budget_reservation_unavailable",
        )
    await _advance(session, run, "running")
    event_sequence += 1
    await _append_event(session, run, event_sequence, "running", {})
    role_outputs: list[CommitteeRoleOutput] = []
    for role in ROLE_ORDER:
        persisted = prompts[role]
        prompt = PromptAsset(
            role=role,
            version=persisted.version,
            content=persisted.content,
            content_hash=persisted.content_hash,
        )
        output = await _analyze_with_one_retry(
            session,
            run,
            committee=committee,
            role=role,
            request=request,
            evidence_refs=evidence_refs,
            prompt=prompt,
        )
        if output is None:
            failure_reason = getattr(committee, "failure_reason", None)
            return await _terminal_after_committee(
                session,
                run,
                model=model,
                committee=committee,
                role_outputs=role_outputs,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="failed",
                reason=failure_reason
                if isinstance(failure_reason, str) and failure_reason in SAFE_MODEL_FAILURES
                else "model_execution_failed",
            )
        if output.role != role or output.prompt_hash != prompt.content_hash:
            return await _terminal_after_committee(
                session,
                run,
                model=model,
                committee=committee,
                role_outputs=role_outputs,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="failed",
                reason="role_output_identity_invalid",
            )
        if not citations_bound(output.claims, evidence_refs):
            return await _terminal_after_committee(
                session,
                run,
                model=model,
                committee=committee,
                role_outputs=role_outputs,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="abstained",
                reason="role_output_evidence_invalid",
            )
        role_outputs.append(output)
        await _append_output(
            session,
            run,
            sequence=output_sequence,
            status="running",
            role=role,
            prompt_hash=prompt.content_hash,
            payload=output.model_dump(mode="json"),
            intent=None,
        )
        output_sequence += 1
    await _advance(session, run, "validated")
    event_sequence += 1
    await _append_event(session, run, event_sequence, "validated", {"role_count": 8})
    await _advance(session, run, "audited")
    event_sequence += 1
    await _append_event(
        session,
        run,
        event_sequence,
        "audited",
        {"evidence_count": len(evidence_records)},
    )
    if any(
        record.source in {"alpaca_current_trends", "alpaca_news"} for record in evidence_records
    ) and not await load_frozen_evidence(
        session,
        run,
        allow_fixture_evidence=allow_fixture_evidence,
    ):
        return await _terminal_after_committee(
            session,
            run,
            model=model,
            committee=committee,
            role_outputs=role_outputs,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="frozen_evidence_invalid"
            if any(r.source == "alpaca_news" for r in evidence_records)
            else "current_price_trends_invalid",
        )
    final_price_authority_status = await revalidate_frozen_price_authority(
        session,
        run=run,
        evidence_records=evidence_records,
        allow_fixture=allow_fixture_price_authority,
    )
    final_paper_context_status = await revalidate_frozen_agent_paper_context(
        session,
        run=run,
        evidence_records=evidence_records,
    )
    final_monitor_context_status = await revalidate_monitor_context(
        session, run=run, evidence_records=evidence_records
    )
    if (
        final_monitor_context_status is FrozenMonitorContextStatus.INVALID
        or final_monitor_context_status is not monitor_context_status
    ):
        return await _terminal_after_committee(
            session,
            run,
            model=model,
            committee=committee,
            role_outputs=role_outputs,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="monitor_context_invalid",
        )
    if (
        final_price_authority_status is FrozenPriceAuthorityStatus.INVALID
        or final_price_authority_status is not price_authority_status
        or final_paper_context_status is FrozenAgentPaperContextStatus.INVALID
        or final_paper_context_status is not paper_context_status
    ):
        return await _terminal_after_committee(
            session,
            run,
            model=model,
            committee=committee,
            role_outputs=role_outputs,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="price_authority_invalid",
        )
    usage = _committee_usage(committee, role_outputs)
    if not await _settle_or_hold_usage(session, run, model=model, usage=usage):
        return await _persist_terminal(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="failed",
            reason="usage_accounting_failed",
        )
    if any(output.conflicting_facts for output in role_outputs):
        return await _persist_terminal(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="conflicting_facts",
        )
    intent = synthesize_intent(
        run,
        symbol,
        snapshot,
        prompts,
        model,
        evidence_refs,
        role_outputs,
        price_authority_valid=(
            final_price_authority_status is FrozenPriceAuthorityStatus.VALID
            or final_paper_context_status is FrozenAgentPaperContextStatus.VALID
            or final_monitor_context_status is FrozenMonitorContextStatus.VALID
        ),
        portfolio_context=await read_portfolio_context(
            session, run.data_snapshot_id, run.instrument_id, run.decision_time
        ),
    )
    await _append_output(
        session,
        run,
        sequence=output_sequence,
        status="synthesized",
        role=None,
        prompt_hash=None,
        payload={
            "role_count": len(role_outputs),
            "evidence_count": len(evidence_refs),
            **_committee_timing_payload(committee),
        },
        intent=intent.model_dump(mode="json"),
    )
    output_sequence += 1
    await _advance(session, run, "synthesized")
    event_sequence += 1
    await _append_event(session, run, event_sequence, "synthesized", {})
    final_output = await _append_output(
        session,
        run,
        sequence=output_sequence,
        status="completed",
        role=None,
        prompt_hash=None,
        payload={"quality_status": intent.quality_status},
        intent=intent.model_dump(mode="json"),
    )
    _bind_final_evidence(session, final_output, evidence_records)
    await session.flush()
    await _advance(session, run, "completed")
    event_sequence += 1
    await _append_event(session, run, event_sequence, "completed", {})
    return ResearchRunResult(status="completed", intent=intent)


async def _analyze_with_one_retry(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    committee: CommitteeModel,
    role: AgentRole,
    request: ResearchRequest,
    evidence_refs: tuple[EvidenceRef, ...],
    prompt: PromptAsset,
) -> CommitteeRoleOutput | None:
    for _attempt in range(2):
        try:
            return await committee.analyze(role, request, evidence_refs, prompt)
        except Exception as error:
            from nexus.agents.research_dispatch import stop_current_dispatch

            stop_current_dispatch(error)
            if getattr(committee, "retry_on_error", True) is False or run.retry_count >= 1:
                return None
            run.retry_count += 1
            await session.flush()
    return None


def _bind_final_evidence(
    session: AsyncSession,
    output: AgentResearchOutputRecord,
    records: tuple[ResearchEvidenceRef, ...],
) -> None:
    session.add_all(
        AgentResearchOutputEvidenceRecord(
            output_id=output.id,
            evidence_ref_id=record.id,
            source=record.source,
            provider_object_id=record.provider_object_id,
            revision=record.revision,
            available_at=record.available_at,
            location=record.location,
            content_hash=record.content_hash,
        )
        for record in records
    )


async def _advance(session: AsyncSession, run: AgentResearchRunRecord, state: str) -> None:
    run.status = state
    await session.flush()


async def _append_event(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    sequence: int,
    state: str,
    details: Mapping[str, object],
) -> None:
    payload = {"state": state, "details": dict(details)}
    session.add(
        AgentResearchEventRecord(
            run_id=run.id,
            sequence=sequence,
            state=state,
            event_hash=_hash(payload),
            details=dict(details),
        )
    )
    await session.flush()


async def _append_output(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    sequence: int,
    status: str,
    role: AgentRole | None,
    prompt_hash: str | None,
    payload: Mapping[str, object],
    intent: Mapping[str, object] | None,
) -> AgentResearchOutputRecord:
    content = {"status": status, "role": role, "payload": dict(payload), "intent": intent}
    output = AgentResearchOutputRecord(
        run_id=run.id,
        sequence=sequence,
        status=status,
        role=role,
        prompt_hash=prompt_hash,
        content_hash=_hash(content),
        payload=dict(payload),
        intent=None if intent is None else dict(intent),
    )
    session.add(output)
    await session.flush()
    return output


async def stop_uncalled_research(session: AsyncSession, *, run_id: UUID) -> ResearchRunResult:
    """Only the claimed handler's pure preparation may prove zero SDK calls."""
    from nexus.agents.research_dispatch import current_dispatch

    state = current_dispatch(run_id)
    run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
    if (
        state is None
        or not state.stopped
        or run_id not in state.claims
        or run is None
        or run.status != "prepared"
    ):
        raise ValueError("RESEARCH_DISPATCH_INVALID")
    await require_reserved_daily_budget(session, run_id=run.id)
    await _append_event(session, run, 1, "prepared", {})
    return await _terminal_without_usage(
        session,
        run,
        output_sequence=1,
        event_sequence=2,
        status="abstained",
        reason="model_research_stopped",
    )


async def _terminal_without_usage(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    output_sequence: int,
    event_sequence: int,
    status: Literal["failed", "abstained"],
    reason: str,
) -> ResearchRunResult:
    from nexus.agents.research_dispatch import stop_current_dispatch

    stop_current_dispatch()
    if any(
        value not in (None, 0)
        for value in (run.settled_cost_usd, run.input_tokens, run.output_tokens)
    ):
        raise BudgetExhausted("zero-call close conflicts with recorded usage")
    reservation = await release_daily_budget_if_reserved(session, run_id=run.id)
    if reservation.state != "released":
        raise BudgetExhausted("zero-call close conflicts with settled reservation")
    run.settled_cost_usd = Decimal("0")
    run.input_tokens = 0
    run.output_tokens = 0
    return await _persist_terminal(
        session,
        run,
        output_sequence=output_sequence,
        event_sequence=event_sequence,
        status=status,
        reason=reason,
    )


async def _terminal_after_committee(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    model: AgentModelConfigRecord,
    committee: CommitteeModel,
    role_outputs: list[CommitteeRoleOutput],
    output_sequence: int,
    event_sequence: int,
    status: Literal["failed", "abstained"],
    reason: str,
) -> ResearchRunResult:
    from nexus.agents.research_dispatch import stop_current_dispatch

    stop_current_dispatch()
    usage = _committee_usage(committee, role_outputs)
    if reason == "model_usage_uncertain":
        # Saved stages are a known lower bound. An invocation without complete
        # usage cannot settle or release the whole native reservation.
        if usage.input_tokens or usage.output_tokens:
            run.input_tokens = usage.input_tokens
            run.output_tokens = usage.output_tokens
            await session.flush()
    elif usage.input_tokens or usage.output_tokens:
        accounted = await _settle_or_hold_usage(session, run, model=model, usage=usage)
        if not accounted:
            reason = "usage_accounting_failed"
    else:
        await release_daily_budget_if_reserved(session, run_id=run.id)
        if reason == "model_research_stopped":
            run.settled_cost_usd = Decimal("0")
            run.input_tokens = 0
            run.output_tokens = 0
    stage_input = getattr(committee, "last_stage_input", None)
    diagnostic = getattr(committee, "failure_diagnostic", None)
    return await _persist_terminal(
        session,
        run,
        output_sequence=output_sequence,
        event_sequence=event_sequence,
        status=status,
        reason=reason,
        stage_input=stage_input if isinstance(stage_input, StageInputIdentity) else None,
        diagnostic=diagnostic
        if (
            isinstance(diagnostic, StageFailureDiagnostic)
            and isinstance(stage_input, StageInputIdentity)
            and diagnostic.stage == stage_input.stage
        )
        else None,
        timing_payload=_committee_timing_payload(committee),
    )


def _committee_timing_payload(committee: CommitteeModel) -> dict[str, object]:
    timings = getattr(committee, "stage_timings", None)
    if not isinstance(timings, dict):
        return {}
    serialized = {
        stage: timing.model_dump(mode="json")
        for stage, timing in timings.items()
        if stage in {"research", "critique", "synthesis"}
        and isinstance(timing, StageInvocationTiming)
    }
    return {"stage_timings": serialized} if serialized else {}


def _committee_usage(
    committee: CommitteeModel, role_outputs: list[CommitteeRoleOutput]
) -> UsageSnapshot:
    reported = getattr(committee, "usage", None)
    if isinstance(reported, UsageSnapshot):
        return reported
    return UsageSnapshot(
        input_tokens=sum(output.input_tokens for output in role_outputs),
        output_tokens=sum(output.output_tokens for output in role_outputs),
    )


async def _settle_or_hold_usage(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    model: AgentModelConfigRecord,
    usage: UsageSnapshot,
) -> bool:
    """Settle known usage, retaining the full reservation if exact settlement rejects."""
    from nexus.agents.research_dispatch import dispatch_failure_boundary, stop_current_dispatch

    try:
        async with (
            dispatch_failure_boundary(session),
            session.begin_nested(),
            dispatch_failure_boundary(),
        ):
            actual_usd = usage_cost(model, usage.input_tokens, usage.output_tokens)
            await settle_daily_budget(
                session,
                run_id=run.id,
                actual_usd=actual_usd,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
            )
    except Exception:
        stop_current_dispatch()
        run.input_tokens = usage.input_tokens
        run.output_tokens = usage.output_tokens
        await session.flush()
        return False
    return True


async def _persist_terminal(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    output_sequence: int,
    event_sequence: int,
    status: Literal["failed", "abstained"],
    reason: str,
    stage_input: StageInputIdentity | None = None,
    diagnostic: StageFailureDiagnostic | None = None,
    timing_payload: dict[str, object] | None = None,
) -> ResearchRunResult:
    from nexus.agents.research_dispatch import stop_current_dispatch

    stop_current_dispatch()
    await _append_output(
        session,
        run,
        sequence=output_sequence,
        status=status,
        role=None,
        prompt_hash=None,
        payload={
            "reason": reason,
            **(timing_payload or {}),
            **({"stage_input": stage_input.model_dump(mode="json")} if stage_input else {}),
            **({"diagnostic": diagnostic.model_dump(mode="json")} if diagnostic else {}),
        },
        intent=None,
    )
    await _advance(session, run, status)
    await _append_event(session, run, event_sequence, status, {"reason": reason})
    return ResearchRunResult(status=status)


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
