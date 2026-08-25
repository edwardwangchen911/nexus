"""Durable lifecycle runner for the bounded eight-role research committee."""

import hashlib
import json
from collections.abc import Mapping
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
)
from nexus.agents.execution_authority import (
    load_frozen_evidence,
    load_frozen_prompts,
    load_frozen_symbol,
)
from nexus.agents.models import ResearchRequest, ResearchRunResult
from nexus.agents.persistence import (
    release_daily_budget_if_reserved,
    require_reserved_daily_budget,
    settle_daily_budget,
)
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
    if price_authority_status is FrozenPriceAuthorityStatus.INVALID:
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
            return await _terminal_after_committee(
                session,
                run,
                model=model,
                committee=committee,
                role_outputs=role_outputs,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="failed",
                reason="model_execution_failed",
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
    final_price_authority_status = await revalidate_frozen_price_authority(
        session,
        run=run,
        evidence_records=evidence_records,
        allow_fixture=allow_fixture_price_authority,
    )
    if (
        final_price_authority_status is FrozenPriceAuthorityStatus.INVALID
        or final_price_authority_status is not price_authority_status
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
        ),
    )
    await _append_output(
        session,
        run,
        sequence=output_sequence,
        status="synthesized",
        role=None,
        prompt_hash=None,
        payload={"role_count": len(role_outputs), "evidence_count": len(evidence_refs)},
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
        except Exception:
            if run.retry_count >= 1:
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


async def _terminal_without_usage(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    output_sequence: int,
    event_sequence: int,
    status: Literal["failed", "abstained"],
    reason: str,
) -> ResearchRunResult:
    await release_daily_budget_if_reserved(session, run_id=run.id)
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
    usage = _committee_usage(committee, role_outputs)
    if usage.input_tokens or usage.output_tokens:
        accounted = await _settle_or_hold_usage(session, run, model=model, usage=usage)
        if not accounted:
            reason = "usage_accounting_failed"
    else:
        await release_daily_budget_if_reserved(session, run_id=run.id)
    return await _persist_terminal(
        session,
        run,
        output_sequence=output_sequence,
        event_sequence=event_sequence,
        status=status,
        reason=reason,
    )


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
    try:
        async with session.begin_nested():
            actual_usd = usage_cost(model, usage.input_tokens, usage.output_tokens)
            await settle_daily_budget(
                session,
                run_id=run.id,
                actual_usd=actual_usd,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
            )
    except Exception:
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
) -> ResearchRunResult:
    await _append_output(
        session,
        run,
        sequence=output_sequence,
        status=status,
        role=None,
        prompt_hash=None,
        payload={"reason": reason},
        intent=None,
    )
    await _advance(session, run, status)
    await _append_event(session, run, event_sequence, status, {"reason": reason})
    return ResearchRunResult(status=status)


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
