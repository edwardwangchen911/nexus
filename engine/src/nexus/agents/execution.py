"""Durable lifecycle runner for the bounded eight-role research committee."""

import hashlib
import json
from collections.abc import Mapping
from typing import Final, Literal, cast
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.committee import (
    ROLE_ORDER,
    AgentRole,
    CommitteeModel,
    CommitteeRoleOutput,
    PromptAsset,
)
from nexus.agents.models import ResearchRequest, ResearchRunResult
from nexus.agents.persistence import release_daily_budget_if_reserved, settle_daily_budget
from nexus.agents.synthesis import (
    citations_bound,
    domain_evidence,
    synthesize_intent,
    usage_cost,
)
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchEventRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import InstrumentAlias
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
    evidence_ref_ids: tuple[UUID, ...],
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
    evidence_records = await _load_evidence(session, run, evidence_ref_ids)
    if not evidence_records:
        return await _terminal_without_usage(
            session,
            run,
            output_sequence=output_sequence,
            event_sequence=event_sequence + 1,
            status="abstained",
            reason="evidence_authority_missing",
        )
    prompts = await _load_persisted_prompts(session, run)
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
    symbol = await _load_symbol(session, run)
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
            return await _terminal_without_usage(
                session,
                run,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="failed",
                reason="model_execution_failed",
            )
        if output.role != role or output.prompt_hash != prompt.content_hash:
            return await _terminal_without_usage(
                session,
                run,
                output_sequence=output_sequence,
                event_sequence=event_sequence + 1,
                status="failed",
                reason="role_output_identity_invalid",
            )
        if not citations_bound(output.claims, evidence_refs):
            return await _terminal_without_usage(
                session,
                run,
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
    input_tokens = sum(output.input_tokens for output in role_outputs)
    output_tokens = sum(output.output_tokens for output in role_outputs)
    try:
        actual_usd = usage_cost(model, input_tokens, output_tokens)
        await settle_daily_budget(
            session,
            run_id=run.id,
            actual_usd=actual_usd,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
    except (BudgetExhausted, ValueError):
        return await _terminal_without_usage(
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
    intent = synthesize_intent(run, symbol, snapshot, prompts, model, evidence_refs, role_outputs)
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


async def _load_evidence(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    evidence_ref_ids: tuple[UUID, ...],
) -> tuple[ResearchEvidenceRef, ...]:
    if not evidence_ref_ids or len(set(evidence_ref_ids)) != len(evidence_ref_ids):
        return ()
    records = list(
        await session.scalars(
            select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(evidence_ref_ids))
        )
    )
    by_id = {record.id: record for record in records}
    if set(by_id) != set(evidence_ref_ids):
        return ()
    ordered = tuple(by_id[evidence_id] for evidence_id in evidence_ref_ids)
    return () if any(record.available_at > run.decision_time for record in ordered) else ordered


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


async def _load_persisted_prompts(
    session: AsyncSession,
    run: AgentResearchRunRecord,
) -> Mapping[AgentRole, AgentPromptVersionRecord] | None:
    manifest = run.prompt_manifest
    if not isinstance(manifest, dict) or set(manifest) != set(ROLE_ORDER):
        return None
    if _hash(cast(Mapping[str, object], manifest)) != run.prompt_hash:
        return None
    prompt_ids: list[UUID] = []
    try:
        for role in ROLE_ORDER:
            item = manifest[role]
            if not isinstance(item, dict):
                return None
            prompt_ids.append(UUID(cast(str, item.get("id"))))
    except (TypeError, ValueError, AttributeError):
        return None
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord).where(AgentPromptVersionRecord.id.in_(prompt_ids))
        )
    )
    by_id = {record.id: record for record in records}
    prompts: dict[AgentRole, AgentPromptVersionRecord] = {}
    for role, prompt_id in zip(ROLE_ORDER, prompt_ids, strict=True):
        record = by_id.get(prompt_id)
        item = manifest[role]
        if (
            record is None
            or record.role != role
            or not isinstance(item, dict)
            or item.get("version") != record.version
            or item.get("content_hash") != record.content_hash
            or hashlib.sha256(record.content.encode()).hexdigest() != record.content_hash
        ):
            return None
        prompts[role] = record
    return prompts


async def _load_symbol(session: AsyncSession, run: AgentResearchRunRecord) -> str | None:
    value = await session.scalar(
        select(InstrumentAlias.symbol)
        .where(
            InstrumentAlias.instrument_id == run.instrument_id,
            InstrumentAlias.valid_from <= run.decision_time.date(),
            or_(
                InstrumentAlias.valid_to.is_(None),
                InstrumentAlias.valid_to >= run.decision_time.date(),
            ),
        )
        .order_by(InstrumentAlias.valid_from.desc(), InstrumentAlias.symbol)
        .limit(1)
    )
    return cast(str | None, value)


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
