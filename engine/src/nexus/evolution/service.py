"""Deterministic admission service for bounded Strategy R&D experiment families."""

from __future__ import annotations

import hashlib
import re
from calendar import monthrange
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.responses import ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot
from nexus.audit.canonical import canonical_json
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.evolution.agent import (
    StrategyRNDRequestV1,
    StrategyRNDRuntime,
    load_strategy_rnd_prompt,
)
from nexus.evolution.agent_tools import (
    StrategyRNDTools,
    strategy_rnd_function_tools,
)
from nexus.evolution.candidate import apply_candidate_delta, generate_candidate_deltas
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import (
    load_candidate_budget_policy_v1,
    load_evolution_search_space_v1,
)
from nexus.evolution.repository import (
    active_lineage_projection,
    append_admitted_event,
    cumulative_lineage_trial_count,
    find_family_by_idempotency,
    latest_family_created_at,
    lock_internal_paper_registry_head,
    publish_challenger_definition,
)
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
)
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyResearchLineageRecord,
    StrategyRNDOutputRecord,
    StrategyRNDRunRecord,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.strategy.dsl import (
    baseline_feature_registry,
    compile_strategy,
    evolution_feature_registry_v1,
)

_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")


class EvolutionAdmissionError(ValueError):
    """A fixed admission gate failed closed with a safe public reason code."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EvolutionAdmissionAuthorityV1(_FrozenModel):
    schema_version: str = Field(pattern=r"^strategy_evolution_admission_authority_v1$")
    data_snapshot_id: UUID
    quality_report_id: UUID
    candidate_budget_policy_id: UUID
    evolution_policy_id: UUID
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    protocol_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    rnd_output_id: UUID | None = None


class AdmittedFamily(_FrozenModel):
    family_id: UUID
    candidate_count: int = Field(ge=1, le=24)
    champion_generation: int = Field(ge=1)
    replayed: bool


class StrategyRNDExecutionResult(_FrozenModel):
    run_id: UUID
    status: Literal["completed", "failed", "abstained"]
    output_id: UUID | None
    proposal: StrategyExperimentProposalV1 | None
    reason_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{0,63}$")


class StrategyRNDAbstained(RuntimeError):
    """Explicit runtime abstention that must never create a candidate family."""


class StrategyRNDAuthorityError(ValueError):
    """Frozen Prompt, model, snapshot, or runtime authority is invalid."""


async def execute_strategy_rnd(
    session: AsyncSession,
    *,
    request: StrategyRNDRequestV1,
    tools: StrategyRNDTools,
    runtime: StrategyRNDRuntime,
    prompt_version_id: UUID,
    model_config_id: UUID,
    now: datetime,
) -> StrategyRNDExecutionResult:
    """Run one bounded Agent call and append only its terminal usage and output facts."""
    now = require_utc(now)
    if request.as_of != now:
        raise StrategyRNDAuthorityError("RND_AS_OF_MISMATCH")
    _require_tool_context(request, tools)
    prompt_record = await session.get(AgentPromptVersionRecord, prompt_version_id)
    model_record = await session.get(AgentModelConfigRecord, model_config_id)
    snapshot = await session.get(DataSnapshot, request.data_snapshot_id)
    parent = await session.get(
        StrategyDefinitionRecord, request.parent_strategy_definition_id
    )
    prompt = load_strategy_rnd_prompt()
    if (
        prompt_record is None
        or prompt_record.role != prompt.role
        or prompt_record.version != prompt.version
        or prompt_record.content != prompt.content
        or prompt_record.content_hash != prompt.content_hash
        or hashlib.sha256(prompt_record.content.encode()).hexdigest()
        != prompt_record.content_hash
    ):
        raise StrategyRNDAuthorityError("RND_PROMPT_AUTHORITY_INVALID")
    if model_record is None or not _runtime_matches_model(
        runtime.configuration, model_record, now
    ):
        raise StrategyRNDAuthorityError("RND_MODEL_AUTHORITY_INVALID")
    if (
        snapshot is None
        or parent is None
        or snapshot.available_at > now
        or snapshot.received_at > now
    ):
        raise StrategyRNDAuthorityError("RND_INPUT_AUTHORITY_INVALID")
    await _require_request_evidence(session, request, now)

    request_hash = _strategy_rnd_request_hash(
        request=request,
        prompt_hash=prompt_record.content_hash,
        model_hash=model_record.config_hash,
    )
    status: Literal["completed", "failed", "abstained"]
    proposal: StrategyExperimentProposalV1 | None = None
    reason_code: str | None = None
    try:
        generated = await runtime.generate(request, tools)
        _require_generated_proposal_identity(request, generated)
        proposal = generated
        status = "completed"
    except StrategyRNDAbstained:
        status = "abstained"
        reason_code = "AGENT_ABSTAINED"
    except Exception:
        status = "failed"
        reason_code = "MODEL_EXECUTION_FAILED"

    usage, usage_valid = _bounded_usage(runtime.usage, model_record)
    if not usage_valid:
        status = "failed"
        proposal = None
        reason_code = "USAGE_AUTHORITY_INVALID"
    settled_cost = _usage_cost(model_record, usage)
    run = StrategyRNDRunRecord(
        id=new_uuid7(),
        prompt_version_id=prompt_record.id,
        model_config_id=model_record.id,
        data_snapshot_id=request.data_snapshot_id,
        champion_definition_id=request.parent_strategy_definition_id,
        champion_generation=request.champion_generation,
        as_of=request.as_of,
        status=status,
        reserved_cost_usd=model_record.worst_case_cost_usd,
        settled_cost_usd=settled_cost,
        tool_call_count=usage.tool_calls,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        request_hash=request_hash,
        created_at=now,
    )
    session.add(run)
    await session.flush()
    output: StrategyRNDOutputRecord | None = None
    if status == "completed" and proposal is not None:
        proposal_document = proposal.model_dump(mode="json")
        output = StrategyRNDOutputRecord(
            id=new_uuid7(),
            run_id=run.id,
            schema_version="strategy_experiment_proposal_v1",
            proposal=proposal_document,
            proposal_hash=hashlib.sha256(
                canonical_json(proposal_document).encode()
            ).hexdigest(),
            validation_status="VALID",
            reason_code=None,
            created_at=now,
        )
        session.add(output)
        await session.flush()
    await append_audit_event(
        session,
        actor_id="service:strategy_rnd",
        action=(
            "strategy.proposal.created"
            if status == "completed"
            else "strategy.proposal.rejected"
        ),
        schema_version="strategy_evolution_event_v1",
        payload={
            "schema_version": "strategy_evolution_event_v1",
            "event_type": (
                "PROPOSAL_CREATED" if status == "completed" else "PROPOSAL_REJECTED"
            ),
            "status": status.upper(),
            "reason_code": reason_code or "PROPOSAL_CREATED",
            "request_hash": request_hash,
            **({} if output is None else {"proposal_hash": output.proposal_hash}),
        },
        occurred_at=now,
    )
    return StrategyRNDExecutionResult(
        run_id=run.id,
        status=status,
        output_id=None if output is None else output.id,
        proposal=proposal,
        reason_code=reason_code,
    )


async def admit_experiment_family(
    session: AsyncSession,
    *,
    proposal: StrategyExperimentProposalV1,
    authority: EvolutionAdmissionAuthorityV1,
    now: datetime,
    idempotency_key: str,
) -> AdmittedFamily:
    """Admit one bounded family without accepting DSL, targets, risk, or order input."""
    now = require_utc(now)
    if _IDEMPOTENCY_KEY.fullmatch(idempotency_key) is None:
        raise EvolutionAdmissionError("IDEMPOTENCY_KEY_INVALID")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-evolution-idempotency:' || :key, 2204))"
        ),
        {"key": idempotency_key},
    )
    existing = await find_family_by_idempotency(session, idempotency_key)
    if existing is not None:
        if (
            not _family_matches(existing, proposal, authority)
            or authority.rnd_output_id is None
        ):
            raise EvolutionAdmissionError("IDEMPOTENCY_CONFLICT")
        try:
            await _require_valid_rnd_output(
                session,
                proposal,
                authority,
                existing.created_at,
                expected_family_id=existing.id,
            )
        except EvolutionAdmissionError as error:
            raise EvolutionAdmissionError("IDEMPOTENCY_CONFLICT") from error
        return AdmittedFamily(
            family_id=existing.id,
            candidate_count=existing.candidate_count,
            champion_generation=existing.champion_generation,
            replayed=True,
        )

    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-evolution-lineage:' || :lineage_id, 2204))"
        ),
        {"lineage_id": str(proposal.research_lineage_id)},
    )
    head = await lock_internal_paper_registry_head(session)
    if head is None:
        raise EvolutionAdmissionError("REGISTRY_AUTHORITY_INCOMPLETE")
    if head.auto_promotion_paused:
        raise EvolutionAdmissionError("EVOLUTION_PAUSED")
    if (
        head.active_definition_id != proposal.parent_strategy_definition_id
        or head.active_generation != proposal.champion_generation
    ):
        raise EvolutionAdmissionError("STALE_CHAMPION_BASELINE")

    lineage = await session.scalar(
        select(StrategyResearchLineageRecord)
        .where(StrategyResearchLineageRecord.id == proposal.research_lineage_id)
        .with_for_update()
    )
    if lineage is None or lineage.status != "active":
        raise EvolutionAdmissionError("LINEAGE_AUTHORITY_INCOMPLETE")
    if lineage.evolution_policy_id != authority.evolution_policy_id:
        raise EvolutionAdmissionError("EVOLUTION_POLICY_MISMATCH")
    if await active_lineage_projection(session, lineage.id) is not None:
        raise EvolutionAdmissionError("ACTIVE_FAMILY_EXISTS")
    latest_created_at = await latest_family_created_at(session, lineage.id)
    if latest_created_at is not None and now < _calendar_quarter_after(
        latest_created_at
    ):
        raise EvolutionAdmissionError("EXPERIMENT_COOLDOWN_ACTIVE")

    parent = await session.get(StrategyDefinitionRecord, head.active_definition_id)
    quality = await session.get(QualityReport, authority.quality_report_id)
    snapshot = await session.get(DataSnapshot, authority.data_snapshot_id)
    if parent is None:
        raise EvolutionAdmissionError("STRATEGY_PARENT_INCOMPLETE")
    if (
        quality is None
        or quality.status != "PASS"
        or quality.data_snapshot_id != authority.data_snapshot_id
        or snapshot is None
        or snapshot.available_at > now
    ):
        raise EvolutionAdmissionError("EVIDENCE_INCOMPLETE")

    candidate_policy_record = await session.get(
        StrategyEvolutionPolicyRecord, authority.candidate_budget_policy_id
    )
    search_policy_record = await session.get(
        StrategyEvolutionPolicyRecord, authority.evolution_policy_id
    )
    if (
        candidate_policy_record is None
        or candidate_policy_record.policy_kind != "candidate_budget"
        or search_policy_record is None
        or search_policy_record.policy_kind != "evolution_search_space"
    ):
        raise EvolutionAdmissionError("EVOLUTION_POLICY_MISMATCH")
    try:
        candidate_policy = load_candidate_budget_policy_v1(
            candidate_policy_record.document, candidate_policy_record.content_hash
        )
        search_policy = load_evolution_search_space_v1(
            search_policy_record.document, search_policy_record.content_hash
        )
    except ValueError as error:
        raise EvolutionAdmissionError("EVOLUTION_POLICY_MISMATCH") from error

    await _require_frozen_evidence(session, proposal, authority, now)
    parent_registry = (
        baseline_feature_registry()
        if parent.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    try:
        compiled_parent = compile_strategy(parent.document, parent_registry)
    except ValueError as error:
        raise EvolutionAdmissionError("DSL_COMPILATION_FAILED") from error
    if compiled_parent.expression_hash != parent.expression_hash:
        raise EvolutionAdmissionError("STRATEGY_PARENT_INCOMPLETE")
    try:
        deltas = generate_candidate_deltas(proposal, parent.document, candidate_policy)
    except ValueError as error:
        raise EvolutionAdmissionError("PROPOSAL_SCHEMA_INVALID") from error
    if not deltas:
        raise EvolutionAdmissionError("CANDIDATE_DUPLICATE")
    if authority.rnd_output_id is None:
        raise EvolutionAdmissionError("RND_OUTPUT_AUTHORITY_INVALID")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-rnd-output:' || :output_id, 2204))"
        ),
        {"output_id": str(authority.rnd_output_id)},
    )
    await _require_valid_rnd_output(session, proposal, authority, now)

    family = StrategyExperimentFamilyRecord(
        id=new_uuid7(),
        research_lineage_id=lineage.id,
        parent_strategy_definition_id=parent.id,
        rnd_output_id=authority.rnd_output_id,
        champion_generation=head.active_generation,
        admission_status="ADMITTED",
        candidate_count=len(deltas),
        idempotency_key=idempotency_key,
        authority_hash=authority.authority_hash,
        protocol_hash=authority.protocol_hash,
        code_hash=authority.code_hash,
        created_at=now,
    )
    session.add(family)
    await session.flush()
    candidate_hashes: list[str] = []
    for delta in deltas:
        try:
            document = apply_candidate_delta(parent.document, delta, search_policy)
            compiled = compile_strategy(document, evolution_feature_registry_v1())
        except ValueError as error:
            raise EvolutionAdmissionError("DSL_COMPILATION_FAILED") from error
        definition = await publish_challenger_definition(
            session,
            document=document,
            expression_hash=compiled.expression_hash,
            published_at=now,
        )
        session.add(
            StrategyExperimentRecord(
                id=new_uuid7(),
                family_id=family.id,
                candidate_strategy_definition_id=definition.id,
                candidate_ordinal=delta.ordinal,
                candidate_delta=delta.model_dump(mode="json"),
                candidate_hash=delta.candidate_hash,
                expression_hash=compiled.expression_hash,
                status="ADMITTED",
                created_at=now,
            )
        )
        candidate_hashes.append(delta.candidate_hash)

    cumulative_trials = max(
        lineage.cumulative_oos_trial_count,
        await cumulative_lineage_trial_count(session, lineage.id),
    )
    event_key = "family-admitted-" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:40]
    await append_admitted_event(
        session,
        family_id=family.id,
        idempotency_key=event_key,
        occurred_at=now,
        payload={
            "schema_version": "strategy_family_admitted_payload_v1",
            "champion_generation": head.active_generation,
            "candidate_count": len(deltas),
            "candidate_hashes": candidate_hashes,
            "authority_hash": authority.authority_hash,
            "protocol_hash": authority.protocol_hash,
            "code_hash": authority.code_hash,
        },
    )
    session.add(
        StrategyExperimentStateProjectionRecord(
            family_id=family.id,
            research_lineage_id=lineage.id,
            current_state="ADMITTED",
            event_sequence=1,
            cumulative_oos_trial_count=cumulative_trials,
            version=1,
            updated_at=now,
        )
    )
    outbox_payload = {
        "status": "ADMITTED",
        "candidate_count": len(deltas),
        "champion_generation": head.active_generation,
        "authority_hash": authority.authority_hash,
    }
    await append_audit_event(
        session,
        actor_id="service:strategy_rnd",
        action="strategy.experiment.admitted",
        schema_version="strategy_evolution_event_v1",
        payload={
            "schema_version": "strategy_evolution_event_v1",
            "event_type": "CANDIDATES_ADMITTED",
            "status": "ADMITTED",
            "authority_hash": authority.authority_hash,
            "protocol_hash": authority.protocol_hash,
            "code_hash": authority.code_hash,
        },
        occurred_at=now,
    )
    session.add(
        OutboxEvent(
            id=new_uuid7(),
            event_type="strategy.experiment.admitted",
            aggregate_id=family.id,
            schema_version="strategy_evolution_event_v1",
            payload=outbox_payload,
            published_at=None,
        )
    )
    await session.flush()
    return AdmittedFamily(
        family_id=family.id,
        candidate_count=len(deltas),
        champion_generation=head.active_generation,
        replayed=False,
    )


def _family_matches(
    family: StrategyExperimentFamilyRecord,
    proposal: StrategyExperimentProposalV1,
    authority: EvolutionAdmissionAuthorityV1,
) -> bool:
    return (
        family.research_lineage_id == proposal.research_lineage_id
        and family.parent_strategy_definition_id
        == proposal.parent_strategy_definition_id
        and family.champion_generation == proposal.champion_generation
        and family.authority_hash == authority.authority_hash
        and family.protocol_hash == authority.protocol_hash
        and family.code_hash == authority.code_hash
        and family.rnd_output_id == authority.rnd_output_id
    )


async def _require_frozen_evidence(
    session: AsyncSession,
    proposal: StrategyExperimentProposalV1,
    authority: EvolutionAdmissionAuthorityV1,
    now: datetime,
) -> None:
    records = list(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id
                == authority.data_snapshot_id,
                AgentDataSnapshotEvidenceRecord.evidence_ref_id.in_(proposal.evidence_refs),
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
            )
        )
    )
    if (
        {record.evidence_ref_id for record in records} != set(proposal.evidence_refs)
        or any(record.available_at > now for record in records)
    ):
        raise EvolutionAdmissionError("EVIDENCE_INCOMPLETE")


def _require_tool_context(
    request: StrategyRNDRequestV1, tools: StrategyRNDTools
) -> None:
    context = tools.context
    if tuple(tools.names) != tuple(strategy_rnd_function_tools()) or (
        context.parent_strategy_definition_id != request.parent_strategy_definition_id
        or context.champion_generation != request.champion_generation
        or context.research_lineage_id != request.research_lineage_id
        or context.evolution_policy_id != request.evolution_policy_id
        or context.training_validation_summary_id
        != request.training_validation_summary_id
        or context.as_of != request.as_of
    ):
        raise StrategyRNDAuthorityError("RND_TOOL_AUTHORITY_INVALID")


def _runtime_matches_model(
    config: ResponsesModelConfig,
    model: AgentModelConfigRecord,
    now: datetime,
) -> bool:
    return (
        model.pricing_available_at <= now < model.pricing_expires_at
        and model.model_id == config.model_id
        and model.version == config.pricing_version
        and model.max_input_tokens == config.max_input_tokens
        and model.max_output_tokens == config.max_output_tokens
        and model.max_tool_calls == config.max_tool_calls
        and model.max_tool_calls <= 4
        and model.input_usd_per_million == config.input_usd_per_million
        and model.output_usd_per_million == config.output_usd_per_million
    )


async def _require_request_evidence(
    session: AsyncSession, request: StrategyRNDRequestV1, now: datetime
) -> None:
    records = list(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id
                == request.data_snapshot_id,
                AgentDataSnapshotEvidenceRecord.evidence_ref_id.in_(
                    request.evidence_refs
                ),
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
            )
        )
    )
    if (
        {record.evidence_ref_id for record in records} != set(request.evidence_refs)
        or any(record.available_at > now for record in records)
    ):
        raise StrategyRNDAuthorityError("RND_EVIDENCE_AUTHORITY_INVALID")


def _strategy_rnd_request_hash(
    *, request: StrategyRNDRequestV1, prompt_hash: str, model_hash: str
) -> str:
    tool_document = strategy_rnd_function_tools()
    tool_hash = hashlib.sha256(canonical_json(tool_document).encode()).hexdigest()
    identity = {
        "schema_version": "strategy_rnd_execution_identity_v1",
        "request": request.model_dump(mode="json"),
        "prompt_hash": prompt_hash,
        "model_hash": model_hash,
        "tool_hash": tool_hash,
    }
    return hashlib.sha256(canonical_json(identity).encode()).hexdigest()


def _require_generated_proposal_identity(
    request: StrategyRNDRequestV1, proposal: StrategyExperimentProposalV1
) -> None:
    if (
        proposal.parent_strategy_definition_id
        != request.parent_strategy_definition_id
        or proposal.champion_generation != request.champion_generation
        or proposal.research_lineage_id != request.research_lineage_id
        or not set(proposal.evidence_refs) <= set(request.evidence_refs)
    ):
        raise StrategyRNDAuthorityError("RND_PROPOSAL_IDENTITY_INVALID")


def _bounded_usage(
    usage: UsageSnapshot, model: AgentModelConfigRecord
) -> tuple[UsageSnapshot, bool]:
    valid = (
        0 <= usage.input_tokens <= model.max_input_tokens
        and 0 <= usage.output_tokens <= model.max_output_tokens
        and 0 <= usage.tool_calls <= min(model.max_tool_calls, 4)
        and usage.provider_calls >= 0
    )
    if valid:
        return usage, True
    return (
        UsageSnapshot(
            input_tokens=model.max_input_tokens,
            output_tokens=model.max_output_tokens,
            tool_calls=min(model.max_tool_calls, 4),
            provider_calls=max(usage.provider_calls, 0),
        ),
        False,
    )


def _usage_cost(model: AgentModelConfigRecord, usage: UsageSnapshot) -> Decimal:
    return (
        Decimal(usage.input_tokens) * model.input_usd_per_million
        + Decimal(usage.output_tokens) * model.output_usd_per_million
    ) / Decimal(1_000_000)


async def _require_valid_rnd_output(
    session: AsyncSession,
    proposal: StrategyExperimentProposalV1,
    authority: EvolutionAdmissionAuthorityV1,
    now: datetime,
    *,
    expected_family_id: UUID | None = None,
) -> None:
    assert authority.rnd_output_id is not None
    output = await session.get(StrategyRNDOutputRecord, authority.rnd_output_id)
    run = None if output is None else await session.get(StrategyRNDRunRecord, output.run_id)
    existing_family_id = await session.scalar(
        select(StrategyExperimentFamilyRecord.id).where(
            StrategyExperimentFamilyRecord.rnd_output_id == authority.rnd_output_id
        )
    )
    proposal_document = proposal.model_dump(mode="json")
    proposal_hash = hashlib.sha256(canonical_json(proposal_document).encode()).hexdigest()
    if (
        output is None
        or run is None
        or output.validation_status != "VALID"
        or output.proposal != proposal_document
        or output.proposal_hash != proposal_hash
        or run.status != "completed"
        or run.as_of != now
        or run.data_snapshot_id != authority.data_snapshot_id
        or run.champion_definition_id != proposal.parent_strategy_definition_id
        or run.champion_generation != proposal.champion_generation
        or (
            existing_family_id is not None
            if expected_family_id is None
            else existing_family_id != expected_family_id
        )
    ):
        raise EvolutionAdmissionError("RND_OUTPUT_AUTHORITY_INVALID")


def _calendar_quarter_after(value: datetime) -> datetime:
    month_index = value.month - 1 + 3
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)
