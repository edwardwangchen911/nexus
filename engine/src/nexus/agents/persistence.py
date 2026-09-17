"""PostgreSQL-authoritative admission and daily budget accounting for Agent work."""

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.actionable_research import (
    RESEARCH_OUTPUT_NOT_ACTIONABLE,
    ActionableResearchDecision,
    ActionableResearchError,
    admit_actionable_research_output,
)
from nexus.agents.budget import BudgetExhausted
from nexus.agents.codex_versions import (
    PORTFOLIO_DECISION_CONFIG_VERSION,
    SDK_CONFIG_VERSIONS,
    STAGED_CONFIG_VERSIONS,
)
from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.agents.execution_authority import (
    MAX_FROZEN_EVIDENCE,
    load_frozen_evidence,
    load_frozen_prompts,
    load_frozen_symbol,
)
from nexus.agents.models import ResearchRequest
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    read_agent_paper_research_context,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.price_authority import (
    freeze_price_authority,
    revalidate_frozen_price_authority,
)
from nexus.agents.tools import EXPECTED_READ_ONLY_TOOL_NAMES
from nexus.jobs.models import (
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    canonical_job_key,
    job_input_hash,
)
from nexus.jobs.repository import enqueue_job
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.strategy.intents import ResearchIntent

_ADMISSION_SCHEMA_VERSION = "agent_research_run_v1"


@dataclass(frozen=True)
class ResearchAdmission:
    run: AgentResearchRunRecord
    replayed: bool

    @property
    def run_id(self) -> UUID:
        return self.run.id


async def load_actionable_research_output(
    session: AsyncSession,
    *,
    run_id: UUID,
    decision_time: datetime,
    allow_safe_no_trade: bool = False,
) -> ActionableResearchDecision:
    """Load and revalidate the one complete v4 terminal without model I/O."""
    try:
        run = await session.get(AgentResearchRunRecord, run_id, populate_existing=True)
        if run is None:
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        terminals = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .where(
                    AgentResearchOutputRecord.run_id == run_id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status.in_(("completed", "failed", "abstained")),
                )
                .order_by(AgentResearchOutputRecord.sequence.desc())
                .limit(2)
                .execution_options(populate_existing=True)
            )
        )
        if len(terminals) != 1 or terminals[0].status != "completed":
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        output = terminals[0]
        if output.intent is None:
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        intent = ResearchIntent.model_validate(output.intent)
        bindings = tuple(
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord)
                .where(AgentResearchOutputEvidenceRecord.output_id == output.id)
                .order_by(AgentResearchOutputEvidenceRecord.evidence_ref_id)
                .limit(101)
                .execution_options(populate_existing=True)
            )
        )
        evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
        current_by_id = {record.id: record for record in evidence}
        if (
            not evidence
            or len(bindings) > 100
            or set(current_by_id) != {binding.evidence_ref_id for binding in bindings}
            or any(
                (
                    binding.source,
                    binding.provider_object_id,
                    binding.revision,
                    binding.available_at,
                    binding.location,
                    binding.content_hash,
                )
                != (
                    current_by_id[binding.evidence_ref_id].source,
                    current_by_id[binding.evidence_ref_id].provider_object_id,
                    current_by_id[binding.evidence_ref_id].revision,
                    current_by_id[binding.evidence_ref_id].available_at,
                    current_by_id[binding.evidence_ref_id].location,
                    current_by_id[binding.evidence_ref_id].content_hash,
                )
                for binding in bindings
            )
        ):
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        instrument = await session.get(Instrument, run.instrument_id, populate_existing=True)
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id, populate_existing=True)
        symbol = await load_frozen_symbol(session, run)
        price_status = await revalidate_frozen_price_authority(
            session,
            run=run,
            evidence_records=evidence,
            allow_fixture=False,
        )
        paper_context_status = await revalidate_frozen_agent_paper_context(
            session,
            run=run,
            evidence_records=evidence,
        )
        if (
            instrument is None
            or snapshot is None
            or symbol != intent.display_symbol
            or snapshot.available_at > run.decision_time
            or snapshot.received_at > run.decision_time
            or max(run.created_at, output.created_at) > decision_time
        ):
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        if paper_context_status is FrozenAgentPaperContextStatus.INVALID:
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        return admit_actionable_research_output(
            run=run,
            output=output,
            intent=intent,
            terminal_outputs=terminals,
            bound_evidence=bindings,
            decision_time=decision_time,
            price_authority_status=price_status,
            allow_safe_no_trade=allow_safe_no_trade,
            paper_decision_context_valid=(
                paper_context_status is FrozenAgentPaperContextStatus.VALID
            ),
        )
    except ActionableResearchError:
        raise
    except (AttributeError, KeyError, TypeError, ValueError, ValidationError) as error:
        raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE) from error


async def load_complete_prompt_authority(
    session: AsyncSession, *, orchestrator_id: UUID | None = None
) -> dict[AgentRole, AgentPromptVersionRecord] | None:
    """Select the active authority or one explicit complete challenger bundle."""
    explicit: AgentPromptVersionRecord | None = None
    if orchestrator_id is not None:
        explicit = await session.get(AgentPromptVersionRecord, orchestrator_id)
        if explicit is None or explicit.role != "orchestrator":
            return None
        if explicit.release_channel == "challenger":
            bundle = list(
                await session.scalars(
                    select(AgentPromptVersionRecord).where(
                        AgentPromptVersionRecord.release_channel == "challenger",
                        AgentPromptVersionRecord.version == explicit.version,
                        AgentPromptVersionRecord.role.in_(ROLE_ORDER),
                    )
                )
            )
            selected_bundle = {
                cast(AgentRole, record.role): record
                for record in bundle
                if record.role in ROLE_ORDER
            }
            if len(bundle) != len(ROLE_ORDER) or set(selected_bundle) != set(ROLE_ORDER):
                return None
            if selected_bundle["orchestrator"].id != orchestrator_id:
                return None
            if any(
                hashlib.sha256(record.content.encode()).hexdigest() != record.content_hash
                for record in selected_bundle.values()
            ):
                return None
            return {role: selected_bundle[role] for role in ROLE_ORDER}
        if explicit.release_channel == "active":
            bundle = list(
                await session.scalars(
                    select(AgentPromptVersionRecord).where(
                        AgentPromptVersionRecord.release_channel == "active",
                        AgentPromptVersionRecord.version == explicit.version,
                        AgentPromptVersionRecord.role.in_(ROLE_ORDER),
                    )
                )
            )
            selected_bundle = {
                cast(AgentRole, record.role): record
                for record in bundle
                if record.role in ROLE_ORDER
            }
            if len(bundle) != len(ROLE_ORDER) or set(selected_bundle) != set(ROLE_ORDER):
                return None
            if selected_bundle["orchestrator"].id != orchestrator_id:
                return None
            if any(
                hashlib.sha256(record.content.encode()).hexdigest() != record.content_hash
                for record in selected_bundle.values()
            ):
                return None
            return {role: selected_bundle[role] for role in ROLE_ORDER}
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord)
            .where(
                AgentPromptVersionRecord.release_channel == "active",
                AgentPromptVersionRecord.role.in_(ROLE_ORDER),
            )
            .order_by(
                AgentPromptVersionRecord.role,
                AgentPromptVersionRecord.created_at.desc(),
                AgentPromptVersionRecord.id.desc(),
            )
        )
    )
    selected: dict[AgentRole, AgentPromptVersionRecord] = {}
    for record in records:
        if record.role not in ROLE_ORDER or record.role in selected:
            continue
        if hashlib.sha256(record.content.encode()).hexdigest() != record.content_hash:
            return None
        selected[cast(AgentRole, record.role)] = record
    if set(selected) != set(ROLE_ORDER):
        return None
    return {role: selected[role] for role in ROLE_ORDER}


async def select_authoritative_snapshot(
    session: AsyncSession, *, instrument_id: UUID, as_of: datetime
) -> DataSnapshot | None:
    """Select only a frozen snapshot that covers Task 14 evidence for the instrument."""
    return cast(
        DataSnapshot | None,
        await session.scalar(
            select(DataSnapshot)
            .join(
                AgentDataSnapshotEvidenceRecord,
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == DataSnapshot.id,
            )
            .where(
                AgentDataSnapshotEvidenceRecord.instrument_id == instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                AgentDataSnapshotEvidenceRecord.available_at <= as_of,
                DataSnapshot.source == "nexus_research",
                DataSnapshot.schema_version == "research_filings_v1",
                DataSnapshot.payload["agent_paper_context"].is_(None),
                DataSnapshot.available_at <= as_of,
                DataSnapshot.received_at <= as_of,
            )
            .order_by(
                DataSnapshot.available_at.desc(),
                DataSnapshot.received_at.desc(),
                DataSnapshot.id.desc(),
            )
            .limit(1)
        ),
    )


async def reserve_daily_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    worst_case_usd: Decimal,
    pricing_version: str,
    pricing_fresh: bool,
) -> AgentDailyBudgetReservationRecord:
    """Serialize one UTC day and count reservations plus settled actual usage."""
    _validate_budget_inputs(now, daily_limit_usd, worst_case_usd, pricing_version)
    if not pricing_fresh:
        raise BudgetExhausted("stale pricing cannot reserve research budget")
    business_date = now.date()
    await _lock_budget_day(session, business_date.isoformat())
    used = await _shared_daily_budget_used(session, business_date=business_date)
    if used + worst_case_usd > daily_limit_usd:
        raise BudgetExhausted("daily research budget exhausted")
    reservation = AgentDailyBudgetReservationRecord(
        business_date=business_date,
        run_id=run_id,
        pricing_version=pricing_version,
        reserved_usd=worst_case_usd,
        settled_usd=None,
        state="reserved",
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def reserve_portfolio_decision_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    worst_case_usd: Decimal,
    pricing_version: str,
    pricing_fresh: bool,
) -> PortfolioDecisionBudgetReservationRecord:
    """Reserve portfolio decision work against the same Agent daily limit."""
    _validate_budget_inputs(now, daily_limit_usd, worst_case_usd, pricing_version)
    if not pricing_fresh:
        raise BudgetExhausted("stale pricing cannot reserve research budget")
    business_date = now.date()
    await _lock_budget_day(session, business_date.isoformat())
    used = await _shared_daily_budget_used(session, business_date=business_date)
    if used + worst_case_usd > daily_limit_usd:
        raise BudgetExhausted("daily research budget exhausted")
    reservation = PortfolioDecisionBudgetReservationRecord(
        business_date=business_date,
        run_id=run_id,
        pricing_version=pricing_version,
        reserved_usd=worst_case_usd,
        settled_usd=None,
        state="reserved",
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def require_reserved_portfolio_decision_budget(
    session: AsyncSession, *, run_id: UUID
) -> PortfolioDecisionBudgetReservationRecord:
    """Lock and require the exact open portfolio-decision reservation."""
    _, reservation = await _locked_portfolio_decision_budget(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved":
        raise BudgetExhausted("portfolio decision run has no open budget reservation")
    return reservation


async def settle_portfolio_decision_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    actual_usd: Decimal,
    input_tokens: int,
    output_tokens: int,
) -> PortfolioDecisionBudgetReservationRecord:
    """Settle one portfolio-decision reservation using complete known usage."""
    _require_nonnegative_decimal(actual_usd, "actual_usd")
    if input_tokens < 0 or output_tokens < 0:
        raise ValueError("token usage must be non-negative")
    run, reservation = await _locked_portfolio_decision_budget(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved" or actual_usd > reservation.reserved_usd:
        raise BudgetExhausted("portfolio decision reservation cannot settle reported usage")
    reservation.settled_usd = actual_usd
    reservation.state = "settled"
    run.settled_cost_usd = actual_usd
    run.input_tokens = input_tokens
    run.output_tokens = output_tokens
    await session.flush()
    return reservation


async def settle_full_portfolio_decision_budget(
    session: AsyncSession, *, run_id: UUID
) -> PortfolioDecisionBudgetReservationRecord:
    """Conservatively retain the full reservation when provider usage is unknown."""
    run, reservation = await _locked_portfolio_decision_budget(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved":
        raise BudgetExhausted("portfolio decision reservation cannot be held")
    reservation.settled_usd = reservation.reserved_usd
    reservation.state = "settled"
    run.settled_cost_usd = reservation.reserved_usd
    await session.flush()
    return reservation


async def release_portfolio_decision_budget_if_reserved(
    session: AsyncSession, *, run_id: UUID
) -> PortfolioDecisionBudgetReservationRecord:
    """Release unused portfolio-decision capacity without changing terminal accounting."""
    _, reservation = await _locked_portfolio_decision_budget(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state == "reserved":
        reservation.state = "released"
        reservation.settled_usd = None
        await session.flush()
    return reservation


async def settle_daily_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    actual_usd: Decimal,
    input_tokens: int,
    output_tokens: int,
) -> AgentDailyBudgetReservationRecord:
    """Replace one reservation with reported actual usage without freeing that usage."""
    _require_nonnegative_decimal(actual_usd, "actual_usd")
    if input_tokens < 0 or output_tokens < 0:
        raise ValueError("token usage must be non-negative")
    reservation = await _locked_reservation(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved" or actual_usd > reservation.reserved_usd:
        raise BudgetExhausted("reservation cannot settle reported usage")
    reservation.settled_usd = actual_usd
    reservation.state = "settled"
    run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
    if run is None:
        raise ValueError("research run is missing")
    run.settled_cost_usd = actual_usd
    run.input_tokens = input_tokens
    run.output_tokens = output_tokens
    await session.flush()
    return reservation


async def require_reserved_daily_budget(
    session: AsyncSession, *, run_id: UUID
) -> AgentDailyBudgetReservationRecord:
    """Lock and require the exact open reservation before any provider work starts."""
    reservation = await _locked_reservation(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved":
        raise BudgetExhausted("research run has no open budget reservation")
    return reservation


async def release_daily_budget(
    session: AsyncSession, *, run_id: UUID
) -> AgentDailyBudgetReservationRecord:
    """Release only a still-reserved row; settled usage is terminal and remains counted."""
    reservation = await _locked_reservation(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state != "reserved":
        raise BudgetExhausted("only an open reservation can be released")
    reservation.state = "released"
    reservation.settled_usd = None
    await session.flush()
    return reservation


async def release_daily_budget_if_reserved(
    session: AsyncSession, *, run_id: UUID
) -> AgentDailyBudgetReservationRecord:
    """Release open capacity while preserving already terminal accounting."""
    reservation = await _locked_reservation(session, run_id)
    await _lock_budget_day(session, reservation.business_date.isoformat())
    if reservation.state == "reserved":
        reservation.state = "released"
        reservation.settled_usd = None
        await session.flush()
    return reservation


async def admit_research_run(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    request: ResearchRequest,
    prompt_version_id: UUID | None,
    model_config_id: UUID | None,
    daily_limit_usd: Decimal,
    now: datetime,
    allow_fixture_price_authority: bool = False,
    after_authority: Callable[[], Awaitable[None]] | None = None,
    job_type: JobType = JobType.AGENT_RESEARCH,
    reuse_latest_actor_agent_version: bool = False,
    version_source_run_id: UUID | None = None,
) -> ResearchAdmission:
    """Atomically claim, validate point-in-time authority, reserve, and append one run."""
    if now.tzinfo != UTC or request.decision_time > now:
        raise ValueError("point-in-time data authority is invalid")
    if version_source_run_id is not None and (
        reuse_latest_actor_agent_version
        or prompt_version_id is not None
        or model_config_id is not None
    ):
        raise ValueError("Agent version authority is ambiguous")
    if after_authority is not None:
        await after_authority()
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        route,
        idempotency_key,
        request_hash,
        _ADMISSION_SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash:
            raise IdempotencyConflict("idempotency key body hash conflicts with prior request")
        raw_run_id = claim.response_payload.get("run_id")
        if claim.schema_version != _ADMISSION_SCHEMA_VERSION or not isinstance(raw_run_id, str):
            raise RuntimeError("Agent idempotency response is incomplete")
        run = await session.get(AgentResearchRunRecord, UUID(raw_run_id))
        if run is None:
            raise RuntimeError("Agent idempotency run is missing")
        return ResearchAdmission(run=run, replayed=True)
    if reuse_latest_actor_agent_version:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1415))"),
            {"scope": f"agent-version:{actor_id}"},
        )
    instrument = await session.get(Instrument, request.instrument_id)
    snapshot = await session.get(DataSnapshot, request.data_snapshot_id)
    if version_source_run_id is not None:
        prior_run = await session.get(AgentResearchRunRecord, version_source_run_id)
    elif reuse_latest_actor_agent_version:
        prior_run = await session.scalar(
            select(AgentResearchRunRecord)
            .where(AgentResearchRunRecord.actor_id == actor_id)
            .order_by(
                AgentResearchRunRecord.decision_time.desc(),
                AgentResearchRunRecord.created_at.desc(),
                AgentResearchRunRecord.id.desc(),
            )
            .limit(1)
        )
    else:
        prior_run = None
    if prior_run is not None:
        prompts = await load_frozen_prompts(session, prior_run)
    else:
        prompts = await load_complete_prompt_authority(session, orchestrator_id=prompt_version_id)
    if prior_run is not None:
        model = await session.get(AgentModelConfigRecord, prior_run.model_config_id)
        if model is not None and (
            model.config_hash != prior_run.model_hash or model.version != prior_run.pricing_version
        ):
            model = None
    elif model_config_id is not None:
        model = await session.get(AgentModelConfigRecord, model_config_id)
    else:
        model = await session.scalar(
            select(AgentModelConfigRecord)
            .where(
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > now,
                # Staged research is explicit; strategy generation is a separate consumer.
                AgentModelConfigRecord.version.not_in(
                    (
                        *STAGED_CONFIG_VERSIONS,
                        "codex-sdk-strategy-rnd-v1",
                        PORTFOLIO_DECISION_CONFIG_VERSION,
                    )
                ),
            )
            .order_by(AgentModelConfigRecord.created_at.desc())
            .limit(1)
        )
    if instrument is not None and snapshot is not None:
        await freeze_price_authority(
            session,
            snapshot=snapshot,
            instrument_id=request.instrument_id,
            decision_time=request.decision_time,
            allow_fixture=allow_fixture_price_authority,
        )
    has_paper_context = snapshot is not None and "agent_paper_context" in snapshot.payload
    if has_paper_context and job_type is not JobType.AGENT_PAPER_RESEARCH:
        raise ValueError("Agent paper context authority is invalid")
    evidence_records = list(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord)
            .where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == request.data_snapshot_id,
                AgentDataSnapshotEvidenceRecord.instrument_id == request.instrument_id,
                AgentDataSnapshotEvidenceRecord.available_at <= request.decision_time,
            )
            .order_by(
                AgentDataSnapshotEvidenceRecord.available_at,
                AgentDataSnapshotEvidenceRecord.evidence_ref_id,
            )
            .limit(MAX_FROZEN_EVIDENCE + 1)
        )
    )
    if len(evidence_records) > MAX_FROZEN_EVIDENCE:
        raise ValueError("evidence authority exceeds 100 rows")
    if (
        instrument is None
        or snapshot is None
        or snapshot.available_at > request.decision_time
        or snapshot.received_at > request.decision_time
        or prompts is None
        or not evidence_records
        or not any(record.authority_kind == "research" for record in evidence_records)
        or model is None
        or model.version == "codex-sdk-strategy-rnd-v1"
        or model.version == PORTFOLIO_DECISION_CONFIG_VERSION
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
    ):
        raise ValueError("point-in-time data authority is invalid")
    prompt_manifest = {
        role: {
            "id": str(prompts[role].id),
            "version": prompts[role].version,
            "content_hash": prompts[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    prompt_manifest_hash = hashlib.sha256(
        json.dumps(prompt_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    evidence_manifest = [
        {
            "id": str(record.evidence_ref_id),
            "source": record.source,
            "provider_object_id": record.provider_object_id,
            "revision": record.revision,
            "available_at": record.available_at.isoformat().replace("+00:00", "Z"),
            "location": record.location,
            "content_hash": record.content_hash,
        }
        for record in evidence_records
    ]
    # Portfolio context is a separate local input, not a filing or price authority.
    # Freeze it into the run manifest only after its exact snapshot payload validates.
    if "portfolio_context" in snapshot.payload:
        context = await read_portfolio_context(
            session,
            snapshot.id,
            request.instrument_id,
            request.decision_time,
        )
        if context is None or context.evidence_ref_id is None:
            raise ValueError("portfolio context authority is invalid")
        context_evidence = await session.get(ResearchEvidenceRef, context.evidence_ref_id)
        assert context_evidence is not None
        evidence_manifest.append(
            {
                "id": str(context_evidence.id),
                "source": context_evidence.source,
                "provider_object_id": context_evidence.provider_object_id,
                "revision": context_evidence.revision,
                "available_at": context_evidence.available_at.isoformat().replace("+00:00", "Z"),
                "location": context_evidence.location,
                "content_hash": context_evidence.content_hash,
            }
        )
        if len(evidence_manifest) > MAX_FROZEN_EVIDENCE:
            raise ValueError("evidence authority exceeds 100 rows")
    if "agent_paper_context" in snapshot.payload:
        paper_context = await read_agent_paper_research_context(
            session,
            snapshot_id=snapshot.id,
            instrument_id=request.instrument_id,
            as_of=request.decision_time,
        )
        if paper_context is None:
            raise ValueError("Agent paper context authority is invalid")
        context_evidence = paper_context[1]
        evidence_manifest.append(
            {
                "id": str(context_evidence.id),
                "source": context_evidence.source,
                "provider_object_id": context_evidence.provider_object_id,
                "revision": context_evidence.revision,
                "available_at": context_evidence.available_at.isoformat().replace("+00:00", "Z"),
                "location": context_evidence.location,
                "content_hash": context_evidence.content_hash,
            }
        )
        if len(evidence_manifest) > MAX_FROZEN_EVIDENCE:
            raise ValueError("evidence authority exceeds 100 rows")
    evidence_manifest_hash = hashlib.sha256(
        json.dumps(evidence_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    prompt = prompts["orchestrator"]
    worst_case = model.worst_case_cost_usd
    durable_input_hash = job_input_hash(
        {
            "instrument_id": str(request.instrument_id),
            "data_snapshot_id": str(request.data_snapshot_id),
            "decision_time": request.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": request_hash,
            "schema_hash": _schema_hash(),
            "model_hash": model.config_hash,
            "prompt_hash": prompt_manifest_hash,
            "evidence_hash": evidence_manifest_hash,
            "tool_hash": _tool_hash(),
            "pricing_version": model.version,
        }
    )
    job_key = canonical_job_key(
        JobKeyV1(
            job_type=job_type,
            scope_kind="instrument",
            scope_id=str(request.instrument_id),
            effective_at=request.decision_time,
            input_hash=durable_input_hash,
        )
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:job_key, 1414))"),
        {"job_key": job_key},
    )
    existing_job = await session.scalar(select(Job).where(Job.job_key == job_key))
    if existing_job is not None:
        raw_run_id = existing_job.payload.get("run_id")
        if not isinstance(raw_run_id, str):
            raise RuntimeError("Agent durable Job payload is incomplete")
        existing_run = await session.get(AgentResearchRunRecord, UUID(raw_run_id))
        if existing_run is None:
            raise RuntimeError("Agent durable Job run is missing")
        claim.response_payload = {
            "run_id": str(existing_run.id),
            "status": existing_run.status,
        }
        await session.flush()
        return ResearchAdmission(run=existing_run, replayed=True)
    run = AgentResearchRunRecord(
        instrument_id=request.instrument_id,
        data_snapshot_id=request.data_snapshot_id,
        prompt_version_id=prompt.id,
        model_config_id=model.id,
        decision_time=request.decision_time,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        status="prepared",
        retry_count=0,
        schema_hash=_schema_hash(),
        model_hash=model.config_hash,
        prompt_hash=prompt_manifest_hash,
        prompt_manifest=prompt_manifest,
        evidence_hash=evidence_manifest_hash,
        evidence_manifest=evidence_manifest,
        tool_hash=_tool_hash(),
        input_hash=request_hash,
        reserved_cost_usd=worst_case,
        settled_cost_usd=None,
        pricing_version=model.version,
        input_tokens=None,
        output_tokens=None,
    )
    session.add(run)
    await session.flush()
    await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=job_type,
            scope_kind="instrument",
            scope_id=str(request.instrument_id),
            effective_at=request.decision_time,
            input_hash=durable_input_hash,
        ),
        schema_version="agent_research_job_v1",
        payload={"run_id": str(run.id)},
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=1 if model.version in SDK_CONFIG_VERSIONS else 3,
        available_at=now,
    )
    await session.flush()
    await reserve_daily_budget(
        session,
        run_id=run.id,
        now=now,
        daily_limit_usd=daily_limit_usd,
        worst_case_usd=worst_case,
        pricing_version=model.version,
        pricing_fresh=True,
    )
    claim.response_payload = {"run_id": str(run.id), "status": "prepared"}
    await session.flush()
    return ResearchAdmission(run=run, replayed=False)


async def _locked_reservation(
    session: AsyncSession, run_id: UUID
) -> AgentDailyBudgetReservationRecord:
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord)
        .where(AgentDailyBudgetReservationRecord.run_id == run_id)
        .with_for_update()
    )
    if reservation is None:
        raise BudgetExhausted("reservation is not available")
    return reservation


async def _locked_portfolio_decision_budget(
    session: AsyncSession, run_id: UUID
) -> tuple[PortfolioDecisionRunRecord, PortfolioDecisionBudgetReservationRecord]:
    run = await session.get(PortfolioDecisionRunRecord, run_id, with_for_update=True)
    reservation = await session.scalar(
        select(PortfolioDecisionBudgetReservationRecord)
        .where(PortfolioDecisionBudgetReservationRecord.run_id == run_id)
        .with_for_update()
    )
    if run is None or reservation is None:
        raise BudgetExhausted("portfolio decision reservation is not available")
    if (
        run.reserved_cost_usd != reservation.reserved_usd
        or run.pricing_version != reservation.pricing_version
    ):
        raise BudgetExhausted("portfolio decision reservation authority does not match run")
    return run, reservation


async def _shared_daily_budget_used(session: AsyncSession, *, business_date: date) -> Decimal:
    totals: list[Decimal] = []
    for model in (
        AgentDailyBudgetReservationRecord,
        PortfolioDecisionBudgetReservationRecord,
    ):
        used = await session.scalar(
            select(
                func.coalesce(
                    func.sum(
                        case(
                            (model.state == "reserved", model.reserved_usd),
                            (model.state == "settled", model.settled_usd),
                            else_=Decimal("0"),
                        )
                    ),
                    0,
                )
            ).where(model.business_date == business_date)
        )
        totals.append(Decimal(str(used)))
    return sum(totals, Decimal("0"))


async def _lock_budget_day(session: AsyncSession, business_date: str) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": f"agent-budget:{business_date}"},
    )


def _validate_budget_inputs(
    now: datetime, daily_limit_usd: Decimal, worst_case_usd: Decimal, pricing_version: str
) -> None:
    if now.tzinfo != UTC:
        raise ValueError("budget clock must be canonical UTC")
    _require_nonnegative_decimal(daily_limit_usd, "daily_limit_usd", positive=True)
    _require_nonnegative_decimal(worst_case_usd, "worst_case_usd")
    if not pricing_version.strip():
        raise ValueError("pricing_version is required")


def _require_nonnegative_decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be a finite non-negative Decimal")
    if positive and value == 0:
        raise ValueError(f"{name} must be positive")


def _schema_hash() -> str:
    return hashlib.sha256(
        (
            Path(__file__).resolve().parents[4] / "contracts/agent/research-intent-v1.schema.json"
        ).read_bytes()
    ).hexdigest()


def _tool_hash() -> str:
    return hashlib.sha256("|".join(EXPECTED_READ_ONLY_TOOL_NAMES).encode()).hexdigest()
