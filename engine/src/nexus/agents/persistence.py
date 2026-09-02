"""PostgreSQL-authoritative admission and daily budget accounting for Agent work."""

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE
from nexus.agents.models import ResearchRequest
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.price_authority import freeze_price_authority
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
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_ADMISSION_SCHEMA_VERSION = "agent_research_run_v1"


@dataclass(frozen=True)
class ResearchAdmission:
    run: AgentResearchRunRecord
    replayed: bool

    @property
    def run_id(self) -> UUID:
        return self.run.id


async def load_complete_prompt_authority(
    session: AsyncSession, *, orchestrator_id: UUID | None = None
) -> dict[AgentRole, AgentPromptVersionRecord] | None:
    """Select one hash-valid version for every role as a complete admission unit."""
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord).order_by(
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
    if orchestrator_id is not None and selected["orchestrator"].id != orchestrator_id:
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
    used = await session.scalar(
        select(
            func.coalesce(
                func.sum(
                    case(
                        (
                            AgentDailyBudgetReservationRecord.state == "reserved",
                            AgentDailyBudgetReservationRecord.reserved_usd,
                        ),
                        (
                            AgentDailyBudgetReservationRecord.state == "settled",
                            AgentDailyBudgetReservationRecord.settled_usd,
                        ),
                        else_=Decimal("0"),
                    )
                ),
                0,
            )
        ).where(AgentDailyBudgetReservationRecord.business_date == business_date)
    )
    if Decimal(str(used)) + worst_case_usd > daily_limit_usd:
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
) -> ResearchAdmission:
    """Atomically claim, validate point-in-time authority, reserve, and append one run."""
    if now.tzinfo != UTC or request.decision_time > now:
        raise ValueError("point-in-time data authority is invalid")
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
    instrument = await session.get(Instrument, request.instrument_id)
    snapshot = await session.get(DataSnapshot, request.data_snapshot_id)
    prompts = await load_complete_prompt_authority(session, orchestrator_id=prompt_version_id)
    model = (
        await session.get(AgentModelConfigRecord, model_config_id)
        if model_config_id is not None
        else await session.scalar(
            select(AgentModelConfigRecord)
            .where(
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > now,
                # Experimental staged research requires an explicit immutable model ID.
                AgentModelConfigRecord.version != "codex-sdk-chatgpt-v2",
            )
            .order_by(AgentModelConfigRecord.created_at.desc())
            .limit(1)
        )
    )
    if instrument is not None and snapshot is not None:
        await freeze_price_authority(
            session,
            snapshot=snapshot,
            instrument_id=request.instrument_id,
            decision_time=request.decision_time,
            allow_fixture=allow_fixture_price_authority,
        )
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
            job_type=JobType.AGENT_RESEARCH,
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
            job_type=JobType.AGENT_RESEARCH,
            scope_kind="instrument",
            scope_id=str(request.instrument_id),
            effective_at=request.decision_time,
            input_hash=durable_input_hash,
        ),
        schema_version="agent_research_job_v1",
        payload={"run_id": str(run.id)},
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=1
        if model.version in {"codex-sdk-chatgpt-v1", "codex-sdk-chatgpt-v2"}
        else 3,
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
