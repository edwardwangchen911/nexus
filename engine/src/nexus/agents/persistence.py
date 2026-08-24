"""PostgreSQL-authoritative admission and daily budget accounting for Agent work."""

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.models import ResearchRequest
from nexus.agents.tools import EXPECTED_READ_ONLY_TOOL_NAMES
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
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
    prompt = (
        await session.get(AgentPromptVersionRecord, prompt_version_id)
        if prompt_version_id is not None
        else await session.scalar(
            select(AgentPromptVersionRecord)
            .where(AgentPromptVersionRecord.role == "orchestrator")
            .order_by(AgentPromptVersionRecord.created_at.desc())
            .limit(1)
        )
    )
    model = (
        await session.get(AgentModelConfigRecord, model_config_id)
        if model_config_id is not None
        else await session.scalar(
            select(AgentModelConfigRecord)
            .where(
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > now,
            )
            .order_by(AgentModelConfigRecord.created_at.desc())
            .limit(1)
        )
    )
    if (
        instrument is None
        or snapshot is None
        or snapshot.available_at > request.decision_time
        or snapshot.received_at > request.decision_time
        or prompt is None
        or prompt.role != "orchestrator"
        or model is None
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
    ):
        raise ValueError("point-in-time data authority is invalid")
    worst_case = model.worst_case_cost_usd
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
        prompt_hash=prompt.content_hash,
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
    session.add(
        Job(
            job_key=f"agent-research:{run.id}",
            input_hash=request_hash,
            status="QUEUED",
            schema_version="agent_research_job_v1",
            payload={"run_id": str(run.id)},
        )
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
