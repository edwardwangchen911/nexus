"""Immutable parent thesis and exact trigger observations for one-security review."""

import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.paper_context import AgentPaperPortfolioContext
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
from nexus.monitoring.evaluation import evaluate_price
from nexus.paper.agent_cycle_authority import CycleAuthority, validate_candidate_manifest
from nexus.paper.agent_cycle_models import CandidateManifestEntry
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchIntent

SOURCE = "nexus_price_monitor_increment"
PURPOSE: Literal["price_monitor_increment_v1"] = "price_monitor_increment_v1"


class FrozenMonitorContextStatus(StrEnum):
    MISSING = "missing"
    VALID = "valid"
    INVALID = "invalid"


class TriggerCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    trigger_id: UUID
    check_id: UUID
    cause: Literal["initial_match", "crossed"]
    checked_at: datetime
    observation: PriceObservation

    @field_validator("checked_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class MonitorResearchContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["price_monitor_increment_v1"] = PURPOSE
    account_id: UUID
    instrument_id: UUID
    parent_run_id: UUID
    parent_output_id: UUID
    parent_output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_intent: ResearchIntent
    plan_id: UUID
    plan_generation: int = Field(ge=1)
    trigger_checks: tuple[TriggerCheck, ...] = Field(min_length=1, max_length=32)
    decision_time: datetime
    decision_quote: PriceObservation
    candidate: CandidateManifestEntry
    authority: CycleAuthority
    paper_portfolio: AgentPaperPortfolioContext
    execution_locked: Literal[True] = True

    @field_validator("decision_time")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


def content(context: MonitorResearchContext) -> bytes:
    return json.dumps(context.model_dump(mode="json"), sort_keys=True).encode()


def is_monitor_snapshot(snapshot: DataSnapshot | None) -> bool:
    return bool(
        snapshot is not None
        and (
            snapshot.source == SOURCE
            or snapshot.schema_version == PURPOSE
            or snapshot.payload.get("purpose") == PURPOSE
            or "monitor_context" in snapshot.payload
        )
    )


async def read_monitor_context(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[MonitorResearchContext, ResearchEvidenceRef] | None:
    snapshot = await session.get(DataSnapshot, snapshot_id)
    if (
        snapshot is None
        or snapshot.source != SOURCE
        or snapshot.schema_version != PURPOSE
        or snapshot.provider_object_id != str(instrument_id)
        or snapshot.observed_at != as_of
        or snapshot.available_at > as_of
        or snapshot.received_at > as_of
        or snapshot.created_at > as_of
        or snapshot.payload.get("purpose") != PURPOSE
        or "agent_paper_context" in snapshot.payload
        or "portfolio_context" in snapshot.payload
        or hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
        != snapshot.revision
    ):
        return None
    try:
        context = MonitorResearchContext.model_validate(snapshot.payload["monitor_context"])
    except (KeyError, TypeError, ValidationError):
        return None
    if (
        context.instrument_id != instrument_id
        or context.decision_time != as_of
        or context.authority.account_id != context.account_id
        or context.authority.decision_time != as_of
        or context.candidate.instrument_id != instrument_id
        or len({t.trigger_id for t in context.trigger_checks}) != len(context.trigger_checks)
    ):
        return None
    parent = await session.get(AgentResearchOutputRecord, context.parent_output_id)
    parent_run = await session.get(AgentResearchRunRecord, context.parent_run_id)
    plan = await session.get(MonitorPlan, context.plan_id)
    if (
        parent is None
        or parent_run is None
        or plan is None
        or parent.run_id != parent_run.id
        or parent_run.instrument_id != instrument_id
        or parent.status != "completed"
        or parent.intent != context.parent_intent.model_dump(mode="json")
        or parent.content_hash != context.parent_output_hash
        or parent.created_at > as_of
        or parent_run.decision_time >= as_of
        or plan.account_id != context.account_id
        or plan.instrument_id != instrument_id
        or plan.source_run_id != parent_run.id
        or plan.source_output_id != parent.id
        or plan.source_hash != parent.content_hash
        or plan.generation != context.plan_generation
        or not plan.effective_at <= as_of < plan.valid_until
    ):
        return None
    condition = PriceReviewCondition.model_validate(plan.condition)
    if (
        condition.semantic_hash != plan.condition_hash
        or context.decision_quote.quote_id != context.candidate.decision_quote_id
        or context.decision_quote.artifact_id != context.candidate.decision_artifact_id
        or evaluate_price(
            condition, context.decision_quote, instrument_id=instrument_id, now=as_of
        ).result
        == "unknown"
    ):
        return None
    if not await validate_candidate_manifest(
        session,
        authority=context.authority,
        manifest=(context.candidate,),
        requested_instrument_ids=(instrument_id,),
        max_instruments=1,
    ):
        return None
    from nexus.monitoring.observations import read_latest_price

    if (
        await read_latest_price(session, instrument_id=instrument_id, now=as_of)
        != context.decision_quote
    ):
        return None
    # Each trigger retains its original observation/time, separately from the new quote.
    for item in context.trigger_checks:
        trigger = await session.get(MonitorTrigger, item.trigger_id)
        check = await session.get(MonitorCheck, item.check_id)
        if (
            trigger is None
            or check is None
            or trigger.check_id != check.id
            or trigger.account_id != context.account_id
            or trigger.instrument_id != instrument_id
            or check.plan_id != plan.id
            or check.result != "true"
            or trigger.cause != item.cause
            or check.checked_at != item.checked_at
            or check.observation != item.observation.model_dump(mode="json")
            or item.checked_at > as_of
            or evaluate_price(
                condition, item.observation, instrument_id=instrument_id, now=item.checked_at
            ).result
            != "true"
        ):
            return None
    raw = content(context)
    digest = hashlib.sha256(raw).hexdigest()
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == str(context.trigger_checks[0].trigger_id),
            ResearchEvidenceRef.revision == digest,
            ResearchEvidenceRef.content_hash == digest,
            ResearchEvidenceRef.byte_length == len(raw),
            ResearchEvidenceRef.available_at == as_of,
            ResearchEvidenceRef.location == f"memory://sha256/{digest}",
        )
    )
    return (context, evidence) if evidence is not None else None


async def revalidate_monitor_context(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    evidence_records: tuple[ResearchEvidenceRef, ...],
) -> FrozenMonitorContextStatus:
    records = tuple(row for row in evidence_records if row.source == SOURCE)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if not records and not is_monitor_snapshot(snapshot):
        return FrozenMonitorContextStatus.MISSING
    bound = await read_monitor_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if bound is None or len(records) != 1 or records[0].id != bound[1].id:
        return FrozenMonitorContextStatus.INVALID
    # A snapshot alone grants no job authority; the immutable member must match.
    from nexus.persistence.models.monitoring import MonitorIncrement

    member = await session.scalar(select(MonitorIncrement).where(MonitorIncrement.run_id == run.id))
    if (
        member is None
        or member.snapshot_id != run.data_snapshot_id
        or member.account_id != bound[0].account_id
    ):
        return FrozenMonitorContextStatus.INVALID
    return FrozenMonitorContextStatus.VALID
