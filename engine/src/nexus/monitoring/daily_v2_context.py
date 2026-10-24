"""Historical typed events in an independent daily snapshot, without re-admission."""

import json
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.monitoring.batch_inputs import BatchInputs, read_batch_inputs
from nexus.monitoring.daily_context import (
    DailyMonitorContext,
    freeze_daily_pending,
    validate_daily_pending,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job

if TYPE_CHECKING:
    from nexus.monitoring.source_qualification import QualifiedSource


class DailyEventCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    trigger_id: UUID
    check_id: UUID
    plan_id: UUID
    kind: Literal["price", "disclosure"]
    event_key: str
    cause: Literal["initial_match", "crossed", "disclosure_added"]
    checked_at: datetime
    observation: dict[str, object]
    original_run_id: UUID | None
    original_output_id: UUID | None
    batch_id: UUID | None
    materials_snapshot_id: UUID | None

    @field_validator("checked_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class DailyEventMaterial(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    snapshot_id: UUID
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: BatchInputs


class DailyEventPredecessor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    plan_id: UUID | None
    generation: int | None = Field(ge=1)

    @model_validator(mode="after")
    def paired(self) -> "DailyEventPredecessor":
        if (self.plan_id is None) != (self.generation is None):
            raise ValueError("DAILY_EVENT_PREDECESSOR_INVALID")
        return self


class DailyEventContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["daily_monitor_context_v2"] = "daily_monitor_context_v2"
    cycle_id: UUID
    account_id: UUID
    instrument_id: UUID
    decision_time: datetime
    trigger_checks: tuple[DailyEventCheck, ...] = Field(min_length=1, max_length=32)
    materials: tuple[DailyEventMaterial, ...] = Field(min_length=1, max_length=32)
    legacy: DailyMonitorContext | None = None
    predecessors: dict[Literal["price", "disclosure"], DailyEventPredecessor]

    @field_validator("decision_time")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def complete(self) -> "DailyEventContext":
        if set(self.predecessors) != {"price", "disclosure"} or len(
            {row.trigger_id for row in self.trigger_checks}
        ) != len(self.trigger_checks):
            raise ValueError("DAILY_EVENT_CONTEXT_INVALID")
        return self

    @property
    def content_hash(self) -> str:
        return sha256(self.canonical()).hexdigest()

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class DailyEventGap(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["daily_monitor_gap_v2"] = "daily_monitor_gap_v2"
    scope: Literal["whole_stock_typed_v2"] = "whole_stock_typed_v2"
    observed_at: datetime
    pending_count: int = Field(ge=1, strict=True)
    max_pending: Literal[32] = 32
    reason: Literal[
        "MONITOR_PENDING_OVERFLOW",
        "MONITOR_CONTENT_MISSING",
        "MONITOR_CONTENT_INVALID",
        "MONITOR_CONTENT_CAPACITY",
        "MONITOR_ORIGINAL_REVIEW_UNAVAILABLE",
    ]

    @field_validator("observed_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


async def _predecessors(
    session: AsyncSession, account: UUID, instrument: UUID, as_of: datetime
) -> dict[Literal["price", "disclosure"], DailyEventPredecessor]:
    result: dict[Literal["price", "disclosure"], DailyEventPredecessor] = {}
    kinds: tuple[Literal["price", "disclosure"], ...] = ("price", "disclosure")
    for kind in kinds:
        plan = await session.scalar(
            select(MonitorPlan)
            .where(
                MonitorPlan.account_id == account,
                MonitorPlan.instrument_id == instrument,
                MonitorPlan.kind == kind,
                MonitorPlan.created_at <= as_of,
            )
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        result[kind] = DailyEventPredecessor(
            plan_id=None if plan is None else plan.id,
            generation=None if plan is None else plan.generation,
        )
    return result


async def _valid_predecessors(session: AsyncSession, context: DailyEventContext) -> bool:
    # Later installation may share T0 in a fixed clock. Re-reading latest would
    # rewrite the historical predecessor; only the replacement writer does CAS.
    for kind, frozen in context.predecessors.items():
        if frozen.plan_id is None:
            continue
        plan = await session.get(MonitorPlan, frozen.plan_id)
        if (
            plan is None
            or (plan.account_id, plan.instrument_id, plan.kind, plan.generation)
            != (context.account_id, context.instrument_id, kind, frozen.generation)
            or plan.created_at > context.decision_time
        ):
            return False
    return True


async def _original_unresolved(
    session: AsyncSession,
    trigger: MonitorTrigger,
    as_of: datetime,
    *,
    qualified_sources: dict[tuple[UUID, UUID, datetime], "QualifiedSource"] | None = None,
) -> tuple[UUID, UUID]:
    # Lazy imports: source qualification and mixed context import paper_context.
    from nexus.monitoring.admission import require_increment_binding
    from nexus.monitoring.mixed_context import MixedMonitorContext
    from nexus.monitoring.source_qualification import load_qualified_source

    run = await session.get(AgentResearchRunRecord, trigger.research_run_id)
    if run is None or trigger.status != "completed" or run.status != "completed":
        raise ValueError("MONITOR_ORIGINAL_REVIEW_UNAVAILABLE")
    member = await require_increment_binding(session, run)
    job = await session.get(Job, member.job_id)
    output = await session.scalar(
        select(AgentResearchOutputRecord).where(
            AgentResearchOutputRecord.run_id == run.id,
            AgentResearchOutputRecord.role.is_(None),
            AgentResearchOutputRecord.status == "completed",
        )
    )
    if (
        job is None
        or job.finished_at is None
        or job.finished_at > as_of
        or output is None
        or max(run.created_at, output.created_at) > as_of
        or member.batch_id != trigger.batch_id
    ):
        raise ValueError("MONITOR_ORIGINAL_REVIEW_UNAVAILABLE")
    key = (trigger.account_id, output.id, job.finished_at)
    source = None if qualified_sources is None else qualified_sources.get(key)
    if source is None:
        source = await load_qualified_source(
            session, account_id=trigger.account_id, output_id=output.id, now=job.finished_at
        )
    if not isinstance(source.context, MixedMonitorContext):
        raise ValueError("MONITOR_ORIGINAL_REVIEW_UNAVAILABLE")
    if qualified_sources is not None:
        qualified_sources[key] = source
    market = next(row for row in source.role_outputs if row.role == "market_state")
    proposal = market.mixed_event_proposal
    items = (
        []
        if proposal is None
        else [item for item in proposal.items if item.trigger_id == trigger.id]
    )
    context_items = [
        item for item in source.context.trigger_checks if item.trigger_id == trigger.id
    ]
    if (
        len(items) != 1
        or items[0].status != "unresolved"
        or items[0].kind != trigger.kind
        or items[0].event_key != trigger.event_key
        or len(context_items) != 1
        or context_items[0].check_id != trigger.check_id
        or source.context.batch_id != trigger.batch_id
    ):
        raise ValueError("MONITOR_ORIGINAL_REVIEW_UNAVAILABLE")
    from nexus.monitoring.mixed_contracts import validate_mixed_proposal

    assert proposal is not None
    validate_mixed_proposal(
        proposal, context=source.context.model_dump(mode="json"), evidence_refs=source.evidence
    )
    return output.id, source.context.materials_snapshot_id


async def _material(
    session: AsyncSession, snapshot_id: UUID, as_of: datetime
) -> DailyEventMaterial:
    row = await session.get(DataSnapshot, snapshot_id)
    payload = await read_batch_inputs(session, snapshot_id=snapshot_id, as_of=as_of)
    if row is None or payload is None:
        raise ValueError("MONITOR_CONTENT_INVALID")
    return DailyEventMaterial(snapshot_id=row.id, content_hash=row.revision, payload=payload)


async def freeze_daily_events(
    session: AsyncSession, *, cycle_id: UUID, instrument_id: UUID, as_of: datetime
) -> tuple[DailyEventContext | None, DailyEventGap | None]:
    from nexus.monitoring.daily_consumption import not_daily_covered
    from nexus.monitoring.mixed_readback import not_mixed_consumed
    from nexus.monitoring.storage import lock_security

    as_of = require_utc(as_of)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.decision_time != as_of:
        raise ValueError("DAILY_EVENT_CONTEXT_INVALID")
    await lock_security(session, account_id=cycle.account_id, instrument_id=instrument_id)
    predicates = (
        MonitorTrigger.account_id == cycle.account_id,
        MonitorTrigger.instrument_id == instrument_id,
        MonitorTrigger.created_at <= as_of,
        MonitorCheck.created_at <= as_of,
        MonitorCheck.checked_at <= as_of,
        not_daily_covered(),
        not_mixed_consumed(),
    )
    typed = await session.scalar(
        select(MonitorTrigger.id)
        .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
        .where(*predicates, MonitorTrigger.batch_id.is_not(None))
        .limit(1)
    )
    if typed is None:
        return None, None
    rows = (
        await session.execute(
            select(MonitorTrigger, MonitorCheck, func.count().over())
            .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
            .where(
                *predicates,
                or_(
                    MonitorTrigger.batch_id.is_not(None),
                    (MonitorTrigger.kind == "price")
                    & MonitorTrigger.research_run_id.is_(None)
                    & MonitorTrigger.status.in_(("pending", "blocked")),
                ),
            )
            .order_by(MonitorCheck.checked_at, MonitorTrigger.id)
            .limit(33)
        )
    ).all()
    count = int(rows[0][2])

    def gap(reason: str) -> tuple[None, DailyEventGap]:
        return None, DailyEventGap.model_validate(
            {"observed_at": as_of, "pending_count": count, "reason": reason}
        )

    if count > 32:
        return gap("MONITOR_PENDING_OVERFLOW")
    checks, materials = [], {}
    qualified_sources: dict[tuple[UUID, UUID, datetime], QualifiedSource] = {}
    try:
        legacy = await freeze_daily_pending(
            session, cycle_id=cycle_id, instrument_id=instrument_id, as_of=as_of
        )
        for trigger, check, _ in rows:
            output_id, material_id = None, None
            if trigger.batch_id is not None:
                if trigger.research_run_id is not None:
                    output_id, material_id = await _original_unresolved(
                        session, trigger, as_of, qualified_sources=qualified_sources
                    )
                else:
                    if trigger.status not in {"pending", "blocked"}:
                        return gap("MONITOR_ORIGINAL_REVIEW_UNAVAILABLE")
                    material_id = await session.scalar(
                        select(DataSnapshot.id)
                        .where(
                            DataSnapshot.source == "nexus_monitor_batch_inputs",
                            DataSnapshot.provider_object_id == str(trigger.batch_id),
                            DataSnapshot.created_at <= as_of,
                            DataSnapshot.available_at <= as_of,
                            DataSnapshot.received_at <= as_of,
                            DataSnapshot.observed_at <= as_of,
                        )
                        .order_by(DataSnapshot.created_at, DataSnapshot.id)
                        .limit(1)
                    )
                    if material_id is None:
                        return gap("MONITOR_CONTENT_MISSING")
                materials[material_id] = await _material(session, material_id, as_of)
            checks.append(
                DailyEventCheck(
                    trigger_id=trigger.id,
                    check_id=check.id,
                    plan_id=check.plan_id,
                    kind=trigger.kind,
                    event_key=trigger.event_key,
                    cause=trigger.cause,
                    checked_at=check.checked_at,
                    observation=check.observation,
                    original_run_id=trigger.research_run_id,
                    original_output_id=output_id,
                    batch_id=trigger.batch_id,
                    materials_snapshot_id=material_id,
                )
            )
        context = DailyEventContext(
            cycle_id=cycle.id,
            account_id=cycle.account_id,
            instrument_id=instrument_id,
            decision_time=as_of,
            trigger_checks=tuple(checks),
            materials=tuple(materials[key] for key in sorted(materials, key=str)),
            legacy=legacy,
            predecessors=await _predecessors(session, cycle.account_id, instrument_id, as_of),
        )
        if len(context.canonical()) > 256 * 1024:
            return gap("MONITOR_CONTENT_CAPACITY")
        if not await validate_daily_events(session, context):
            return gap("MONITOR_CONTENT_INVALID")
        return context, None
    except (ValueError, ValidationError, KeyError, TypeError) as error:
        return gap(
            "MONITOR_ORIGINAL_REVIEW_UNAVAILABLE"
            if str(error) == "MONITOR_ORIGINAL_REVIEW_UNAVAILABLE"
            else "MONITOR_CONTENT_INVALID"
        )


async def validate_daily_events(session: AsyncSession, context: DailyEventContext) -> bool:
    """Recheck the exact frozen set, never re-select today's pending or predecessors."""
    qualified_sources: dict[tuple[UUID, UUID, datetime], QualifiedSource] = {}
    try:
        cycle = await session.get(AgentPaperCycleRecord, context.cycle_id)
        if (
            cycle is None
            or cycle.account_id != context.account_id
            or cycle.decision_time != context.decision_time
            or len(context.canonical()) > 256 * 1024
            or not await _valid_predecessors(session, context)
        ):
            return False
        materials = {row.snapshot_id: row for row in context.materials}
        if len(materials) != len(context.materials) or set(materials) != {
            item.materials_snapshot_id
            for item in context.trigger_checks
            if item.batch_id is not None
        }:
            return False
        for material in materials.values():
            if await _material(session, material.snapshot_id, context.decision_time) != material:
                return False
        legacy_ids = set()
        if context.legacy is not None:
            if (
                context.legacy.cycle_id,
                context.legacy.account_id,
                context.legacy.instrument_id,
                context.legacy.decision_time,
            ) != (
                context.cycle_id,
                context.account_id,
                context.instrument_id,
                context.decision_time,
            ) or not await validate_daily_pending(session, context.legacy):
                return False
            legacy_ids = {item.trigger_id for item in context.legacy.trigger_checks}
        if legacy_ids != {
            item.trigger_id for item in context.trigger_checks if item.batch_id is None
        }:
            return False
        for item in context.trigger_checks:
            trigger = await session.get(MonitorTrigger, item.trigger_id)
            check = await session.get(MonitorCheck, item.check_id)
            if (
                trigger is None
                or check is None
                or trigger.check_id != check.id
                or (trigger.account_id, trigger.instrument_id, trigger.batch_id)
                != (context.account_id, context.instrument_id, item.batch_id)
                or (check.account_id, check.instrument_id, check.plan_id, check.kind)
                != (context.account_id, context.instrument_id, item.plan_id, item.kind)
                or (trigger.kind, trigger.event_key, trigger.cause)
                != (item.kind, item.event_key, item.cause)
                or check.checked_at != item.checked_at
                or check.observation != item.observation
                or check.result != "true"
                or max(trigger.created_at, check.created_at, check.checked_at)
                > context.decision_time
            ):
                return False
            if item.original_run_id is not None:
                if trigger.research_run_id != item.original_run_id:
                    return False
                output_id, material_id = await _original_unresolved(
                    session, trigger, context.decision_time, qualified_sources=qualified_sources
                )
                if (
                    output_id != item.original_output_id
                    or material_id != item.materials_snapshot_id
                ):
                    return False
            elif item.original_output_id is not None:
                return False
            # A formerly unassigned trigger may be claimed after this snapshot.
            # Keep that historical input readable; consumption performs the live fence.
            if item.batch_id is not None:
                if item.materials_snapshot_id is None:
                    return False
                material = materials[item.materials_snapshot_id]
                payload = material.payload
                expected = {
                    key: value
                    for key, value in item.model_dump(mode="json").items()
                    if key
                    not in {
                        "original_run_id",
                        "original_output_id",
                        "batch_id",
                        "materials_snapshot_id",
                    }
                }
                expected["checked_at"] = item.checked_at.isoformat()
                if (payload.account_id, payload.instrument_id, payload.batch_id) != (
                    context.account_id,
                    context.instrument_id,
                    item.batch_id,
                ) or [
                    row for row in payload.members if row["trigger_id"] == str(item.trigger_id)
                ] != [expected]:
                    return False
            elif item.materials_snapshot_id is not None:
                return False
        return True
    except (ValueError, ValidationError, KeyError, TypeError):
        return False
