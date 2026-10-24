"""Exact historical pending evidence frozen inside a normal daily research context."""

import hashlib
import json
from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
from nexus.monitoring.evaluation import evaluate_price
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.strategy.intents import ResearchIntent


class DailyMonitorParent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    plan_id: UUID
    generation: int = Field(ge=1)
    condition: PriceReviewCondition
    condition_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_run_id: UUID
    parent_output_id: UUID
    parent_output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_intent: ResearchIntent


class DailyMonitorCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    trigger_id: UUID
    check_id: UUID
    plan_id: UUID
    cause: Literal["initial_match", "crossed"]
    checked_at: datetime
    observation: PriceObservation

    @field_validator("checked_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class DailyMonitorContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["daily_monitor_context_v1"] = "daily_monitor_context_v1"
    cycle_id: UUID
    account_id: UUID
    instrument_id: UUID
    decision_time: datetime
    parents: tuple[DailyMonitorParent, ...] = Field(min_length=1, max_length=32)
    trigger_checks: tuple[DailyMonitorCheck, ...] = Field(min_length=1, max_length=32)

    @field_validator("decision_time")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


async def freeze_daily_pending(
    session: AsyncSession, *, cycle_id: UUID, instrument_id: UUID, as_of: datetime
) -> DailyMonitorContext | None:
    from nexus.monitoring.pending import pending_stock
    from nexus.monitoring.storage import lock_security

    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or cycle.decision_time != as_of:
        raise ValueError("DAILY_MONITOR_CONTEXT_INVALID")
    await lock_security(session, account_id=cycle.account_id, instrument_id=instrument_id)
    rows = await pending_stock(
        session,
        account_id=cycle.account_id,
        instrument_id=instrument_id,
        now=as_of,
        allow_empty=True,
        freeze_visible_only=True,
    )
    if not rows:
        return None
    parents = {}
    checks = []
    for trigger, check in rows:
        plan = await session.get(MonitorPlan, check.plan_id)
        output = (
            None
            if plan is None
            else await session.get(AgentResearchOutputRecord, plan.source_output_id)
        )
        if plan is None or output is None or output.intent is None:
            raise ValueError("DAILY_MONITOR_CONTEXT_INVALID")
        parents[plan.id] = DailyMonitorParent(
            plan_id=plan.id,
            generation=plan.generation,
            condition=PriceReviewCondition.model_validate(plan.condition),
            condition_hash=plan.condition_hash,
            parent_run_id=plan.source_run_id,
            parent_output_id=plan.source_output_id,
            parent_output_hash=plan.source_hash,
            parent_intent=ResearchIntent.model_validate(output.intent),
        )
        checks.append(
            DailyMonitorCheck(
                trigger_id=trigger.id,
                check_id=check.id,
                plan_id=plan.id,
                cause=cast(Literal["initial_match", "crossed"], trigger.cause),
                checked_at=check.checked_at,
                observation=PriceObservation.model_validate(check.observation),
            )
        )
    context = DailyMonitorContext(
        cycle_id=cycle_id,
        account_id=cycle.account_id,
        instrument_id=instrument_id,
        decision_time=as_of,
        parents=tuple(parents.values()),
        trigger_checks=tuple(checks),
    )
    if not await validate_daily_pending(session, context):
        raise ValueError("DAILY_MONITOR_CONTEXT_INVALID")
    return context


async def validate_daily_pending(session: AsyncSession, context: DailyMonitorContext) -> bool:
    """Compare immutable original facts; never require historical plans still be active."""
    cycle = await session.get(AgentPaperCycleRecord, context.cycle_id)
    if (
        cycle is None
        or cycle.account_id != context.account_id
        or cycle.decision_time != context.decision_time
    ):
        return False
    parents = {parent.plan_id: parent for parent in context.parents}
    if (
        len(parents) != len(context.parents)
        or set(parents) != {item.plan_id for item in context.trigger_checks}
        or len({item.trigger_id for item in context.trigger_checks}) != len(context.trigger_checks)
        or len({item.check_id for item in context.trigger_checks}) != len(context.trigger_checks)
    ):
        return False
    plans = {}
    for parent in context.parents:
        plan = await session.get(MonitorPlan, parent.plan_id)
        output = await session.get(AgentResearchOutputRecord, parent.parent_output_id)
        if (
            plan is None
            or output is None
            or output.intent is None
            or plan.account_id != context.account_id
            or plan.instrument_id != context.instrument_id
            or plan.generation != parent.generation
            or plan.source_run_id != parent.parent_run_id
            or plan.source_output_id != output.id
            or output.run_id != parent.parent_run_id
            or plan.source_hash != parent.parent_output_hash
            or output.content_hash != parent.parent_output_hash
            or output.status != "completed"
            or output.created_at > context.decision_time
            or plan.created_at > context.decision_time
            or plan.condition_hash != parent.condition_hash
            or parent.condition.semantic_hash != parent.condition_hash
            or PriceReviewCondition.model_validate(plan.condition) != parent.condition
            or ResearchIntent.model_validate(output.intent) != parent.parent_intent
        ):
            return False
        plans[plan.id] = plan
    for item in context.trigger_checks:
        trigger = await session.get(MonitorTrigger, item.trigger_id)
        check = await session.get(MonitorCheck, item.check_id)
        plan = plans[item.plan_id]
        if (
            trigger is None
            or check is None
            or trigger.check_id != check.id
            or trigger.account_id != context.account_id
            or trigger.instrument_id != context.instrument_id
            or check.account_id != context.account_id
            or check.instrument_id != context.instrument_id
            or check.plan_id != item.plan_id
            or trigger.condition_hash != plan.condition_hash
            or trigger.cause != item.cause
            or check.checked_at != item.checked_at
            or check.result != "true"
            or check.observation != item.observation.model_dump(mode="json")
            or not plan.effective_at <= item.checked_at < plan.valid_until
            or max(trigger.created_at, check.created_at, item.checked_at) > context.decision_time
            or evaluate_price(
                parents[item.plan_id].condition,
                item.observation,
                instrument_id=context.instrument_id,
                now=item.checked_at,
            ).result
            != "true"
        ):
            return False
    return True
