"""Pure typed identities and state transitions for durable jobs."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

from nexus.audit.canonical import canonical_json

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ScopeKind = Literal["global", "portfolio", "instrument", "provider"]


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    DEAD_LETTER = "dead_letter"


class QueueName(StrEnum):
    GENERAL = "general"
    PROVIDER = "provider"
    BROKER = "broker"
    STRATEGY_RND = "strategy_rnd"


class RetryPolicy(StrEnum):
    AUTOMATIC_SAFE = "automatic_safe"
    MANUAL_SAFE = "manual_safe"
    NEVER = "never"


class JobType(StrEnum):
    DAILY_PIPELINE = "daily.pipeline"
    DAILY_MANUAL_PIPELINE = "daily.manual_pipeline"
    BROKER_SYNC = "broker.sync"
    BROKER_RECONCILE = "broker.reconcile"
    BROKER_SIMULATION_DISPATCH = "broker.simulation.dispatch"
    BROKER_SIMULATION_RECONCILE = "broker.simulation.reconcile"
    COLLECT_MARKET = "collect.market"
    COLLECT_FOSUN_DAILY_INSTRUMENT = "collect.fosun_daily_instrument"
    PUBLISH_FOSUN_DAILY = "publish.fosun_daily"
    COLLECT_SEC_MACRO = "collect.sec_macro"
    COLLECT_INTRADAY_SNAPSHOT = "collect.intraday_snapshot"
    QUALITY_FREEZE = "quality.freeze"
    DIAGNOSTICS_RUN = "diagnostics.run"
    PAPER_EXECUTE_DUE = "paper.execute_due"
    SHADOW_ADVANCE_DUE = "shadow.advance_due"
    AGENT_RESEARCH = "agent.research"
    AGENT_PORTFOLIO_DECISION = "agent.portfolio_decision"
    OPTIMIZATION_PROPOSE = "optimization.propose"
    PAPER_ADMIT_ORDERS = "paper.admit_orders"
    PAPER_PERFORMANCE = "paper.performance"
    DAILY_BRIEF = "daily.brief"
    AUDIT_MANIFEST = "audit.manifest"
    STRATEGY_RND = "strategy.rnd"


JOB_PRIORITIES: dict[JobType, int] = {
    JobType.BROKER_SYNC: 300,
    JobType.BROKER_RECONCILE: 300,
    JobType.BROKER_SIMULATION_DISPATCH: 300,
    JobType.BROKER_SIMULATION_RECONCILE: 300,
    JobType.QUALITY_FREEZE: 280,
    JobType.DIAGNOSTICS_RUN: 260,
    JobType.PAPER_EXECUTE_DUE: 260,
    JobType.SHADOW_ADVANCE_DUE: 260,
    JobType.PAPER_PERFORMANCE: 220,
    JobType.DAILY_BRIEF: 220,
    JobType.AUDIT_MANIFEST: 220,
    JobType.AGENT_RESEARCH: 180,
    JobType.AGENT_PORTFOLIO_DECISION: 180,
    JobType.OPTIMIZATION_PROPOSE: 180,
    JobType.PAPER_ADMIT_ORDERS: 180,
    JobType.DAILY_PIPELINE: 280,
    JobType.DAILY_MANUAL_PIPELINE: 280,
    JobType.COLLECT_MARKET: 280,
    JobType.COLLECT_FOSUN_DAILY_INSTRUMENT: 280,
    JobType.PUBLISH_FOSUN_DAILY: 280,
    JobType.COLLECT_SEC_MACRO: 280,
    JobType.COLLECT_INTRADAY_SNAPSHOT: 290,
    JobType.STRATEGY_RND: 20,
}

BROKER_JOB_TYPES = frozenset(
    {
        JobType.BROKER_SYNC,
        JobType.BROKER_RECONCILE,
        JobType.BROKER_SIMULATION_DISPATCH,
        JobType.BROKER_SIMULATION_RECONCILE,
    }
)

PROVIDER_JOB_TYPES = frozenset(
    {
        JobType.COLLECT_MARKET,
        JobType.COLLECT_FOSUN_DAILY_INSTRUMENT,
        JobType.PUBLISH_FOSUN_DAILY,
        JobType.COLLECT_SEC_MACRO,
        JobType.COLLECT_INTRADAY_SNAPSHOT,
    }
)


class JobKeyV1(BaseModel):
    """A canonical business identity independent of worker attempts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    job_type: JobType
    scope_kind: ScopeKind
    scope_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,100}$")
    effective_at: datetime
    input_hash: Hash64

    @field_validator("effective_at")
    @classmethod
    def _effective_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("job effective_at must use UTC")
        return value


def canonical_job_key(key: JobKeyV1) -> str:
    """Render the unique durable key exactly once."""
    effective_at = key.effective_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ":".join(
        (
            key.job_type.value,
            key.scope_kind,
            key.scope_id,
            effective_at,
            key.input_hash,
        )
    )


def job_input_hash(payload: object) -> str:
    """Hash one strict canonical input authority."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


_TRANSITIONS = frozenset(
    {
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.SKIPPED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.DEAD_LETTER),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.FAILED, JobStatus.QUEUED),
        (JobStatus.FAILED, JobStatus.DEAD_LETTER),
        (JobStatus.DEAD_LETTER, JobStatus.QUEUED),
    }
)


def require_job_transition(source: JobStatus, target: JobStatus) -> JobStatus:
    """Return the target only when it is a documented state-machine edge."""
    if (source, target) not in _TRANSITIONS:
        raise ValueError(f"job transition {source.value} -> {target.value} is invalid")
    return target
