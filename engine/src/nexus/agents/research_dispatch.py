"""One foreground request's bounded slots and final SDK submission barrier."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.repository import ClaimedJob
from nexus.paper.manual_request import ResearchManifestView
from nexus.persistence.models.manual_paper import ManualPaperRequest


def two_flight_policy() -> dict[str, object]:
    return {
        "version": "research_two_flight_v1",
        "max_in_flight": 2,
        "dispatch_order": "manifest_order",
        "first_failure": "stop_at_stage_boundary",
    }


def uses_two_flight(profile: dict[str, object] | None) -> bool:
    from nexus.jobs.models import job_input_hash

    if profile is None:
        return False
    if not isinstance(profile, dict):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if "execution_policy" not in profile:
        return False
    policy = profile["execution_policy"]
    if not isinstance(policy, dict) or job_input_hash(policy) != job_input_hash(
        two_flight_policy()
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    return True


@dataclass
class ResearchDispatch:
    request_id: UUID
    manifest: ResearchManifestView
    stopped: bool = False
    active: dict[UUID, asyncio.Event] = field(default_factory=dict)
    claims: dict[UUID, ClaimedJob] = field(default_factory=dict)

    def start(self, run_id: UUID) -> None:
        if self.stopped or len(self.active) >= 2 or run_id in self.active:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        if run_id not in {m.run_id for m in self.manifest.members}:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        self.active[run_id] = asyncio.Event()

    def bind_claim(self, run_id: UUID, claim: ClaimedJob) -> None:
        member = next((m for m in self.manifest.members if m.run_id == run_id), None)
        if run_id not in self.active or member is None or member.job_id != claim.job_id:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        if run_id in self.claims and self.claims[run_id] != claim:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        self.claims[run_id] = claim
        self.active[run_id].set()

    def guard(self) -> None:
        if self.stopped:
            raise CodexInvocationError(
                "CODEX_RESEARCH_STOPPED", usage=UsageSnapshot(), provider_call_started=False
            )


_DISPATCH: ContextVar[ResearchDispatch | None] = ContextVar("research_dispatch", default=None)
_RUN: ContextVar[UUID | None] = ContextVar("research_dispatch_run", default=None)


def current_dispatch(run_id: UUID | None = None) -> ResearchDispatch | None:
    state = _DISPATCH.get()
    selected = _RUN.get()
    if state is None or selected not in state.active or run_id not in (None, selected):
        return None
    return state


def stop_current_dispatch() -> None:
    state = current_dispatch()
    if state is not None:
        state.stopped = True


@asynccontextmanager
async def dispatch_failure_boundary() -> AsyncIterator[None]:
    """Observe errors before an enclosing transaction's asynchronous rollback."""
    try:
        yield
    except BaseException:
        stop_current_dispatch()
        raise


@contextmanager
def dispatch_member(state: ResearchDispatch, run_id: UUID) -> Iterator[None]:
    token = _DISPATCH.set(state)
    run_token = _RUN.set(run_id)
    try:
        yield
    finally:
        _RUN.reset(run_token)
        _DISPATCH.reset(token)


async def require_dispatch_pool(
    session: AsyncSession,
    request: ManualPaperRequest,
    manifest: ResearchManifestView,
    selected_run_id: UUID,
) -> None:
    """Read brothers without taking their handler locks; never grants a claim."""
    from sqlalchemy import String, cast, select

    from nexus.paper.manual_request import manual_request_hash, read_manual_request
    from nexus.paper.research_request import database_now
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

    invalid = "RESEARCH_ONLY_POOL_BINDING_INVALID"
    state = current_dispatch(selected_run_id)
    receipt = await read_manual_request(session, request)
    if (
        state is None
        or state.request_id != request.id
        or state.manifest.manifest_hash != manifest.manifest_hash
        or receipt.status != "active"
        or request.mode != "research_only"
        or request.request_hash
        != manual_request_hash(
            account_id=request.account_id,
            batch_id=request.candidate_batch_id,
            mode="research_only",
            contract_version=request.contract_version,
            research_profile=request.research_profile,
        )
    ):
        raise ValueError(invalid)
    state.guard()
    now = await database_now(session)
    # One MVCC statement: a brother may commit between two separate table reads.
    rows = (
        await session.execute(
            select(Job, AgentResearchRunRecord, JobLease, JobAttempt)
            .join(
                AgentResearchRunRecord,
                Job.payload["run_id"].astext == cast(AgentResearchRunRecord.id, String),
            )
            .outerjoin(JobLease, JobLease.job_id == Job.id)
            .outerjoin(JobAttempt, (JobAttempt.job_id == Job.id) & (JobAttempt.attempt_no == 1))
            .where(Job.id.in_([m.job_id for m in manifest.members]))
            .execution_options(populate_existing=True)
        )
    ).all()
    state.guard()
    if len(rows) != len(manifest.members):
        raise ValueError(invalid)
    expected = {m.job_id: m.run_id for m in manifest.members}
    for job, run, lease, attempt in rows:
        if (
            expected.get(job.id) != run.id
            or job.max_attempts != 1
            or job.manual_retry_count != 0
            or run.retry_count != 0
        ):
            raise ValueError(invalid)
        if job.status == "queued" and job.attempt_count == 0:
            continue
        if (
            job.status == "succeeded"
            and run.status == "completed"
            and run.settled_cost_usd is not None
        ):
            continue
        claim = state.claims.get(run.id)
        if (
            job.status != "running"
            or job.attempt_count != 1
            or job.finished_at is not None
            or claim is None
            or run.status not in {"prepared", "running"}
            or run.settled_cost_usd is not None
        ):
            raise ValueError(invalid)
        if (
            lease is None
            or attempt is None
            or attempt.outcome != "running"
            or attempt.finished_at is not None
            or attempt.lease_token != claim.lease_token
            or attempt.fence_token != claim.fence_token
            or attempt.worker_id != claim.worker_id
            or lease.lease_token != claim.lease_token
            or lease.fence_token != claim.fence_token
            or lease.worker_id != claim.worker_id
            or lease.expires_at <= now
        ):
            raise ValueError(invalid)
