"""One foreground request's bounded slots and final SDK submission barrier."""

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic_ns
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
    sequence: int = 0
    first_error: BaseException | None = None
    first_failure_run: UUID | None = None

    def trace(
        self, event: str, run_id: UUID, *, code: str | None = None, stage: str | None = None
    ) -> None:
        self.sequence += 1
        row: dict[str, object] = {
            "event": event,
            "sequence": self.sequence,
            "monotonic_ns": monotonic_ns(),
            "request_id": str(self.request_id),
            "run_id": str(run_id),
        }
        if code is not None:
            row["code"] = code
        if stage in {"research", "critique", "synthesis"}:
            row["stage"] = stage
        logging.getLogger("nexus.research_dispatch").info(json.dumps(row))

    def stop(self, run_id: UUID, error: BaseException | None = None) -> None:
        if not self.stopped:
            self.first_error = error
            self.first_failure_run = run_id
            code = (
                error.reason
                if isinstance(error, CodexInvocationError)
                else "RESEARCH_EXECUTION_FAILED"
            )
            self.trace("first_failure", run_id, code=code)
        self.stopped = True

    def start(self, run_id: UUID) -> None:
        if self.stopped or len(self.active) >= 2 or run_id in self.active:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        if run_id not in {m.run_id for m in self.manifest.members}:
            raise ValueError("RESEARCH_DISPATCH_INVALID")
        self.active[run_id] = asyncio.Event()
        self.trace("slot_acquired", run_id)

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


def stop_current_dispatch(error: BaseException | None = None) -> None:
    state = current_dispatch()
    if state is not None:
        run_id = _RUN.get()
        assert run_id is not None
        state.stop(run_id, error)


@asynccontextmanager
async def dispatch_failure_boundary(session: AsyncSession | None = None) -> AsyncIterator[None]:
    """Stop before body cleanup and SQLAlchemy's internal rollback I/O."""
    from sqlalchemy import event

    state, run_id = current_dispatch(), _RUN.get()
    engine = session.get_bind() if session is not None and state is not None else None

    def before_rollback(*_: object) -> None:
        if current_dispatch(run_id) is state:
            stop_current_dispatch(sys.exc_info()[1])

    if engine is not None:
        event.listen(engine, "rollback", before_rollback)
        event.listen(engine, "rollback_savepoint", before_rollback)
    try:
        yield
    except BaseException as error:
        stop_current_dispatch(error)
        raise
    finally:
        if engine is not None:
            event.remove(engine, "rollback", before_rollback)
            event.remove(engine, "rollback_savepoint", before_rollback)


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


@contextmanager
def sdk_submission_trace(stage: str) -> Iterator[None]:
    state, run_id = current_dispatch(), _RUN.get()
    if state is not None and run_id is not None:
        state.trace("sdk_submitted", run_id, stage=stage)
    try:
        yield
    finally:
        if state is not None and run_id is not None:
            state.trace("sdk_returned", run_id, stage=stage)


@contextmanager
def dispatch_trace_output() -> Iterator[None]:
    """CLI-only safe diagnostics; no account facts, prompts or SDK identities."""
    logger = logging.getLogger("nexus.research_dispatch")
    handler = logging.StreamHandler()
    previous_level, previous_propagate = logger.level, logger.propagate
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate
        handler.close()
