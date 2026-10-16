"""Normal manual request admission. No provider/model calls or execution in GET/POST."""

from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.jobs.models import job_input_hash
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.current_preparation_recovery import PreparationRecoveryView
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    lock_portfolio_account,
    require_activation_projection_unoccupied,
    require_no_unknown_cancel_requests,
)
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

ManualMode = Literal["research_only", "research_and_paper"]
ManualStatus = Literal["queued", "preparing", "active", "completed", "unavailable", "unknown"]
_TERMINAL = {"completed", "unavailable", "unknown"}
_TRANSITIONS = {
    "queued": {"preparing", "unavailable"},
    "preparing": {"active", "unavailable", "unknown"},
    "active": {"completed", "unavailable", "unknown"},
}
_SAFE_CODES = {
    "MANUAL_REQUEST_QUEUED",
    "MANUAL_PREPARATION_STARTED",
    "MANUAL_CYCLE_CREATED",
    "MANUAL_RESEARCH_COMPLETED",
    "MANUAL_PAPER_COMPLETED",
    "MANUAL_INPUTS_UNAVAILABLE",
    "MANUAL_PREPARATION_CANCELLED",
    "MANUAL_RESULT_UNKNOWN",
}


class ManualRequestInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    mode: ManualMode


class ManualRequestView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: UUID
    mode: ManualMode
    requested_at: datetime
    status: ManualStatus
    code: str
    cycle_id: UUID | None
    execution_locked: Literal[True] = True
    preparation_recovery: PreparationRecoveryView | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    preparation_disposition: Literal["unknown_acknowledged"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def require_disposition_scope(self) -> "ManualRequestView":
        if self.preparation_disposition is not None and (
            self.status != "unknown"
            or self.code != "MANUAL_RESULT_UNKNOWN"
            or self.cycle_id is not None
        ):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        return self


def manual_request_hash(*, account_id: UUID, batch_id: UUID, mode: ManualMode) -> str:
    return job_input_hash(
        {
            "contract": "manual_paper_v1",
            "account_id": str(account_id),
            "candidate_batch_id": str(batch_id),
            "mode": mode,
        }
    )


async def read_manual_request(session: AsyncSession, row: ManualPaperRequest) -> ManualRequestView:
    events = (
        await session.scalars(
            select(ManualPaperRequestEvent)
            .where(ManualPaperRequestEvent.request_id == row.id)
            .order_by(ManualPaperRequestEvent.sequence)
        )
    ).all()
    if row.mode not in {"research_only", "research_and_paper"}:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if row.request_hash != manual_request_hash(
        account_id=row.account_id, batch_id=row.candidate_batch_id, mode=cast(ManualMode, row.mode)
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    previous = None
    prior = None
    for sequence, event in enumerate(events, 1):
        _validate_event(row, event, prior)
        if event.sequence != sequence or event.event_hash != _event_hash(row, event, previous):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        previous = event.event_hash
        prior = event
    if not events:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if events[-1].cycle_id is not None:
        await _require_manual_cycle_binding(session, row, events[-1].cycle_id)
    return ManualRequestView(
        request_id=row.id,
        mode=cast(ManualMode, row.mode),
        requested_at=row.requested_at,
        status=cast(ManualStatus, events[-1].status),
        code=events[-1].code,
        cycle_id=events[-1].cycle_id,
    )


def _validate_event(
    row: ManualPaperRequest,
    event: ManualPaperRequestEvent,
    prior: ManualPaperRequestEvent | None,
) -> None:
    codes = {
        "queued": {"MANUAL_REQUEST_QUEUED"},
        "preparing": {"MANUAL_PREPARATION_STARTED"},
        "active": {"MANUAL_CYCLE_CREATED"},
        "completed": {"MANUAL_RESEARCH_COMPLETED"}
        if row.mode == "research_only"
        else {"MANUAL_PAPER_COMPLETED"},
        "unavailable": {"MANUAL_INPUTS_UNAVAILABLE", "MANUAL_PREPARATION_CANCELLED"},
        "unknown": {"MANUAL_RESULT_UNKNOWN"},
    }
    if (
        event.code not in codes.get(event.status, set())
        or event.occurred_at.utcoffset() is None
        or event.occurred_at < row.requested_at
        or (prior is None and (event.status != "queued" or event.cycle_id is not None))
        or (
            prior is not None
            and (
                event.status not in _TRANSITIONS.get(prior.status, set())
                or event.occurred_at < prior.occurred_at
                or (prior.cycle_id is not None and event.cycle_id != prior.cycle_id)
            )
        )
        or (event.status in {"queued", "preparing"} and event.cycle_id is not None)
        or (event.status in {"active", "completed"} and event.cycle_id is None)
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")


def _event_hash(
    row: ManualPaperRequest, event: ManualPaperRequestEvent, previous: str | None
) -> str:
    return job_input_hash(
        {
            "request_id": str(row.id),
            "request_hash": row.request_hash,
            "sequence": event.sequence,
            "status": event.status,
            "code": event.code,
            "cycle_id": None if event.cycle_id is None else str(event.cycle_id),
            "occurred_at": event.occurred_at.isoformat(),
            "previous": previous,
        }
    )


async def _append(
    session: AsyncSession,
    row: ManualPaperRequest,
    *,
    status: ManualStatus,
    code: str,
    at: datetime,
    cycle_id: UUID | None = None,
) -> ManualRequestView:
    await lock_portfolio_account(session, row.account_id)
    await session.scalar(
        select(ManualPaperRequest).where(ManualPaperRequest.id == row.id).with_for_update()
    )
    if code not in _SAFE_CODES:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    prior = await session.scalar(
        select(ManualPaperRequestEvent)
        .where(ManualPaperRequestEvent.request_id == row.id)
        .order_by(ManualPaperRequestEvent.sequence.desc())
        .limit(1)
    )
    if prior is not None and status not in _TRANSITIONS.get(prior.status, set()):
        raise ValueError("MANUAL_REQUEST_TERMINAL_OR_UNKNOWN")
    if prior is None and status != "queued":
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    event = ManualPaperRequestEvent(
        request_id=row.id,
        sequence=1 if prior is None else prior.sequence + 1,
        status=status,
        code=code,
        cycle_id=cycle_id,
        occurred_at=at,
    )
    _validate_event(row, event, prior)
    event.event_hash = _event_hash(row, event, None if prior is None else prior.event_hash)
    session.add(event)
    await session.flush()
    return await read_manual_request(session, row)


async def require_manual_account_available(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime,
    excluding_request_id: UUID | None = None,
) -> None:
    from nexus.paper.preparation_unknown_ack import require_acknowledged_native_claims

    # Exactly the target activation lock and projection authority, not a parallel lock system.
    await lock_portfolio_account(session, account_id)
    await require_acknowledged_native_claims(
        session, account_id=account_id, now=now, excluding_request_id=excluding_request_id
    )
    requests = (
        await session.scalars(
            select(ManualPaperRequest).where(ManualPaperRequest.account_id == account_id)
        )
    ).all()
    for request in requests:
        if request.id == excluding_request_id:
            continue
        view = await read_manual_request(session, request)
        if view.code == "MANUAL_PREPARATION_CANCELLED":
            from nexus.paper.current_preparation_recovery import read_preparation_recovery

            recovery = await read_preparation_recovery(session, request)
            if recovery is None or recovery.disposition != "cancelled_before_call":
                raise ValueError("MANUAL_PREPARATION_RECOVERY_BLOCKED")
        if view.status == "unknown":
            from nexus.paper.manual_preparation_close import preparation_closed

            if await preparation_closed(session, request, now=now):
                continue
        if view.status not in _TERMINAL or view.status == "unknown":
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    cycles = (
        await session.scalars(
            select(AgentPaperCycleRecord.id).where(AgentPaperCycleRecord.account_id == account_id)
        )
    ).all()
    for cycle_id in cycles:
        status = await session.scalar(
            select(AgentPaperCycleEventRecord.to_status)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if status not in {"completed", "unavailable", "failed"}:
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        await _require_cycle_jobs_settled(session, cycle_id=cycle_id, now=now)
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )


async def admit_manual_request(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    model_config_id: UUID,
    mode: ManualMode,
    idempotency_key: str,
) -> ManualRequestView:
    ManualRequestInput(mode=mode)
    if not idempotency_key.strip() or len(idempotency_key) > 255:
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    key_hash = sha256(idempotency_key.encode()).hexdigest()
    identity = manual_request_hash(account_id=account_id, batch_id=batch_id, mode=mode)
    existing = await session.scalar(
        select(ManualPaperRequest).where(
            ManualPaperRequest.account_id == account_id, ManualPaperRequest.key_hash == key_hash
        )
    )
    if existing is not None:
        if existing.request_hash != identity:
            raise ValueError("MANUAL_IDEMPOTENCY_CONFLICT")
        # Read historical receipt before validating today's model/batch/availability.
        return await read_manual_request(session, existing)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    now = now.astimezone(UTC)
    await require_manual_account_available(session, account_id=account_id, now=now)
    batch, _ = await validate_agent_paper_candidate_batch(
        session, batch_id=batch_id, account_id=account_id, as_of=now
    )
    model = await require_formal_stage_model_profile(
        session, model_config_id=model_config_id, now=now
    )
    row = ManualPaperRequest(
        account_id=account_id,
        candidate_batch_id=batch_id,
        model_config_id=model.id,
        model_hash=model.config_hash,
        batch_hash=batch.batch_hash,
        mode=mode,
        key_hash=key_hash,
        request_hash=identity,
        requested_at=now,
    )
    session.add(row)
    await session.flush()
    return await _append(session, row, status="queued", code="MANUAL_REQUEST_QUEUED", at=now)


async def _require_cycle_jobs_settled(
    session: AsyncSession, *, cycle_id: UUID, now: datetime, require_no_live_lease: bool = True
) -> None:
    """A terminal cycle cannot hide a queued job, lease, or unknown model usage."""
    events = (
        await session.scalars(
            select(AgentPaperCycleEventRecord).where(
                AgentPaperCycleEventRecord.cycle_id == cycle_id
            )
        )
    ).all()
    run_ids: set[UUID] = set()
    try:
        for event in events:
            raw = event.fact_ids.get("research_run_ids", [])
            if not isinstance(raw, list):
                raise ValueError
            run_ids.update(UUID(str(value)) for value in raw)
    except (ValueError, TypeError):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN") from None
    research = (
        (
            await session.scalars(
                select(AgentResearchRunRecord).where(AgentResearchRunRecord.id.in_(run_ids))
            )
        ).all()
        if run_ids
        else []
    )
    synthesis = (
        await session.scalars(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id
            )
        )
    ).all()
    runs: list[AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord] = [*research, *synthesis]
    if len(research) != len(run_ids):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    for candidate_run in runs:
        if (
            candidate_run.status in {"completed", "failed", "abstained"}
            and candidate_run.settled_cost_usd is not None
        ):
            continue
        from nexus.paper.history_seal import legacy_research_sealed

        if not isinstance(
            candidate_run, AgentResearchRunRecord
        ) or not await legacy_research_sealed(
            session, cycle_id=cycle_id, run_id=candidate_run.id, now=now
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    if not runs:
        return
    jobs = (
        await session.scalars(
            select(Job).where(
                Job.job_type.in_(
                    ("agent.research", "agent.paper.research", "agent.portfolio_synthesis")
                ),
                Job.payload["run_id"].as_string().in_([str(run.id) for run in runs]),
            )
        )
    ).all()
    if len(jobs) != len(runs) or any(
        job.status not in {"succeeded", "failed", "skipped", "dead_letter"} for job in jobs
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    # Equal totals can hide two Jobs for one Run and none for another. A terminal
    # controller must prove each binding, not merely count terminal consumers.
    for candidate_run in runs:
        bound = [job for job in jobs if job.payload.get("run_id") == str(candidate_run.id)]
        allowed_types = (
            {"agent.research", "agent.paper.research"}
            if isinstance(candidate_run, AgentResearchRunRecord)
            else {"agent.portfolio_synthesis"}
        )
        if (
            len(bound) != 1
            or bound[0].payload != {"run_id": str(candidate_run.id)}
            or bound[0].job_type not in allowed_types
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    if require_no_live_lease:
        leases = tuple(
            await session.scalars(
                select(JobLease).where(
                    JobLease.job_id.in_([job.id for job in jobs]), JobLease.expires_at > now
                )
            )
        )
        jobs_by_id = {job.id: job for job in jobs}
        runs_by_id = {str(run.id): run for run in research}
        for lease in leases:
            job = jobs_by_id[lease.job_id]
            run = runs_by_id.get(str(job.payload.get("run_id")))
            # A freshly finalized cancellation retains its fenced lease audit.
            # Only its exact skipped attempt and untouched zero-cost Run prove
            # the lease is consumed; other unexpired leases remain blocking.
            if (
                job.status != "skipped"
                or job.result_code != "MANUAL_UNUSED_RESEARCH_CANCELLED"
                or job.attempt_count != 1
                or job.manual_retry_count != 0
                or lease.worker_id != f"manual-cancel:{cycle_id}"
                or run is None
                or run.status != "abstained"
                or run.settled_cost_usd != 0
                or run.input_tokens != 0
                or run.output_tokens != 0
                or not await session.scalar(
                    select(JobAttempt.id)
                    .where(
                        JobAttempt.job_id == job.id,
                        JobAttempt.lease_token == lease.lease_token,
                        JobAttempt.fence_token == lease.fence_token,
                        JobAttempt.attempt_no == 1,
                        JobAttempt.outcome == "skipped",
                        JobAttempt.finished_at <= now,
                    )
                    .limit(1)
                )
            ):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return now.astimezone(UTC)


async def close_failed_manual_request(
    session: AsyncSession, *, request_id: UUID, cancel_unused: bool = False
) -> ManualRequestView:
    """Reconcile a persisted native terminal only after all account work is settled."""
    from nexus.paper.agent_cycle_models import CycleStatus, canonical_cycle_event_hash

    row = await session.get(ManualPaperRequest, request_id)
    if row is None:
        raise ValueError("MANUAL_NOT_FOUND")
    await lock_portfolio_account(session, row.account_id)
    view = await read_manual_request(session, row)
    if view.status == "unavailable":
        return view
    if view.status != "active" or view.cycle_id is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    await _require_manual_cycle_binding(session, row, view.cycle_id)
    now = await _database_now(session)
    cycle = await session.scalar(
        select(AgentPaperCycleRecord)
        .where(AgentPaperCycleRecord.id == view.cycle_id)
        .with_for_update()
    )
    if cycle is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    events = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
            .order_by(AgentPaperCycleEventRecord.sequence)
        )
    )
    prior = None
    for sequence, event in enumerate(events, start=1):
        if (
            event.sequence != sequence
            or event.from_status != prior
            or event.occurred_at > now
            or event.event_hash
            != canonical_cycle_event_hash(
                cycle_id=cycle.id,
                sequence=event.sequence,
                from_status=cast(CycleStatus | None, event.from_status),
                to_status=cast(CycleStatus, event.to_status),
                result_code=event.result_code,
                fact_ids=event.fact_ids,
                occurred_at=event.occurred_at,
            )
        ):
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        prior = event.to_status
    if prior not in {"failed", "unavailable"}:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if cancel_unused:
        from nexus.paper.manual_cancel import cancel_unused_manual_research

        await cancel_unused_manual_research(session, cycle=cycle, events=events, now=now)
    await require_manual_account_available(
        session, account_id=row.account_id, now=now, excluding_request_id=row.id
    )
    return await record_manual_request_event(
        session, row, status="unavailable", code="MANUAL_INPUTS_UNAVAILABLE"
    )


async def claim_manual_request(
    session: AsyncSession,
    *,
    request_id: UUID,
) -> tuple[ManualRequestView, bool]:
    row = await session.get(ManualPaperRequest, request_id)
    if row is None:
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, row.account_id)
    row = await session.scalar(
        select(ManualPaperRequest).where(ManualPaperRequest.id == request_id).with_for_update()
    )
    assert row is not None
    view = await read_manual_request(session, row)
    if view.status != "queued":
        # Preparing is durable intent, never permission to repeat external work.
        return view, False
    now = await _database_now(session)
    await require_manual_account_available(
        session, account_id=row.account_id, now=now, excluding_request_id=row.id
    )
    batch, _ = await validate_agent_paper_candidate_batch(
        session, batch_id=row.candidate_batch_id, account_id=row.account_id, as_of=now
    )
    model = await require_formal_stage_model_profile(
        session, model_config_id=row.model_config_id, now=now
    )
    if batch.batch_hash != row.batch_hash or model.config_hash != row.model_hash:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return await _append(
        session, row, status="preparing", code="MANUAL_PREPARATION_STARTED", at=now
    ), True


async def record_manual_request_event(
    session: AsyncSession,
    row: ManualPaperRequest,
    *,
    status: ManualStatus,
    code: str,
    cycle_id: UUID | None = None,
) -> ManualRequestView:
    await lock_portfolio_account(session, row.account_id)
    view = await read_manual_request(session, row)
    if cycle_id is not None:
        await _require_manual_cycle_binding(session, row, cycle_id)
    return await _append(
        session,
        row,
        status=status,
        code=code,
        at=await _database_now(session),
        cycle_id=cycle_id if cycle_id is not None else view.cycle_id,
    )


async def _require_manual_cycle_binding(
    session: AsyncSession,
    row: ManualPaperRequest,
    cycle_id: UUID,
) -> None:
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    first = await session.scalar(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == cycle_id,
            AgentPaperCycleEventRecord.sequence == 1,
        )
    )
    if (
        cycle is None
        or first is None
        or cycle.account_id != row.account_id
        or cycle.candidate_batch_id != row.candidate_batch_id
        or cycle.decision_time < row.requested_at
        or first.fact_ids.get("manual_request_id") != str(row.id)
        or first.fact_ids.get("manual_mode") != row.mode
        or first.fact_ids.get("formal_model_config_id") != str(row.model_config_id)
        or first.fact_ids.get("formal_model_hash") != row.model_hash
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")


async def require_preparing_manual_request(
    session: AsyncSession,
    *,
    request_id: UUID,
    account_id: UUID,
    batch_id: UUID | None,
    model_config_id: UUID | None,
    now: datetime,
    execution_id: UUID | None = None,
) -> ManualPaperRequest:
    row = await session.get(ManualPaperRequest, request_id)
    if (
        row is None
        or row.account_id != account_id
        or row.candidate_batch_id != batch_id
        or row.model_config_id != model_config_id
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    view = await read_manual_request(session, row)
    if view.status != "preparing":
        raise ValueError("MANUAL_REQUEST_TERMINAL_OR_UNKNOWN")
    from nexus.paper.current_preparation_recovery import require_preparation_execution

    await require_preparation_execution(session, row, execution_id=execution_id)
    await require_manual_account_available(
        session,
        account_id=account_id,
        now=now,
        excluding_request_id=request_id,
    )
    batch, _ = await validate_agent_paper_candidate_batch(
        session, batch_id=batch_id, account_id=account_id, as_of=now
    )
    model = await require_formal_stage_model_profile(
        session, model_config_id=model_config_id, now=now
    )
    if batch.batch_hash != row.batch_hash or model.config_hash != row.model_hash:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return row


async def require_no_running_manual_request(
    session: AsyncSession, *, account_id: UUID, current_trigger_key: str | None = None
) -> None:
    from nexus.paper.preparation_unknown_ack import require_acknowledged_native_claims

    await lock_portfolio_account(session, account_id)
    await require_acknowledged_native_claims(
        session,
        account_id=account_id,
        now=await _database_now(session),
        current_trigger_key=current_trigger_key,
    )
    rows = (
        await session.scalars(
            select(ManualPaperRequest).where(ManualPaperRequest.account_id == account_id)
        )
    ).all()
    for row in rows:
        view = await read_manual_request(session, row)
        if view.code == "MANUAL_PREPARATION_CANCELLED":
            from nexus.paper.current_preparation_recovery import read_preparation_recovery

            recovery = await read_preparation_recovery(session, row)
            if recovery is None or recovery.disposition != "cancelled_before_call":
                raise ValueError("MANUAL_PREPARATION_RECOVERY_BLOCKED")
        # A pending click yields to the scheduled entry; started/unknown work never does.
        if view.status == "unknown":
            from nexus.paper.manual_preparation_close import preparation_closed

            if await preparation_closed(session, row, now=await _database_now(session)):
                continue
        if view.status in {"preparing", "active", "unknown"}:
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def manual_request_for_cycle(
    session: AsyncSession, *, cycle_id: UUID
) -> ManualPaperRequest | None:
    first = await session.scalar(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == cycle_id,
            AgentPaperCycleEventRecord.sequence == 1,
        )
    )
    if first is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    raw = first.fact_ids.get("manual_request_id")
    if raw is None:
        if "manual_mode" in first.fact_ids:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        return None
    try:
        request_id = UUID(str(raw))
    except (ValueError, TypeError):
        raise ValueError("MANUAL_RESULT_UNKNOWN") from None
    row = await session.get(ManualPaperRequest, request_id)
    if row is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    await _require_manual_cycle_binding(session, row, cycle_id)
    view = await read_manual_request(session, row)
    if view.cycle_id != cycle_id:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return row


async def require_cycle_execution_mode(session: AsyncSession, *, cycle_id: UUID) -> None:
    row = await manual_request_for_cycle(session, cycle_id=cycle_id)
    if row is not None and row.mode == "research_only":
        raise ValueError("MANUAL_RESEARCH_ONLY_EXECUTION_FORBIDDEN")
