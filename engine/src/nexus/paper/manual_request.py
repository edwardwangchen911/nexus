"""Normal manual request admission. No provider/model calls or execution in GET/POST."""

from datetime import UTC, datetime
from decimal import Decimal
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
from nexus.persistence.models.runtime import Job, JobLease

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
    "MANUAL_RESEARCH_STARTED",
    "MANUAL_RESEARCH_FINISHED",
    "MANUAL_RESEARCH_COMPLETED",
    "MANUAL_PAPER_COMPLETED",
    "MANUAL_INPUTS_UNAVAILABLE",
    "MANUAL_PREPARATION_CANCELLED",
    "MANUAL_RESULT_UNKNOWN",
}


class ManualRequestInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    mode: ManualMode


class ResearchMemberView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument_id: UUID
    run_id: UUID
    job_id: UUID
    status: str
    quality: str | None
    usage_known: bool


class ResearchManifestView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    manifest_id: UUID
    research_as_of: datetime
    manifest_hash: str
    members: list[ResearchMemberView]
    remote_latest_verified: Literal[False] = False
    execution_locked: Literal[True] = True


class ManualRequestView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: UUID
    mode: ManualMode
    requested_at: datetime
    status: ManualStatus
    code: str
    cycle_id: UUID | None
    execution_locked: Literal[True] = True
    research_contract: Literal["research_only_v2"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    research: ResearchManifestView | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
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


def manual_request_hash(
    *,
    account_id: UUID,
    batch_id: UUID,
    mode: ManualMode,
    contract_version: int = 1,
    research_profile: dict[str, object] | None = None,
) -> str:
    if contract_version not in {1, 2} or contract_version == 2 and mode != "research_only":
        raise ValueError("MANUAL_REQUEST_INVALID")
    return job_input_hash(
        {
            "contract": "manual_paper_v1" if contract_version == 1 else "research_only_v2",
            "account_id": str(account_id),
            "candidate_batch_id": str(batch_id),
            "mode": mode,
            **({"research_profile": research_profile} if contract_version == 2 else {}),
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
        account_id=row.account_id,
        batch_id=row.candidate_batch_id,
        mode=cast(ManualMode, row.mode),
        contract_version=row.contract_version,
        research_profile=row.research_profile,
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
    research = None
    if row.contract_version == 2:
        from nexus.paper.research_request import read_research_manifest

        research = await read_research_manifest(session, row)
        if events[-1].status in {"active", "completed"} and research is None:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
    return ManualRequestView(
        research_contract="research_only_v2" if row.contract_version == 2 else None,
        research=research,
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
    pure = getattr(row, "contract_version", 1) == 2
    if pure and row.mode != "research_only":
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    codes = {
        "queued": {"MANUAL_REQUEST_QUEUED"},
        "preparing": {"MANUAL_PREPARATION_STARTED"},
        "active": {"MANUAL_RESEARCH_STARTED"} if pure else {"MANUAL_CYCLE_CREATED"},
        "completed": {"MANUAL_RESEARCH_FINISHED"}
        if pure
        else {"MANUAL_RESEARCH_COMPLETED"}
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
        or (pure and event.cycle_id is not None)
        or (not pure and event.status in {"active", "completed"} and event.cycle_id is None)
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
    allow_scheduled_completion: bool = False,
) -> None:
    from nexus.paper.preparation_unknown_ack import require_acknowledged_native_claims

    # Exactly the target activation lock and projection authority, not a parallel lock system.
    await lock_portfolio_account(session, account_id)
    # Keep the primary request/occupancy diagnostic before its auxiliary claim.
    # Both guards remain required before admitting any new request.
    await require_manual_account_state_available(
        session,
        account_id=account_id,
        now=now,
        excluding_request_id=excluding_request_id,
        allow_scheduled_completion=allow_scheduled_completion,
    )
    await require_acknowledged_native_claims(
        session, account_id=account_id, now=now, excluding_request_id=excluding_request_id
    )


async def require_manual_account_state_available(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime,
    excluding_request_id: UUID | None = None,
    allow_scheduled_completion: bool = False,
) -> None:
    """Account state gates independent of native claim acceptance checks."""
    await lock_portfolio_account(session, account_id)
    from nexus.monitoring.admission import require_monitor_account_available

    await require_monitor_account_available(session, account_id=account_id, now=now)
    await require_manual_request_history(
        session, account_id=account_id, now=now, excluding_request_id=excluding_request_id
    )
    await require_settled_account_cycle_history(
        session,
        account_id=account_id,
        now=now,
        allow_scheduled_completion=allow_scheduled_completion,
    )
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=account_id, projection=projection
    )


async def require_manual_request_history(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime,
    excluding_request_id: UUID | None = None,
) -> None:
    """Shared request-history check; no cycle usage exemption is implied."""
    requests = (
        await session.scalars(
            select(ManualPaperRequest).where(ManualPaperRequest.account_id == account_id)
        )
    ).all()
    for request in requests:
        if request.id == excluding_request_id:
            continue
        view = await read_manual_request(session, request)
        if request.contract_version == 2 and view.research is not None:
            from nexus.paper.research_request import require_research_jobs_settled

            await require_research_jobs_settled(session, row=request, view=view.research, now=now)
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


async def require_settled_account_cycle_history(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime | None = None,
    allow_scheduled_completion: bool = False,
) -> None:
    """Share historical research safety without changing entry scheduling priority."""
    await lock_portfolio_account(session, account_id)
    if now is None:
        now = await _database_now(session)
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
            # Scheduled execution has no manual finalization event. Preserve
            # its existing deterministic completion projection without writing
            # history or treating an unfinished manual request as completed.
            if (
                not allow_scheduled_completion
                or status != "awaiting_market"
                or await manual_request_for_cycle(session, cycle_id=cycle_id) is not None
            ):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
            from nexus.paper.read_service import PaperReadService

            detail = await PaperReadService(session, now).agent_cycle_detail(cycle_id)
            if (
                detail is None
                or detail.cycle.status != "completed"
                or detail.cycle.result_code != "PAPER_CYCLE_COMPLETED"
            ):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        await _require_cycle_jobs_settled(
            session, cycle_id=cycle_id, now=now, allow_accepted_synthesis=True
        )


async def admit_manual_request(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    model_config_id: UUID,
    mode: ManualMode,
    idempotency_key: str,
    contract_version: int = 1,
    runtime_mode: str = "codex_sdk",
    daily_limit_usd: Decimal = Decimal("3.00"),
    two_flight: bool = False,
) -> ManualRequestView:
    ManualRequestInput(mode=mode)
    if (
        type(two_flight) is not bool
        or two_flight
        and (contract_version != 2 or mode != "research_only")
    ):
        raise ValueError("MANUAL_REQUEST_INVALID")
    if not idempotency_key.strip() or len(idempotency_key) > 255:
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    key_hash = sha256(idempotency_key.encode()).hexdigest()
    identity = manual_request_hash(
        account_id=account_id, batch_id=batch_id, mode=mode, contract_version=contract_version
    )
    existing = await session.scalar(
        select(ManualPaperRequest).where(
            ManualPaperRequest.account_id == account_id, ManualPaperRequest.key_hash == key_hash
        )
    )
    if existing is not None:
        if existing.request_hash != (
            manual_request_hash(
                account_id=account_id,
                batch_id=batch_id,
                mode=mode,
                contract_version=contract_version,
                research_profile=existing.research_profile,
            )
        ):
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
    if two_flight:
        from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION

        if model.version != BOUNDED_STAGE_BUDGET_CONFIG_VERSION:
            raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    profile: dict[str, object] | None = None
    if contract_version == 2:
        from nexus.agents.research_only import ensure_research_only_prompts

        if runtime_mode != "codex_sdk":
            raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
        prompts = await ensure_research_only_prompts(session)
        profile = {
            "runtime_mode": runtime_mode,
            "daily_limit_usd": format(daily_limit_usd.normalize(), "f"),
            "orchestrator_id": str(prompts["orchestrator"].id),
            "prompt_hashes": {role: prompt.content_hash for role, prompt in prompts.items()},
        }
        if two_flight:
            from nexus.agents.research_dispatch import two_flight_policy

            profile["execution_policy"] = two_flight_policy()
        identity = manual_request_hash(
            account_id=account_id,
            batch_id=batch_id,
            mode=mode,
            contract_version=contract_version,
            research_profile=profile,
        )
    row = ManualPaperRequest(
        research_profile=profile,
        contract_version=contract_version,
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
    session: AsyncSession,
    *,
    cycle_id: UUID,
    now: datetime,
    require_no_live_lease: bool = True,
    allow_accepted_synthesis: bool = False,
    read_only: bool = False,
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
        if isinstance(candidate_run, AgentPortfolioSynthesisRunRecord):
            from nexus.paper.synthesis_usage_acceptance import synthesis_usage_accepted

            if await synthesis_usage_accepted(
                session, cycle_id=cycle_id, run_id=candidate_run.id, now=now
            ):
                if not allow_accepted_synthesis:
                    raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
                # Scheduling only; never report financial settlement or output qualification.
                continue

        if (
            candidate_run.status in {"completed", "failed", "abstained"}
            and candidate_run.settled_cost_usd is not None
        ):
            continue
        from nexus.paper.history_seal import legacy_research_sealed

        if isinstance(candidate_run, AgentPortfolioSynthesisRunRecord):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
        if not await legacy_research_sealed(
            session, cycle_id=cycle_id, run_id=candidate_run.id, now=now, read_only=read_only
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
        from nexus.jobs.repository import job_lease_consumed

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
                or not await job_lease_consumed(session, job=job, lease=lease, now=now)
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
    from nexus.monitoring.admission import require_monitor_account_available

    await require_monitor_account_available(
        session, account_id=account_id, now=await _database_now(session)
    )
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
