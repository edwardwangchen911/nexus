"""Public consumers share the formal terminal proof and retain native eligibility policy."""

from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.agents.research_stages.routes import owner, selected_route
from nexus.monitoring import admission
from nexus.paper import (
    history_seal,
    manual_cancel,
    manual_request,
    research_orphan,
    research_orphan_proof,
    research_request,
)
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import Job


async def _zero_call_increment(session: Any, *, run_id: Any, now: Any) -> bool:
    from nexus.monitoring.preclaim import preclaim_rejection_qualified
    from nexus.persistence.models.agents import AgentResearchRunRecord

    if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is None:
        return False
    run = await session.get(AgentResearchRunRecord, run_id)
    if run is None or not await preclaim_rejection_qualified(session, run=run, now=now):
        return False
    row = (
        (
            await session.execute(
                text(
                    "SELECT r.scope_id,s.stopped FROM research_stage_route r "
                    "JOIN research_stage_scope s ON s.id=r.scope_id "
                    "WHERE r.run_id=:run AND r.kind='increment'"
                ),
                {"run": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    return bool(
        row is not None
        and row["stopped"]
        and not await session.scalar(
            text("SELECT 1 FROM research_stage_member WHERE run_id=:run"), {"run": run_id}
        )
    )


async def _member_available(session: Any, *, member: Any, now: Any) -> Any:
    if await _zero_call_increment(session, run_id=member.run_id, now=now):
        return await admission._legacy_require_monitor_member_available(
            session, member=member, now=now
        )
    try:
        proof = await read_terminal_proof(session, run_id=member.run_id, now=now)
    except ProtocolRejected:
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN") from None
    if proof is None:
        return await admission._legacy_require_monitor_member_available(
            session, member=member, now=now
        )
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    if (
        not proof.known
        or not proof.cleanup_confirmed
        or trigger is None
        or (trigger.status not in {"completed", "failed"})
    ):
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")


async def _finished(session: Any, **kwargs: Any) -> Any:
    if await _zero_call_increment(session, run_id=kwargs["run"].id, now=kwargs["now"]):
        return True
    try:
        proof = await read_terminal_proof(session, run_id=kwargs["run"].id, now=kwargs["now"])
    except ProtocolRejected:
        return False
    if proof is None:
        return await admission._legacy_monitor_job_finished(session, **kwargs)
    return proof.cleanup_confirmed


async def _lock_orphans(session: Any, *, run_ids: Any, now: Any) -> Any:
    if await session.scalar(text("SELECT to_regclass('research_stage_scope')")) is not None:
        await session.execute(
            text(
                "SELECT s.id FROM research_stage_scope s JOIN research_stage_member m ON "
                "m.scope_id=s.id WHERE m.run_id=ANY(:runs) ORDER BY s.id FOR UPDATE OF s"
            ),
            {"runs": list(run_ids)},
        )
    return await research_orphan._legacy_lock_research_orphan_rows(
        session, run_ids=run_ids, now=now
    )


async def _close_orphan(session: Any, **binding: Any) -> Any:
    from nexus.agents.research_stages.protocol import StagedProtocol
    from nexus.jobs.repository import ClaimedJob

    job, lease = (binding["job"], binding["lease"])
    run_id = UUID(job.payload["run_id"])
    if await session.scalar(text("SELECT to_regclass('research_stage_member')")) is None:
        return await research_orphan._legacy_close_research_orphan(session, **binding)
    member = await session.scalar(
        text("SELECT 1 FROM research_stage_member WHERE run_id=:run"), {"run": run_id}
    )
    if member is None:
        if await session.scalar(
            text("SELECT to_regclass('research_stage_route')")
        ) is not None and await session.scalar(
            text("SELECT 1 FROM research_stage_route WHERE run_id=:run"), {"run": run_id}
        ):
            raise ProtocolRejected("STAGED_RECOVERY_EVIDENCE_REQUIRED")
        return await research_orphan._legacy_close_research_orphan(session, **binding)
    run, attempt = await research_orphan._require_research_orphan_binding(session, **binding)
    route = await selected_route(session, job.id)
    scope, kind = await owner(session, run, job)
    if kind != "increment" and route is None:
        raise ProtocolRejected("STAGED_RECOVERY_EVIDENCE_REQUIRED")
    stored_scope = await session.scalar(
        text("SELECT scope_id FROM research_stage_member WHERE run_id=:run"), {"run": run.id}
    )
    if scope != stored_scope:
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    claim = ClaimedJob(
        job_id=job.id,
        job_type=job.job_type,
        schema_version=job.schema_version,
        payload=job.payload,
        queue=job.queue,
        priority=job.priority,
        retry_policy=job.retry_policy,
        max_attempts=job.max_attempts,
        effective_at=job.effective_at,
        expires_at=lease.expires_at,
        version=job.version,
        dependencies_satisfied=False,
        dependency_result_code=None,
        attempt_no=attempt.attempt_no,
        worker_id=lease.worker_id,
        lease_token=lease.lease_token,
        fence_token=lease.fence_token,
    )
    await StagedProtocol(async_sessionmaker(bind=session.bind)).close_in_transaction(
        session, claim, now=binding["now"], orphan=True
    )


async def _expired_proof(session: Any, **kwargs: Any) -> Any:
    proof = await read_terminal_proof(session, run_id=kwargs["run"].id, now=kwargs["now"])
    if proof is None:
        return await research_orphan_proof._legacy_require_expired_research_proof(session, **kwargs)
    if proof.known or not proof.cleanup_confirmed or not proof.orphan:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.runtime import JobAttempt, JobLease

    # The reader establishes the persisted proof, while this interface also
    # validates the caller's exact component set, as its legacy contract did.
    persisted_run = await session.get(AgentResearchRunRecord, proof.run_id)
    for name, model, predicate in (
        (
            "reservation",
            AgentDailyBudgetReservationRecord,
            AgentDailyBudgetReservationRecord.run_id == proof.run_id,
        ),
        ("job", Job, Job.payload["run_id"].astext == str(proof.run_id)),
        ("attempt", JobAttempt, JobAttempt.job_id == kwargs["job"].id),
        ("lease", JobLease, JobLease.job_id == kwargs["job"].id),
    ):
        values = tuple(await session.scalars(select(model).where(predicate)))
        if len(values) != 1 or any(
            getattr(kwargs[name], column.key) != getattr(values[0], column.key)
            for column in model.__table__.columns
        ):
            raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    await research_orphan_proof._legacy_require_expired_research_proof(
        session,
        **kwargs,
        _verified_stage_usage=(persisted_run.input_tokens, persisted_run.output_tokens),
    )


async def _history_proof(session: Any, **kwargs: Any) -> Any:
    from nexus.jobs.models import job_input_hash

    native = await history_seal._legacy_proof(session, **kwargs)
    proof = await read_terminal_proof(session, run_id=kwargs["run_id"], now=kwargs["now"])
    if proof is None:
        return native
    if proof.known or not proof.cleanup_confirmed:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    return {
        **native,
        "staged_protocol": "research-stages-v1",
        "stage_chain_hash": proof.evidence_hash,
        "cleanup_hash": proof.cleanup_hash,
        "fact_hash": job_input_hash(
            {
                "native_fact_hash": native["fact_hash"],
                "stage_chain_hash": proof.evidence_hash,
                "cleanup_hash": proof.cleanup_hash,
                "protocol": "research-stages-v1",
                "run_id": str(proof.run_id),
            }
        ),
    }


async def _research_settled(
    session: Any, *, row: Any, view: Any, now: Any, allow_unknown_usage: Any = False
) -> Any:
    historical = []
    for member in view.members:
        if await selected_route(session, member.job_id) is None:
            historical.append(member)
            continue
        # The formal proof validates the consumed fence and cleanup separately
        # from usage. A retained, unexpired lease must not prevent recording an
        # unknown outcome; it still cannot grant next-round admission.
        await _selected_member_settled(
            session,
            owner_id=row.id,
            kind="independent",
            member=member,
            now=now,
            allow_unknown_usage=allow_unknown_usage,
        )
    if historical:
        await research_request._legacy_require_research_jobs_settled(
            session,
            row=row,
            view=view.model_copy(update={"members": historical}),
            now=now,
            allow_unknown_usage=allow_unknown_usage,
        )


async def _selected_member_settled(
    session: Any, *, owner_id: Any, kind: Any, member: Any, now: Any, allow_unknown_usage: Any
) -> Any:
    from nexus.paper.research_failure import require_unused_research_member_closed

    route = await selected_route(session, member.job_id)
    if route is None:
        return
    if route["kind"] != kind or route["scope_id"] != owner_id:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    staged = await session.scalar(
        text("SELECT 1 FROM research_stage_member WHERE run_id=:run"), {"run": member.run_id}
    )
    if staged is None:
        job = await session.get(Job, member.job_id)
        if job.result_code == "AGENT_ABSTAINED":
            from nexus.paper.research_failure import require_claimed_uncalled_research_closed

            if not await session.scalar(
                text("SELECT stopped FROM research_stage_scope WHERE id=:scope"),
                {"scope": route["scope_id"]},
            ):
                raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
            await require_claimed_uncalled_research_closed(
                session, run_id=member.run_id, job_id=member.job_id, now=now
            )
            return
        await require_unused_research_member_closed(
            session, owner_id=owner_id, member=member, now=now
        )
        return
    try:
        proof = await read_terminal_proof(session, run_id=member.run_id, now=now)
    except ProtocolRejected:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN") from None
    if (
        proof is None
        or not proof.cleanup_confirmed
        or (not allow_unknown_usage and (not proof.known))
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def _cycle_settled(session: Any, **kwargs: Any) -> Any:
    await manual_request._legacy_require_cycle_jobs_settled(session, **kwargs)
    if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is None:
        return
    from types import SimpleNamespace

    from nexus.persistence.models.paper import AgentPaperCycleEventRecord

    events = await session.scalars(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == kwargs["cycle_id"]
        )
    )
    run_ids = {
        UUID(str(run)) for event in events for run in event.fact_ids.get("research_run_ids", [])
    }
    for run_id in run_ids:
        job = (
            await session.scalars(select(Job).where(Job.payload["run_id"].astext == str(run_id)))
        ).one()
        await _selected_member_settled(
            session,
            owner_id=kwargs["cycle_id"],
            kind="formal",
            member=SimpleNamespace(run_id=run_id, job_id=job.id),
            now=kwargs["now"],
            allow_unknown_usage=True,
        )


async def _cancel_unused(session: Any, **kwargs: Any) -> Any:
    if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is None:
        return await manual_cancel._legacy_cancel_unused_manual_research(session, **kwargs)
    old = await session.scalar(text("SELECT current_setting('nexus.stage_protocol',true)"))

    async def restore() -> Any:
        await session.execute(
            text("SELECT set_config('nexus.stage_protocol',:value,true)"), {"value": old or ""}
        )

    await session.execute(text("SET LOCAL nexus.stage_protocol='research-stages-v1'"))
    try:
        result = await manual_cancel._legacy_cancel_unused_manual_research(session, **kwargs)
    except BaseException as error:
        try:
            await restore()
        except BaseException:
            error.add_note("PROTOCOL_MARKER_RESTORE_UNCONFIRMED")
        raise
    await restore()
    return result
