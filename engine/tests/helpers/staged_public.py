"""Experimental public consumers of one durable terminal proof; not production routing."""

from dataclasses import dataclass
from uuid import UUID

from helpers.staged_protocol import ProtocolRejected, evidence_digest, stage_evidence, usage_known
from helpers.staged_routes import owner, selected_route
from nexus.agents.codex_formal_stage import _formal_stage_job_hash
from nexus.agents.execution import _hash, usage_cost
from nexus.jobs.models import JOB_PRIORITIES, JobKeyV1, JobType, canonical_job_key
from nexus.jobs.repository import job_lease_consumed
from nexus.monitoring import admission
from nexus.paper import (
    history_seal,
    manual_cancel,
    manual_request,
    research_orphan,
    research_orphan_proof,
    research_request,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import select, text

_native_member = admission._require_monitor_member_available
_native_finished = admission._monitor_job_finished


@dataclass(frozen=True)
class TerminalProof:
    run_id: object
    known: bool
    cleanup_confirmed: bool
    orphan: bool
    evidence_hash: str
    cleanup_hash: str


async def read_terminal_proof(session, *, run_id, now):
    """Same frozen membership for accounting, closeout, seal inspection and admission.

    A result is neither host-exit evidence nor permission to accept unknown effects.
    Late diagnostic rows cannot retroactively enlarge the terminal's proof.
    """
    invalid = "STAGED_TERMINAL_PROOF_INVALID"
    if await session.scalar(text("SELECT to_regclass('stage_proto_member')")) is None:
        return None
    member = (
        (
            await session.execute(
                text(
                    "SELECT m.*,s.stopped FROM stage_proto_member m JOIN stage_proto_scope s "
                    "ON s.id=m.scope_id WHERE m.run_id=:run"
                ),
                {"run": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if member is None:
        if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is not None:
            if await session.scalar(
                text("SELECT 1 FROM stage_proto_route WHERE run_id=:run"), {"run": run_id}
            ):
                raise ProtocolRejected(invalid)
        return None
    terminal = (
        (
            await session.execute(
                text("SELECT * FROM stage_proto_terminal WHERE run_id=:run"), {"run": run_id}
            )
        )
        .mappings()
        .one_or_none()
    )
    if terminal is None or terminal["closed_at"] > now:
        raise ProtocolRejected(invalid)
    run = await session.get(AgentResearchRunRecord, run_id)
    jobs = tuple(
        await session.scalars(select(Job).where(Job.payload["run_id"].astext == str(run_id)))
    )
    if run is None or len(jobs) != 1 or jobs[0].id != member["job_id"]:
        raise ProtocolRejected(invalid)
    job = jobs[0]
    scope, kind = await owner(session, run, job)
    if scope != member["scope_id"]:
        raise ProtocolRejected(invalid)
    if kind == "increment":
        await admission.require_increment_binding(session, run)
    elif await selected_route(session, job.id) is None:
        raise ProtocolRejected(invalid)
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    budget = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == job.id)))
    attempts = tuple(await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)))
    if (
        model is None
        or budget is None
        or len(leases) != 1
        or len(attempts) != 1
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
        or budget.pricing_version != run.pricing_version
        or budget.reserved_usd != run.reserved_cost_usd
        or run.retry_count != 0
        or run.status not in {"completed", "failed", "abstained"}
        or job.job_type != "agent.paper.research"
        or job.queue != "general"
        or job.schema_version != "agent_research_job_v1"
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.priority != JOB_PRIORITIES[JobType.AGENT_PAPER_RESEARCH]
        or job.payload != {"run_id": str(run.id)}
        or job.input_hash != _formal_stage_job_hash(run)
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.attempt_count != 1
        or job.retry_policy != "automatic_safe"
        or job.finished_at != terminal["closed_at"]
        or job.job_key
        != canonical_job_key(
            JobKeyV1(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                scope_kind="instrument",
                scope_id=job.scope_id,
                effective_at=job.effective_at,
                input_hash=job.input_hash,
            )
        )
    ):
        raise ProtocolRejected(invalid)
    lease, attempt = leases[0], attempts[0]
    if (
        job.started_at != lease.acquired_at
        or attempt.started_at != job.started_at
        or attempt.finished_at != job.finished_at
        or attempt.worker_id != lease.worker_id
        or attempt.lease_token != lease.lease_token
        or attempt.fence_token != lease.fence_token
        or attempt.attempt_no != 1
        or not lease.acquired_at <= lease.heartbeat_at < lease.expires_at
    ):
        raise ProtocolRejected(invalid)
    orphan = job.status == "dead_letter"
    if orphan:
        if (
            not member["stopped"]
            or run.status != "failed"
            or job.last_error_code != "LEASE_EXPIRED"
            or job.result_code is not None
            or attempt.outcome != "lost"
            or attempt.error_code != "LEASE_EXPIRED"
            or lease.expires_at > now
            or job.finished_at < lease.expires_at
        ):
            raise ProtocolRejected(invalid)
    elif job.status not in {"succeeded", "failed"} or not await job_lease_consumed(
        session, job=job, lease=lease, now=now
    ):
        raise ProtocolRejected(invalid)
    auths = (
        (
            await session.execute(
                text("SELECT * FROM stage_proto_authorization WHERE run_id=:run ORDER BY stage"),
                {"run": run.id},
            )
        )
        .mappings()
        .all()
    )
    if not auths or [a["stage"] for a in auths] != list(range(1, len(auths) + 1)) or len(auths) > 3:
        raise ProtocolRejected(invalid)
    for auth in auths:
        if (
            (
                auth["job_id"],
                auth["attempt_no"],
                auth["worker_id"],
                auth["lease_token"],
                auth["fence_token"],
                auth["reserved_id"],
                auth["schema_hash"],
            )
            != (
                job.id,
                attempt.attempt_no,
                attempt.worker_id,
                attempt.lease_token,
                attempt.fence_token,
                budget.id,
                run.schema_hash,
            )
            or not lease.acquired_at
            <= auth["authorized_at"]
            < min(lease.expires_at, member["work_deadline"])
            or auth["authorized_at"] > terminal["closed_at"]
        ):
            raise ProtocolRejected(invalid)
    rows = await stage_evidence(session, run.id, terminal["evidence"])
    if (
        rows != terminal["evidence"]
        or evidence_digest(rows) != terminal["chain_hash"]
        or terminal["observed_stages"]
        != [r["stage"] for r in rows if r["response_hash"] is not None]
        or terminal["known"] is not usage_known(rows)
    ):
        raise ProtocolRejected(invalid)
    known = terminal["known"]
    if known:
        tokens = (sum(r["input_tokens"] for r in rows), sum(r["output_tokens"] for r in rows))
        if (
            (run.input_tokens, run.output_tokens) != tokens
            or budget.state != "settled"
            or run.settled_cost_usd != budget.settled_usd
            or run.settled_cost_usd != usage_cost(model, *tokens)
        ):
            raise ProtocolRejected(invalid)
    elif (
        budget.state != "reserved"
        or budget.settled_usd is not None
        or run.settled_cost_usd is not None
    ):
        raise ProtocolRejected(invalid)
    if orphan:
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run.id)
            )
        )
        events = tuple(
            await session.scalars(
                select(AgentResearchEventRecord).where(AgentResearchEventRecord.run_id == run.id)
            )
        )
        reason = {
            "reason": "STAGED_PROTOTYPE_CLOSED"
            if known
            else "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"
        }
        if len(outputs) != 1 or len(events) != 1:
            raise ProtocolRejected(invalid)
        output, event = outputs[0], events[0]
        if (
            output.sequence != 1
            or output.status != "failed"
            or output.role is not None
            or output.prompt_hash is not None
            or output.intent is not None
            or output.payload != reason
            or output.content_hash
            != _hash({"status": "failed", "role": None, "payload": reason, "intent": None})
            or event.sequence != 1
            or event.state != "failed"
            or event.details != reason
            or event.event_hash != _hash({"state": "failed", "details": reason})
            or output.created_at > now
            or event.created_at > now
        ):
            raise ProtocolRejected(invalid)
    # Financial membership stays frozen; lifecycle may gain a later, immutable
    # confirmation from the exact original authorization. As-of filtering also
    # pins historical seal readback to the facts available at that seal.
    cleanup_rows = list(
        (
            await session.execute(
                text(
                    "SELECT c.stage,c.confirmed,c.observed_at,a.authorized_at "
                    "FROM stage_proto_cleanup c JOIN stage_proto_authorization a "
                    "USING(run_id,stage) "
                    "WHERE c.run_id=:run AND c.observed_at<=:now ORDER BY c.stage"
                ),
                {"run": run.id, "now": now},
            )
        ).mappings()
    )
    if any(c["observed_at"] < c["authorized_at"] for c in cleanup_rows):
        raise ProtocolRejected(invalid)
    cleanup_facts = [
        {
            "run_id": str(run.id),
            "stage": c["stage"],
            "confirmed": c["confirmed"],
            "observed_at": c["observed_at"].isoformat(),
        }
        for c in cleanup_rows
    ]
    confirmed = len(cleanup_rows) == len(auths) and all(
        c["confirmed"] is True for c in cleanup_rows
    )
    return TerminalProof(
        run.id, known, confirmed, orphan, terminal["chain_hash"], evidence_digest(cleanup_facts)
    )


async def _member_available(session, *, member, now):
    try:
        proof = await read_terminal_proof(session, run_id=member.run_id, now=now)
    except ProtocolRejected:
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN") from None
    if proof is None:
        return await _native_member(session, member=member, now=now)
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    if (
        not proof.known
        or not proof.cleanup_confirmed
        or trigger is None
        or trigger.status not in {"completed", "failed"}
    ):
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")


async def _finished(session, **kwargs):
    try:
        proof = await read_terminal_proof(session, run_id=kwargs["run"].id, now=kwargs["now"])
    except ProtocolRejected:
        return False
    if proof is None:
        return await _native_finished(session, **kwargs)
    return proof.cleanup_confirmed


def install_public_readers(monkeypatch):
    monkeypatch.setattr(admission, "_require_monitor_member_available", _member_available)
    monkeypatch.setattr(admission, "_monitor_job_finished", _finished)
    monkeypatch.setattr(research_orphan, "_lock_research_orphan_rows", _lock_orphans)
    monkeypatch.setattr(research_orphan, "_close_research_orphan", _close_orphan)
    monkeypatch.setattr(research_orphan_proof, "require_expired_research_proof", _expired_proof)
    monkeypatch.setattr(history_seal, "_proof", _history_proof)
    monkeypatch.setattr(research_request, "require_research_jobs_settled", _research_settled)
    monkeypatch.setattr(manual_request, "_require_cycle_jobs_settled", _cycle_settled)
    monkeypatch.setattr(manual_cancel, "cancel_unused_manual_research", _cancel_unused)


# The production defaults keep their original policies. Only isolated tests install
# these readers; all common account/cycle/model/process/acceptance checks stay native.

_native_lock_orphans = research_orphan._lock_research_orphan_rows
_native_close_orphan = research_orphan._close_research_orphan
_native_expired_proof = research_orphan_proof.require_expired_research_proof
_native_history_proof = history_seal._proof


async def _lock_orphans(session, *, run_ids, now):
    if await session.scalar(text("SELECT to_regclass('stage_proto_scope')")) is not None:
        # Same lock order as authorize/observe/final: scope before Job/Lease.
        await session.execute(
            text(
                "SELECT s.id FROM stage_proto_scope s JOIN stage_proto_member m ON m.scope_id=s.id "
                "WHERE m.run_id=ANY(:runs) ORDER BY s.id FOR UPDATE OF s"
            ),
            {"runs": list(run_ids)},
        )
    return await _native_lock_orphans(session, run_ids=run_ids, now=now)


async def _close_orphan(session, **binding):
    from types import SimpleNamespace

    from helpers.staged_protocol import StagedProtocol

    job, lease = binding["job"], binding["lease"]
    run_id = UUID(job.payload["run_id"])
    if await session.scalar(text("SELECT to_regclass('stage_proto_member')")) is None:
        return await _native_close_orphan(session, **binding)
    member = await session.scalar(
        text("SELECT 1 FROM stage_proto_member WHERE run_id=:run"), {"run": run_id}
    )
    if member is None:
        if await session.scalar(
            text("SELECT to_regclass('stage_proto_route')")
        ) is not None and await session.scalar(
            text("SELECT 1 FROM stage_proto_route WHERE run_id=:run"), {"run": run_id}
        ):
            raise ProtocolRejected("STAGED_RECOVERY_EVIDENCE_REQUIRED")
        return await _native_close_orphan(session, **binding)
    run, attempt = await research_orphan._require_research_orphan_binding(session, **binding)
    route = await selected_route(session, job.id)
    scope, kind = await owner(session, run, job)
    if kind != "increment" and route is None:
        raise ProtocolRejected("STAGED_RECOVERY_EVIDENCE_REQUIRED")
    stored_scope = await session.scalar(
        text("SELECT scope_id FROM stage_proto_member WHERE run_id=:run"), {"run": run.id}
    )
    if scope != stored_scope:
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    claim = SimpleNamespace(
        job_id=job.id,
        attempt_no=attempt.attempt_no,
        worker_id=lease.worker_id,
        lease_token=lease.lease_token,
        fence_token=lease.fence_token,
    )
    await StagedProtocol(None).close_in_transaction(session, claim, now=binding["now"], orphan=True)


async def _expired_proof(session, **kwargs):
    # Preserve all original expired-attempt/unknown/native-output checks.
    await _native_expired_proof(session, **kwargs)
    proof = await read_terminal_proof(session, run_id=kwargs["run"].id, now=kwargs["now"])
    if proof is not None and (proof.known or not proof.cleanup_confirmed or not proof.orphan):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


async def _history_proof(session, **kwargs):
    from nexus.jobs.models import job_input_hash

    native = await _native_history_proof(session, **kwargs)
    proof = await read_terminal_proof(session, run_id=kwargs["run_id"], now=kwargs["now"])
    if proof is None:
        return native
    if proof.known or not proof.cleanup_confirmed:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    # Write and historical readback use the same versioned frozen proof identity.
    return {
        **native,
        "staged_protocol": "staged-experiment-v1",
        "stage_chain_hash": proof.evidence_hash,
        "cleanup_hash": proof.cleanup_hash,
        "fact_hash": job_input_hash(
            {
                "native_fact_hash": native["fact_hash"],
                "stage_chain_hash": proof.evidence_hash,
                "cleanup_hash": proof.cleanup_hash,
                "protocol": "staged-experiment-v1",
                "run_id": str(proof.run_id),
            }
        ),
    }


_native_research_settled = research_request.require_research_jobs_settled
_native_cancel_unused = manual_cancel.cancel_unused_manual_research


async def _research_settled(session, *, row, view, now, allow_unknown_usage=False):
    await _native_research_settled(
        session, row=row, view=view, now=now, allow_unknown_usage=allow_unknown_usage
    )
    if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is None:
        return
    for member in view.members:
        await _selected_member_settled(
            session,
            owner_id=row.id,
            kind="independent",
            member=member,
            now=now,
            allow_unknown_usage=allow_unknown_usage,
        )


async def _selected_member_settled(session, *, owner_id, kind, member, now, allow_unknown_usage):
    from nexus.paper.research_failure import require_unused_research_member_closed

    route = await selected_route(session, member.job_id)
    if route is None:
        return
    if route["kind"] != kind or route["scope_id"] != owner_id:
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")
    staged = await session.scalar(
        text("SELECT 1 FROM stage_proto_member WHERE run_id=:run"), {"run": member.run_id}
    )
    if staged is None:
        # Original native no-call proof; a missing stage row is never enough.
        job = await session.get(Job, member.job_id)
        if job.result_code == "AGENT_ABSTAINED":
            from nexus.paper.research_failure import require_claimed_uncalled_research_closed

            if not await session.scalar(
                text("SELECT stopped FROM stage_proto_scope WHERE id=:scope"),
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
        or (not allow_unknown_usage and not proof.known)
    ):
        raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")


_native_cycle_settled = manual_request._require_cycle_jobs_settled


async def _cycle_settled(session, **kwargs):
    # Native policy first: any unknown still needs its existing exact acceptance.
    await _native_cycle_settled(session, **kwargs)
    if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is None:
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


async def _cancel_unused(session, **kwargs):
    if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is None:
        return await _native_cancel_unused(session, **kwargs)
    old = await session.scalar(text("SELECT current_setting('nexus.stage_protocol',true)"))

    async def restore():
        await session.execute(
            text("SELECT set_config('nexus.stage_protocol',:value,true)"), {"value": old or ""}
        )

    await session.execute(text("SET LOCAL nexus.stage_protocol='staged-experiment-v1'"))
    try:
        result = await _native_cancel_unused(session, **kwargs)
    except BaseException as error:
        try:
            await restore()
        except BaseException:
            error.add_note("PROTOCOL_MARKER_RESTORE_UNCONFIRMED")
        raise
    await restore()
    return result
