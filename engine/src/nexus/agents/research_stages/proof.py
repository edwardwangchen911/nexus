"""Read-only stage proof shared by closeout, recovery and future admission consumers.

No result from this reader grants permission to accept unknown effects.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import _formal_stage_job_hash
from nexus.agents.execution import _hash, usage_cost
from nexus.agents.research_stages.protocol import (
    ProtocolRejected,
    evidence_digest,
    known_usage_lower_bound,
    stage_evidence,
    usage_known,
)
from nexus.agents.research_stages.routes import owner, selected_route
from nexus.jobs.models import JOB_PRIORITIES, JobKeyV1, JobType, canonical_job_key
from nexus.jobs.repository import job_lease_consumed
from nexus.monitoring import admission
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


@dataclass(frozen=True)
class TerminalProof:
    run_id: UUID
    known: bool
    cleanup_confirmed: bool
    orphan: bool
    evidence_hash: str
    cleanup_hash: str


async def read_terminal_proof(
    session: AsyncSession, *, run_id: UUID, now: datetime
) -> TerminalProof | None:
    """Same frozen membership for accounting, closeout, seal inspection and admission.

    A result is neither host-exit evidence nor permission to accept unknown effects.
    Late diagnostic rows cannot retroactively enlarge the terminal's proof.
    """
    invalid = "STAGED_TERMINAL_PROOF_INVALID"
    if await session.scalar(text("SELECT to_regclass('research_stage_member')")) is None:
        return None
    member = (
        (
            await session.execute(
                text(
                    "SELECT m.*,s.stopped FROM research_stage_member m JOIN research_stage_scope s "
                    "ON s.id=m.scope_id WHERE m.run_id=:run"
                ),
                {"run": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if member is None:
        if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is not None:
            if await session.scalar(
                text("SELECT 1 FROM research_stage_route WHERE run_id=:run"), {"run": run_id}
            ):
                raise ProtocolRejected(invalid)
        return None
    terminal = (
        (
            await session.execute(
                text("SELECT * FROM research_stage_terminal WHERE run_id=:run"), {"run": run_id}
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
                text("SELECT * FROM research_stage_authorization WHERE run_id=:run ORDER BY stage"),
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
    if not known:
        lower_bound = known_usage_lower_bound(rows)
        expected = (
            (lower_bound.input_tokens, lower_bound.output_tokens)
            if lower_bound.input_tokens or lower_bound.output_tokens
            else (None, None)
        )
        if (run.input_tokens, run.output_tokens) != expected:
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
            "reason": "STAGED_RESEARCH_CLOSED" if known else "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"
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
                    "FROM research_stage_cleanup c JOIN research_stage_authorization a "
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
