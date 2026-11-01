"""One historical no-dispatch boundary; never a model terminal or fee settlement.

The audited revision authorizes and commits before constructing the HTTP client
or starting provider tasks. Authorization rows are immutable. Its premature
capture bug could nevertheless publish an empty, invalid stage terminal. Only
that revision and its complete native exit shape are recognized here.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_manifest import PLATFORM_PURPOSE, parse_comparison_manifest
from nexus.agents.comparison_routes import comparison_owner
from nexus.agents.execution import _hash
from nexus.agents.platform_manifest import PlatformManifest
from nexus.agents.research_stages.protocol import evidence_digest
from nexus.agents.research_stages.routes import VERSION
from nexus.jobs.repository import job_lease_consumed
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease

# A fixed compatibility boundary, not a caller-selected current-code guarantee.
# See the causal proof in docs/runbooks/platform-qualification.md.
HISTORICAL_IMPLEMENTATION = "28e5ff2fb4770ea415a599c0fa3d3763f1a899bf"


async def historical_platform_preparation_isolated(
    session: AsyncSession, *, run_id: UUID, job_id: UUID, now: datetime
) -> bool:
    """Read under the caller's G/day lock; acquire no other experiment scope lock.

    True means no provider resource lifecycle was entered and the original
    identity cannot send again. Cleanup is inapplicable, not confirmed. Public
    terminal inspection and all financial consumers retain their original rules.
    """
    row = (
        (
            await session.execute(
                text("""SELECT c.experiment_id,c.ordinal,m.purpose,m.payload,m.manifest_hash,
              s.stopped,s.comparison_id,s.cycle_id,s.increment_id,s.request_id,
              r.scope_id AS route_scope,r.kind,r.version,
              member.scope_id AS member_scope,member.job_id AS member_job,
              member.work_deadline,member.cleanup_deadline,
              t.known,t.closed_at,t.evidence,t.chain_hash,t.observed_stages,stop.stopped_at
              FROM research_comparison_chain c
              JOIN research_comparison_manifest m ON m.id=c.experiment_id
              JOIN research_stage_route r ON r.run_id=c.run_id AND r.job_id=c.job_id
              JOIN research_stage_scope s ON s.id=c.experiment_id
              JOIN research_stage_member member ON member.run_id=c.run_id
              JOIN research_stage_terminal t ON t.run_id=c.run_id
              JOIN research_comparison_chain_stop stop
                ON stop.run_id=c.run_id AND stop.scope_id=c.experiment_id
              WHERE c.run_id=:run AND c.job_id=:job"""),
                {"run": run_id, "job": job_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["purpose"] != PLATFORM_PURPOSE:
        return False
    try:
        manifest = parse_comparison_manifest(row["payload"])
    except ValueError:
        return False
    if (
        type(manifest) is not PlatformManifest
        or manifest.implementation_sha != HISTORICAL_IMPLEMENTATION
        or manifest.digest != row["manifest_hash"]
        or manifest.experiment_id != row["experiment_id"]
        or row["ordinal"] != 1
        or not row["stopped"]
        or row["comparison_id"] != manifest.experiment_id
        or any(row[k] is not None for k in ("cycle_id", "increment_id", "request_id"))
        or row["route_scope"] != manifest.experiment_id
        or row["member_scope"] != manifest.experiment_id
        or row["member_job"] != job_id
        or (row["kind"], row["version"]) != ("comparison", VERSION)
        or row["known"] is not False
        or row["evidence"] != []
        or row["observed_stages"] != []
        or row["chain_hash"] != evidence_digest([])
    ):
        return False
    if (
        await session.scalar(
            text("SELECT count(*) FROM research_comparison_chain WHERE experiment_id=:id"),
            {"id": manifest.experiment_id},
        )
        != 1
    ):
        return False
    # In the audited entry, even client construction is after successful
    # authorization COMMIT. None of these immutable resources may exist.
    for table in (
        "research_stage_authorization",
        "research_stage_observation",
        "research_stage_verification",
        "research_stage_cleanup",
    ):
        if await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE run_id=:run)"), {"run": run_id}
        ):
            return False
    run = await session.get(AgentResearchRunRecord, run_id)
    job = await session.get(Job, job_id)
    if run is None or job is None:
        return False
    try:
        if await comparison_owner(session, run, job) != manifest.experiment_id:
            return False
    except ValueError:
        return False
    budgets = tuple(
        await session.scalars(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
    )
    attempts = tuple(await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id)))
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == job_id)))
    events = tuple(
        await session.scalars(
            select(AgentResearchEventRecord)
            .where(AgentResearchEventRecord.run_id == run_id)
            .order_by(AgentResearchEventRecord.sequence)
        )
    )
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
    )
    jobs = tuple(
        await session.scalars(select(Job.id).where(Job.payload["run_id"].astext == str(run_id)))
    )
    if (
        jobs != (job_id,)
        or len(budgets) != 1
        or len(attempts) != 1
        or len(leases) != 1
        or len(events) != 2
        or len(outputs) != 1
        or run.status != "abstained"
        or job.status != "skipped"
        or job.result_code != "AGENT_ABSTAINED"
        or job.attempt_count != 1
        or job.started_at is None
        or job.finished_at != row["closed_at"]
        or job.started_at != leases[0].acquired_at
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
        or not job.started_at <= row["stopped_at"] <= job.finished_at <= now
        or not job.started_at
        < row["work_deadline"]
        <= row["cleanup_deadline"]
        <= leases[0].expires_at
        # These fields must retain the exact historical defect shape. They are
        # not evidence of zero cost; the no-dispatch causal proof above is.
        or (run.input_tokens, run.output_tokens, run.settled_cost_usd) != (0, 0, 0)
        or budgets[0].state != "released"
        or budgets[0].settled_usd is not None
    ):
        return False
    reason = {"reason": "evidence_authority_missing"}
    for event, sequence, state, details in (
        (events[0], 1, "prepared", {}),
        (events[1], 2, "abstained", reason),
    ):
        if (
            (event.sequence, event.state, event.details) != (sequence, state, details)
            or event.event_hash != _hash({"state": state, "details": details})
            or not job.started_at <= event.created_at <= job.finished_at
        ):
            return False
    output = outputs[0]
    return (
        output.sequence == 1
        and output.status == "abstained"
        and output.role is None
        and output.prompt_hash is None
        and output.intent is None
        and output.payload == reason
        and job.started_at <= output.created_at <= job.finished_at
        and output.content_hash
        == _hash({"status": "abstained", "role": None, "payload": reason, "intent": None})
    )
