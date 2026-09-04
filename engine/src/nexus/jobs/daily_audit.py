"""Observe selected manual facts, then seal their existing global audit chain."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.audit.manifest import AuditManifestObjectStore, publish_audit_chain_manifest
from nexus.audit.service import append_audit_event
from nexus.jobs.daily_manual import _root, manual_daily_job_ids
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.jobs.daily_research import resolve_daily_research_job
from nexus.jobs.models import JobType, job_input_hash
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import AuditChainManifest, AuditEvent, Job

_ACTION = "audit.manual_daily_observed"
_SCHEMA = "manual_daily_audit_v1"


class DailyAuditError(ValueError):
    """Fixed boundary code only, without private source details."""


async def _transaction(session: AsyncSession) -> None:
    # READ COMMITTED refreshes the chain head after waiting on the global lock.
    # A REPEATABLE READ snapshot established before that wait would be stale.
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise DailyAuditError("DAILY_AUDIT_ISOLATION_UNAVAILABLE")
    await session.execute(text("SET LOCAL lock_timeout = '5s'"))
    await assert_daily_pipeline_schema_ready(session)


async def _read_job(session: AsyncSession, job_id: UUID) -> Job:
    job = await session.scalar(
        select(Job).where(Job.id == job_id).execution_options(populate_existing=True)
    )
    if job is None:
        raise DailyAuditError("DAILY_AUDIT_BINDING_INVALID")
    return job


def _job_fact(job: Job) -> dict[str, object]:
    return {
        "job_id": str(job.id),
        "stage": job.job_type,
        "status": job.status,
        "version": job.version,
        "attempt_count": job.attempt_count,
        "input_hash": job.input_hash,
        "schema_version": job.schema_version,
        "finished_at": None if job.finished_at is None else job.finished_at.isoformat(),
        "result_code": job.result_code,
        "error_code": job.last_error_code,
    }


async def _facts(session: AsyncSession, root_id: UUID, run_id: UUID | None) -> dict[str, object]:
    # Only read from one MVCC snapshot. Never block a worker's long-lived Job/Run locks.
    try:
        _, root_payload = await _root(session, root_id)
        ids = await manual_daily_job_ids(session, root_id=root_id)
        jobs = [_job_fact(await _read_job(session, job_id)) for job_id in ids]
        research: dict[str, object] | None = None
        if run_id is not None:
            candidates = tuple(
                await session.scalars(
                    select(Job.id)
                    .where(
                        Job.job_type == JobType.AGENT_RESEARCH.value,
                        Job.payload["run_id"].as_string() == str(run_id),
                    )
                    .limit(2)
                )
            )
            if len(candidates) != 1:
                raise ValueError("invalid binding")
            run = await session.scalar(
                select(AgentResearchRunRecord)
                .where(AgentResearchRunRecord.id == run_id)
                .execution_options(populate_existing=True)
            )
            if run is None:
                raise ValueError("invalid binding")
            job = await resolve_daily_research_job(session, root_id=root_id, run_id=run_id)
            latest = await session.scalar(
                select(AgentResearchOutputRecord)
                .where(
                    AgentResearchOutputRecord.run_id == run_id,
                )
                .order_by(AgentResearchOutputRecord.sequence.desc())
                .limit(1)
            )
            research = {
                "run_id": str(run.id),
                "status": run.status,
                "job": _job_fact(job),
                "input_hash": run.input_hash,
                "data_snapshot_id": str(run.data_snapshot_id),
                "evidence_hash": run.evidence_hash,
                "model_hash": run.model_hash,
                "prompt_hash": run.prompt_hash,
                "schema_hash": run.schema_hash,
                "tool_hash": run.tool_hash,
                "decision_time": run.decision_time.isoformat(),
                "reported_input_tokens": run.input_tokens,
                "reported_output_tokens": run.output_tokens,
                "latest_output": None
                if latest is None
                else {
                    "output_id": str(latest.id),
                    "sequence": latest.sequence,
                    "content_hash": latest.content_hash,
                    "status": latest.status,
                    "created_at": latest.created_at.isoformat(),
                    "reason": latest.payload.get("reason"),
                },
            }
        return {
            "root_id": str(root_id),
            "cutoff_at": root_payload.cutoff_at.isoformat(),
            "snapshot_id": str(root_payload.snapshot_id),
            "jobs": jobs,
            "research": research,
        }
    except ValueError as error:
        raise DailyAuditError("DAILY_AUDIT_BINDING_INVALID") from error


async def _observe(
    session: AsyncSession,
    root_id: UUID,
    run_id: UUID | None,
    facts: dict[str, object],
    observed_at: datetime,
    clock: Callable[[], datetime],
) -> AuditEvent:
    await _transaction(session)
    scope = f"manual-daily-audit:{root_id}:{run_id}"
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": scope},
    )
    digest = job_input_hash(facts)
    existing = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == _ACTION,
            AuditEvent.schema_version == _SCHEMA,
            AuditEvent.payload["facts_hash"].as_string() == digest,
        )
    )
    if existing is not None:
        return existing
    return await append_audit_event(
        session,
        actor_id="system:manual-daily-audit",
        action=_ACTION,
        schema_version=_SCHEMA,
        payload={
            "status": "recorded",
            "facts_hash": digest,
            "facts": facts,
            "observed_at": observed_at.isoformat(),
        },
        occurred_at=clock(),
    )


async def _seal(
    session: AsyncSession,
    event_seq: int,
    object_store: AuditManifestObjectStore,
    clock: Callable[[], datetime],
) -> tuple[str, str | None]:
    await _transaction(session)
    # This transaction does not acquire any business row locks.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('nexus:audit-chain', 2020))")
    )
    covering = await session.scalar(
        select(AuditChainManifest).where(
            AuditChainManifest.first_event_seq <= event_seq,
            AuditChainManifest.last_event_seq >= event_seq,
        )
    )
    if covering is not None:
        return "sealed", str(covering.id)
    latest = await session.scalar(
        select(AuditChainManifest)
        .order_by(
            AuditChainManifest.last_event_seq.desc(),
        )
        .limit(1)
    )
    today = clock().astimezone(UTC).date()
    if latest is not None and latest.session >= today:
        return "pending", None
    manifest = await publish_audit_chain_manifest(
        session,
        session_date=today,
        object_store=object_store,
    )
    if not manifest.first_event_seq <= event_seq <= manifest.last_event_seq:
        raise RuntimeError("audit coverage unavailable")
    return "sealed", str(manifest.id)


async def audit_manual_daily(
    factory: async_sessionmaker[AsyncSession],
    *,
    root_id: UUID,
    object_store: AuditManifestObjectStore,
    run_id: UUID | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Commit the selected observation before attempting any object-store write."""
    result: dict[str, object] = {
        "status": "unknown",
        "manifest_status": "unknown",
        "execution_locked": True,
        "scope": {
            "root_and_diagnostics": True,
            "research": "selected" if run_id is not None else "not_selected",
            "excluded": ["proposals", "paper", "performance"],
        },
    }
    try:
        async with factory() as session, session.begin():
            # First SQL: establish a read-only snapshot, then discard it before writing
            # the global chain. Concurrent business changes remain later observations.
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            facts = await _facts(session, root_id, run_id)
            observed_at = clock()
        async with factory() as session, session.begin():
            event = await _observe(session, root_id, run_id, facts, observed_at, clock)
            identity = {
                "event_id": str(event.id),
                "event_seq": event.event_seq,
                "facts_hash": event.payload["facts_hash"],
                "observed_at": event.payload["observed_at"],
                "recorded_at": event.occurred_at.isoformat(),
            }
    except DailyAuditError:
        raise
    except Exception:
        return result
    result.update(identity)
    result["status"] = "recorded"
    try:
        async with factory() as session, session.begin():
            status, manifest_id = await _seal(session, event.event_seq, object_store, clock)
        result.update(manifest_status=status, manifest_id=manifest_id)
    except Exception:
        # The observation is durable; upload/commit acknowledgement is not rollback evidence.
        pass
    return result
