"""Close only the sole, never-claimed first comparison position; no stage proof."""

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_compiler import validate_comparison_capacity
from nexus.agents.comparison_inputs import PreparedComparisonInput
from nexus.agents.comparison_manifest import ACTOR, ComparisonManifest
from nexus.agents.comparison_preparation import reconstruct_archive
from nexus.agents.persistence import release_daily_budget
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.models import job_input_hash
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job

ACTION = "research.comparison.preclaim_closed"
SCHEMA = "comparison_preclaim_close_v1"
REASON = "COMPARISON_PRECLAIM_CAPACITY_REJECTED"
BLOCKED = "COMPARISON_PRECLAIM_NOT_ELIGIBLE"


def capacity_refusal(
    source: bytes, prepared: dict[str, Any], manifest: ComparisonManifest
) -> dict[str, object]:
    rebuilt = reconstruct_archive(
        source,
        expected_source_hash=manifest.source_manifest_hash,
        experiment_id=manifest.experiment_id,
    )
    if (
        job_input_hash({k: v for k, v in prepared.items() if k != "rebuilt_at"})
        != job_input_hash(rebuilt)
        or tuple(rebuilt["input_hashes"]) != manifest.input_hashes
        or tuple(rebuilt["instrument_ids"]) != tuple(map(str, manifest.instrument_ids))
        or rebuilt["identity_equivalence_hash"] != manifest.identity_equivalence_hash
        or rebuilt["outbound_safety_hash"] != manifest.outbound_safety_hash
    ):
        raise ValueError("COMPARISON_PREPARATION_CHANGED")
    rejected = []
    for row in rebuilt["members"]:
        try:
            validate_comparison_capacity(
                PreparedComparisonInput(row["payload"], row["proof"], tuple(row["forbidden"]))
            )
        except ValueError as error:
            if str(error) not in {
                "CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
                "CODEX_CAPACITY_POOL_INPUT_EXCEEDED",
            }:
                raise
            rejected.append({"symbol": row["symbol"], "reason": str(error)})
    if not rejected:
        raise ValueError("COMPARISON_CAPACITY_REFUSAL_REQUIRED")
    return {
        "source_manifest_hash": manifest.source_manifest_hash,
        "input_hashes": list(manifest.input_hashes),
        "rejected": rejected,
    }


async def preclaim_close(
    session: AsyncSession,
    *,
    manifest: ComparisonManifest,
    refusal: dict[str, object],
    close: bool,
) -> dict[str, object]:
    """Caller owns commit. Inspection grants no authority; unknown commit is read-only recovery."""
    if (
        await session.scalar(
            text("SELECT to_regprocedure('research_comparison_preclaim_bound(uuid,timestamptz)')")
        )
        is None
    ):
        raise ValueError("COMPARISON_PRECLAIM_SCHEMA_REQUIRED")
    if close and await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("COMPARISON_TRANSACTION_INVALID")
    suffix = " FOR UPDATE" if close else ""
    stored = (
        (
            await session.execute(
                text(
                    "SELECT manifest_hash,payload FROM research_comparison_manifest WHERE id=:id"
                    + suffix
                ),
                {"id": manifest.experiment_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        stored is None
        or stored["manifest_hash"] != manifest.digest
        or stored["payload"] != manifest.model_dump(mode="json")
    ):
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    scope = (
        (
            await session.execute(
                text(
                    "SELECT stopped,comparison_id FROM research_stage_scope WHERE id=:id" + suffix
                ),
                {"id": manifest.experiment_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    rows = (
        (
            await session.execute(
                text(
                    "SELECT * FROM research_comparison_chain "
                    "WHERE experiment_id=:id ORDER BY ordinal"
                ),
                {"id": manifest.experiment_id},
            )
        )
        .mappings()
        .all()
    )
    if (
        scope is None
        or scope["comparison_id"] != manifest.experiment_id
        or len(rows) != 1
        or rows[0]["ordinal"] != 1
    ):
        raise ValueError(BLOCKED)
    row = rows[0]
    job = await session.get(Job, row["job_id"], with_for_update=close, populate_existing=True)
    run = await session.get(
        AgentResearchRunRecord, row["run_id"], with_for_update=close, populate_existing=True
    )
    budget = await session.get(
        AgentDailyBudgetReservationRecord,
        row["reserved_id"],
        with_for_update=close,
        populate_existing=True,
    )
    if job is None or run is None or budget is None:
        raise ValueError(BLOCKED)
    if not await session.scalar(
        text("""SELECT research_comparison_binding(:run,:job,:scope)
      AND EXISTS(SELECT 1 FROM research_stage_route r WHERE r.run_id=:run AND r.job_id=:job
      AND r.scope_id=:scope AND r.kind='comparison' AND r.version='research-stages-v1')"""),
        {"run": run.id, "job": job.id, "scope": manifest.experiment_id},
    ):
        raise ValueError(BLOCKED)
    if (
        job.attempt_count != 0
        or job.input_hash != job_input_hash(job.payload)
        or job.manual_retry_count != 0
        or job.started_at is not None
        or any(
            v is not None
            for v in (run.input_tokens, run.output_tokens, run.settled_cost_usd, budget.settled_usd)
        )
    ):
        raise ValueError(BLOCKED)
    for table in ("job_attempts", "job_leases", "job_dependencies"):
        if await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE job_id=:id)"), {"id": job.id}
        ):
            raise ValueError(BLOCKED)
    for table in (
        "research_stage_member",
        "research_stage_authorization",
        "research_stage_observation",
        "research_stage_verification",
        "research_stage_cleanup",
        "research_stage_terminal",
        "research_comparison_chain_stop",
        "agent_research_outputs",
        "agent_research_events",
    ):
        if await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE run_id=:id)"), {"id": run.id}
        ):
            raise ValueError(BLOCKED)
    audit = (
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["experiment_id"].astext == str(manifest.experiment_id),
            )
        )
    ).one_or_none()
    now = await session.scalar(text("SELECT clock_timestamp()"))
    proof: dict[str, object] = {
        "experiment_id": str(manifest.experiment_id),
        "manifest_hash": manifest.digest,
        "implementation_sha": manifest.implementation_sha,
        "sdk_config_hash": manifest.sdk_config_hash,
        "api_config_hash": manifest.api_config_hash,
        "position": 1,
        "run_id": str(run.id),
        "job_id": str(job.id),
        "reservation_id": str(budget.id),
        "business_date": budget.business_date.isoformat(),
        "reserved_usd": str(budget.reserved_usd),
        "reason": REASON,
        "capacity_refusal": refusal,
        "capacity_refusal_hash": job_input_hash(refusal),
        "job_version_before": job.version - 1 if audit else job.version,
        "closed_at": (audit.occurred_at if audit else now).isoformat(),
        "disposition": "release_unspent_only",
        "authority": "native_unclaimed_permanently_stopped",
    }
    proof["proof_hash"] = job_input_hash(proof)
    if audit is not None:
        canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=audit.event_seq,
                occurred_at=audit.occurred_at,
                actor_id=ACTOR,
                action=ACTION,
                schema_version=SCHEMA,
                payload=proof,
            )
        )
        previous = await session.scalar(
            select(AuditEvent.event_hash).where(AuditEvent.event_seq == audit.event_seq - 1)
        )
        if (
            audit.actor_id != ACTOR
            or audit.schema_version != SCHEMA
            or audit.payload != proof
            or audit.canonical_event != canonical
            or audit.chain_version != "audit_jcs_sha256_v1"
            or audit.previous_hash != ("0" * 64 if audit.event_seq == 1 else previous)
            or audit.event_hash != audit_event_hash(audit.previous_hash, canonical)
            or audit.occurred_at > now
            or not scope["stopped"]
            or job.status != "failed"
            or run.status != "failed"
            or budget.state != "released"
            or job.finished_at != audit.occurred_at
            or job.last_error_code != REASON
            or job.result_code != REASON
        ):
            raise ValueError("COMPARISON_PRECLAIM_RECEIPT_INVALID")
    else:
        if (
            scope["stopped"]
            or job.status != "queued"
            or job.version != 1
            or job.last_error_code is not None
            or job.result_code is not None
            or job.finished_at is not None
            or run.status != "prepared"
            or budget.state != "reserved"
        ):
            raise ValueError(BLOCKED)
        if not close:
            return {
                "status": "preclaim_close_eligible",
                "experiment_id": str(manifest.experiment_id),
                "reason": REASON,
                "dispatch_authority": False,
                "replay_allowed": False,
            }
        await session.execute(
            text("UPDATE research_stage_scope SET stopped=true WHERE id=:id"),
            {"id": manifest.experiment_id},
        )
        # Take the existing financial mutex before the global audit append lock.
        await release_daily_budget(session, run_id=run.id)
        audit = await append_audit_event(
            session,
            actor_id=ACTOR,
            action=ACTION,
            schema_version=SCHEMA,
            payload=proof,
            occurred_at=now,
        )
        run.status = "failed"
        await session.flush()
        job.status = "failed"
        job.finished_at = now
        job.last_error_code = job.result_code = REASON
        job.version += 1
        await session.flush()
    return {
        "status": "preclaim_closed",
        "experiment_id": str(manifest.experiment_id),
        "audit_id": str(audit.id),
        "audit_hash": audit.event_hash,
        "reason": REASON,
        "reservation_state": "released",
        "model_calls": 0,
        "dispatch_authority": False,
        "replay_allowed": False,
    }
