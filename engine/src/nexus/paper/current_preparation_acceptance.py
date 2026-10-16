"""Explicitly end one current preparation's scheduling hold, never its uncertainty."""

from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import or_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.paper import current_preparation_recovery as recovery
from nexus.paper.history_seal import _row_hash
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord
from nexus.persistence.models.runtime import AuditEvent, Job

ACTION = "paper.manual_preparation.unknown_accepted"
SCHEMA = "manual_preparation_acceptance_v1"
ACTOR = "local-user:manual-preparation-acceptance"
BLOCKED = recovery.BLOCKED


class PreparationAcceptanceReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: UUID
    status: Literal["preparation_unknown_acknowledged"] = "preparation_unknown_acknowledged"
    external_effects: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    replayed: bool
    execution_locked: Literal[True] = True


async def _acceptances(session: AsyncSession, request_id: UUID) -> list[AuditEvent]:
    return list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["request_id"].astext == str(request_id),
            )
        )
    )


async def require_execution_not_accepted(session: AsyncSession, request_id: UUID) -> None:
    # A damaged/invalid disposition must never resurrect the old execution.
    # Caller holds the same account lock used by acceptance and native writes.
    if await _acceptances(session, request_id):
        raise ValueError(BLOCKED)


async def _proof(session: AsyncSession, row: ManualPaperRequest, at: datetime) -> dict[str, object]:
    from nexus.paper.manual_request import read_manual_request

    view = await read_manual_request(session, row)
    journal = await recovery._journal(session, row, at)
    if (
        view.status != "unknown"
        or view.code != "MANUAL_RESULT_UNKNOWN"
        or view.cycle_id is not None
        or len(journal) not in (2, 3)
        or journal[0].action != recovery.STARTED
        or journal[1].action != recovery.EFFECTS
    ):
        raise ValueError(BLOCKED)
    if len(journal) == 3:
        prior = await recovery.read_preparation_recovery(session, row)
        if prior is None or prior.disposition != "unknown_preserved":
            raise ValueError(BLOCKED)
    claims, cycles = await recovery._bindings(session, row)
    late_cycle = await session.scalar(
        select(AgentPaperCycleEventRecord.cycle_id)
        .where(
            or_(
                AgentPaperCycleEventRecord.fact_ids["manual_request_id"].astext == str(row.id),
                AgentPaperCycleEventRecord.fact_ids["manual_execution_id"].astext
                == str(journal[0].id),
            )
        )
        .limit(1)
    )
    if claims or cycles or late_cycle is not None:
        raise ValueError(BLOCKED)
    events = list(
        await session.scalars(
            select(ManualPaperRequestEvent)
            .where(
                ManualPaperRequestEvent.request_id == row.id,
            )
            .order_by(ManualPaperRequestEvent.sequence)
        )
    )
    if not events or any(e.occurred_at > at for e in events):
        raise ValueError(BLOCKED)
    return {
        "request_id": str(row.id),
        "account_id": str(row.account_id),
        "execution_id": str(journal[0].id),
        "request_hash": _row_hash(row),
        "event_hashes": [e.event_hash for e in events],
        "journal_hashes": [e.event_hash for e in journal],
        "external_effects": "unknown",
        "budget_preserved": True,
    }


async def current_unknown_accepted(session: AsyncSession, row: ManualPaperRequest) -> bool:
    """Validate only this request's proof; later legitimate work is not invalidation."""
    audits = await _acceptances(session, row.id)
    if not audits:
        return False
    try:
        if len(audits) != 1:
            raise ValueError(BLOCKED)
        audit = audits[0]
        now = await recovery._now(session)
        canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=audit.event_seq,
                occurred_at=audit.occurred_at,
                actor_id=audit.actor_id,
                action=audit.action,
                schema_version=audit.schema_version,
                payload=audit.payload,
            )
        )
        previous = await session.scalar(
            select(AuditEvent.event_hash).where(
                AuditEvent.event_seq == audit.event_seq - 1,
            )
        )
        if (
            audit.schema_version != SCHEMA
            or audit.actor_id != ACTOR
            or audit.occurred_at > now
            or audit.chain_version != "audit_jcs_sha256_v1"
            or audit.canonical_event != canonical
            or audit.previous_hash != ("0" * 64 if audit.event_seq == 1 else previous)
            or audit.event_hash != audit_event_hash(audit.previous_hash, canonical)
            or audit.payload != await _proof(session, row, audit.occurred_at)
        ):
            raise ValueError(BLOCKED)
        return True
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError(BLOCKED) from None


async def accept_current_unknown(
    session: AsyncSession,
    *,
    account_id: UUID,
    request_id: UUID,
    repo_root: Path,
    env_file: Path,
) -> PreparationAcceptanceReceipt:
    from nexus.paper.manual_request import require_manual_account_available

    await lock_portfolio_account(session, account_id)
    row = await session.get(ManualPaperRequest, request_id, with_for_update=True)
    if row is None or row.account_id != account_id:
        raise ValueError(BLOCKED)
    if await current_unknown_accepted(session, row):
        return PreparationAcceptanceReceipt(request_id=row.id, replayed=True)
    now = await recovery._now(session)
    proof = await _proof(session, row, now)
    journal = await recovery._journal(session, row, now)
    recovery._require_execution_producer_stopped(journal[0], repo_root, env_file)
    # Audit append may flush other writers' pending Jobs. Take its exact lock
    # without waiting, before any table fence, to avoid reversed lock ordering.
    if not await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended('nexus:audit-chain', 2020))")
    ):
        raise ValueError(BLOCKED)
    # Row locks cannot fence inserts into an empty queue. Hold this brief maintenance
    # snapshot against every Job/lease writer, including non-manual producers.
    try:
        await session.execute(
            text("LOCK TABLE jobs, job_leases, agent_research_runs IN SHARE MODE NOWAIT")
        )
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) == "55P03":
            # The caller rolls back the entire maintenance transaction. Never wait
            # while holding an earlier table lock: normal writers use both orders.
            raise ValueError(BLOCKED) from None
        raise
    kinds = ("agent.research", "agent.paper.research", "agent.portfolio_synthesis")
    if (
        await session.scalar(
            select(Job.id)
            .where(
                Job.job_type.in_(kinds),
                Job.status.in_(["queued", "running"]),
            )
            .limit(1)
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    if (
        await session.scalar(
            select(AgentResearchRunRecord.id)
            .where(
                AgentResearchRunRecord.status.not_in(["completed", "failed", "abstained"]),
            )
            .limit(1)
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    await recovery._require_idle_consumers(session, row, now)
    await recovery._require_other_claims_settled(session, row, now)
    await require_manual_account_available(
        session,
        account_id=account_id,
        now=now,
        excluding_request_id=row.id,
    )
    # No old record or budget is mutated. The audit is the persistent execution fence.
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        occurred_at=now,
        payload=proof,
    )
    recovery._require_execution_producer_stopped(journal[0], repo_root, env_file)
    if not await current_unknown_accepted(session, row):
        raise ValueError(BLOCKED)
    return PreparationAcceptanceReceipt(request_id=row.id, replayed=False)
