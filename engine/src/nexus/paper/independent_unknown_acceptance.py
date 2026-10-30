"""Explicit v3 historical occupancy disposition; unknown money never becomes known."""

import json
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
from nexus.jobs.models import job_input_hash
from nexus.paper.independent_unknown_proof import (
    BLOCKED,
    FACT_FILTERS,
    independent_unknown_proof,
    load_independent_request,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease
from nexus.settings import EngineSettings

ACTION = "paper.independent_unknown.accepted"
SCHEMA = "independent_unknown_accept_v1"
ACTOR = "local-user:independent-unknown-acceptance"


class IndependentAcceptanceCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    request_id: UUID
    checked_at: datetime
    proof_hash: str
    status: Literal["eligible_at_check", "already_acknowledged"]
    check_only: Literal[True] = True
    budget_preserved: Literal[True] = True
    execution_locked: Literal[True] = True


class IndependentAcceptanceReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    request_id: UUID
    accepted_at: datetime
    proof_hash: str
    status: Literal["independent_unknown_acknowledged"] = "independent_unknown_acknowledged"
    original_result: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    execution_locked: Literal[True] = True
    replayed: bool


async def _audits(session: AsyncSession, request_id: UUID) -> list[AuditEvent]:
    rows = await session.scalars(
        select(AuditEvent)
        .where(
            or_(
                AuditEvent.action == ACTION,
                AuditEvent.schema_version == SCHEMA,
                AuditEvent.actor_id == ACTOR,
            )
        )
        .execution_options(populate_existing=True)
    )
    result = []
    for audit in rows:
        try:
            canonical = json.loads(audit.canonical_event)["payload"]
            if not isinstance(canonical, dict) or not isinstance(audit.payload, dict):
                raise ValueError(BLOCKED)
            if any(p.get("request_id") == str(request_id) for p in (canonical, audit.payload)):
                if canonical != audit.payload:
                    raise ValueError(BLOCKED)
                result.append(audit)
        except (TypeError, KeyError, ValueError):
            raise ValueError(BLOCKED) from None
    return result


async def _validate(
    session: AsyncSession,
    *,
    row: ManualPaperRequest,
    audit: AuditEvent,
    now: datetime,
    admission: bool = True,
) -> str:
    if admission and await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(BLOCKED)
    # Native late observation/verification/cleanup writers hold this scope.
    # Keep a validated exception stable until the admission transaction commits.
    try:
        # Service readiness is a database-enforced READ ONLY observation. It
        # cannot acquire row locks or confer execution rights; the later
        # writable admission transaction still obtains the shared scope lock.
        if admission and await session.scalar(text("SHOW transaction_read_only")) != "on":
            await session.execute(
                text("SELECT id FROM research_stage_scope WHERE id=:id FOR SHARE NOWAIT"),
                {"id": row.id},
            )
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) == "55P03":
            raise ValueError(BLOCKED) from None
        raise
    proof = await independent_unknown_proof(session, row=row, now=now)
    digest = job_input_hash(proof)
    payload = {**proof, "proof_hash": digest}
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=audit.event_seq,
            occurred_at=audit.occurred_at,
            actor_id=ACTOR,
            action=ACTION,
            schema_version=SCHEMA,
            payload=payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == audit.event_seq - 1)
    )
    if (
        audit.actor_id != ACTOR
        or audit.action != ACTION
        or audit.schema_version != SCHEMA
        or audit.occurred_at > now
        or audit.payload != payload
        or audit.chain_version != "audit_jcs_sha256_v1"
        or audit.canonical_event != canonical
        or audit.previous_hash != ("0" * 64 if audit.event_seq == 1 else previous)
        or audit.event_hash != audit_event_hash(audit.previous_hash, canonical)
    ):
        raise ValueError(BLOCKED)
    return digest


async def read_independent_disposition(
    session: AsyncSession, row: ManualPaperRequest, *, now: datetime
) -> Literal["unknown_acknowledged", "recheck_required"] | None:
    """Snapshot presentation only; never grants admission or acquires a writer lock."""
    audits = await _audits(session, row.id)
    if not audits:
        return None
    if len(audits) != 1:
        return "recheck_required"
    try:
        await _validate(session, row=row, audit=audits[0], now=now, admission=False)
    except ValueError:
        return "recheck_required"
    return "unknown_acknowledged"


async def independent_unknown_accepted(
    session: AsyncSession, row: ManualPaperRequest, *, now: datetime
) -> bool:
    audits = await _audits(session, row.id)
    if not audits:
        return False
    if len(audits) != 1:
        raise ValueError(BLOCKED)
    await _validate(session, row=row, audit=audits[0], now=now)
    return True


async def require_independent_execution_open(session: AsyncSession, *, request_id: UUID) -> None:
    """Any original acceptance tombstone forbids replay, even after proof invalidation."""
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(BLOCKED)
    if await _audits(session, request_id):
        raise ValueError(BLOCKED)


async def inspect_or_accept_independent_unknown(
    session: AsyncSession,
    *,
    settings: EngineSettings,
    request_id: UUID,
    repo_root: Path,
    env_file: Path,
    accept: bool = False,
    accept_unknown_effects: bool = False,
    expected_proof_hash: str | None = None,
) -> IndependentAcceptanceCheck | IndependentAcceptanceReceipt:
    from nexus.paper.current_preparation_recovery import _now
    from nexus.paper.manual_preparation_close import require_preparation_producers_stopped
    from nexus.paper.manual_request import require_manual_account_available

    account_id = settings.agent_paper_account_id
    if account_id is None or (
        accept and (accept_unknown_effects is not True or expected_proof_hash is None)
    ):
        raise ValueError(BLOCKED)
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(BLOCKED)
    if not await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope,1818))"),
        {"scope": f"internal-paper:{account_id}"},
    ):
        raise ValueError(BLOCKED)
    now = await _now(session)
    row = await load_independent_request(session, request_id=request_id, account_id=account_id)
    audits = await _audits(session, request_id)
    if audits:
        if len(audits) != 1:
            raise ValueError(BLOCKED)
        digest = await _validate(session, row=row, audit=audits[0], now=now)
        if accept:
            if digest != expected_proof_hash:
                raise ValueError(BLOCKED)
            return IndependentAcceptanceReceipt(
                request_id=request_id,
                accepted_at=audits[0].occurred_at,
                proof_hash=digest,
                replayed=True,
            )
        return IndependentAcceptanceCheck(
            request_id=request_id, checked_at=now, proof_hash=digest, status="already_acknowledged"
        )
    require_preparation_producers_stopped(repo_root, env_file)
    try:
        await session.execute(
            text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE NOWAIT"),
            {"id": request_id},
        )
        days = await session.scalars(
            text(
                "SELECT DISTINCT b.business_date FROM agent_daily_budget_reservations b "
                "JOIN research_request_members m ON m.run_id=b.run_id "
                "JOIN research_request_manifests f ON f.id=m.manifest_id "
                "WHERE f.request_id=:id ORDER BY b.business_date"
            ),
            {"id": request_id},
        )
        for day in days:
            if not await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                {"key": f"agent-budget:{day.isoformat()}"},
            ):
                raise ValueError(BLOCKED)
        if not await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended('nexus:audit-chain',2020))")
        ):
            raise ValueError(BLOCKED)
        # Fence inserts too, including absent stage observations. No waiting while
        # holding an earlier lock; competing writers retain their original facts.
        tables = ", ".join((*FACT_FILTERS, "outbox_events", "outbox_delivery_attempts"))
        await session.execute(text(f"LOCK TABLE {tables} IN SHARE MODE NOWAIT"))
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) == "55P03":
            raise ValueError(BLOCKED) from None
        raise
    if await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE status IN ('queued','running')) "
            "OR EXISTS(SELECT 1 FROM outbox_events WHERE status='delivering') "
            "OR EXISTS(SELECT 1 FROM outbox_delivery_attempts WHERE outcome='delivering')"
        )
    ):
        raise ValueError(BLOCKED)
    from nexus.jobs.repository import job_lease_consumed

    leased = await session.execute(
        select(Job, JobLease)
        .join(JobLease, Job.id == JobLease.job_id)
        .where(JobLease.expires_at > now)
        .execution_options(populate_existing=True)
    )
    for job, lease in leased:
        if not await job_lease_consumed(session, job=job, lease=lease, now=now):
            raise ValueError(BLOCKED)
    proof = await independent_unknown_proof(session, row=row, now=now)
    await require_manual_account_available(
        session, account_id=account_id, now=now, excluding_request_id=request_id, read_only=True
    )
    digest = job_input_hash(proof)
    require_preparation_producers_stopped(repo_root, env_file)
    if not accept:
        return IndependentAcceptanceCheck(
            request_id=request_id, checked_at=now, proof_hash=digest, status="eligible_at_check"
        )
    if digest != expected_proof_hash:
        raise ValueError(BLOCKED)
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload={**proof, "proof_hash": digest},
        occurred_at=now,
    )
    return IndependentAcceptanceReceipt(
        request_id=request_id, accepted_at=now, proof_hash=digest, replayed=False
    )
