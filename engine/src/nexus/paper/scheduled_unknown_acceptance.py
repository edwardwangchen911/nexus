"""Explicit scheduled uncertainty acceptance; never settle or revive the old slot."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import or_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.models import job_input_hash
from nexus.paper.history_seal import _row_hash
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job, JobLease
from nexus.settings import AgentRuntimeMode, EngineSettings

ACTION = "paper.scheduled_unknown.accepted"
SCHEMA = "scheduled_unknown_accept_v1"
ACTOR = "local-user:scheduled-unknown-acceptance"
BLOCKED = "SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED"
RECEIPT = {
    "outcome": "failed",
    "error_code": "AGENT_PAPER_TRIGGER_FAILED",
    "external_effects": "unknown",
}


class ScheduledAcceptanceReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    claim_id: UUID
    decision_slot_at: datetime
    accepted_at: datetime
    status: Literal["scheduled_unknown_acknowledged"] = "scheduled_unknown_acknowledged"
    external_effects: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    execution_locked: Literal[True] = True
    replayed: bool


class ScheduledAcceptanceCheck(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    claim_id: UUID
    decision_slot_at: datetime
    checked_at: datetime
    status: Literal["eligible_at_check", "already_acknowledged"]
    check_only: Literal[True] = True
    external_effects: Literal["unknown"] = "unknown"
    budget_preserved: Literal[True] = True
    execution_locked: Literal[True] = True


def _audit_payloads(row: AuditEvent) -> tuple[dict, dict]:
    # Both immutable representations must agree. A damaged index field cannot hide
    # the old execution fence or create a second acceptance record.
    try:
        canonical = json.loads(row.canonical_event)
        payload = canonical["payload"]
        if not isinstance(payload, dict) or not isinstance(row.payload, dict):
            raise ValueError(BLOCKED)
    except (ValueError, TypeError, KeyError):
        raise ValueError(BLOCKED) from None
    return row.payload, payload


async def _audits(session: AsyncSession, account_id: UUID) -> list[AuditEvent]:
    rows = await session.scalars(
        select(AuditEvent)
        .where(
            or_(
                AuditEvent.action == ACTION,
                AuditEvent.actor_id == ACTOR,
                AuditEvent.schema_version == SCHEMA,
            )
        )
        .execution_options(populate_existing=True)
    )
    result = []
    for row in rows:
        payloads = _audit_payloads(row)
        if any(p.get("account_id") == str(account_id) for p in payloads):
            if payloads[0] != payloads[1]:
                raise ValueError(BLOCKED)
            result.append(row)
    return result


async def require_scheduled_execution_open(
    session: AsyncSession, *, account_id: UUID, slot: datetime, claim_id: UUID | None = None
) -> None:
    """Permanent denial is independent of whether an acceptance proof is valid."""
    if not isinstance(slot, datetime) or slot.utcoffset() is None:
        raise ValueError(BLOCKED)
    slot = slot.astimezone(UTC)
    await lock_portfolio_account(session, account_id)
    # An advisory lock cannot refresh an already established repeatable-read snapshot.
    # Normal producers use READ COMMITTED; unsupported old snapshots fail closed.
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(BLOCKED)
    rows = await _audits(session, account_id)
    for row in rows:
        if row.payload.get("decision_slot_at") == slot.isoformat() or (
            claim_id is not None and row.payload.get("claim_id") == str(claim_id)
        ):
            raise ValueError(BLOCKED)


async def _proof(
    session: AsyncSession,
    *,
    account_id: UUID,
    claim_id: UUID,
    batch_id: UUID,
    model_id: UUID,
    at: datetime,
) -> dict[str, object]:
    from nexus.agents.codex_formal_stage import require_formal_stage_model_profile

    claim = await session.get(IdempotencyRecord, claim_id, populate_existing=True)
    if claim is None:
        raise ValueError(BLOCKED)
    try:
        slot = datetime.fromisoformat(claim.idempotency_key)
    except (TypeError, ValueError):
        raise ValueError(BLOCKED) from None
    local = slot.astimezone(ZoneInfo("America/New_York"))
    if (
        slot.utcoffset() != timedelta(0)
        or slot.isoformat() != claim.idempotency_key
        or (local.hour, local.minute, local.second, local.microsecond) != (10, 0, 0, 0)
        or not slot <= claim.created_at < slot + timedelta(minutes=5)
        or claim.created_at > at
        or claim.actor_id != f"local-agent-paper-schwab:{account_id}"
        or claim.route != "agent-paper-schwab-trigger-v2"
        or claim.schema_version != "agent_paper_schwab_trigger_v2"
        or claim.response_payload != RECEIPT
        or claim.formal_budget_binding_id is not None
    ):
        raise ValueError(BLOCKED)
    batch = await session.get(AgentPaperCandidateBatchRecord, batch_id, populate_existing=True)
    model = await require_formal_stage_model_profile(
        session, model_config_id=model_id, now=claim.created_at
    )
    if (
        batch is None
        or batch.account_id != account_id
        or batch.created_at > claim.created_at
        or model.input_usd_per_million != 0
        or model.output_usd_per_million != 0
        or model.max_tool_calls != 0
    ):
        raise ValueError(BLOCKED)
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

    try:
        if await load_agent_paper_candidate_batch(session, batch_id=batch.id) is None:
            raise ValueError(BLOCKED)
    except ValueError:
        raise ValueError(BLOCKED) from None
    request = {
        "schema": claim.route,
        "account_id": str(account_id),
        "decision_session": local.date().isoformat(),
        "decision_slot_at": slot.isoformat(),
        "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
        "client_idempotency_hash": sha256(
            f"agent-paper-loop:{slot.isoformat()}".encode()
        ).hexdigest(),
        "candidate_batch_id": str(batch.id),
        "candidate_batch_hash": batch.batch_hash,
        "formal_stage_capacity": True,
        "formal_model_config_id": str(model.id),
        "formal_model_hash": model.config_hash,
    }
    if claim.request_hash != job_input_hash(request):
        raise ValueError(BLOCKED)
    # Do not restrict this query to accepted_at: a late old cycle invalidates exemption.
    if (
        await session.scalar(
            select(AgentPaperCycleRecord.id)
            .where(
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_slot_at == slot,
            )
            .limit(1)
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    return {
        "claim_id": str(claim.id),
        "account_id": str(account_id),
        "decision_slot_at": slot.isoformat(),
        "claim_hash": _row_hash(claim),
        "batch_id": str(batch.id),
        "model_id": str(model.id),
        "request_hash": claim.request_hash,
        "external_effects": "unknown",
        "historical_usage": "unknown",
        "historical_fees": "unknown",
        "budget_preserved": True,
        "formal_budget_binding_id": None,
        "disposition": "accepted_unknown_scheduling_only",
    }


async def _validate(session: AsyncSession, row: AuditEvent, *, now: datetime) -> dict[str, object]:
    try:
        payload = row.payload
        proof = await _proof(
            session,
            account_id=UUID(str(payload["account_id"])),
            claim_id=UUID(str(payload["claim_id"])),
            batch_id=UUID(str(payload["batch_id"])),
            model_id=UUID(str(payload["model_id"])),
            at=row.occurred_at,
        )
        canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=row.event_seq,
                occurred_at=row.occurred_at,
                actor_id=row.actor_id,
                action=row.action,
                schema_version=row.schema_version,
                payload=payload,
            )
        )
        previous = await session.scalar(
            select(AuditEvent.event_hash).where(AuditEvent.event_seq == row.event_seq - 1)
        )
        if (
            row.actor_id != ACTOR
            or row.action != ACTION
            or row.schema_version != SCHEMA
            or row.occurred_at > now
            or row.payload != proof
            or row.chain_version != "audit_jcs_sha256_v1"
            or row.canonical_event != canonical
            or row.previous_hash != ("0" * 64 if row.event_seq == 1 else previous)
            or row.event_hash != audit_event_hash(row.previous_hash, canonical)
        ):
            raise ValueError(BLOCKED)
        return proof
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError(BLOCKED) from None


async def accepted_scheduled_claim_ids(session: AsyncSession, account_id: UUID) -> set[UUID]:
    from nexus.paper.current_preparation_recovery import _now

    now = await _now(session)
    result: set[UUID] = set()
    for audit in await _audits(session, account_id):
        proof = await _validate(session, audit, now=now)
        claim_id = UUID(str(proof["claim_id"]))
        if claim_id in result:
            raise ValueError(BLOCKED)
        result.add(claim_id)
    return result


async def inspect_or_accept_scheduled_unknown(
    session: AsyncSession,
    *,
    settings: EngineSettings,
    claim_id: UUID,
    repo_root: Path,
    env_file: Path,
    accept: bool = False,
    accept_unknown_effects: bool = False,
) -> ScheduledAcceptanceCheck | ScheduledAcceptanceReceipt:
    from nexus.paper import manual_preparation_close as stopped
    from nexus.paper.current_preparation_acceptance import (
        accepted_claim_ids,
        require_no_live_research_runs,
    )
    from nexus.paper.current_preparation_recovery import _now
    from nexus.paper.formal_budget_protection import latest_binding
    from nexus.paper.manual_request import require_manual_account_state_available
    from nexus.paper.preparation_unknown_ack import _audits as legacy_audits
    from nexus.paper.preparation_unknown_ack import _claim_ids, _terminal_claim
    from nexus.paper.preparation_unknown_ack import _validate as legacy_validate

    account_id = settings.agent_paper_account_id
    if account_id is None or (accept and accept_unknown_effects is not True):
        raise ValueError(BLOCKED)
    # Nonblocking order: lifecycle/schema (CLI), account, budget, audit, execution tables.
    if not await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope,1818))"),
        {"scope": f"internal-paper:{account_id}"},
    ):
        raise ValueError(BLOCKED)
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError(BLOCKED)
    now = await _now(session)
    claim = await session.get(IdempotencyRecord, claim_id, populate_existing=True)
    if claim is None or claim.actor_id != f"local-agent-paper-schwab:{account_id}":
        raise ValueError(BLOCKED)
    audits = [
        a
        for a in await _audits(session, account_id)
        if a.payload.get("claim_id") == str(claim_id)
        or a.payload.get("decision_slot_at") == claim.idempotency_key
    ]
    if audits:
        if len(audits) != 1 or (await _validate(session, audits[0], now=now))["claim_id"] != str(
            claim_id
        ):
            raise ValueError(BLOCKED)
        slot = datetime.fromisoformat(claim.idempotency_key)
        if not accept:
            return ScheduledAcceptanceCheck(
                claim_id=claim_id,
                decision_slot_at=slot,
                checked_at=now,
                status="already_acknowledged",
            )
        return ScheduledAcceptanceReceipt(
            claim_id=claim_id,
            decision_slot_at=slot,
            accepted_at=audits[0].occurred_at,
            replayed=True,
        )
    if (
        settings.agent_runtime_mode != AgentRuntimeMode.CODEX_SDK
        or settings.formal_budget_protection_enabled
        or settings.agent_paper_candidate_batch_id is None
        or settings.agent_paper_formal_model_config_id is None
    ):
        raise ValueError(BLOCKED)
    proof = await _proof(
        session,
        account_id=account_id,
        claim_id=claim_id,
        batch_id=settings.agent_paper_candidate_batch_id,
        model_id=settings.agent_paper_formal_model_config_id,
        at=now,
    )
    slot = datetime.fromisoformat(claim.idempotency_key)
    for day in sorted({slot.date(), now.date()}):
        if not await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
            {"key": f"agent-budget:{day.isoformat()}"},
        ):
            raise ValueError(BLOCKED)
        if await latest_binding(session, business_date=day) is not None:
            raise ValueError(BLOCKED)
    stopped.require_preparation_producers_stopped(repo_root, env_file)
    if not await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended('nexus:audit-chain',2020))")
    ):
        raise ValueError(BLOCKED)
    try:
        await session.execute(
            text(
                "LOCK TABLE jobs, job_leases, agent_research_runs, job_attempts "
                "IN SHARE MODE NOWAIT"
            )
        )
    except DBAPIError as e:
        if getattr(e.orig, "sqlstate", None) == "55P03":
            raise ValueError(BLOCKED) from None
        raise
    kinds = ("agent.research", "agent.paper.research", "agent.portfolio_synthesis")
    if (
        await session.scalar(
            select(Job.id)
            .where(Job.job_type.in_(kinds), Job.status.in_(("queued", "running")))
            .limit(1)
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    from nexus.jobs.repository import job_lease_consumed

    leased = await session.execute(
        select(Job, JobLease)
        .join(JobLease, Job.id == JobLease.job_id)
        .where(Job.job_type.in_(kinds), JobLease.expires_at > now)
        .execution_options(populate_existing=True)
    )
    for job, lease in leased:
        if not await job_lease_consumed(session, job=job, lease=lease, now=now):
            raise ValueError(BLOCKED)
    await require_no_live_research_runs(session, now=now)
    allowed = await accepted_claim_ids(session, account_id) | await accepted_scheduled_claim_ids(
        session, account_id
    )
    old = await legacy_audits(session, account_id)
    if len(old) > 1:
        raise ValueError(BLOCKED)
    for audit in old:
        allowed.update(_claim_ids(await legacy_validate(session, audit, now=now)))
    allowed.add(claim_id)
    claims = await session.scalars(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}"
        )
    )
    for other in claims:
        if other.id not in allowed and not await _terminal_claim(
            session, other, account_id=account_id
        ):
            raise ValueError(BLOCKED)
    await require_manual_account_state_available(session, account_id=account_id, now=now)
    stopped.require_preparation_producers_stopped(repo_root, env_file)
    if proof != await _proof(
        session,
        account_id=account_id,
        claim_id=claim_id,
        batch_id=settings.agent_paper_candidate_batch_id,
        model_id=settings.agent_paper_formal_model_config_id,
        at=now,
    ):
        raise ValueError(BLOCKED)
    if not accept:
        return ScheduledAcceptanceCheck(
            claim_id=claim_id, decision_slot_at=slot, checked_at=now, status="eligible_at_check"
        )
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload=proof,
        occurred_at=now,
    )
    stopped.require_preparation_producers_stopped(repo_root, env_file)
    if claim_id not in await accepted_scheduled_claim_ids(session, account_id):
        raise ValueError(BLOCKED)
    return ScheduledAcceptanceReceipt(
        claim_id=claim_id, decision_slot_at=slot, accepted_at=now, replayed=False
    )
