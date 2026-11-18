"""Historical occupancy disposition for the single approved failed NVDA increment."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.repository import job_lease_consumed
from nexus.paper.target_service import try_lock_portfolio_account
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease

RUN_ID = UUID("01a1ef15-3481-73ba-9379-7e7c3b034c38")
JOB_ID = UUID("01a1ef15-3487-7aab-9918-84cb793a7e7a")
_FAILURE_HASH = "1e94891e78981fcf0c6d3c022dc41d63b0fc60a84cb2dd5c46268d1901663221"
_PROOF_HASH = "b5c07e08d3026d80607a4686fda8fc01045e5188b44ea10faf7fc52ccde764ca"
_ACCEPT = "research.monitor.original_occupancy_accepted"
_REVOKE = "research.monitor.original_occupancy_revoked"
_ACTOR = "local-monitor-occupancy-disposition"
_SCHEMA = "monitor_original_occupancy_v1"
BLOCKED = "MONITOR_ORIGINAL_OCCUPANCY_BLOCKED"

# The hash is over the exact frozen facts of this one disposition, not an
# alternative terminal or settlement proof. Keep it independent of today's clock.
_FACTS = text("""
    SELECT b.business_date, l.id AS lease_id,
           encode(sha256(convert_to(jsonb_build_object(
               'increment', to_jsonb(i), 'owner', to_jsonb(m),
               'trigger', to_jsonb(q), 'binding', to_jsonb(f), 'day_claim', to_jsonb(d),
               'run', to_jsonb(r), 'job', to_jsonb(j), 'attempt', to_jsonb(a),
               'lease', to_jsonb(l), 'route', to_jsonb(v), 'scope', to_jsonb(s),
               'terminal', to_jsonb(t), 'budget', to_jsonb(b), 'model', to_jsonb(p),
               'failure', to_jsonb(e),
               'outputs', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.sequence)
                           FROM agent_research_outputs x WHERE x.run_id=i.run_id),
               'events', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.sequence)
                          FROM agent_research_events x WHERE x.run_id=i.run_id)
           )::text, 'UTF8')), 'hex') AS digest
    FROM research_monitor_increments i
    JOIN research_monitor_triggers q ON q.id=i.trigger_id AND q.research_run_id=i.run_id
    JOIN sec_event_research_bindings f ON f.trigger_id=q.id AND f.batch_id=i.batch_id
    JOIN sec_auto_increment_day_claims d ON d.run_id=i.run_id AND d.increment_id=i.id
      AND d.account_id=i.account_id AND d.instrument_id=i.instrument_id
      AND d.session_date=i.session_date
    JOIN agent_research_runs r ON r.id=i.run_id AND r.instrument_id=i.instrument_id
    JOIN jobs j ON j.id=i.job_id
    JOIN job_attempts a ON a.job_id=j.id AND a.attempt_no=1
    JOIN job_leases l ON l.job_id=j.id
    JOIN research_stage_member m ON m.run_id=i.run_id AND m.scope_id=i.id AND m.job_id=j.id
    JOIN research_stage_scope s ON s.id=m.scope_id AND s.increment_id=i.id
    JOIN research_stage_route v ON v.run_id=i.run_id AND v.scope_id=i.id AND v.job_id=j.id
    JOIN research_stage_terminal t ON t.run_id=i.run_id
    JOIN agent_daily_budget_reservations b ON b.run_id=i.run_id
    JOIN agent_model_configs p ON p.id=r.model_config_id AND p.config_hash=r.model_hash
    JOIN audit_events e ON e.event_hash=:failure
    WHERE i.run_id=:run AND i.job_id=:job AND i.account_id=:account
      AND q.account_id=i.account_id AND q.instrument_id=i.instrument_id
      AND q.status='admitted' AND f.account_id=i.account_id AND f.instrument_id=i.instrument_id
      AND r.status='failed' AND r.retry_count=0 AND r.settled_cost_usd IS NULL
      AND r.input_tokens IS NULL AND r.output_tokens IS NULL
      AND j.job_type='agent.paper.research' AND j.status='failed'
      AND j.payload=jsonb_build_object('run_id', i.run_id::text)
      AND j.attempt_count=1 AND j.max_attempts=1 AND j.manual_retry_count=0
      AND a.outcome='failed' AND t.closed_at=j.finished_at AND l.expires_at<=:now
      AND s.stopped AND v.kind='increment' AND v.platform_binding IS NOT NULL
      AND NOT t.known AND cardinality(t.observed_stages)=0 AND t.evidence='[]'::jsonb
      AND b.state='reserved' AND b.settled_usd IS NULL AND b.reserved_usd=2.12192
      AND r.reserved_cost_usd=b.reserved_usd AND b.pricing_version=r.pricing_version
      AND e.action='research.stage_failure' AND e.actor_id='research-stage-worker'
      AND e.schema_version='research_stage_failure_v1'
      AND e.payload->>'run_id'=i.run_id::text AND e.payload->>'job_id'=j.id::text
      AND e.payload->>'failure_phase'='authorization'
      AND e.payload->>'exception_kind'='protocol_rejected'
      AND NOT EXISTS (SELECT FROM job_attempts x WHERE x.job_id=j.id AND x.attempt_no<>1)
      AND NOT EXISTS (SELECT FROM research_stage_authorization x WHERE x.run_id=i.run_id)
      AND NOT EXISTS (SELECT FROM research_stage_observation x WHERE x.run_id=i.run_id)
      AND NOT EXISTS (SELECT FROM research_stage_verification x WHERE x.run_id=i.run_id)
      AND NOT EXISTS (SELECT FROM research_stage_cleanup x WHERE x.run_id=i.run_id)
      AND NOT EXISTS (SELECT FROM audit_events x WHERE x.action='research.stage_failure'
                      AND x.payload->>'run_id'=i.run_id::text AND x.id<>e.id)
""")


async def _account_lock(session: AsyncSession, account_id: UUID) -> None:
    if not await try_lock_portfolio_account(session, account_id):
        raise ValueError(BLOCKED)


async def _lock_disposition(session: AsyncSession, account_id: UUID, *, shared: bool) -> None:
    key = {"key": f"nexus:original-monitor-disposition:{account_id}:{RUN_ID}"}
    statement = (
        "SELECT pg_try_advisory_xact_lock_shared(hashtextextended(:key,0))"
        if shared
        else "SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))"
    )
    if not await session.scalar(text(statement), key):
        raise ValueError(BLOCKED)


async def fence_failure_append(session: AsyncSession, run_id: object) -> None:
    """Serialize a conflict on the one original Run with new dependent headers."""
    if run_id != str(RUN_ID):
        return
    account_id = await session.scalar(
        text("SELECT account_id FROM research_monitor_increments WHERE run_id=:run"),
        {"run": RUN_ID},
    )
    if account_id is None:
        raise ValueError(BLOCKED)
    await _lock_disposition(session, account_id, shared=False)


async def _proof(session: AsyncSession, account_id: UUID, now: datetime) -> str:
    rows = (
        (
            await session.execute(
                _FACTS,
                {
                    "run": RUN_ID,
                    "job": JOB_ID,
                    "account": account_id,
                    "now": now,
                    "failure": _FAILURE_HASH,
                },
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != 1 or rows[0]["digest"] != _PROOF_HASH:
        raise ValueError(BLOCKED)
    job = await session.get(Job, JOB_ID, populate_existing=True)
    lease = await session.get(JobLease, rows[0]["lease_id"], populate_existing=True)
    if (
        job is None
        or lease is None
        or not await job_lease_consumed(session, job=job, lease=lease, now=now)
    ):
        raise ValueError(BLOCKED)
    return _PROOF_HASH


async def _verified_audit(session: AsyncSession, event: AuditEvent, now: datetime) -> None:
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            schema_version=event.schema_version,
            action=event.action,
            payload=event.payload,
        )
    )
    previous = (
        "0" * 64
        if event.event_seq == 1
        else await session.scalar(
            select(AuditEvent.event_hash).where(AuditEvent.event_seq == event.event_seq - 1)
        )
    )
    if (
        event.occurred_at > now
        or previous != event.previous_hash
        or event.chain_version != "audit_jcs_sha256_v1"
        or canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
    ):
        raise ValueError(BLOCKED)


def _payload(account_id: UUID) -> dict[str, object]:
    return {
        "account_id": str(account_id),
        "run_id": str(RUN_ID),
        "job_id": str(JOB_ID),
        "proof_hash": _PROOF_HASH,
    }


async def _decision(
    session: AsyncSession, account_id: UUID, now: datetime
) -> tuple[AuditEvent | None, AuditEvent | None]:
    rows = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action.in_((_ACCEPT, _REVOKE)),
                AuditEvent.payload["run_id"].astext == str(RUN_ID),
            )
            .order_by(AuditEvent.event_seq)
            .limit(3)
        )
    )
    if not rows:
        return None, None
    accepted_event = rows[0]
    if len(rows) > 2 or accepted_event.action != _ACCEPT:
        raise ValueError(BLOCKED)
    expected = _payload(account_id)
    for event in rows:
        if event.actor_id != _ACTOR or event.schema_version != _SCHEMA:
            raise ValueError(BLOCKED)
        await _verified_audit(session, event, now)
    if accepted_event.payload != expected:
        raise ValueError(BLOCKED)
    if len(rows) == 2:
        revoked_event = rows[1]
        if revoked_event.action != _REVOKE or revoked_event.payload != {
            **expected,
            "acceptance_hash": accepted_event.event_hash,
        }:
            raise ValueError(BLOCKED)
        return accepted_event, revoked_event
    return accepted_event, None


async def accepted(session: AsyncSession, *, run_id: UUID, account_id: UUID, now: datetime) -> bool:
    if run_id != RUN_ID:
        return False
    await _lock_disposition(session, account_id, shared=True)
    decision, revoked = await _decision(session, account_id, now)
    if decision is None or revoked is not None:
        return False
    await _proof(session, account_id, now)
    return True


async def active_monitor_runs(
    session: AsyncSession, *, account_id: UUID, now: datetime
) -> tuple[UUID | None, ...]:
    # Read one extra so excluding the original cannot hide two other members.
    # Preserve unbound/unknown rows for the existing queue checks.
    active = tuple(
        await session.scalars(
            select(MonitorTrigger.research_run_id)
            .where(
                MonitorTrigger.account_id == account_id,
                MonitorTrigger.status.in_(("admitted", "unknown")),
            )
            .distinct()
            .limit(3)
        )
    )
    if RUN_ID in active and await accepted(session, run_id=RUN_ID, account_id=account_id, now=now):
        return tuple(run_id for run_id in active if run_id != RUN_ID)
    return active


async def dispose(
    session: AsyncSession,
    *,
    action: str,
    run_id: UUID,
    account_id: UUID,
    expected_hash: str | None,
    now: datetime,
) -> dict[str, object]:
    """Check, record or tighten the single disposition under its account lock."""
    if run_id != RUN_ID or action not in {"check", "record", "revoke"}:
        raise ValueError(BLOCKED)
    await _account_lock(session, account_id)
    await _lock_disposition(session, account_id, shared=action == "check")
    decision, revoked = await _decision(session, account_id, now)
    if action == "check":
        if revoked is not None:
            return _receipt(revoked, revoked=True)
        try:
            await _proof(session, account_id, now)
        except ValueError as error:
            if decision is None or str(error) != BLOCKED:
                raise
            return {**_receipt(decision, revoked=False), "status": "inapplicable"}
        if decision is not None:
            return _receipt(decision, revoked=False)
        return {"status": "eligible_at_check", "proof_hash": _PROOF_HASH, "run_id": str(RUN_ID)}

    if expected_hash != _PROOF_HASH:
        raise ValueError(BLOCKED)
    if action == "revoke":
        if decision is None:
            raise ValueError(BLOCKED)
        event = revoked or await append_audit_event(
            session,
            actor_id=_ACTOR,
            action=_REVOKE,
            schema_version=_SCHEMA,
            payload={**_payload(account_id), "acceptance_hash": decision.event_hash},
            occurred_at=now,
        )
    else:
        if revoked is not None:
            raise ValueError(BLOCKED)
        if decision is None:
            # Fence originals through commit. The stopped scope itself is
            # irreversible and SQL prohibits inserting another authorization.
            await _lock_original(session)
        await _proof(session, account_id, now)
        event = decision or await append_audit_event(
            session,
            actor_id=_ACTOR,
            action=_ACCEPT,
            schema_version=_SCHEMA,
            payload=_payload(account_id),
            occurred_at=now,
        )
    return _receipt(event, revoked=action == "revoke")


def _receipt(event: AuditEvent, *, revoked: bool) -> dict[str, object]:
    return {
        "status": "revoked" if revoked else "recorded",
        "run_id": str(RUN_ID),
        "proof_hash": _PROOF_HASH,
        "event_hash": event.event_hash,
        "recorded_at": event.occurred_at.isoformat(),
    }


async def _lock_original(session: AsyncSession) -> None:
    await session.execute(
        text("""
            SELECT i.id FROM research_monitor_increments i
            JOIN research_stage_scope s ON s.id=i.id
            JOIN jobs j ON j.id=i.job_id
            JOIN job_leases l ON l.job_id=j.id
            JOIN agent_research_runs r ON r.id=i.run_id
            JOIN agent_daily_budget_reservations b ON b.run_id=i.run_id
            WHERE i.run_id=:run
            FOR SHARE OF s,j,l,r,b NOWAIT
        """),
        {"run": RUN_ID},
    )


async def require_for_send(
    session: AsyncSession, run_id: UUID, *, fenced_transport: bool, hold_original: bool = False
) -> bool:
    """Retain the exact account fence until the subsequent native header boundary."""
    if run_id == RUN_ID:
        raise ValueError(BLOCKED)
    account_id = await session.scalar(
        text("""
            SELECT original.account_id
            FROM research_monitor_increments original
            JOIN research_stage_route route ON route.run_id=:run
            LEFT JOIN research_monitor_increments increment
              ON route.kind='increment' AND route.scope_id=increment.id
            LEFT JOIN manual_paper_requests request
              ON route.kind='independent' AND route.scope_id=request.id
            LEFT JOIN agent_paper_cycles cycle
              ON route.kind='formal' AND route.scope_id=cycle.id
            WHERE original.run_id=:original AND original.account_id=
              coalesce(increment.account_id,request.account_id,cycle.account_id)
        """),
        {"run": run_id, "original": RUN_ID},
    )
    if account_id is None:
        return False
    return await _require_account_send(session, account_id, fenced_transport, hold_original)


async def require_for_account_send(
    session: AsyncSession, account_id: UUID | None, *, fenced_transport: bool
) -> bool:
    if account_id is None or not await session.scalar(
        text("SELECT 1 FROM research_monitor_increments WHERE run_id=:run AND account_id=:account"),
        {"run": RUN_ID, "account": account_id},
    ):
        return False
    return await _require_account_send(session, account_id, fenced_transport, fenced_transport)


async def _require_account_send(
    session: AsyncSession, account_id: UUID, fenced_transport: bool, hold_original: bool
) -> bool:
    # The writer holds the account lock plus this narrower lock. Keeping
    # only the latter through headers leaves normal five-minute scans free.
    await _lock_disposition(session, account_id, shared=True)
    now = await session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(now, datetime):
        raise ValueError(BLOCKED)
    decision, revoked = await _decision(session, account_id, now)
    if decision is None:
        return False
    if not fenced_transport or revoked is not None:
        raise ValueError(BLOCKED)
    if hold_original:
        # Finish handlers require the original Job/lease locks. The shared
        # disposition lock also fences concurrent revocation and failure audit.
        await _lock_original(session)
    await _proof(session, account_id, now)
    return True
