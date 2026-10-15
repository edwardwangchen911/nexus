"""Current manual producer facts and bounded, non-replaying preparation recovery.

The existing append-only audit chain is the execution journal. A committed
started record precedes any possible external effect; the effects marker is
committed before entering the trigger (including refresh and source cleanup).
Missing markers on legacy requests never prove that nothing was called.
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.paper.history_seal import _row_hash
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job, JobLease

PREFIX = "paper.manual_preparation."
STARTED = PREFIX + "execution_started"
EFFECTS = PREFIX + "effects_possible"
RECOVERED = PREFIX + "recovered"
SCHEMA = "manual_preparation_execution_v1"
ACTOR = "local-host:manual-preparation"
MAINTAINER = "local-user:manual-preparation-recovery"
BLOCKED = "MANUAL_PREPARATION_RECOVERY_BLOCKED"


class PreparationRecoveryView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    disposition: Literal["cancelled_before_call", "unknown_preserved", "cycle_reconciled"]
    external_effects: Literal["not_started", "unknown", "cycle_recorded"]
    cycle_id: UUID | None = None


class PreparationRecoveryReceipt(PreparationRecoveryView):
    request_id: UUID
    replayed: bool
    execution_locked: Literal[True] = True


async def _now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError(BLOCKED)
    return now


async def _verify(session: AsyncSession, event: AuditEvent, now: datetime) -> None:
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == event.event_seq - 1)
    )
    if (
        event.schema_version != SCHEMA
        or event.occurred_at > now
        or event.actor_id != (MAINTAINER if event.action == RECOVERED else ACTOR)
        or event.chain_version != "audit_jcs_sha256_v1"
        or event.canonical_event != canonical
        or event.previous_hash != ("0" * 64 if event.event_seq == 1 else previous)
        or event.event_hash != audit_event_hash(event.previous_hash, canonical)
    ):
        raise ValueError(BLOCKED)


async def _journal(
    session: AsyncSession, row: ManualPaperRequest, now: datetime
) -> list[AuditEvent]:
    events = list(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action.in_([STARTED, EFFECTS, RECOVERED]),
                AuditEvent.payload["request_id"].astext == str(row.id),
            )
            .order_by(AuditEvent.event_seq)
        )
    )
    for event in events:
        await _verify(session, event, now)
        if event.payload.get("account_id") != str(row.account_id):
            raise ValueError(BLOCKED)
    actions = [e.action for e in events]
    if actions not in (
        [],
        [STARTED],
        [STARTED, EFFECTS],
        [STARTED, RECOVERED],
        [STARTED, EFFECTS, RECOVERED],
    ):
        raise ValueError(BLOCKED)
    if events:
        start = events[0]
        preparing = await session.scalar(
            select(ManualPaperRequestEvent).where(
                ManualPaperRequestEvent.request_id == row.id,
                ManualPaperRequestEvent.status == "preparing",
            )
        )
        pid = start.payload.get("producer_pid")
        if (
            start.payload.get("request_hash") != row.request_hash
            or preparing is None
            or start.payload.get("preparing_hash") != preparing.event_hash
            or start.occurred_at < preparing.occurred_at
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not re.fullmatch(r"[0-9a-f]{40}", str(start.payload.get("producer_revision")))
        ):
            raise ValueError(BLOCKED)
        if any(
            e.payload.get("execution_id") != str(start.id) or e.occurred_at < start.occurred_at
            for e in events[1:]
        ):
            raise ValueError(BLOCKED)
    return events


async def start_preparation_execution(
    session: AsyncSession, *, request_id: UUID, revision: str | None
) -> UUID:
    from nexus.paper.manual_request import read_manual_request

    row = await session.get(ManualPaperRequest, request_id)
    if row is None or revision is None or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError(BLOCKED)
    await lock_portfolio_account(session, row.account_id)
    now = await _now(session)
    if (await read_manual_request(session, row)).status != "preparing" or await _journal(
        session, row, now
    ):
        raise ValueError(BLOCKED)
    preparing = await session.scalar(
        select(ManualPaperRequestEvent).where(
            ManualPaperRequestEvent.request_id == row.id,
            ManualPaperRequestEvent.status == "preparing",
        )
    )
    assert preparing is not None
    event = await append_audit_event(
        session,
        actor_id=ACTOR,
        action=STARTED,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "request_id": str(row.id),
            "account_id": str(row.account_id),
            "request_hash": row.request_hash,
            "preparing_hash": preparing.event_hash,
            "producer_revision": revision,
            "producer_pid": os.getpid(),
        },
    )
    return event.id


async def begin_preparation_effects(
    session: AsyncSession, *, request_id: UUID, execution_id: UUID
) -> None:
    from nexus.paper.manual_request import read_manual_request

    row = await session.get(ManualPaperRequest, request_id)
    if row is None:
        raise ValueError(BLOCKED)
    await lock_portfolio_account(session, row.account_id)
    now = await _now(session)
    events = await _journal(session, row, now)
    if (
        len(events) != 1
        or events[0].id != execution_id
        or events[0].payload["producer_pid"] != os.getpid()
        or (await read_manual_request(session, row)).status != "preparing"
    ):
        raise ValueError(BLOCKED)
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=EFFECTS,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "request_id": str(row.id),
            "account_id": str(row.account_id),
            "execution_id": str(execution_id),
        },
    )


async def require_preparation_execution(
    session: AsyncSession, row: ManualPaperRequest, *, execution_id: UUID | None
) -> None:
    """Fence both native reservation and cycle creation, under the same account lock."""
    await lock_portfolio_account(session, row.account_id)
    events = await _journal(session, row, await _now(session))
    if not events and execution_id is None:
        return  # Existing legacy requests retain their original admission rules.
    if (
        len(events) != 2
        or events[-1].action != EFFECTS
        or events[0].id != execution_id
        or events[0].payload["producer_pid"] != os.getpid()
    ):
        raise ValueError(BLOCKED)


def _require_execution_producer_stopped(start: AuditEvent, root: Path, env: Path) -> None:
    from nexus.paper.manual_preparation_close import require_preparation_producers_stopped

    require_preparation_producers_stopped(root, env)
    pid = start.payload.get("producer_pid")
    if not isinstance(pid, int) or isinstance(pid, bool):
        raise ValueError(BLOCKED)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    except (OSError, ValueError, TypeError):
        raise ValueError(BLOCKED) from None
    # PID reuse is conservatively blocked too. Stale heartbeats are never proof of exit.
    raise ValueError(BLOCKED)


async def _bindings(
    session: AsyncSession, row: ManualPaperRequest
) -> tuple[list[IdempotencyRecord], list[UUID]]:
    claims = list(
        await session.scalars(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == "local-agent-paper-schwab:" + str(row.account_id),
                IdempotencyRecord.idempotency_key.in_([str(row.id), "manual-entry:" + str(row.id)]),
            )
        )
    )
    cycles = list(
        await session.scalars(
            select(AgentPaperCycleEventRecord.cycle_id).where(
                AgentPaperCycleEventRecord.sequence == 1,
                AgentPaperCycleEventRecord.fact_ids["manual_request_id"].astext == str(row.id),
            )
        )
    )
    if len(claims) > 1 or len(cycles) > 1:
        raise ValueError(BLOCKED)
    return claims, cycles


async def _require_idle_consumers(
    session: AsyncSession, row: ManualPaperRequest, now: datetime
) -> None:
    # Maintenance shares the account lock with producers. Running research or an
    # unexpired consumer fence is not equivalent to a stopped host process.
    kinds = ("agent.research", "agent.paper.research", "agent.portfolio_synthesis")
    jobs = list(
        await session.scalars(
            select(Job)
            .where(Job.job_type.in_(kinds), Job.status.in_(["queued", "running"]))
            .order_by(Job.id)
            .with_for_update()
        )
    )
    leased = await session.scalar(
        select(JobLease.job_id)
        .join(Job, Job.id == JobLease.job_id)
        .where(Job.job_type.in_(kinds), JobLease.expires_at > now)
        .limit(1)
    )
    if any(job.status == "running" for job in jobs) or leased is not None:
        raise ValueError(BLOCKED)
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import require_activation_projection_unoccupied

    projection = await load_paper_projection(session, account_id=row.account_id, as_of=now)
    await require_activation_projection_unoccupied(
        session, account_id=row.account_id, projection=projection
    )


async def _require_other_claims_settled(
    session: AsyncSession, row: ManualPaperRequest, now: datetime
) -> None:
    from nexus.paper.preparation_unknown_ack import _audits, _claim_ids, _terminal_claim, _validate

    audits = await _audits(session, row.account_id)
    allowed = set()
    if audits:
        if len(audits) != 1:
            raise ValueError(BLOCKED)
        allowed = set(_claim_ids(await _validate(session, audits[0], now=now)))
    claims = await session.scalars(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == "local-agent-paper-schwab:" + str(row.account_id)
        )
    )
    for claim in claims:
        if claim.id not in allowed and not await _terminal_claim(
            session, claim, account_id=row.account_id
        ):
            raise ValueError(BLOCKED)


async def _require_execution_cycle_binding(
    session: AsyncSession, row: ManualPaperRequest, cycle_id: UUID, execution_id: UUID
) -> None:
    from nexus.paper.agent_cycle_models import canonical_cycle_event_hash
    from nexus.paper.manual_request import _require_manual_cycle_binding

    await _require_manual_cycle_binding(session, row, cycle_id)
    first = await session.scalar(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == cycle_id,
            AgentPaperCycleEventRecord.sequence == 1,
        )
    )
    if (
        first is None
        or first.fact_ids.get("manual_execution_id") != str(execution_id)
        or first.from_status is not None
        or first.to_status != "prepared"
        or first.result_code != "CYCLE_PREPARED"
    ):
        raise ValueError(BLOCKED)
    if first.event_hash != canonical_cycle_event_hash(
        cycle_id=first.cycle_id,
        sequence=first.sequence,
        from_status=None,
        to_status="prepared",
        result_code=first.result_code,
        fact_ids=first.fact_ids,
        occurred_at=first.occurred_at,
    ):
        raise ValueError(BLOCKED)


async def read_preparation_recovery(
    session: AsyncSession, row: ManualPaperRequest
) -> PreparationRecoveryView | None:
    try:
        return await _read_preparation_recovery(session, row)
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError(BLOCKED) from None


async def _read_preparation_recovery(
    session: AsyncSession, row: ManualPaperRequest
) -> PreparationRecoveryView | None:
    from nexus.paper.manual_request import read_manual_request

    events = await _journal(session, row, await _now(session))
    if not events or events[-1].action != RECOVERED:
        return None
    audit = events[-1]
    view = await read_manual_request(session, row)
    anchor = await session.scalar(
        select(ManualPaperRequestEvent).where(
            ManualPaperRequestEvent.request_id == row.id,
            ManualPaperRequestEvent.event_hash == audit.payload.get("request_event_hash"),
        )
    )
    if anchor is None or anchor.occurred_at > audit.occurred_at:
        raise ValueError(BLOCKED)
    result = PreparationRecoveryView.model_validate(audit.payload["result"])
    claims, cycles = await _bindings(session, row)
    if result.disposition == "cancelled_before_call":
        if (
            len(events) != 2
            or claims
            or cycles
            or view.status != "unavailable"
            or view.code != "MANUAL_PREPARATION_CANCELLED"
            or anchor.status != view.status
            or anchor.code != view.code
            or anchor.cycle_id is not None
            or result.external_effects != "not_started"
            or result.cycle_id is not None
        ):
            raise ValueError(BLOCKED)
    elif result.disposition == "unknown_preserved":
        if (
            cycles
            or view.status != "unknown"
            or anchor.status != "unknown"
            or anchor.code != "MANUAL_RESULT_UNKNOWN"
            or anchor.cycle_id is not None
            or audit.payload.get("claim_hashes") != [_row_hash(c) for c in claims]
            or result.external_effects != "unknown"
            or result.cycle_id
        ):
            raise ValueError(BLOCKED)
    else:
        if (
            result.cycle_id is None
            or cycles != [result.cycle_id]
            or result.external_effects != "cycle_recorded"
            or anchor.status not in {"active", "unknown"}
            or (
                anchor.status == "active"
                and (anchor.code != "MANUAL_CYCLE_CREATED" or anchor.cycle_id != result.cycle_id)
            )
            or (
                anchor.status == "unknown"
                and (
                    anchor.code != "MANUAL_RESULT_UNKNOWN"
                    or anchor.cycle_id not in {None, result.cycle_id}
                )
            )
        ):
            raise ValueError(BLOCKED)
        await _require_execution_cycle_binding(session, row, result.cycle_id, events[0].id)
    return result


async def recover_current_preparation(
    session: AsyncSession, *, account_id: UUID, request_id: UUID, repo_root: Path, env_file: Path
) -> PreparationRecoveryReceipt:
    from nexus.paper.manual_request import (
        read_manual_request,
        record_manual_request_event,
        require_manual_account_available,
    )

    await lock_portfolio_account(session, account_id)
    row = await session.get(ManualPaperRequest, request_id, with_for_update=True)
    if row is None or row.account_id != account_id:
        raise ValueError(BLOCKED)
    now = await _now(session)
    existing = await read_preparation_recovery(session, row)
    if existing:
        return PreparationRecoveryReceipt(**existing.model_dump(), request_id=row.id, replayed=True)
    events = await _journal(session, row, now)
    if not events:
        raise ValueError(BLOCKED)
    _require_execution_producer_stopped(events[0], repo_root, env_file)
    view = await read_manual_request(session, row)
    if view.status not in {"preparing", "unknown", "active"}:
        raise ValueError(BLOCKED)
    claims, cycles = await _bindings(session, row)
    await _require_idle_consumers(session, row, now)
    if cycles:
        # An existing cycle is never hidden by a pre-cycle cancellation.
        await _require_execution_cycle_binding(session, row, cycles[0], events[0].id)
        if view.status == "preparing":
            await record_manual_request_event(
                session, row, status="active", code="MANUAL_CYCLE_CREATED", cycle_id=cycles[0]
            )
        result = PreparationRecoveryView(
            disposition="cycle_reconciled", external_effects="cycle_recorded", cycle_id=cycles[0]
        )
    elif len(events) == 1 and not claims and view.status == "preparing":
        await _require_other_claims_settled(session, row, now)
        await require_manual_account_available(
            session, account_id=account_id, now=now, excluding_request_id=request_id
        )
        await record_manual_request_event(
            session, row, status="unavailable", code="MANUAL_PREPARATION_CANCELLED"
        )
        result = PreparationRecoveryView(
            disposition="cancelled_before_call", external_effects="not_started"
        )
    else:
        if view.status == "active":
            raise ValueError(BLOCKED)
        if view.status == "preparing":
            await record_manual_request_event(
                session, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
            )
        result = PreparationRecoveryView(
            disposition="unknown_preserved", external_effects="unknown"
        )
    last = await session.scalar(
        select(ManualPaperRequestEvent)
        .where(ManualPaperRequestEvent.request_id == row.id)
        .order_by(ManualPaperRequestEvent.sequence.desc())
        .limit(1)
    )
    assert last is not None
    now = await _now(session)
    await append_audit_event(
        session,
        actor_id=MAINTAINER,
        action=RECOVERED,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={
            "request_id": str(row.id),
            "account_id": str(row.account_id),
            "execution_id": str(events[0].id),
            "request_event_hash": last.event_hash,
            "claim_hashes": [_row_hash(c) for c in claims],
            "result": result.model_dump(mode="json"),
        },
    )
    _require_execution_producer_stopped(events[0], repo_root, env_file)
    if await read_preparation_recovery(session, row) != result:
        raise ValueError(BLOCKED)
    return PreparationRecoveryReceipt(**result.model_dump(), request_id=row.id, replayed=False)
