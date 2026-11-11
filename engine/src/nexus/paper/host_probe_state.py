"""Host intake state, ordered with admissions by the existing account transaction lock."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.models import job_input_hash
from nexus.ops.pending_probe import STAGES, ReadProbeDeadline
from nexus.paper.manual_host_exit import valid_host_exit
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.runtime import IdempotencyRecord, WorkerHeartbeat

ROUTE = "agent-paper-host-read-probe-v1"
SCHEMA = "host_read_probe_v1"
INVALID = "HOST_READ_PROBE_STATE_INVALID"
PAUSED = "MANUAL_HOST_NOT_READY"


class ProbeState(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    worker: str
    started: str
    version: str
    consecutive: int = Field(ge=0)
    total: int = Field(ge=0)
    stage: str
    stage_ms: int = Field(ge=0)
    elapsed_ms: int = Field(ge=0)
    reason: str


def _actor(account_id: UUID) -> str:
    return f"host-read-probe:{account_id}"


def _identity(worker: str, started: datetime, version: str) -> dict[str, str]:
    return {"worker": worker, "started": started.astimezone(UTC).isoformat(), "version": version}


def _validated(record: IdempotencyRecord) -> ProbeState:
    state = ProbeState.model_validate(record.response_payload)
    if (
        record.schema_version != SCHEMA
        or state.worker != record.idempotency_key
        or state.stage not in STAGES
        or state.reason != "READ_PROBE_DEADLINE"
        or state.total < state.consecutive
        or state.stage_ms > state.elapsed_ms
        or record.request_hash
        != job_input_hash(
            {"actor": record.actor_id, **state.model_dump(include={"worker", "started", "version"})}
        )
    ):
        raise ValueError(INVALID)
    return state


async def latest_probe_state(session: AsyncSession, account_id: UUID) -> ProbeState | None:
    row = await session.scalar(
        select(IdempotencyRecord)
        .where(IdempotencyRecord.actor_id == _actor(account_id), IdempotencyRecord.route == ROUTE)
        .order_by(IdempotencyRecord.created_at.desc(), IdempotencyRecord.id.desc())
        .limit(1)
        .execution_options(populate_existing=True)
    )
    return None if row is None else _validated(row)


async def require_host_admitting(session: AsyncSession, account_id: UUID) -> None:
    """Caller holds the same account lock as publication, or is a read-only projection."""
    isolation, readonly = (
        await session.execute(
            text(
                "SELECT current_setting('transaction_isolation'), "
                "current_setting('transaction_read_only')"
            )
        )
    ).one()
    if readonly != "on":
        if isolation != "read committed":
            raise ValueError(INVALID)
        await lock_portfolio_account(session, account_id)
    state = await latest_probe_state(session, account_id)
    if state is not None and state.consecutive:
        raise ValueError(PAUSED)


async def publish_probe_state(
    session: AsyncSession,
    *,
    account_id: UUID,
    worker: str,
    started: datetime,
    version: str,
    deadline: ReadProbeDeadline | None,
) -> None:
    from nexus.ops.monitor_probe_idle import require_monitor_probe_idle

    await lock_portfolio_account(session, account_id)
    await require_monitor_probe_idle(session, account_id)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError(INVALID)
    live = (
        await session.scalars(
            select(WorkerHeartbeat)
            .where(
                WorkerHeartbeat.queue == "agent_paper",
                ~valid_host_exit(),
                WorkerHeartbeat.seen_at >= now - timedelta(seconds=30),
            )
            .limit(2)
        )
    ).all()
    if len(live) != 1:
        raise ValueError(INVALID)
    heartbeat = live[0]
    if (
        heartbeat.worker_id != worker
        or heartbeat.started_at != started
        or heartbeat.process_version != version
        or heartbeat.seen_at > now
    ):
        raise ValueError(INVALID)
    old = await latest_probe_state(session, account_id)
    identity = _identity(worker, started, version)
    same = old is not None and old.model_dump(include=set(identity)) == identity
    if old is not None and not same and datetime.fromisoformat(old.started) >= started:
        raise ValueError(INVALID)
    if deadline is None and (old is None or not old.consecutive):
        raise ValueError(INVALID)
    if deadline is not None:
        diagnostic = {
            "reason": "READ_PROBE_DEADLINE",
            "stage": deadline.stage,
            "stage_ms": deadline.stage_ms,
            "elapsed_ms": deadline.elapsed_ms,
        }
    else:
        assert old is not None
        diagnostic = old.model_dump(include={"reason", "stage", "stage_ms", "elapsed_ms"})
    state = ProbeState.model_validate(
        {
            **identity,
            **diagnostic,
            "total": (old.total if same and old else 0) + int(deadline is not None),
            "consecutive": ((old.consecutive if same and old else 0) + 1) if deadline else 0,
        }
    )
    actor = _actor(account_id)
    record = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == actor,
            IdempotencyRecord.route == ROUTE,
            IdempotencyRecord.idempotency_key == worker,
        )
    )
    if record is None:
        record = IdempotencyRecord(
            actor_id=actor,
            route=ROUTE,
            idempotency_key=worker,
            request_hash=job_input_hash({"actor": actor, **identity}),
            schema_version=SCHEMA,
        )
        session.add(record)
    elif not same:
        raise ValueError(INVALID)
    record.response_payload = state.model_dump()
    _validated(record)
