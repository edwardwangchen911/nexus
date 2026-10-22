"""Exact local host retirement, independent of Job/lease/usage admission."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.persistence.models.runtime import ManualHostExit, WorkerHeartbeat


def valid_host_exit() -> ColumnElement[bool]:
    """Correlated predicate shared by live admission and read-only status."""
    return exists(
        select(ManualHostExit.heartbeat_id).where(
            ManualHostExit.heartbeat_id == WorkerHeartbeat.id,
            ManualHostExit.queue == WorkerHeartbeat.queue,
            ManualHostExit.worker_id == WorkerHeartbeat.worker_id,
            ManualHostExit.started_at == WorkerHeartbeat.started_at,
            ManualHostExit.process_version == WorkerHeartbeat.process_version,
            ManualHostExit.migration_head == WorkerHeartbeat.migration_head,
            ManualHostExit.final_seen_at == WorkerHeartbeat.seen_at,
            ManualHostExit.ended_at >= WorkerHeartbeat.seen_at,
        )
    )


async def record_host_exit(
    session: AsyncSession,
    *,
    worker_id: str,
    started_at: datetime,
    process_version: str,
) -> ManualHostExit:
    host = await session.scalar(
        select(WorkerHeartbeat)
        .where(
            WorkerHeartbeat.queue == "agent_paper",
            WorkerHeartbeat.worker_id == worker_id,
            WorkerHeartbeat.started_at == started_at,
            WorkerHeartbeat.process_version == process_version,
        )
        .with_for_update()
    )
    if host is None:
        raise ValueError("MANUAL_HOST_EXIT_UNCONFIRMED")
    previous = await session.get(ManualHostExit, host.id)
    if previous is not None:
        if not await session.scalar(
            select(valid_host_exit())
            .select_from(WorkerHeartbeat)
            .where(WorkerHeartbeat.id == host.id)
        ):
            raise ValueError("MANUAL_HOST_EXIT_UNCONFIRMED")
        return previous
    record = ManualHostExit(
        heartbeat_id=host.id,
        queue=host.queue,
        worker_id=host.worker_id,
        started_at=host.started_at,
        process_version=host.process_version,
        migration_head=host.migration_head,
        final_seen_at=host.seen_at,
    )
    session.add(record)
    await session.flush()
    return record


@dataclass
class HostDrain:
    confirmed: bool = True


_DRAIN: ContextVar[HostDrain | None] = ContextVar("manual_host_drain", default=None)


@contextmanager
def host_drain_scope() -> Iterator[HostDrain]:
    state = HostDrain()
    token = _DRAIN.set(state)
    try:
        yield state
    finally:
        _DRAIN.reset(token)


def mark_host_drain_unconfirmed() -> None:
    state = _DRAIN.get()
    if state is not None:
        state.confirmed = False
