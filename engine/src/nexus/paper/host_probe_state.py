"""Host intake state, ordered with admissions by the existing account transaction lock."""

from datetime import UTC, datetime, timedelta
from typing import Literal, NamedTuple
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.jobs.models import job_input_hash
from nexus.ops.pending_probe import STAGES, ReadProbeDeadline
from nexus.paper.manual_host_exit import valid_host_exit
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.runtime import IdempotencyRecord, WorkerHeartbeat

ROUTE = "agent-paper-host-read-probe-v1"
SCHEMA = "host_read_probe_v1"
DENY_SCHEMA = "host_read_probe_deny_v1"
MONITOR_SCHEMA = "host_monitor_only_v1"
MONITOR_REASON: Literal["RESEARCH_SETTLEMENT_HOLD"] = "RESEARCH_SETTLEMENT_HOLD"
DENY_REASON: Literal["READ_PROBE_PAUSE_UNCONFIRMED"] = "READ_PROBE_PAUSE_UNCONFIRMED"
INVALID = "HOST_READ_PROBE_STATE_INVALID"
PAUSED = "MANUAL_HOST_NOT_READY"


class HostSendBlocked(ValueError):
    """No new external call was authorized by the host's send boundary."""


class HostSendHeld(HostSendBlocked):
    def __init__(self, account_id: UUID, state: "ProbeState") -> None:
        super().__init__(PAUSED)
        self.account_id = account_id
        self.state_hash = job_input_hash(state.model_dump(mode="json"))
        self.sequence = state.total


_SEND_SCOPE = "agent-paper-host-send:"


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


class DeniedProbeState(ProbeState):
    reason: Literal["READ_PROBE_PAUSE_UNCONFIRMED"]
    operation_id: str
    recorded_at: str
    prior_hash: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    pause_outcome: Literal["rejected", "commit_unknown"]
    admitting: Literal[False]


class MonitorOnlyState(ProbeState):
    reason: Literal["RESEARCH_SETTLEMENT_HOLD"]
    operation_id: str
    recorded_at: str
    admitting: Literal[False]


class MonitorReplacement(NamedTuple):
    attempt: UUID
    worker: str
    version: str
    digest: str


def _actor(account_id: UUID) -> str:
    return f"host-read-probe:{account_id}"


def _identity(worker: str, started: datetime, version: str) -> dict[str, str]:
    return {"worker": worker, "started": started.astimezone(UTC).isoformat(), "version": version}


def _validated(record: IdempotencyRecord) -> ProbeState:
    try:
        if record.schema_version == SCHEMA:
            state = ProbeState.model_validate(record.response_payload)
        elif record.schema_version == DENY_SCHEMA:
            state = DeniedProbeState.model_validate(record.response_payload)
            if (
                state.consecutive == 0
                or str(UUID(state.operation_id)) != state.operation_id
                or datetime.fromisoformat(state.recorded_at).utcoffset() != timedelta()
                or datetime.fromisoformat(state.recorded_at) < datetime.fromisoformat(state.started)
                or record.response_payload.get("admitting") is not False
            ):
                raise ValueError(INVALID)
        elif record.schema_version == MONITOR_SCHEMA:
            state = MonitorOnlyState.model_validate(record.response_payload)
            if (
                state.consecutive != 1
                or state.total != 1
                or state.stage != "monitor_only_startup"
                or str(UUID(state.operation_id)) != state.operation_id
                or not state.worker.startswith(f"agent-paper-loop-{state.operation_id}:manual:")
                or datetime.fromisoformat(state.recorded_at).utcoffset() != timedelta()
                or datetime.fromisoformat(state.recorded_at) < datetime.fromisoformat(state.started)
                or record.response_payload.get("admitting") is not False
            ):
                raise ValueError(INVALID)
        else:
            raise ValueError(INVALID)
    except (TypeError, ValueError):
        raise ValueError(INVALID) from None
    if (
        state.worker != record.idempotency_key
        or (record.schema_version != MONITOR_SCHEMA and state.stage not in STAGES)
        or (record.schema_version == SCHEMA and state.reason != "READ_PROBE_DEADLINE")
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


async def monitor_only_origin_hash(
    session: AsyncSession,
    *,
    account_id: UUID,
    operation_id: UUID,
    worker: str,
    version: str,
    exited_at: datetime | None = None,
) -> str:
    """Read the old monitoring denial and its last heartbeat without granting recovery."""
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    if (
        await session.scalar(text("SELECT version_num FROM alembic_version"))
        != CURRENT_MIGRATION_HEAD
    ):
        raise ValueError(INVALID)
    state = await latest_probe_state(session, account_id)
    if (
        not isinstance(state, MonitorOnlyState)
        or state.operation_id != str(operation_id)
        or state.worker != worker
        or state.version != version
    ):
        raise ValueError(INVALID)
    heartbeat = await session.scalar(
        select(WorkerHeartbeat).where(
            WorkerHeartbeat.queue == "agent_paper",
            WorkerHeartbeat.worker_id == worker,
        )
    )
    now = await session.scalar(select(func.clock_timestamp()))
    if (
        heartbeat is None
        or not isinstance(now, datetime)
        or heartbeat.started_at != datetime.fromisoformat(state.started)
        or heartbeat.process_version != state.version
        or heartbeat.migration_head != CURRENT_MIGRATION_HEAD
        or not heartbeat.started_at <= heartbeat.seen_at <= now
        or exited_at is not None
        and heartbeat.seen_at > exited_at
    ):
        raise ValueError(INVALID)
    outstanding_capture = await session.scalar(
        text("""
            SELECT EXISTS (
                SELECT FROM audit_events claim
                WHERE claim.action = 'research.monitor.disclosure_refresh_claimed'
                  AND claim.payload->>'account_id' = :account
                  AND claim.occurred_at >= :started
                  AND NOT EXISTS (
                      SELECT FROM audit_events result
                      WHERE result.action = 'research.monitor.disclosure_refresh_result'
                        AND result.payload->>'account_id' = :account
                        AND result.payload->>'operation_id' = claim.payload->>'operation_id'
                        AND result.occurred_at >= claim.occurred_at
                  )
            )
            """),
        {"account": str(account_id), "started": heartbeat.started_at},
    )
    if (
        outstanding_capture is not False
        or await session.scalar(
            text(
                "SELECT EXISTS (SELECT FROM outbox_delivery_attempts WHERE outcome = 'delivering')"
            )
        )
        is not False
    ):
        raise ValueError(INVALID)
    return job_input_hash(
        {
            "denial": state.model_dump(mode="json"),
            "last_heartbeat": heartbeat.seen_at.astimezone(UTC).isoformat(),
        }
    )


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


async def require_host_send(session: AsyncSession, account_id: UUID) -> None:
    """Retain a shared hold through send submission without the portfolio lock."""
    if (
        await session.scalar(text("SHOW transaction_isolation")) != "read committed"
        or await session.scalar(
            text("SELECT pg_try_advisory_xact_lock_shared(hashtextextended(:scope, 0))"),
            {"scope": _SEND_SCOPE + str(account_id)},
        )
        is not True
    ):
        raise HostSendBlocked(INVALID)
    await check_host_send_state(session, account_id)


async def check_host_send_state(session: AsyncSession, account_id: UUID) -> None:
    """Recheck the current marker after acquiring the shared send fence."""
    try:
        state = await latest_probe_state(session, account_id)
    except ValueError as error:
        raise HostSendBlocked(INVALID) from error
    if state is not None and state.consecutive:
        raise HostSendHeld(account_id, state)


async def _verified_host(
    session: AsyncSession, *, worker: str, started: datetime, version: str
) -> datetime:
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
    return now


async def publish_probe_state(
    session: AsyncSession,
    *,
    account_id: UUID,
    worker: str,
    started: datetime,
    version: str,
    deadline: ReadProbeDeadline | None,
) -> ProbeState:
    from nexus.ops.monitor_probe_idle import require_monitor_probe_idle

    await lock_portfolio_account(session, account_id)
    await require_monitor_probe_idle(session, account_id)
    await _verified_host(session, worker=worker, started=started, version=version)
    old = await latest_probe_state(session, account_id)
    if isinstance(old, DeniedProbeState | MonitorOnlyState):
        # A real pause or resume cannot overwrite a deny-only witness.
        raise ValueError(INVALID)
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
    await _store_state(session, account_id, state, SCHEMA, same=same, expected=old)
    return state


async def publish_monitor_only_start(
    session: AsyncSession,
    *,
    account_id: UUID,
    worker: str,
    started: datetime,
    version: str,
    operation_id: UUID,
    replacement: MonitorReplacement | None = None,
) -> MonitorOnlyState:
    """Record the capability ceiling in the same transaction as the first heartbeat."""
    from nexus.agents.research_usage import require_research_accounting_clear
    from nexus.backtest.universe import universe_definition_parse_scope
    from nexus.market.current_identity import identity_parse_scope
    from nexus.paper.history_seal import terminal_proof_read_scope
    from nexus.paper.manual_request import inspect_recovery_terminal_holds
    from nexus.research.artifact_read_scope import artifact_read_scope
    from nexus.research.identity_sources import identity_source_parse_scope
    from nexus.workbench.evidence_read_scope import evidence_read_scope

    await lock_portfolio_account(session, account_id)
    now = await _verified_host(session, worker=worker, started=started, version=version)
    if (
        replacement is not None
        and await monitor_only_origin_hash(
            session,
            account_id=account_id,
            operation_id=replacement.attempt,
            worker=replacement.worker,
            version=replacement.version,
        )
        != replacement.digest
    ):
        raise ValueError(INVALID)
    prior = await latest_probe_state(session, account_id)
    if prior is not None and (
        datetime.fromisoformat(prior.started) >= started
        or (prior.consecutive and not isinstance(prior, MonitorOnlyState))
    ):
        raise ValueError(INVALID)
    with (
        terminal_proof_read_scope(session),
        identity_parse_scope(),
        identity_source_parse_scope(),
        artifact_read_scope(session),
        evidence_read_scope(session),
        universe_definition_parse_scope(),
    ):
        if not await inspect_recovery_terminal_holds(
            session, account_id=account_id, now=now, read_only=False
        ):
            raise ValueError(INVALID)
    await require_research_accounting_clear(session)
    state = MonitorOnlyState(
        **_identity(worker, started, version),
        consecutive=1,
        total=1,
        reason=MONITOR_REASON,
        stage="monitor_only_startup",
        stage_ms=0,
        elapsed_ms=0,
        operation_id=str(operation_id),
        recorded_at=now.astimezone(UTC).isoformat(),
        admitting=False,
    )
    await _store_state(session, account_id, state, MONITOR_SCHEMA, same=False, expected=prior)
    return state


async def require_monitor_only_capture(
    session: AsyncSession, account_id: UUID, worker: str
) -> None:
    await lock_portfolio_account(session, account_id)
    state = await latest_probe_state(session, account_id)
    if not isinstance(state, MonitorOnlyState) or state.worker != worker:
        raise ValueError(PAUSED)
    await verify_monitor_only_binding(session, state)


async def verify_monitor_only_binding(session: AsyncSession, state: MonitorOnlyState) -> None:
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    if (
        await session.scalar(text("SELECT version_num FROM alembic_version"))
        != CURRENT_MIGRATION_HEAD
    ):
        raise ValueError(PAUSED)
    await _verified_host(
        session,
        worker=state.worker,
        started=datetime.fromisoformat(state.started),
        version=state.version,
    )


async def publish_and_confirm_probe_state(
    factory: async_sessionmaker[AsyncSession],
    *,
    account_id: UUID,
    worker: str,
    started: datetime,
    version: str,
    deadline: ReadProbeDeadline | None,
    allow_denial: bool = False,
) -> ProbeState | None:
    """Return the verified durable state only when publication needed safe recovery."""
    expected = None
    previous = None
    previous_read = False
    try:
        async with factory.begin() as session:
            if deadline is not None and allow_denial:
                await lock_portfolio_account(session, account_id)
                previous = await latest_probe_state(session, account_id)
                previous_read = True
            expected = await publish_probe_state(
                session,
                account_id=account_id,
                worker=worker,
                started=started,
                version=version,
                deadline=deadline,
            )
    except (SQLAlchemyError, OSError, TimeoutError, RuntimeError, ValueError) as error:
        if deadline is None or (expected is None and not previous_read):
            raise
        async with factory.begin() as reader:
            await reader.execute(text("SET TRANSACTION READ ONLY"))
            observed = await latest_probe_state(reader, account_id)
        if expected is not None and observed == expected:
            return expected
        if not previous_read or not allow_denial or observed != previous:
            raise
        denial = None
        try:
            async with factory.begin() as session:
                denial = await publish_probe_denial(
                    session,
                    account_id=account_id,
                    worker=worker,
                    started=started,
                    version=version,
                    previous=previous,
                    deadline=deadline,
                    operation_id=uuid4(),
                    pause_outcome="rejected"
                    if isinstance(error, ValueError) and error.args == (INVALID,)
                    else "commit_unknown",
                )
        except (SQLAlchemyError, OSError, TimeoutError, RuntimeError, ValueError):
            if denial is None:
                raise
        async with factory.begin() as reader:
            await reader.execute(text("SET TRANSACTION READ ONLY"))
            if await latest_probe_state(reader, account_id) != denial:
                raise ValueError(INVALID)
        return denial
    return None


async def _store_state(
    session: AsyncSession,
    account_id: UUID,
    state: ProbeState,
    schema: str,
    *,
    same: bool,
    expected: ProbeState | None = None,
) -> None:
    # Account writers acquire this last. Publishers holding the shared lock do
    # not take the account lock or run provider cleanup inside the writer path.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": _SEND_SCOPE + str(account_id)},
    )
    if await latest_probe_state(session, account_id) != expected:
        raise ValueError(INVALID)
    identity = state.model_dump(include={"worker", "started", "version"})
    worker = state.worker
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
            schema_version=schema,
        )
        session.add(record)
    elif not same:
        raise ValueError(INVALID)
    record.schema_version = schema
    record.response_payload = state.model_dump()
    _validated(record)


async def publish_probe_denial(
    session: AsyncSession,
    *,
    account_id: UUID,
    worker: str,
    started: datetime,
    version: str,
    previous: ProbeState | None,
    deadline: ReadProbeDeadline,
    operation_id: UUID,
    pause_outcome: Literal["rejected", "commit_unknown"],
) -> DeniedProbeState:
    """Persist denial under the existing lock without claiming monitoring is idle."""
    await lock_portfolio_account(session, account_id)
    now = await _verified_host(session, worker=worker, started=started, version=version)
    old = await latest_probe_state(session, account_id)
    if old != previous or isinstance(old, DeniedProbeState):
        raise ValueError(INVALID)
    identity = _identity(worker, started, version)
    same = old is not None and old.model_dump(include=set(identity)) == identity
    if old is not None and not same and datetime.fromisoformat(old.started) >= started:
        raise ValueError(INVALID)
    state = DeniedProbeState(
        **identity,
        consecutive=(old.consecutive if same and old else 0) + 1,
        total=(old.total if same and old else 0) + 1,
        reason=DENY_REASON,
        stage=deadline.stage,
        stage_ms=deadline.stage_ms,
        elapsed_ms=deadline.elapsed_ms,
        operation_id=str(operation_id),
        recorded_at=now.astimezone(UTC).isoformat(),
        prior_hash=job_input_hash(old.model_dump()) if old is not None else None,
        pause_outcome=pause_outcome,
        admitting=False,
    )
    await _store_state(session, account_id, state, DENY_SCHEMA, same=same, expected=old)
    return state
