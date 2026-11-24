"""Managed SEC source capture under a durable denial of paid work."""

import asyncio
import json
import subprocess
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic_ns
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select, text

from nexus.jobs.models import QueueName
from nexus.jobs.repository import upsert_worker_heartbeat
from nexus.monitoring.host import PriceMonitorCoordinator
from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
from nexus.ops.preparation_diagnostics import exception_code
from nexus.paper.host_probe_state import (
    MonitorReplacement,
    latest_probe_state,
    publish_monitor_only_start,
)
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

if TYPE_CHECKING:
    from nexus.ops.agent_paper_loop import AgentPaperLoopAction


def _report_pulse_failure(phase: str, started: int, error: Exception) -> None:
    try:
        code = (
            "DEPENDENCY_TIMEOUT"
            if isinstance(error, subprocess.TimeoutExpired)
            else exception_code(error)
        )
        if type(error) is ValueError and error.args in (
            ("AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED",),
            ("MONITOR_HOST_SCOPE_CHANGED",),
            ("MANUAL_HOST_NOT_READY",),
            ("HOST_READ_PROBE_STATE_INVALID",),
            ("FORMAL_BUDGET_APPLY_REQUIRED",),
            ("FORMAL_BUDGET_BINDING_INVALID",),
        ):
            code = error.args[0]
        print(
            json.dumps(
                {
                    "event": "agent_paper.monitor_only.pulse_failed",
                    "observed_at": datetime.now(UTC).isoformat(),
                    "phase": phase,
                    "phase_elapsed_ms": (monotonic_ns() - started) // 1_000_000,
                    "code": code,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
    except Exception:
        # Diagnostics cannot replace the original failure or authorize a pulse.
        return


async def run_monitor_only(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None,
    operation_id: UUID | None,
    verify_activation: Callable[[], None] | None,
    started_at: datetime,
    clock: Callable[[], datetime],
    shutdown: asyncio.Event,
    wait_sleep: Callable[[float], Awaitable[None]],
    replacement: MonitorReplacement | None = None,
) -> "AgentPaperLoopAction":
    from nexus.ops.agent_paper_loop import (
        AgentPaperLoopAction,
        agent_paper_host_identity,
        heartbeat_agent_paper_loop,
    )
    from nexus.ops.research_monitor import load_host_settings

    if operation_id is None or verify_activation is None or env_file is None:
        raise ValueError("AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED")

    async def runtime_settings() -> tuple[EngineSettings, dict[str, str]]:
        environment: dict[str, str] = {}
        current = await load_host_settings(
            settings,
            account_id=account_id,
            calendar_authority_id=calendar_authority_id,
            repo_root=repo_root,
            env_file=env_file,
            private_context=environment,
        )
        if (
            not current.agent_paper_cycle_enabled
            or manual_host_scope(current) != manual_host_scope(settings)
            or current.agent_daily_budget_usd != settings.agent_daily_budget_usd
        ):
            raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
        return current, environment

    async with asyncio.timeout(15):
        await asyncio.to_thread(verify_activation)
        await runtime_settings()
    worker_id = f"agent-paper-loop-{operation_id}"
    worker, version = agent_paper_host_identity(
        settings, worker_id=worker_id, account_id=account_id
    )
    factory = session_factory(settings)
    expected = None
    first_seen_at = clock()
    try:
        try:
            async with asyncio.timeout(15), factory.begin() as db:
                head = await db.scalar(text("SELECT version_num FROM alembic_version"))
                if head != CURRENT_MIGRATION_HEAD:
                    raise ValueError("AGENT_PAPER_RECOVERY_SCHEMA_MISMATCH")
                await lock_portfolio_account(db, account_id)
                await upsert_worker_heartbeat(
                    db,
                    queue=QueueName.AGENT_PAPER,
                    worker_id=worker,
                    process_version=version,
                    migration_head=CURRENT_MIGRATION_HEAD,
                    started_at=started_at,
                    seen_at=first_seen_at,
                )
                expected = await publish_monitor_only_start(
                    db,
                    account_id=account_id,
                    worker=worker,
                    started=started_at,
                    version=version,
                    operation_id=operation_id,
                    replacement=replacement,
                )
        except Exception:
            if expected is None:
                raise
        async with asyncio.timeout(15), factory.begin() as db:
            await db.execute(text("SET TRANSACTION READ ONLY"))
            heartbeat = await db.scalar(
                select(WorkerHeartbeat).where(
                    WorkerHeartbeat.queue == QueueName.AGENT_PAPER.value,
                    WorkerHeartbeat.worker_id == worker,
                )
            )
            if (
                expected is None
                or await latest_probe_state(db, account_id) != expected
                or heartbeat is None
                or heartbeat.started_at != started_at
                or heartbeat.seen_at != first_seen_at
                or heartbeat.process_version != version
                or heartbeat.migration_head != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("AGENT_PAPER_RECOVERY_ACTIVATION_BLOCKED")
    finally:
        await factory.kw["bind"].dispose()

    async def capture(bucket: datetime) -> object:
        await asyncio.to_thread(verify_activation)
        current, environment = await runtime_settings()
        return await refresh_disclosure_observations(
            current,
            account_id=account_id,
            calendar_authority_id=calendar_authority_id,
            scheduled_at=bucket,
            environment=environment,
            repo_root=repo_root,
            restricted_worker=worker,
        )

    async def no_scan() -> object:
        return {"status": "disabled", "model_requests": 0, "provider_requests": 0}

    monitor = PriceMonitorCoordinator(no_scan, prepare=capture)
    try:
        while not shutdown.is_set():
            phase, phase_started = "activation", monotonic_ns()
            try:
                async with asyncio.timeout(15):
                    await asyncio.to_thread(verify_activation)
                    phase, phase_started = "configuration", monotonic_ns()
                    await runtime_settings()
                    phase, phase_started = "heartbeat", monotonic_ns()
                    await heartbeat_agent_paper_loop(
                        settings,
                        worker_id=worker_id,
                        account_id=account_id,
                        started_at=started_at,
                        seen_at=clock(),
                        expected_hold=expected,
                    )
                phase, phase_started = "monitor_schedule", monotonic_ns()
                monitor.kick(clock())
                phase, phase_started = "wait", monotonic_ns()
                await wait_sleep(15)
            except Exception as error:
                _report_pulse_failure(phase, phase_started, error)
                raise
        return AgentPaperLoopAction.STOP_REQUESTED
    finally:
        await monitor.close()
