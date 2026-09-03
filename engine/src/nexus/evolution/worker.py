"""Dedicated runtime and CLI for the isolated Strategy R&D queue."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from nexus.domain.ids import new_uuid7
from nexus.evolution.executor import build_strategy_rnd_executor
from nexus.evolution.jobs import StrategyRNDOperationExecutor, build_strategy_rnd_registry
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings

STRATEGY_RND_WORKER_PROCESS_VERSION = "nexus-engine-phase2-strategy-rnd-v1"


async def run_strategy_rnd_worker_once(
    settings: EngineSettings,
    *,
    executor: StrategyRNDOperationExecutor,
    worker_id: str | None = None,
    worker_started_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> bool:
    """Claim at most one isolated operation using an explicitly injected executor."""
    runtime_clock = clock or _utc_now
    started_at = worker_started_at or runtime_clock()
    return await run_job_worker_once(
        session_factory(settings),
        registry=build_strategy_rnd_registry(executor=executor),
        queue=QueueName.STRATEGY_RND,
        worker_id=worker_id or f"strategy-rnd-{new_uuid7()}",
        worker_started_at=started_at,
        process_version=STRATEGY_RND_WORKER_PROCESS_VERSION,
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=runtime_clock,
        claim_registered_only=True,
    )


async def run_strategy_rnd_worker(
    settings: EngineSettings,
    *,
    executor: StrategyRNDOperationExecutor,
    run_forever: bool = False,
    clock: Callable[[], datetime] | None = None,
) -> None:
    """Run the isolated queue only after the caller supplies its full capability root."""
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK and run_forever:
        raise ValueError("CODEX_STRATEGY_RND_RUN_ONCE_REQUIRED")
    runtime_clock = clock or _utc_now
    worker_id = f"strategy-rnd-{new_uuid7()}"
    started_at = runtime_clock()
    while True:
        worked = await run_strategy_rnd_worker_once(
            settings,
            executor=executor,
            worker_id=worker_id,
            worker_started_at=started_at,
            clock=runtime_clock,
        )
        if not run_forever:
            return
        if not worked:
            await asyncio.sleep(0.5)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def main() -> None:
    """Run the fail-closed composition; Codex requires explicit one-shot mode."""
    parser = argparse.ArgumentParser(description="Run the isolated Strategy R&D worker.")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    settings = EngineSettings()
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK and not args.once:
        raise ValueError("CODEX_STRATEGY_RND_RUN_ONCE_REQUIRED")
    executor = build_strategy_rnd_executor(settings)
    asyncio.run(
        run_strategy_rnd_worker(
            settings,
            executor=executor,
            run_forever=not args.once,
        )
    )
