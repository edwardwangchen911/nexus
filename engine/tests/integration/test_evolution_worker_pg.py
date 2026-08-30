"""Dedicated Strategy R&D worker runtime remains isolated and capability-injected."""

from datetime import UTC, datetime, timedelta

import pytest
from helpers.evolution_evaluation import seed_evaluation_pair
from nexus.evolution.executor import build_strategy_rnd_executor
from nexus.evolution.jobs import build_strategy_rnd_job_payload, enqueue_strategy_rnd_job
from nexus.evolution.worker import (
    STRATEGY_RND_WORKER_PROCESS_VERSION,
    run_strategy_rnd_worker_once,
)
from nexus.jobs.models import QueueName
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import Job, WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select

_NOW = datetime(2026, 8, 27, 23, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _legacy_adjusted_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the v1 worker failure-path coverage behind the historical test gate."""
    for target in (
        "nexus.evolution.jobs.current_strategy_rnd_adjusted_authority_available",
        "nexus.evolution.authority.current_strategy_rnd_adjusted_authority_available",
        "nexus.evolution.executor.current_strategy_rnd_adjusted_authority_available",
    ):
        monkeypatch.setattr(target, lambda: True)


@pytest.mark.asyncio
async def test_dedicated_worker_claims_only_strategy_queue_with_real_executor(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    pair = await seed_evaluation_pair(factory)
    payload = build_strategy_rnd_job_payload(
        operation="evaluate_candidate",
        family_id=pair.family_id,
        experiment_id=pair.experiment_id,
        expected_champion_generation=1,
        candidate_state="OOS_RESERVED",
        candidate_version=4,
    )
    async with factory() as session, session.begin():
        job = await enqueue_strategy_rnd_job(session, payload=payload, effective_at=_NOW)

    worked = await run_strategy_rnd_worker_once(
        settings,
        executor=build_strategy_rnd_executor(settings),
        worker_id="strategy-rnd-runtime-test",
        worker_started_at=_NOW,
        clock=lambda: _NOW + timedelta(seconds=1),
    )

    async with factory() as session:
        stored = await session.get(Job, job.id)
        heartbeat = await session.scalar(
            select(WorkerHeartbeat).where(
                WorkerHeartbeat.queue == QueueName.STRATEGY_RND.value
            )
        )
        broker_heartbeat = await session.scalar(
            select(WorkerHeartbeat).where(
                WorkerHeartbeat.queue == QueueName.BROKER.value
            )
        )

    assert worked is True
    assert stored is not None and stored.status == "failed"
    assert stored.last_error_code == "BACKTEST_AUTHORITY_INCOMPLETE"
    assert heartbeat is not None
    assert heartbeat.process_version == STRATEGY_RND_WORKER_PROCESS_VERSION
    assert heartbeat.migration_head == CURRENT_MIGRATION_HEAD
    assert broker_heartbeat is None
