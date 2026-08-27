"""Dedicated Strategy R&D worker runtime remains isolated and capability-injected."""

from datetime import UTC, datetime, timedelta

import pytest
from helpers.evolution_evaluation import seed_evaluation_pair
from nexus.evolution.jobs import build_strategy_rnd_job_payload, enqueue_strategy_rnd_job
from nexus.evolution.worker import (
    STRATEGY_RND_WORKER_PROCESS_VERSION,
    run_strategy_rnd_worker_once,
)
from nexus.jobs.models import QueueName
from nexus.jobs.registry import JobHandlerResult
from nexus.persistence.models.runtime import Job, WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select

_NOW = datetime(2026, 8, 27, 23, tzinfo=UTC)


@pytest.mark.asyncio
async def test_dedicated_worker_claims_only_strategy_queue_with_injected_executor(
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

    seen: list[str] = []

    async def execute(context, accepted_payload):
        del context
        seen.append(accepted_payload.operation)
        return JobHandlerResult(outcome="succeeded", result_code="CANDIDATE_EVALUATED")

    worked = await run_strategy_rnd_worker_once(
        settings,
        executor=execute,
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
    assert stored is not None and stored.status == "succeeded"
    assert seen == ["evaluate_candidate"]
    assert heartbeat is not None
    assert heartbeat.process_version == STRATEGY_RND_WORKER_PROCESS_VERSION
    assert heartbeat.migration_head == "0023_evolution_authority"
    assert broker_heartbeat is None
