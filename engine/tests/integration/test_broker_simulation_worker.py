from datetime import UTC, datetime
from pathlib import Path

import pytest
from helpers.broker_simulation import (
    approval_authority,
    preparation,
    seed_broker_authority,
)
from nexus.broker_simulation.intents import (
    approve_execution_intent,
    prepare_execution_intent,
)
from nexus.broker_worker import build_broker_runtime, run_broker_worker_once
from nexus.jobs.models import JobKeyV1, JobStatus, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.registry import JobRegistry
from nexus.jobs.repository import enqueue_job
from nexus.jobs.worker import UnsafeJobError, run_job_worker_once
from nexus.persistence.migrations import (
    CURRENT_GENERAL_WORKER_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.broker_simulation import (
    BrokerExecutionIntentRecord,
    BrokerSimulationAdmissionRecord,
)
from nexus.persistence.models.runtime import Job, WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.broker.simulation_gateway import FixtureSimulationGatewayClient
from nexus.settings import EngineSettings
from sqlalchemy import select, text

_NOW = datetime(2026, 8, 28, 14, 1, tzinfo=UTC)


async def _enqueue_dispatch(database_url: str):
    await seed_broker_authority(database_url)
    factory = session_factory(EngineSettings(database_url=database_url))
    authority = preparation()
    async with factory() as session, session.begin():
        prepared = await prepare_execution_intent(session, authority)
    async with factory() as session, session.begin():
        await approve_execution_intent(
            session,
            intent_id=prepared.intent_id,
            actor_id="private-user",
            expected_version=1,
            if_match=prepared.body_hash,
            request_body_hash="f" * 64,
            idempotency_key="worker-approval-1",
            approved_at=_NOW,
            authority=approval_authority(authority),
        )
        payload = {"intent_id": str(prepared.intent_id)}
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.BROKER_SIMULATION_DISPATCH,
                scope_kind="provider",
                scope_id="fosun.simulation",
                effective_at=_NOW,
                input_hash=job_input_hash(payload),
            ),
            schema_version="broker_sim_dispatch_v1",
            payload=payload,
            queue=QueueName.BROKER,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
        )
    return factory, prepared.intent_id, job.id


@pytest.mark.asyncio
async def test_general_worker_cannot_claim_broker_simulation_job(
    migrated_database_url: str,
) -> None:
    factory, _, job_id = await _enqueue_dispatch(migrated_database_url)

    worked = await run_job_worker_once(
        factory,
        registry=JobRegistry(),
        queue=QueueName.GENERAL,
        worker_id="general-boundary-test",
        worker_started_at=_NOW,
        process_version=CURRENT_GENERAL_WORKER_PROCESS_VERSION,
        migration_head=CURRENT_MIGRATION_HEAD,
        clock=lambda: _NOW,
        claim_registered_only=True,
    )

    assert worked is False
    async with factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None and job.status == JobStatus.QUEUED.value


@pytest.mark.asyncio
async def test_offline_broker_worker_executes_fixture_dispatch_once(
    migrated_database_url: str,
) -> None:
    factory, intent_id, job_id = await _enqueue_dispatch(migrated_database_url)
    gateway = FixtureSimulationGatewayClient(outcomes=("acknowledged",))
    settings = BrokerWorkerSettings.model_validate(
        {
            "NEXUS_ENV": "test",
            "DATABASE_URL": migrated_database_url,
            "NEXUS_BROKER_SIMULATION_MODE": "offline_fixture",
        }
    )
    runtime = build_broker_runtime(settings, simulation_gateway=gateway)
    now = [_NOW]
    worker_id = "nexus-broker-lifecycle-test"
    try:
        worked = await run_broker_worker_once(
            runtime,
            clock=lambda: now[0],
            worker_id=worker_id,
            worker_started_at=_NOW,
        )
        now[0] = datetime(2026, 8, 28, 14, 1, 1, tzinfo=UTC)
        replay = await run_broker_worker_once(
            runtime,
            clock=lambda: now[0],
            worker_id=worker_id,
            worker_started_at=_NOW,
        )
    finally:
        await runtime.aclose()

    assert worked is True
    assert replay is False
    assert gateway.call_count == 1
    async with factory() as session:
        job = await session.get(Job, job_id)
        intent = await session.get(BrokerExecutionIntentRecord, intent_id)
        heartbeats = list(
            (
                await session.scalars(
                    select(WorkerHeartbeat).where(
                        WorkerHeartbeat.queue == QueueName.BROKER.value,
                        WorkerHeartbeat.worker_id == worker_id,
                    )
                )
            ).all()
        )
    assert job is not None and job.status == JobStatus.SUCCEEDED.value
    assert intent is not None and intent.status == "completed"
    assert len(heartbeats) == 1
    assert heartbeats[0].started_at == _NOW
    assert heartbeats[0].seen_at == now[0]


@pytest.mark.asyncio
async def test_broker_worker_rejects_noncurrent_ticker_head_before_claim(
    migrated_database_url: str,
) -> None:
    factory, _, job_id = await _enqueue_dispatch(migrated_database_url)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "UPDATE public.alembic_version "
                "SET version_num='0031_intraday_schema_admission'"
            )
        )
    gateway = FixtureSimulationGatewayClient(outcomes=("acknowledged",))
    settings = BrokerWorkerSettings.model_validate(
        {
            "NEXUS_ENV": "test",
            "DATABASE_URL": migrated_database_url,
            "NEXUS_BROKER_SIMULATION_MODE": "offline_fixture",
        }
    )
    runtime = build_broker_runtime(settings, simulation_gateway=gateway)
    try:
        with pytest.raises(RuntimeError, match="BROKER_DATABASE_MIGRATION_HEAD_MISMATCH"):
            await run_broker_worker_once(runtime, clock=lambda: _NOW)
    finally:
        await runtime.aclose()

    assert gateway.call_count == 0
    async with factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None and job.status == JobStatus.QUEUED.value


@pytest.mark.asyncio
async def test_external_worker_revalidates_expired_capability_before_claim(
    migrated_database_url: str,
) -> None:
    factory, _, job_id = await _enqueue_dispatch(migrated_database_url)
    async with factory() as session, session.begin():
        session.add(
            BrokerSimulationAdmissionRecord(
                subject="expired-fixture-contract",
                manifest_hash="1" * 64,
                artifact_hash="2" * 64,
                capability_status="SIMULATION_WRITE_ACTIVATED",
                valid_from=datetime(2026, 8, 28, 12, tzinfo=UTC),
                valid_until=datetime(2026, 8, 28, 13, tzinfo=UTC),
                revoked=False,
                revoked_at=None,
            )
        )
    settings = BrokerWorkerSettings.for_test(
        database_url=migrated_database_url,
        client_cert=Path("/synthetic/client.crt"),
        client_key=Path("/synthetic/client.key"),
        client_ca=Path("/synthetic/ca.crt"),
    )
    gateway = FixtureSimulationGatewayClient(outcomes=("acknowledged",))
    runtime = build_broker_runtime(
        settings,
        gateway_client=object(),  # type: ignore[arg-type]
        simulation_gateway=gateway,
    )
    try:
        with pytest.raises(UnsafeJobError, match="SIMULATION_WRITE_NOT_ACTIVATED"):
            await run_broker_worker_once(runtime, clock=lambda: _NOW)
    finally:
        await runtime.aclose()

    assert gateway.call_count == 0
    async with factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None and job.status == JobStatus.QUEUED.value
