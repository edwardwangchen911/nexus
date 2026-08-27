"""PostgreSQL contracts for bounded Strategy R&D scheduling and execution."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from helpers.evolution_evaluation import seed_evaluation_pair
from helpers.evolution_shadow import seed_strategy_shadow_ready
from nexus.evolution.jobs import (
    build_strategy_rnd_job_payload,
    build_strategy_rnd_registry,
    enqueue_strategy_rnd_job,
    load_strategy_evolution_metrics,
    materialize_strategy_rnd_outbox_jobs,
    resolve_due_strategy_rnd_jobs,
)
from nexus.evolution.registry import pause_auto_promotion
from nexus.evolution.shadow.service import start_strategy_shadow
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName
from nexus.jobs.registry import JobHandlerResult
from nexus.jobs.worker import run_job_worker_once
from nexus.outbox.models import OutboxMessage
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentFamilyRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

_NOW = datetime(2026, 8, 27, 22, tzinfo=UTC)


@pytest.mark.asyncio
async def test_same_candidate_job_is_idempotent_and_executes_only_on_strategy_queue(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    payload = build_strategy_rnd_job_payload(
        operation="evaluate_candidate",
        family_id=pair.family_id,
        experiment_id=pair.experiment_id,
        expected_champion_generation=1,
    )
    async with factory() as session, session.begin():
        first = await enqueue_strategy_rnd_job(
            session,
            payload=payload,
            effective_at=_NOW,
        )
    async with factory() as session, session.begin():
        replay = await enqueue_strategy_rnd_job(
            session,
            payload=payload,
            effective_at=_NOW,
        )
    assert replay.id == first.id
    assert first.queue == QueueName.STRATEGY_RND.value
    assert first.priority == JOB_PRIORITIES[JobType.STRATEGY_RND] == 20

    seen: list[str] = []

    async def execute(context, accepted_payload):
        del context
        seen.append(accepted_payload.operation)
        return JobHandlerResult(outcome="succeeded", result_code="CANDIDATE_EVALUATED")

    assert await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="strategy-rnd-test",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0022_strategy_evolution",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        persisted = await session.get(Job, first.id)
        broker_jobs = await session.scalar(
            select(func.count()).select_from(Job).where(Job.queue == QueueName.BROKER.value)
        )
    assert persisted is not None and persisted.status == "succeeded"
    assert persisted.result_code == "CANDIDATE_EVALUATED"
    assert seen == ["evaluate_candidate"]
    assert broker_jobs == 0


@pytest.mark.asyncio
async def test_admission_outbox_materializes_one_idempotent_job_per_candidate(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    payload = {
        "status": "ADMITTED",
        "candidate_count": 1,
        "champion_generation": 1,
        "authority_hash": "a" * 64,
    }
    async with factory() as session, session.begin():
        scheduled_first = await enqueue_strategy_rnd_job(
            session,
            payload=build_strategy_rnd_job_payload(
                operation="evaluate_candidate",
                family_id=pair.family_id,
                experiment_id=pair.experiment_id,
                expected_champion_generation=1,
            ),
            effective_at=_NOW - timedelta(minutes=1),
        )
        event = OutboxEvent(
            event_type="strategy.experiment.admitted",
            aggregate_id=pair.family_id,
            schema_version="strategy_evolution_event_v1",
            payload=payload,
            published_at=None,
            available_at=_NOW,
        )
        session.add(event)
        await session.flush()
        message = OutboxMessage(
            event_id=event.id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            schema_version=event.schema_version,
            payload=event.payload,
        )
        first = await materialize_strategy_rnd_outbox_jobs(session, message=message)
    async with factory() as session, session.begin():
        replay = await materialize_strategy_rnd_outbox_jobs(session, message=message)
    assert len(first) == len(replay) == 1
    assert first[0].id == replay[0].id == scheduled_first.id
    assert first[0].payload["experiment_id"] == str(pair.experiment_id)


@pytest.mark.asyncio
async def test_generation_requires_pass_quality_one_lineage_and_obeys_pause_and_quarter(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    snapshot_id = UUID("0198d500-1000-7000-8000-000000002211")
    async with factory() as session, session.begin():
        search_policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert search_policy is not None
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        session.add(
            StrategyResearchLineageRecord(
                lineage_key="job-scheduler-fixture-v1",
                evolution_policy_id=search_policy.id,
                cumulative_oos_trial_count=0,
                status="active",
            )
        )
        retired = StrategyResearchLineageRecord(
            lineage_key="retired-job-scheduler-fixture-v1",
            evolution_policy_id=search_policy.id,
            cumulative_oos_trial_count=0,
            status="retired",
        )
        session.add(retired)
        await session.flush()
        session.add(
            StrategyExperimentFamilyRecord(
                research_lineage_id=retired.id,
                parent_strategy_definition_id=head.active_definition_id,
                rnd_output_id=None,
                champion_generation=head.active_generation,
                admission_status="REJECTED",
                candidate_count=0,
                idempotency_key="retired-job-scheduler-family",
                authority_hash="a" * 64,
                protocol_hash="b" * 64,
                code_hash="c" * 64,
                created_at=_NOW - timedelta(days=1),
            )
        )
        session.add(
            DataSnapshot(
                id=snapshot_id,
                source="fixture",
                provider_object_id="strategy-job-quality",
                revision="v1",
                observed_at=_NOW - timedelta(hours=2),
                available_at=_NOW - timedelta(hours=1),
                received_at=_NOW - timedelta(hours=1),
                schema_version="1",
                payload={},
            )
        )
        await session.flush()
        session.add(
            QualityReport(
                data_snapshot_id=snapshot_id,
                status="PASS",
                schema_version="1",
                payload={},
            )
        )
    async with factory() as session, session.begin():
        due = await resolve_due_strategy_rnd_jobs(
            session,
            session_date=_NOW.date(),
            now=_NOW,
        )
        assert [item.operation for item in due] == ["generate_family"]
        generation_job = await enqueue_strategy_rnd_job(
            session,
            payload=due[0],
            effective_at=_NOW,
        )
    async with factory() as session, session.begin():
        assert not await resolve_due_strategy_rnd_jobs(
            session,
            session_date=(_NOW + timedelta(days=1)).date(),
            now=_NOW + timedelta(days=1),
        )
        failed_snapshot = DataSnapshot(
            source="fixture",
            provider_object_id="strategy-job-quality-failed",
            revision="v1",
            observed_at=_NOW + timedelta(hours=10),
            available_at=_NOW + timedelta(hours=12),
            received_at=_NOW + timedelta(hours=12),
            schema_version="1",
            payload={},
        )
        session.add(failed_snapshot)
        await session.flush()
        session.add(
            QualityReport(
                data_snapshot_id=failed_snapshot.id,
                status="FAIL",
                schema_version="1",
                payload={},
            )
        )

    async def must_not_execute(context, payload):
        del context, payload
        raise AssertionError("failed quality must stop generation before executor")

    assert await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=must_not_execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="strategy-rnd-quality-gate",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0022_strategy_evolution",
        clock=lambda: _NOW + timedelta(days=1),
        claim_registered_only=True,
    )
    async with factory() as session, session.begin():
        gated = await session.get(Job, generation_job.id)
        assert gated is not None
        assert (gated.status, gated.result_code) == ("skipped", "GENERATION_NOT_DUE")
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        await pause_auto_promotion(
            session,
            expected_version=head.version,
            expected_generation=head.active_generation,
            now=_NOW + timedelta(days=2),
            idempotency_key="strategy-job-test-pause",
        )
    async with factory() as session:
        assert not await resolve_due_strategy_rnd_jobs(
            session,
            session_date=(_NOW + timedelta(days=100)).date(),
            now=_NOW + timedelta(days=100),
        )


@pytest.mark.asyncio
async def test_daily_shadow_target_is_one_typed_operation_and_queued_work_is_not_duplicated(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_strategy_shadow_ready(migrated_database_url, factory)
    async with factory() as session, session.begin():
        started = await start_strategy_shadow(
            session,
            experiment_id=seed.pair.experiment_id,
            now=seed.now,
        )
    first_session = started.start_session
    planned_at = datetime.combine(first_session, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=22
    )
    async with factory() as session, session.begin():
        due = await resolve_due_strategy_rnd_jobs(
            session,
            session_date=first_session,
            now=planned_at,
        )
        advances = [item for item in due if item.operation == "advance_shadow"]
        assert len(advances) == 1
        assert advances[0].family_id == seed.pair.family_id
        assert advances[0].experiment_id == seed.pair.experiment_id
        await enqueue_strategy_rnd_job(
            session,
            payload=advances[0],
            effective_at=planned_at,
        )
    async with factory() as session:
        replay = await resolve_due_strategy_rnd_jobs(
            session,
            session_date=first_session,
            now=planned_at,
        )
        assert not [item for item in replay if item.operation == "advance_shadow"]

        metrics = await load_strategy_evolution_metrics(session, now=planned_at)
    serialized = metrics.model_dump_json()
    assert metrics.active_family_count == 1
    assert metrics.candidate_status_counts == {"SHADOW_RUNNING": 1}
    assert metrics.shadow_running_count == 1
    assert metrics.strategy_queue_oldest_age_seconds >= 0
    for private_id in (
        str(seed.pair.family_id),
        str(seed.pair.experiment_id),
        str(started.group_id),
    ):
        assert private_id not in serialized

    async with factory() as session:
        audit_events = tuple(
            await session.scalars(
                select(AuditEvent)
                .where(AuditEvent.action.startswith("strategy."))
                .order_by(AuditEvent.event_seq)
            )
        )
    allowed_audit_keys = {
        "authority_hash",
        "code_hash",
        "decision_hash",
        "event_hash",
        "event_type",
        "final_authority_hash",
        "gate_code",
        "live_execution_enabled",
        "policy_version",
        "proposal_hash",
        "protocol_hash",
        "reason_code",
        "request_hash",
        "schema_version",
        "status",
    }
    assert audit_events
    assert all(set(event.payload) <= allowed_audit_keys for event in audit_events)
