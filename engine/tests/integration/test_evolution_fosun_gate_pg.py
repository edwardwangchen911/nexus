from datetime import UTC, datetime, timedelta

import pytest
from helpers.evolution_evaluation import seed_evaluation_pair
from nexus.evolution.jobs import (
    build_strategy_rnd_job_payload,
    build_strategy_rnd_registry,
    enqueue_strategy_rnd_job,
    strategy_generation_eligibility,
)
from nexus.jobs.models import QueueName
from nexus.jobs.registry import JobHandlerResult
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

_NOW = datetime(2026, 8, 29, 12, tzinfo=UTC)


@pytest.mark.asyncio
async def test_generation_authority_gate_precedes_active_family_reason(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_evaluation_pair(factory)

    async with factory() as session:
        eligibility = await strategy_generation_eligibility(session, now=_NOW)

    assert (eligibility.eligible, eligibility.reason_code) == (
        False,
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
    )


@pytest.mark.asyncio
async def test_queued_evolution_job_stops_before_executor_without_misleading_audit(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
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
        job = await enqueue_strategy_rnd_job(
            session,
            payload=payload,
            effective_at=_NOW,
        )
        audit_before = int(
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.schema_version == "strategy_evolution_event_v1")
            )
            or 0
        )

    executed = False

    async def execute(context, accepted_payload):
        del context, accepted_payload
        nonlocal executed
        executed = True
        return JobHandlerResult(outcome="succeeded", result_code="MUST_NOT_RUN")

    assert await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="fosun-adjusted-gate-test",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        stored = await session.get(Job, job.id)
        audit_after = int(
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.schema_version == "strategy_evolution_event_v1")
            )
            or 0
        )

    assert stored is not None
    assert (stored.status, stored.result_code) == (
        "skipped",
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
    )
    assert executed is False
    assert audit_after == audit_before


@pytest.mark.asyncio
async def test_queued_generation_reports_adjusted_authority_gate_before_due_reason(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_evaluation_pair(factory)
    payload = build_strategy_rnd_job_payload(
        operation="generate_family",
        family_id=None,
        experiment_id=None,
        expected_champion_generation=1,
    )
    async with factory() as session, session.begin():
        job = await enqueue_strategy_rnd_job(
            session,
            payload=payload,
            effective_at=_NOW,
        )

    async def must_not_execute(context, accepted_payload):
        del context, accepted_payload
        raise AssertionError("adjusted authority gate must stop generation")

    assert await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=must_not_execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="fosun-adjusted-generation-gate-test",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        stored = await session.get(Job, job.id)

    assert stored is not None
    assert (stored.status, stored.result_code) == (
        "skipped",
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
    )


@pytest.mark.asyncio
async def test_executor_gate_closure_after_preflight_does_not_write_misleading_audit(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
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
        job = await enqueue_strategy_rnd_job(
            session,
            payload=payload,
            effective_at=_NOW,
        )
        audit_before = int(
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.schema_version == "strategy_evolution_event_v1")
            )
            or 0
        )

    monkeypatch.setattr(
        "nexus.evolution.jobs.current_strategy_rnd_adjusted_authority_available",
        lambda: True,
    )

    async def execute(context, accepted_payload):
        del context, accepted_payload
        return JobHandlerResult(
            outcome="failed",
            result_code=None,
            error_code="BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
        )

    assert await run_job_worker_once(
        factory,
        registry=build_strategy_rnd_registry(executor=execute),
        queue=QueueName.STRATEGY_RND,
        worker_id="fosun-adjusted-toctou-gate-test",
        worker_started_at=_NOW,
        process_version="test-v1",
        migration_head="0029_fosun_bar_lineage",
        clock=lambda: _NOW + timedelta(seconds=1),
        claim_registered_only=True,
    )
    async with factory() as session:
        stored = await session.get(Job, job.id)
        audit_after = int(
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.schema_version == "strategy_evolution_event_v1")
            )
            or 0
        )

    assert stored is not None
    assert (stored.status, stored.result_code, stored.last_error_code) == (
        "failed",
        None,
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
    )
    assert audit_after == audit_before
