"""Runtime qualification must be rechecked under the selected Job lock."""

from datetime import timedelta

import pytest
from integration.test_job_scope import _NOW, _enqueue, _Payload, _state
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import claim_next_job, fail_job
from nexus.jobs.worker import run_job_worker_once
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
@pytest.mark.parametrize("qualified", [False, True, None])
async def test_normal_worker_checks_selected_locked_job_before_attempt_or_lease(
    migrated_database_url,
    qualified,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await _enqueue(factory, "qualification-target")
    executed = []
    observed = []

    async def guard(session, job):
        assert job.id == target and job.status == "queued" and job.attempt_count == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == target)
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(JobLease).where(JobLease.job_id == target)
            )
            == 0
        )
        # A second actual connection cannot alter the selected Job while its
        # frozen authority is checked. NOWAIT makes this deterministic.
        async with factory() as competitor, competitor.begin():
            assert (
                await claim_next_job(
                    competitor,
                    queue=QueueName.GENERAL,
                    worker_id="competing-owner",
                    now=_NOW,
                    job_ids=(target,),
                )
                is None
            )
            with pytest.raises(DBAPIError) as error:
                await competitor.scalar(
                    select(Job).where(Job.id == target).with_for_update(nowait=True)
                )
            assert error.value.orig.sqlstate == "55P03"
        observed.append(job.id)
        return qualified

    async def handler(context):
        executed.append(context.claim.job_id)
        return JobHandlerResult(outcome="succeeded", result_code="QUALIFICATION_OK")

    registry = JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.DIAGNOSTICS_RUN,
                schema_version="job_scope_test_v1",
                payload_model=_Payload,
                queue=QueueName.GENERAL,
                timeout_seconds=1,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="scope-test",
                handler=handler,
            ),
        )
    )
    before = await _state(factory, target)
    arguments = dict(
        registry=registry,
        queue=QueueName.GENERAL,
        worker_id="qualified-owner",
        worker_started_at=_NOW,
        process_version="test-qualification",
        migration_head="0066",
        clock=lambda: _NOW,
        job_ids=(target,),
        locked_job_guard=guard,
    )
    if qualified is None:
        with pytest.raises(ValueError, match="LOCKED_JOB_GUARD_RESULT_INVALID"):
            await run_job_worker_once(factory, **arguments)
        assert executed == []
        assert await _state(factory, target) == before
        return
    worked = await run_job_worker_once(factory, **arguments)
    assert observed == [target]
    assert worked is qualified
    if qualified:
        assert executed == [target]
        assert (await _state(factory, target))[0] == "succeeded"
        terminal = await _state(factory, target)
        assert not await run_job_worker_once(factory, **arguments)
        assert observed == [target] and executed == [target]
        assert await _state(factory, target) == terminal
    else:
        assert executed == []
        assert await _state(factory, target) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("elapsed_seconds", [0, 61])
async def test_qualification_path_does_not_reclaim_an_expired_existing_attempt(
    migrated_database_url,
    elapsed_seconds,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await _enqueue(factory, "existing-attempt")
    async with factory() as session, session.begin():
        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="original-owner",
                now=_NOW,
                lease_seconds=30,
                job_ids=(target,),
            )
            is not None
        )
    before = await _state(factory, target)
    calls = []

    async def guard(session, job):
        calls.append(job.id)
        return False

    worked = await run_job_worker_once(
        factory,
        registry=JobRegistry(),
        queue=QueueName.GENERAL,
        worker_id="qualification-only",
        worker_started_at=_NOW,
        process_version="test-qualification",
        migration_head="0066",
        clock=lambda: _NOW + timedelta(seconds=elapsed_seconds),
        job_ids=(target,),
        locked_job_guard=guard,
    )
    assert not worked
    assert calls == []
    assert await _state(factory, target) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [None, (), ("first", "second")])
async def test_guarded_worker_rejects_non_exact_scope_before_database_access(scope):
    async def forbidden_factory():
        pytest.fail("invalid qualification scope must not open a connection")

    async def guard(session, job):
        pytest.fail("invalid scope must not invoke its guard")

    with pytest.raises(ValueError, match="LOCKED_JOB_GUARD_REQUIRES_EXACT_SCOPE"):
        await run_job_worker_once(
            forbidden_factory,
            registry=JobRegistry(),
            queue=QueueName.GENERAL,
            worker_id="qualification-only",
            worker_started_at=_NOW,
            process_version="test-qualification",
            migration_head="0066",
            clock=lambda: _NOW,
            job_ids=scope,
            locked_job_guard=guard,
        )


@pytest.mark.asyncio
async def test_locked_guard_refreshes_cached_job_after_another_connection_requeues(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    target = await _enqueue(factory, "changed-after-preview")
    async with factory() as session, session.begin():
        preview = await session.get(Job, target)
        assert preview is not None and preview.attempt_count == 0
        async with factory() as competitor, competitor.begin():
            claim = await claim_next_job(
                competitor,
                queue=QueueName.GENERAL,
                worker_id="previous-attempt",
                now=_NOW,
                job_ids=(target,),
            )
            assert claim is not None
            assert (
                await fail_job(
                    competitor,
                    claim=claim,
                    now=_NOW,
                    error_code="SYNTHETIC_FAILURE",
                )
                == "queued"
            )
        before = await _state(factory, target)

        async def guard(locked_session, job):
            assert job is preview
            assert job.attempt_count == 1
            assert job.version == before[2]
            return False

        assert (
            await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="qualification-only",
                now=_NOW,
                job_ids=(target,),
                locked_job_guard=guard,
            )
            is None
        )
    assert await _state(factory, target) == before
