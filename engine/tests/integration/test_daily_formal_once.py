"""CLI service path over real PostgreSQL; research outputs are explicit synthetic facts."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from integration.test_formal_optimization_producer import setup
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import enqueue_job
from nexus.ops.daily_once import (
    DailyFormalProposalAdmitRequest,
    DailyFormalProposalRunRequest,
    DailyOnceError,
    DailyOnceSettings,
    run_once,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
    ProposalComparisonRecord,
)
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select


def _settings(url):
    return DailyOnceSettings(
        database_url=url,
        object_store_endpoint="http://127.0.0.1:19000",
        object_store_region="test-region", object_store_bucket="synthetic-test",
        object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )


async def _unrelated_job(factory, *, legacy=False):
    payload = (
        {name: str(uuid4()) for name in
         ("portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id")}
        if legacy else {"synthetic": "unrelated"}
    )
    async with factory() as session, session.begin():
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.OPTIMIZATION_PROPOSE if legacy else JobType.QUALITY_FREEZE,
                scope_kind="portfolio", scope_id=str(uuid4()), effective_at=datetime.now(UTC),
                input_hash=job_input_hash(payload),
            ),
            schema_version="optimization_job_v1" if legacy else "unrelated_test_v1",
            payload=payload, queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER, max_attempts=1,
        )
        return job.id


@pytest.mark.asyncio
async def test_explicit_formal_admit_and_run_preserve_partiality_and_leave_other_jobs_alone(
    migrated_database_url, monkeypatch,
):
    factory, formal = await setup(migrated_database_url)
    unrelated_id = await _unrelated_job(factory)
    settings = _settings(migrated_database_url)

    def no_model(*args, **kwargs):
        pytest.fail("formal construction must not invoke research or any model")

    monkeypatch.setattr("nexus.ops.daily_once.run_agent_worker_once", no_model)
    monkeypatch.setattr("nexus.ops.daily_once.S3ProviderEvidenceStore", lambda _: SimpleNamespace(
        read_normalized=FixtureArtifactReader({}),
    ))
    request = DailyFormalProposalAdmitRequest(
        action="formal-proposal-admit", idempotency_key="cli-normal",
        **formal.model_dump(),
    )
    async with factory() as session:
        before = tuple((await session.execute(select(
            AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash,
        ))).all())
    admitted = await run_once(settings, request)
    replayed = await run_once(settings, request)
    assert admitted["job_id"] == replayed["job_id"] and replayed["replayed"] is True
    assert admitted["execution_locked"] is True
    command = DailyFormalProposalRunRequest(
        action="formal-proposal-run", job_id=UUID(admitted["job_id"]),
    )
    result = await run_once(settings, command)
    assert result["status"] == "completed" and result["processed"] == 1
    assert result["execution_locked"] is True
    repeated = await run_once(settings, command)
    assert repeated["processed"] == 0
    async with factory() as session:
        untouched = await session.get(Job, unrelated_id)
        assert untouched.status == "queued" and untouched.attempt_count == 0
        group = await session.scalar(select(OptimizationProposalGroupRecord))
        assert group.partial and not group.diagnostic_complete and not group.cost_complete
        comparisons = tuple(await session.scalars(select(ProposalComparisonRecord)))
        assert len(comparisons) == 3
        assert all(c.non_executable and not c.cost_complete for c in comparisons)
        assert tuple((await session.execute(select(
            AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash,
        ))).all()) == before


@pytest.mark.asyncio
async def test_formal_cli_bad_research_reference_rolls_back_all_new_authority(
    migrated_database_url,
):
    factory, formal = await setup(migrated_database_url)
    reference = formal.research_outputs[0].model_copy(update={"output_id": uuid4()})
    request = DailyFormalProposalAdmitRequest(
        action="formal-proposal-admit", idempotency_key="invalid-output",
        **formal.model_copy(update={"research_outputs": (reference,)}).model_dump(),
    )
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_FORMAL_INPUT_UNAVAILABLE"):
        await run_once(_settings(migrated_database_url), request)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(
            OptimizationAdmissionAuthorityRecord,
        )) == 0
        assert await session.scalar(select(func.count()).select_from(Job).where(
            Job.job_type == JobType.OPTIMIZATION_PROPOSE.value,
        )) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_formal_run_rejects_unrelated_or_legacy_job_before_claim(
    migrated_database_url, monkeypatch, legacy,
):
    factory, _ = await setup(migrated_database_url)
    identifier = await _unrelated_job(factory, legacy=legacy)

    def no_worker(*args, **kwargs):
        pytest.fail("a non-formal job must be rejected before worker execution")

    monkeypatch.setattr("nexus.ops.daily_once.run_job_worker_once", no_worker)
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_FORMAL_JOB_UNAVAILABLE"):
        await run_once(_settings(migrated_database_url), DailyFormalProposalRunRequest(
            action="formal-proposal-run", job_id=identifier,
        ))
    async with factory() as session:
        job = await session.get(Job, identifier)
        assert job.status == "queued" and job.attempt_count == 0
