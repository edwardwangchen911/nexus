"""COMMIT ambiguity harness for the staged protocol; not T safety-equivalence.

Both physical outcomes produce the same application exception. This exercises
native claim/checkpoint persistence, not the missing protocol authorization.
A future protocol must use this harness at authorization AND observation commit.
"""

from datetime import timedelta

import pytest
from helpers.commit_ack_fault import CommitAcknowledgementLost, lose_commit_ack
from integration.test_job_checkpoint import NOW, _claim
from nexus.jobs.repository import checkpoint_job
from nexus.persistence.models.runtime import IntegrationStatus, Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True], ids=["rolled-back", "committed"])
async def test_native_checkpoint_commit_ambiguity_never_reaches_following_action(
    migrated_database_url, server_commits
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    observer_factory = session_factory(EngineSettings(database_url=migrated_database_url))
    engine = factory.kw["bind"]
    try:
        claim = await _claim(factory, lease_seconds=30)
        reached_following_action = False

        async def apply(session):
            session.add(
                IntegrationStatus(
                    provider="staged-commit-observation",
                    status="READY",
                    last_success_at=NOW,
                    mode_changed_at=NOW,
                )
            )

        with pytest.raises(CommitAcknowledgementLost, match="COMMIT_OUTCOME_UNCONFIRMED"):
            async with factory() as session:
                await session.begin()
                deadline = await checkpoint_job(
                    session,
                    claim=claim,
                    now=NOW + timedelta(seconds=10),
                    apply=apply,
                    extend_seconds=1,
                )
                assert deadline == claim.expires_at
                # The fault is in the driver's COMMIT, before Session.commit
                # can acknowledge it. No later business action or retry occurs.
                with lose_commit_ack(engine, server_commits=server_commits) as fault:
                    await session.commit()
                reached_following_action = True

        assert fault.boundary_calls == 1
        assert fault.server_commit_completed is server_commits
        assert not reached_following_action
        # Independent connection verifies both physical outcomes, not inference
        # from the exception class, local ORM identity map, or requested branch.
        async with observer_factory() as observer:
            assert await observer.scalar(
                select(func.count())
                .select_from(IntegrationStatus)
                .where(IntegrationStatus.provider == "staged-commit-observation")
            ) == (1 if server_commits else 0)
            lease = await observer.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
            assert lease.expires_at == claim.expires_at
            assert lease.heartbeat_at == NOW + timedelta(seconds=10 if server_commits else 0)
            job = await observer.get(Job, claim.job_id)
            assert job.status == "running" and job.attempt_count == 1
            assert job.manual_retry_count == 0
            assert (
                await observer.scalar(
                    select(func.count())
                    .select_from(JobAttempt)
                    .where(JobAttempt.job_id == claim.job_id)
                )
                == 1
            )
    finally:
        await engine.dispose()
        await observer_factory.kw["bind"].dispose()
