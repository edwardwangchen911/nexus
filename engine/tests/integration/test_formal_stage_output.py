"""Synthetic terminal metadata qualifies identity only, never content or live acceptance."""

from datetime import timedelta

import pytest
from integration.test_candidate_research_authority import NOW
from integration.test_formal_stage_worker import frozen_formal_test_job
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["valid", "missing_attempt", "wrong_hash", "prepared", "future_finish"]
)
async def test_formal_output_identity_requires_complete_unique_job_chain(
    migrated_database_url, case
):
    from nexus.agents.codex_formal_stage import (
        _formal_stage_job_hash,
        formal_stage_output_qualified,
    )

    factory, source_job_id = await frozen_formal_test_job(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    )
    async with factory.begin() as session:
        source_job = await session.get(Job, source_job_id)
        source_run = await session.get(AgentResearchRunRecord, source_job.payload["run_id"])
        if case == "prepared":
            run_id = source_run.id
        else:
            values = {
                c.name: getattr(source_run, c.name)
                for c in source_run.__table__.columns
                if c.name not in {"id", "created_at"}
            }
            values.update(status="completed", idempotency_key="formal-output-synthetic")
            run = AgentResearchRunRecord(id=new_uuid7(), **values)
            session.add(run)
            await session.flush()
            values = {
                c.name: getattr(source_job, c.name)
                for c in source_job.__table__.columns
                if c.name not in {"id", "created_at"}
            }
            values.update(
                job_key=source_job.job_key + ":output-proof",
                status="succeeded",
                payload={"run_id": str(run.id)},
                started_at=NOW,
                finished_at=NOW + timedelta(seconds=1),
                attempt_count=1,
                input_hash="d" * 64 if case == "wrong_hash" else _formal_stage_job_hash(run),
            )
            job = Job(id=new_uuid7(), **values)
            session.add(job)
            await session.flush()
            if case != "missing_attempt":
                session.add(
                    JobAttempt(
                        job_id=job.id,
                        attempt_no=1,
                        lease_token=new_uuid7(),
                        fence_token=1,
                        worker_id="synthetic-output-proof",
                        started_at=NOW,
                        finished_at=NOW + timedelta(seconds=1),
                        outcome="succeeded",
                        error_code=None,
                    )
                )
            run_id = run.id
    async with factory() as session:
        assert await formal_stage_output_qualified(
            session,
            run_id,
            decision_time=NOW if case == "future_finish" else NOW + timedelta(seconds=2),
        ) == (case == "valid")
