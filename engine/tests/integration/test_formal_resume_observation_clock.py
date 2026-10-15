"""A normal resume may wait while the last research Job commits its output."""

import asyncio
from datetime import datetime, timedelta

import pytest
from integration import test_candidate_multistock_path as path
from nexus.ops import agent_paper_once as once
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperOrderRecord,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ["completed", "session_closed", "future_finish"])
async def test_v10_resume_observes_jobs_committed_after_entry_time(
    migrated_database_url, monkeypatch, observation
):
    original_worker = path.run_agent_worker_once
    original_get = AsyncSession.get
    entered_read = asyncio.Event()
    allow_read = asyncio.Event()
    resume_task = None
    worker_calls = 0

    class EntryClock(datetime):
        @classmethod
        def now(cls, tz=None):
            # Captured before the final worker commits at AT + 1 second.
            at = path.AT + timedelta(milliseconds=500)
            return at if tz is None else at.astimezone(tz)

    class TerminalObserved(Exception):
        pass

    async def get(session, entity, *args, **kwargs):
        if entity is AgentPaperCycleRecord and asyncio.current_task() is resume_task:
            entered_read.set()
            await allow_read.wait()
        return await original_get(session, entity, *args, **kwargs)

    async def complete_last_worker(settings, **kwargs):
        nonlocal worker_calls, resume_task
        worker_calls += 1
        # The existing scenario checks each completed Job cannot be called again.
        if worker_calls != 5:
            return await original_worker(settings, **kwargs)
        engine = create_async_engine(migrated_database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                cycle_id = await session.scalar(select(AgentPaperCycleRecord.id))
            original_scalar = AsyncSession.scalar
            observed_at = (
                path.AT
                + {
                    "completed": timedelta(seconds=1),
                    "session_closed": timedelta(hours=7),
                    "future_finish": timedelta(milliseconds=750),
                }[observation]
            )

            async def clock(session, statement, *args, **kwargs):
                if (
                    asyncio.current_task() is resume_task
                    and str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1"
                ):
                    return observed_at
                return await original_scalar(session, statement, *args, **kwargs)

            monkeypatch.setattr(AsyncSession, "scalar", clock)
            resume_task = asyncio.create_task(
                once.resume_once(
                    settings,
                    once.AgentPaperResumeRequest(
                        action="resume",
                        cycle_id=cycle_id,
                        idempotency_key="resume-before-last-worker-commit",
                        formal_stage_capacity=True,
                    ),
                )
            )
            await asyncio.wait_for(entered_read.wait(), timeout=10)
            completed = await original_worker(settings, **kwargs)
            assert completed is True
            allow_read.set()
            result = await asyncio.wait_for(resume_task, timeout=30)
            async with async_sessionmaker(engine)() as session:
                last = await session.scalar(
                    select(AgentPaperCycleEventRecord)
                    .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
                    .order_by(AgentPaperCycleEventRecord.created_at.desc())
                    .limit(1)
                )
                diagnostic = last.fact_ids
                if observation != "completed":
                    assert result.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
                    assert diagnostic["research_admission_at"] == observed_at.isoformat()
                    assert diagnostic["failure_code"] == (
                        "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
                        if observation == "session_closed"
                        else "RESEARCH_OUTPUT_NOT_ACTIONABLE"
                    )
                    for model in (AgentPortfolioSynthesisRunRecord, PaperOrderRecord):
                        assert await session.scalar(select(func.count()).select_from(model)) == 0
                    raise TerminalObserved
            assert result.result_code == "PORTFOLIO_SYNTHESIS_PENDING", diagnostic
            return completed
        finally:
            allow_read.set()
            if resume_task is not None and not resume_task.done():
                resume_task.cancel()
                await asyncio.gather(resume_task, return_exceptions=True)
            await engine.dispose()

    monkeypatch.setattr(once, "datetime", EntryClock)
    monkeypatch.setattr(AsyncSession, "get", get)
    monkeypatch.setattr(path, "run_agent_worker_once", complete_last_worker)
    # Also checks synthesis, deterministic risk, orders, execution and readback.
    scenario = path.test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url, monkeypatch, 3, True, "formal_v10"
    )
    if observation == "completed":
        await scenario
    else:
        with pytest.raises(TerminalObserved):
            await scenario
    assert resume_task is not None and resume_task.done()
