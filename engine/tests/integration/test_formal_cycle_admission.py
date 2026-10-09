"""Explicit formal cycles freeze native batch admission, without model or provider calls."""

from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import (
    advance_agent_paper_cycle,
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
async def test_formal_cycle_freezes_complete_native_batch_and_exact_replay(
    migrated_database_url, monkeypatch, count
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        candidate_instruments=FIVE[:count],
        decision_time=AT,
    )
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
                )
            )
            assert model.created_at < AT
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key=f"formal-batch-{count}",
                created_at=AT,
            )
            args = dict(
                authority=authority,
                decision_slot_at=AT,
                idempotency_key=f"formal-cycle-{count}",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            result = await start_schwab_agent_paper_cycle(session, **args)
            assert result.result_code == "RESEARCH_JOBS_PREPARED"
            assert len(result.research_run_ids) == count
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            assert cycle.execution_locked is True
            first = await session.scalar(
                select(AgentPaperCycleEventRecord).where(
                    AgentPaperCycleEventRecord.cycle_id == cycle.id,
                    AgentPaperCycleEventRecord.sequence == 1,
                )
            )
            assert first.fact_ids == {
                "formal_stage_capacity": True,
                "formal_model_config_id": str(model.id),
                "formal_model_hash": model.config_hash,
            }
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            assert len(runs) == len(jobs) == count
            assert {run.instrument_id for run in runs} == set(batch.instrument_ids)
            assert all(run.model_config_id == model.id for run in runs)
            assert all(run.pricing_version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION for run in runs)
            assert all([await formal_stage_job_qualified(session, job) for job in jobs])
        async with sessions.begin() as session:
            replay = await start_schwab_agent_paper_cycle(session, **args)
            assert replay.replayed and replay.cycle_id == result.cycle_id
            assert replay.research_run_ids == result.research_run_ids
            before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
            for change in (
                {"formal_stage_capacity": False, "formal_model_config_id": None},
                {"formal_model_config_id": UUID(int=999)},
                {"formal_stage_capacity": 1},
                {"candidate_stage_capacity": True},
            ):
                with pytest.raises(ValueError, match="CODEX_FORMAL"):
                    await start_schwab_agent_paper_cycle(session, **(args | change))
            with pytest.raises(ValueError, match="CODEX_FORMAL"):
                await resume_schwab_agent_paper_cycle(
                    session, cycle_id=result.cycle_id, idempotency_key="default-resume", now=AT
                )
            with pytest.raises(ValueError, match="CODEX_FORMAL"):
                await advance_agent_paper_cycle(
                    session,
                    account_id=authority.account_id,
                    decision_time=AT,
                    candidate_batch_id=batch.batch_id,
                    idempotency_key="default-advance",
                )
            assert (
                await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == count
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 1
            )
    finally:
        await engine.dispose()
