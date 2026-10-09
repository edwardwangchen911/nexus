"""Normal host formal trigger with controlled source boundaries and real PostgreSQL."""

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.ops import agent_paper_loop as loop
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_agent_paper_loop import snapshot
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
async def test_host_preparation_and_formal_trigger_freeze_exact_batch(
    migrated_database_url, monkeypatch, count
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    preparation_at = AT - timedelta(seconds=1)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        candidate_instruments=FIVE[:count],
        decision_time=preparation_at,
    )
    try:
        await _set_test_insert_clock(sessions, preparation_at)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key=f"host-formal-batch-{count}",
                created_at=preparation_at,
            )
        host = EngineSettings(
            database_url=migrated_database_url,
            agent_runtime_mode="codex_sdk",
            NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=str(model.id),
        )
        environment = host.model_dump() | {
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(batch.batch_id),
            "ALPACA_API_KEY_ID": "controlled-key",
            "ALPACA_API_SECRET_KEY": "controlled-secret",
            "object_store_endpoint": "http://127.0.0.1:9",
        }
        monkeypatch.setattr(
            loop, "load_agent_paper_service_environment", lambda *a, **kw: environment
        )
        clock_at = [preparation_at]
        original_scalar = AsyncSession.scalar

        async def controlled_clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return clock_at[0]
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", controlled_clock)
        preparations = []

        def preparation(phase):
            async def ready(*args, **kwargs):
                assert kwargs["account_id"] == authority.account_id
                assert kwargs["expected_calendar_authority_id"] == authority.calendar_authority_id
                candidate = kwargs.get("candidate")
                if candidate is not None:
                    assert candidate.instrument_id in batch.instrument_ids
                preparations.append((phase, None if candidate is None else candidate.instrument_id))
                return "ready"

            return ready

        for name, phase in (
            ("prepare_prior_daily_once", "daily"),
            ("prepare_macro_once", "macro"),
            ("prepare_current_trends_once", "trends"),
            ("prepare_agent_paper_identity_once", "identity"),
        ):
            monkeypatch.setattr(loop, name, preparation(phase))
        source_calls = []

        async def controlled_authority(*args, **kwargs):
            assert kwargs["decision_slot_at"] == AT
            assert kwargs["expected_calendar_authority_id"] == authority.calendar_authority_id
            assert (
                tuple(item.instrument_id for item in kwargs["identities"]) == batch.instrument_ids
            )
            source_calls.append(batch.instrument_ids)
            async with sessions() as session:
                return await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=authority.account_id,
                    now=AT,
                    expected_calendar_authority_id=authority.calendar_authority_id,
                )

        monkeypatch.setattr(
            "nexus.ops.agent_paper_once.prepare_schwab_decision_authority", controlled_authority
        )
        monkeypatch.setattr(
            "nexus.ops.agent_paper_once.S3ProviderEvidenceStore", lambda *a, **kw: object()
        )
        tables = (AgentPaperCycleRecord, AgentResearchRunRecord, Job, IdempotencyRecord)

        async def counts():
            async with sessions() as session:
                return tuple(
                    [await session.scalar(select(func.count()).select_from(t)) for t in tables]
                )

        before = await counts()
        preparation_snapshot = snapshot(
            now=preparation_at,
            calendar_authority_id=authority.calendar_authority_id,
            market_open_at=AT - timedelta(minutes=30),
        )
        current = snapshot(now=AT, calendar_authority_id=authority.calendar_authority_id)
        for action in (loop.AgentPaperLoopAction.PREPARE_INPUTS, loop.AgentPaperLoopAction.TRIGGER):
            action_snapshot = (
                preparation_snapshot
                if action == loop.AgentPaperLoopAction.PREPARE_INPUTS
                else current
            )
            clock_at[0] = action_snapshot.now
            environment["agent_paper_formal_model_config_id"] = UUID(int=999)
            with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
                await loop.execute_agent_paper_loop_action(
                    host,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=action,
                    snapshot=action_snapshot,
                )
            invalid_host = host.model_copy(
                update={"agent_paper_formal_model_config_id": UUID(int=999)}
            )
            with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
                await loop.execute_agent_paper_loop_action(
                    invalid_host,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=action,
                    snapshot=action_snapshot,
                )
            assert preparations == source_calls == []
            assert await counts() == before
        clock_at[0] = preparation_at
        environment["agent_paper_formal_model_config_id"] = model.id
        assert (
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=authority.account_id,
                repo_root=Path.cwd(),
                action=loop.AgentPaperLoopAction.PREPARE_INPUTS,
                snapshot=preparation_snapshot,
            )
            is None
        )
        assert len(preparations) == 3 * count + 1
        for phase in ("daily", "macro", "trends"):
            assert tuple(instrument for step, instrument in preparations if step == phase) == (
                batch.instrument_ids
            )
        clock_at[0] = AT
        assert await counts() == before
        assert (
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=authority.account_id,
                repo_root=Path.cwd(),
                action=loop.AgentPaperLoopAction.TRIGGER,
                snapshot=current,
            )
            is None
        )
        assert source_calls == [batch.instrument_ids]
        async with sessions() as session:
            cycles = tuple(await session.scalars(select(AgentPaperCycleRecord)))
            assert len(cycles) == 1
            cycle = cycles[0]
            assert cycle.account_id == authority.account_id and cycle.execution_locked
            assert cycle.candidate_batch_id == batch.batch_id and cycle.decision_slot_at == AT
            first = await session.get(AgentPaperCycleEventRecord, (cycle.id, 1))
            assert first.fact_ids == {
                "formal_stage_capacity": True,
                "formal_model_config_id": str(model.id),
                "formal_model_hash": model.config_hash,
            }
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            assert len(runs) == len(jobs) == count
            assert all(job.status == "queued" and job.attempt_count == 0 for job in jobs)
            assert {run.instrument_id for run in runs} == set(batch.instrument_ids)
            assert all(run.model_config_id == model.id for run in runs)
            assert all([await formal_stage_job_qualified(session, job) for job in jobs])
            claim = await session.scalar(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.idempotency_key == AT.isoformat(),
                )
            )
            assert claim.response_payload["outcome"] == "completed"
            assert claim.response_payload["result"]["cycle_id"] == str(cycle.id)
        after = await counts()
        assert (
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=authority.account_id,
                repo_root=Path.cwd(),
                action=loop.AgentPaperLoopAction.TRIGGER,
                snapshot=current,
            )
            is None
        )
        assert await counts() == after
        assert source_calls == [batch.instrument_ids]
    finally:
        await engine.dispose()
