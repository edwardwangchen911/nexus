"""Explicit formal cycles freeze native batch admission, without model or provider calls."""

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import (
    advance_agent_paper_cycle,
    finish_schwab_agent_paper_trigger,
    reserve_schwab_agent_paper_trigger,
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("formal_trigger", [False, True], ids=["native", "trigger"])
async def test_formal_cycle_freezes_complete_native_batch_and_exact_replay(
    migrated_database_url, monkeypatch, count, formal_trigger
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
            if formal_trigger:
                trigger_args = dict(
                    account_id=authority.account_id,
                    candidate_batch_id=batch.batch_id,
                    client_idempotency_key=f"formal-trigger-{count}",
                    now=AT,
                    require_synthesis_prompt=True,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
                )
                reservation = await reserve_schwab_agent_paper_trigger(session, **trigger_args)
                assert reservation.formal_stage_capacity is True
                assert reservation.formal_model_config_id == model.id
                assert reservation.formal_model_hash == model.config_hash
            if formal_trigger:
                from nexus.ops.agent_paper_once import (
                    AgentPaperTriggerRequest,
                    AgentPaperTriggerSettings,
                    trigger_in_session,
                )
                from pydantic import SecretStr

                original_scalar = session.scalar

                async def controlled_clock(statement, *positional, **keywords):
                    if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                        return AT
                    return await original_scalar(statement, *positional, **keywords)

                monkeypatch.setattr(session, "scalar", controlled_clock)

                async def frozen_provider_authority(*positional, **keywords):
                    assert keywords["decision_slot_at"] == AT
                    assert {item.instrument_id for item in keywords["identities"]} == set(
                        batch.instrument_ids
                    )
                    return authority

                monkeypatch.setattr(
                    "nexus.ops.agent_paper_once.prepare_schwab_decision_authority",
                    frozen_provider_authority,
                )
                result = await trigger_in_session(
                    session,
                    object(),
                    AgentPaperTriggerSettings(
                        database_url=migrated_database_url,
                        agent_runtime_mode="codex_sdk",
                        alpaca_key=SecretStr("controlled-key"),
                        alpaca_secret=SecretStr("controlled-secret"),
                    ),
                    AgentPaperTriggerRequest(
                        action="trigger",
                        account_id=authority.account_id,
                        candidate_batch_id=batch.batch_id,
                        idempotency_key=args["idempotency_key"],
                        formal_stage_capacity=True,
                        formal_model_config_id=model.id,
                    ),
                    reservation=reservation,
                    repo_root=Path.cwd(),
                    client=object(),
                )
                await finish_schwab_agent_paper_trigger(
                    session,
                    reservation=reservation,
                    result=result,
                )
            else:
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
            if formal_trigger:
                trigger_replay = await reserve_schwab_agent_paper_trigger(session, **trigger_args)
                assert trigger_replay.replay.cycle_id == result.cycle_id
                assert trigger_replay.replay.research_run_ids == result.research_run_ids
                # Corrupt only the owned test DB within a rolled-back savepoint.
                # A completed trigger receipt must not bypass frozen cycle proof.
                from sqlalchemy import text

                savepoint = await session.begin_nested()
                await session.execute(
                    text("ALTER TABLE agent_paper_cycle_events DISABLE TRIGGER USER")
                )
                damaged = await session.execute(
                    text(
                        "UPDATE agent_paper_cycle_events SET fact_ids = "
                        "jsonb_set(fact_ids, '{formal_model_hash}', to_jsonb(CAST(:hash AS text))) "
                        "WHERE cycle_id = :id AND sequence = 1"
                    ),
                    {"hash": "0" * 64, "id": result.cycle_id},
                )
                assert damaged.rowcount == 1
                await session.execute(
                    text("ALTER TABLE agent_paper_cycle_events ENABLE TRIGGER USER")
                )
                with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
                    await reserve_schwab_agent_paper_trigger(session, **trigger_args)
                await savepoint.rollback()
                with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
                    await finish_schwab_agent_paper_trigger(
                        session,
                        reservation=reservation.model_copy(update={"formal_model_hash": "0" * 64}),
                        result=result,
                    )
                with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
                    await finish_schwab_agent_paper_trigger(
                        session,
                        reservation=reservation.model_copy(
                            update={
                                "formal_stage_capacity": False,
                                "formal_model_config_id": None,
                                "formal_model_hash": None,
                            }
                        ),
                        result=result,
                    )
                for change, code in (
                    (
                        {"formal_stage_capacity": False, "formal_model_config_id": None},
                        "IDEMPOTENCY",
                    ),
                    ({"formal_model_config_id": UUID(int=999)}, "CODEX_FORMAL"),
                    ({"trigger_mode": "manual_recovery"}, "CODEX_FORMAL"),
                ):
                    with pytest.raises(ValueError, match=code):
                        await reserve_schwab_agent_paper_trigger(session, **(trigger_args | change))

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


@pytest.mark.asyncio
@pytest.mark.parametrize("historical_formal", [False, True], ids=["legacy", "formal"])
@pytest.mark.parametrize("invalid_risk", [False, True], ids=["valid-risk", "invalid-risk"])
async def test_formal_start_reads_historical_target_without_inheriting_new_profile(
    migrated_database_url, monkeypatch, historical_formal, invalid_risk
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=FIVE[:3], decision_time=AT
    )
    decision_id = UUID(int=987)
    target_reads = []

    async def historical_proposal(session, **kwargs):
        target_reads.append(kwargs)
        assert kwargs["account_id"] == authority.account_id
        assert kwargs["decision_id"] == decision_id
        if kwargs.get("formal_stage_capacity", False) is not historical_formal:
            raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
        return SimpleNamespace(proposal=SimpleNamespace(weights={}))

    monkeypatch.setattr(
        "nexus.paper.agent_decision_store.load_portfolio_proposal", historical_proposal
    )
    if invalid_risk:

        async def reject_risk(*args, **kwargs):
            raise ValueError("risk query authority is invalid")

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.prepare_schwab_realtime_risk_snapshot", reject_risk
        )
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            original_scalar = session.scalar
            original_get = session.get

            async def with_historical_decision(model_type, identifier, *args, **kwargs):
                if model_type is StrategyDecisionRecord and identifier == decision_id:
                    return SimpleNamespace(
                        account_id=authority.account_id, agent_paper_cycle_id=UUID(int=988)
                    )
                return await original_get(model_type, identifier, *args, **kwargs)

            monkeypatch.setattr(session, "get", with_historical_decision)

            async def with_historical_target(statement, *args, **kwargs):
                if any(
                    item.get("entity") is PaperTargetVersionRecord
                    for item in getattr(statement, "column_descriptions", ())
                ):
                    return SimpleNamespace(strategy_decision_id=decision_id)
                if any(
                    item.get("entity") is AgentPortfolioSynthesisRunRecord
                    for item in getattr(statement, "column_descriptions", ())
                ):
                    return SimpleNamespace(
                        pricing_version=(
                            FORMAL_STAGE_CAPACITY_CONFIG_VERSION
                            if historical_formal
                            else "codex-sdk-portfolio-decision-v1"
                        )
                    )
                return await original_scalar(statement, *args, **kwargs)

            monkeypatch.setattr(session, "scalar", with_historical_target)
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
                idempotency_key="historical-target-batch",
                created_at=AT,
            )
            result = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="historical-target-formal-start",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            assert result.result_code == (
                "RISK_INPUT_REQUIRED" if invalid_risk else "RESEARCH_JOBS_PREPARED"
            )
            assert len(target_reads) == 1
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            assert len(runs) == len(jobs) == (0 if invalid_risk else 3)
            assert all(run.model_config_id == model.id for run in runs)
            assert all(run.pricing_version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION for run in runs)
    finally:
        await engine.dispose()
