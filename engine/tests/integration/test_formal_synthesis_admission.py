"""Normal formal research to synthesis admission with controlled SDK responses."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.models import JobType
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select
from unit.ops.test_formal_stage_registration import OPTIONS

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
FIVE = INSTRUMENTS + (
    (UUID(int=0x51000000000070008000000000000014), Decimal("70"), Decimal("0"), "industrials"),
    (UUID(int=0x51000000000070008000000000000015), Decimal("80"), Decimal("0"), "consumer_staples"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
async def test_formal_multistock_research_admits_exact_portfolio_synthesis(
    migrated_database_url, monkeypatch, count, trade
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url,
        monkeypatch,
        candidate_instruments=FIVE[:count],
        decision_time=AT,
    )
    calls = []

    clock_at = [AT + timedelta(seconds=1)]

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            at = clock_at[0]
            return at if tz is None else at.astimezone(tz)

    async def invoke(payload, **kwargs):
        stage = payload["stage"]
        calls.append(stage)
        assert kwargs["timeout_seconds"] > 0
        if stage == "synthesis":
            response = {
                "synthesis_assessment": {
                    "stance": "buy_candidate" if trade else "watch",
                    "claims": [{"text": "受控来源支持本场景保持观察。", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": claim,
                            "status": "accepted",
                            "rationale": "冻结证据已核对。",
                            "citations": [0],
                            "resolved": True,
                            "missing_evidence": [],
                            "prevented_conclusion": None,
                        }
                        for claim in ("bear:0", "evidence_audit:0")
                    ],
                    "review_conclusion": "sufficient",
                    "uncertainties": [],
                    "conflicting_facts": False,
                }
            }
        else:
            response = {
                "roles": [
                    {
                        "role": prompt["role"],
                        "vote": "support",
                        "claims": [{"text": "受控证据引用在冻结输入中。", "citations": [0]}],
                        "reviewed_claim_ids": [] if stage == "research" else ["bull:0"],
                        "conflicting_facts": False,
                    }
                    for prompt in payload["prompts"]
                ]
            }
        return json.dumps(response), UsageSnapshot(input_tokens=100, output_tokens=100)

    monkeypatch.setattr("nexus.worker._ONCE_STARTED_AT", AT)
    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            assert model.created_at < AT
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(entry.instrument_id for entry in manifest),
                idempotency_key=f"fresh-{count}",
                created_at=AT,
            )
            from nexus.paper.projection import load_paper_projection
            from nexus.paper.target_service import approve_initial_target_projection

            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=AT
            )
            await approve_initial_target_projection(
                session,
                account_id=authority.account_id,
                projection_as_of=AT,
                expected_projection_hash=projection.projection_hash,
                approved_by="controlled-fixture-user",
                approved_at=AT,
                idempotency_key="controlled-initial-baseline",
            )
            result = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key=f"candidate-{count}",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            assert result.result_code == "RESEARCH_JOBS_PREPARED", result
            assert len(result.research_run_ids) == count
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            assert len(runs) == len(jobs) == count
            assert {run.instrument_id for run in runs} == set(batch.instrument_ids)
            assert all(run.model_config_id == model.id for run in runs)
            assert all(
                job.job_type == "agent.paper.research" and job.attempt_count == 0 for job in jobs
            )
        await _set_test_insert_clock(sessions, clock_at[0])
        for job in jobs:
            assert await run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
            assert not await run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
        assert calls == ["research", "critique", "synthesis"] * count
        async with sessions() as session:
            for run_id in result.research_run_ids:
                run = await session.get(AgentResearchRunRecord, run_id)
                assert run.status == "completed" and run.retry_count == 0
                from nexus.agents.actionable_research import actionable_intent_shape_valid
                from nexus.persistence.models.agents import AgentResearchOutputRecord
                from nexus.strategy.intents import ResearchIntent

                terminal = await session.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == run_id,
                        AgentResearchOutputRecord.role.is_(None),
                        AgentResearchOutputRecord.status == "completed",
                    )
                )
                intent = ResearchIntent.model_validate(terminal.intent)
                assert actionable_intent_shape_valid(intent), (
                    intent.quality_status,
                    intent.evidence_audit,
                    intent.stance,
                )
                assert terminal.payload.get("quality_status") == intent.quality_status
                action = await load_actionable_research_output(
                    session,
                    run_id=run_id,
                    decision_time=AT + timedelta(seconds=2),
                    allow_safe_no_trade=True,
                    formal_stage_capacity=True,
                )
                assert action.instrument_id in batch.instrument_ids
                assert action.stance == ("buy_candidate" if trade else "watch")
        from nexus.agents.actionable_research import ActionableResearchError
        from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
        from nexus.paper.projection import load_paper_projection
        from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
        from nexus.persistence.models.runtime import IdempotencyRecord
        from sqlalchemy import func

        async with sessions.begin() as session:
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=AT
            )
            options = dict(
                cycle_id=result.cycle_id,
                research_output_ids=tuple(
                    action.output_id
                    for action in [
                        await load_actionable_research_output(
                            session,
                            run_id=run_id,
                            decision_time=AT + timedelta(seconds=2),
                            allow_safe_no_trade=True,
                            formal_stage_capacity=True,
                        )
                        for run_id in result.research_run_ids
                    ]
                ),
                active_target_version_id=None,
                projection_hash=projection.projection_hash,
                daily_limit_usd=Decimal("3"),
                now=AT + timedelta(seconds=2),
                formal_stage_capacity=True,
            )
            first = await admit_portfolio_synthesis(session, **options)
            assert first.run.model_config_id == model.id
            assert first.run.model_hash == model.config_hash
            assert first.run.pricing_version == model.version
            assert len(first.run.research_output_ids) == count
        async with sessions.begin() as session:
            second = await admit_portfolio_synthesis(session, **options)
            assert second.replayed and second.run_id == first.run_id
            before = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await admit_portfolio_synthesis(
                    session, **(options | {"formal_stage_capacity": False})
                )
            for changes in ({"formal_stage_capacity": 1}, {"candidate_stage_capacity": True}):
                with pytest.raises(ValueError, match="CODEX_FORMAL"):
                    await admit_portfolio_synthesis(session, **(options | changes))
            assert (
                await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == before
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == count + 1
        from nexus.worker import run_portfolio_synthesis_worker_once

        async with sessions() as session:
            synthesis_job = await session.scalar(
                select(Job).where(Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value)
            )
        # Default and v7 workers must leave a pending formal Job untouched.
        assert not await run_portfolio_synthesis_worker_once(settings, job_ids=(synthesis_job.id,))
        assert not await run_portfolio_synthesis_worker_once(
            settings, job_ids=(synthesis_job.id,), candidate_stage_capacity=True
        )
        async with sessions() as session:
            unchanged = await session.get(Job, synthesis_job.id, populate_existing=True)
            assert unchanged.status == "queued" and unchanged.attempt_count == 0
        from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
        from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord
        from nexus.persistence.models.runtime import JobAttempt

        portfolio_intent = synthesis_intent(first.run)
        if trade:
            portfolio_intent = portfolio_intent.model_copy(
                update={
                    "choices": tuple(
                        choice.model_copy(
                            update={
                                "action": "include",
                                "role": "tactical",
                                "rank": index,
                            }
                        )
                        for index, choice in enumerate(portfolio_intent.choices, start=1)
                    )
                }
            )
        synthesis_calls = []

        async def invoke_portfolio(payload, **kwargs):
            synthesis_calls.append("portfolio")
            return portfolio_intent.model_dump_json(), UsageSnapshot(
                input_tokens=100, output_tokens=100, provider_calls=1
            )

        monkeypatch.setattr(
            "nexus.agents.portfolio_synthesis_worker.invoke_codex_portfolio", invoke_portfolio
        )
        clock_at[0] = AT + timedelta(seconds=3)
        await _set_test_insert_clock(sessions, clock_at[0])
        # Fault injection is confined to a rolled-back savepoint in the owned test DB.
        # A frozen identity mismatch must stop before lease/attempt/model I/O.
        if count == 3 and not trade:
            from nexus.agents.portfolio_synthesis_worker import formal_synthesis_job_qualified
            from nexus.jobs.models import QueueName
            from nexus.jobs.repository import claim_next_job
            from nexus.persistence.models.runtime import JobLease
            from sqlalchemy import text

            async with sessions.begin() as session:
                savepoint = await session.begin_nested()
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                await session.execute(
                    text("UPDATE jobs SET input_hash = :hash WHERE id = :id"),
                    {"hash": "0" * 64, "id": synthesis_job.id},
                )
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))

                async def guard(locked_session, job):
                    return await formal_synthesis_job_qualified(
                        locked_session, job, now=clock_at[0]
                    )

                assert (
                    await claim_next_job(
                        session,
                        queue=QueueName.GENERAL,
                        worker_id="formal-corruption-test",
                        now=clock_at[0],
                        lease_seconds=120,
                        job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
                        job_ids=(synthesis_job.id,),
                        locked_job_guard=guard,
                        formal_synthesis_profile=True,
                    )
                    is None
                )
                for record in (JobAttempt, JobLease):
                    assert (
                        await session.scalar(
                            select(func.count())
                            .select_from(record)
                            .where(record.job_id == synthesis_job.id)
                        )
                        == 0
                    )
                assert synthesis_calls == []
                await savepoint.rollback()
        assert await run_portfolio_synthesis_worker_once(
            settings, job_ids=(synthesis_job.id,), formal_stage_capacity=True
        )
        assert not await run_portfolio_synthesis_worker_once(
            settings, job_ids=(synthesis_job.id,), formal_stage_capacity=True
        )
        assert synthesis_calls == ["portfolio"]
        async with sessions() as session:
            completed = await session.get(AgentPortfolioSynthesisRunRecord, first.run_id)
            assert completed.status == "completed"
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisOutputRecord)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(JobAttempt)
                    .where(JobAttempt.job_id == synthesis_job.id)
                )
                == 1
            )
    finally:
        await engine.dispose()
