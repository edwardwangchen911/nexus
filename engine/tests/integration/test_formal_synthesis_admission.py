"""Normal formal research to synthesis admission with controlled SDK responses."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
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
@pytest.mark.parametrize("protected", [False, True], ids=["unprotected", "budget-protected"])
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
async def test_formal_multistock_research_admits_exact_portfolio_synthesis(
    migrated_database_url, monkeypatch, count, trade, protected
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
    if protected:
        from sqlalchemy.ext.asyncio import AsyncSession

        original_scalar = AsyncSession.scalar

        async def database_clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return clock_at[0]
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", database_clock)

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
    monkeypatch.setattr("nexus.ops.agent_paper_once.datetime", WorkerClock)
    monkeypatch.setattr(
        "nexus.ops.agent_paper_once.local_settings", lambda *args, **kwargs: settings
    )
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
                created_at=AT - timedelta(hours=1) if protected else AT,
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
            if protected:
                from hashlib import sha256

                from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
                from nexus.paper.agent_cycle import reserve_schwab_agent_paper_trigger
                from nexus.paper.formal_budget_protection import apply_protection
                from nexus.persistence.models.agents import AgentPromptVersionRecord

                clock_at[0] = AT - timedelta(hours=1)
                if (
                    await session.scalar(
                        select(AgentPromptVersionRecord).where(
                            AgentPromptVersionRecord.role == "portfolio_decision",
                            AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
                        )
                    )
                    is None
                ):
                    content = "Synthetic protected portfolio prompt."
                    session.add(
                        AgentPromptVersionRecord(
                            role="portfolio_decision",
                            version=SYNTHESIS_PROMPT_VERSION,
                            content=content,
                            content_hash=sha256(content.encode()).hexdigest(),
                            created_at=clock_at[0],
                        )
                    )
                    await session.flush()
                binding = await apply_protection(
                    session,
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    enabled=True,
                    daily_limit_usd=Decimal("3"),
                    configuration_hash="a" * 64,
                    idempotency_key="protected-formal-chain",
                    formal_model_config_id=model.id,
                )
                clock_at[0] = AT
                await reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=authority.account_id,
                    client_idempotency_key=f"candidate-{count}",
                    now=AT,
                    candidate_batch_id=batch.batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=model.id,
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
        clock_at[0] = AT + timedelta(seconds=1)
        await _set_test_insert_clock(sessions, clock_at[0])
        from nexus.ops.agent_paper_once import (
            AgentPaperResumeRequest,
            AgentPaperRunResearchRequest,
            resume_once,
            run_research_once,
        )

        if count == 3 and not trade:
            with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
                await run_research_once(
                    settings,
                    AgentPaperRunResearchRequest(action="run-research", cycle_id=result.cycle_id),
                    repo_root=Path.cwd(),
                )
            assert calls == []
            async with sessions() as session:
                untouched = tuple(await session.scalars(select(Job)))
                assert all(job.status == "queued" and job.attempt_count == 0 for job in untouched)

        from nexus.ops.agent_paper_loop import AgentPaperLoopAction, execute_agent_paper_loop_action
        from unit.ops.test_agent_paper_loop import snapshot

        host_settings = EngineSettings(
            database_url=migrated_database_url,
            agent_runtime_mode="codex_sdk",
            NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=str(model.id),
            NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED=protected,
        )
        if protected:
            environment = host_settings.model_dump() | {
                "DATABASE_URL": migrated_database_url,
                "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
                "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
                "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(authority.account_id),
                "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(batch.batch_id),
                "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(model.id),
                "AGENT_DAILY_BUDGET_USD": "3",
            }
            monkeypatch.setattr(
                "nexus.ops.agent_paper_loop.load_agent_paper_service_environment",
                # Pending model must never replace the cycle's applied binding.
                lambda *a, **kw: environment
                | {"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=999))},
            )
            async with sessions() as session:
                from nexus.persistence.models.paper import AgentPaperCycleRecord

                cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
                assert cycle.formal_budget_binding_id == binding.id
        from nexus.ops.agent_paper_formal import require_formal_host_trigger_profile

        await require_formal_host_trigger_profile(host_settings, now=AT)
        with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
            await require_formal_host_trigger_profile(
                host_settings.model_copy(
                    update={"agent_paper_formal_model_config_id": UUID(int=999)}
                ),
                now=AT,
            )
        assert calls == []
        monkeypatch.setattr(
            "nexus.ops.agent_paper_loop.local_settings",
            lambda *a, **kw: host_settings.model_copy(
                update={"agent_paper_formal_model_config_id": UUID(int=999)}
            )
            if protected
            else host_settings,
        )

        async def controlled_sdk_ready():
            return None

        monkeypatch.setattr(
            "nexus.ops.agent_paper_loop._codex_sdk_preflight_blocker_async", controlled_sdk_ready
        )
        if count == 3 and not trade and not protected:
            for bad_settings, bad_account, bad_job in (
                (settings, authority.account_id, jobs[0].id),
                (
                    host_settings.model_copy(
                        update={"agent_paper_formal_model_config_id": UUID(int=999)}
                    ),
                    authority.account_id,
                    jobs[0].id,
                ),
                (host_settings, UUID(int=999), jobs[0].id),
                (host_settings, authority.account_id, UUID(int=999)),
            ):
                with pytest.raises(ValueError, match="CODEX_FORMAL"):
                    await execute_agent_paper_loop_action(
                        bad_settings,
                        account_id=bad_account,
                        repo_root=Path.cwd(),
                        action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
                        snapshot=snapshot(
                            calendar_authority_id=authority.calendar_authority_id,
                            now=clock_at[0],
                            cycle_id=result.cycle_id,
                            research_job_id=bad_job,
                            research_job_status="queued",
                        ),
                    )
            assert calls == []
            async with sessions() as session:
                assert all(
                    job.status == "queued" and job.attempt_count == 0
                    for job in await session.scalars(select(Job))
                )
        if protected:
            assert (
                await execute_agent_paper_loop_action(
                    host_settings,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
                    snapshot=snapshot(
                        now=clock_at[0],
                        cycle_id=result.cycle_id,
                        calendar_authority_id=UUID(int=999),
                        research_job_id=jobs[0].id,
                    ),
                )
                == AgentPaperLoopAction.STOP_CALENDAR_AUTHORITY_MISMATCH
            )
            assert calls == []
            async with sessions() as session:
                from nexus.persistence.models.paper import AgentPaperCycleRecord

                cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
                assert cycle.formal_budget_binding_id == binding.id
                assert all(job.attempt_count == 0 for job in await session.scalars(select(Job)))
        for job in jobs:
            assert (
                await execute_agent_paper_loop_action(
                    host_settings,
                    account_id=authority.account_id,
                    repo_root=Path.cwd(),
                    action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
                    snapshot=snapshot(
                        calendar_authority_id=authority.calendar_authority_id,
                        now=clock_at[0],
                        cycle_id=result.cycle_id,
                        research_job_id=job.id,
                        research_job_status="queued",
                    ),
                )
                is None
            )
        for job in jobs:
            assert not await run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
        clock_at[0] = AT + timedelta(seconds=2)
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
            from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
            from nexus.persistence.models.paper import AgentPaperCycleEventRecord

            tracked = (IdempotencyRecord, AgentPaperCycleEventRecord, Job)
            before = [await session.scalar(select(func.count()).select_from(t)) for t in tracked]
            for changes in (
                {},
                {"formal_stage_capacity": 1},
                {"formal_stage_capacity": True, "candidate_stage_capacity": True},
            ):
                with pytest.raises(ValueError, match="CODEX_FORMAL"):
                    await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=result.cycle_id,
                        idempotency_key="reject-mode",
                        now=AT + timedelta(seconds=2),
                        **changes,
                    )
            assert [
                await session.scalar(select(func.count()).select_from(t)) for t in tracked
            ] == before
            pending = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=result.cycle_id,
                idempotency_key="formal-admit-synthesis",
                now=AT + timedelta(seconds=2),
                daily_limit_usd=Decimal("3"),
                formal_stage_capacity=True,
            )
            assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
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
        assert (
            await execute_agent_paper_loop_action(
                host_settings,
                account_id=authority.account_id,
                repo_root=Path.cwd(),
                action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
                snapshot=snapshot(
                    calendar_authority_id=authority.calendar_authority_id,
                    now=clock_at[0],
                    cycle_id=result.cycle_id,
                    research_job_id=synthesis_job.id,
                    research_job_status="queued",
                ),
            )
            is None
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
        from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
        from nexus.persistence.models.paper import (
            AgentPaperCycleRecord,
            PaperOrderRecord,
            PaperTargetVersionRecord,
            RiskEvaluationRecord,
            StrategyDecisionAgentOutputRecord,
        )

        await _set_test_insert_clock(sessions, AT + timedelta(seconds=4))
        clock_at[0] = AT + timedelta(seconds=4)
        if count == 3 and not trade:
            from integration.test_portfolio_management_e2e import _business_facts

            async with sessions() as session:
                before_default_resume = await _business_facts(session)
            with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
                await resume_once(
                    settings,
                    AgentPaperResumeRequest(
                        action="resume",
                        cycle_id=result.cycle_id,
                        idempotency_key="default-must-not-consume-formal",
                    ),
                )
            async with sessions() as session:
                assert await _business_facts(session) == before_default_resume
        assert (
            await execute_agent_paper_loop_action(
                host_settings,
                account_id=authority.account_id,
                repo_root=Path.cwd(),
                action=AgentPaperLoopAction.RESUME,
                snapshot=snapshot(
                    calendar_authority_id=authority.calendar_authority_id,
                    now=clock_at[0],
                    cycle_id=result.cycle_id,
                ),
            )
            is None
        )
        from integration.test_portfolio_management_e2e import _business_facts

        async with sessions() as session:
            from nexus.persistence.models.paper import AgentPaperCycleEventRecord
            from nexus.persistence.models.runtime import IdempotencyRecord

            latest_host_event = await session.scalar(
                select(AgentPaperCycleEventRecord)
                .where(AgentPaperCycleEventRecord.cycle_id == result.cycle_id)
                .order_by(AgentPaperCycleEventRecord.sequence.desc())
            )
            assert latest_host_event.result_code == (
                "PAPER_ORDERS_AWAITING_MARKET" if trade else "NO_ORDER_REQUIRED"
            )
            assert latest_host_event.to_status == ("awaiting_market" if trade else "completed")
            host_target = await session.scalar(select(PaperTargetVersionRecord))
            assert (
                str(host_target.strategy_decision_id)
                == latest_host_event.fact_ids["strategy_decision_id"]
            )
            assert (
                await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.actor_id
                        == f"local-agent-paper-schwab-resume:{result.cycle_id}",
                        IdempotencyRecord.idempotency_key
                        == f"agent-paper-loop-resume:{result.cycle_id}",
                    )
                )
                is not None
            )
            before_resume_readback = await _business_facts(session)
        finished = await resume_once(
            settings,
            AgentPaperResumeRequest(
                action="resume",
                cycle_id=result.cycle_id,
                idempotency_key="formal-activate-target",
                formal_stage_capacity=True,
            ),
        )
        assert finished.result_code == (
            "PAPER_ORDERS_AWAITING_MARKET" if trade else "NO_ORDER_REQUIRED"
        ), finished
        assert bool(finished.order_ids) is trade
        async with sessions() as session:
            assert await _business_facts(session) == before_resume_readback
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            assert cycle.execution_locked is True
            target = await session.scalar(select(PaperTargetVersionRecord))
            assert target.strategy_decision_id == finished.strategy_decision_id
            from nexus.paper.agent_decision_store import _load_portfolio_target_baseline

            stored_formal = await _load_portfolio_target_baseline(
                session,
                target=target,
                account_id=authority.account_id,
                candidate_stage_capacity=False,
                formal_stage_capacity=True,
            )
            assert stored_formal.decision_id == finished.strategy_decision_id

            assert (
                await session.scalar(
                    select(func.count()).select_from(StrategyDecisionAgentOutputRecord)
                )
                == count
            )
            orders = tuple(await session.scalars(select(PaperOrderRecord)))
            assert len(orders) == (2 if trade else 0)
            if trade:
                assert {order.instrument_id for order in orders} == {
                    choice.instrument_id for choice in portfolio_intent.choices[:2]
                }
                assert all(order.target_weight == Decimal("0.05") for order in orders)

            assert {order.id for order in orders} == set(finished.order_ids)
            assert all(
                order.instrument_id in batch.instrument_ids
                and order.side == "buy"
                and order.quantity > 0
                and order.risk_outcome in {"approved", "reduced"}
                for order in orders
            )
            assert await session.scalar(
                select(func.count()).select_from(RiskEvaluationRecord)
            ) == len(orders)
            tables = (Job, PaperOrderRecord, PaperTargetVersionRecord, RiskEvaluationRecord)
            before = [await session.scalar(select(func.count()).select_from(t)) for t in tables]
            with pytest.raises(ValueError, match="CODEX_FORMAL"):
                await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=result.cycle_id,
                    idempotency_key="default-terminal-replay",
                    now=AT + timedelta(seconds=5),
                )
            replay = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=result.cycle_id,
                idempotency_key="formal-activate-target",
                now=AT + timedelta(seconds=5),
                formal_stage_capacity=True,
            )
            assert replay.order_ids == finished.order_ids
            assert replay.strategy_decision_id == finished.strategy_decision_id
            assert [
                await session.scalar(select(func.count()).select_from(t)) for t in tables
            ] == before
        if trade:
            from integration.test_candidate_multistock_path import _verify_internal_fills

            await _verify_internal_fills(
                settings,
                sessions,
                result.cycle_id,
                authority.account_id,
                orders,
                count,
                verify_reads=False,
                host_settings=host_settings,
                monkeypatch=monkeypatch,
                protected_environment=environment if protected else None,
                calendar_authority_id=authority.calendar_authority_id,
                database_clock_at=clock_at if protected else None,
            )
        else:
            from nexus.paper.projection import load_paper_projection
            from nexus.persistence.models.paper import PaperFillRecord, PaperLedgerTransactionRecord

            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(PaperLedgerTransactionRecord)
                        .where(PaperLedgerTransactionRecord.transaction_type == "fill")
                    )
                    == 0
                )
                unchanged = await load_paper_projection(
                    session, account_id=authority.account_id, as_of=AT + timedelta(seconds=5)
                )
                assert unchanged.positions == projection.positions
                assert unchanged.available_cash == projection.available_cash
                assert unchanged.reserved_cash == projection.reserved_cash
                assert not unchanged.open_orders
        if protected:
            from nexus.persistence.models.agents import (
                AgentDailyBudgetReservationRecord,
                AgentPortfolioSynthesisBudgetReservationRecord,
            )

            async with sessions() as session:
                claim = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.actor_id
                        == f"local-agent-paper-schwab:{authority.account_id}",
                        IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                        IdempotencyRecord.idempotency_key == AT.isoformat(),
                    )
                )
                assert claim is not None and claim.formal_budget_binding_id == binding.id
                reservations = tuple(
                    await session.scalars(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id.in_(result.research_run_ids)
                        )
                    )
                )
                assert {row.run_id for row in reservations} == set(result.research_run_ids)
                synth_reservations = tuple(
                    await session.scalars(
                        select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                            AgentPortfolioSynthesisBudgetReservationRecord.run_id == first.run.id
                        )
                    )
                )
                assert len(synth_reservations) == 1
                assert all(
                    row.state == "settled"
                    and row.business_date == AT.date()
                    and row.pricing_version == model.version
                    and row.settled_usd == 0
                    for row in (*reservations, *synth_reservations)
                )
        import httpx
        from helpers.paper_api import app, headers
        from integration.test_portfolio_management_e2e import _business_facts
        from nexus.persistence.models.paper import AgentPaperMarketWindowRecord

        async with sessions() as session:
            window_end = await session.scalar(
                select(func.max(AgentPaperMarketWindowRecord.window_end_at))
            )
            api_now = (window_end or AT) + timedelta(seconds=5)
            before_reads = await _business_facts(session)
        if count == 3 and not trade:
            from nexus.paper.management_read_authority import (
                formal_proposal_read_profile,
                load_synthesis_read_authority,
            )
            from sqlalchemy import text

            async with sessions.begin() as session:
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
                assert (
                    await formal_proposal_read_profile(
                        session,
                        account_id=authority.account_id,
                        decision_id=finished.strategy_decision_id,
                        read_as_of=api_now,
                    )
                    is True
                )
                cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
                before_bad_read = await _business_facts(session)
                with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
                    await load_synthesis_read_authority(session, cycle=cycle, read_as_of=api_now)
                assert await _business_facts(session) == before_bad_read
                await savepoint.rollback()
        paths = (
            "/internal/v1/paper/orders?limit=50",
            "/internal/v1/paper/actual",
            f"/internal/v1/paper/management/cycles/{result.cycle_id}",
            "/internal/v1/paper/management/portfolio",
            "/internal/v1/paper/cycles?limit=20",
        )
        envelopes = []
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(settings.database_url, now=api_now)),
            base_url="http://testserver",
        ) as client:
            for path in paths:
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200, (path, response.json())
                envelopes.append(response.json()["data"])
            assert (await client.get(paths[0])).status_code == 401
        assert {row["order_id"] for row in envelopes[0]["items"]} == {
            str(order.id) for order in orders
        }
        assert all(row["status"] == "filled" for row in envelopes[0]["items"])
        assert envelopes[2]["cycle"]["cycle_id"] == str(result.cycle_id)
        assert envelopes[2]["cycle"]["execution_locked"] is True
        if trade:
            import os

            if os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1":
                from helpers.controlled_browser import verify_controlled_browser

                await verify_controlled_browser(
                    settings.database_url,
                    api_now=api_now,
                    cycle_id=result.cycle_id,
                    order_ids=tuple(order.id for order in orders),
                    candidate_count=count,
                    expected_defer_count=0,
                    formal_readback=True,
                )
        async with sessions() as session:
            assert await _business_facts(session) == before_reads
    finally:
        await engine.dispose()
