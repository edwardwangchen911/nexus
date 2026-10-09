"""Fresh dated candidate scenarios; these are controlled facts, not live acceptance."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_candidate_research_authority import OPTIONS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
FIVE = INSTRUMENTS + (
    (UUID(int=0x51000000000070008000000000000014), Decimal("70"), Decimal("0"), "industrials"),
    (UUID(int=0x51000000000070008000000000000015), Decimal("80"), Decimal("0"), "consumer_staples"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("trade", [False, True], ids=["no-trade", "buy"])
async def test_fresh_multistock_candidate_cycle_freezes_native_jobs(
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
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version
                    == "codex-sdk-chatgpt-v7-stage-capacity-candidate"
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
                candidate_stage_capacity=True,
                candidate_model_config_id=model.id,
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
        for job in jobs:
            assert await run_agent_worker_once(
                settings, job_ids=(job.id,), candidate_stage_capacity=True
            )
            assert not await run_agent_worker_once(
                settings, job_ids=(job.id,), candidate_stage_capacity=True
            )
        assert calls == ["research", "critique", "synthesis"] * count
        async with sessions() as session:
            for run_id in result.research_run_ids:
                run = await session.get(AgentResearchRunRecord, run_id)
                assert run.status == "completed" and run.retry_count == 0
                action = await load_actionable_research_output(
                    session,
                    run_id=run_id,
                    decision_time=AT + timedelta(seconds=2),
                    allow_safe_no_trade=True,
                    candidate_stage_capacity=True,
                )
                assert action.instrument_id in batch.instrument_ids
                assert action.stance == ("buy_candidate" if trade else "watch")
        from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
        from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
        from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
        from nexus.worker import run_portfolio_synthesis_worker_once

        async with sessions.begin() as session:
            pending = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=result.cycle_id,
                idempotency_key="synthesis-ready",
                daily_limit_usd=Decimal("3"),
                now=AT + timedelta(seconds=2),
                candidate_stage_capacity=True,
            )
            assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING", pending
            synth = await session.scalar(select(AgentPortfolioSynthesisRunRecord))
            intent = synthesis_intent(synth)
            if trade:
                intent = intent.model_copy(
                    update={
                        "choices": tuple(
                            choice.model_copy(
                                update={"action": "include", "role": "tactical", "rank": index}
                            )
                            for index, choice in enumerate(intent.choices, start=1)
                        )
                    }
                )
            synth_job = await session.scalar(
                select(Job).where(Job.payload["run_id"].astext == str(synth.id))
            )

        async def invoke_portfolio(payload, **kwargs):
            return intent.model_dump_json(), UsageSnapshot(
                input_tokens=100, output_tokens=100, provider_calls=1
            )

        monkeypatch.setattr(
            "nexus.agents.portfolio_synthesis_worker.invoke_codex_portfolio", invoke_portfolio
        )
        clock_at[0] = AT + timedelta(seconds=3)
        assert await run_portfolio_synthesis_worker_once(
            settings, job_ids=(synth_job.id,), candidate_stage_capacity=True
        )
        if not trade and count == 3:
            from sqlalchemy import event
            from sqlalchemy.exc import DBAPIError

            def corrupt_frozen_hash_after_reader(conn, cursor, statement, *args):
                if not statement.lower().startswith("insert into strategy_decision_agent_outputs"):
                    return
                # Fault injection only in this isolated test transaction. The output-binding
                # trigger stays enabled; rollback restores this deliberately invalid Run.
                conn.exec_driver_sql("ALTER TABLE agent_research_runs DISABLE TRIGGER USER")
                conn.exec_driver_sql("UPDATE agent_research_runs SET model_hash=repeat('0',64)")
                conn.exec_driver_sql("ALTER TABLE agent_research_runs ENABLE TRIGGER USER")

            event.listen(
                engine.sync_engine, "before_cursor_execute", corrupt_frozen_hash_after_reader
            )
            try:
                with pytest.raises(DBAPIError, match="agent decision output binding is invalid"):
                    async with sessions.begin() as session:
                        await resume_schwab_agent_paper_cycle(
                            session,
                            cycle_id=result.cycle_id,
                            idempotency_key="corrupt-frozen-model",
                            now=AT + timedelta(seconds=4),
                            candidate_stage_capacity=True,
                        )
            finally:
                event.remove(
                    engine.sync_engine, "before_cursor_execute", corrupt_frozen_hash_after_reader
                )
            async with sessions() as session:
                assert all(
                    row.model_hash == model.config_hash
                    for row in await session.scalars(select(AgentResearchRunRecord))
                )
        async with sessions.begin() as session:
            finished = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=result.cycle_id,
                idempotency_key="activate-target",
                now=AT + timedelta(seconds=4),
                candidate_stage_capacity=True,
            )
            assert finished.result_code == (
                "PAPER_ORDERS_AWAITING_MARKET" if trade else "NO_ORDER_REQUIRED"
            ), finished
            assert bool(finished.order_ids) is trade
        from nexus.persistence.models.paper import (
            AgentPaperCycleRecord,
            PaperOrderRecord,
            PaperTargetVersionRecord,
            RiskEvaluationRecord,
            StrategyDecisionAgentOutputRecord,
        )
        from sqlalchemy import func

        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            assert cycle.execution_locked is True
            target = await session.scalar(select(PaperTargetVersionRecord))
            assert target.strategy_decision_id == finished.strategy_decision_id
            assert (
                await session.scalar(
                    select(func.count()).select_from(StrategyDecisionAgentOutputRecord)
                )
                == count
            )
            orders = tuple(await session.scalars(select(PaperOrderRecord)))
            assert len(orders) == (2 if trade else 0)
            assert {order.id for order in orders} == set(finished.order_ids)
            if trade:
                assert {order.instrument_id for order in orders} == {
                    choice.instrument_id for choice in intent.choices[:2]
                }
                assert all(order.target_weight == Decimal("0.05") for order in orders)
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
            counts_before = [
                await session.scalar(select(func.count()).select_from(table)) for table in tables
            ]
            replay = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=result.cycle_id,
                idempotency_key="activate-target",
                now=AT + timedelta(seconds=5),
                candidate_stage_capacity=True,
            )
            assert replay.order_ids == finished.order_ids
            assert replay.strategy_decision_id == finished.strategy_decision_id
            assert [
                await session.scalar(select(func.count()).select_from(table)) for table in tables
            ] == counts_before
    finally:
        await engine.dispose()
