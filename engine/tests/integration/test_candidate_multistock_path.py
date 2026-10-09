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
        await _set_test_insert_clock(sessions, AT)
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
        await _set_test_insert_clock(sessions, clock_at[0])
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

        await _set_test_insert_clock(sessions, AT + timedelta(seconds=2))
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
        await _set_test_insert_clock(sessions, clock_at[0])
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
        await _set_test_insert_clock(sessions, AT + timedelta(seconds=4))
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
        if trade:
            await _verify_internal_fills(
                settings, sessions, result.cycle_id, authority.account_id, orders, count
            )
    finally:
        await engine.dispose()


async def _set_test_insert_clock(sessions, at):
    """Date fresh inserts before writes; never rewrite persisted synthetic facts."""
    from nexus.agents.codex_candidate import require_candidate_database
    from sqlalchemy import text

    assert at.tzinfo == UTC
    async with sessions.begin() as session:
        await require_candidate_database(session)
        await session.execute(
            text(
                "DO $$ DECLARE item record; BEGIN FOR item IN "
                "SELECT table_name FROM information_schema.columns "
                "WHERE table_schema='public' AND column_name='created_at' "
                "LOOP EXECUTE format('ALTER TABLE %I ALTER COLUMN created_at SET DEFAULT "
                f"''{at.isoformat()}''::timestamptz', item.table_name); END LOOP; END $$"
            )
        )


async def _verify_internal_fills(settings, sessions, cycle_id, account_id, orders, count):
    from integration.test_agent_paper_cycle_pg import _ObjectStore, _SchwabPriceHistoryProvider
    from nexus.api.errors import NexusError
    from nexus.ops.paper_once import PaperAdvanceRequest, run_once
    from nexus.paper.agent_market import (
        collect_agent_paper_window,
        resolve_agent_paper_market_scope,
    )
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.read_service import PaperReadService
    from nexus.persistence.models.paper import (
        AgentPaperCycleRecord,
        AgentPaperMarketWindowRecord,
        PaperExecutionWindowAuthorityRecord,
        PaperFillRecord,
        PaperLedgerEntryRecord,
        PaperLedgerTransactionRecord,
        PaperPositionAdjustmentEventRecord,
    )
    from sqlalchemy import func

    provider = _SchwabPriceHistoryProvider()
    windows = []
    async with sessions() as session:
        before = await load_paper_projection(
            session, account_id=account_id, as_of=AT + timedelta(seconds=5)
        )
    for order in orders:
        async with sessions() as session:
            scope = await resolve_agent_paper_market_scope(
                session, cycle_id=cycle_id, instrument_id=order.instrument_id
            )
            assert scope.window_start > AT + timedelta(seconds=4)
        evaluated_at = scope.window_end + timedelta(seconds=2)
        await _set_test_insert_clock(sessions, evaluated_at)
        window = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle_id,
            instrument_id=order.instrument_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        windows.append(window)
        assert window.source == "schwab" and window.purpose == "paper_fill"
        replay = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle_id,
            instrument_id=order.instrument_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at + timedelta(seconds=1),
        )
        assert replay.authority_id == window.authority_id and replay.replayed
        request = PaperAdvanceRequest(
            action="advance",
            account_id=account_id,
            order_id=order.id,
            evaluated_at=evaluated_at,
            agent_cycle_id=cycle_id,
            agent_market_window_id=window.authority_id,
            idempotency_key=f"candidate-window:{order.id}:{window.authority_id}",
        )
        filled = await run_once(settings, request)
        assert filled["status"] == filled["action"] == "filled", filled
        assert (
            await run_once(
                settings,
                request.model_copy(update={"evaluated_at": evaluated_at + timedelta(seconds=3)}),
            )
            == filled
        )
    assert provider.calls == len(orders)
    async with sessions() as session:
        fills = tuple(await session.scalars(select(PaperFillRecord)))
        assert len(fills) == len(orders)
        assert {fill.order_id for fill in fills} == {order.id for order in orders}
        for fill in fills:
            order = next(order for order in orders if order.id == fill.order_id)
            assert fill.quantity == order.quantity and fill.notional == fill.quantity * fill.price
            adjustments = tuple(
                await session.scalars(
                    select(PaperPositionAdjustmentEventRecord).where(
                        PaperPositionAdjustmentEventRecord.source_fill_id == fill.id
                    )
                )
            )
            assert len(adjustments) == 1
            assert adjustments[0].quantity_delta == fill.quantity
            assert adjustments[0].cost_delta == fill.notional
            entries = tuple(
                await session.scalars(
                    select(PaperLedgerEntryRecord).where(
                        PaperLedgerEntryRecord.transaction_id == fill.ledger_transaction_id
                    )
                )
            )
            assert entries and sum(entry.debit_amount for entry in entries) == sum(
                entry.credit_amount for entry in entries
            )
        projection = await load_paper_projection(
            session,
            account_id=account_id,
            as_of=max(window.window_end for window in windows) + timedelta(seconds=5),
        )
        assert projection.reserved_cash == 0
        assert projection.available_cash == before.available_cash + before.reserved_cash - sum(
            (fill.notional + fill.fee for fill in fills), start=Decimal(0)
        )
        assert not projection.open_orders
        initial_quantities = {
            position.instrument_id: position.quantity for position in before.positions
        }
        final_quantities = {
            position.instrument_id: position.quantity for position in projection.positions
        }
        for order in orders:
            assert (
                final_quantities[order.instrument_id]
                == initial_quantities.get(order.instrument_id, Decimal(0)) + order.quantity
            )
        with pytest.raises(NexusError, match="order projection is incomplete"):
            await PaperReadService(
                session, projection.as_of, cursor_signing_secrets=("controlled-test-cursor",)
            ).orders(status=None, cursor=None, limit=20)
        page, page_at = await PaperReadService(
            session,
            projection.as_of,
            cursor_signing_secrets=("controlled-test-cursor",),
            candidate_stage_capacity=True,
        ).orders(status=None, cursor=None, limit=20)
        assert page_at == projection.as_of and page.next_cursor is None
        assert {item.order_id for item in page.items} == {order.id for order in orders}
        for item in page.items:
            assert item.status == "filled" and not item.cancellable
            assert Decimal(item.remaining_quantity) == 0
            assert Decimal(item.filled_quantity) == Decimal(item.quantity)
            assert len(item.fills) == 1
        assert (await session.get(AgentPaperCycleRecord, cycle_id)).execution_locked is True
        assert await session.scalar(
            select(func.count()).select_from(AgentPaperMarketWindowRecord)
        ) == len(orders)
        assert await session.scalar(
            select(func.count()).select_from(PaperExecutionWindowAuthorityRecord)
        ) == len(orders)
        assert await session.scalar(
            select(func.count())
            .select_from(PaperLedgerTransactionRecord)
            .where(PaperLedgerTransactionRecord.transaction_type == "fill")
        ) == len(orders)
    from integration.test_portfolio_management_e2e import _business_facts

    async with sessions() as session:
        before_reads = await _business_facts(session)
    await _verify_http_reads(settings.database_url, cycle_id, orders, projection.as_of)
    import os

    if os.environ.get("NEXUS_CONTROLLED_BROWSER") == "1":
        from helpers.controlled_browser import verify_controlled_browser

        await verify_controlled_browser(
            settings.database_url,
            api_now=projection.as_of,
            cycle_id=cycle_id,
            order_ids=tuple(order.id for order in orders),
            candidate_count=count,
            candidate_stage_capacity_reads=True,
            expected_defer_count=0,
        )
    async with sessions() as session:
        assert await _business_facts(session) == before_reads


async def _verify_http_reads(database_url, cycle_id, orders, api_now):
    import httpx
    from helpers.paper_api import app, headers

    paths = (
        "/internal/v1/paper/orders?limit=50",
        "/internal/v1/paper/actual",
        f"/internal/v1/paper/management/cycles/{cycle_id}",
        "/internal/v1/paper/management/portfolio",
        "/internal/v1/paper/cycles?limit=20",
    )
    envelopes = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(database_url, now=api_now)),
        base_url="http://testserver",
    ) as client:
        path = paths[0] + "&candidate_stage_capacity=true"
        rejected = await client.get(path, headers=headers("GET", path))
        assert rejected.status_code == 409 and rejected.json()["error"]["code"] == "DATA_PARTIAL"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app(database_url, now=api_now, candidate_stage_capacity_reads=True)
        ),
        base_url="http://testserver",
    ) as client:
        for path in paths:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, (path, response.json())
            envelopes.append(response.json()["data"])
        unauthenticated = await client.get(paths[0])
        assert unauthenticated.status_code == 401
        cancel_path = f"/internal/v1/paper/orders/{UUID(int=404)}/cancel"
        body = b'{"expected_event_seq":1}'
        signed = headers("POST", cancel_path, body)
        signed["Idempotency-Key"] = "candidate-read-only-cancel"
        rejected = await client.post(cancel_path, headers=signed, content=body)
        assert rejected.status_code == 409
        assert rejected.json()["error"]["message"] == "Candidate Paper API is read-only."
    assert {item["order_id"] for item in envelopes[0]["items"]} == {
        str(order.id) for order in orders
    }
    assert all(item["status"] == "filled" for item in envelopes[0]["items"])
    assert envelopes[1]["data_state"] == "READY" and len(envelopes[1]["positions"]) == 2
    assert envelopes[2]["cycle"]["cycle_id"] == str(cycle_id)
    assert envelopes[2]["cycle"]["execution_locked"] is True
