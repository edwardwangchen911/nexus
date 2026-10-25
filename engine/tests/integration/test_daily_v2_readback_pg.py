"""Normal partial daily consumption retains history and fences damaged proof.

All sources/SDK responses are the existing isolated native fixtures. New daily
admission and readback use product entrypoints; no batch is split or replayed.
"""

import json
from datetime import timedelta
from uuid import UUID

import pytest
from integration import test_monitor_daily_v2_native_pg as native_fixture
from integration.test_daily_v2_frozen_context_pg import (
    CALENDAR_ID,
    _set_test_insert_clock,
    append_fresh_inputs,
)
from nexus.agents import codex_staged
from nexus.api.research_monitoring import read_monitoring_view
from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
from nexus.monitoring.reconciliation import reconcile_account_results
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.monitoring import (
    MonitorDailyEventReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import (
    AuditEvent,
    IdempotencyRecord,
    Job,
    JobLease,
    OutboxEvent,
)
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select, text
from unit.agents.test_codex_sdk import _restore_view


async def close_daily_cycle(factory, settings, captured, pointer, result, now, monkeypatch):
    # Research settlement alone leaves the cycle researching. Use its normal
    # portfolio Worker and resume boundary before admitting an independent cycle.
    from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
    from nexus.agents.portfolio_synthesis_contracts import (
        PortfolioSynthesisInputV1,
        PortfolioSynthesisIntentV1,
    )
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisBudgetReservationRecord,
        AgentPortfolioSynthesisOutputRecord,
        AgentPortfolioSynthesisRunRecord,
    )
    from nexus.worker import run_portfolio_synthesis_worker_once

    close_at = now + timedelta(seconds=1)
    captured["clock"][0] = close_at
    await _set_test_insert_clock(factory, close_at)
    async with factory() as session, session.begin():
        pending = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=result.cycle_id,
            idempotency_key="partial-daily-synthesis",
            now=close_at,
            daily_limit_usd=settings.agent_daily_budget_usd,
            formal_stage_capacity=True,
        )
        assert pending.result_code == "PORTFOLIO_SYNTHESIS_PENDING", pending
        synthesis = await session.scalar(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == result.cycle_id
            )
        )
        synthesis_job = await session.scalar(
            select(Job).where(Job.payload["run_id"].astext == str(synthesis.id))
        )
        intent = synthesis_intent(synthesis)
    portfolio_calls = []

    async def invoke_portfolio(payload, **kwargs):
        assert payload["input_hash"] == synthesis.input_hash
        portfolio_calls.append(synthesis.id)
        return intent.model_dump_json(), UsageSnapshot(
            input_tokens=100, output_tokens=100, provider_calls=1
        )

    monkeypatch.setattr(
        "nexus.agents.portfolio_synthesis_worker.invoke_codex_portfolio", invoke_portfolio
    )
    assert await run_portfolio_synthesis_worker_once(
        settings, job_ids=(synthesis_job.id,), formal_stage_capacity=True
    )
    assert portfolio_calls == [synthesis.id]
    captured["clock"][0] = close_at + timedelta(seconds=1)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session, session.begin():
        closed = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=result.cycle_id,
            idempotency_key="partial-daily-close",
            now=captured["clock"][0],
            formal_stage_capacity=True,
        )
        persisted = await session.get(AgentPortfolioSynthesisRunRecord, synthesis.id)
        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == synthesis.id
            )
        )
        reservation = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == synthesis.id
            )
        )
        job = await session.get(Job, synthesis_job.id)
        assert persisted.status == output.status == "completed"
        assert job.status == "succeeded" and job.attempt_count == 1
        assert persisted.input_tokens == persisted.output_tokens == 100
        assert persisted.settled_cost_usd is not None
        assert reservation.state == "settled"
        assert reservation.settled_usd == persisted.settled_cost_usd
        frozen_portfolio = PortfolioSynthesisInputV1.model_validate(persisted.frozen_input)
        adopted = PortfolioSynthesisIntentV1.model_validate(output.intent)
        assert adopted.input_hash == persisted.input_hash
        assert len(adopted.choices) == 3 and all(
            choice.action == "defer" for choice in adopted.choices
        )
        assert set(adopted.compared_instrument_ids) == set(
            frozen_portfolio.candidate_instrument_ids
        )
        observed_facts = {
            "result_code": closed.result_code,
            "source_research": [
                {"quality": source.intent.quality_status, "stance": source.intent.stance}
                for source in frozen_portfolio.security_outputs
            ],
            "choices": [choice.action for choice in adopted.choices],
            "portfolio_status": persisted.status,
            "budget_state": reservation.state,
        }
        assert (
            observed_facts["source_research"] == [{"quality": "complete", "stance": "watch"}] * 3
        ), json.dumps(observed_facts, sort_keys=True)
        assert closed.status == "completed", observed_facts
        assert closed.result_code == "NO_ORDER_REQUIRED", observed_facts
        assert not closed.order_ids
        from nexus.persistence.models.paper import (
            AgentPaperCycleRecord,
            PaperOrderRecord,
            PaperTargetVersionRecord,
            StrategyDecisionAgentOutputRecord,
            StrategyDecisionRecord,
        )

        decision = await session.get(StrategyDecisionRecord, closed.strategy_decision_id)
        assert decision.agent_paper_cycle_id == result.cycle_id
        assert decision.account_id == pointer.account_id
        assert decision.decision_origin == "agent_research"
        assert decision.portfolio_synthesis_output_id == output.id
        assert set(
            await session.scalars(
                select(StrategyDecisionAgentOutputRecord.agent_research_output_id).where(
                    StrategyDecisionAgentOutputRecord.strategy_decision_id == decision.id
                )
            )
        ) == set(persisted.research_output_ids)
        active_target = await session.scalar(
            select(PaperTargetVersionRecord)
            .where(PaperTargetVersionRecord.account_id == pointer.account_id)
            .order_by(PaperTargetVersionRecord.version_seq.desc())
            .limit(1)
        )
        from nexus.paper.target_service import TargetActivationResult

        activation_row = await session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == f"internal-paper:{pointer.account_id}",
                IdempotencyRecord.route == "paper-target-activation-v1",
                IdempotencyRecord.idempotency_key == f"portfolio-target:{result.cycle_id}",
            )
        )
        activation = TargetActivationResult.model_validate(activation_row.response_payload)
        assert activation.account_id == pointer.account_id
        assert activation.strategy_decision_id == decision.id
        assert (
            activation.target_version_id == active_target.id == persisted.active_target_version_id
        )
        assert activation.target_created is False
        assert activation.batch_id is None and activation.result_code == "NO_ORDER_REQUIRED"
        assert activation.activated_at == active_target.activated_at < captured["clock"][0]
        assert active_target.strategy_decision_id != decision.id
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PaperOrderRecord)
                .where(PaperOrderRecord.strategy_decision_id == decision.id)
            )
            == 0
        )
        assert (await session.get(AgentPaperCycleRecord, result.cycle_id)).execution_locked is True


async def partial_daily(url, monkeypatch, root, *, before_reconcile=None):
    saved = []
    prepare = native_fixture.ready_host_batch

    async def capture(*args, **kwargs):
        value = await prepare(*args, **kwargs)
        saved.append(value)
        return value

    monkeypatch.setattr(native_fixture, "ready_host_batch", capture)
    (
        factory,
        settings,
        pointer,
        batch_id,
        members,
        old_run_id,
        old_hash,
        now,
        result,
        native,
    ) = await native_fixture.unresolved_then_daily(url, monkeypatch, root)
    captured = saved[0][2]
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids)
                )
            )
        )
        target = next(row for row in runs if row.instrument_id == pointer.instrument_id)
        frozen = (await session.get(DataSnapshot, target.data_snapshot_id)).payload[
            "agent_paper_context"
        ]["pending_monitoring_v2"]
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext.in_([str(row.id) for row in runs]))
            )
        )
    calls = []

    async def invoke(payload, **kwargs):
        wire = _restore_view(payload)
        calls.append(wire["stage"])
        raw, usage = await native(payload, **kwargs)
        if wire.get("daily_pending_context_hash") is None:
            return raw, usage
        body = json.loads(raw)
        citation = next(
            row["index"] for row in wire["evidence"] if row["source"] == "nexus_agent_paper_context"
        )
        if wire["stage"] == "research":
            for row in body["roles"]:
                if row["role"] == "orchestrator":
                    row["daily_pending_proposal"] = {
                        "context_hash": wire["daily_pending_context_hash"],
                        "items": [
                            {
                                "trigger_id": item["trigger_id"],
                                "status": "addressed" if item["kind"] == "price" else "unresolved",
                                "rationale": "价格已核对，公告待解决，保留原件及归属。",
                                "citations": [citation],
                            }
                            for item in frozen["trigger_checks"]
                        ],
                    }
        elif wire["stage"] == "critique":
            for row in body["roles"]:
                row["daily_pending_review"] = {
                    "proposal_hash": wire["daily_pending_proposal"]["content_hash"],
                    "decision": "accepted",
                    "rationale": "逐项确认已解决与未解决事件，保留独立审核。",
                    "citations": [citation],
                }
        else:
            body["daily_pending_adoption_hash"] = wire["daily_pending_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    for job in jobs:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
    assert len(calls) == 9 and all(
        calls.count(stage) == 3 for stage in ("research", "critique", "synthesis")
    )
    if before_reconcile is not None:
        await before_reconcile(factory, settings, captured, pointer, batch_id, target, now)
    async with factory() as session, session.begin():
        report = await reconcile_account_results(
            session, account_id=pointer.account_id, instrument_ids=(pointer.instrument_id,), now=now
        )
        assert report["unverified"] == 0, report
    async with factory() as session:
        proofs = tuple(await session.scalars(select(MonitorDailyEventReceipt)))
        assert len(proofs) == 1 and proofs[0].kind == "price"
        assert proofs[0].original_run_id == old_run_id and proofs[0].run_id == target.id
        assert await validate_daily_event_receipt(session, proofs[0].id)
    await close_daily_cycle(factory, settings, captured, pointer, result, now, monkeypatch)
    return (
        factory,
        settings,
        captured,
        pointer,
        batch_id,
        members,
        old_run_id,
        old_hash,
        target,
        proofs[0],
        now,
    )


@pytest.mark.asyncio
async def test_partial_daily_next_normal_entry_and_damaged_historical_proof(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.ops.research_monitor import parser
    from nexus.ops.research_monitor import run as monitor_entry
    from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
    from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

    (
        factory,
        settings,
        captured,
        pointer,
        batch_id,
        members,
        old_run_id,
        old_hash,
        daily_run,
        proof,
        now,
    ) = await partial_daily(migrated_database_url, monkeypatch, tmp_path)
    calls = []

    async def forbidden_sdk(*args, **kwargs):
        calls.append(True)
        raise AssertionError("readback and normal admission must not invoke a model")

    monkeypatch.setattr(codex_staged, "invoke_codex", forbidden_sdk)
    try:
        async with factory() as session:
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_monitoring_view(session, parent, now=now)
            display = next(item for item in view.triggers if item.trigger_id == proof.trigger_id)
            assert (display.status, display.run_id, display.usage_status) == (
                "covered",
                daily_run.id,
                "known",
            )
            old_rows = [
                (row.id, row.research_run_id, row.status, row.batch_id)
                for row in await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            ]
            old_snapshot = (await session.get(DataSnapshot, daily_run.data_snapshot_id)).payload
            before = {
                model: set(await session.scalars(select(model.id)))
                for model in (
                    AgentResearchRunRecord,
                    Job,
                    JobLease,
                    AgentDailyBudgetReservationRecord,
                    MonitorDailyEventReceipt,
                    DataSnapshot,
                    MonitorPlan,
                    OutboxEvent,
                    AuditEvent,
                    IdempotencyRecord,
                )
            }
            old_budgets = [
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ]
            later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(
                seconds=1
            )
            batch = await load_agent_paper_candidate_batch(
                session, batch_id=settings.agent_paper_candidate_batch_id
            )
            scope = tuple(await session.scalars(select(LatestQuote.instrument_id).distinct()))
        captured["clock"][0] = later
        await _set_test_insert_clock(factory, later)
        for instrument_id in scope:
            await append_fresh_inputs(factory, instrument_id, later)
        # The next independent ordinary three-stock cycle, not a snapshot helper
        # or a replay/cropped replacement of the original sealed batch Job.
        async with factory() as session, session.begin():
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=pointer.account_id,
                now=later,
                expected_calendar_authority_id=CALENDAR_ID,
                expected_universe_snapshot_id=batch.universe_snapshot_id,
            )
            result = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=later.replace(second=0, microsecond=0),
                idempotency_key="daily-after-partial-consumption",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                formal_stage_capacity=True,
                formal_model_config_id=settings.agent_paper_formal_model_config_id,
            )
            assert result.result_code == "RESEARCH_JOBS_PREPARED", result
        async with factory() as session:
            new_runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.in_(result.research_run_ids),
                    )
                )
            )
            assert len(new_runs) == 3 and all(row.status == "prepared" for row in new_runs)
            new_run = next(row for row in new_runs if row.instrument_id == pointer.instrument_id)
            foreign_prepared = next(
                row for row in new_runs if row.instrument_id != pointer.instrument_id
            )
            frozen = (await session.get(DataSnapshot, new_run.data_snapshot_id)).payload[
                "agent_paper_context"
            ]
            assert frozen.get("pending_monitoring_v2") is not None, frozen.get(
                "pending_monitoring_v2_gap"
            )
            remaining = frozen["pending_monitoring_v2"]["trigger_checks"]
            assert {UUID(item["trigger_id"]) for item in remaining} == {
                row.id for row in members if row.kind == "disclosure"
            }
            assert len(remaining) == 2 and all(
                item["original_run_id"] == str(old_run_id) and item["batch_id"] == str(batch_id)
                for item in remaining
            )
            assert (
                set(await session.scalars(select(MonitorDailyEventReceipt.id)))
                == before[MonitorDailyEventReceipt]
            )
            assert await validate_daily_event_receipt(session, proof.id)
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_monitoring_view(session, parent, now=later)
            display = next(item for item in view.triggers if item.trigger_id == proof.trigger_id)
            assert (display.status, display.run_id, display.usage_status) == (
                "covered",
                daily_run.id,
                "known",
            )
            assert (
                await session.get(DataSnapshot, daily_run.data_snapshot_id)
            ).payload == old_snapshot
            for identity, run_id, status, original_batch in old_rows:
                row = await session.get(MonitorTrigger, identity)
                assert (row.research_run_id, row.status, row.batch_id) == (
                    run_id,
                    status,
                    original_batch,
                )
            old_output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == old_run_id,
                    AgentResearchOutputRecord.role.is_(None),
                )
            )
            assert old_output.content_hash == old_hash
            for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord):
                assert len(set(await session.scalars(select(model.id))) - before[model]) == 3
            assert set(await session.scalars(select(JobLease.id))) == before[JobLease]
            assert [
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord)
                    .where(
                        AgentDailyBudgetReservationRecord.id.in_(
                            before[AgentDailyBudgetReservationRecord]
                        )
                    )
                    .order_by(AgentDailyBudgetReservationRecord.id)
                )
            ] == old_budgets
            after_admission = {
                model: set(await session.scalars(select(model.id))) for model in before
            }
        # Damage only an isolated proof's pointer to another stock's new prepared
        # Run. Its usage differs from the original known completed Run. Disable
        # exactly the append-only UPDATE guard, restore it in this transaction.
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "ALTER TABLE research_monitor_daily_event_receipts "
                    "DISABLE TRIGGER trg_daily_v2_immutable"
                )
            )
            await session.execute(
                text("UPDATE research_monitor_daily_event_receipts SET run_id=:run WHERE id=:id"),
                {"run": foreign_prepared.id, "id": proof.id},
            )
            await session.execute(
                text(
                    "ALTER TABLE research_monitor_daily_event_receipts "
                    "ENABLE TRIGGER trg_daily_v2_immutable"
                )
            )
        async with factory() as session:
            assert not await validate_daily_event_receipt(session, proof.id)
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_monitoring_view(session, parent, now=later)
            display = next(item for item in view.triggers if item.trigger_id == proof.trigger_id)
            assert (display.status, display.run_id, display.usage_status) == (
                "unknown",
                old_run_id,
                "known",
            )
            assert display.run_id != foreign_prepared.id
            assert (
                foreign_prepared.status == "prepared" and foreign_prepared.settled_cost_usd is None
            )
        # Public normal CLI service entry must stop at damaged-proof presence,
        # before existing queued work or a typed-context rejection can mask it.
        with pytest.raises(ValueError, match="^MONITOR_DAILY_EVENT_RESULT_UNVERIFIED$"):
            await monitor_entry(
                settings,
                parser().parse_args(
                    [
                        "admit",
                        "--trigger-id",
                        str(proof.trigger_id),
                        "--calendar-authority-id",
                        str(CALENDAR_ID),
                    ]
                ),
            )
        async with factory() as session:
            assert {
                model: set(await session.scalars(select(model.id))) for model in before
            } == after_admission
            assert await session.get(MonitorDailyEventReceipt, proof.id) is not None
            assert (
                await session.get(DataSnapshot, daily_run.data_snapshot_id)
            ).payload == old_snapshot
            original_output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == old_run_id,
                    AgentResearchOutputRecord.role.is_(None),
                )
            )
            assert original_output.content_hash == old_hash
            for identity, run_id, status, original_batch in old_rows:
                row = await session.get(MonitorTrigger, identity)
                assert (row.research_run_id, row.status, row.batch_id) == (
                    run_id,
                    status,
                    original_batch,
                )
            assert [
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord)
                    .where(
                        AgentDailyBudgetReservationRecord.id.in_(
                            before[AgentDailyBudgetReservationRecord]
                        )
                    )
                    .order_by(AgentDailyBudgetReservationRecord.id)
                )
            ] == old_budgets
        assert calls == []
    finally:
        await factory.kw["bind"].dispose()
