"""Exercise a real generation conflict after research freezes its predecessor pair."""

import json

from integration.test_daily_v2_frozen_context_pg import new_daily
from nexus.agents import codex_staged
from nexus.monitoring.reconciliation import reconcile_account_results
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorIncrement,
    MonitorMixedEventReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _restore_view


async def assert_old_run_superseded(
    factory,
    settings,
    captured,
    pointer,
    old_run_id,
    native,
    monkeypatch,
    *,
    advanced_kind,
    mixed_member=None,
    unfrozen_kind=None,
):
    """A one-kind newer daily install must fence both old-kind replacements/consumptions."""
    async with factory() as session:
        old = await session.get(AgentResearchRunRecord, old_run_id)
        old_snapshot = await session.get(DataSnapshot, old.data_snapshot_id)
        frozen_before = json.dumps(old_snapshot.payload, sort_keys=True)
        old_output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == old_run_id,
                AgentResearchOutputRecord.role.is_(None),
            )
        )
        old_hash = old_output.content_hash
        original_triggers = {
            row.id: (row.research_run_id, row.status)
            for row in await session.scalars(select(MonitorTrigger))
        }
        predecessors = {
            row.kind: row.id
            for row in await session.scalars(
                select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
            )
        }
        assert set(predecessors) == (
            {"disclosure"} if unfrozen_kind == "price" else {"price", "disclosure"}
        )
        assert not await session.scalar(
            select(MonitorCoverageReceipt.id).where(MonitorCoverageReceipt.run_id == old_run_id)
        )
    now, result = await new_daily(factory, settings, captured, pointer)
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(result.research_run_ids)
                )
            )
        )
        current = next(row for row in runs if row.instrument_id == pointer.instrument_id)
        snapshot = await session.get(DataSnapshot, current.data_snapshot_id)
        paper = snapshot.payload["agent_paper_context"]
        pending = paper.get("pending_monitoring_v2")
        if mixed_member is not None:
            assert pending is None
            assert (
                paper["pending_monitoring_v2_gap"]["reason"]
                == "MONITOR_ORIGINAL_REVIEW_UNAVAILABLE"
            )
        else:
            assert pending is not None
            assert len(pending["trigger_checks"]) == 3
        jobs = tuple(
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext == str(current.id))
            )
        )
    stages = []

    async def invoke(payload, **kwargs):
        wire = _restore_view(payload)
        stages.append(wire["stage"])
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        # A normal daily with an optional gap can install either approved kind.
        # Keep the other kind absent so only one predecessor actually changes.
        if mixed_member is not None:
            opposite = "disclosure" if advanced_kind == "price" else "monitoring"
            if wire["stage"] == "research":
                for role in body["roles"]:
                    if role["role"] == "market_state":
                        role[f"{opposite}_proposal"] = None
            elif wire["stage"] == "critique":
                for role in body["roles"]:
                    role[f"{opposite}_review"] = None
            else:
                body[f"{opposite}_adoption_hash"] = None
        if wire.get("daily_pending_context_hash") is not None:
            citation = next(
                row["index"]
                for row in wire["evidence"]
                if row["source"] == "nexus_agent_paper_context"
            )
            if wire["stage"] == "research":
                for role in body["roles"]:
                    if role["role"] == "orchestrator":
                        role["daily_pending_proposal"] = {
                            "context_hash": wire["daily_pending_context_hash"],
                            "items": [
                                {
                                    "trigger_id": item["trigger_id"],
                                    "status": "addressed"
                                    if item["kind"] == advanced_kind
                                    else "unresolved",
                                    "rationale": "新一轮按原件核对，仅解决本类事件。",
                                    "citations": [citation],
                                }
                                for item in pending["trigger_checks"]
                            ],
                        }
            elif wire["stage"] == "critique":
                for role in body["roles"]:
                    role["daily_pending_review"] = {
                        "proposal_hash": wire["daily_pending_proposal"]["content_hash"],
                        "decision": "accepted",
                        "rationale": "逐项核验新一轮原件与保留未解决事件。",
                        "citations": [citation],
                    }
            else:
                body["daily_pending_adoption_hash"] = wire["daily_pending_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    for job in jobs:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
    async with factory() as session:
        observed_jobs = [
            (row.status, row.last_error_code)
            for row in await session.scalars(
                select(Job).where(Job.id.in_([job.id for job in jobs]))
            )
        ]
    assert len(jobs) == 1 and stages == ["research", "critique", "synthesis"], {
        "stages": stages,
        "jobs": observed_jobs,
    }
    async with factory() as session, session.begin():
        report = await reconcile_account_results(
            session, account_id=pointer.account_id, instrument_ids=(pointer.instrument_id,), now=now
        )
        assert report["unverified"] == 0, report
    guarded = (MonitorPlan, MonitorDailyEventReceipt, MonitorMixedEventReceipt, Job, OutboxEvent)
    budget = AgentDailyBudgetReservationRecord
    usage_queries = (
        select(budget.id, budget.state, budget.reserved_usd, budget.settled_usd).order_by(
            budget.id
        ),
        select(
            MonitorIncrement.id,
            MonitorIncrement.reserved_sdk_calls,
            MonitorIncrement.daily_sdk_call_limit,
        ).order_by(MonitorIncrement.id),
        select(
            AgentResearchRunRecord.id,
            AgentResearchRunRecord.input_tokens,
            AgentResearchRunRecord.output_tokens,
            AgentResearchRunRecord.settled_cost_usd,
        ).order_by(AgentResearchRunRecord.id),
    )
    async with factory() as session:
        new_plans = tuple(
            await session.scalars(
                select(MonitorPlan).where(MonitorPlan.source_run_id == current.id)
            )
        )
        assert len(new_plans) == 1
        replacement = new_plans[0]
        assert replacement.kind == advanced_kind
        assert replacement.generation == (1 if unfrozen_kind == advanced_kind else 2)
        assert replacement.predecessor_id == predecessors.get(advanced_kind)
        if advanced_kind == "price":
            from nexus.api.research_monitoring import read_monitoring_view
            from nexus.monitoring.reconciliation import plan_covered

            view_before = await read_monitoring_view(session, current, now=now)
            assert view_before.state == "valid" and view_before.plan_id == replacement.id
            assert await plan_covered(session, plan=replacement, now=now)

        saved_counts = {
            model: await session.scalar(select(func.count()).select_from(model))
            for model in guarded
        }
        proof_ids = set(await session.scalars(select(MonitorDailyEventReceipt.id)))
        usage_before = [tuple((await session.execute(query)).all()) for query in usage_queries]
        assert len(proof_ids) == (
            0 if mixed_member is not None else 1 if advanced_kind == "price" else 2
        )
        output_hashes = dict(
            (
                await session.execute(
                    select(AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash)
                )
            ).all()
        )
    # Account reconciliation chooses the latest Run. Address the precise old Run
    # here so this test actually reaches the stale-predecessor product branch.
    from nexus.monitoring.daily_v2_reconciliation import reconcile_daily_event_run
    from nexus.monitoring.mixed_reconciliation import reconcile_mixed_run

    receipt_ids = None
    for _ in range(2 if mixed_member is None else 1):
        async with factory() as session, session.begin():
            old = await session.get(AgentResearchRunRecord, old_run_id)
            if mixed_member is None:
                receipts = await reconcile_daily_event_run(
                    session, account_id=pointer.account_id, run=old, now=now
                )
            else:
                receipts = await reconcile_mixed_run(
                    session, account_id=pointer.account_id, run=old, member=mixed_member, now=now
                )
            assert len(receipts) == 2
            assert {row.kind for row in receipts} == {"price", "disclosure"}
            assert all(
                row.status == "superseded"
                and row.reason == "MONITOR_SOURCE_SUPERSEDED"
                and row.active_plan_id is None
                for row in receipts
            )
            ids = {row.id for row in receipts}
            if receipt_ids is not None:
                assert ids == receipt_ids
            receipt_ids = ids
    async with factory() as session:
        assert {
            model: await session.scalar(select(func.count()).select_from(model))
            for model in guarded
        } == saved_counts
        assert set(await session.scalars(select(MonitorDailyEventReceipt.id))) == proof_ids
        assert [
            tuple((await session.execute(query)).all()) for query in usage_queries
        ] == usage_before
        for model in (MonitorDailyEventReceipt, MonitorMixedEventReceipt):
            assert not await session.scalar(select(model.id).where(model.run_id == old_run_id))
        assert (
            dict(
                (
                    await session.execute(
                        select(AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash)
                    )
                ).all()
            )
            == output_hashes
        )
        assert (
            await session.get(AgentResearchOutputRecord, old_output.id)
        ).content_hash == old_hash
        assert (
            json.dumps(
                (await session.get(DataSnapshot, old.data_snapshot_id)).payload, sort_keys=True
            )
            == frozen_before
        )
        assert {
            row.id: (row.research_run_id, row.status)
            for row in await session.scalars(select(MonitorTrigger))
        } == original_triggers
        action = (
            "research.monitor.daily_events_reconciled"
            if mixed_member is None
            else "research.monitor.mixed_result_reconciled"
        )
        events = tuple(
            await session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == action,
                    AuditEvent.payload["run_id"].astext == str(old_run_id),
                )
            )
        )
        assert len(events) == 1
        if advanced_kind == "price":
            view_after = await read_monitoring_view(session, current, now=now)
            assert view_after.model_dump(mode="json") == view_before.model_dump(mode="json")
            assert await plan_covered(session, plan=replacement, now=now)
        count_key = "consumed_count" if mixed_member is None else "addressed_events"
        assert events[0].payload[count_key] == 0
