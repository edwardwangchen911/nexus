"""Historical typed material survives expiry and normal successor installation."""

import json
from datetime import timedelta

import pytest
from integration import test_monitor_mixed_increment_pg as original
from integration.test_candidate_multistock_path import AT
from integration.test_daily_v2_frozen_context_pg import new_daily
from nexus.agents import codex_staged
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
from nexus.monitoring.daily_v2_context import validate_daily_events
from nexus.monitoring.mixed_readback import mixed_batch_readback
from nexus.monitoring.reconciliation import reconcile_account_results
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorMixedEventReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import Job
from nexus.settings import AgentRuntimeMode
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from unit.agents import test_monitoring_producer as price_wire
from unit.agents.test_codex_sdk import _restore_view


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True], ids=["replaced-parent", "expired-and-replaced"])
async def test_normal_daily_consumes_historical_material_with_current_predecessors(
    migrated_database_url, monkeypatch, tmp_path, expired
):
    saved = []
    prepare = original.prepared_materials
    normal_price = price_wire.proposal_wire

    def shorter_price(now):
        proposal = normal_price(now)
        proposal.update(
            review_due_at=(AT + timedelta(seconds=500)).isoformat(),
            valid_until=(AT + timedelta(seconds=600)).isoformat(),
        )
        return proposal

    async def capture(*args, **kwargs):
        value = await prepare(*args, **kwargs)
        saved.append((*value, codex_staged.invoke_codex))
        return value

    monkeypatch.setattr(original, "prepared_materials", capture)
    if expired:
        # Bound the original synthetic model proposal before any role output is
        # persisted. No UPDATE of a plan, output, check or lease is permitted.
        monkeypatch.setattr(price_wire, "proposal_wire", shorter_price)
    await original.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
        migrated_database_url, monkeypatch, tmp_path, "unresolved"
    )
    factory, settings, _, captured, pointer, batch_id, material_id, native = saved[0]
    monkeypatch.setattr(price_wire, "proposal_wire", normal_price)
    monkeypatch.setattr(codex_staged, "invoke_codex", native)
    try:
        async with factory() as session:
            members = tuple(
                await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            )
            price = next(row for row in members if row.kind == "price")
            old_output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == price.research_run_id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            old_hash = old_output.content_hash
            material = await session.get(DataSnapshot, material_id)
            old_material = json.dumps(material.payload, sort_keys=True)
            check = await session.get(MonitorCheck, price.check_id)
            old_price = await session.get(MonitorPlan, check.plan_id)
            old_run = await session.get(AgentResearchRunRecord, price.research_run_id)
            settings = settings.model_copy(
                update={
                    "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
                    "agent_paper_formal_model_config_id": old_run.model_config_id,
                    "agent_paper_cycle_enabled": True,
                }
            )
            predecessors = {}
            for kind in ("price", "disclosure"):
                row = await session.scalar(
                    select(MonitorPlan)
                    .where(
                        MonitorPlan.account_id == pointer.account_id,
                        MonitorPlan.instrument_id == pointer.instrument_id,
                        MonitorPlan.kind == kind,
                    )
                    .order_by(MonitorPlan.generation.desc())
                    .limit(1)
                )
                predecessors[kind] = row
            historical_disclosure = next(
                row for row in material.payload["parents"] if row["kind"] == "disclosure"
            )
            assert predecessors["disclosure"].generation == 2
            assert (
                str(predecessors["disclosure"].predecessor_id) == historical_disclosure["plan_id"]
            )
            assert predecessors["price"].id == old_price.id
            original_identity = [(row.id, row.research_run_id, row.status) for row in members]
        now, result = await new_daily(factory, settings, captured, pointer)
        assert len(result.research_run_ids) == 3
        assert (old_price.valid_until <= now) is expired
        async with factory() as session:
            runs = tuple(
                await session.scalars(
                    select(AgentResearchRunRecord).where(
                        AgentResearchRunRecord.id.in_(result.research_run_ids)
                    )
                )
            )
            target = next(row for row in runs if row.instrument_id == pointer.instrument_id)
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=target.data_snapshot_id,
                instrument_id=target.instrument_id,
                as_of=now,
            )
            assert bound is not None and bound[0].pending_monitoring_v2_gap is None
            frozen = bound[0].pending_monitoring_v2
            assert frozen is not None and await validate_daily_events(session, frozen)
            assert len(frozen.trigger_checks) == 1
            item = frozen.trigger_checks[0]
            assert (item.trigger_id, item.original_run_id, item.original_output_id) == (
                price.id,
                price.research_run_id,
                old_output.id,
            )
            assert item.materials_snapshot_id == material_id
            for kind, previous in predecessors.items():
                assert frozen.predecessors[kind].plan_id == previous.id
                assert frozen.predecessors[kind].generation == previous.generation
            jobs = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.payload["run_id"].astext.in_([str(run.id) for run in runs])
                    )
                )
            )
        stages = []

        async def daily_invoke(payload, **kwargs):
            wire = _restore_view(payload)
            stages.append(wire["stage"])
            raw, usage = await native(payload, **kwargs)
            if wire.get("daily_pending_context_hash") is None:
                return raw, usage
            body = json.loads(raw)
            assert wire["daily_pending_context_hash"] == frozen.content_hash
            citation = next(
                row["index"]
                for row in wire["evidence"]
                if row["source"] == "nexus_agent_paper_context"
            )
            if wire["stage"] == "research":
                proposer = next(row for row in body["roles"] if row["role"] == "orchestrator")
                proposer["daily_pending_proposal"] = {
                    "context_hash": frozen.content_hash,
                    "items": [
                        {
                            "trigger_id": str(price.id),
                            "status": "addressed",
                            "rationale": "核对历史原件和当前前代后保留观察。",
                            "citations": [citation],
                        }
                    ],
                }
            elif wire["stage"] == "critique":
                for reviewer in body["roles"]:
                    reviewer["daily_pending_review"] = {
                        "proposal_hash": wire["daily_pending_proposal"]["content_hash"],
                        "decision": "accepted",
                        "rationale": "历史材料不因计划过期而失效。",
                        "citations": [citation],
                    }
            else:
                body["daily_pending_adoption_hash"] = wire["daily_pending_proposal"]["content_hash"]
            return json.dumps(body), usage

        monkeypatch.setattr(codex_staged, "invoke_codex", daily_invoke)
        for job in jobs:
            assert await run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
            assert not await run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
        assert len(stages) == 9 and all(
            stages.count(stage) == 3 for stage in ("research", "critique", "synthesis")
        )
        async with factory() as session, session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            report = await reconcile_account_results(
                session,
                account_id=pointer.account_id,
                instrument_ids=(pointer.instrument_id,),
                now=now,
            )
            assert report["installed"] == 2 and report["unverified"] == 0, report
        async with factory() as session:
            proof = await session.scalar(
                select(MonitorDailyEventReceipt).where(
                    MonitorDailyEventReceipt.trigger_id == price.id
                )
            )
            assert proof is not None and proof.run_id == target.id
            assert await validate_daily_event_receipt(session, proof.id)
            assert await validate_daily_events(session, frozen)
            assert (
                await session.get(AgentResearchOutputRecord, old_output.id)
            ).content_hash == old_hash
            assert (
                json.dumps((await session.get(DataSnapshot, material_id)).payload, sort_keys=True)
                == old_material
            )
            for identity, original_run, status in original_identity:
                member = await session.get(MonitorTrigger, identity)
                assert (member.research_run_id, member.status) == (original_run, status)
            coverage = tuple(
                await session.scalars(
                    select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == target.id)
                )
            )
            assert len(coverage) == 2
            for row in coverage:
                successor = await session.get(MonitorPlan, row.active_plan_id)
                assert successor.predecessor_id == predecessors[row.kind].id
                assert successor.generation == predecessors[row.kind].generation + 1
            assert len(tuple(await session.scalars(select(MonitorMixedEventReceipt)))) == 2
            assert (await mixed_batch_readback(session, batch_id))["impact_review"] == "verified"
    finally:
        await factory.kw["bind"].dispose()
