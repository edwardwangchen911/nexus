"""Completed incremental proposals retain their own quality and frozen predecessor."""

import json
from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_monitor_increment_pg import ready_trigger
from nexus.agents import codex_staged
from nexus.monitoring.installation import install_plan_from_output, load_approved_source
from nexus.ops.research_monitor import parser
from nexus.ops.research_monitor import run as monitor_entry
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.worker import run_agent_worker_once
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["complete", "partial", "audit-veto", "review-veto", "no-adoption"]
)
async def test_increment_output_installs_only_against_frozen_parent(
    migrated_database_url, monkeypatch, mode
):
    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    receipt = await monitor_entry(
        settings,
        parser().parse_args(
            ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
        ),
    )
    assert receipt["status"] == "admitted"
    partial = mode in {"partial", "audit-veto"}
    original = codex_staged.invoke_codex

    async def invoke(payload, **kwargs):
        raw, usage = await original(payload, **kwargs)
        value = json.loads(raw)
        if mode == "partial" and payload["stage"] == "synthesis":
            value["synthesis_assessment"]["review_conclusion"] = "limited"
            value["synthesis_assessment"]["uncertainties"] = [
                "Independent evidence remains incomplete."
            ]
        if payload["stage"] == "critique":
            for role in value["roles"]:
                if role["role"] == "evidence_audit" and mode == "audit-veto":
                    role["vote"] = "oppose"
                if role["role"] == "bear" and mode == "review-veto":
                    role["monitoring_review"]["decision"] = "rejected"
        if payload["stage"] == "synthesis" and mode == "no-adoption":
            value["monitoring_adoption_hash"] = None
        raw = json.dumps(value)
        return raw, usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    assert await run_agent_worker_once(
        settings, job_ids=(UUID(receipt["job_id"]),), formal_stage_capacity=True
    )
    async with factory() as session:
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == UUID(receipt["run_id"]),
                AgentResearchOutputRecord.status == "completed",
            )
        )
        assert output is not None
        assert output.intent["quality_status"] == ("partial" if partial else "complete")
        assert bool(output.intent.get("monitoring_proposal")) is (mode in {"complete", "partial"})
        old = await session.scalar(
            select(MonitorPlan).where(MonitorPlan.source_run_id == parent.id)
        )
    from unittest.mock import AsyncMock

    from nexus.monitoring import source_qualification

    async with factory() as session:
        qualified = await source_qualification.load_qualified_source(
            session, account_id=settings.agent_paper_account_id, output_id=output.id, now=now
        )
        assert qualified.intent.quality_status == ("partial" if partial else "complete")
    if mode not in {"complete", "partial"}:
        async with factory() as session:
            with pytest.raises(ValueError):
                await load_approved_source(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=output.id,
                    now=now,
                )
        await factory.kw["bind"].dispose()
        return
    for function, result in (("job_lease_consumed", False), ("load_frozen_evidence", ())):
        with monkeypatch.context() as probe:
            probe.setattr(source_qualification, function, AsyncMock(return_value=result))
            async with factory() as session:
                with pytest.raises(ValueError, match="MONITOR_SOURCE_INVALID"):
                    await source_qualification.load_qualified_source(
                        session,
                        account_id=settings.agent_paper_account_id,
                        output_id=output.id,
                        now=now,
                    )
    from nexus.agents import price_authority

    with monkeypatch.context() as probe:
        probe.setattr(
            price_authority,
            "revalidate_frozen_price_authority",
            AsyncMock(return_value=price_authority.FrozenPriceAuthorityStatus.INVALID),
        )
        async with factory() as session:
            with pytest.raises(ValueError, match="MONITOR_SOURCE_INVALID"):
                await source_qualification.load_qualified_source(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=output.id,
                    now=now,
                )
    for predecessor in (None, uuid4()):
        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="MONITOR_PLAN_CONFLICT"):
                await install_plan_from_output(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=output.id,
                    expected_predecessor_id=predecessor,
                    now=now,
                )
    async with factory() as session, session.begin():
        source = await load_approved_source(
            session, account_id=settings.agent_paper_account_id, output_id=output.id, now=now
        )
        assert source.context.plan_id == old.id
        plan = await install_plan_from_output(
            session,
            account_id=settings.agent_paper_account_id,
            output_id=output.id,
            expected_predecessor_id=old.id,
            now=now,
        )
        assert plan.predecessor_id == old.id and plan.generation == 2
        assert plan.source_hash == output.content_hash
        assert source.output.intent["quality_status"] == ("partial" if partial else "complete")
        replay = await install_plan_from_output(
            session,
            account_id=settings.agent_paper_account_id,
            output_id=output.id,
            expected_predecessor_id=old.id,
            now=now,
        )
        assert replay.id == plan.id
    await factory.kw["bind"].dispose()
