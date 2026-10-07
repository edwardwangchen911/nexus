"""Synthetic Paper contexts traverse the real worker; no live model qualification."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from integration.test_portfolio_same_day_research_pg import NOW, seed_normal_path
from nexus.agents import codex_staged
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, DEADLINE_BUDGET_CONFIG_VERSION
from nexus.ops import openai_local
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import PaperOrderRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select, text
from unit.agents.test_codex_sdk import _restore_view


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_time_budget", [False, True])
async def test_frozen_paper_context_reaches_budget_sdk_and_unknown_result_is_not_replayed(
    migrated_database_url, monkeypatch, stage_time_budget
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    # Isolated historical fixture only: registration validity precedes its cutoff.
    monkeypatch.setitem(
        openai_local._CONFIG, "pricing_available_at", (NOW - timedelta(days=1)).isoformat()
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, occupied=True
    )
    await openai_local.prepare_database(
        settings,
        actionable_paper=True,
        structured_synthesis=True,
        aggregate_input_tokens=96000,
        max_wire_bytes=64000,
        inline_citations=True,
        stage_time_budget=stage_time_budget,
    )
    # The migration fixture pins DB timestamps: make this synthetic registration
    # newer than its historical v4 seed instead of relying on UUID tie-breaking.
    async with sessions.begin() as session:
        await session.execute(text("ALTER TABLE agent_model_configs DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text("UPDATE agent_model_configs SET created_at=:at WHERE version=:version"),
                {
                    "at": NOW - timedelta(hours=1),
                    "version": DEADLINE_BUDGET_CONFIG_VERSION
                    if stage_time_budget
                    else ACTIONABLE_CONFIG_VERSION,
                },
            )
        finally:
            await session.execute(text("ALTER TABLE agent_model_configs ENABLE TRIGGER USER"))
    captured = {}

    async def unknown_result(payload, **options):
        captured["payload"] = payload
        captured["options"] = options
        captured["observed_at"] = asyncio.get_running_loop().time()
        raise RuntimeError("synthetic-unknown-result")

    sdk = AsyncMock(side_effect=unknown_result)
    monkeypatch.setattr(codex_staged, "invoke_codex", sdk)
    try:
        async with sessions.begin() as session:
            original_orders = await session.scalar(
                select(func.count()).select_from(PaperOrderRecord)
            )
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=NOW
            )
            assert projection.open_orders and projection.reserved_cash > 0
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(entry.instrument_id for entry in manifest),
                idempotency_key="budget-context",
                created_at=NOW,
            )
            result = await agent_cycle._start_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(NOW),
                daily_limit_usd=Decimal("3"),
                candidate_batch_id=batch.batch_id,
                requested_instrument_ids=batch.instrument_ids,
            )
            assert result.status == "researching", result.result_code
            assert len(result.research_run_ids) == 2
            job_id = await session.scalar(
                select(Job.id).where(
                    Job.payload["run_id"].as_string() == str(result.research_run_ids[0])
                )
            )
            assert job_id is not None
        assert await run_agent_worker_once(settings, job_ids=(job_id,))
        async with sessions() as session:
            observed_run = await session.get(AgentResearchRunRecord, result.research_run_ids[0])
            reasons = list(
                await session.scalars(
                    select(AgentResearchOutputRecord.intent).where(
                        AgentResearchOutputRecord.run_id == result.research_run_ids[0]
                    )
                )
            )
        assert sdk.await_count == 1, (observed_run.pricing_version, observed_run.status, reasons)
        payload = captured["payload"]
        restored = _restore_view(payload["frozen_inputs"])
        contexts = [
            item["agent_paper_context"]
            for item in restored["security_snapshot"]["items"]
            if "agent_paper_context" in item
        ]
        assert len(contexts) == 1
        context = contexts[0]
        assert context["decision_quote"]["source"] == "schwab"
        assert context["decision_quote"]["delayed"] is False
        assert context["prior_daily"]["source"] == "alpaca"
        assert context["paper_portfolio"]["projection_hash"] == projection.projection_hash
        assert (
            Decimal(context["paper_portfolio"]["cash_weight"])
            == (projection.available_cash + projection.reserved_cash) / projection.nav
        )
        assert context["execution_locked"] is True
        assert "account_id" not in context
        assert "target weights" in payload["instructions"]
        options = captured["options"]
        if stage_time_budget:
            assert 0 < options["timeout_seconds"] <= 53
            assert (
                0 < options["deadline_at"] - captured["observed_at"] <= options["timeout_seconds"]
            )
        else:
            assert "timeout_seconds" not in options
            assert "deadline_at" not in options
        async with sessions() as session:
            run = await session.get(AgentResearchRunRecord, result.research_run_ids[0])
            job = await session.get(Job, job_id)
            assert run.status == "failed"
            assert run.pricing_version == (
                DEADLINE_BUDGET_CONFIG_VERSION if stage_time_budget else ACTIONABLE_CONFIG_VERSION
            )
            assert job.status == "failed" and job.attempt_count == 1
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderRecord))
                == original_orders
            )
            other = await session.get(AgentResearchRunRecord, result.research_run_ids[1])
            assert other.status == "prepared"
        assert not await run_agent_worker_once(settings, job_ids=(job_id,))
        assert sdk.await_count == 1
    finally:
        await engine.dispose()
