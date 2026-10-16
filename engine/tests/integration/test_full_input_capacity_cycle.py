"""Complete frozen candidate cycles use v9 without rewriting any earlier profile."""

import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_candidate_research_authority import OPTIONS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_capacity import capacity_config, capacity_config_hash
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "formal,flexible",
    [(False, False), (True, False), (False, True), (True, True)],
    ids=["isolated-v9", "formal-v9", "isolated-v10", "formal-v10"],
)
@pytest.mark.parametrize(
    "count,oversized_tail",
    [(3, False), (5, False), (3, True), (3, "invalid_context"), (3, "wrong_mode")],
)
async def test_full_cycle_worker_rederives_whole_pool_before_claim_or_sdk(
    migrated_database_url,
    monkeypatch,
    count,
    oversized_tail,
    formal,
    flexible,
    research_timeout=False,
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=FIVE[:count], decision_time=AT
    )
    calls = []
    if flexible:
        from nexus.agents.codex_sdk import invoke_codex as real_sdk_invoke
        from unit.agents.test_codex_sdk import _sdk

        client, thread, sdk_result, _ = _sdk(monkeypatch)
        if research_timeout:
            import asyncio
            from types import SimpleNamespace
            from unittest.mock import AsyncMock

            # Real product SDK wrapper deadline/cancellation and close dispatch,
            # driven by an event. SDK client, turn and close are test doubles.
            timers = []
            timeout_at = asyncio.timeout_at

            def observe_timer(deadline):
                timer = timeout_at(deadline)
                timers.append(timer)
                return timer

            monkeypatch.setattr(asyncio, "timeout_at", observe_timer)
            closed = asyncio.Event()
            client.close.side_effect = closed.set
            client._client.next_turn_notification = AsyncMock(
                return_value=SimpleNamespace(
                    method="item/reasoning/textDelta", payload="private-progress-canary"
                )
            )

            async def unfinished_turn(*args, **kwargs):
                await client._client.next_turn_notification("private-turn-canary")
                timers[-1].reschedule(asyncio.get_running_loop().time())
                await closed.wait()
                return sdk_result

            thread.run.side_effect = unfinished_turn
    from nexus.agents.codex_flexible_budget import flexible_stage_config, flexible_stage_config_hash

    config = flexible_stage_config() if flexible else capacity_config()
    config_hash = flexible_stage_config_hash() if flexible else capacity_config_hash()

    async def invoke(payload, **options):
        calls.append(payload["stage"])
        assert options["max_wire_bytes"] == 100479
        if flexible:
            assert options["stage_budget_version"] == config["version"]
            later = {"research": 104, "critique": 52, "synthesis": 0}[payload["stage"]]
            assert options["deadline_at"] <= options["parent_deadline_at"] - 7 - later
            if payload["stage"] == "critique":
                assert options["timeout_seconds"] > 45
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        if flexible:
            sdk_result.final_response = json.dumps(body)
            return await real_sdk_invoke(payload, **options)
        return json.dumps(body), UsageSnapshot(input_tokens=100, output_tokens=80)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            model = AgentModelConfigRecord(
                model_id=config["model_id"],
                version=config["version"],
                config_hash=config_hash,
                max_input_tokens=96000,
                max_output_tokens=8192,
                max_tool_calls=0,
                input_usd_per_million=Decimal(0),
                output_usd_per_million=Decimal(0),
                pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
                pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
                created_at=AT - timedelta(seconds=1),
            )
            session.add(model)
            await session.flush()
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(row.instrument_id for row in manifest),
                idempotency_key="v9-whole-batch",
                created_at=AT,
            )
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=AT
            )
            await approve_initial_target_projection(
                session,
                account_id=authority.account_id,
                projection_as_of=AT,
                expected_projection_hash=projection.projection_hash,
                approved_by="isolated-fixture",
                approved_at=AT,
                idempotency_key="v9-initial-baseline",
            )
            result = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="v9-fresh-cycle",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                **(
                    {"formal_stage_capacity": True, "formal_model_config_id": model.id}
                    if formal
                    else {"candidate_stage_capacity": True, "candidate_model_config_id": model.id}
                ),
            )
            assert result.result_code == "RESEARCH_JOBS_PREPARED"
            assert len(result.research_run_ids) == count
            jobs = tuple(await session.scalars(select(Job).order_by(Job.created_at, Job.id)))
        if oversized_tail == "wrong_mode":
            with pytest.raises(ValueError, match="CODEX_CAPACITY_POOL_BINDING_INVALID"):
                await run_agent_worker_once(
                    settings,
                    job_ids=(jobs[0].id,),
                    **(
                        {"full_input_capacity": True} if formal else {"formal_stage_capacity": True}
                    ),
                )
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
                assert all(job.status == "queued" for job in await session.scalars(select(Job)))
            assert calls == []
            return
        if oversized_tail:
            from nexus.agents.codex_staged import StructuredStagedCodexCommittee

            original = StructuredStagedCodexCommittee._inputs

            async def inputs(self, request):
                data = await original(self, request)
                if request.instrument_id == manifest[-1].instrument_id:
                    data["filing_disclosures"] = {"items": [], "text": "x" * 110000}
                return data

            if oversized_tail == "invalid_context":
                from nexus.agents import paper_context

                original_validator = paper_context.revalidate_frozen_agent_paper_context

                async def validator(session, *, run, evidence_records):
                    if run.instrument_id == manifest[-1].instrument_id:
                        return paper_context.FrozenAgentPaperContextStatus.INVALID
                    return await original_validator(
                        session, run=run, evidence_records=evidence_records
                    )

                monkeypatch.setattr(
                    paper_context, "revalidate_frozen_agent_paper_context", validator
                )
                error = "CODEX_CAPACITY_POOL_BINDING_INVALID"
            else:
                monkeypatch.setattr(StructuredStagedCodexCommittee, "_inputs", inputs)
                error = "CODEX_CAPACITY_POOL_WIRE_EXCEEDED"
            with pytest.raises(ValueError, match=error):
                await run_agent_worker_once(
                    settings,
                    job_ids=(jobs[0].id,),
                    **(
                        {"formal_stage_capacity": True} if formal else {"full_input_capacity": True}
                    ),
                )
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
                assert all(job.status == "queued" for job in await session.scalars(select(Job)))
            assert calls == []
            return
        for job in jobs:
            assert await run_agent_worker_once(
                settings,
                job_ids=(job.id,),
                **({"formal_stage_capacity": True} if formal else {"full_input_capacity": True}),
            )
            if research_timeout:
                from uuid import UUID

                import httpx
                from helpers.portfolio_api import app, headers
                from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
                from nexus.persistence.models.agents import (
                    AgentDailyBudgetReservationRecord,
                    AgentPortfolioSynthesisRunRecord,
                    AgentResearchOutputRecord,
                )
                from nexus.persistence.models.paper import PaperOrderRecord

                assert not await run_agent_worker_once(
                    settings, job_ids=(job.id,), formal_stage_capacity=True
                )
                async with sessions.begin() as check:
                    failed = await check.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
                    assert failed.status == "failed" and failed.retry_count == 0
                    assert failed.input_tokens is None and failed.output_tokens is None
                    reservation = await check.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == failed.id
                        )
                    )
                    assert reservation.state == "reserved" and reservation.settled_usd is None
                    terminal = await check.scalar(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == failed.id,
                            AgentResearchOutputRecord.status == "failed",
                        )
                    )
                    diagnostic = terminal.payload["diagnostic"]
                    assert diagnostic["stage"] == "research"
                    assert diagnostic["invocation_phase"] == "turn_run"
                    assert diagnostic["timeout_source"] == "stage_deadline"
                    timing = diagnostic["timing"]
                    assert timing["stage_preparation_ms"] >= 0
                    assert 0 < timing["committee_remaining_ms"] <= 175000
                    assert 0 < timing["allocated_timeout_ms"] <= 60000
                    assert timing["stream_progress"]["reasoning_notifications"] == 1
                    assert timing["stream_progress"]["answer_notifications"] == 0
                    assert not timing["stream_progress"]["terminal_notification_seen"]
                    assert "private-" not in json.dumps(terminal.payload)
                    stopped = await resume_schwab_agent_paper_cycle(
                        check,
                        cycle_id=result.cycle_id,
                        idempotency_key="synthetic-timeout-close",
                        now=AT + timedelta(minutes=5),
                        formal_stage_capacity=True,
                    )
                    assert stopped.status == "unavailable"
                    assert stopped.result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
                    assert (
                        await check.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
                    )
                    assert (
                        await check.scalar(
                            select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                        )
                        == 0
                    )
                path = f"/internal/v1/research-runs/{failed.id}"
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app(migrated_database_url)),
                    base_url="http://testserver",
                ) as api:
                    response = await api.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                assert response.json()["data"]["abstention_reason"] == "model_usage_uncertain"
                assert calls == ["research"]
                thread.run.assert_awaited_once()
                client.close.assert_awaited_once()
                return
            if flexible:
                from nexus.persistence.models.agents import AgentResearchOutputRecord

                async with sessions() as check:
                    finished = await check.get(Job, job.id, populate_existing=True)
                    terminal = await check.scalar(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == job.payload["run_id"],
                            AgentResearchOutputRecord.role.is_(None),
                        )
                    )
                    assert finished.status == "succeeded", (
                        calls,
                        finished.result_code,
                        None if terminal is None else terminal.payload.get("reason"),
                        None if terminal is None else terminal.payload.get("diagnostic"),
                    )
            assert not await run_agent_worker_once(
                settings,
                job_ids=(job.id,),
                **({"formal_stage_capacity": True} if formal else {"full_input_capacity": True}),
            )
        async with sessions() as session:
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            assert len(runs) == count and all(
                r.status == "completed" and r.retry_count == 0 for r in runs
            )
            if formal:
                from nexus.agents.codex_formal_stage import formal_stage_output_qualified

                read_as_of = await session.scalar(select(func.clock_timestamp()))
                for run in runs:
                    assert await formal_stage_output_qualified(
                        session, run.id, decision_time=read_as_of
                    )
            if flexible:
                from nexus.api.research_stages import read_stage_trace
                from nexus.ops.research_budget_diagnostic import timing_metadata
                from nexus.persistence.models.agents import AgentResearchOutputRecord

                for run in runs:
                    trace = await read_stage_trace(session, run)
                    assert trace.status == "verified" and len(trace.stages) == 3
                    outputs = list(
                        await session.scalars(
                            select(AgentResearchOutputRecord).where(
                                AgentResearchOutputRecord.run_id == run.id
                            )
                        )
                    )
                    timing = timing_metadata(outputs)["stage_timings"]["critique"]
                    assert timing["flexible_critique_profile"] == config["version"]
                    assert 45000 < timing["allocated_timeout_ms"] <= 116000
            assert all(
                r.pricing_version == config["version"] and r.model_hash == config_hash for r in runs
            )
            assert all(
                j.status == "succeeded" and j.attempt_count == 1
                for j in await session.scalars(select(Job))
            )
        assert calls == ["research", "critique", "synthesis"] * count
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_formal_v10_research_deadline_preserves_unknown_usage_and_closes_cycle(
    migrated_database_url, monkeypatch
):
    await test_full_cycle_worker_rederives_whole_pool_before_claim_or_sdk(
        migrated_database_url, monkeypatch, 3, False, True, True, research_timeout=True
    )
