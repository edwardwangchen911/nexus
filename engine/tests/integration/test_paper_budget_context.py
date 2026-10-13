"""Synthetic Paper contexts traverse the real worker; no live model qualification."""

import asyncio
import json
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
from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage_time_budget,complete_model,critique_timeout",
    [
        (False, False, None),
        (True, False, None),
        (True, True, None),
        (True, False, "stage_deadline"),
        (True, False, "invocation_deadline"),
        (True, False, "unknown"),
    ],
)
async def test_frozen_paper_context_reaches_budget_sdk_and_unknown_result_is_not_replayed(
    migrated_database_url, monkeypatch, stage_time_budget, complete_model, critique_timeout
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
    stages = []

    async def unknown_result(payload, **options):
        captured.setdefault("payload", payload)
        captured.setdefault("options", options)
        captured.setdefault("observed_at", asyncio.get_running_loop().time())
        stages.append((payload, options))
        if critique_timeout and payload["stage"] == "critique":
            from types import SimpleNamespace

            from nexus.agents.codex_sdk import CodexInvocationError
            from nexus.agents.sdk_stream_progress import observe_sdk_notifications

            reader = AsyncMock(
                return_value=SimpleNamespace(
                    method="item/reasoning/textDelta", payload="private-progress-canary"
                )
            )
            client = SimpleNamespace(_client=SimpleNamespace(next_turn_notification=reader))
            with observe_sdk_notifications(client, options["stream_observer"]):
                await client._client.next_turn_notification("private-turn-canary")

            failure = CodexInvocationError(
                "CODEX_TIMEOUT", phase="turn_run", provider_call_started=True
            )
            failure.timeout_source = critique_timeout
            failure.phase_elapsed_ms = {"turn_run": 11}
            raise failure
        if not complete_model and not critique_timeout:
            raise RuntimeError("synthetic-unknown-result")
        from nexus.agents.responses_budget import UsageSnapshot

        # Only the external SDK response is replaced. Context, stage compiler,
        # validation, worker, guarded DB terminal and timing remain real.
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        options["output_model"].model_validate(body)
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=80)

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
        assert sdk.await_count == (3 if complete_model else 2 if critique_timeout else 1), (
            observed_run.pricing_version,
            observed_run.status,
            reasons,
        )
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
            assert run.status == ("completed" if complete_model else "failed")
            assert run.pricing_version == (
                DEADLINE_BUDGET_CONFIG_VERSION if stage_time_budget else ACTIONABLE_CONFIG_VERSION
            )
            assert job.status == ("succeeded" if complete_model else "failed")
            assert job.attempt_count == 1
            from nexus.ops.research_budget_diagnostic import usage_is_known

            assert usage_is_known(run) is bool(complete_model)
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderRecord))
                == original_orders
            )
            other = await session.get(AgentResearchRunRecord, result.research_run_ids[1])
            assert other.status == "prepared"
        assert not await run_agent_worker_once(settings, job_ids=(job_id,))
        assert sdk.await_count == (3 if complete_model else 2 if critique_timeout else 1)
        if critique_timeout:
            from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

            assert [p["stage"] for p, _ in stages] == ["research", "critique"]
            async with sessions() as session:
                terminal = await session.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == run.id,
                        AgentResearchOutputRecord.status == "failed",
                    )
                )
                diagnostic = terminal.payload["diagnostic"]
                assert diagnostic["stage"] == "critique"
                assert diagnostic["invocation_phase"] == "turn_run"
                assert diagnostic["invocation_kind"] == "timeout"
                assert diagnostic["timeout_source"] == critique_timeout
                assert diagnostic["timing"]["phase_elapsed_ms"] == {"turn_run": 11}
                progress = diagnostic["timing"]["stream_progress"]
                assert progress["available"] and progress["progress_notifications"] == 1
                assert progress["notifications"] == 1
                assert progress["first_notification_ms"] is not None
                assert not progress["terminal_notification_seen"]
                assert "private-progress-canary" not in json.dumps(terminal.payload)
                assert "private-turn-canary" not in json.dumps(terminal.payload)
                assert 0 < diagnostic["timing"]["allocated_timeout_ms"] <= 78000
                assert set(terminal.payload["stage_timings"]) == {"research"}
                roles = tuple(
                    await session.scalars(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == run.id,
                            AgentResearchOutputRecord.role.is_not(None),
                        )
                    )
                )
                assert len(roles) == 5
                reservation = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id
                    )
                )
                assert reservation.state == "reserved"
                assert reservation.settled_usd is None
        if complete_model:
            from nexus.agents.codex_sdk import codex_request_wire

            assert [p["stage"] for p, _ in stages] == ["research", "critique", "synthesis"]
            frozen_hashes = set()
            for submitted, sdk_options in stages:
                restored = _restore_view(submitted["frozen_inputs"])
                bound = [
                    item["agent_paper_context"]
                    for item in restored["security_snapshot"]["items"]
                    if "agent_paper_context" in item
                ]
                assert bound == [context]
                assert "paper_portfolio supplies" in submitted["instructions"]
                assert len(codex_request_wire(submitted).encode()) <= 64000
                assert 0 < sdk_options["timeout_seconds"] <= 108
            async with sessions() as session:
                outputs = list(
                    await session.scalars(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == run.id
                        )
                    )
                )
                roles = [output for output in outputs if output.role is not None]
                assert len(roles) == 8
                for output in roles:
                    frozen_hashes.add(output.payload["frozen_inputs_hash"])
                assert len(frozen_hashes) == 1
                synthesized = [output for output in outputs if output.status == "synthesized"]
                assert len(synthesized) == 1
                timed = synthesized[0].payload["stage_timings"]
                assert set(timed) == {"research", "critique", "synthesis"}
                assert all(timing["allocated_timeout_ms"] > 0 for timing in timed.values())
                assert all(timing["elapsed_ms"] >= 0 for timing in timed.values())
                assert run.input_tokens == 900 and run.output_tokens == 240
            import httpx
            from helpers.portfolio_api import app, headers

            path = f"/internal/v1/research-runs/{run.id}"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app(migrated_database_url)),
                base_url="http://testserver",
            ) as client:
                response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            trace = response.json()["data"]["stage_trace"]
            assert trace["status"] == "verified"
            assert [stage["status"] for stage in trace["stages"]] == ["complete"] * 3
            assert {stage["frozen_inputs_hash"] for stage in trace["stages"]} == frozen_hashes
    finally:
        await engine.dispose()
