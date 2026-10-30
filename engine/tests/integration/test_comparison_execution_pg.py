"""Normal comparison execution; only SDK process and HTTP provider edges are replaced."""

import json
from argparse import Namespace
from copy import copy
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from helpers.formal_sdk import controlled_sdk
from nexus.agents.chatgpt_grant import Grant
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.independent_input_view import restore_independent_input
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.responses_budget import UsageSnapshot
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from test_comparison_producer_pg import seed
from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch
from unit.agents.test_http_stage_lifecycle import Body, terminal


def stage_response(wire, route, seen):
    payload = _restore_view(wire)
    stage = payload["stage"]
    seen.append((route, stage))
    previous = payload.get("previous_claims", [])
    assert all(c["text"].startswith(route + ":") for c in previous)
    if stage == "synthesis":
        body = _assessment_payload(previous)
        body["synthesis_assessment"]["stance"] = "watch"
        body["synthesis_assessment"]["claims"][0]["text"] = route + ":保留缺口，保持观察。"
        for key in (
            "monitoring_adoption_hash",
            "daily_pending_adoption_hash",
            "disclosure_adoption_hash",
        ):
            body[key] = None
    else:
        body = _staged_batch(stage, payload["requested_roles"])
        for row in body["roles"]:
            row["claims"][0]["text"] = route + ":冻结来源支持该论点，结论保留证据缺口。"
            for key in (
                ("monitoring_proposal", "daily_pending_proposal", "disclosure_proposal")
                if stage == "research"
                else ("monitoring_review", "daily_pending_review", "disclosure_review")
            ):
                row[key] = None
    return body


@pytest.mark.asyncio
@pytest.mark.parametrize("api_failure", [False, True, "overrun"])
async def test_normal_sdk_and_http_chain_use_own_predecessors_and_common_persistence(
    migrated_database_url, monkeypatch, api_failure, tmp_path
):
    from nexus.agents import codex_staged
    from nexus.agents.chatgpt_grant import GrantStore
    from nexus.ops.research_comparison import native_command

    settings = EngineSettings(database_url=migrated_database_url)
    monkeypatch.setattr("nexus.ops.openai_local.local_settings", lambda *args, **kwargs: settings)

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, sdk_model, old = await seed(factory, source_kind="SEC")
    seen = []
    sdk_calls = controlled_sdk(monkeypatch)

    async def no_held_transaction():
        async with factory() as session:
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND xact_start IS NOT NULL"
                    )
                )
                == 0
            )

    async def invoke(payload, **kwargs):
        await no_held_transaction()
        body = stage_response(payload, "sdk", seen)
        kwargs["output_model"].model_validate(body)
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    bodies = []

    async def handle(request):
        await no_held_transaction()
        envelope = json.loads(request.content)
        assert envelope["store"] is False and envelope["tools"] == []
        assert request.headers["authorization"] == "Bearer fixture-token"
        payload = json.loads(envelope["input"][1]["content"])
        body = stage_response(payload, "responses", seen)
        fail = api_failure is True and restore_independent_input(payload)["stage"] == "critique"
        event = terminal("failed" if fail else "completed")
        event["response"]["output"][0]["content"][0]["text"] = json.dumps(body)
        event["response"]["usage"].update(input_tokens=300, output_tokens=80, total_tokens=380)
        if api_failure == "overrun":
            event["response"]["usage"].update(
                input_tokens=20_000_000, output_tokens=80, total_tokens=20_000_080
            )
        stream = Body(("data: " + json.dumps(event) + "\n\n").encode())
        bodies.append(stream)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    clients = []

    real_client = httpx.AsyncClient

    def client_factory(**kwargs):
        client = real_client(transport=httpx.MockTransport(handle), **kwargs)
        clients.append(client)
        return client

    grant = Grant({"access_token": "fixture-token"}, datetime.now(UTC).timestamp() + 3600, 0)
    chosen_grant = [grant]

    async def load_grant(self, **kwargs):
        assert self.directory == tmp_path
        return chosen_grant[0]

    monkeypatch.setattr(GrantStore, "load_verified", load_grant)
    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    source_path = tmp_path / "source.json"
    source_path.write_bytes(source)
    prepared_path = tmp_path / "prepared.json"
    prepared_path.write_text(json.dumps(prepared))
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(manifest.model_dump_json())
    runs = []
    for ordinal in (1, 2):
        async with factory.begin() as session:
            model = (
                sdk_model
                if ordinal == 1
                else await session.scalar(
                    select(AgentModelConfigRecord.id).where(
                        AgentModelConfigRecord.config_hash == manifest.api_config_hash
                    )
                )
            )
            admitted = await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=ordinal,
                model_config_id=model,
                daily_limit_usd=Decimal(4),
            )
        runs.append(admitted.run_id)
        args = Namespace(
            command="run-position",
            repo_root=tmp_path,
            source=source_path,
            prepared=prepared_path,
            manifest=manifest_path,
            position=ordinal,
            worker_id="comparison-offline",
            credentials_dir=tmp_path,
        )
        if ordinal == 1:
            chosen_grant[0] = replace(grant, expires_at=datetime.now(UTC).timestamp() - 1)
            with pytest.raises(ValueError, match="CHATGPT_ACCESS_EXPIRED"):
                await native_command(args)
            chosen_grant[0] = grant
            changed = manifest.model_copy(update={"api_config_hash": "b" * 64})
            manifest_path.write_text(changed.model_dump_json())
            with pytest.raises(ValueError, match="COMPARISON_REGISTERED_MANIFEST_CHANGED"):
                await native_command(args)
            manifest_path.write_text(manifest.model_dump_json())
            async with factory() as session:
                assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
                run = await session.get(AgentResearchRunRecord, admitted.run_id)
                from nexus.agents.execution_authority import load_frozen_evidence

                assert (
                    len(await load_frozen_evidence(session, run, allow_fixture_evidence=False)) == 2
                )
                forged = copy(run)
                forged.actor_id = "service"
                assert (
                    await load_frozen_evidence(session, forged, allow_fixture_evidence=False) == ()
                )
            assert not seen and not clients
        result = await native_command(args)
        assert (
            result["status"] == "position_executor_returned" and result["replay_allowed"] is False
        )
        if ordinal == 1:
            before = tuple(seen)
            with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED"):
                await native_command(args)
            assert tuple(seen) == before
        async with factory() as session:
            run = await session.get(AgentResearchRunRecord, admitted.run_id)
            if ordinal == 2 and api_failure == "overrun":
                from nexus.agents.persistence import _shared_daily_budget_used
                from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id
                    )
                )
                model_record = await session.get(AgentModelConfigRecord, run.model_config_id)
                cost = (
                    Decimal(20_000_000) * model_record.input_usd_per_million
                    + Decimal(80) * model_record.output_usd_per_million
                ) / Decimal(1_000_000)
                charged = await _shared_daily_budget_used(
                    session, business_date=budget.business_date
                )
                stopped = await session.scalar(text("SELECT stopped FROM research_stage_scope"))
                assert run.status == "failed"
                assert (run.input_tokens, run.output_tokens) == (20_000_000, 80)
                assert budget.state == "reserved" and budget.settled_usd is None
                assert run.settled_cost_usd is None and cost > budget.reserved_usd
                assert seen == [("sdk", s) for s in ("research", "critique", "synthesis")] + [
                    ("responses", "research")
                ]
                assert all(c.is_closed for c in clients) and all(b.closed == 1 for b in bodies)
                assert (stopped, charged >= cost + Decimal(2)) == (True, True), {
                    "scope_stopped": stopped,
                    "shared_charge": str(charged),
                    "known_cost": str(cost),
                    "reservation": str(budget.reserved_usd),
                }
                return
            assert run.status == ("failed" if ordinal == 2 and api_failure else "completed"), (
                seen,
                await session.scalar(
                    text(
                        "SELECT details FROM agent_research_events WHERE run_id=:run "
                        "ORDER BY sequence DESC LIMIT 1"
                    ),
                    {"run": run.id},
                ),
            )
            count = 2 if ordinal == 2 and api_failure else 3
            if run.status == "completed":
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM agent_research_outputs WHERE run_id=:run"),
                        {"run": run.id},
                    )
                    == 10
                )
            assert (run.input_tokens, run.output_tokens) == (count * 300, count * 80)
            proof = await read_terminal_proof(session, run_id=run.id, now=datetime.now(UTC))
            assert proof.known and proof.cleanup_confirmed and not proof.orphan
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:run"),
                    {"run": run.id},
                )
                == count
            )
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": run.id},
                )
                == "settled"
            )
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
                    {"id": old},
                )
                == "reserved"
            )
    assert seen == [("sdk", s) for s in ("research", "critique", "synthesis")] + [
        ("responses", s)
        for s in (
            ("research", "critique") if api_failure else ("research", "critique", "synthesis")
        )
    ]
    assert len(sdk_calls) == 3 and all(close.await_count == 1 for close, _ in sdk_calls)
    assert all(c.is_closed for c in clients) and all(b.closed == 1 for b in bodies)
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 2
        assert await session.scalar(text("SELECT stopped FROM research_stage_scope")) is False
        assert (
            await session.scalar(
                text("SELECT count(*) FROM agent_research_outputs WHERE run_id=:run"),
                {"run": runs[0]},
            )
            == 10
        )
