"""Full normal native executor with only the HTTP provider replaced; no real calls."""

import json
from argparse import Namespace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from integration.test_comparison_execution_pg import stage_response
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_execution import execute_comparison_position
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from unit.agents.test_http_stage_lifecycle import Body, terminal


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "changed_key", "missing_cache"])
async def test_three_full_native_chains_share_actual_cache_fees_and_no_sdk(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    settings = EngineSettings(
        database_url=migrated_database_url, agent_daily_budget_usd=Decimal(20)
    )
    f = session_factory(settings)
    source, prepared, m, model, old, protected = await setup(f, monkeypatch, source_kind="SEC")
    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch)
    from nexus.agents import codex_staged

    async def forbidden(*a, **k):
        raise AssertionError("Platform must never fall back to SDK")

    monkeypatch.setattr(codex_staged, "invoke_codex", forbidden)
    seen = []
    clients = []
    wires = []

    async def handle(request):
        async with f() as s:
            assert (
                await s.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND xact_start IS NOT NULL"
                    )
                )
                == 0
            )
        assert request.headers["authorization"] == "Bearer synthetic-platform-native-funding"
        assert request.headers["openai-project"] == "proj-fixture"
        body = json.loads(request.content)
        wires.append(body)
        payload = json.loads(body["input"][1]["content"])
        answer = stage_response(payload, "platform", seen)
        event = terminal("completed")
        event["response"]["service_tier"] = "default"
        event["response"]["output"][0]["content"][0]["text"] = json.dumps(answer)
        event["response"]["usage"] = {
            "input_tokens": 300,
            "output_tokens": 80,
            "total_tokens": 380,
            "input_tokens_details": {
                "cached_tokens": 100,
                "cache_write_tokens": 100,
                "cache_write_tokens_details": {"ttl_30m_tokens": 100, "ttl_12h_tokens": 0},
            },
            "output_tokens_details": {"reasoning_tokens": 60},
        }
        if mode == "changed_key":
            from nexus.ops.platform_credentials import PLATFORM_KEY_PATH

            (tmp_path / PLATFORM_KEY_PATH).write_text("synthetic-changed-key")
        if mode == "missing_cache":
            del event["response"]["usage"]["input_tokens_details"]["cache_write_tokens"]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(event) + "\n\n").encode()),
        )

    def client_factory():
        c = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        clients.append(c)
        return c

    from nexus.agents import comparison_execution
    from nexus.agents.chatgpt_grant import GrantStore
    from nexus.ops import openai_local
    from nexus.ops.research_comparison import native_command

    async def with_mock_transport(**kwargs):
        return await execute_comparison_position(**kwargs, client_factory=client_factory)

    monkeypatch.setattr(comparison_execution, "execute_comparison_position", with_mock_transport)
    real_settings = openai_local.local_settings

    def settings_for_command(*a, **k):
        return settings if k.get("load_model_key") is False else real_settings(*a, **k)

    monkeypatch.setattr(openai_local, "local_settings", settings_for_command)
    monkeypatch.setattr(GrantStore, "load_verified", forbidden)
    paths = {}
    for name, content in {
        "source": source,
        "prepared": json.dumps(prepared).encode(),
        "manifest": m.model_dump_json().encode(),
    }.items():
        paths[name] = tmp_path / (name + ".json")
        paths[name].write_bytes(content)
    args = Namespace(
        command="admit-position",
        repo_root=tmp_path,
        position=1,
        model_config_id=model,
        funding_proof=tmp_path / "funding.json",
        worker_id="platform-native-nine",
        **paths,
    )
    try:
        for ordinal in (1, 2, 3):
            args.command, args.position = "admit-position", ordinal
            admitted = await native_command(args)
            assert admitted["status"] == "position_prepared"
            args.command = "run-position"
            reply = await native_command(args)
            assert reply["status"] == "position_executor_returned"
            assert reply["readback_required"] and not reply["replay_allowed"]
            if mode == "complete":
                before = tuple(seen)
                with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED"):
                    await native_command(args)
                assert tuple(seen) == before
            async with f() as s:
                proof = await read_terminal_proof(
                    s, run_id=UUID(admitted["run_id"]), now=datetime.now(UTC)
                )
                if mode != "complete":
                    assert proof.known is False
                    assert not proof.budget_settled and proof.cleanup_confirmed
                    held = await s.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
                        )
                    )
                    assert held.state == "reserved" and held.settled_usd is None
                    assert (
                        await s.scalar(
                            text(
                                "SELECT to_jsonb(b) FROM agent_daily_budget_reservations b "
                                "WHERE id=:id"
                            ),
                            {"id": old},
                        )
                        == protected
                    )
                    assert seen == [("platform", "research")]
                    assert all(c.is_closed for c in clients)
                    return
                assert (
                    proof.known
                    and proof.budget_settled
                    and proof.cleanup_confirmed
                    and not proof.orphan
                )
                budget = await s.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
                    )
                )
                assert budget.settled_usd == Decimal("0.00429")
                assert (
                    await s.scalar(
                        text(
                            "SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"
                        ),
                        {"id": old},
                    )
                    == protected
                )
        assert seen == [
            ("platform", s) for _ in range(3) for s in ("research", "critique", "synthesis")
        ]
        assert [w["max_output_tokens"] for w in wires] == [8192, 8112, 8032] * 3
        assert all(c.is_closed for c in clients) and len(clients) == 9
    finally:
        await f.kw["bind"].dispose()
