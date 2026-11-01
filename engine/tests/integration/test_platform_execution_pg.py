"""Full normal native executor with only the HTTP provider replaced; no real calls."""

import json
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from integration.test_comparison_execution_pg import stage_response
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_execution import execute_comparison_position
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from unit.agents.test_http_stage_lifecycle import Body, terminal


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_spend", [False, True], ids=["gift", "explicit-spend"])
@pytest.mark.parametrize(
    "mode",
    [
        "complete",
        "changed_key",
        "missing_cache",
        "wait_key",
        "wait_proof",
        "wait_console",
        "wait_price",
        "wait_authorization",
    ],
)
async def test_three_full_native_chains_share_actual_cache_fees_and_no_sdk(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    mode,
    explicit_spend,
    price_version=PLATFORM_PRICE_VERSION,
    purpose=None,
):
    settings = EngineSettings(
        database_url=migrated_database_url, agent_daily_budget_usd=Decimal(20)
    )
    f = session_factory(settings)
    source, prepared, m, model, old, protected = await setup(
        f, monkeypatch, source_kind="SEC", price_version=price_version, purpose=purpose
    )
    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch, explicit_spend=explicit_spend)
    from nexus.agents import codex_staged

    async def forbidden(*a, **k):
        raise AssertionError("Platform must never fall back to SDK")

    monkeypatch.setattr(codex_staged, "invoke_codex", forbidden)
    if mode.startswith("wait_"):
        from nexus.agents import platform_funding
        from nexus.ops.platform_credentials import PLATFORM_KEY_PATH

        check_funding = platform_funding.require_platform_funding
        expired_at_send = [False]
        if mode in ("wait_price", "wait_authorization"):
            from nexus.agents import comparison_execution as execution_clock

            class SendBoundaryClock:
                @staticmethod
                def now(tz):
                    value = datetime.now(tz)
                    if expired_at_send[0]:
                        expired_at_send[0] = False
                        return value + (
                            timedelta(days=2) if mode == "wait_price" else timedelta(hours=1)
                        )
                    return value

            monkeypatch.setattr(execution_clock, "datetime", SendBoundaryClock)

        async def change_while_qualifying(session, manifest, funding, **kwargs):
            await check_funding(session, manifest, funding, **kwargs)
            if kwargs.get("required_valid_until") is not None:
                # This is after the native pre-send loader and its async DB
                # checks, before the owning transaction exits and HTTP sends.
                if mode in ("wait_price", "wait_authorization"):
                    expired_at_send[0] = True
                elif mode == "wait_key":
                    (tmp_path / PLATFORM_KEY_PATH).write_text("synthetic-key-changed-during-wait")
                else:
                    path = tmp_path / ("funding.json" if mode == "wait_proof" else "console.txt")
                    path.write_text(path.read_text() + "\n")

        monkeypatch.setattr(platform_funding, "require_platform_funding", change_while_qualifying)
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
        if explicit_spend:
            assert "openai-project" not in request.headers
            assert "openai-organization" not in request.headers
        else:
            assert request.headers["openai-project"] == "proj-fixture"
        body = json.loads(request.content)
        wires.append(body)
        payload = json.loads(body["input"][1]["content"])
        if mode.startswith("http_"):
            # Observe the actual requested stage even when no answer is generated.
            seen.append(("platform", payload["stage"]))
            return httpx.Response(
                int(mode.removeprefix("http_")),
                headers={"content-type": "application/json"},
                stream=Body(
                    b'{"error":{"code":"insufficient_quota","message":"PRIVATE_SENTINEL"}}'
                ),
            )
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
            if explicit_spend and mode == "complete" and ordinal == 3:
                # Revocation/expiry after all sends cannot block fact readback or closeout.
                path = tmp_path / "funding.json"
                record = json.loads(path.read_text())
                record["window_end"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
                path.write_text(json.dumps(record))
            read_args = Namespace(
                command="inspect-position",
                repo_root=tmp_path,
                experiment_id=m.experiment_id,
                position=ordinal,
            )
            try:
                readback = await native_command(read_args)
            except ValueError as error:
                if str(error) == "COMPARISON_TERMINAL_PROOF_INVALID":
                    # Synthetic fixtures only: retain the exact rejected proof
                    # line without changing the public safe error or its object.
                    raise error from error.__context__
                raise
            assert readback["status"] == "position_registered"
            assert readback["attempt_count"] == 1 and not readback["replay_allowed"]
            assert readback["terminal_proof"]["known"] is (mode == "complete")
            read_args.command = "close-expired-position"
            assert await native_command(read_args) == readback
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
                    if mode.startswith("http_"):
                        from nexus.agents.committee import StageFailureDiagnostic
                        from nexus.persistence.models.agents import AgentResearchOutputRecord

                        output = await s.scalar(
                            select(AgentResearchOutputRecord).where(
                                AgentResearchOutputRecord.run_id == UUID(admitted["run_id"]),
                                AgentResearchOutputRecord.status == "failed",
                            )
                        )
                        assert output.payload["reason"] == "model_usage_uncertain"
                        diagnostic = StageFailureDiagnostic.model_validate(
                            output.payload["diagnostic"]
                        )
                        facts = diagnostic.http_failure_facts
                        assert facts is not None
                        assert facts.phase == "headers" and facts.error_kind == "http_status"
                        assert facts.http_status == int(mode.removeprefix("http_"))
                        assert facts.provider_error_code == "insufficient_quota"
                        assert (
                            facts.send_started
                            and facts.headers_received
                            and facts.cleanup_confirmed
                        )
                        assert not facts.terminal_seen and diagnostic.failure_facts is None
                        assert readback["http_failure_facts"] == facts.model_dump(mode="json")
                        assert "PRIVATE_SENTINEL" not in json.dumps(output.payload)
                        assert len(wires) == len(clients) == 1
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
                    assert seen == ([] if mode.startswith("wait_") else [("platform", "research")])
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


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "missing_cache", "wait_price", "wait_authorization"])
@pytest.mark.parametrize("explicit_spend", [False, True], ids=["gift", "explicit-spend-v2"])
async def test_renewed_price_full_worker_http_and_fail_closed_matrix(
    migrated_database_url, monkeypatch, tmp_path, mode, explicit_spend
):
    await test_three_full_native_chains_share_actual_cache_fees_and_no_sdk(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        mode,
        explicit_spend,
        price_version=CURRENT_PLATFORM_PRICE_VERSION,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["http_400", "http_401", "http_429", "http_502"])
async def test_http_failure_survives_real_worker_output_and_normal_inspection(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    await test_three_full_native_chains_share_actual_cache_fees_and_no_sdk(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        mode,
        True,
        price_version=CURRENT_PLATFORM_PRICE_VERSION,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "missing_cache", "wait_authorization", "http_400"])
async def test_third_round_full_native_worker_keeps_unknown_budget_and_new_authority(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    await test_three_full_native_chains_share_actual_cache_fees_and_no_sdk(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        mode,
        True,
        price_version=CURRENT_PLATFORM_PRICE_VERSION,
        purpose="platform-terra-v3-20261101",
    )
