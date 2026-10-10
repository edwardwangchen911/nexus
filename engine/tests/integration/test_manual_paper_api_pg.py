"""HTTP/CLI and host dispatch on temporary PG with controlled source authority."""

import asyncio
import json
import subprocess
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.main import create_engine_app
from nexus.ops import manual_paper
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop
from nexus.ops.manual_paper_host import (
    claim_configured_manual_request,
    manual_work_pending,
    peek_manual_request,
)
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.parametrize(
    "claim_before_close", [False, True, "dispatch_unknown", "dispatch_success"]
)
@pytest.mark.asyncio
async def test_manual_http_and_cli_bind_protected_scope_and_read_persisted_only(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    capsys,
    count,
    mode,
    claim_before_close,
    foreground_dispatch=False,
):
    at = AT - timedelta(minutes=20) if foreground_dispatch else AT
    dispatch_unknown = claim_before_close == "dispatch_unknown"
    dispatch_success = claim_before_close == "dispatch_success"
    claim_before_close = bool(claim_before_close)
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    candidates = (
        INSTRUMENTS
        if count == 3
        else INSTRUMENTS
        + (
            (UUID(int=101), Decimal("70"), Decimal("0"), "industrials"),
            (UUID(int=102), Decimal("80"), Decimal("0"), "consumer_staples"),
        )
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=candidates, decision_time=at
    )
    scalar = AsyncSession.scalar
    clock_at = [at]

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock_at[0]
        return await scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    try:
        async with sessions.begin() as session:
            next_at = at + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=at - timedelta(hours=1),
                )
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="api-manual-full-pool",
                created_at=at,
            )
        settings = EngineSettings(
            **{
                **api_settings(migrated_database_url).model_dump(),
                "agent_runtime_mode": "codex_sdk",
                "agent_paper_account_id": authority.account_id,
                "agent_paper_candidate_batch_id": batch.batch_id,
                "agent_paper_formal_model_config_id": model.id,
                "agent_paper_cycle_enabled": True,
                "agent_paper_manual_enabled": True,
                "release_revision": "a" * 40,
            }
        )
        app = create_engine_app(settings)
        app.state.utc_clock = lambda: at + timedelta(days=10)  # must not choose the business clock
        path = "/internal/v1/paper/manual-requests"
        body = json.dumps({"mode": mode}, separators=(",", ":")).encode()

        async def post(client, key="click", payload=body):
            return await client.post(
                path,
                content=payload,
                headers={
                    **headers("POST", path, payload),
                    "Idempotency-Key": key,
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (
                await client.get(
                    path, headers={**headers("GET", path, b""), "Idempotency-Key": "never-recorded"}
                )
            ).json()["data"] is None
            assert (await client.post(path, content=body)).status_code == 401
            assert (
                await post(
                    client,
                    payload=b'{"mode":"research_only","account_id":"00000000-0000-0000-0000-000000000001"}',
                )
            ).status_code == 422
            assert (await post(client)).json()["error"]["details"][
                "reason_code"
            ] == "MANUAL_HOST_NOT_READY"
            async with sessions.begin() as session:
                assert (
                    await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 0
                )
            await heartbeat_agent_paper_loop(
                settings,
                account_id=authority.account_id,
                worker_id=f"agent-paper-loop-{UUID(int=1)}",
                started_at=at - timedelta(minutes=1),
                seen_at=at,
            )
            settings.formal_budget_protection_enabled = True
            assert (await post(client)).json()["error"]["details"][
                "reason_code"
            ] == "MANUAL_BUDGET_PROTECTED"
            settings.formal_budget_protection_enabled = False
            settings.agent_paper_candidate_batch_id = UUID(int=993)
            assert (await post(client)).json()["error"]["details"][
                "reason_code"
            ] == "MANUAL_HOST_NOT_READY"
            settings.agent_paper_candidate_batch_id = batch.batch_id
            result = await post(client)
            assert result.status_code == 200, result.text
            receipt = result.json()["data"]
            assert result.json()["meta"]["as_of"].startswith(at.isoformat()[:19])
            assert receipt["status"] == "queued" and receipt["execution_locked"] is True
            assert receipt["requested_at"].startswith(at.isoformat()[:19])
            request_id = UUID(receipt["request_id"])
            assert (await post(client)).json()["data"] == receipt
            opposite_mode = "research_and_paper" if mode == "research_only" else "research_only"
            conflict = await post(client, payload=json.dumps({"mode": opposite_mode}).encode())
            assert (
                conflict.status_code == 409
                and conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
            )
            assert (await post(client, key="another-click")).status_code == 409
            get_path = f"{path}/{request_id}"
            assert (await client.get(get_path, headers=headers("GET", get_path))).json()[
                "data"
            ] == receipt
            assert (
                await client.get(
                    f"{path}/{UUID(int=991)}", headers=headers("GET", f"{path}/{UUID(int=991)}")
                )
            ).status_code == 404
            # Disable/rotate config: exact key reads original receipt before all new-request gates.
            settings.agent_paper_manual_enabled = False
            settings.agent_paper_candidate_batch_id = UUID(int=999)
            assert (await post(client)).json()["data"] == receipt
            assert (await post(client, key="disabled-new")).json()["error"]["details"][
                "reason_code"
            ] == "MANUAL_ENTRY_DISABLED"
            cli_args = manual_paper.parser().parse_args(["read", "--request-id", str(request_id)])
            assert (await manual_paper.run(settings, cli_args)).model_dump(mode="json") == receipt
            replay_args = manual_paper.parser().parse_args(
                ["start", "--mode", mode, "--idempotency-key", "click"]
            )
            assert (await manual_paper.run(settings, replay_args)).request_id == request_id
            # Invoke the registered command's real parser/config/transaction path,
            # with an ignored private file in an isolated temporary Git checkout.
            subprocess.run(
                ["git", "init", "--quiet", str(tmp_path)], check=True, capture_output=True
            )
            (tmp_path / ".gitignore").write_text(".secrets/\n")
            private = tmp_path / ".secrets"
            private.mkdir(mode=0o700)
            env_file = private / "manual.env"
            env_file.write_text(
                f"NEXUS_ENV=test\nDATABASE_URL={migrated_database_url}\n"
                f"NEXUS_AGENT_PAPER_ACCOUNT_ID={authority.account_id}\n"
                "NEXUS_AGENT_PAPER_MANUAL_ENABLED=false\n"
                f"NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID={UUID(int=999)}\n"
            )
            env_file.chmod(0o600)
            try:
                for command in (
                    ["read", "--request-id", str(request_id)],
                    ["start", "--mode", mode, "--idempotency-key", "click"],
                ):
                    capsys.readouterr()
                    code = await asyncio.to_thread(
                        manual_paper.main,
                        [
                            "--repo-root",
                            str(tmp_path),
                            "--env-file",
                            ".secrets/manual.env",
                            *command,
                        ],
                    )
                    output = capsys.readouterr().out
                    assert code == 0 and json.loads(output) == receipt
            finally:
                env_file.unlink()
            # Configured account controls reads even when a UUID was learned elsewhere.
            settings.agent_paper_account_id = UUID(int=992)
            assert (await client.get(get_path, headers=headers("GET", get_path))).status_code == 404
            assert (
                await client.get(
                    path, headers={**headers("GET", path, b""), "Idempotency-Key": "click"}
                )
            ).json()["data"] is None
            settings.agent_paper_account_id = authority.account_id
            assert (
                await client.get(
                    path, headers={**headers("GET", path, b""), "Idempotency-Key": "click"}
                )
            ).json()["data"] == receipt
            settings.agent_paper_candidate_batch_id = batch.batch_id
            settings.agent_paper_manual_enabled = True
            assert await manual_work_pending(settings, account_id=authority.account_id)
            expected_receipt = receipt
            if dispatch_success:
                from nexus.ops import agent_paper_once
                from nexus.ops.manual_paper_host import dispatch_manual_request
                from nexus.paper.manual_request import read_manual_request

                calls = []

                async def controlled_authority(*_args, **kwargs):
                    assert kwargs["manual_market_window"] is True
                    assert kwargs["account_id"] == authority.account_id
                    assert {item.instrument_id for item in kwargs["identities"]} == set(
                        batch.instrument_ids
                    )
                    calls.append(kwargs["idempotency_key"])
                    return authority

                monkeypatch.setattr(
                    agent_paper_once, "prepare_schwab_decision_authority", controlled_authority
                )
                monkeypatch.setattr(
                    agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object()
                )
                trigger_settings = agent_paper_once.AgentPaperTriggerSettings.model_validate(
                    settings.model_dump()
                    | {
                        "object_store_endpoint": "http://localhost:9000",
                        "alpaca_key": "fixture",
                        "alpaca_secret": "fixture",
                    }
                )
                if foreground_dispatch:
                    from dataclasses import replace

                    from nexus.ops import agent_paper_loop as loop
                    from nexus.ops.manual_paper_host import (
                        inspect_manual_close,
                        inspect_manual_queue,
                    )
                    from nexus.paper import agent_cycle

                    environment = trigger_settings.model_dump()
                    monkeypatch.setattr(
                        loop, "load_agent_paper_service_environment", lambda *_a, **_k: environment
                    )
                    actions = []
                    inspections = []

                    async def inspect():
                        state = await loop.inspect_agent_paper_loop(
                            settings,
                            account_id=authority.account_id,
                            expected_calendar_authority_id=CALENDAR_ID,
                        )
                        state = await inspect_manual_close(
                            settings, account_id=authority.account_id, snapshot=state
                        )
                        state = await inspect_manual_queue(
                            settings, account_id=authority.account_id, snapshot=state
                        )
                        inspections.append(state)
                        if len(actions) < 2:
                            assert loop.choose_loop_action(state) in {
                                loop.AgentPaperLoopAction.TRIGGER_MANUAL,
                                loop.AgentPaperLoopAction.FINISH_MANUAL,
                            }
                        return replace(state, enabled=False) if len(actions) == 2 else state

                    async def execute(action, state):
                        assert action in {
                            loop.AgentPaperLoopAction.TRIGGER_MANUAL,
                            loop.AgentPaperLoopAction.FINISH_MANUAL,
                        }
                        actions.append(action)
                        await loop.execute_agent_paper_loop_action(
                            settings,
                            account_id=authority.account_id,
                            repo_root=tmp_path,
                            action=action,
                            snapshot=state,
                        )
                        if action == loop.AgentPaperLoopAction.TRIGGER_MANUAL:
                            # Controlled native input failure, before any research/model invocation.
                            # Tests recovery consumption, not successful investment research.
                            async with sessions.begin() as session:
                                row = await session.get(ManualPaperRequest, request_id)
                                active_view = await read_manual_request(session, row)
                                cycle = await session.get(
                                    AgentPaperCycleRecord, active_view.cycle_id
                                )
                                events = await agent_cycle._events(session, cycle.id)
                                await agent_cycle._stop_unavailable(session, cycle, events)
                            clock_at[0] += timedelta(seconds=5)

                    async def heartbeat(seen_at):
                        await heartbeat_agent_paper_loop(
                            settings,
                            account_id=authority.account_id,
                            worker_id=f"agent-paper-loop-{UUID(int=1)}",
                            started_at=at - timedelta(minutes=1),
                            seen_at=seen_at,
                        )

                    async def heartbeat_watchdog_wait(_seconds):
                        # Heartbeat watchdog runs concurrently with an action and is cancelled
                        # when it ends. Inspection above rejects business wait actions.
                        await asyncio.Future()

                    assert (
                        await loop.run_foreground_loop(
                            inspect=inspect,
                            execute=execute,
                            heartbeat=heartbeat,
                            sleep=heartbeat_watchdog_wait,
                            clock=lambda: clock_at[0],
                        )
                        == loop.AgentPaperLoopAction.STOP_DISABLED
                    )
                    assert actions == [
                        loop.AgentPaperLoopAction.TRIGGER_MANUAL,
                        loop.AgentPaperLoopAction.FINISH_MANUAL,
                    ]
                    assert len(inspections) == 3
                else:
                    await dispatch_manual_request(
                        settings,
                        trigger_settings,
                        request_id=request_id,
                        repo_root=tmp_path,
                        calendar_authority_id=CALENDAR_ID,
                        process_environment={},
                    )
                await dispatch_manual_request(
                    settings,
                    trigger_settings,
                    request_id=request_id,
                    repo_root=tmp_path,
                    calendar_authority_id=CALENDAR_ID,
                    process_environment={},
                )
                assert calls == [f"manual-entry:{request_id}"]
                async with sessions() as session:
                    row = await session.get(ManualPaperRequest, request_id)
                    view = await read_manual_request(session, row)
                    assert (
                        view.status == ("unavailable" if foreground_dispatch else "active")
                        and view.cycle_id is not None
                    )
                    cycle = await session.get(AgentPaperCycleRecord, view.cycle_id)
                    assert cycle.execution_locked is True
                    assert cycle.candidate_batch_id == batch.batch_id
                    jobs = tuple(await session.scalars(select(Job)))
                    assert len(jobs) == count
                    assert all(
                        job.status == ("skipped" if foreground_dispatch else "queued")
                        and job.attempt_count == (1 if foreground_dispatch else 0)
                        for job in jobs
                    )
                    expected_receipt = view.model_dump(mode="json")
            elif dispatch_unknown:
                from nexus.ops import agent_paper_once
                from nexus.ops.manual_paper_host import dispatch_manual_request
                from nexus.paper.manual_request import read_manual_request

                calls = []

                async def uncertain_trigger(_settings, request, **_kwargs):
                    async with sessions() as session:
                        row = await session.get(ManualPaperRequest, request_id)
                        persisted = await read_manual_request(session, row)
                        assert persisted.status == "preparing"
                    assert request.trigger_mode == "manual"
                    assert request.manual_request_id == request_id
                    assert request.candidate_batch_id == batch.batch_id
                    assert request.formal_model_config_id == model.id
                    calls.append(request.idempotency_key)
                    raise RuntimeError("sensitive-canary")

                monkeypatch.setattr(agent_paper_once, "trigger_once", uncertain_trigger)
                trigger_settings = agent_paper_once.AgentPaperTriggerSettings.model_validate(
                    settings.model_dump()
                    | {
                        "object_store_endpoint": "http://localhost:9000",
                        "alpaca_key": "fixture",
                        "alpaca_secret": "fixture",
                    }
                )
                with pytest.raises(ValueError, match="^MANUAL_RESULT_UNKNOWN$"):
                    await dispatch_manual_request(
                        settings,
                        trigger_settings,
                        request_id=request_id,
                        repo_root=tmp_path,
                        calendar_authority_id=CALENDAR_ID,
                        process_environment={},
                    )
                await dispatch_manual_request(
                    settings,
                    trigger_settings,
                    request_id=request_id,
                    repo_root=tmp_path,
                    calendar_authority_id=CALENDAR_ID,
                    process_environment={},
                )
                assert calls == [f"manual-entry:{request_id}"]
                async with sessions() as session:
                    row = await session.get(ManualPaperRequest, request_id)
                    view = await read_manual_request(session, row)
                    assert view.status == "unknown"
                    expected_receipt = view.model_dump(mode="json")
            elif claim_before_close:
                async with sessions.begin() as session:
                    view, allowed = await claim_configured_manual_request(
                        session, settings, request_id=request_id
                    )
                    assert allowed and view.status == "preparing"
                    expected_receipt = view.model_dump(mode="json")
                async with sessions.begin() as session:
                    replay, allowed = await claim_configured_manual_request(
                        session, settings, request_id=request_id
                    )
                    assert not allowed and replay == view
            clock_at[0] = at.replace(hour=20, minute=1)
            async with sessions.begin() as session:
                await session.execute(update(WorkerHeartbeat).values(seen_at=clock_at[0]))
            assert (await post(client, key="after-close")).json()["error"]["details"][
                "reason_code"
            ] == "MANUAL_SESSION_UNAVAILABLE"
            assert (await post(client)).json()["data"] == expected_receipt
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request_id)
            assert row.candidate_batch_id == batch.batch_id and row.model_config_id == model.id
            assert len(batch.instrument_ids) == count
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 1
            assert await session.scalar(
                select(func.count()).select_from(ManualPaperRequestEvent)
            ) == (
                4
                if foreground_dispatch
                else 3
                if dispatch_unknown or dispatch_success
                else 2
                if claim_before_close
                else 1
            )
            assert await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            ) == (1 if dispatch_success else 0)
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            assert "account_id" not in json.dumps(receipt)
        async with sessions.begin() as session:
            view, allowed = await claim_configured_manual_request(
                session, settings, request_id=request_id
            )
            assert not allowed
            assert view.status == (
                "unavailable"
                if foreground_dispatch
                else "unknown"
                if dispatch_unknown
                else "active"
                if dispatch_success
                else "preparing"
                if claim_before_close
                else "unavailable"
            )
            pending = await peek_manual_request(session, account_id=authority.account_id)
            assert (pending is not None) == (claim_before_close and not foreground_dispatch)
            assert await session.scalar(
                select(func.count()).select_from(ManualPaperRequestEvent)
            ) == (4 if foreground_dispatch else 3 if dispatch_unknown or dispatch_success else 2)
        assert await manual_work_pending(settings, account_id=authority.account_id) == (
            claim_before_close and not foreground_dispatch
        )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("count", [3, 5])
@pytest.mark.parametrize("mode", ["research_only", "research_and_paper"])
@pytest.mark.asyncio
async def test_manual_foreground_consumes_persisted_request_and_native_failure(
    migrated_database_url, monkeypatch, tmp_path, capsys, count, mode
):
    await test_manual_http_and_cli_bind_protected_scope_and_read_persisted_only(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        capsys,
        count,
        mode,
        claim_before_close="dispatch_success",
        foreground_dispatch=True,
    )
