"""Normal legacy mixed admission: pause between observed native stages preserves usage."""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sec_primary",
    [
        False,
        True,
        "pure_price",
        "unknown_usage",
        "pause_inflight",
        "first_registration_wait",
        "first_registration_before_acquire",
    ],
)
async def test_legacy_mixed_pause_after_stage_one_has_no_next_send(
    migrated_database_url, monkeypatch, tmp_path, sec_primary
):
    import httpx
    from helpers import disclosure_observations as reader
    from helpers.mixed_stage import add_mixed_fields
    from helpers.monitor_materials import prepared_materials
    from helpers.paper_batch import CALENDAR_ID
    from integration import test_candidate_multistock_path as candidate
    from integration import test_monitor_batches_pg as batches
    from integration import test_monitor_disclosure_refresh_pg as refresh
    from integration import test_monitor_filing_content_pg as filing
    from integration.test_monitor_increment_pg import append_fresh_inputs
    from integration.test_sol_native_stage_pg import install_sol_http
    from integration.test_sol_profile_pg import current_record, register
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.agents.research_stages.protocol import StagedProtocol, stage_evidence
    from nexus.monitoring import durable_sec_storage as durable
    from nexus.ops.research_monitor import run
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.monitoring import (
        MonitorIncrement,
        MonitorObservationBatch,
        MonitorPlan,
        MonitorTrigger,
    )
    from nexus.persistence.models.runtime import JobLease
    from nexus.settings import AgentRuntimeMode
    from nexus.worker import run_agent_worker_once

    async def forbidden_real_http(*args, **kwargs):
        raise AssertionError("only the explicit synthetic MockTransport is permitted")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden_real_http)

    # The existing normal fixture is dated; all original observations are authored
    # at one recent legal session. New policy facts still use actual PostgreSQL time.
    at = datetime.now(UTC).replace(hour=15, minute=0, second=0, microsecond=0) - timedelta(days=1)
    while at.weekday() > 4:
        at -= timedelta(days=1)
    for module in (reader, candidate, batches, refresh, filing):
        monkeypatch.setattr(module, "AT", at)
    pure_price = sec_primary == "pure_price"
    unknown_usage = sec_primary == "unknown_usage"
    pause_inflight = sec_primary in ("pause_inflight", "unknown_usage")
    registration_wait = sec_primary == "first_registration_wait"
    registration_before = sec_primary == "first_registration_before_acquire"
    if pure_price:
        from nexus.monitoring.batches import scan_stock_batch

        factory, settings, store, captured, pointer = await batches.installed(
            migrated_database_url, monkeypatch, tmp_path, added=0, independent=True
        )
        async with factory.begin() as session:
            scanned = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            assert scanned["new_triggers"] == 1
        batch_id = UUID(scanned["batch_id"])
        prepared = await run(
            settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id)
        )
        assert prepared["status"] == "prepared", prepared
        materials_id = UUID(prepared["snapshot_id"])
    else:
        (
            factory,
            settings,
            store,
            captured,
            pointer,
            batch_id,
            materials_id,
        ) = await prepared_materials(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            sec_only=sec_primary is True,
            independent=True,
        )
    factory.kw.setdefault("info", {})["sec_exhibit_store"] = store
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    async with factory() as session:
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        original_parent = (
            parent.status,
            parent.settled_cost_usd,
            parent.input_tokens,
            parent.output_tokens,
        )
        batch = await session.get(MonitorObservationBatch, batch_id)
        assert batch.routing_version == "legacy_v1"
    captured["clock"][0] = later
    await candidate._set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    async with factory.begin() as session:
        next_day = later + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=next_day.date(),
                decision_at=next_day.replace(hour=21),
                market_open_at=next_day.replace(hour=13, minute=30),
                market_close_at=next_day.replace(hour=20),
            )
        )
    price = await current_record(factory)
    price["verified_at"] = (at - timedelta(minutes=1)).isoformat()
    model_id = await register(factory, price)
    settings = settings.model_copy(
        update={
            "agent_paper_formal_model_config_id": model_id,
            "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
            "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
            "openai_extraction_model": "gpt-6.1-sol",
            "openai_reasoning_model": "gpt-6.1-sol",
            "agent_paper_cycle_enabled": True,
        }
    )
    args = argparse.Namespace(
        action="admit-batch",
        batch_id=batch_id,
        materials_snapshot_id=materials_id,
        calendar_authority_id=CALENDAR_ID,
    )
    admitted = await run(settings, args)
    assert admitted["status"] == "admitted", admitted
    async with factory() as session:
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == UUID(admitted["run_id"]))
        )
        original_hold = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == member.run_id
            )
        )
        hold_identity = original_hold.id, original_hold.reserved_usd
        primary = await session.get(MonitorTrigger, member.trigger_id)
        assert (primary.kind == "disclosure") is (sec_primary is True)
        plan = await session.scalar(
            select(MonitorPlan).where(
                MonitorPlan.kind == "disclosure", MonitorPlan.instrument_id == pointer.instrument_id
            )
        )
    committed = []

    async def refresh_scope(*, pause):
        async with factory.begin() as session:
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
            from nexus.monitoring.durable_sec_permission import require_new_send
            from nexus.research.durable_sec_index_storage import persist_durable_index_archive

            received = datetime.now(UTC)

            def receipt(raw, url):
                return json.dumps(
                    dict(
                        source_url=url,
                        received_at=received.isoformat(),
                        sha256=sha256(raw).hexdigest(),
                        byte_length=len(raw),
                        http_status=200,
                    )
                ).encode()

            directory = (tmp_path / "current/directory.json").read_bytes()
            submissions = (tmp_path / "current/AAPL-submissions.json").read_bytes()
            await persist_durable_index_archive(
                session,
                store,
                identity_snapshot_id=rule.identity_snapshot_id,
                directory=(
                    directory,
                    receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
                ),
                submissions=(
                    submissions,
                    receipt(
                        submissions,
                        f"https://data.sec.gov/submissions/CIK{rule.issuer_cik}.json",
                    ),
                ),
                now=received,
            )
            # Current fresh source and active policy must actually permit this run.
            # Thus the next-stage rejection below cannot pass merely due to stale SEC.
            assert await require_new_send(session, member.run_id) is (not pure_price)
            if pause:
                await durable.change_state(
                    session, account_id=pointer.account_id, rule_id=rule.id, state="paused"
                )
            return rule.id

    active_rule_id = await refresh_scope(pause=False) if pause_inflight else None
    if registration_before:
        await refresh_scope(pause=True)
        committed.append("pause_committed_before_acquire")
    if unknown_usage:
        from integration import test_sol_native_stage_pg as native_fixture

        monkeypatch.setattr(native_fixture, "usage", lambda: None)

    async def pause_before_response(payload):
        if payload["stage"] == "research" and pause_inflight:
            # This callback is inside the controlled MockTransport, after its
            # explicit header trace and before returning response/usage bytes.
            async with factory.begin() as session:
                assert await stage_evidence(session, member.run_id)
                await durable.change_state(
                    session, account_id=pointer.account_id, rule_id=active_rule_id, state="paused"
                )
            committed.append("headers_sent_before_observe")

    calls, clients = install_sol_http(
        monkeypatch, store, model_response=add_mixed_fields, on_request=pause_before_response
    )
    if pause_inflight or registration_wait:
        original_transport = httpx.MockTransport.handle_async_request

        async def synthetic_headers(self, request):
            registration = None
            if registration_wait:
                async with factory.begin() as other:
                    assert (
                        await other.scalar(
                            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope,0))"),
                            {
                                "scope": (
                                    f"research-monitor:{pointer.account_id}:{pointer.instrument_id}"
                                )
                            },
                        )
                        is False
                    )
                registration = asyncio.create_task(refresh_scope(pause=True))
                await asyncio.sleep(0)
                assert not registration.done()
            trace = request.extensions.get("trace")
            assert trace is not None, "active SEC run must acquire its actual request fence"
            await trace("http11.send_request_headers.started", {})
            await trace("http11.send_request_headers.complete", {})
            if registration is not None:
                await asyncio.wait_for(registration, 10)
                committed.append("registration_waited_for_header_release")
            return await original_transport(self, request)

        monkeypatch.setattr(httpx.MockTransport, "handle_async_request", synthetic_headers)

    original_cleanup = StagedProtocol.cleanup

    async def cleanup_then_pause(self, ticket, **kwargs):
        await original_cleanup(self, ticket, **kwargs)
        if (
            ticket.run_id == member.run_id
            and ticket.stage == 1
            and not (pause_inflight or registration_wait or registration_before)
        ):
            async with factory() as session:
                original = await stage_evidence(session, ticket.run_id)
                assert len(original) == 1 and original[0]["confirmed"] is True
                assert original[0]["input_tokens"] is not None
                assert original[0]["verified"] is True
            await refresh_scope(pause=True)
            committed.append(ticket)

    monkeypatch.setattr(StagedProtocol, "cleanup", cleanup_then_pause)
    try:
        assert await run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert len(committed) == 1
        expected_calls = (
            ["research", "critique", "synthesis"]
            if pure_price
            else []
            if registration_before
            else ["research"]
        )
        assert calls == expected_calls, calls
        async with factory() as session:
            child = await session.get(AgentResearchRunRecord, member.run_id)
            rows = await stage_evidence(session, child.id)
            assert len(rows) == len(expected_calls)
            if unknown_usage:
                # An invalid completed frame has no verified response receipt.
                # Preserve absence plus its original authorization/cleanup and hold.
                assert all(row["response_hash"] is None for row in rows)
            else:
                assert all(row["response_hash"] is not None for row in rows)
            assert all(row["confirmed"] for row in rows)
            if unknown_usage:
                assert not all(row["verified"] is True for row in rows)
            else:
                assert all(row["verified"] is True for row in rows)
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert (hold.id, hold.reserved_usd) == hold_identity
            if unknown_usage or registration_before:
                assert child.settled_cost_usd is None
                assert hold.state == "reserved" and hold.settled_usd is None
                assert hold.reserved_usd > 0
            else:
                assert child.settled_cost_usd == Decimal("0.0299") * len(expected_calls)
                assert hold.state == "settled" and hold.settled_usd == child.settled_cost_usd
            parent_after = await session.get(AgentResearchRunRecord, parent.id)
            assert (
                parent_after.status,
                parent_after.settled_cost_usd,
                parent_after.input_tokens,
                parent_after.output_tokens,
            ) == original_parent
            if registration_before:
                from nexus.agents.research_stages.protocol import ProtocolRejected

                with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                    await read_terminal_proof(
                        session,
                        run_id=child.id,
                        now=await session.scalar(select(func.clock_timestamp())),
                    )
                terminal = (
                    (
                        await session.execute(
                            text(
                                "SELECT known,evidence,observed_stages "
                                "FROM research_stage_terminal WHERE run_id=:run"
                            ),
                            {"run": child.id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert terminal["known"] is False
                assert terminal["evidence"] == terminal["observed_stages"] == []
            else:
                proof = await read_terminal_proof(
                    session,
                    run_id=child.id,
                    now=await session.scalar(select(func.clock_timestamp())),
                )
                assert proof.known is (not unknown_usage) and proof.cleanup_confirmed
        assert not await run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert calls == expected_calls
    finally:
        for client in clients:
            await client.aclose()
        await factory.kw["bind"].dispose()
