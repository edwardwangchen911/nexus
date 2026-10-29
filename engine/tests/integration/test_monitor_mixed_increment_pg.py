"""One sealed stock batch reaches one native job, never three event jobs."""

import argparse
import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.monitor_materials import prepared_materials
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.agents.persistence import _shared_daily_budget_used
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentResearchRunRecord,
    PortfolioDecisionBudgetReservationRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import AgentRuntimeMode
from sqlalchemy import func, select

_BUDGET_MODELS = (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    PortfolioDecisionBudgetReservationRecord,
)


async def budget_rows(session):
    return {
        model: {
            row.id: tuple((c.name, getattr(row, c.name)) for c in row.__table__.columns)
            for row in await session.scalars(select(model))
        }
        for model in _BUDGET_MODELS
    }


async def assert_shared_day_preserved(session, originals, day, used, child_hold):
    current = await budget_rows(session)
    for model, rows in originals.items():
        assert all(current[model].get(key) == value for key, value in rows.items())
        assert set(current[model]) - set(rows) == (
            {child_hold.id} if model is AgentDailyBudgetReservationRecord else set()
        )
    assert child_hold.business_date == day
    child_used = (
        child_hold.reserved_usd if child_hold.state == "reserved" else child_hold.settled_usd
    )
    assert child_used is not None
    assert await _shared_daily_budget_used(session, business_date=day) == used + child_used


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unknown",
    [
        False,
        True,
        "capacity",
        "unresolved",
        "veto",
        "rollback",
        "sec_only",
        "superseded_price",
        "superseded_disclosure",
        "superseded_unfrozen_price",
    ],
)
async def test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
    migrated_database_url, monkeypatch, tmp_path, unknown, independent=False
):
    frozen_without_price = unknown == "superseded_unfrozen_price"
    if frozen_without_price:
        unknown = "sec_only"
    factory, settings, store, captured, pointer, batch_id, materials_id = await prepared_materials(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        sec_only=unknown in ("capacity", "sec_only"),
        independent=independent,
        # A real imported original large enough for byte fallback to exceed the
        # unchanged 64k allowance even after v2 lossless input compression.
        exhibit_text=(
            "Detailed quarterly reconciliation identifiers: "
            + " ".join(sha256(str(i).encode()).hexdigest() for i in range(200))
        )
        if unknown == "tokenizer_missing"
        else "",
    )
    expected_count = 2 if unknown in ("capacity", "sec_only") else 3
    from nexus.monitoring.mixed_readback import mixed_batch_readback

    async with factory() as session:
        empty = await mixed_batch_readback(session, UUID(int=0))
        assert empty == {
            "impact_review": "not_verified",
            "coverage": "unresolved",
            "unresolved_event_count": 0,
        }
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        )
    async with factory() as session:
        roots = tuple(await session.scalars(select(AgentResearchRunRecord)))
        assert len(roots) == len({r.instrument_id for r in roots}) == 3
        assert all(
            r.status == "completed"
            and r.input_tokens is not None
            and r.output_tokens is not None
            and r.settled_cost_usd is not None
            for r in roots
        )
        root_ids = {r.id for r in roots}
        budget_day = parent.decision_time.date()
        assert later.date() == budget_day
        budgets_before = await budget_rows(session)
        for model in _BUDGET_MODELS:
            assert all(
                r.business_date == budget_day and r.state == "settled" and r.settled_usd is not None
                for r in await session.scalars(select(model))
            )
        assert {
            r.run_id for r in await session.scalars(select(AgentDailyBudgetReservationRecord))
        } == root_ids
        assert len(budgets_before[AgentPortfolioSynthesisBudgetReservationRecord]) == (
            0 if independent else 1
        )
        assert not budgets_before[PortfolioDecisionBudgetReservationRecord]
        shared_before = await _shared_daily_budget_used(session, business_date=budget_day)
    assert settings.agent_daily_budget_usd == Decimal("3.00")
    assert settings.agent_monitor_daily_sdk_call_limit == 9
    captured["clock"][0] = later
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    if independent:
        from nexus.monitoring.batches import scan_stock_batch
        from nexus.persistence.models.monitoring import MonitorCheck
        from nexus.persistence.models.runtime import OutboxEvent

        async with factory.begin() as session:
            notification_ids = set(await session.scalars(select(OutboxEvent.id)))
            scanned = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=later,
            )
            repeated_scan = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=later,
            )
            assert scanned["checks"] == (1 if unknown in ("capacity", "sec_only") else 2)
            assert scanned["new_triggers"] == scanned["notifications"] == 0
            assert scanned["model_requests"] == scanned["provider_requests"] == 0
            assert repeated_scan["batch_id"] == scanned["batch_id"]
            assert repeated_scan["new_triggers"] == repeated_scan["notifications"] == 0
            assert set(await session.scalars(select(OutboxEvent.id))) == notification_ids
            checks = tuple(
                await session.scalars(
                    select(MonitorCheck).where(MonitorCheck.instrument_id == pointer.instrument_id)
                )
            )
            assert len({c.scheduled_at for c in checks}) == 2
            assert all(
                c.scheduled_at.minute % 5 == 0 and c.scheduled_at.second == 0 for c in checks
            )
    async with factory() as session, session.begin():
        monday = later + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )
    settings = settings.model_copy(
        update={
            "agent_paper_formal_model_config_id": parent.model_config_id,
            "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
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
    assert admitted["status"] == "admitted" and admitted["reserved_sdk_calls"] == 3, admitted
    repeated = await run(settings, args)
    assert repeated["run_id"] == admitted["run_id"] and repeated["job_id"] == admitted["job_id"]
    from nexus.monitoring.context import read_monitor_context

    async with factory() as session:
        research = await session.get(AgentResearchRunRecord, UUID(admitted["run_id"]))
        frozen = await read_monitor_context(
            session,
            snapshot_id=research.data_snapshot_id,
            instrument_id=research.instrument_id,
            as_of=research.decision_time,
        )
        assert frozen is not None and frozen[0].purpose == "monitor_increment_v2"
        assert (
            len(frozen[0].trigger_checks) == expected_count
            and len(frozen[0].materials.contents) == 2
        )
        assert {item.kind for item in frozen[0].trigger_checks} == (
            {"disclosure"} if unknown in ("capacity", "sec_only") else {"price", "disclosure"}
        )
        assert research.instrument_id == pointer.instrument_id
        assert tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        ) == tuple(count + 1 for count in before)
        rows = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        )
        assert len(rows) == expected_count and all(
            row.research_run_id == research.id and row.status == "admitted" for row in rows
        )
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == research.id)
        )
        assert member.batch_id == batch_id and member.reserved_sdk_calls == 3
        assert research.decision_time.date() == member.session_date == budget_day
        assert member.daily_sdk_call_limit == 9
        child_hold = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == research.id
            )
        )
        assert child_hold.state == "reserved" and child_hold.settled_usd is None
        await assert_shared_day_preserved(
            session, budgets_before, budget_day, shared_before, child_hold
        )
        assert (
            await session.scalar(
                select(func.sum(MonitorIncrement.reserved_sdk_calls)).where(
                    MonitorIncrement.account_id == pointer.account_id,
                    MonitorIncrement.session_date == budget_day,
                )
            )
            == 3
        )
    if unknown == "capacity":
        from nexus.audit.service import append_audit_event
        from nexus.monitoring.preclaim import (
            close_capacity_rejected_increment_once,
            preclaim_rejection_qualified,
        )

        assert rows[0].kind == "disclosure"
        async with factory() as session, session.begin():
            # Dispatch follows admission on the shared advancing database clock.
            later = await session.scalar(select(func.clock_timestamp()))
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action="research.monitor.worker_dispatched",
                schema_version="monitor_dispatch_v1",
                occurred_at=later,
                payload={"trigger_id": str(rows[0].id), "job_id": str(member.job_id)},
            )
        closed = await close_capacity_rejected_increment_once(
            settings,
            account_id=pointer.account_id,
            trigger_id=rows[0].id,
            job_id=member.job_id,
            code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
        )
        assert closed["status"] == "failed" and closed["reason"] == "MONITOR_CAPACITY_REJECTED"
        assert (
            await close_capacity_rejected_increment_once(
                settings,
                account_id=pointer.account_id,
                trigger_id=rows[0].id,
                job_id=member.job_id,
                code="CODEX_CAPACITY_POOL_WIRE_EXCEEDED",
            )
            == closed
        )
        async with factory() as session:
            # Read the native close proof after its committed terminal facts.
            later = await session.scalar(select(func.clock_timestamp()))
            child = await session.get(AgentResearchRunRecord, member.run_id)
            assert (child.input_tokens, child.output_tokens, child.settled_cost_usd) == (0, 0, 0)
            assert await preclaim_rejection_qualified(session, run=child, now=later)
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert (
                hold.state == "released"
                and (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
            )
        reconciled = await run(settings, argparse.Namespace(action="reconcile"))
        from nexus.ops.monitor_dispatch import confirm_monitor_shutdown, dispatch_configured_once
        from nexus.persistence.models.monitoring import (
            MonitorCoverageReceipt,
            MonitorMixedEventReceipt,
        )

        async with factory() as session:
            coverage = tuple(
                await session.scalars(
                    select(MonitorCoverageReceipt).where(
                        MonitorCoverageReceipt.run_id == member.run_id
                    )
                )
            )
            assert len(coverage) == 2 and all(row.status == "gap" for row in coverage)
            assert not await session.scalar(
                select(MonitorMixedEventReceipt.id).where(
                    MonitorMixedEventReceipt.run_id == member.run_id
                )
            )
        assert await confirm_monitor_shutdown(
            settings, account_id=pointer.account_id, trigger_id=rows[0].id
        )
        followed = await dispatch_configured_once(
            settings, trigger_id=rows[0].id, calendar_authority_id=CALENDAR_ID
        )
        assert followed["status"] == "failed" and followed["coverage"] == "gap"
        await factory.kw["bind"].dispose()
        return
    from nexus.agents import codex_staged
    from nexus.worker import run_agent_worker_once
    from unit.agents.test_codex_sdk import _restore_view

    native = codex_staged.invoke_codex
    stages = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def tracked(payload, **kwargs):
        wire = _restore_view(payload)
        stages.append(wire["stage"])
        if unknown is False and wire["stage"] == "research":
            entered.set()
            await release.wait()
        security = wire["frozen_inputs"]["security_snapshot"]
        assert security["purpose"] == "monitor_increment_v2"
        context = next(
            item["monitor_context"]
            for item in security["items"]
            if item["source"] == "nexus_monitor_increment_v2"
        )
        assert len(context["materials"]["contents"]) == 2
        assert len(context["trigger_checks"]) == expected_count
        assert any(
            "Additional GAAP reconciliation" in document["text"]
            for content in context["materials"]["contents"]
            for document in content["archive"]["documents"]
        )
        if unknown is True and wire["stage"] == "critique":
            raise TimeoutError("controlled mixed unknown usage")
        from helpers.mixed_stage import add_mixed_fields

        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        add_mixed_fields(body, wire)
        if unknown == "unresolved" and wire["stage"] == "research":
            proposal = next(
                row["mixed_event_proposal"]
                for row in body["roles"]
                if row["role"] == "market_state"
            )
            proposal["items"][0]["status"] = "unresolved"
        if unknown == "veto" and wire["stage"] == "critique":
            row = next(row for row in body["roles"] if row["role"] == "evidence_audit")
            row["mixed_event_review"]["decision"] = "rejected"
            row["vote"] = "oppose"
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", tracked)
    if unknown == "tokenizer_missing":
        from nexus.agents import codex_token_budget

        monkeypatch.setattr(
            codex_token_budget, "tokenizer_cache_path", lambda: tmp_path / "missing-vocabulary"
        )
        assert codex_token_budget.local_vocabulary() is None
        from nexus.agents import codex_capacity

        original_allowance = codex_capacity.codex_input_allowance
        measured = []

        def observed_allowance(wire, schema):
            value = original_allowance(wire, schema)
            measured.append((len(wire.encode()), value))
            return value

        monkeypatch.setattr(codex_capacity, "codex_input_allowance", observed_allowance)
        with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_INPUT_EXCEEDED$"):
            await run_agent_worker_once(
                settings, job_ids=(member.job_id,), formal_stage_capacity=True
            )
        # Envelope diagnostics also count each stage; the final count is the
        # exact research request that preclaim refuses before any Job claim.
        assert measured and max(size for size, _ in measured) <= 100479
        assert measured[-1][1] > 64000
        assert stages == []
        async with factory() as session:
            child = await session.get(AgentResearchRunRecord, member.run_id)
            job = await session.get(Job, member.job_id)
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert child.status == "prepared" and job.status == "queued"
            assert hold.state == "reserved" and child.settled_cost_usd is None
            assert tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
                ]
            ) == tuple(count + 1 for count in before)
        await factory.kw["bind"].dispose()
        return
    if unknown is False:
        from nexus.ops.research_monitor import scan_configured_once
        from nexus.persistence.models.monitoring import MonitorCheck
        from nexus.persistence.models.runtime import OutboxEvent

        async with factory() as session:
            existing_checks = set(await session.scalars(select(MonitorCheck.id)))
            outbox_count = await session.scalar(select(func.count()).select_from(OutboxEvent))
        worker = asyncio.create_task(
            run_agent_worker_once(settings, job_ids=(member.job_id,), formal_stage_capacity=True)
        )
        try:
            await asyncio.wait_for(entered.wait(), 30)
            async with factory() as session:
                assert (await session.get(Job, member.job_id)).status == "running"
                assert await session.scalar(
                    select(JobLease.id).where(
                        JobLease.job_id == member.job_id, JobLease.expires_at > later
                    )
                )
            scanned = await scan_configured_once(settings, calendar_authority_id=CALENDAR_ID)
            assert scanned["new_triggers"] == scanned["notifications"] == 0
            assert not worker.done() and stages == ["research"]
            async with factory() as session:
                checks = tuple(await session.scalars(select(MonitorCheck)))
                assert any(row.id not in existing_checks for row in checks)
                assert (
                    await session.scalar(select(func.count()).select_from(OutboxEvent))
                    == outbox_count
                )
        finally:
            release.set()
            result = await worker
        assert result
    else:
        assert await run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
    assert stages == (
        ["research", "critique"] if unknown is True else ["research", "critique", "synthesis"]
    )
    assert not await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    if unknown is True:
        captured["clock"][0] += timedelta(minutes=2)
    if unknown != "rollback":
        finished = await run(settings, argparse.Namespace(action="finish", trigger_id=rows[0].id))
        assert finished["status"] == ("unknown" if unknown is True else "completed"), finished
        assert finished["impact_review"] == "not_verified" and finished["coverage"] == "unresolved"
        assert finished["unresolved_event_count"] == expected_count
        assert await run(settings, args) == {**admitted, "status": finished["status"]}
    async with factory() as session:
        child = await session.get(AgentResearchRunRecord, member.run_id)
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == child.id
            )
        )
        assert (child.settled_cost_usd is None) == (unknown is True)
        assert reservation.state == ("reserved" if unknown is True else "settled")
        assert reservation.run_id not in root_ids
        await assert_shared_day_preserved(
            session, budgets_before, budget_day, shared_before, reservation
        )
        assert settings.agent_daily_budget_usd == Decimal("3.00")
        if independent:
            from nexus.agents.research_only import require_formal_research

            for frozen_run, code in (
                (parent, "INDEPENDENT_MONITOR_FORMAL_CONSUMPTION_FORBIDDEN"),
                (child, "MONITOR_FORMAL_CONSUMPTION_FORBIDDEN"),
            ):
                with pytest.raises(ValueError, match=code):
                    await require_formal_research(session, frozen_run)
        members = tuple(
            await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        )
        assert all(
            t.status
            == (
                "unknown"
                if unknown is True
                else "admitted"
                if unknown == "rollback"
                else "completed"
            )
            for t in members
        )
        assert (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
    from nexus.monitoring.admission import require_monitor_account_available

    async with factory() as session, session.begin():
        from nexus.agents.research_stages.routes import selected_route

        formal_route = await selected_route(session, member.job_id)
        # A known model result is insufficient while the native trigger is still
        # admitted. Keep the old unselected contract in an explicit legacy case.
        if unknown is True or (unknown == "rollback" and formal_route is not None):
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=pointer.account_id, now=captured["clock"][0]
                )
        else:
            await require_monitor_account_available(
                session, account_id=pointer.account_id, now=captured["clock"][0]
            )
    if unknown in (False, "unresolved", "veto"):
        from nexus.persistence.models.agents import AgentResearchOutputRecord

        async with factory() as session:
            terminal = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == member.run_id,
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            terminal_id = terminal.id
        for kind in ("price", "disclosure"):
            with pytest.raises(ValueError, match="MONITOR_MIXED_ATOMIC_RECONCILIATION_REQUIRED"):
                await run(
                    settings,
                    argparse.Namespace(
                        action="install",
                        kind=kind,
                        output_id=terminal_id,
                        expected_predecessor_id=None,
                    ),
                )
    if frozen_without_price or (isinstance(unknown, str) and unknown.startswith("superseded_")):
        from helpers.monitor_generation import assert_old_run_superseded

        if frozen_without_price:
            assert {item["kind"] for item in frozen[0].materials.parents} == {"disclosure"}
        await assert_old_run_superseded(
            factory,
            settings,
            captured,
            pointer,
            member.run_id,
            native,
            monkeypatch,
            advanced_kind="price" if frozen_without_price else unknown.removeprefix("superseded_"),
            mixed_member=member,
            unfrozen_kind="price" if frozen_without_price else None,
        )
        assert len(stages) == 3
        await factory.kw["bind"].dispose()
        return
    if unknown == "rollback":
        import nexus.monitoring.disclosure_installation as disclosure_installation

        async def fail_second(*args, **kwargs):
            raise ValueError("controlled second-kind install failure")

        monkeypatch.setattr(
            disclosure_installation, "_install_mixed_or_legacy_disclosure", fail_second
        )
    reconciled = await run(settings, argparse.Namespace(action="reconcile"))
    from nexus.ops.monitor_dispatch import confirm_monitor_shutdown
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorMixedEventReceipt

    async with factory() as session:
        receipts = tuple(
            await session.scalars(
                select(MonitorMixedEventReceipt).where(
                    MonitorMixedEventReceipt.run_id == member.run_id
                )
            )
        )
        coverage = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == member.run_id)
            )
        )
        if unknown is True:
            assert not receipts and not coverage
            assert reconciled["awaiting_mixed_review"] == 1
        else:
            assert len(coverage) == 2 and {row.kind for row in coverage} == {"price", "disclosure"}
            assert len(receipts) == (
                0
                if unknown in ("veto", "rollback")
                else 2
                if unknown in ("unresolved", "sec_only")
                else 3
            )
            if unknown == "sec_only":
                assert sorted(row.status for row in coverage) == ["gap", "installed"]
                assert any(
                    row.kind == "price" and row.reason == "MONITOR_KIND_PARENT_NOT_FROZEN"
                    for row in coverage
                )
            elif unknown == "unresolved":
                assert sorted(row.status for row in coverage) == ["gap", "installed"]
                assert any(row.reason == "MONITOR_KIND_EVENTS_UNRESOLVED" for row in coverage)
            else:
                assert all(
                    row.status == ("gap" if unknown in ("veto", "rollback") else "installed")
                    for row in coverage
                )
            if unknown == "rollback":
                from nexus.persistence.models.monitoring import MonitorPlan

                assert not await session.scalar(
                    select(MonitorPlan.id).where(MonitorPlan.source_run_id == member.run_id)
                )
                assert all(
                    row.reason == "MONITOR_CONDITION_REPLACEMENT_REJECTED" for row in coverage
                )
    assert await confirm_monitor_shutdown(
        settings, account_id=pointer.account_id, trigger_id=rows[0].id
    ) == (unknown is not True)
    replay = await run(settings, argparse.Namespace(action="finish", trigger_id=rows[0].id))
    assert replay["unresolved_event_count"] == (
        3
        if unknown is True or unknown in ("veto", "rollback")
        else 1
        if unknown == "unresolved"
        else 0
    )
    if unknown is not True:
        async with factory.begin() as session:
            await require_monitor_account_available(
                session, account_id=pointer.account_id, now=captured["clock"][0]
            )
    if replay["impact_review"] == "verified":
        assert replay["reason"] == "MONITOR_EVENT_REVIEW_VERIFIED"
        assert replay["native_reason"] == "MONITOR_EVENT_REVIEW_UNRESOLVED"
    repeated_reconcile = await run(settings, argparse.Namespace(action="reconcile"))
    assert repeated_reconcile["recorded"] == 0
    if unknown is True:
        assert repeated_reconcile["awaiting_mixed_review"] == reconciled["awaiting_mixed_review"]
    from nexus.ops.monitor_dispatch import dispatch_configured_once

    followed = await dispatch_configured_once(
        settings, trigger_id=rows[0].id, calendar_authority_id=CALENDAR_ID
    )
    assert followed["unresolved_event_count"] == replay["unresolved_event_count"]
    assert followed["impact_review"] == replay["impact_review"]
    assert len(stages) == (2 if unknown is True else 3)
    if independent and unknown is False:
        from helpers.independent_monitor_readview import verify_readview

        await verify_readview(
            migrated_database_url,
            factory,
            monkeypatch,
            pointer,
            member.run_id,
            batch_id,
            captured["clock"][0],
        )
    await factory.kw["bind"].dispose()
