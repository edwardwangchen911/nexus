"""Saved monitor facts through authenticated API; browser is explicitly opt-in."""

import json
import os
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from helpers.monitor_materials import prepared_materials
from helpers.monitoring_readview_browser import stage_facts, verify_monitoring_browser
from helpers.paper_api import app, headers
from helpers.paper_batch import CALENDAR_ID
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import (
    AuditEvent,
    IdempotencyRecord,
    Job,
    JobLease,
    OutboxEvent,
)
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    [
        None,
        "MONITOR_KIND_EVENTS_UNRESOLVED",
        "MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED",
        "MONITOR_DAILY_REPLACEMENT_REJECTED",
        "unknown_usage",
        "addressed",
    ],
)
async def test_saved_monitoring_api_and_optional_normal_browser(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    reason,
):
    if reason in {"unknown_usage", "addressed"}:
        from integration.test_monitor_mixed_host_pg import ready_host_batch

        factory, settings, captured, pointer, batch_id, _ = await ready_host_batch(
            migrated_database_url, monkeypatch, tmp_path
        )
    else:
        factory, settings, _, captured, pointer, batch_id, _ = await prepared_materials(
            migrated_database_url, monkeypatch, tmp_path
        )
    now = captured["clock"][0]
    try:
        if reason in {"unknown_usage", "addressed"}:
            from helpers.mixed_stage import add_mixed_fields
            from integration.test_candidate_multistock_path import _set_test_insert_clock
            from nexus.agents import codex_staged
            from nexus.monitoring.reconciliation import reconcile_account_results
            from nexus.ops.monitor_dispatch import dispatch_configured_once
            from nexus.ops.research_monitor import parser, run
            from unit.agents.test_codex_sdk import _restore_view

            native = codex_staged.invoke_codex
            stages = []

            async def unknown(payload, **kwargs):
                wire = _restore_view(payload)
                stages.append(wire["stage"])
                if reason == "unknown_usage" and wire["stage"] == "critique":
                    raise TimeoutError("controlled monitoring unknown")
                raw, usage = await native(payload, **kwargs)
                body = json.loads(raw)
                add_mixed_fields(body, wire)
                return json.dumps(body), usage

            monkeypatch.setattr(codex_staged, "invoke_codex", unknown)
            async with factory() as session:
                if reason == "addressed":
                    prior_plans = {
                        plan.kind: (plan.id, plan.generation)
                        for plan in await session.scalars(
                            select(MonitorPlan).where(
                                MonitorPlan.account_id == pointer.account_id,
                                MonitorPlan.instrument_id == pointer.instrument_id,
                                MonitorPlan.source_run_id == pointer.source_run_id,
                            )
                        )
                    }
                    assert set(prior_plans) == {"price", "disclosure"}
                    assert {generation for _, generation in prior_plans.values()} == {1}
                trigger = await session.scalar(
                    select(MonitorTrigger)
                    .where(MonitorTrigger.batch_id == batch_id)
                    .order_by(MonitorTrigger.id)
                )
            dispatched = await dispatch_configured_once(
                settings, trigger_id=trigger.id, calendar_authority_id=CALENDAR_ID
            )
            native_run_id = UUID(str(dispatched["run_id"]))
            assert dispatched["status"] == (
                "unknown" if reason == "unknown_usage" else "completed"
            ), dispatched
            assert stages == (
                ["research", "critique"]
                if reason == "unknown_usage"
                else ["research", "critique", "synthesis"]
            )
            # Respect the original persisted lease, rather than assuming a
            # two-minute timeout authorizes closeout of this formal profile.
            async with factory() as session:
                end = await session.scalar(select(func.max(JobLease.expires_at)))
            captured["clock"][0] = end + timedelta(seconds=1)
            now = captured["clock"][0]
            await _set_test_insert_clock(factory, now)
            await run(settings, parser().parse_args(["finish", "--trigger-id", str(trigger.id)]))
            async with factory() as session, session.begin():
                reconciled = await reconcile_account_results(
                    session,
                    account_id=pointer.account_id,
                    instrument_ids=(pointer.instrument_id,),
                    now=now,
                )
                if reason == "unknown_usage":
                    # The normal mixed consumer deliberately requires known
                    # settlement before recording coverage. Unknown fees remain
                    # awaiting review; elapsed leases do not grant acceptance.
                    assert reconciled.get("awaiting_mixed_review") == 1, reconciled
                    assert (
                        await session.scalar(
                            select(func.count())
                            .select_from(MonitorCoverageReceipt)
                            .where(MonitorCoverageReceipt.run_id == native_run_id)
                        )
                        == 0
                    )
                else:
                    child = await session.get(AgentResearchRunRecord, native_run_id)
                    member = await session.scalar(
                        select(MonitorIncrement).where(MonitorIncrement.run_id == native_run_id)
                    )
                    output = await session.scalar(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == native_run_id,
                            AgentResearchOutputRecord.role.is_(None),
                            AgentResearchOutputRecord.status == "completed",
                        )
                    )
                    assert child.status == "completed" and child.settled_cost_usd is not None
                    assert (
                        member.batch_id == batch_id and member.snapshot_id == child.data_snapshot_id
                    )
                    replacements = {
                        plan.kind: plan
                        for plan in await session.scalars(
                            select(MonitorPlan).where(MonitorPlan.source_run_id == native_run_id)
                        )
                    }
                    assert set(replacements) == set(prior_plans)
                    for kind, plan in replacements.items():
                        prior_id, prior_generation = prior_plans[kind]
                        assert plan.generation == prior_generation + 1 == 2
                        assert plan.predecessor_id == prior_id
                        assert plan.account_id == pointer.account_id
                        assert plan.instrument_id == pointer.instrument_id
                        assert plan.source_output_id == output.id
                    receipts = {
                        receipt.kind: receipt
                        for receipt in await session.scalars(
                            select(MonitorCoverageReceipt).where(
                                MonitorCoverageReceipt.run_id == native_run_id
                            )
                        )
                    }
                    assert set(receipts) == set(replacements)
                    for kind, receipt in receipts.items():
                        assert receipt.status == "installed"
                        assert receipt.output_id == output.id
                        assert receipt.prior_plan_id == prior_plans[kind][0]
                        assert receipt.active_plan_id == replacements[kind].id
        elif reason is not None:
            # Controlled persisted coverage projection, not a synthetic model
            # verdict or a claim that a real research source had this outcome.
            async with factory() as session, session.begin():
                for kind in ("price", "disclosure"):
                    plan = await session.scalar(
                        select(MonitorPlan).where(
                            MonitorPlan.account_id == pointer.account_id,
                            MonitorPlan.instrument_id == pointer.instrument_id,
                            MonitorPlan.kind == kind,
                        )
                    )
                    assert (
                        await session.scalar(
                            select(MonitorCoverageReceipt.id).where(
                                MonitorCoverageReceipt.run_id == pointer.source_run_id,
                                MonitorCoverageReceipt.kind == kind,
                            )
                        )
                        is None
                    )
                    session.add(
                        MonitorCoverageReceipt(
                            account_id=pointer.account_id,
                            instrument_id=pointer.instrument_id,
                            run_id=pointer.source_run_id,
                            output_id=pointer.source_output_id,
                            kind=kind,
                            prior_plan_id=plan.id,
                            active_plan_id=None,
                            research_as_of=now,
                            observed_at=now,
                            status="gap",
                            reason=reason,
                            quality_status="complete",
                        )
                    )
        # This dated synthetic database and its normal worker clock share one
        # explicit read instant. HMAC timestamps and elapsed deadlines stay real.
        monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
        application = app(migrated_database_url, now=now)
        path = f"/internal/v1/research-runs/{pointer.source_run_id}"
        guarded = (
            AgentResearchRunRecord,
            AgentDailyBudgetReservationRecord,
            Job,
            MonitorPlan,
            MonitorTrigger,
            MonitorCoverageReceipt,
            OutboxEvent,
            AuditEvent,
            IdempotencyRecord,
        )
        async with factory() as session:
            stage_before = await stage_facts(session)
            before = {model: set(await session.scalars(select(model.id))) for model in guarded}
            budget_states = [
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ]
            if reason == "unknown_usage":
                assert any(
                    state == "reserved" and settled is None
                    for _, state, _, settled in budget_states
                )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        price, disclosure = data["monitoring"], data["disclosure_monitoring"]
        assert price["execution_locked"] and disclosure["execution_locked"]
        assert disclosure["trigger_count"] == len(disclosure["triggers"]) == 2
        assert {row["batch_id"] for row in disclosure["triggers"]} == {str(batch_id)}
        if reason == "unknown_usage":
            assert price["state"] == disclosure["state"] == "unknown"
            assert (
                price["coverage_reason"]
                == disclosure["coverage_reason"]
                == "awaiting_reconciliation"
            )
            assert all(row["usage_status"] == "unknown" for row in disclosure["triggers"])
        elif reason == "addressed":
            for kind, view in (("price", price), ("disclosure", disclosure)):
                assert view["state"] == "superseded"
                assert view["plan_id"] == str(prior_plans[kind][0])
                assert view["replacement_run_id"] == str(native_run_id)
            assert all(
                row["event_review"] == "addressed" and row["usage_status"] == "known"
                for row in disclosure["triggers"]
            )
            assert all(
                row["run_id"] and row["run_id"] != str(pointer.source_run_id)
                for row in disclosure["triggers"]
            )
        else:
            assert all(row["event_review"] == "not_performed" for row in disclosure["triggers"])
        if reason not in {None, "unknown_usage", "addressed"}:
            expected = {
                "MONITOR_KIND_EVENTS_UNRESOLVED": "events_unresolved",
                "MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED": "review_unverified",
                "MONITOR_DAILY_REPLACEMENT_REJECTED": "replacement_rejected",
            }[reason]
            assert price["coverage_reason"] == disclosure["coverage_reason"] == expected
            assert price["state"] == disclosure["state"] == "withdrawn"
        browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
        if browser_root:
            directory = Path(browser_root).resolve() / (reason or "pending")
            directory.mkdir(parents=True, exist_ok=False)
            (directory / "ready.json").write_text(
                json.dumps(
                    {
                        "run_id": str(pointer.source_run_id),
                        "instrument_id": str(pointer.instrument_id),
                        "reason": reason,
                        "replacement_run_id": str(native_run_id) if reason == "addressed" else None,
                        "response": response.json(),
                    }
                )
            )
            await verify_monitoring_browser(application, directory=directory)
        async with factory() as session:
            assert await stage_facts(session) == stage_before
            assert {
                model: set(await session.scalars(select(model.id))) for model in guarded
            } == before
            assert [
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            ] == budget_states
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_disclosure_readview_survives_slow_archive_preparation(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers import disclosure_observations
    from integration import test_candidate_multistock_path as candidate

    native_monotonic = candidate.monotonic
    native_capture = disclosure_observations.import_baseline_capture
    preparation_elapsed = 0.0

    def elapsed_clock():
        return native_monotonic() + preparation_elapsed

    async def delayed_capture(*args, **kwargs):
        nonlocal preparation_elapsed
        # Advance setup time past the fixed historical research cutoff without
        # sleeping or changing any persisted evidence or business time limit.
        preparation_elapsed += 3.0
        return await native_capture(*args, **kwargs)

    browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
    if browser_root:
        monkeypatch.setenv(
            "NEXUS_TEST_MONITOR_BROWSER_DIR", str(Path(browser_root) / "slow-preparation")
        )
    monkeypatch.setattr(candidate, "monotonic", elapsed_clock)
    monkeypatch.setattr(disclosure_observations, "import_baseline_capture", delayed_capture)
    await test_saved_monitoring_api_and_optional_normal_browser(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "MONITOR_KIND_EVENTS_UNRESOLVED",
    )
